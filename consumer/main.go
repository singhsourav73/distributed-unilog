package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/singhsourav73/distributed-unilog/models"
)

var db *sql.DB
var rdb *redis.Client

const (
	BatchSize     = 100
	FlushInterval = 2 * time.Second
)

// initInfraStructure handles resilient connections to our dependencies
func initInfraStructure() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb = redis.NewClient(&redis.Options{Addr: redisAddr})

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		os.Getenv("DB_HOST"), os.Getenv("DB_PORT"), os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"), os.Getenv("DB_NAME"))

	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to open DB connection: %v", err)
	}

	log.Println("Waiting for PostgreSQL to become ready...")
	for i := 1; i <= 10; i++ {
		if err = db.Ping(); err == nil {
			break
		}
		log.Printf("Database not ready (Attempt %d/10)... retrying in 2s", i)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("Could not establish database connection after retries: %v", err)
	}
	log.Println("Successfully connected to PostgreSQL!")

	createTableQuery := `
	CREATE TABLE IF NOT EXISTS processed_logs (
		id UUID PRIMARY KEY,
		organization_id VARCHAR(255),
		level VARCHAR(50),
		message TEXT,
		source VARCHAR(255),
		timestamp TIMESTAMP
	);`
	if _, err = db.Exec(createTableQuery); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// Initialize the RedisBloom filter structure if it doesn't exist
	// ERROR: 0.01 is the error rate, 1000000 is the initial capacity
	_, err = rdb.Do(context.Background(), "BF.RESERVE", "global_log_filter", "0.01", "1000000").Result()
	if err != nil && err.Error() != "ERR item exists" {
		log.Printf("Note: Bloom filter initialization issue (might already exist): %v", err)
	}
}

// flushBatch handles bulk database inserts and pipelines Redis updates safely
func flushBatch(batch []models.LogEvent) {
	if len(batch) == 0 {
		return
	}

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return
	}

	// Prepare statement for bulk execution. ON CONFLICT handles database-level deduplication
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO processed_logs (id, organization_id, level, message, source, timestamp) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (id) DO NOTHING`)
	if err != nil {
		log.Printf("Failed to prepare statement: %v", err)
		tx.Rollback()
		return
	}
	defer stmt.Close()

	for _, event := range batch {
		_, err = stmt.ExecContext(ctx, event.ID, event.OrganizationID, event.Level, event.Message, event.Source, event.Timestamp)
		if err != nil {
			log.Printf("Failed to insert event %s: %v", event.ID, err)
		}
	}

	if err = tx.Commit(); err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		return
	}

	// Pipeline Redis updates to avoid multiple network round-trips
	pipe := rdb.Pipeline()
	for _, event := range batch {
		// Store the ID permanently in the global Redis Bloom filter
		pipe.Do(ctx, "BF.ADD", "global_log_filter", event.ID)
	}
	if _, err = pipe.Exec(ctx); err != nil {
		log.Printf("Failed to pipeline Redis Bloom updates: %v", err)
	}

	log.Printf("Successfully flushed batch of %d logs to database.", len(batch))
}

func main() {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(":8082", nil); err != nil {
			log.Fatalf("Metrics server failed: %v", err)
		}
	}()

	initInfraStructure()
	defer db.Close()

	kafkaBroker := os.Getenv("KAFKA_BROKERS")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		GroupID:  "log-consumer-group",
		Topic:    "log-events",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	msgChan := make(chan models.LogEvent, BatchSize*2)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Consumer started. Ingesting messages...")

	// Background Reader: Pulls from Kafka and feeds the channel
	go func() {
		for {
			m, err := reader.ReadMessage(context.Background())
			if err != nil {
				log.Printf("Kafka read error: %v", err)
				continue
			}

			var event models.LogEvent
			if err := json.Unmarshal(m.Value, &event); err != nil {
				continue
			}
			msgChan <- event
		}
	}()

	var currentBatch []models.LogEvent
	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()

	// Main Processor: Safely manages state and limits network I/O
	for {
		select {
		case event := <-msgChan:

			// Hit the persistent RedisBloom module using raw commands.
			// This returns true if the event *might* exist, and false if it *definitely* does not.
			exists, err := rdb.Do(context.Background(), "BF.EXISTS", "global_log_filter", event.ID).Bool()
			if err == nil && exists {
				continue // We've likely processed this. The DB ON CONFLICT acts as the ultimate fallback.
			}

			currentBatch = append(currentBatch, event)

			if len(currentBatch) >= BatchSize {
				flushBatch(currentBatch)
				currentBatch = currentBatch[:0]
			}

		case <-ticker.C:
			if len(currentBatch) > 0 {
				flushBatch(currentBatch)
				currentBatch = currentBatch[:0]
			}

		case <-sigChan:
			log.Println("Termination signal received. Flushing final batch and shutting down...")
			if len(currentBatch) > 0 {
				flushBatch(currentBatch)
			}
			return
		}
	}
}
