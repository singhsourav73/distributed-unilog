package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/singhsourav73/distributed-unilog/models"
)

var db *sql.DB
var rdb *redis.Client

func initInfraStructure() {
	// Initialize Redis (for Idemptency/Caching)
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb = redis.NewClient(&redis.Options{Addr: redisAddr})

	// Initialize PostgreSQL
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		os.Getenv("DB_HOST"), os.Getenv("DB_PORT"), os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"), os.Getenv("DB_NAME"))

	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}

	log.Println("Waiting for PostgreSQL to become ready...")
	for i := 1; i <= 10; i++ {
		err = db.Ping()
		if err == nil {
			break
		}
		log.Printf("Database not ready yet (Attempt %d/10), retrying in 2 seconds... (%v)", i, err)
		time.Sleep(2 * time.Second)
	}

	// Create table if doesn't exist
	createTableQuery := `
	CREATE TABLE IF NOT EXISTS processed_logs (
		id UUID PRIMARY KEY,
		organization_id VARCHAR(255),
		level VARCHAR(50),
		message TEXT,
		source VARCHAR(255),
		timestamp TIMESTAMP
	);`
	_, err = db.Exec(createTableQuery)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
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
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	defer reader.Close()

	log.Println("Consumer started, waiting for messages...")

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		var event models.LogEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf("Failed to unmarshal: %v", err)
			continue
		}

		// Fast Deduplication chexk via Cache
		ctx := context.Background()
		if rdb.Exists(ctx, "processed:"+event.ID).Val() > 0 {
			log.Printf("Duplicate event ignored: %s", event.ID)
			continue
		}

		// Insert into PostgreSQL
		insertQuery := `INSERT INTO processed_logs (id, organization_id, level, message, source, timestamp) 
						VALUES ($1, $2, $3, $4, $5, $6)`
		_, err = db.Exec(insertQuery, event.ID, event.OrganizationID, event.Level, event.Message, event.Source, event.Timestamp)

		if err != nil {
			log.Printf("Database insert failed for %s: %v", event.ID, err)
			continue
		}

		// // Mark as processed in cache (expires after 24 hours to keep Redis lean)
		rdb.Set(ctx, "processed:"+event.ID, "1", 24*time.Hour)

		log.Printf("Successfully processed and saved log event: %s", event.ID)
	}
}
