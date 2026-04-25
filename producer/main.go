package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
	"github.com/singhsourav73/distributed-unilog/models"
)

func main() {
	kafkaBroker := os.Getenv("KAFKA_BROKERS")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	// Initialize Kafka Writer
	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        "log-events",
		Balancer:     &kafka.Hash{},    // Guarantees routing by Message Key
		RequiredAcks: kafka.RequireOne, // Balance between durability and latency
	}
	defer writer.Close()

	http.HandleFunc("/api/logs", func(w http.ResponseWriter, r *http.Request) {
		var req models.LogEvent
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Enrich the event with UUID and server time
		event := models.NewLogEvent(req.OrganizationID, req.Level, req.Message, req.Source)
		eventBytes, _ := json.Marshal(event)

		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(event.OrganizationID),
			Value: eventBytes,
		})

		if err != nil {
			http.Error(w, "Failed to publish", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"status": "queued", "id": event.ID})
		log.Printf("Published event: %s", event.ID)
	})

	http.Handle("/metrics", promhttp.Handler())

	log.Println("Producer running on: 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
