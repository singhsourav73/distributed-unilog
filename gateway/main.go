package main

import (
	"context"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

var rdb *redis.Client

func init() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb = redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
}

// gatewayMiddleware acts as an interceptor for incoming traffic
// This is where you inject Rate Limiting or Auth token validation
func gatewayMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.Background()
		clientIP := r.RemoteAddr

		// Basic Redis Rate limiting (eg: max 50 request per 10 seconds per IP)
		key := "rate_limit:" + clientIP
		count, err := rdb.Incr(ctx, key).Result()
		if err == nil && count == 1 {
			rdb.Expire(ctx, key, 10*time.Second)
		}

		if count > 50 {
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			log.Printf("[Gateway] Rate limit triggered for %s", clientIP)
			return
		}

		log.Printf("[Gateway] Proxied %s request for %s", r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

func main() {
	producerURLStr := os.Getenv("PRODUCER_URL")
	if producerURLStr == "" {
		producerURLStr = "http://localhost:8080" // Fallback for local testing
	}

	producerURL, err := url.Parse(producerURLStr)
	if err != nil {
		log.Fatalf("Failed to parse producer URL: %v", err)
	}

	// Create a reverse proxy to forward request to the Producer service
	proxy := httputil.NewSingleHostReverseProxy(producerURL)

	// Gateway Multiplexer
	mux := http.NewServeMux()

	// Route external API traffic to the internal Producer
	mux.Handle("/api/logs", proxy)

	// Gateway's own health check (does not proxy)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Gateway is up and routing traffic"))
	})

	mux.Handle("/metrics", promhttp.Handler())

	// Wrap the router in our middleware
	handler := gatewayMiddleware(mux)

	log.Println("API Gateway listening on: 8000")
	if err := http.ListenAndServe(":8000", handler); err != nil {
		log.Fatalf("Gateway encountered a fatal error: %v", err)
	}
}
