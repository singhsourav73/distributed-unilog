distributed-unilog/
├── docker-compose.yml # Infrastructure orchestration
├── prometheus.yml # Telemetry configuration
├── go.mod # Go module dependencies
├── go.sum
├── scripts/
│ └── load_test.sh
├── models/ # Shared domain logic
│ └── models.go # Structs used across boundaries
├── gateway/ # Entry point for external clients
│ ├── main.go
│ └── Dockerfile
├── producer/ # Ingestion layer
│ ├── main.go
│ └── Dockerfile
└── consumer/ # Processing and persistence layer
| ├── main.go
| └── Dockerfile
