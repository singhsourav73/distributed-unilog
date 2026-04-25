FROM --platform=$BUILDPLATFORM golang:1.21-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY consumer/ ./consumer/
COPY models/ ./models/

ARG TARGETARCH

RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -a -installsuffix cgo -o consumer-service ./consumer/main.go

# Production Stage
FROM alpine:latest  
RUN apk --no-cache add ca-certificates
WORKDIR /root/

COPY --from=builder /app/consumer-service .

RUN chmod +x ./consumer-service

# Consumer port (for health checks and metrics)
EXPOSE 8082
CMD ["./consumer-service"]