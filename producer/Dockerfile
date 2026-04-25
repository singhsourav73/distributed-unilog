FROM --platform=$BUILDPLATFORM golang:1.21-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY producer/ ./producer/
COPY models/ ./models/

ARG TARGETARCH

RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -a -installsuffix cgo -o producer-service ./producer/main.go

# Production Stage
FROM alpine:latest  
RUN apk --no-cache add ca-certificates
WORKDIR /root/

COPY --from=builder /app/producer-service .

RUN chmod +x ./producer-service

# Producer internal port
EXPOSE 8080
CMD ["./producer-service"]