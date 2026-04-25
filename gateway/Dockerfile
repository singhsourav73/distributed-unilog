# 1. Force builder to use your Mac's native architecture for build speed
FROM --platform=$BUILDPLATFORM golang:1.21-alpine AS builder
WORKDIR /app

# Copy dependency files first to leverage Docker layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the gateway source code and shared models
COPY gateway/ ./gateway/
COPY models/ ./models/

# 2. Expose Docker's automatic target architecture variable
ARG TARGETARCH

# 3. Pass TARGETARCH directly into Go's cross-compiler
RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -a -installsuffix cgo -o gateway-service ./gateway/main.go

# Production Stage
FROM alpine:latest  
RUN apk --no-cache add ca-certificates
WORKDIR /root/

COPY --from=builder /app/gateway-service .

# 4. Explicitly grant execution rights to avoid "permission denied"
RUN chmod +x ./gateway-service

# Gateway port
EXPOSE 8000
CMD ["./gateway-service"]