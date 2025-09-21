# ----------- Build stage -----------
FROM golang:1.25 AS builder

WORKDIR /app

# Copy go.mod and go.sum first for caching
COPY go.mod go.sum ./
RUN go mod download

# Now copy the rest of the source
COPY . .

# Build statically linked binary
RUN go build -o alert-service main.go

# ----------- Runtime stage -----------
FROM debian:bookworm-slim

# Install CA certs for HTTPS calls to PagerDuty
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/alert-service /app/alert-service

CMD ["./alert-service"]
