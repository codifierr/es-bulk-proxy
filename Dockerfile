# Build stage
FROM golang:1.26-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates

WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application from cmd
# CGO_ENABLED=0 for static binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -o es-bulk-proxy \
    ./cmd

# Final stage - minimal distroless image
FROM gcr.io/distroless/static-debian11:nonroot

WORKDIR /app

# Copy CA certificates from builder
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy the binary from builder
COPY --from=builder /build/es-bulk-proxy /app/es-bulk-proxy

# Copy config files (optional)
COPY --from=builder /build/configs /app/configs

# Use non-root user
USER nonroot:nonroot

# Expose the application port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/app/es-bulk-proxy"]

# Run the application
ENTRYPOINT ["/app/es-bulk-proxy"]
