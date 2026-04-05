# Elasticsearch Proxy with Bulk Aggregation

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A production-ready Go service that acts as a transparent Elasticsearch proxy with intelligent bulk request aggregation. Designed to optimize Elasticsearch performance by batching small bulk requests while transparently proxying all other operations.

Built following Go best practices with the [Standard Go Project Layout](https://github.com/golang-standards/project-layout).

## ✨ Features

- **Smart Bulk Aggregation**: Automatically aggregates `/_bulk` requests in memory with per-index buffers
- **Transparent Proxying**: All non-bulk requests pass through unchanged
- **Intelligent Request Classification**: Distinguishes between bulk writes, searches, reads, maintenance, and other operations
- **Time-based Flushing**: Configurable flush intervals (default: 3s)
- **Size-based Flushing**: Automatic flush on size threshold (default: 5MB)
- **Backpressure Handling**: Returns HTTP 429 when buffer is full
- **Retry Logic**: Exponential backoff for failed bulk sends
- **Rich Prometheus Metrics**: Detailed metrics with operation type and HTTP method labels
- **Structured Logging**: JSON logs using [zerolog](https://github.com/rs/zerolog)
- **Configuration Management**: Flexible config using [Viper](https://github.com/spf13/viper)
- **Production Ready**: Health checks, graceful shutdown, resource limits
- **High Performance**: <5ms overhead for non-bulk requests

## 📁 Project Structure

Following the [standard Go project layout](https://github.com/golang-standards/project-layout):

```
es-bulk-proxy/
├── cmd/
│   └── es-bulk-proxy/           # Main application entry point
│       └── main.go
├── internal/               # Private application code
│   ├── buffer/            # Bulk buffer aggregation logic
│   │   └── buffer.go
│   ├── config/            # Configuration with Viper
│   │   └── config.go
│   ├── handler/           # HTTP handlers and routing
│   │   └── handler.go
│   ├── logger/            # Structured logging with zerolog
│   │   └── logger.go
│   └── metrics/           # Prometheus metrics
│       └── metrics.go
├── configs/               # Configuration files
│   └── config.yaml       # Example configuration
├── deployments/          # Deployment configurations
│   ├── docker-compose.yml
│   ├── kubernetes.yaml
│   └── prometheus.yml
├── Dockerfile            # Multi-stage Docker build
├── Makefile             # Build and deployment commands
├── go.mod               # Go module dependencies
└── README.md
```

## 🏗️ Architecture

```
┌─────────────┐         ┌──────────────┐         ┌──────────────────┐
│             │         │              │         │                  │
│  Zenarmor   │───────▶ │   ES Proxy   │───────▶ │  Elasticsearch   │
│             │         │              │         │                  │
└─────────────┘         └──────────────┘         └──────────────────┘
                              │
                              │ Per-Index
                              │ Buffering
                              │
                        ┌─────▼──────┐
                        │ Buffer Mgr │
                        ├────────────┤
                        │index1/_bulk│
                        │index2/_bulk│
                        │index3/_bulk│
                        └────────────┘
```

**Key Components:**

- **Request Router**: Classifies requests by operation type (bulk, search, read, maintenance, write, delete)
- **Buffer Manager**: Maintains separate buffers for each index-specific bulk endpoint
- **Per-Index Buffers**: Each buffer aggregates requests for its specific index path
- **Flush Logic**: Time-based and size-based flushing per buffer
- **Metrics Collector**: Tracks requests by type and method for detailed visibility

## 🚀 Quick Start

### Prerequisites

- Go 1.25 or higher
- Docker & Docker Compose (for containerized deployment)
- Elasticsearch instance (for testing)

### Option 1: Docker Compose (Recommended)

The fastest way to get started with a complete stack:

```bash
cd deployments
docker-compose up -d
```

This starts:

- Elasticsearch on port 9200
- ES Proxy on port 8080
- Prometheus on port 9090
- Grafana on port 3001 (admin/admin) with pre-configured dashboard

**Access the Dashboard:**

- **Grafana Dashboard**: <http://localhost:3001/d/es-bulk-proxy-dashboard>
- Login with `admin` / `admin`

### Option 2: Build from Source

```bash
# Clone and navigate
cd es-bulk-proxy

# Download dependencies
go mod download

# Build
make build

# Run with environment variables
ES_URL=http://localhost:9200 ./es-bulk-proxy

# Or using go run
make run
```

### Option 3: Using Docker

```bash
# Build image
docker build -t es-bulk-proxy:latest .

# Run container
docker run -d \
  -p 8080:8080 \
  -e ES_URL=http://elasticsearch:9200 \
  --name es-bulk-proxy \
  es-bulk-proxy:latest
```

## ⚙️ Configuration

ES Proxy supports configuration through:

1. **Config file** (YAML) - `configs/config.yaml`
2. **Environment variables** - Override config file values
3. **Defaults** - Built-in sensible defaults

### Configuration File

Create a `configs/config.yaml`:

```yaml
server:
  port: "8080"

elasticsearch:
  url: "http://elasticsearch:9200"

buffer:
  flushinterval: "30s"
  maxbatchsize: 5242880    # 5MB
  maxbuffersize: 52428800  # 50MB

retry:
  attempts: 3
  backoffmin: "100ms"
```

### Environment Variables

All config values can be overridden with environment variables:

| Variable | Description | Default |
| --- | --- | --- |
| `PORT` | HTTP server port | `8080` |
| `ES_URL` | Elasticsearch endpoint URL | `http://localhost:9200` |
| `FLUSH_INTERVAL` | Time-based flush interval | `30s` |
| `MAX_BATCH_SIZE` | Size threshold for flushing (bytes) | `5242880` (5MB) |
| `MAX_BUFFER_SIZE` | Maximum buffer size (bytes) | `52428800` (50MB) |
| `RETRY_ATTEMPTS` | Number of retry attempts | `3` |
| `RETRY_BACKOFF_MIN` | Minimum backoff duration | `100ms` |
| `ENVIRONMENT` | Set to `development` for debug logs with pretty console output | (production - INFO level JSON logs) |

### Examples

```bash
# Production with custom settings
export ES_URL=https://elasticsearch:9200
export FLUSH_INTERVAL=30s
export MAX_BATCH_SIZE=10485760
export ENVIRONMENT=production
./es-bulk-proxy

# Development mode with pretty console logs
export ENVIRONMENT=development
./es-bulk-proxy
```

## 📡 API Endpoints

### Proxy Endpoints

#### `POST /_bulk`

Bulk write operations (aggregated)

```bash
curl -X POST http://localhost:8080/_bulk \
  -H "Content-Type: application/x-ndjson" \
  -d '{"index":{"_index":"myindex"}}
{"field1":"value1"}
'
```

Response:

```json
{"errors":false}
```

#### All Other Requests

All other Elasticsearch APIs are transparently proxied:

```bash
# Search
curl http://localhost:8080/_search

# Cluster health
curl http://localhost:8080/_cluster/health

# Index operations
curl -X PUT http://localhost:8080/myindex
```

### Health & Metrics

#### `GET /health`

Health check endpoint

```bash
curl http://localhost:8080/health
```

#### `GET /ready`

Readiness check endpoint

```bash
curl http://localhost:8080/ready
```

#### `GET /metrics`

Prometheus metrics endpoint

```bash
curl http://localhost:8080/metrics
```

**Available Metrics:**

- `es_proxy_requests_total{type, method}` - Total requests by operation type and HTTP method
  - `type="bulk"` - Bulk write operations (/_bulk endpoints)
  - `type="search"` - Search queries (POST to /_search, /_count)
  - `type="read"` - Read operations (GET, HEAD requests)
  - `type="maintenance"` - Index maintenance (/_refresh, /_flush, /_forcemerge)
  - `type="write"` - Single document writes
  - `type="delete"` - Delete operations
- `es_proxy_bulk_batches_total` - Number of bulk batches sent to Elasticsearch
- `es_proxy_bulk_failures_total` - Number of failed bulk sends
- `es_proxy_buffer_size_bytes` - Current buffer size in bytes
- `es_proxy_latency_seconds{type, method}` - Request latency histogram by operation type and method

## 🎯 Use with Zenarmor

To use this proxy with Zenarmor:

1. **Deploy ES Proxy** alongside your Elasticsearch cluster
2. **Configure Zenarmor** to use the proxy URL:

   ```
   Instead of: http://elasticsearch:9200
   Use:        http://es-bulk-proxy:8080
   ```

3. **Monitor Performance** via `/metrics` endpoint
4. **Tune Settings** based on your traffic patterns

### Expected Improvements

- **Reduced Load**: 80-90% fewer requests to Elasticsearch
- **Better Throughput**: Larger batches = better compression & indexing
- **Lower Latency**: Fewer round trips to ES cluster
- **Index-Aware Buffering**: Separate buffers per index maintain context and prevent cross-index conflicts
- **Cost Savings**: Reduced CPU/memory on ES nodes

## 🔧 Development

### Project Layout

- `/cmd` - Main applications for this project
- `/internal` - Private application and library code (not importable by external projects)
- `/configs` - Configuration file templates or default configs
- `/deployments` - Deployment configurations (Docker, Kubernetes, etc.)

### Build Commands

```bash
# Show all available commands
make help

# Download dependencies
make deps

# Build binary
make build

# Run locally
make run

# Run tests
make test

# Run linters
make lint

# Build Docker image
make docker-build

# Start full dev environment
make dev
```

### Running Tests

```bash
# Run unit tests
go test -v ./...

# Run integration tests
make integration-test

# Run with coverage
make test
```

## 🐳 Deployment

### Docker Compose

```bash
cd deployments
docker-compose up -d

# View logs
docker-compose logs -f es-bulk-proxy

# Stop
docker-compose down
```

### Kubernetes

```bash
# Deploy
kubectl apply -f deployments/kubernetes.yaml

# Check status
kubectl get pods -l app=es-bulk-proxy
kubectl get svc es-bulk-proxy

# View logs
kubectl logs -l app=es-bulk-proxy -f

# Port forward for testing
kubectl port-forward svc/es-bulk-proxy 8080:8080

# Scale
kubectl scale deployment es-bulk-proxy --replicas=5

# Delete
kubectl delete -f deployments/kubernetes.yaml
```

The Kubernetes deployment includes:

- Deployment with 2 replicas
- ClusterIP Service
- Horizontal Pod Autoscaler (2-10 pods)
- ConfigMap for configuration
- ServiceMonitor for Prometheus Operator
- PodDisruptionBudget for high availability

## 📊 Monitoring

### Grafana Dashboard

A pre-configured Grafana dashboard is included for comprehensive monitoring:

**Access:** <http://localhost:3001/d/es-bulk-proxy-dashboard> (admin/admin)

**Dashboard Features:**

- 📈 Real-time request rate by type and HTTP method (bulk, search, read, maintenance)
- 📊 Buffer size gauge with thresholds
- 🎯 Success rate and failure tracking
- ⏱️ Latency percentiles (p50, p95, p99) per operation type
- 📉 Bulk batch rate and trends
- 🥧 Request type distribution with method breakdown

**Quick Setup:**

```bash
# Dashboard is auto-provisioned with docker-compose
cd deployments && docker-compose up -d

# Generate test traffic
chmod +x generate-test-traffic.sh
./generate-test-traffic.sh
```

See [GRAFANA_DASHBOARD.md](deployments/GRAFANA_DASHBOARD.md) for detailed documentation.

### Prometheus

The service exposes Prometheus metrics at `/metrics`. Key metrics to monitor:

```promql
# Request rate by type
rate(es_proxy_requests_total[5m])

# Bulk write rate specifically
rate(es_proxy_requests_total{type="bulk"}[5m])

# Search query rate
rate(es_proxy_requests_total{type="search"}[5m])

# Bulk batch rate
rate(es_proxy_bulk_batches_total[5m])

# Error rate
rate(es_proxy_bulk_failures_total[5m])

# Buffer size
es_proxy_buffer_size_bytes

# Latency by operation type
histogram_quantile(0.95, rate(es_proxy_latency_seconds_bucket[5m]))
```

### Grafana

Import the provided dashboard or create custom dashboards using the metrics above.

**Pre-configured Dashboard:**

- Located at: `deployments/grafana-dashboard.json`
- Auto-provisioned when using docker-compose
- Access: <http://localhost:3001/d/es-bulk-proxy-dashboard>
- Includes 11 panels covering all key metrics

**Manual Import:**

1. Login to Grafana (admin/admin)
2. Go to Dashboards → Import
3. Upload `deployments/grafana-dashboard.json`
4. Select Prometheus datasource

## 🐛 Troubleshooting

### HTTP 429 - Too Many Requests

**Cause**: Buffer is full (exceeded MAX_BUFFER_SIZE)

**Solutions**:

```bash
# Increase buffer size
export MAX_BUFFER_SIZE=104857600  # 100MB

# Decrease flush interval
export FLUSH_INTERVAL=1s

# Scale horizontally
kubectl scale deployment es-bulk-proxy --replicas=5
```

### High Latency

**Cause**: Large batches or slow Elasticsearch

**Solutions**:

```bash
# Decrease batch size
export MAX_BATCH_SIZE=2621440  # 2.5MB

# Decrease flush interval
export FLUSH_INTERVAL=1s
```

### Failed Bulk Sends

**Cause**: Elasticsearch unreachable or rejecting requests

**Solutions**:

```bash
# Check metrics
curl http://localhost:8080/metrics | grep bulk_failures

# View logs
docker logs es-bulk-proxy 2>&1 | grep "failed to send bulk"

# Test ES connectivity
curl http://localhost:9200/_cluster/health
```

## 📈 Performance

### Benchmarks

Tested on 4-core CPU, 8GB RAM:

- **Throughput**: 1000+ bulk requests/sec
- **Latency**: <5ms overhead for proxy requests
- **Memory**: ~100MB under normal load
- **CPU**: <200m under normal load

### Tuning Tips

1. **Flush Interval**: Lower for faster writes, higher for better batching
2. **Batch Size**: Larger batches = better compression, but higher latency
3. **Scale Horizontally**: Use HPA for high-traffic scenarios
4. **Monitor Metrics**: Watch `buffer_size_bytes` for tuning

## 🛠️ Technology Stack

- **Language**: Go 1.25+
- **Logging**: [zerolog](https://github.com/rs/zerolog) - High-performance structured logging
- **Configuration**: [Viper](https://github.com/spf13/viper) - Flexible configuration management
- **Metrics**: [Prometheus](https://github.com/prometheus/client_golang) - Production monitoring
- **HTTP**: Go standard library - Reverse proxy and HTTP server

## 📝 License

See LICENSE file in repository root.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📞 Support

For issues and questions:

- GitHub Issues: [github.com/codifierr/go-scratchpad/issues](https://github.com/codifierr/go-scratchpad/issues)

---

**Built with ❤️ for optimizing Elasticsearch bulk operations**
