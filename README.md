# Elasticsearch Proxy with Bulk Aggregation

[![Go Version](https://img.shields.io/badge/Go-1.26.2+-00ADD8?style=flat&logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A Go service that acts as a transparent Elasticsearch proxy with intelligent bulk request aggregation. Optimizes Elasticsearch performance by batching small bulk requests while transparently proxying all other operations.

## ✨ Features

- **Smart Bulk Aggregation** with per-index buffers and automatic time/size-based flushing
- **Partial Failure Handling** - Retries only failed documents from bulk responses
- **Client Authentication Forwarding** - Supports Bearer, Basic, and API Key authentication
- **Transparent Proxying** - Non-bulk requests pass through unchanged
- **Intelligent Request Classification** - Tracks bulk, search, read, maintenance, write, and delete operations
- **Rich Prometheus Metrics** - Detailed metrics with operation type and HTTP method labels
- **Production Ready** - Health checks, graceful shutdown, backpressure handling, exponential retry
- **High Performance** - <5ms overhead for non-bulk requests

## 📁 Project Structure

Following the [standard Go project layout](https://github.com/golang-standards/project-layout):

```
es-bulk-proxy/
├── cmd/
│   └── main.go                 # Main application entry point
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

Get started in under 2 minutes with Docker Compose:

```bash
cd deployments
docker compose up -d
```

This starts a complete stack:

- **Elasticsearch** (port 9200)
- **ES Proxy** (port 8080)
- **Prometheus** (port 9090)  
- **Grafana** (port 3001, admin/admin) with pre-configured dashboard

**Access Dashboard:** <http://localhost:3001/d/es-bulk-proxy-dashboard>

**Other deployment options:** See [DEPLOYMENT.md](DEPLOYMENT.md) for Docker, Kubernetes, and building from source.

## ⚙️ Configuration

Configure via environment variables or YAML config file. **Essential settings:**

| Variable | Description | Default |
|----------|-------------|---------|
| `ES_URL` | Elasticsearch endpoint | `http://localhost:9200` |
| `FLUSH_INTERVAL` | Time-based flush interval | `30s` |
| `MAX_BATCH_SIZE` | Size threshold for flushing | `5242880` (5MB) |
| `MAX_BUFFER_SIZE` | Maximum buffer size | `52428800` (50MB) |
| `ENVIRONMENT` | Set to `development` for debug logs | `production` |

**Example:**

```bash
export ES_URL=https://elasticsearch:9200
export FLUSH_INTERVAL=30s
export MAX_BATCH_SIZE=10485760
./es-bulk-proxy
```

**Full configuration reference:** See [CONFIGURATION.md](CONFIGURATION.md) for all settings, timeout tuning, and troubleshooting.

## 📡 API

### Bulk Aggregation

`POST /_bulk` requests are automatically buffered and batched:

```bash
curl -X POST http://localhost:8080/_bulk \
  -H "Content-Type: application/x-ndjson" \
  -d '{"index":{"_index":"myindex"}}
{"field1":"value1"}'
```

### Transparent Proxying

All other Elasticsearch APIs pass through unchanged:

```bash
curl http://localhost:8080/_search
curl http://localhost:8080/_cluster/health
curl -X PUT http://localhost:8080/myindex
```

### Authentication

The proxy forwards client authentication headers (Bearer tokens, Basic auth, API keys) to Elasticsearch. Auth from the first request in each batch is used when flushing.

**Details:** See [AUTHENTICATION.md](AUTHENTICATION.md)

### Health & Metrics

- `GET /health` - Health check
- `GET /ready` - Readiness check  
- `GET /metrics` - Prometheus metrics

## 🎯 Use with Zenarmor

Replace your Elasticsearch URL with the proxy:

```
Instead of: http://elasticsearch:9200
Use:        http://es-bulk-proxy:8080
```

**Expected improvements:**

- 80-90% fewer requests to Elasticsearch
- Better throughput via larger batches
- Lower latency (fewer round trips)
- Reduced CPU/memory on ES nodes

## 🔧 Development

### Quick Commands

```bash
make dev              # Start full dev environment
make build            # Build binary
make test             # Run tests
make lint             # Run linters
make format           # Format code
make integration-test # Run integration tests (Docker required)
```

### Project Structure

Follows [Standard Go Project Layout](https://github.com/golang-standards/project-layout):

- `/cmd` - Application entry points
- `/internal` - Private application code (buffer, config, handler, logger, metrics)
- `/configs` - Configuration files
- `/deployments` - Docker, Kubernetes, monitoring configs

**Code Quality:** golangci-lint, pre-commit hooks, gofmt, go vet

## Monitoring

### Key Metrics

```promql
# Request rate by type
rate(es_proxy_requests_total{type="bulk"}[5m])

# Buffer usage
es_proxy_buffer_size_bytes

# Error rate
rate(es_proxy_bulk_failures_total[5m])

# Latency by operation
histogram_quantile(0.95, rate(es_proxy_latency_seconds_bucket[5m]))
```

### Grafana Dashboard

Pre-configured dashboard with 11 panels covering all key metrics:

- Request rates by type and method
- Buffer size with thresholds
- Success/failure tracking
- Latency percentiles (p50, p95, p99)
- Request distribution

**Import:** `deployments/grafana-dashboard.json` or access at <http://localhost:3001/d/es-bulk-proxy-dashboard>

**Details:** See [GRAFANA_DASHBOARD.md](deployments/GRAFANA_DASHBOARD.md)

## 🐛 Troubleshooting

| Issue | Quick Fix |
|-------|-----------|
| HTTP 429 | Increase `MAX_BUFFER_SIZE` or decrease `FLUSH_INTERVAL` |
| Timeout errors | Increase `SERVER_READ_TIMEOUT` and `ES_REQUEST_TIMEOUT` |
| High latency | Decrease `MAX_BATCH_SIZE` or `FLUSH_INTERVAL` |
| Failed bulk sends | Check ES connectivity and logs |

**Full guide:** [CONFIGURATION.md](CONFIGURATION.md)

## 📈 Performance

**Benchmarks** (4-core CPU, 8GB RAM): 1000+ req/sec, <5ms overhead, ~100MB memory, <200m CPU

**Tuning:** Lower flush interval = faster writes | Larger batches = better compression | Scale horizontally for high traffic

## 🛠️ Technology Stack

Go 1.25+ • [zerolog](https://github.com/rs/zerolog) • [Viper](https://github.com/spf13/viper) • [Prometheus](https://github.com/prometheus/client_golang) • Go stdlib

## 📚 Documentation

- [DEPLOYMENT.md](DEPLOYMENT.md) - Docker, Kubernetes, source builds
- [CONFIGURATION.md](CONFIGURATION.md) - Full config reference & tuning guide
- [AUTHENTICATION.md](AUTHENTICATION.md) - Auth forwarding details
- [GRAFANA_DASHBOARD.md](deployments/GRAFANA_DASHBOARD.md) - Dashboard guide

## 📝 License

See LICENSE file in repository root.

## 🤝 Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details on:

- Development setup and workflow
- Code standards and best practices
- Testing requirements
- Submitting pull requests

Please feel free to submit issues and pull requests.

## 📞 Support

For issues and questions:

- GitHub Issues: [https://github.com/codifierr/es-bulk-proxy/issues](https://github.com/codifierr/es-bulk-proxy/issues)

---

**Built with ❤️ for optimizing Elasticsearch bulk operations**
