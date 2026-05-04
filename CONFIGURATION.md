# Configuration Guide

ES Bulk Proxy supports flexible configuration through multiple sources with the following precedence (highest to lowest):

1. **Environment variables** - Override all other sources
2. **Config file** (YAML) - Located at `configs/config.yaml`
3. **Defaults** - Built-in sensible defaults

## Configuration File

Create a `configs/config.yaml` file:

```yaml
server:
  port: "8080"
  
  # HTTP server timeouts
  readtimeout: "30s"       # Timeout for reading entire request including body
  writetimeout: "30s"      # Timeout for writing response
  idletimeout: "2m"        # Keep-alive timeout

elasticsearch:
  url: "http://elasticsearch:9200"
  
  # Timeout for individual HTTP requests to Elasticsearch
  # Increase for large bulk payloads or slow ES clusters
  requesttimeout: "30s"

buffer:
  flushinterval: "30s"
  maxbatchsize: 5242880    # 5MB
  maxbuffersize: 52428800  # 50MB

retry:
  attempts: 3
  backoffmin: "100ms"
```

## Environment Variables

All configuration values can be overridden using environment variables:

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `PORT` | HTTP server port | `8080` | `8080` |
| `SERVER_READ_TIMEOUT` | Server read timeout (increase for large bulk requests) | `30s` | `5m` |
| `SERVER_WRITE_TIMEOUT` | Server write timeout | `30s` | `30s` |
| `SERVER_IDLE_TIMEOUT` | Keep-alive timeout | `2m` | `2m` |
| `ES_URL` | Elasticsearch endpoint URL | `http://localhost:9200` | `https://es.example.com:9200` |
| `ES_REQUEST_TIMEOUT` | Timeout for ES requests (increase for large payloads) | `30s` | `2m` |
| `FLUSH_INTERVAL` | Time-based flush interval | `30s` | `10s` |
| `MAX_BATCH_SIZE` | Size threshold for flushing (bytes) | `5242880` (5MB) | `10485760` (10MB) |
| `MAX_BUFFER_SIZE` | Maximum buffer size (bytes) | `52428800` (50MB) | `104857600` (100MB) |
| `RETRY_ATTEMPTS` | Number of retry attempts | `3` | `5` |
| `RETRY_BACKOFF_MIN` | Minimum backoff duration | `100ms` | `500ms` |
| `ENVIRONMENT` | Set to `development` for debug logs | (production) | `development` |

## Configuration Examples

### Production Deployment

```bash
export ES_URL=https://elasticsearch.prod.example.com:9200
export FLUSH_INTERVAL=30s
export MAX_BATCH_SIZE=10485760      # 10MB
export MAX_BUFFER_SIZE=104857600    # 100MB
export RETRY_ATTEMPTS=5
export ENVIRONMENT=production
./es-bulk-proxy
```

### Development Mode

```bash
export ENVIRONMENT=development      # Pretty console logs with DEBUG level
export ES_URL=http://localhost:9200
export FLUSH_INTERVAL=5s           # Faster flushing for testing
./es-bulk-proxy
```

### High-Volume Scenario

```bash
export FLUSH_INTERVAL=10s           # Frequent flushes
export MAX_BATCH_SIZE=20971520      # 20MB batches
export MAX_BUFFER_SIZE=209715200    # 200MB buffer
export ES_REQUEST_TIMEOUT=5m        # Longer timeout for large payloads
export SERVER_READ_TIMEOUT=5m       # Handle slow clients
./es-bulk-proxy
```

### Low-Latency Scenario

```bash
export FLUSH_INTERVAL=1s            # Minimal buffering time
export MAX_BATCH_SIZE=1048576       # 1MB batches
export ES_REQUEST_TIMEOUT=10s       # Quick timeouts
./es-bulk-proxy
```

## Configuration Tuning Guide

### Flush Interval (`FLUSH_INTERVAL`)

- **Lower values (1-5s)**: Faster data visibility, lower latency, but less batching efficiency
- **Higher values (30-60s)**: Better batching, higher throughput, but higher latency
- **Recommended**: Start with `30s` and adjust based on your latency requirements

### Batch Size (`MAX_BATCH_SIZE`)

- **Smaller batches (1-5MB)**: Lower memory usage, faster individual requests
- **Larger batches (10-20MB)**: Better compression, more efficient indexing
- **Recommended**: Start with `5MB` and increase if ES can handle it

### Buffer Size (`MAX_BUFFER_SIZE`)

- **Should be 10x your batch size** minimum
- **Too small**: Frequent 429 responses under load
- **Too large**: High memory usage
- **Recommended**: `50MB` for 5MB batches, `100MB` for 10MB batches

### Timeout Configuration

#### Server Read Timeout (`SERVER_READ_TIMEOUT`)

- Controls how long the proxy waits for the client to send the complete request
- **Increase if**: Clients are slow or sending large payloads over slow networks
- **Symptoms**: `context deadline exceeded` errors from client requests
- **Recommended**: `30s` for fast networks, `5m` for slow clients or large payloads

#### Server Write Timeout (`SERVER_WRITE_TIMEOUT`)

- Controls how long the proxy waits to write the complete response to the client
- **Increase if**: Clients are slow to receive responses
- **Recommended**: `30s` is usually sufficient

#### Elasticsearch Request Timeout (`ES_REQUEST_TIMEOUT`)

- Controls how long the proxy waits for Elasticsearch to process bulk requests
- **Increase if**: ES takes long to process large batches or cluster is under load
- **Symptoms**: `context deadline exceeded` in proxy logs when sending to ES
- **Recommended**: `30s` for small batches, `2-5m` for large batches (10MB+)

### Retry Configuration

#### Retry Attempts (`RETRY_ATTEMPTS`)

- Number of times to retry failed bulk requests
- **Lower (1-3)**: Fail faster, less retry overhead
- **Higher (5-10)**: More resilient to transient failures
- **Recommended**: `3` for stable clusters, `5` for clusters with occasional issues

#### Retry Backoff (`RETRY_BACKOFF_MIN`)

- Initial backoff duration (doubles with each retry)
- **Pattern**: 100ms → 200ms → 400ms → 800ms...
- **Recommended**: `100ms` is usually good

## Monitoring Configuration

To verify your configuration is working:

```bash
# Check buffer size metrics
curl http://localhost:8080/metrics | grep buffer_size_bytes

# Check if requests are being buffered
curl http://localhost:8080/metrics | grep bulk_batches_total

# Check for backpressure (429 responses)
curl http://localhost:8080/metrics | grep requests_total | grep 429
```

## Troubleshooting Configuration Issues

### Issue: HTTP 429 (Too Many Requests)

**Cause**: `MAX_BUFFER_SIZE` exceeded

**Solution**:
```bash
# Increase buffer size
export MAX_BUFFER_SIZE=104857600  # 100MB

# OR decrease flush interval for faster draining
export FLUSH_INTERVAL=10s
```

### Issue: Context Deadline Exceeded

**Cause**: Timeouts too short for workload

**Solution**:
```bash
# For client-side timeouts
export SERVER_READ_TIMEOUT=5m

# For ES-side timeouts
export ES_REQUEST_TIMEOUT=5m
```

### Issue: High Memory Usage

**Cause**: Buffer sizes too large or not flushing fast enough

**Solution**:
```bash
# Reduce buffer size
export MAX_BUFFER_SIZE=26214400  # 25MB

# Reduce batch size
export MAX_BATCH_SIZE=2621440    # 2.5MB

# Flush more frequently
export FLUSH_INTERVAL=10s
```

### Issue: Poor Batching Efficiency

**Cause**: Flushing too frequently or batch size too small

**Solution**:
```bash
# Increase flush interval
export FLUSH_INTERVAL=60s

# Increase batch size
export MAX_BATCH_SIZE=10485760   # 10MB
```

## Docker Compose Configuration

When using Docker Compose, add environment variables to `docker-compose.yml`:

```yaml
services:
  es-bulk-proxy:
    image: ssingh3339/es-bulk-proxy:latest
    environment:
      - ES_URL=http://elasticsearch:9200
      - FLUSH_INTERVAL=30s
      - MAX_BATCH_SIZE=10485760
      - MAX_BUFFER_SIZE=104857600
    ports:
      - "8080:8080"
```

## Kubernetes Configuration

Use a ConfigMap for configuration:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: es-bulk-proxy-config
data:
  ES_URL: "http://elasticsearch:9200"
  FLUSH_INTERVAL: "30s"
  MAX_BATCH_SIZE: "10485760"
  MAX_BUFFER_SIZE: "104857600"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: es-bulk-proxy
spec:
  template:
    spec:
      containers:
      - name: es-bulk-proxy
        image: ssingh3339/es-bulk-proxy:latest
        envFrom:
        - configMapRef:
            name: es-bulk-proxy-config
```

## Best Practices

1. **Start with defaults** and tune based on metrics
2. **Monitor buffer_size_bytes** to prevent 429 errors
3. **Set ES_REQUEST_TIMEOUT** > time needed for largest batch
4. **Scale horizontally** before increasing buffer sizes too much
5. **Use development mode** only in non-production environments
6. **Test configuration changes** under realistic load
