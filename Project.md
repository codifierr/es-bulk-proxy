Build a production-ready Go service that acts as a transparent Elasticsearch proxy with smart bulk aggregation.

## Context

Zenarmor sends frequent small `_bulk` requests to Elasticsearch and also performs read queries (`_search`, `_cluster/health`, etc.). I need a proxy that:

* Aggregates `_bulk` write requests
* Transparently forwards all other requests

The service must be fully compatible with Elasticsearch APIs so that Zenarmor works without modification.

---

## Core Behavior

### 1. Bulk Write Handling (SPECIAL CASE)

For:
POST /_bulk

* Accept NDJSON (application/x-ndjson)

* Buffer requests in memory (RAM only, no disk)

* Merge multiple `_bulk` requests

* Flush based on:

  * Time (default: 3s)
  * OR size (default: 5MB)

* Send merged request to upstream Elasticsearch `_bulk`

* Immediately respond to client with:
  `{ "errors": false }`

(Optional advanced mode: map real ES responses back to individual requests)

---

### 2. Read & Other Requests (TRANSPARENT PROXY)

For ALL other routes:

* GET /_search
* GET /_cluster/health
* GET /index/_search
* PUT /index
* DELETE /index
* etc.

The service must:

* Forward request as-is to Elasticsearch
* Preserve:

  * method
  * headers
  * query params
  * body
* Return response unchanged

---

## Requirements

### Proxy Layer

* Implement full HTTP reverse proxy
* Use Go standard library (`httputil.ReverseProxy`) or equivalent
* Route:

  * `/ _bulk` → custom handler
  * everything else → proxy

---

### Concurrency & Buffering

* Thread-safe buffer using mutex or channels
* Support concurrent writes
* No data races

---

### Backpressure

* Configurable max buffer size (e.g., 50MB)
* If exceeded:

  * return HTTP 429 OR block

---

### Reliability

* Retry failed bulk sends with exponential backoff
* Do not crash on Elasticsearch failure

---

### Observability

Expose `/metrics` (Prometheus):

* requests_total
* bulk_batches_total
* bulk_failures_total
* buffer_size_bytes

---

### Logging

* Structured JSON logs
* Include:

  * request type (bulk/read)
  * batch size
  * errors

---

## Configuration (ENV)

* ES_URL (e.g., <http://elasticsearch:9200>)
* FLUSH_INTERVAL (default: 30s)
* MAX_BATCH_SIZE (default: 5MB)
* MAX_BUFFER_SIZE (default: 50MB)
* PORT (default: 8080)

---

## Docker

* Multi-stage Dockerfile
* Minimal image (distroless/alpine)

---

## docker-compose

* Service: bulk-aggregator
* Expose 8080
* Configurable ES_URL
* Resource limits

---

## Kubernetes

Provide:

* Deployment
* Service (ClusterIP)
* Readiness & liveness probes
* Optional HPA

---

## Performance Targets

* 1000+ req/sec
* <5ms overhead for non-bulk requests
* Efficient memory usage

---

## Output

Provide:

1. Full Go source code
2. Dockerfile
3. docker-compose.yml
4. Kubernetes manifests
5. README

---
