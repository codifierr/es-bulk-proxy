# Grafana Dashboard for ES Proxy

This directory contains a pre-configured Grafana dashboard to monitor the Elasticsearch Proxy application.

## 📊 Dashboard Features

The dashboard includes the following panels:

### Key Metrics

1. **Request Rate by Type** - Real-time requests per second by request type
2. **Current Buffer Size** - Gauge showing buffer usage with thresholds
3. **Total Requests** - Counter of all requests processed
4. **Bulk Batches Sent** - Number of batches sent to Elasticsearch
5. **Bulk Failures** - Count of failed batch operations
6. **Success Rate** - Percentage of successful bulk operations

### Detailed Visualizations

1. **Bulk Batch Rate** - Batches and failures per second over time
2. **Buffer Size Over Time** - Historical buffer usage with warning thresholds
3. **Request Latency Percentiles** - p50, p95, p99 latency by request type and method
4. **Request Type Distribution** - Pie chart showing traffic by request type and method
5. **Buffer Usage By Index** - Buffer usage trend per index path

## 🚀 Quick Start

### Automatic Setup (Docker Compose)

The dashboard is automatically provisioned when you run:

```bash
cd deployments
docker compose up -d
```

Then access Grafana at: **<http://localhost:3001>**

**Default credentials:**

- Username: `admin`
- Password: `admin`

The dashboard will be automatically available at:
<http://localhost:3001/d/es-bulk-proxy-dashboard>

### Manual Import

If you need to manually import the dashboard:

1. Open Grafana: <http://localhost:3001>
2. Login with admin/admin
3. Navigate to **Dashboards** → **Import**
4. Click **Upload JSON file** or paste the contents of `grafana-dashboard.json`
5. Select the **Prometheus** datasource
6. Click **Import**

## 📁 Dashboard Files

- **`grafana-dashboard.json`** - Complete dashboard configuration
- **`grafana-datasource.yml`** - Prometheus datasource provisioning
- **`grafana-dashboards.yml`** - Dashboard provisioning configuration

## 🔍 Understanding the Metrics

### Buffer Thresholds

- **Green**: 0 - 25 MB (0 - 50% of max)
- **Yellow**: 25 - 40 MB (50 - 80% of max)
- **Red**: 40 - 50 MB (80 - 100% of max)

### Success Rate

- **Green**: ≥ 99% success rate
- **Yellow**: 95 - 99% success rate
- **Red**: < 95% success rate

### What to Monitor

**High Priority:**

- 🔴 **Bulk Failures** - Should be 0 or near 0
- 🟡 **Success Rate** - Should be ≥ 99%
- 🟡 **Buffer Size** - Should not stay in red zone

**Performance:**

- **Request Rate** - Shows traffic patterns
- **Latency p99** - Should be < 50ms for optimal performance
- **Batch Rate** - Indicates batching efficiency

**Capacity:**

- **Buffer Size** - Monitor for capacity planning
- **Request Distribution** - Understand traffic mix by request type

## 🎨 Customization

### Modify Refresh Rate

The dashboard auto-refreshes every 5 seconds. To change:

1. Click the refresh dropdown (top right)
2. Select your preferred interval

### Adjust Time Range

Default is last 15 minutes. Use the time picker (top right) to adjust.

### Add Custom Panels

1. Click **Add panel** (top right)
2. Choose **Add a new panel**
3. Select Prometheus datasource
4. Use available metrics:
   - `es_proxy_requests_total{type, method}`
   - `es_proxy_bulk_batches_total`
   - `es_proxy_bulk_failures_total`
   - `es_proxy_bulk_requeues_total{index_path}`
   - `es_proxy_buffer_size_bytes{index_path}`
   - `es_proxy_buffer_in_flight_bytes{index_path}`
   - `es_proxy_buffer_in_flight_requests{index_path}`
   - `es_proxy_latency_seconds`

## 🔗 Dashboard Links

After starting with `docker compose up -d`:

- **Grafana**: <http://localhost:3001>
- **Dashboard**: <http://localhost:3001/d/es-bulk-proxy-dashboard>
- **Prometheus**: <http://localhost:9090>
- **ES Proxy Metrics**: <http://localhost:8080/metrics>

## 🐛 Troubleshooting

### Dashboard Not Showing Data

1. Check Prometheus is scraping metrics:

   ```bash
   curl http://localhost:9090/api/v1/targets
   ```

2. Verify ES Proxy metrics endpoint:

   ```bash
   curl http://localhost:8080/metrics | grep es_proxy
   ```

3. Check Grafana datasource:
   - Go to **Configuration** → **Data sources**
   - Click **Prometheus**
   - Click **Test** button

### Panels Show "No Data"

- Ensure ES Proxy is running and receiving traffic
- Generate some test traffic:

  ```bash
  # Send bulk requests
  curl -X POST http://localhost:8080/_bulk \
    -H "Content-Type: application/x-ndjson" \
    -d '{"index":{"_index":"test"}}
  {"field":"value"}
  '

  # Send proxy requests
  curl http://localhost:8080/_search
  ```

### Grafana Won't Start

Check port conflict:

```bash
# Check what's using port 3001
lsof -i :3001

# Or modify docker-compose.yml to use different port
# Change "3001:3000" to another port like "3002:3000"
```

## 📊 Example Queries

### Custom Prometheus Queries

You can create custom panels using these queries:

**Request rate (last 5m):**

```promql
rate(es_proxy_requests_total[5m])
```

**Error rate:**

```promql
rate(es_proxy_bulk_failures_total[1m])
```

**Requeue rate:**

```promql
rate(es_proxy_bulk_requeues_total[1m])
```

**Average latency:**

```promql
rate(es_proxy_latency_seconds_sum[1m]) / rate(es_proxy_latency_seconds_count[1m])
```

**Requests by type (total):**

```promql
sum by(type) (es_proxy_requests_total)
```

**Total buffer utilization percentage:**

```promql
(sum(es_proxy_buffer_size_bytes) / 52428800) * 100
```

**In-flight buffer bytes:**

```promql
sum(es_proxy_buffer_in_flight_bytes)
```

## 📝 Notes

- Dashboard auto-refreshes every 5 seconds
- Default time range is last 15 minutes
- All panels support drill-down (click to zoom)
- Dashboard can be exported and shared via JSON
- Supports Grafana alerting (configure in Alert rules)

## 🔔 Setting Up Alerts (Optional)

Create alerts for critical metrics:

1. **High Failure Rate:**
   - Metric: `es_proxy_bulk_failures_total`
   - Condition: Rate > 0 for 5 minutes

2. **Buffer Nearly Full:**
   - Metric: `sum(es_proxy_buffer_size_bytes)`
   - Condition: > 45 MB (90% of max)

3. **Excessive Requeues:**
   - Metric: `rate(es_proxy_bulk_requeues_total[5m])`
   - Condition: > 0 for 5 minutes

4. **High Latency:**
   - Metric: `histogram_quantile(0.99, rate(es_proxy_latency_seconds_bucket[1m]))`
   - Condition: > 0.1 (100ms)

To configure alerts:

1. Open a panel
2. Click **Alert** tab
3. Click **Create alert rule from this panel**
4. Configure conditions and notifications

---

**Dashboard Version:** 1.0.0
**Last Updated:** April 2026
**Compatible with:** Grafana 10.0+, Prometheus 2.0+
