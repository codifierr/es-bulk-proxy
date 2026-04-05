#!/bin/bash

# Generate test traffic for ES Proxy to populate Grafana dashboard

echo "🚀 Generating test traffic for ES Proxy..."
echo ""

PROXY_URL="http://localhost:8080"
NUM_REQUESTS=50

echo "📊 Sending $NUM_REQUESTS bulk requests..."
for i in $(seq 1 $NUM_REQUESTS); do
    curl -s -X POST "$PROXY_URL/_bulk" \
        -H "Content-Type: application/x-ndjson" \
        -d "{\"index\":{\"_index\":\"test-index\"}}
{\"field\":\"value$i\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}
" > /dev/null

    if [ $((i % 10)) -eq 0 ]; then
        echo "  ✓ Sent $i bulk requests"
    fi
    sleep 0.1
done

echo ""
echo "🔍 Sending proxy requests (search, cluster health)..."
for i in $(seq 1 20); do
    curl -s "$PROXY_URL/_search" > /dev/null
    curl -s "$PROXY_URL/_cluster/health" > /dev/null

    if [ $((i % 5)) -eq 0 ]; then
        echo "  ✓ Sent $i proxy requests"
    fi
    sleep 0.2
done

echo ""
echo "📈 Current metrics:"
echo ""
curl -s "$PROXY_URL/metrics" | grep -E "^es_proxy_(requests_total|bulk_batches_total|bulk_failures_total|buffer_size_bytes)" | head -10

echo ""
echo "✅ Test traffic generated successfully!"
echo ""
echo "📊 View dashboard at: http://localhost:3001/d/es-bulk-proxy-dashboard"
echo "   Username: admin"
echo "   Password: admin"
echo ""
