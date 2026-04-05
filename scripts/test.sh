#!/bin/bash

set -e

echo "🚀 Starting ES Proxy Integration Tests"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Configuration
PROXY_URL="http://localhost:8080"
ES_URL="http://localhost:9200"

# Start services
echo "📦 Starting Docker Compose..."
cd deployments
docker-compose up -d
cd ..

# Wait for services to be healthy
echo "⏳ Waiting for services to be ready..."
sleep 10

# Function to check service health
check_health() {
    local url=$1
    local max_attempts=30
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if curl -f -s "$url" > /dev/null; then
            echo -e "${GREEN}✓${NC} Service at $url is healthy"
            return 0
        fi
        echo "Attempt $attempt/$max_attempts: Waiting for $url..."
        sleep 2
        ((attempt++))
    done

    echo -e "${RED}✗${NC} Service at $url failed to become healthy"
    return 1
}

# Check Elasticsearch
check_health "$ES_URL/_cluster/health"

# Check proxy
check_health "$PROXY_URL/health"

echo ""
echo "🧪 Running Tests"
echo "================"

# Test 1: Health endpoint
echo -n "Test 1: Health endpoint... "
if curl -f -s "$PROXY_URL/health" | grep -q "healthy"; then
    echo -e "${GREEN}✓ PASS${NC}"
else
    echo -e "${RED}✗ FAIL${NC}"
    exit 1
fi

# Test 2: Metrics endpoint
echo -n "Test 2: Metrics endpoint... "
if curl -f -s "$PROXY_URL/metrics" | grep -q "requests_total"; then
    echo -e "${GREEN}✓ PASS${NC}"
else
    echo -e "${RED}✗ FAIL${NC}"
    exit 1
fi

# Test 3: Bulk request
echo -n "Test 3: Bulk request... "
BULK_DATA='{"index":{"_index":"test-index"}}
{"field1":"value1","timestamp":"2024-01-01T00:00:00Z"}
{"index":{"_index":"test-index"}}
{"field2":"value2","timestamp":"2024-01-01T00:01:00Z"}
'
if curl -f -s -X POST "$PROXY_URL/_bulk" \
    -H "Content-Type: application/x-ndjson" \
    -d "$BULK_DATA" | grep -q '"errors":false'; then
    echo -e "${GREEN}✓ PASS${NC}"
else
    echo -e "${RED}✗ FAIL${NC}"
    exit 1
fi

# Wait for flush
echo "⏳ Waiting for buffer flush..."
sleep 4

# Test 4: Transparent proxy (search)
echo -n "Test 4: Transparent proxy search... "
if curl -f -s "$PROXY_URL/_search" > /dev/null; then
    echo -e "${GREEN}✓ PASS${NC}"
else
    echo -e "${RED}✗ FAIL${NC}"
    exit 1
fi

# Test 5: Verify data in Elasticsearch
echo -n "Test 5: Verify data in ES... "
sleep 2  # Give ES time to index
if curl -f -s "$ES_URL/test-index/_search" | grep -q "value1"; then
    echo -e "${GREEN}✓ PASS${NC}"
else
    echo -e "${RED}✗ FAIL${NC}"
    exit 1
fi

# Test 6: Cluster health proxy
echo -n "Test 6: Cluster health proxy... "
if curl -f -s "$PROXY_URL/_cluster/health" | grep -q "cluster_name"; then
    echo -e "${GREEN}✓ PASS${NC}"
else
    echo -e "${RED}✗ FAIL${NC}"
    exit 1
fi

# Test 7: Check metrics
echo -n "Test 7: Check metrics... "
METRICS=$(curl -s "$PROXY_URL/metrics")
if echo "$METRICS" | grep -q "requests_total{type=\"bulk\"}" && \
   echo "$METRICS" | grep -q "bulk_batches_total"; then
    echo -e "${GREEN}✓ PASS${NC}"
else
    echo -e "${RED}✗ FAIL${NC}"
    exit 1
fi

echo ""
echo "📊 Metrics Summary"
echo "=================="
curl -s "$PROXY_URL/metrics" | grep -E "(requests_total|bulk_batches_total|buffer_size_bytes)"

echo ""
echo -e "${GREEN}✅ All tests passed!${NC}"

# Cleanup
echo ""
echo "🧹 Cleaning up..."
cd deployments
docker-compose down -v
cd ..

echo -e "${GREEN}✨ Test suite completed successfully!${NC}"
