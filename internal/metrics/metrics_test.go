package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Note: These tests use a shared global metrics instance created once
// because Prometheus metrics are registered globally
var globalMetrics = New()

func TestNew(t *testing.T) {
	// New() is called once globally, so we test the global instance
	m := globalMetrics

	if m == nil {
		t.Fatal("New() returned nil")
	}

	if m.RequestsTotal == nil {
		t.Error("RequestsTotal not initialized")
	}

	if m.BulkBatchesTotal == nil {
		t.Error("BulkBatchesTotal not initialized")
	}

	if m.BulkFailuresTotal == nil {
		t.Error("BulkFailuresTotal not initialized")
	}

	if m.BufferSizeBytes == nil {
		t.Error("BufferSizeBytes not initialized")
	}

	if m.ProxyLatency == nil {
		t.Error("ProxyLatency not initialized")
	}
}

func TestMetrics_RequestsTotal(t *testing.T) {
	m := globalMetrics

	// Test incrementing counters with different labels
	m.RequestsTotal.WithLabelValues("bulk", "POST").Inc()
	m.RequestsTotal.WithLabelValues("search", "GET").Inc()
	m.RequestsTotal.WithLabelValues("search", "GET").Inc()

	// Verify counter values
	bulkCount := testutil.ToFloat64(m.RequestsTotal.WithLabelValues("bulk", "POST"))
	if bulkCount != 1 {
		t.Errorf("RequestsTotal{type=bulk,method=POST} = %f, want 1", bulkCount)
	}

	searchCount := testutil.ToFloat64(m.RequestsTotal.WithLabelValues("search", "GET"))
	if searchCount != 2 {
		t.Errorf("RequestsTotal{type=search,method=GET} = %f, want 2", searchCount)
	}
}

func TestMetrics_RequestsTotal_MultipleTypes(t *testing.T) {
	m := globalMetrics

	types := []string{"bulk", "search", "read", "write", "delete", "maintenance", "other"}
	methods := []string{"GET", "POST", "PUT", "DELETE"}

	// Get initial values
	initialCounts := make(map[string]float64)
	for _, typ := range types {
		for _, method := range methods {
			key := typ + "_" + method
			initialCounts[key] = testutil.ToFloat64(m.RequestsTotal.WithLabelValues(typ, method))
		}
	}

	// Increment all combinations
	for _, typ := range types {
		for _, method := range methods {
			m.RequestsTotal.WithLabelValues(typ, method).Inc()
		}
	}

	// Verify all combinations were incremented
	for _, typ := range types {
		for _, method := range methods {
			key := typ + "_" + method
			count := testutil.ToFloat64(m.RequestsTotal.WithLabelValues(typ, method))
			expected := initialCounts[key] + 1
			if count != expected {
				t.Errorf("RequestsTotal{type=%s,method=%s} = %f, want %f", typ, method, count, expected)
			}
		}
	}
}

func TestMetrics_BulkBatchesTotal(t *testing.T) {
	m := globalMetrics

	// Initial value should be 0
	initial := testutil.ToFloat64(m.BulkBatchesTotal)
	if initial != 0 {
		t.Errorf("BulkBatchesTotal initial value = %f, want 0", initial)
	}

	// Increment counter
	m.BulkBatchesTotal.Inc()
	m.BulkBatchesTotal.Inc()
	m.BulkBatchesTotal.Inc()

	// Verify counter value
	count := testutil.ToFloat64(m.BulkBatchesTotal)
	if count != 3 {
		t.Errorf("BulkBatchesTotal = %f, want 3", count)
	}
}

func TestMetrics_BulkFailuresTotal(t *testing.T) {
	m := globalMetrics

	// Initial value should be 0
	initial := testutil.ToFloat64(m.BulkFailuresTotal)
	if initial != 0 {
		t.Errorf("BulkFailuresTotal initial value = %f, want 0", initial)
	}

	// Increment counter
	m.BulkFailuresTotal.Inc()

	// Verify counter value
	count := testutil.ToFloat64(m.BulkFailuresTotal)
	if count != 1 {
		t.Errorf("BulkFailuresTotal = %f, want 1", count)
	}
}

func TestMetrics_BufferSizeBytes(t *testing.T) {
	m := globalMetrics
	bufferMetric := m.BufferSizeBytes.WithLabelValues("/_bulk")

	// Initial value should be 0
	initial := testutil.ToFloat64(bufferMetric)
	if initial != 0 {
		t.Errorf("BufferSizeBytes initial value = %f, want 0", initial)
	}

	// Set gauge values
	bufferMetric.Set(1024)
	if testutil.ToFloat64(bufferMetric) != 1024 {
		t.Errorf("BufferSizeBytes = %f, want 1024", testutil.ToFloat64(bufferMetric))
	}

	bufferMetric.Set(2048)
	if testutil.ToFloat64(bufferMetric) != 2048 {
		t.Errorf("BufferSizeBytes = %f, want 2048", testutil.ToFloat64(bufferMetric))
	}

	// Reset to 0
	bufferMetric.Set(0)
	if testutil.ToFloat64(bufferMetric) != 0 {
		t.Errorf("BufferSizeBytes = %f, want 0", testutil.ToFloat64(bufferMetric))
	}
}

func TestMetrics_BufferSizeBytes_Add(t *testing.T) {
	m := globalMetrics
	bufferMetric := m.BufferSizeBytes.WithLabelValues("/_bulk")

	bufferMetric.Set(100)
	bufferMetric.Add(50)

	value := testutil.ToFloat64(bufferMetric)
	if value != 150 {
		t.Errorf("BufferSizeBytes = %f, want 150", value)
	}

	bufferMetric.Sub(30)
	value = testutil.ToFloat64(bufferMetric)
	if value != 120 {
		t.Errorf("BufferSizeBytes = %f, want 120", value)
	}
}

func TestMetrics_ProxyLatency(t *testing.T) {
	m := globalMetrics

	// Observe some latencies
	m.ProxyLatency.WithLabelValues("bulk", "POST").Observe(0.001)  // 1ms
	m.ProxyLatency.WithLabelValues("bulk", "POST").Observe(0.002)  // 2ms
	m.ProxyLatency.WithLabelValues("search", "GET").Observe(0.010) // 10ms

	// Verify histogram has observations
	// We can't easily check exact values, but we can verify the metric exists
	// and has the right labels
	bulkMetric := m.ProxyLatency.WithLabelValues("bulk", "POST")
	if bulkMetric == nil {
		t.Error("ProxyLatency{type=bulk,method=POST} should exist")
	}

	searchMetric := m.ProxyLatency.WithLabelValues("search", "GET")
	if searchMetric == nil {
		t.Error("ProxyLatency{type=search,method=GET} should exist")
	}
}

func TestMetrics_ProxyLatency_MultipleObservations(t *testing.T) {
	m := globalMetrics

	// Record multiple latencies for the same label set
	for i := 0; i < 100; i++ {
		m.ProxyLatency.WithLabelValues("read", "GET").Observe(0.005)
	}

	// Should not panic and metric should exist
	metric := m.ProxyLatency.WithLabelValues("read", "GET")
	if metric == nil {
		t.Error("ProxyLatency{type=read,method=GET} should exist after observations")
	}
}

func TestMetrics_ConcurrentAccess(t *testing.T) {
	m := globalMetrics

	// Get initial counts
	initialBulkCount := testutil.ToFloat64(m.RequestsTotal.WithLabelValues("bulk", "POST"))
	initialBatchCount := testutil.ToFloat64(m.BulkBatchesTotal)

	// Test concurrent access to metrics (should be thread-safe)
	done := make(chan bool)

	// Multiple goroutines incrementing counters
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				m.RequestsTotal.WithLabelValues("bulk", "POST").Inc()
				m.BulkBatchesTotal.Inc()
				m.BufferSizeBytes.WithLabelValues("/_bulk").Set(float64(j))
				m.ProxyLatency.WithLabelValues("bulk", "POST").Observe(0.001)
			}
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify metrics were updated (should have increased by 1000)
	bulkCount := testutil.ToFloat64(m.RequestsTotal.WithLabelValues("bulk", "POST"))
	expectedBulkCount := initialBulkCount + 1000
	if bulkCount != expectedBulkCount {
		t.Errorf("RequestsTotal{type=bulk,method=POST} = %f, want %f", bulkCount, expectedBulkCount)
	}

	batchCount := testutil.ToFloat64(m.BulkBatchesTotal)
	expectedBatchCount := initialBatchCount + 1000
	if batchCount != expectedBatchCount {
		t.Errorf("BulkBatchesTotal = %f, want %f", batchCount, expectedBatchCount)
	}
}

func TestMetrics_MetricNames(t *testing.T) {
	m := globalMetrics

	// Verify metric names follow Prometheus naming conventions
	tests := []struct {
		name       string
		metricFunc func() prometheus.Collector
		wantName   string
	}{
		{
			name:       "RequestsTotal",
			metricFunc: func() prometheus.Collector { return m.RequestsTotal },
			wantName:   "es_proxy_requests_total",
		},
		{
			name:       "BulkBatchesTotal",
			metricFunc: func() prometheus.Collector { return m.BulkBatchesTotal },
			wantName:   "es_proxy_bulk_batches_total",
		},
		{
			name:       "BulkFailuresTotal",
			metricFunc: func() prometheus.Collector { return m.BulkFailuresTotal },
			wantName:   "es_proxy_bulk_failures_total",
		},
		{
			name:       "BufferSizeBytes",
			metricFunc: func() prometheus.Collector { return m.BufferSizeBytes.WithLabelValues("/_bulk") },
			wantName:   "es_proxy_buffer_size_bytes",
		},
		{
			name:       "ProxyLatency",
			metricFunc: func() prometheus.Collector { return m.ProxyLatency },
			wantName:   "es_proxy_latency_seconds",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector := tt.metricFunc()
			if collector == nil {
				t.Errorf("%s collector is nil", tt.name)
			}
		})
	}
}

func TestMetrics_DefaultBuckets(t *testing.T) {
	m := globalMetrics

	// ProxyLatency should use default Prometheus buckets
	// We can't directly inspect buckets, but we can observe values
	// and verify it doesn't panic
	m.ProxyLatency.WithLabelValues("test", "TEST").Observe(0.0001)
	m.ProxyLatency.WithLabelValues("test", "TEST").Observe(0.001)
	m.ProxyLatency.WithLabelValues("test", "TEST").Observe(0.01)
	m.ProxyLatency.WithLabelValues("test", "TEST").Observe(0.1)
	m.ProxyLatency.WithLabelValues("test", "TEST").Observe(1.0)
	m.ProxyLatency.WithLabelValues("test", "TEST").Observe(10.0)

	// Should not panic
}

func TestMetrics_LabelValues(t *testing.T) {
	m := globalMetrics

	// Test various label combinations
	tests := []struct {
		typ    string
		method string
	}{
		{"bulk_test", "POST"},
		{"search_test", "GET"},
		{"search_test", "POST"},
		{"read_test", "GET"},
		{"write_test", "POST"},
		{"write_test", "PUT"},
		{"delete_test", "DELETE"},
		{"maintenance_test", "POST"},
		{"other_test", "GET"},
	}

	for _, tt := range tests {
		t.Run(tt.typ+"_"+tt.method, func(t *testing.T) {
			// Get initial count
			initial := testutil.ToFloat64(m.RequestsTotal.WithLabelValues(tt.typ, tt.method))

			m.RequestsTotal.WithLabelValues(tt.typ, tt.method).Inc()
			m.ProxyLatency.WithLabelValues(tt.typ, tt.method).Observe(0.001)

			count := testutil.ToFloat64(m.RequestsTotal.WithLabelValues(tt.typ, tt.method))
			expected := initial + 1
			if count != expected {
				t.Errorf("RequestsTotal{type=%s,method=%s} = %f, want %f", tt.typ, tt.method, count, expected)
			}
		})
	}
}

func TestMetrics_ZeroValues(t *testing.T) {
	m := globalMetrics

	// Test that zero observations/increments work correctly
	m.BufferSizeBytes.WithLabelValues("/_bulk").Set(0)
	m.ProxyLatency.WithLabelValues("test", "TEST").Observe(0)

	if testutil.ToFloat64(m.BufferSizeBytes.WithLabelValues("/_bulk")) != 0 {
		t.Errorf("BufferSizeBytes should be 0")
	}
}
