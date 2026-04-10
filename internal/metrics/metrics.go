package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics.
type Metrics struct {
	RequestsTotal          *prometheus.CounterVec
	BulkBatchesTotal       prometheus.Counter
	BulkFailuresTotal      prometheus.Counter
	BulkRequeuesTotal      *prometheus.CounterVec
	BufferSizeBytes        *prometheus.GaugeVec
	BufferInFlightBytes    *prometheus.GaugeVec
	BufferInFlightRequests *prometheus.GaugeVec
	ProxyLatency           *prometheus.HistogramVec
}

// New creates and registers all metrics.
func New() *Metrics {
	return &Metrics{
		RequestsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "es_proxy_requests_total",
				Help: "Total number of requests by type and method",
			},
			[]string{"type", "method"},
		),
		BulkBatchesTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "es_proxy_bulk_batches_total",
				Help: "Total number of bulk batches sent to Elasticsearch",
			},
		),
		BulkFailuresTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "es_proxy_bulk_failures_total",
				Help: "Total number of bulk batch send failures",
			},
		),
		BulkRequeuesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "es_proxy_bulk_requeues_total",
				Help: "Total number of bulk batches requeued after a failed send",
			},
			[]string{"index_path"},
		),
		BufferSizeBytes: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "es_proxy_buffer_size_bytes",
				Help: "Current occupied buffer size in bytes by bulk index path, including in-flight bytes",
			},
			[]string{"index_path"},
		),
		BufferInFlightBytes: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "es_proxy_buffer_in_flight_bytes",
				Help: "Current in-flight buffer size in bytes by bulk index path",
			},
			[]string{"index_path"},
		),
		BufferInFlightRequests: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "es_proxy_buffer_in_flight_requests",
				Help: "Current in-flight request count by bulk index path",
			},
			[]string{"index_path"},
		),
		ProxyLatency: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "es_proxy_latency_seconds",
				Help:    "Latency of proxy requests by type and method",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"type", "method"},
		),
	}
}
