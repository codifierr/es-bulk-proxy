package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// labelIndexPath is the Prometheus label identifying the bulk index path. It is
// shared by every per-index metric so the label name is defined exactly once.
const labelIndexPath = "index_path"

// Metrics holds all Prometheus metrics.
type Metrics struct {
	RequestsTotal            *prometheus.CounterVec
	BulkBatchesTotal         *prometheus.CounterVec
	BulkFailuresTotal        prometheus.Counter
	BulkRetriesTotal         *prometheus.CounterVec
	BulkRequeuesTotal        *prometheus.CounterVec
	BulkPartialFailuresTotal *prometheus.CounterVec
	BufferSizeBytes          *prometheus.GaugeVec
	BufferInFlightBytes      *prometheus.GaugeVec
	BufferInFlightRequests   *prometheus.GaugeVec
	ProxyLatency             *prometheus.HistogramVec
	FlushDuration            *prometheus.HistogramVec
	DroppedBatchesTotal      *prometheus.CounterVec
	LastSuccessfulFlush      *prometheus.GaugeVec
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
		BulkBatchesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "es_proxy_bulk_batches_total",
				Help: "Total number of bulk batches sent to Elasticsearch by attempt type",
			},
			[]string{"attempt_type"},
		),
		BulkFailuresTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "es_proxy_bulk_failures_total",
				Help: "Total number of bulk batch send failures after all retries exhausted",
			},
		),
		BulkRetriesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "es_proxy_bulk_retries_total",
				Help: "Total number of bulk batch retry attempts by index path",
			},
			[]string{labelIndexPath},
		),
		BulkRequeuesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "es_proxy_bulk_requeues_total",
				Help: "Total number of bulk batches requeued after a failed send",
			},
			[]string{labelIndexPath},
		),
		BulkPartialFailuresTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "es_proxy_bulk_partial_failures_total",
				Help: "Total number of individual document failures in bulk responses",
			},
			[]string{labelIndexPath},
		),
		BufferSizeBytes: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "es_proxy_buffer_size_bytes",
				Help: "Current occupied buffer size in bytes by bulk index path, including in-flight bytes",
			},
			[]string{labelIndexPath},
		),
		BufferInFlightBytes: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "es_proxy_buffer_in_flight_bytes",
				Help: "Current in-flight buffer size in bytes by bulk index path",
			},
			[]string{labelIndexPath},
		),
		BufferInFlightRequests: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "es_proxy_buffer_in_flight_requests",
				Help: "Current in-flight request count by bulk index path",
			},
			[]string{labelIndexPath},
		),
		ProxyLatency: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "es_proxy_latency_seconds",
				Help:    "Latency of proxy requests by type and method",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"type", "method"},
		),
		FlushDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "es_proxy_flush_duration_seconds",
				Help:    "Duration of bulk batch flush operations by index path",
				Buckets: prometheus.DefBuckets,
			},
			[]string{labelIndexPath},
		),
		DroppedBatchesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "es_proxy_dropped_batches_total",
				Help: "Total number of bulk batches dropped by index path",
			},
			[]string{labelIndexPath},
		),
		LastSuccessfulFlush: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "es_proxy_last_successful_flush_timestamp_seconds",
				Help: "Unix timestamp of the last successful flush by index path",
			},
			[]string{labelIndexPath},
		),
	}
}
