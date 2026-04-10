package buffer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/codifierr/go-scratchpad/es-bulk-proxy/internal/config"
	"github.com/codifierr/go-scratchpad/es-bulk-proxy/internal/logger"
	"github.com/codifierr/go-scratchpad/es-bulk-proxy/internal/metrics"
)

const (
	esDialTimeout           = 5 * time.Second
	esKeepAlive             = 30 * time.Second
	esMaxIdleConns          = 100
	esMaxIdleConnsPerHost   = 32
	esIdleConnTimeout       = 90 * time.Second
	esTLSHandshakeTimeout   = 10 * time.Second
	esExpectContinueTimeout = 1 * time.Second
	esRequestTimeout        = 30 * time.Second
	initialBufferCapacity   = 1024 * 1024
)

// BufferManager manages multiple index-specific buffers.
type BufferManager struct {
	mu       sync.RWMutex
	buffers  map[string]*IndexBuffer
	config   *config.Config
	logger   *logger.Logger
	metrics  *metrics.Metrics
	esClient *http.Client
}

// IndexBuffer aggregates bulk requests for a specific index.
type IndexBuffer struct {
	mu            sync.Mutex
	indexPath     string // e.g., "/my-index/_bulk" or "/_bulk"
	data          []byte
	size          int64
	inFlightData  []byte
	inFlightSize  int64
	config        *config.Config
	logger        *logger.Logger
	metrics       *metrics.Metrics
	esClient      *http.Client
	lastFlush     time.Time
	flushTimer    *time.Timer
	requestsTotal int
	inFlightReqs  int
	flushInFlight bool
}

// NewManager creates a new buffer manager.
func NewManager(cfg *config.Config, log *logger.Logger, m *metrics.Metrics) *BufferManager {
	return &BufferManager{
		buffers:  make(map[string]*IndexBuffer),
		config:   cfg,
		logger:   log,
		metrics:  m,
		esClient: newESHTTPClient(),
	}
}

func newESHTTPClient() *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   esDialTimeout,
			KeepAlive: esKeepAlive,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          esMaxIdleConns,
		MaxIdleConnsPerHost:   esMaxIdleConnsPerHost,
		IdleConnTimeout:       esIdleConnTimeout,
		TLSHandshakeTimeout:   esTLSHandshakeTimeout,
		ExpectContinueTimeout: esExpectContinueTimeout,
	}

	return &http.Client{
		Timeout:   esRequestTimeout,
		Transport: transport,
	}
}

// getOrCreateBuffer gets or creates a buffer for a specific index.
func (bm *BufferManager) getOrCreateBuffer(indexPath string) *IndexBuffer {
	bm.mu.RLock()
	buf, exists := bm.buffers[indexPath]
	bm.mu.RUnlock()

	if exists {
		return buf
	}

	// Create new buffer
	bm.mu.Lock()
	defer bm.mu.Unlock()

	// Double-check after acquiring write lock
	if buf, exists := bm.buffers[indexPath]; exists {
		return buf
	}

	buf = &IndexBuffer{
		indexPath: indexPath,
		data:      make([]byte, 0, initialBufferCapacity), // Pre-allocate 1MB
		config:    bm.config,
		logger:    bm.logger,
		metrics:   bm.metrics,
		esClient:  bm.esClient,
		lastFlush: time.Now(),
	}

	// Start flush timer
	buf.flushTimer = time.AfterFunc(bm.config.Buffer.FlushInterval, buf.timedFlush)

	bm.buffers[indexPath] = buf
	bm.logger.InfoFields("created new buffer", map[string]any{
		"indexPath": indexPath,
	})

	return buf
}

// Add appends data to the appropriate index buffer.
func (bm *BufferManager) Add(indexPath string, data []byte) error {
	buf := bm.getOrCreateBuffer(indexPath)

	return buf.Add(data)
}

// Shutdown gracefully shuts down all buffers.
func (bm *BufferManager) Shutdown() {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	for _, buf := range bm.buffers {
		buf.Shutdown()
	}

	if transport, ok := bm.esClient.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
}

func (ib *IndexBuffer) occupiedSizeLocked() int64 {
	return ib.size + ib.inFlightSize
}

func (ib *IndexBuffer) updateBufferMetricLocked() {
	ib.metrics.BufferSizeBytes.WithLabelValues(ib.indexPath).Set(float64(ib.occupiedSizeLocked()))
	ib.metrics.BufferInFlightBytes.WithLabelValues(ib.indexPath).Set(float64(ib.inFlightSize))
	ib.metrics.BufferInFlightRequests.WithLabelValues(ib.indexPath).Set(float64(ib.inFlightReqs))
}

// Add appends data to the buffer.
func (ib *IndexBuffer) Add(data []byte) error {
	ib.mu.Lock()
	defer ib.mu.Unlock()

	// Check if adding this would exceed max buffer size
	if ib.occupiedSizeLocked()+int64(len(data)) > ib.config.Buffer.MaxBufferSize {
		return fmt.Errorf("buffer full: max size %d bytes", ib.config.Buffer.MaxBufferSize)
	}

	ib.data = append(ib.data, data...)
	ib.size += int64(len(data))
	ib.requestsTotal++

	ib.updateBufferMetricLocked()

	// Flush if batch size exceeded
	if ib.size >= ib.config.Buffer.MaxBatchSize {
		ib.logger.DebugFields("flushing buffer", map[string]any{
			"reason":    "size_threshold",
			"size":      ib.size,
			"requests":  ib.requestsTotal,
			"indexPath": ib.indexPath,
		})

		go ib.flush()
	}

	return nil
}

// timedFlush is called by the timer.
func (ib *IndexBuffer) timedFlush() {
	ib.mu.Lock()
	if ib.size > 0 {
		ib.logger.DebugFields("flushing buffer", map[string]any{
			"reason":    "time_threshold",
			"size":      ib.size,
			"requests":  ib.requestsTotal,
			"indexPath": ib.indexPath,
		})

		go ib.flush()
		ib.mu.Unlock()
	} else {
		ib.mu.Unlock()
	}

	// Reset timer
	ib.flushTimer.Reset(ib.config.Buffer.FlushInterval)
}

// flush sends the buffer to Elasticsearch.
func (ib *IndexBuffer) flush() {
	ib.mu.Lock()
	if ib.size == 0 || ib.flushInFlight {
		ib.mu.Unlock()

		return
	}

	// Move the queued batch to an in-flight slot so it still counts against the
	// buffer limit until Elasticsearch acknowledges it.
	dataToSend := ib.data
	batchSize := ib.size
	requestCount := ib.requestsTotal

	ib.inFlightData = dataToSend
	ib.inFlightSize = batchSize
	ib.inFlightReqs = requestCount
	ib.flushInFlight = true
	ib.data = make([]byte, 0, cap(dataToSend))
	ib.size = 0
	ib.requestsTotal = 0
	ib.updateBufferMetricLocked()
	ib.mu.Unlock()

	// Send with retry
	err := ib.sendWithRetry(dataToSend)
	if err != nil {
		ib.mu.Lock()
		queuedData := ib.data
		combined := make([]byte, 0, len(ib.inFlightData)+len(queuedData))
		combined = append(combined, ib.inFlightData...)
		combined = append(combined, queuedData...)

		ib.data = combined
		ib.size += ib.inFlightSize
		ib.requestsTotal += ib.inFlightReqs
		ib.inFlightData = nil
		ib.inFlightSize = 0
		ib.inFlightReqs = 0
		ib.flushInFlight = false
		ib.updateBufferMetricLocked()
		ib.mu.Unlock()

		ib.logger.ErrorFields("failed to send bulk", map[string]any{
			"error":     err.Error(),
			"size":      batchSize,
			"requests":  requestCount,
			"indexPath": ib.indexPath,
			"action":    "requeued_batch",
		})
		ib.metrics.BulkFailuresTotal.Inc()
		ib.metrics.BulkRequeuesTotal.WithLabelValues(ib.indexPath).Inc()
	} else {
		shouldFlushAgain := false

		ib.mu.Lock()
		ib.inFlightData = nil
		ib.inFlightSize = 0
		ib.inFlightReqs = 0
		ib.flushInFlight = false
		ib.lastFlush = time.Now()
		ib.updateBufferMetricLocked()
		shouldFlushAgain = ib.size >= ib.config.Buffer.MaxBatchSize
		ib.mu.Unlock()

		ib.logger.DebugFields("bulk sent successfully", map[string]any{
			"size":      batchSize,
			"requests":  requestCount,
			"indexPath": ib.indexPath,
		})
		ib.metrics.BulkBatchesTotal.Inc()

		if shouldFlushAgain {
			go ib.flush()
		}
	}
}

// sendWithRetry sends data with exponential backoff retry.
func (ib *IndexBuffer) sendWithRetry(data []byte) error {
	var lastErr error

	backoff := ib.config.Retry.BackoffMin

	for attempt := 0; attempt <= ib.config.Retry.Attempts; attempt++ {
		if attempt > 0 {
			time.Sleep(backoff)
			backoff *= 2
			ib.logger.InfoFields("retrying bulk send", map[string]any{
				"attempt":   attempt,
				"backoff":   backoff.String(),
				"indexPath": ib.indexPath,
			})
		}

		// CRITICAL: Forward to same index path to preserve ES context
		reqCtx, cancel := context.WithTimeout(context.Background(), esRequestTimeout)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, ib.config.Elasticsearch.URL+ib.indexPath, bytes.NewReader(data))

		if err != nil {
			cancel()

			lastErr = err

			continue
		}

		req.Header.Set("Content-Type", "application/x-ndjson")

		resp, err := ib.esClient.Do(req)
		if err != nil {
			cancel()

			lastErr = err

			continue
		}

		body, _ := io.ReadAll(resp.Body)

		err = resp.Body.Close()

		cancel()

		if err != nil {
			lastErr = err

			continue
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}

		lastErr = fmt.Errorf("ES returned status %d: %s", resp.StatusCode, string(body))
	}

	return lastErr
}

// Shutdown gracefully shuts down the buffer.
func (ib *IndexBuffer) Shutdown() {
	ib.flushTimer.Stop()
	ib.flush()
}
