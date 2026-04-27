package buffer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/codifierr/es-bulk-proxy/internal/config"
	"github.com/codifierr/es-bulk-proxy/internal/logger"
	"github.com/codifierr/es-bulk-proxy/internal/metrics"
)

const (
	esDialTimeout           = 5 * time.Second
	esKeepAlive             = 30 * time.Second
	esMaxIdleConns          = 100
	esMaxIdleConnsPerHost   = 32
	esIdleConnTimeout       = 90 * time.Second
	esTLSHandshakeTimeout   = 10 * time.Second
	esExpectContinueTimeout = 1 * time.Second
	initialBufferCapacity   = 1024 * 1024
	esHTTPErrorThreshold    = 300
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
	authHeaders   http.Header // Authentication headers from the first request
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
func (bm *BufferManager) Add(indexPath string, data []byte, headers http.Header) error {
	buf := bm.getOrCreateBuffer(indexPath)

	return buf.Add(data, headers)
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
func (ib *IndexBuffer) Add(data []byte, headers http.Header) error {
	ib.mu.Lock()
	defer ib.mu.Unlock()

	// Check if adding this would exceed max buffer size
	if ib.occupiedSizeLocked()+int64(len(data)) > ib.config.Buffer.MaxBufferSize {
		return fmt.Errorf("buffer full: max size %d bytes", ib.config.Buffer.MaxBufferSize)
	}

	// Store auth headers from the first request in this batch
	// All requests in a batch will use the same authentication
	if ib.authHeaders == nil && headers != nil {
		ib.authHeaders = headers.Clone()
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
	flushStart := time.Now()

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

	// Send with retry (handles partial item failures internally)
	attemptType, failedData, err := ib.sendWithRetry(dataToSend)
	if err != nil {
		ib.handleFlushError(err, failedData, batchSize, requestCount)
	} else {
		shouldFlushAgain := false

		ib.mu.Lock()
		ib.inFlightData = nil
		ib.inFlightSize = 0
		ib.inFlightReqs = 0
		ib.flushInFlight = false
		ib.authHeaders = nil // Clear auth headers after successful flush
		ib.lastFlush = time.Now()
		ib.updateBufferMetricLocked()
		shouldFlushAgain = ib.size >= ib.config.Buffer.MaxBatchSize
		ib.mu.Unlock()

		ib.logger.DebugFields("bulk sent successfully", map[string]any{
			"size":         batchSize,
			"requests":     requestCount,
			"indexPath":    ib.indexPath,
			"attempt_type": attemptType,
		})
		ib.metrics.BulkBatchesTotal.WithLabelValues(attemptType).Inc()
		ib.metrics.FlushDuration.WithLabelValues(ib.indexPath).Observe(time.Since(flushStart).Seconds())
		ib.metrics.LastSuccessfulFlush.WithLabelValues(ib.indexPath).Set(float64(time.Now().Unix()))

		if shouldFlushAgain {
			go ib.flush()
		}
	}
}

// itemStatus extracts only the status code from an ES bulk response item.
type itemStatus struct {
	Status int `json:"status"`
}

// bulkResponseItem represents one item in the ES bulk response.
type bulkResponseItem struct {
	Index  *itemStatus `json:"index,omitempty"`
	Create *itemStatus `json:"create,omitempty"`
	Update *itemStatus `json:"update,omitempty"`
	Delete *itemStatus `json:"delete,omitempty"`
}

func (i *bulkResponseItem) getStatus() int {
	switch {
	case i.Index != nil:
		return i.Index.Status
	case i.Create != nil:
		return i.Create.Status
	case i.Update != nil:
		return i.Update.Status
	case i.Delete != nil:
		return i.Delete.Status
	default:
		return 0
	}
}

var errorsTrue = []byte(`"errors":true`)

// findFailedItemIndices scans an ES bulk response and returns the 0-based
// indices of items that failed (HTTP status >= 300). Returns nil when there
// are no errors — the common fast-path.
func findFailedItemIndices(body []byte) []int {
	// Fast path: most batches succeed entirely.
	if !bytes.Contains(body, errorsTrue) {
		return nil
	}

	var resp struct {
		Errors bool               `json:"errors"`
		Items  []bulkResponseItem `json:"items"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil
	}

	if !resp.Errors {
		return nil
	}

	var failed []int

	for i := range resp.Items {
		if resp.Items[i].getStatus() >= esHTTPErrorThreshold {
			failed = append(failed, i)
		}
	}

	return failed
}

// extractFailedPairs streams through an ndjson bulk payload and returns only
// the action+document line pairs at the specified operation indices.
// Delete operations are single-line; all others are two-line (action + doc).
// No JSON document bodies are parsed.
func extractFailedPairs(payload []byte, failedIndices []int) []byte {
	if len(failedIndices) == 0 {
		return nil
	}

	failedSet := make(map[int]struct{}, len(failedIndices))
	for _, idx := range failedIndices {
		failedSet[idx] = struct{}{}
	}

	var result []byte

	opIndex := 0
	offset := 0

	for offset < len(payload) {
		actionStart := offset

		lineEnd := bytes.IndexByte(payload[offset:], '\n')
		if lineEnd == -1 {
			break
		}

		actionLine := payload[offset : offset+lineEnd]
		offset += lineEnd + 1

		isDelete := bytes.Contains(actionLine, []byte(`"delete"`))

		opEnd := offset

		if !isDelete && offset < len(payload) {
			docLineEnd := bytes.IndexByte(payload[offset:], '\n')
			if docLineEnd == -1 {
				opEnd = len(payload)
			} else {
				opEnd = offset + docLineEnd + 1
			}

			offset = opEnd
		}

		if _, ok := failedSet[opIndex]; ok {
			result = append(result, payload[actionStart:opEnd]...)
		}

		opIndex++
	}

	return result
}

// countNDJSONOperations counts bulk operations in an ndjson payload.
func countNDJSONOperations(data []byte) int {
	count := 0
	offset := 0

	for offset < len(data) {
		lineEnd := bytes.IndexByte(data[offset:], '\n')
		if lineEnd == -1 {
			break
		}

		line := data[offset : offset+lineEnd]
		offset += lineEnd + 1
		count++

		if !bytes.Contains(line, []byte(`"delete"`)) && offset < len(data) {
			nextLineEnd := bytes.IndexByte(data[offset:], '\n')
			if nextLineEnd == -1 {
				break
			}

			offset += nextLineEnd + 1
		}
	}

	return count
}

// handleFlushError requeues data after a flush failure.
func (ib *IndexBuffer) handleFlushError(err error, failedData []byte, batchSize int64, requestCount int) {
	ib.mu.Lock()

	requeueData, requeueSize, requeueReqs := ib.prepareRequeueData(failedData)

	queuedData := ib.data
	combined := make([]byte, 0, len(requeueData)+len(queuedData))
	combined = append(combined, requeueData...)
	combined = append(combined, queuedData...)

	ib.data = combined
	ib.size += requeueSize
	ib.requestsTotal += requeueReqs
	ib.inFlightData = nil
	ib.inFlightSize = 0
	ib.inFlightReqs = 0
	ib.flushInFlight = false
	// Keep authHeaders for retry
	ib.updateBufferMetricLocked()
	ib.mu.Unlock()

	action := "requeued_batch"
	if failedData != nil {
		action = "requeued_partial_failures"
	}

	ib.logger.ErrorFields("failed to send bulk", map[string]any{
		"error":     err.Error(),
		"size":      batchSize,
		"requests":  requestCount,
		"indexPath": ib.indexPath,
		"action":    action,
	})
	ib.metrics.BulkFailuresTotal.Inc()
	ib.metrics.BulkRequeuesTotal.WithLabelValues(ib.indexPath).Inc()
}

// prepareRequeueData determines what data to requeue based on failure type.
func (ib *IndexBuffer) prepareRequeueData(failedData []byte) ([]byte, int64, int) {
	if failedData != nil {
		// Partial failure — only requeue the failed items
		return failedData, int64(len(failedData)), countNDJSONOperations(failedData)
	}

	// Total failure — requeue entire batch
	return ib.inFlightData, ib.inFlightSize, ib.inFlightReqs
}

// sendBulkRequest sends a bulk request to Elasticsearch and returns the response body.
func (ib *IndexBuffer) sendBulkRequest(ctx context.Context, data []byte) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ib.config.Elasticsearch.URL+ib.indexPath, bytes.NewReader(data))
	if err != nil {
		return nil, 0, err
	}

	req.Header.Set("Content-Type", "application/x-ndjson")

	if ib.authHeaders != nil {
		for key, values := range ib.authHeaders {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
	}

	resp, err := ib.esClient.Do(req)
	if err != nil {
		return nil, 0, err
	}

	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	return body, resp.StatusCode, nil
}

// handlePartialFailure processes partial failures and returns retry payload.
func (ib *IndexBuffer) handlePartialFailure(currentData []byte, failedIndices []int, attempt int, partialRetry bool) ([]byte, bool, error) {
	ib.metrics.BulkPartialFailuresTotal.WithLabelValues(ib.indexPath).Add(float64(len(failedIndices)))

	retryPayload := extractFailedPairs(currentData, failedIndices)
	if len(retryPayload) == 0 {
		ib.logger.InfoFields("partial failure but could not extract failed items", map[string]any{
			"failed_items": len(failedIndices),
			"indexPath":    ib.indexPath,
		})

		return nil, partialRetry, nil
	}

	ib.logger.InfoFields("partial bulk failure, retrying failed items", map[string]any{
		"attempt":             attempt,
		"failed_items":        len(failedIndices),
		"retry_payload_bytes": len(retryPayload),
		"indexPath":           ib.indexPath,
	})

	return retryPayload, true, fmt.Errorf("partial failure: %d items failed", len(failedIndices))
}

// determineAttemptType returns the appropriate attempt type based on attempt number and partial retry status.
func determineAttemptType(attempt int, partialRetry bool) string {
	if attempt == 0 && !partialRetry {
		return "first_attempt"
	}

	return "retry"
}

// sendWithRetry sends data with exponential backoff retry, handling partial
// item failures. Returns attempt type, any remaining failed ndjson data
// (for partial failures), and error. When failedData is non-nil only that
// subset should be requeued — not the entire original batch.
func (ib *IndexBuffer) sendWithRetry(data []byte) (string, []byte, error) {
	var lastErr error

	backoff := ib.config.Retry.BackoffMin
	currentData := data
	partialRetry := false

	for attempt := 0; attempt <= ib.config.Retry.Attempts; attempt++ {
		if attempt > 0 {
			time.Sleep(backoff)
			backoff *= 2

			ib.metrics.BulkRetriesTotal.WithLabelValues(ib.indexPath).Inc()
			ib.logger.InfoFields("retrying bulk send", map[string]any{
				"attempt":   attempt,
				"backoff":   backoff.String(),
				"indexPath": ib.indexPath,
			})
		}

		reqCtx, cancel := context.WithTimeout(context.Background(), ib.config.Elasticsearch.RequestTimeout)

		body, statusCode, err := ib.sendBulkRequest(reqCtx, currentData)

		cancel()

		if err != nil {
			lastErr = err

			continue
		}

		// HTTP-level error — retry the entire current payload
		if statusCode < 200 || statusCode >= esHTTPErrorThreshold {
			lastErr = fmt.Errorf("ES returned status %d: %s", statusCode, string(body))

			continue
		}

		// 2xx — inspect per-item results for partial failures
		failedIndices := findFailedItemIndices(body)
		if len(failedIndices) == 0 {
			return determineAttemptType(attempt, partialRetry), nil, nil
		}

		// Partial failure: extract only the failed action+document pairs
		retryPayload, updatedPartialRetry, partialErr := ib.handlePartialFailure(currentData, failedIndices, attempt, partialRetry)
		if retryPayload == nil {
			return determineAttemptType(attempt, updatedPartialRetry), nil, nil
		}

		currentData = retryPayload
		partialRetry = updatedPartialRetry
		lastErr = partialErr
	}

	// All retries exhausted
	if partialRetry {
		return "partial_success", currentData, lastErr
	}

	return "", nil, lastErr
}

// Shutdown gracefully shuts down the buffer.
func (ib *IndexBuffer) Shutdown() {
	ib.flushTimer.Stop()
	ib.flush()
}
