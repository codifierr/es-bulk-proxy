package buffer

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/codifierr/es-bulk-proxy/internal/config"
	"github.com/codifierr/es-bulk-proxy/internal/logger"
	"github.com/codifierr/es-bulk-proxy/internal/metrics"
)

// Use a shared metrics instance to avoid Prometheus registration conflicts
var testMetrics = metrics.New()

func TestNewManager(t *testing.T) {
	cfg := &config.Config{
		Buffer: config.BufferConfig{
			FlushInterval: 1 * time.Second,
			MaxBatchSize:  1024,
			MaxBufferSize: 10240,
		},
	}
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}
	log := logger.New(&cfg.Logger, true)
	m := testMetrics

	manager := NewManager(cfg, log, m)

	if manager == nil {
		t.Fatal("NewManager returned nil")
	}
	if manager.buffers == nil {
		t.Error("buffers map not initialized")
	}
	if manager.config != cfg {
		t.Error("config not set correctly")
	}
	if manager.esClient == nil {
		t.Error("shared esClient not initialized")
	}
}

func TestNewManager_UsesSharedHTTPClient(t *testing.T) {
	cfg := &config.Config{
		Buffer: config.BufferConfig{
			FlushInterval: 1 * time.Second,
			MaxBatchSize:  1024,
			MaxBufferSize: 10240,
		},
	}
	manager := NewManager(cfg, logger.New(&cfg.Logger, true), testMetrics)

	first := manager.getOrCreateBuffer("/_bulk")
	second := manager.getOrCreateBuffer("/index/_bulk")

	if first.esClient != manager.esClient {
		t.Fatal("first buffer should reuse manager esClient")
	}
	if second.esClient != manager.esClient {
		t.Fatal("second buffer should reuse manager esClient")
	}

	transport, ok := manager.esClient.Transport.(*http.Transport)
	if !ok {
		t.Fatal("manager esClient transport should be *http.Transport")
	}
	if transport.MaxIdleConns != 100 {
		t.Errorf("MaxIdleConns = %d, want 100", transport.MaxIdleConns)
	}
	if transport.MaxIdleConnsPerHost != 32 {
		t.Errorf("MaxIdleConnsPerHost = %d, want 32", transport.MaxIdleConnsPerHost)
	}
	if transport.IdleConnTimeout != 90*time.Second {
		t.Errorf("IdleConnTimeout = %v, want 90s", transport.IdleConnTimeout)
	}
}

func TestBufferManager_Add(t *testing.T) {
	tests := []struct {
		name      string
		indexPath string
		data      []byte
		wantErr   bool
	}{
		{
			name:      "add to default bulk endpoint",
			indexPath: "/_bulk",
			data:      []byte(`{"index":{"_index":"test"}}\n{"field":"value"}\n`),
			wantErr:   false,
		},
		{
			name:      "add to index-specific bulk endpoint",
			indexPath: "/myindex/_bulk",
			data:      []byte(`{"index":{}}\n{"field":"value"}\n`),
			wantErr:   false,
		},
		{
			name:      "add empty data",
			indexPath: "/_bulk",
			data:      []byte{},
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{
				Buffer: config.BufferConfig{
					FlushInterval: 10 * time.Second,
					MaxBatchSize:  1024 * 1024,
					MaxBufferSize: 10 * 1024 * 1024,
				},
			}
			log := logger.New(nil, true)
			m := testMetrics
			manager := NewManager(cfg, log, m)

			err := manager.Add(tt.indexPath, tt.data, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("Add() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBufferManager_Add_BufferFull(t *testing.T) {
	cfg := &config.Config{
		Buffer: config.BufferConfig{
			FlushInterval: 10 * time.Second,
			MaxBatchSize:  100,
			MaxBufferSize: 100, // Very small buffer
		},
	}
	log := logger.New(nil, true)
	m := testMetrics
	manager := NewManager(cfg, log, m)

	// First add should succeed
	smallData := []byte("small")
	err := manager.Add("/_bulk", smallData, nil)
	if err != nil {
		t.Errorf("First Add() failed: %v", err)
	}

	// Second add should exceed buffer
	largeData := make([]byte, 200)
	err = manager.Add("/_bulk", largeData, nil)
	if err == nil {
		t.Error("Add() should return error when buffer is full")
	}
}

func TestBufferManager_MultipleIndices(t *testing.T) {
	cfg := &config.Config{
		Buffer: config.BufferConfig{
			FlushInterval: 10 * time.Second,
			MaxBatchSize:  1024,
			MaxBufferSize: 10240,
		},
	}
	log := logger.New(nil, true)
	m := testMetrics
	manager := NewManager(cfg, log, m)

	// Add to different indices
	indices := []string{"/_bulk", "/index1/_bulk", "/index2/_bulk"}
	for _, idx := range indices {
		err := manager.Add(idx, []byte("data\n"), nil)
		if err != nil {
			t.Errorf("Add() to %s failed: %v", idx, err)
		}
	}

	// Verify separate buffers were created
	manager.mu.RLock()
	if len(manager.buffers) != len(indices) {
		t.Errorf("Expected %d buffers, got %d", len(indices), len(manager.buffers))
	}
	manager.mu.RUnlock()
}

func TestBufferManager_ConcurrentAdd(t *testing.T) {
	cfg := &config.Config{
		Buffer: config.BufferConfig{
			FlushInterval: 10 * time.Second,
			MaxBatchSize:  1024 * 1024,
			MaxBufferSize: 10 * 1024 * 1024,
		},
	}
	log := logger.New(nil, true)
	m := testMetrics
	manager := NewManager(cfg, log, m)

	const numGoroutines = 10
	const addsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < addsPerGoroutine; j++ {
				data := []byte(`{"index":{}}\n{"field":"value"}\n`)
				err := manager.Add("/_bulk", data, nil)
				if err != nil {
					t.Logf("Add failed in goroutine %d: %v", id, err)
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestIndexBuffer_Add(t *testing.T) {
	cfg := &config.Config{
		Buffer: config.BufferConfig{
			FlushInterval: 10 * time.Second,
			MaxBatchSize:  1024,
			MaxBufferSize: 10240,
		},
	}
	log := logger.New(nil, true)
	m := testMetrics

	buf := &IndexBuffer{
		indexPath: "/_bulk",
		data:      make([]byte, 0, 1024),
		config:    cfg,
		logger:    log,
		metrics:   m,
		esClient:  newESHTTPClient(),
		lastFlush: time.Now(),
	}
	buf.flushTimer = time.AfterFunc(cfg.Buffer.FlushInterval, buf.timedFlush)
	defer buf.flushTimer.Stop()

	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name:    "valid data",
			data:    []byte("test data\n"),
			wantErr: false,
		},
		{
			name:    "empty data",
			data:    []byte{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := buf.Add(tt.data, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("Add() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr && len(tt.data) > 0 {
				if buf.size != int64(len(tt.data)) {
					t.Errorf("Buffer size = %d, want %d", buf.size, len(tt.data))
				}
			}
		})
	}
}

func TestIndexBuffer_FlushOnSizeThreshold(t *testing.T) {
	// Create a test server
	flushed := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flushed = true
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errors":false}`))
	}))
	defer ts.Close()

	cfg := &config.Config{
		Elasticsearch: config.ElasticsearchConfig{
			URL: ts.URL,
		},
		Buffer: config.BufferConfig{
			FlushInterval: 10 * time.Second,
			MaxBatchSize:  100, // Small threshold
			MaxBufferSize: 10240,
		},
		Retry: config.RetryConfig{
			Attempts:   1,
			BackoffMin: 100 * time.Millisecond,
		},
	}
	log := logger.New(nil, true)
	m := testMetrics

	buf := &IndexBuffer{
		indexPath: "/_bulk",
		data:      make([]byte, 0, 1024),
		config:    cfg,
		logger:    log,
		metrics:   m,
		esClient:  newESHTTPClient(),
		lastFlush: time.Now(),
	}
	buf.flushTimer = time.AfterFunc(cfg.Buffer.FlushInterval, buf.timedFlush)
	defer buf.flushTimer.Stop()

	// Add data exceeding threshold
	largeData := make([]byte, 150)
	err := buf.Add(largeData, nil)
	if err != nil {
		t.Fatalf("Add() failed: %v", err)
	}

	// Wait for async flush
	time.Sleep(500 * time.Millisecond)

	if !flushed {
		t.Error("Buffer should have been flushed due to size threshold")
	}
}

func TestIndexBuffer_TimedFlush(t *testing.T) {
	// Create a test server
	flushed := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flushed = true
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errors":false}`))
	}))
	defer ts.Close()

	cfg := &config.Config{
		Elasticsearch: config.ElasticsearchConfig{
			URL: ts.URL,
		},
		Buffer: config.BufferConfig{
			FlushInterval: 200 * time.Millisecond, // Short interval for testing
			MaxBatchSize:  1024 * 1024,
			MaxBufferSize: 10 * 1024 * 1024,
		},
		Retry: config.RetryConfig{
			Attempts:   1,
			BackoffMin: 100 * time.Millisecond,
		},
	}
	log := logger.New(nil, true)
	m := testMetrics

	buf := &IndexBuffer{
		indexPath: "/_bulk",
		data:      make([]byte, 0, 1024),
		config:    cfg,
		logger:    log,
		metrics:   m,
		esClient:  newESHTTPClient(),
		lastFlush: time.Now(),
	}
	buf.flushTimer = time.AfterFunc(cfg.Buffer.FlushInterval, buf.timedFlush)
	defer buf.flushTimer.Stop()

	// Add small data
	err := buf.Add([]byte("small data\n"), nil)
	if err != nil {
		t.Fatalf("Add() failed: %v", err)
	}

	// Wait for timed flush
	time.Sleep(500 * time.Millisecond)

	if !flushed {
		t.Error("Buffer should have been flushed due to time threshold")
	}
}

func TestIndexBuffer_SendWithRetry_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("Expected POST request, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/x-ndjson" {
			t.Errorf("Expected Content-Type: application/x-ndjson, got %s", r.Header.Get("Content-Type"))
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errors":false}`))
	}))
	defer ts.Close()

	cfg := &config.Config{
		Elasticsearch: config.ElasticsearchConfig{
			URL: ts.URL,
		},
		Retry: config.RetryConfig{
			Attempts:   3,
			BackoffMin: 10 * time.Millisecond,
		},
	}
	log := logger.New(nil, true)
	m := testMetrics

	buf := &IndexBuffer{
		indexPath: "/_bulk",
		config:    cfg,
		logger:    log,
		metrics:   m,
		esClient:  newESHTTPClient(),
	}

	data := []byte(`{"index":{}}\n{"field":"value"}\n`)
	attemptType, _, err := buf.sendWithRetry(data)
	if err != nil {
		t.Errorf("sendWithRetry() failed: %v", err)
	}
	if attemptType != "first_attempt" {
		t.Errorf("Expected attempt_type='first_attempt', got '%s'", attemptType)
	}
}

func TestIndexBuffer_SendWithRetry_Failure(t *testing.T) {
	attemptCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attemptCount++
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"internal error"}`))
	}))
	defer ts.Close()

	cfg := &config.Config{
		Elasticsearch: config.ElasticsearchConfig{
			URL: ts.URL,
		},
		Retry: config.RetryConfig{
			Attempts:   2,
			BackoffMin: 10 * time.Millisecond,
		},
	}
	log := logger.New(nil, true)
	m := testMetrics

	buf := &IndexBuffer{
		indexPath: "/_bulk",
		config:    cfg,
		logger:    log,
		metrics:   m,
		esClient:  newESHTTPClient(),
	}

	data := []byte(`{"index":{}}\n{"field":"value"}\n`)
	attemptType, _, err := buf.sendWithRetry(data)
	if err == nil {
		t.Error("sendWithRetry() should return error on failures")
	}
	if attemptType != "" {
		t.Errorf("Expected empty attempt_type on failure, got '%s'", attemptType)
	}

	// Should retry: initial attempt + 2 retries = 3 total
	expectedAttempts := cfg.Retry.Attempts + 1
	if attemptCount != expectedAttempts {
		t.Errorf("Expected %d attempts, got %d", expectedAttempts, attemptCount)
	}
}

func TestIndexBuffer_FlushFailureRequeuesData(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"internal error"}`))
	}))
	defer ts.Close()

	cfg := &config.Config{
		Elasticsearch: config.ElasticsearchConfig{
			URL: ts.URL,
		},
		Buffer: config.BufferConfig{
			FlushInterval: 10 * time.Second,
			MaxBatchSize:  1024,
			MaxBufferSize: 2048,
		},
		Retry: config.RetryConfig{
			Attempts:   0,
			BackoffMin: 10 * time.Millisecond,
		},
	}

	buf := &IndexBuffer{
		indexPath: "/_bulk",
		data:      []byte("failed batch\n"),
		size:      int64(len("failed batch\n")),
		config:    cfg,
		logger:    logger.New(nil, true),
		metrics:   testMetrics,
		esClient:  newESHTTPClient(),
		lastFlush: time.Now(),
	}

	buf.flush()

	buf.mu.Lock()
	defer buf.mu.Unlock()

	if buf.flushInFlight {
		t.Fatal("flush should not remain in-flight after failure")
	}
	if buf.inFlightSize != 0 {
		t.Fatalf("inFlightSize = %d, want 0", buf.inFlightSize)
	}
	if string(buf.data) != "failed batch\n" {
		t.Fatalf("requeued data = %q, want original batch", string(buf.data))
	}
	if buf.size != int64(len("failed batch\n")) {
		t.Fatalf("size = %d, want %d", buf.size, len("failed batch\n"))
	}
}

func TestIndexBuffer_AddCountsInFlightBytesAgainstCapacity(t *testing.T) {
	cfg := &config.Config{
		Buffer: config.BufferConfig{
			FlushInterval: 10 * time.Second,
			MaxBatchSize:  1024,
			MaxBufferSize: 100,
		},
	}

	buf := &IndexBuffer{
		indexPath:     "/_bulk",
		config:        cfg,
		logger:        logger.New(nil, true),
		metrics:       testMetrics,
		esClient:      newESHTTPClient(),
		inFlightData:  make([]byte, 90),
		inFlightSize:  90,
		inFlightReqs:  1,
		flushInFlight: true,
		lastFlush:     time.Now(),
	}

	if err := buf.Add(make([]byte, 10), nil); err != nil {
		t.Fatalf("Add() should allow data within remaining capacity: %v", err)
	}

	if err := buf.Add([]byte("x"), nil); err == nil {
		t.Fatal("Add() should reject writes once queued plus in-flight bytes exceed max buffer size")
	}
}

func TestIndexBuffer_Shutdown(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errors":false}`))
	}))
	defer ts.Close()

	cfg := &config.Config{
		Elasticsearch: config.ElasticsearchConfig{
			URL: ts.URL,
		},
		Buffer: config.BufferConfig{
			FlushInterval: 10 * time.Second,
			MaxBatchSize:  1024,
			MaxBufferSize: 10240,
		},
		Retry: config.RetryConfig{
			Attempts:   1,
			BackoffMin: 10 * time.Millisecond,
		},
	}
	log := logger.New(nil, true)
	m := testMetrics

	buf := &IndexBuffer{
		indexPath: "/_bulk",
		data:      []byte("test data\n"),
		size:      10,
		config:    cfg,
		logger:    log,
		metrics:   m,
		esClient:  newESHTTPClient(),
		lastFlush: time.Now(),
	}
	buf.flushTimer = time.AfterFunc(cfg.Buffer.FlushInterval, buf.timedFlush)

	buf.Shutdown()

	// Timer should be stopped
	if buf.flushTimer.Stop() {
		t.Error("Timer should already be stopped after Shutdown()")
	}
}

func TestBufferManager_Shutdown(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errors":false}`))
	}))
	defer ts.Close()

	cfg := &config.Config{
		Elasticsearch: config.ElasticsearchConfig{
			URL: ts.URL,
		},
		Buffer: config.BufferConfig{
			FlushInterval: 10 * time.Second,
			MaxBatchSize:  1024,
			MaxBufferSize: 10240,
		},
		Retry: config.RetryConfig{
			Attempts:   1,
			BackoffMin: 10 * time.Millisecond,
		},
	}
	log := logger.New(nil, true)
	m := testMetrics
	manager := NewManager(cfg, log, m)

	// Add data to multiple buffers
	_ = manager.Add("/_bulk", []byte("data1\n"), nil)
	_ = manager.Add("/index1/_bulk", []byte("data2\n"), nil)

	// Shutdown should flush all buffers
	manager.Shutdown()

	// Give it time to complete
	time.Sleep(100 * time.Millisecond)
}

func TestIndexBuffer_EmptyFlush(t *testing.T) {
	cfg := &config.Config{
		Buffer: config.BufferConfig{
			FlushInterval: 10 * time.Second,
			MaxBatchSize:  1024,
			MaxBufferSize: 10240,
		},
	}
	log := logger.New(nil, true)
	m := testMetrics

	buf := &IndexBuffer{
		indexPath: "/_bulk",
		data:      make([]byte, 0),
		size:      0,
		config:    cfg,
		logger:    log,
		metrics:   m,
		esClient:  newESHTTPClient(),
	}

	// Flushing empty buffer should be no-op
	buf.flush()

	if buf.size != 0 {
		t.Error("Empty flush should not change size")
	}
}

func TestIndexBuffer_ForwardsAuthenticationHeaders(t *testing.T) {
	// Track received headers
	var receivedAuth string
	var receivedApiKey string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		receivedApiKey = r.Header.Get("X-Elastic-Api-Key")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errors":false}`))
	}))
	defer ts.Close()

	cfg := &config.Config{
		Elasticsearch: config.ElasticsearchConfig{
			URL: ts.URL,
		},
		Buffer: config.BufferConfig{
			FlushInterval: 10 * time.Second,
			MaxBatchSize:  100, // Small threshold to trigger flush
			MaxBufferSize: 10240,
		},
		Retry: config.RetryConfig{
			Attempts:   1,
			BackoffMin: 100 * time.Millisecond,
		},
	}
	log := logger.New(nil, true)
	m := testMetrics

	buf := &IndexBuffer{
		indexPath: "/_bulk",
		data:      make([]byte, 0, 1024),
		config:    cfg,
		logger:    log,
		metrics:   m,
		esClient:  newESHTTPClient(),
		lastFlush: time.Now(),
	}
	buf.flushTimer = time.AfterFunc(cfg.Buffer.FlushInterval, buf.timedFlush)
	defer buf.flushTimer.Stop()

	// Create headers with authentication
	authHeaders := make(http.Header)
	authHeaders.Set("Authorization", "Bearer test-token-123")
	authHeaders.Set("X-Elastic-Api-Key", "api-key-456")

	// Add data with auth headers
	largeData := make([]byte, 150) // Exceeds MaxBatchSize to trigger flush
	err := buf.Add(largeData, authHeaders)
	if err != nil {
		t.Fatalf("Add() failed: %v", err)
	}

	// Wait for async flush
	time.Sleep(500 * time.Millisecond)

	// Verify headers were forwarded
	if receivedAuth != "Bearer test-token-123" {
		t.Errorf("Authorization header not forwarded correctly. Got: %s, Want: Bearer test-token-123", receivedAuth)
	}
	if receivedApiKey != "api-key-456" {
		t.Errorf("X-Elastic-Api-Key header not forwarded correctly. Got: %s, Want: api-key-456", receivedApiKey)
	}
}

func TestIndexBuffer_UsesFirstRequestAuthHeaders(t *testing.T) {
	// Track received headers
	var receivedAuth string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errors":false}`))
	}))
	defer ts.Close()

	cfg := &config.Config{
		Elasticsearch: config.ElasticsearchConfig{
			URL: ts.URL,
		},
		Buffer: config.BufferConfig{
			FlushInterval: 10 * time.Second,
			MaxBatchSize:  200,
			MaxBufferSize: 10240,
		},
		Retry: config.RetryConfig{
			Attempts:   1,
			BackoffMin: 100 * time.Millisecond,
		},
	}

	buf := &IndexBuffer{
		indexPath: "/_bulk",
		data:      make([]byte, 0, 1024),
		config:    cfg,
		logger:    logger.New(nil, true),
		metrics:   testMetrics,
		esClient:  newESHTTPClient(),
		lastFlush: time.Now(),
	}

	// First request with auth
	firstHeaders := make(http.Header)
	firstHeaders.Set("Authorization", "Bearer first-token")
	err := buf.Add([]byte("first data\n"), firstHeaders)
	if err != nil {
		t.Fatalf("First Add() failed: %v", err)
	}

	// Second request with different auth (should be ignored)
	secondHeaders := make(http.Header)
	secondHeaders.Set("Authorization", "Bearer second-token")
	err = buf.Add([]byte("second data\n"), secondHeaders)
	if err != nil {
		t.Fatalf("Second Add() failed: %v", err)
	}

	// Trigger manual flush
	buf.flush()

	// Wait for flush to complete
	time.Sleep(200 * time.Millisecond)

	// Should use the first request's auth
	if receivedAuth != "Bearer first-token" {
		t.Errorf("Should use first request's auth. Got: %s, Want: Bearer first-token", receivedAuth)
	}
}

func TestIndexBuffer_ClearsAuthHeadersAfterSuccessfulFlush(t *testing.T) {
	callCount := 0
	var firstAuth, secondAuth string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount == 1 {
			firstAuth = r.Header.Get("Authorization")
		} else if callCount == 2 {
			secondAuth = r.Header.Get("Authorization")
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errors":false}`))
	}))
	defer ts.Close()

	cfg := &config.Config{
		Elasticsearch: config.ElasticsearchConfig{
			URL: ts.URL,
		},
		Buffer: config.BufferConfig{
			FlushInterval: 10 * time.Second,
			MaxBatchSize:  50,
			MaxBufferSize: 10240,
		},
		Retry: config.RetryConfig{
			Attempts:   1,
			BackoffMin: 100 * time.Millisecond,
		},
	}

	buf := &IndexBuffer{
		indexPath: "/_bulk",
		data:      make([]byte, 0, 1024),
		config:    cfg,
		logger:    logger.New(nil, true),
		metrics:   testMetrics,
		esClient:  newESHTTPClient(),
		lastFlush: time.Now(),
	}

	// First batch with auth1
	headers1 := make(http.Header)
	headers1.Set("Authorization", "Bearer token1")
	_ = buf.Add(make([]byte, 60), headers1) // Triggers flush

	time.Sleep(300 * time.Millisecond)

	// Second batch with auth2
	headers2 := make(http.Header)
	headers2.Set("Authorization", "Bearer token2")
	_ = buf.Add(make([]byte, 60), headers2) // Triggers flush

	time.Sleep(300 * time.Millisecond)

	// Each batch should use its own auth
	if firstAuth != "Bearer token1" {
		t.Errorf("First flush should use token1. Got: %s", firstAuth)
	}
	if secondAuth != "Bearer token2" {
		t.Errorf("Second flush should use token2. Got: %s", secondAuth)
	}
}

func TestIndexBuffer_SendWithRetry_PreservesAuthOnRetry(t *testing.T) {
	attemptCount := 0
	var receivedAuths []string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attemptCount++
		auth := r.Header.Get("Authorization")
		receivedAuths = append(receivedAuths, auth)

		// Fail first attempt, succeed on second
		if attemptCount == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"temporary error"}`))
		} else {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"errors":false}`))
		}
	}))
	defer ts.Close()

	cfg := &config.Config{
		Elasticsearch: config.ElasticsearchConfig{
			URL: ts.URL,
		},
		Retry: config.RetryConfig{
			Attempts:   2,
			BackoffMin: 10 * time.Millisecond,
		},
	}

	buf := &IndexBuffer{
		indexPath:   "/_bulk",
		config:      cfg,
		logger:      logger.New(nil, true),
		metrics:     testMetrics,
		esClient:    newESHTTPClient(),
		authHeaders: http.Header{"Authorization": []string{"Bearer retry-test"}},
	}

	data := []byte(`{"index":{}}\n{"field":"value"}\n`)
	attemptType, _, err := buf.sendWithRetry(data)

	if err != nil {
		t.Errorf("sendWithRetry() should succeed on retry: %v", err)
	}
	if attemptType != "retry" {
		t.Errorf("Expected attempt_type='retry', got '%s'", attemptType)
	}

	// Both attempts should have the auth header
	if len(receivedAuths) != 2 {
		t.Fatalf("Expected 2 attempts, got %d", len(receivedAuths))
	}
	if receivedAuths[0] != "Bearer retry-test" {
		t.Errorf("First attempt missing auth. Got: %s", receivedAuths[0])
	}
	if receivedAuths[1] != "Bearer retry-test" {
		t.Errorf("Retry attempt missing auth. Got: %s", receivedAuths[1])
	}
}

func TestBufferManager_Add_WithAuthentication(t *testing.T) {
	var receivedAuth string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		// Read and discard body
		_, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errors":false}`))
	}))
	defer ts.Close()

	cfg := &config.Config{
		Elasticsearch: config.ElasticsearchConfig{
			URL: ts.URL,
		},
		Buffer: config.BufferConfig{
			FlushInterval: 10 * time.Second,
			MaxBatchSize:  50,
			MaxBufferSize: 10240,
		},
		Retry: config.RetryConfig{
			Attempts:   1,
			BackoffMin: 100 * time.Millisecond,
		},
	}

	manager := NewManager(cfg, logger.New(nil, true), testMetrics)

	authHeaders := make(http.Header)
	authHeaders.Set("Authorization", "Basic dXNlcjpwYXNz") // user:pass in base64

	// Add data that exceeds batch size to trigger flush
	err := manager.Add("/_bulk", make([]byte, 60), authHeaders)
	if err != nil {
		t.Fatalf("Add() failed: %v", err)
	}

	// Wait for flush
	time.Sleep(300 * time.Millisecond)

	// Verify auth was forwarded
	if receivedAuth != "Basic dXNlcjpwYXNz" {
		t.Errorf("Auth header not forwarded. Got: %s, Want: Basic dXNlcjpwYXNz", receivedAuth)
	}
}

func TestFindFailedItemIndices_NoErrors(t *testing.T) {
	body := []byte(`{"errors":false,"items":[{"index":{"_index":"test","_id":"1","status":201}}]}`)
	got := findFailedItemIndices(body)
	if got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestFindFailedItemIndices_WithErrors(t *testing.T) {
	body := []byte(`{"errors":true,"items":[` +
		`{"index":{"_index":"test","_id":"1","status":201}},` +
		`{"index":{"_index":"test","_id":"2","status":429}},` +
		`{"create":{"_index":"test","_id":"3","status":201}},` +
		`{"index":{"_index":"test","_id":"4","status":500}}` +
		`]}`)
	got := findFailedItemIndices(body)
	if len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Errorf("expected [1 3], got %v", got)
	}
}

func TestFindFailedItemIndices_DeleteFailure(t *testing.T) {
	body := []byte(`{"errors":true,"items":[` +
		`{"delete":{"_index":"test","_id":"1","status":404}},` +
		`{"index":{"_index":"test","_id":"2","status":201}}` +
		`]}`)
	got := findFailedItemIndices(body)
	if len(got) != 1 || got[0] != 0 {
		t.Errorf("expected [0], got %v", got)
	}
}

func TestFindFailedItemIndices_InvalidJSON(t *testing.T) {
	body := []byte(`not json`)
	got := findFailedItemIndices(body)
	if got != nil {
		t.Errorf("expected nil for invalid JSON, got %v", got)
	}
}

func TestExtractFailedPairs_IndexOperations(t *testing.T) {
	payload := []byte(
		"{\"index\":{\"_id\":\"1\"}}\n{\"field\":\"v1\"}\n" +
			"{\"index\":{\"_id\":\"2\"}}\n{\"field\":\"v2\"}\n" +
			"{\"index\":{\"_id\":\"3\"}}\n{\"field\":\"v3\"}\n",
	)

	// Extract operations at index 0 and 2
	got := extractFailedPairs(payload, []int{0, 2})
	want := "{\"index\":{\"_id\":\"1\"}}\n{\"field\":\"v1\"}\n" +
		"{\"index\":{\"_id\":\"3\"}}\n{\"field\":\"v3\"}\n"

	if string(got) != want {
		t.Errorf("extractFailedPairs:\ngot:  %q\nwant: %q", string(got), want)
	}
}

func TestExtractFailedPairs_WithDelete(t *testing.T) {
	payload := []byte(
		"{\"index\":{\"_id\":\"1\"}}\n{\"field\":\"v1\"}\n" +
			"{\"delete\":{\"_id\":\"2\"}}\n" +
			"{\"index\":{\"_id\":\"3\"}}\n{\"field\":\"v3\"}\n",
	)

	// Extract the delete at index 1
	got := extractFailedPairs(payload, []int{1})
	want := "{\"delete\":{\"_id\":\"2\"}}\n"

	if string(got) != want {
		t.Errorf("extractFailedPairs with delete:\ngot:  %q\nwant: %q", string(got), want)
	}
}

func TestExtractFailedPairs_Empty(t *testing.T) {
	payload := []byte("{\"index\":{}}\n{\"doc\":1}\n")
	got := extractFailedPairs(payload, nil)
	if got != nil {
		t.Errorf("expected nil for empty failedIndices, got %q", string(got))
	}
}

func TestCountNDJSONOperations(t *testing.T) {
	tests := []struct {
		name    string
		data    string
		wantOps int
	}{
		{"two index ops", "{\"index\":{}}\n{\"d\":1}\n{\"index\":{}}\n{\"d\":2}\n", 2},
		{"index + delete", "{\"index\":{}}\n{\"d\":1}\n{\"delete\":{}}\n", 2},
		{"empty", "", 0},
		{"single delete", "{\"delete\":{}}\n", 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := countNDJSONOperations([]byte(tt.data))
			if got != tt.wantOps {
				t.Errorf("countNDJSONOperations(%q) = %d, want %d", tt.data, got, tt.wantOps)
			}
		})
	}
}

func TestIndexBuffer_SendWithRetry_PartialFailureRetried(t *testing.T) {
	attemptCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attemptCount++
		if attemptCount == 1 {
			// First attempt: item 1 fails with 429
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"errors":true,"items":[` +
				`{"index":{"_index":"t","_id":"1","status":201}},` +
				`{"index":{"_index":"t","_id":"2","status":429}},` +
				`{"index":{"_index":"t","_id":"3","status":201}}]}`,
			))
		} else {
			// Retry succeeds
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"errors":false,"items":[{"index":{"_index":"t","_id":"2","status":201}}]}`))
		}
	}))
	defer ts.Close()

	cfg := &config.Config{
		Elasticsearch: config.ElasticsearchConfig{URL: ts.URL},
		Retry:         config.RetryConfig{Attempts: 3, BackoffMin: 10 * time.Millisecond},
	}
	buf := &IndexBuffer{
		indexPath: "/_bulk",
		config:    cfg,
		logger:    logger.New(nil, true),
		metrics:   testMetrics,
		esClient:  newESHTTPClient(),
	}

	data := []byte(
		"{\"index\":{\"_id\":\"1\"}}\n{\"v\":1}\n" +
			"{\"index\":{\"_id\":\"2\"}}\n{\"v\":2}\n" +
			"{\"index\":{\"_id\":\"3\"}}\n{\"v\":3}\n",
	)

	attemptType, failedData, err := buf.sendWithRetry(data)
	if err != nil {
		t.Fatalf("expected success after partial retry, got: %v", err)
	}
	if failedData != nil {
		t.Fatalf("expected nil failedData, got %d bytes", len(failedData))
	}
	if attemptType != "retry" {
		t.Errorf("expected attempt_type 'retry', got '%s'", attemptType)
	}
	if attemptCount != 2 {
		t.Errorf("expected 2 HTTP requests, got %d", attemptCount)
	}
}

func TestIndexBuffer_SendWithRetry_PartialFailureExhausted(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Always return partial failure for item 0
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errors":true,"items":[{"index":{"_index":"t","_id":"1","status":429}}]}`))
	}))
	defer ts.Close()

	cfg := &config.Config{
		Elasticsearch: config.ElasticsearchConfig{URL: ts.URL},
		Retry:         config.RetryConfig{Attempts: 1, BackoffMin: 10 * time.Millisecond},
	}
	buf := &IndexBuffer{
		indexPath: "/_bulk",
		config:    cfg,
		logger:    logger.New(nil, true),
		metrics:   testMetrics,
		esClient:  newESHTTPClient(),
	}

	data := []byte("{\"index\":{\"_id\":\"1\"}}\n{\"v\":1}\n")

	attemptType, failedData, err := buf.sendWithRetry(data)
	if err == nil {
		t.Fatal("expected error when retries exhausted")
	}
	if attemptType != "partial_success" {
		t.Errorf("expected 'partial_success', got '%s'", attemptType)
	}
	if failedData == nil {
		t.Fatal("expected non-nil failedData for partial failure")
	}
	if string(failedData) != string(data) {
		t.Errorf("failedData should contain the failed item:\ngot:  %q\nwant: %q", string(failedData), string(data))
	}
}
