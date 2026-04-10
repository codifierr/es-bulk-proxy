package buffer

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/codifierr/go-scratchpad/es-bulk-proxy/internal/config"
	"github.com/codifierr/go-scratchpad/es-bulk-proxy/internal/logger"
	"github.com/codifierr/go-scratchpad/es-bulk-proxy/internal/metrics"
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
	log := logger.New(true)
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
	manager := NewManager(cfg, logger.New(true), testMetrics)

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
			log := logger.New(true)
			m := testMetrics
			manager := NewManager(cfg, log, m)

			err := manager.Add(tt.indexPath, tt.data)
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
	log := logger.New(true)
	m := testMetrics
	manager := NewManager(cfg, log, m)

	// First add should succeed
	smallData := []byte("small")
	err := manager.Add("/_bulk", smallData)
	if err != nil {
		t.Errorf("First Add() failed: %v", err)
	}

	// Second add should exceed buffer
	largeData := make([]byte, 200)
	err = manager.Add("/_bulk", largeData)
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
	log := logger.New(true)
	m := testMetrics
	manager := NewManager(cfg, log, m)

	// Add to different indices
	indices := []string{"/_bulk", "/index1/_bulk", "/index2/_bulk"}
	for _, idx := range indices {
		err := manager.Add(idx, []byte("data\n"))
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
	log := logger.New(true)
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
				err := manager.Add("/_bulk", data)
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
	log := logger.New(true)
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
			err := buf.Add(tt.data)
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
	log := logger.New(true)
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
	err := buf.Add(largeData)
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
	log := logger.New(true)
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
	err := buf.Add([]byte("small data\n"))
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
	log := logger.New(true)
	m := testMetrics

	buf := &IndexBuffer{
		indexPath: "/_bulk",
		config:    cfg,
		logger:    log,
		metrics:   m,
		esClient:  newESHTTPClient(),
	}

	data := []byte(`{"index":{}}\n{"field":"value"}\n`)
	err := buf.sendWithRetry(data)
	if err != nil {
		t.Errorf("sendWithRetry() failed: %v", err)
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
	log := logger.New(true)
	m := testMetrics

	buf := &IndexBuffer{
		indexPath: "/_bulk",
		config:    cfg,
		logger:    log,
		metrics:   m,
		esClient:  newESHTTPClient(),
	}

	data := []byte(`{"index":{}}\n{"field":"value"}\n`)
	err := buf.sendWithRetry(data)
	if err == nil {
		t.Error("sendWithRetry() should return error on failures")
	}

	// Should retry: initial attempt + 2 retries = 3 total
	expectedAttempts := cfg.Retry.Attempts + 1
	if attemptCount != expectedAttempts {
		t.Errorf("Expected %d attempts, got %d", expectedAttempts, attemptCount)
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
	log := logger.New(true)
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
	log := logger.New(true)
	m := testMetrics
	manager := NewManager(cfg, log, m)

	// Add data to multiple buffers
	_ = manager.Add("/_bulk", []byte("data1\n"))
	_ = manager.Add("/index1/_bulk", []byte("data2\n"))

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
	log := logger.New(true)
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
