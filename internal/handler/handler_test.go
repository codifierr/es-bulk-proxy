package handler

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/codifierr/es-bulk-proxy/internal/buffer"
	"github.com/codifierr/es-bulk-proxy/internal/config"
	"github.com/codifierr/es-bulk-proxy/internal/logger"
	"github.com/codifierr/es-bulk-proxy/internal/metrics"
)

// Use a shared metrics instance to avoid Prometheus registration conflicts
var testMetrics = metrics.New()

func TestNew(t *testing.T) {
	cfg := &config.Config{
		Elasticsearch: config.ElasticsearchConfig{
			URL:            "http://localhost:9200",
			RequestTimeout: 30 * time.Second,
		},
		Buffer: config.BufferConfig{
			FlushInterval: 1 * time.Second,
			MaxBatchSize:  1024,
			MaxBufferSize: 10240,
		},
	}
	log := logger.New(nil, true)
	m := testMetrics
	bb := buffer.NewManager(cfg, log, m)

	handler := New(cfg, bb, log, m)

	if handler == nil {
		t.Fatal("New() returned nil")
	}
	if handler.proxy == nil {
		t.Error("proxy not initialized")
	}
	if handler.bulkBuffer == nil {
		t.Error("bulkBuffer not initialized")
	}
}

func TestProxyHandler_ServeHTTP_BulkRequest(t *testing.T) {
	// Create a mock Elasticsearch server
	esCalled := false
	esServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		esCalled = true
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errors":false}`))
	}))
	defer esServer.Close()

	cfg := &config.Config{
		Elasticsearch: config.ElasticsearchConfig{
			URL:            esServer.URL,
			RequestTimeout: 30 * time.Second,
		},
		Buffer: config.BufferConfig{
			FlushInterval: 10 * time.Second,
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
	bb := buffer.NewManager(cfg, log, m)
	handler := New(cfg, bb, log, m)

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
		wantBody   string
	}{
		{
			name:       "bulk request to /_bulk",
			method:     "POST",
			path:       "/_bulk",
			body:       `{"index":{"_index":"test"}}` + "\n" + `{"field":"value"}` + "\n",
			wantStatus: http.StatusOK,
			wantBody:   `{"errors":false}`,
		},
		{
			name:       "bulk request to index-specific endpoint",
			method:     "POST",
			path:       "/myindex/_bulk",
			body:       `{"index":{}}` + "\n" + `{"field":"value"}` + "\n",
			wantStatus: http.StatusOK,
			wantBody:   `{"errors":false}`,
		},
		{
			name:       "PUT bulk request",
			method:     "PUT",
			path:       "/_bulk",
			body:       `{"index":{}}` + "\n" + `{"field":"value"}` + "\n",
			wantStatus: http.StatusOK,
			wantBody:   `{"errors":false}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			esCalled = false // Reset
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-ndjson")
			w := httptest.NewRecorder()

			handler.ServeHTTP(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("Status = %d, want %d", w.Code, tt.wantStatus)
			}

			body := w.Body.String()
			if body != tt.wantBody {
				t.Errorf("Body = %s, want %s", body, tt.wantBody)
			}

			// Bulk requests should NOT immediately proxy to ES
			if esCalled {
				t.Error("Bulk request should not immediately call ES")
			}
		})
	}
}

func TestProxyHandler_ServeHTTP_ProxyRequests(t *testing.T) {
	// Create a mock Elasticsearch server
	var lastPath string
	var lastMethod string
	esServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastPath = r.URL.Path
		lastMethod = r.Method
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"success"}`))
	}))
	defer esServer.Close()

	cfg := &config.Config{
		Elasticsearch: config.ElasticsearchConfig{
			URL:            esServer.URL,
			RequestTimeout: 30 * time.Second,
		},
		Buffer: config.BufferConfig{
			FlushInterval: 10 * time.Second,
			MaxBatchSize:  1024,
			MaxBufferSize: 10240,
		},
	}
	log := logger.New(nil, true)
	m := testMetrics
	bb := buffer.NewManager(cfg, log, m)
	handler := New(cfg, bb, log, m)

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "GET search request",
			method:     "GET",
			path:       "/_search",
			wantStatus: http.StatusOK,
		},
		{
			name:       "POST search request",
			method:     "POST",
			path:       "/index/_search",
			wantStatus: http.StatusOK,
		},
		{
			name:       "GET cluster health",
			method:     "GET",
			path:       "/_cluster/health",
			wantStatus: http.StatusOK,
		},
		{
			name:       "PUT index creation",
			method:     "PUT",
			path:       "/myindex",
			wantStatus: http.StatusOK,
		},
		{
			name:       "DELETE index",
			method:     "DELETE",
			path:       "/myindex",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lastPath = ""
			lastMethod = ""

			req := httptest.NewRequest(tt.method, tt.path, nil)
			w := httptest.NewRecorder()

			handler.ServeHTTP(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("Status = %d, want %d", w.Code, tt.wantStatus)
			}

			// Verify request was proxied
			if lastPath != tt.path {
				t.Errorf("Request proxied to path %s, want %s", lastPath, tt.path)
			}
			if lastMethod != tt.method {
				t.Errorf("Request proxied with method %s, want %s", lastMethod, tt.method)
			}
		})
	}
}

func TestProxyHandler_HandleBulk_BufferFull(t *testing.T) {
	esServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer esServer.Close()

	cfg := &config.Config{
		Elasticsearch: config.ElasticsearchConfig{
			URL:            esServer.URL,
			RequestTimeout: 30 * time.Second,
		},
		Buffer: config.BufferConfig{
			FlushInterval: 10 * time.Second,
			MaxBatchSize:  50,
			MaxBufferSize: 50, // Very small buffer
		},
	}
	log := logger.New(nil, true)
	m := testMetrics
	bb := buffer.NewManager(cfg, log, m)
	handler := New(cfg, bb, log, m)

	// First request should succeed
	req1 := httptest.NewRequest("POST", "/_bulk", strings.NewReader("small\n"))
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, req1)
	if w1.Code != http.StatusOK {
		t.Errorf("First request status = %d, want %d", w1.Code, http.StatusOK)
	}

	// Second large request should fail with 429
	largeData := strings.Repeat("x", 100)
	req2 := httptest.NewRequest("POST", "/_bulk", strings.NewReader(largeData))
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)
	if w2.Code != http.StatusTooManyRequests {
		t.Errorf("Second request status = %d, want %d", w2.Code, http.StatusTooManyRequests)
	}
}

func TestProxyHandler_HandleBulk_ReadError(t *testing.T) {
	cfg := &config.Config{
		Elasticsearch: config.ElasticsearchConfig{
			URL:            "http://localhost:9200",
			RequestTimeout: 30 * time.Second,
		},
		Buffer: config.BufferConfig{
			FlushInterval: 10 * time.Second,
			MaxBatchSize:  1024,
			MaxBufferSize: 10240,
		},
	}
	log := logger.New(nil, true)
	m := testMetrics
	bb := buffer.NewManager(cfg, log, m)
	handler := New(cfg, bb, log, m)

	// Create a request with a body that will error on read
	req := httptest.NewRequest("POST", "/_bulk", &errorReader{})
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestProxyHandler_ClassifyRequest(t *testing.T) {
	cfg := &config.Config{
		Elasticsearch: config.ElasticsearchConfig{
			URL:            "http://localhost:9200",
			RequestTimeout: 30 * time.Second,
		},
	}
	log := logger.New(nil, true)
	m := testMetrics
	bb := buffer.NewManager(cfg, log, m)
	handler := New(cfg, bb, log, m)

	tests := []struct {
		name     string
		method   string
		path     string
		wantType string
	}{
		{
			name:     "search with GET",
			method:   "GET",
			path:     "/_search",
			wantType: "read", // GET requests are classified as "read"
		},
		{
			name:     "search with POST",
			method:   "POST",
			path:     "/index/_search",
			wantType: "search",
		},
		{
			name:     "count request",
			method:   "POST",
			path:     "/_count",
			wantType: "search",
		},
		{
			name:     "refresh operation",
			method:   "POST",
			path:     "/_refresh",
			wantType: "maintenance",
		},
		{
			name:     "flush operation",
			method:   "POST",
			path:     "/index/_flush",
			wantType: "maintenance",
		},
		{
			name:     "forcemerge operation",
			method:   "POST",
			path:     "/_forcemerge",
			wantType: "maintenance",
		},
		{
			name:     "cluster health",
			method:   "GET",
			path:     "/_cluster/health",
			wantType: "read",
		},
		{
			name:     "index stats",
			method:   "GET",
			path:     "/index/_stats",
			wantType: "read",
		},
		{
			name:     "document get",
			method:   "GET",
			path:     "/index/_doc/1",
			wantType: "read",
		},
		{
			name:     "document write",
			method:   "POST",
			path:     "/index/_doc",
			wantType: "write",
		},
		{
			name:     "document update",
			method:   "PUT",
			path:     "/index/_doc/1",
			wantType: "write",
		},
		{
			name:     "document delete",
			method:   "DELETE",
			path:     "/index/_doc/1",
			wantType: "delete",
		},
		{
			name:     "index delete",
			method:   "DELETE",
			path:     "/index",
			wantType: "delete",
		},
		{
			name:     "other request",
			method:   "GET",
			path:     "/",
			wantType: "read", // GET requests are classified as "read"
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			got := handler.classifyRequest(req)
			if got != tt.wantType {
				t.Errorf("classifyRequest() = %s, want %s", got, tt.wantType)
			}
		})
	}
}

func TestHealth(t *testing.T) {
	handler := Health()
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
	}

	body := w.Body.String()
	if !strings.Contains(body, "healthy") {
		t.Errorf("Body should contain 'healthy', got %s", body)
	}
}

func TestReady(t *testing.T) {
	handler := Ready()
	req := httptest.NewRequest("GET", "/ready", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
	}

	body := w.Body.String()
	if !strings.Contains(body, "ready") {
		t.Errorf("Body should contain 'ready', got %s", body)
	}
}

func TestProxyHandler_HandleBulk_NewlineAppending(t *testing.T) {
	esServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer esServer.Close()

	cfg := &config.Config{
		Elasticsearch: config.ElasticsearchConfig{
			URL:            esServer.URL,
			RequestTimeout: 30 * time.Second,
		},
		Buffer: config.BufferConfig{
			FlushInterval: 10 * time.Second,
			MaxBatchSize:  1024,
			MaxBufferSize: 10240,
		},
	}
	log := logger.New(nil, true)
	m := testMetrics
	bb := buffer.NewManager(cfg, log, m)
	handler := New(cfg, bb, log, m)

	// Test data without trailing newline
	reqWithoutNewline := httptest.NewRequest("POST", "/_bulk", strings.NewReader("data"))
	wWithoutNewline := httptest.NewRecorder()
	handler.ServeHTTP(wWithoutNewline, reqWithoutNewline)

	if wWithoutNewline.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", wWithoutNewline.Code, http.StatusOK)
	}

	// Test data with trailing newline
	reqWithNewline := httptest.NewRequest("POST", "/_bulk", strings.NewReader("data\n"))
	wWithNewline := httptest.NewRecorder()
	handler.ServeHTTP(wWithNewline, reqWithNewline)

	if wWithNewline.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", wWithNewline.Code, http.StatusOK)
	}
}

func TestProxyHandler_HandleBulk_EmptyBody(t *testing.T) {
	cfg := &config.Config{
		Elasticsearch: config.ElasticsearchConfig{
			URL:            "http://localhost:9200",
			RequestTimeout: 30 * time.Second,
		},
		Buffer: config.BufferConfig{
			FlushInterval: 10 * time.Second,
			MaxBatchSize:  1024,
			MaxBufferSize: 10240,
		},
	}
	log := logger.New(nil, true)
	m := testMetrics
	bb := buffer.NewManager(cfg, log, m)
	handler := New(cfg, bb, log, m)

	req := httptest.NewRequest("POST", "/_bulk", bytes.NewReader([]byte{}))
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	// Empty body should still return OK (it's valid, just empty)
	if w.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
	}
}

// errorReader is a helper type that always returns an error on Read
type errorReader struct{}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, io.ErrUnexpectedEOF
}
