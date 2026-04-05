package handler

import (
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/codifierr/go-scratchpad/es-bulk-proxy/internal/buffer"
	"github.com/codifierr/go-scratchpad/es-bulk-proxy/internal/config"
	"github.com/codifierr/go-scratchpad/es-bulk-proxy/internal/logger"
	"github.com/codifierr/go-scratchpad/es-bulk-proxy/internal/metrics"
)

const (
	contentTypeHeader = "Content-Type"
	contentTypeJSON   = "application/json"
	contentTypeNDJSON = "application/x-ndjson"
)

// ProxyHandler handles routing between bulk and proxy requests
type ProxyHandler struct {
	bulkBuffer *buffer.BufferManager
	proxy      *httputil.ReverseProxy
	config     *config.Config
	logger     *logger.Logger
	metrics    *metrics.Metrics
}

// New creates a new proxy handler
func New(cfg *config.Config, bb *buffer.BufferManager, log *logger.Logger, m *metrics.Metrics) *ProxyHandler {
	esURL, _ := url.Parse(cfg.Elasticsearch.URL)

	proxy := httputil.NewSingleHostReverseProxy(esURL)

	// Customize director to preserve headers and path
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = esURL.Host
	}

	return &ProxyHandler{
		bulkBuffer: bb,
		proxy:      proxy,
		config:     cfg,
		logger:     log,
		metrics:    m,
	}
}

// ServeHTTP handles incoming requests
func (ph *ProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// Log incoming requests for debugging
	ph.logger.DebugFields("incoming request", map[string]interface{}{
		"method": r.Method,
		"path":   r.URL.Path,
		"query":  r.URL.RawQuery,
	})

	// Handle bulk requests - match both /_bulk and /index_name/_bulk patterns
	// Zenarmor may use index-specific bulk endpoints like /my-index/_bulk
	isBulkRequest := (r.Method == "POST" || r.Method == "PUT") &&
		(r.URL.Path == "/_bulk" || strings.HasSuffix(r.URL.Path, "/_bulk"))

	if isBulkRequest {
		ph.logger.DebugFields("handling bulk request", map[string]interface{}{
			"path":   r.URL.Path,
			"method": r.Method,
		})
		ph.metrics.RequestsTotal.WithLabelValues("bulk", r.Method).Inc()
		ph.handleBulk(w, r)
		ph.metrics.ProxyLatency.WithLabelValues("bulk", r.Method).Observe(time.Since(start).Seconds())
		return
	}

	// Proxy all other requests - distinguish between read (GET/HEAD) and write operations
	requestType := ph.classifyRequest(r)
	ph.metrics.RequestsTotal.WithLabelValues(requestType, r.Method).Inc()
	ph.proxy.ServeHTTP(w, r)
	ph.metrics.ProxyLatency.WithLabelValues(requestType, r.Method).Observe(time.Since(start).Seconds())
}

// handleBulk handles /_bulk requests
func (ph *ProxyHandler) handleBulk(w http.ResponseWriter, r *http.Request) {
	// Read body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		ph.logger.ErrorFields("failed to read bulk body", map[string]interface{}{
			"error": err.Error(),
		})
		http.Error(w, "Failed to read request", http.StatusBadRequest)
		return
	}
	defer func() {
		err := r.Body.Close()
		if err != nil {
			ph.logger.ErrorFields("failed to close request body", map[string]interface{}{
				"error": err.Error(),
			})
		}
	}()

	// Ensure body ends with newline (NDJSON requirement)
	if len(body) > 0 && body[len(body)-1] != '\n' {
		body = append(body, '\n')
	}

	// Add to buffer with index path to preserve ES context
	err = ph.bulkBuffer.Add(r.URL.Path, body)
	if err != nil {
		ph.logger.ErrorFields("failed to add to buffer", map[string]interface{}{
			"error":     err.Error(),
			"size":      len(body),
			"indexPath": r.URL.Path,
		})
		http.Error(w, "Buffer full", http.StatusTooManyRequests)
		return
	}

	// Return immediate success
	w.Header().Set(contentTypeHeader, contentTypeJSON)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"errors":false}`))
}

// classifyRequest determines the type of non-bulk request
func (ph *ProxyHandler) classifyRequest(r *http.Request) string {
	path := r.URL.Path

	// Check for common read-only operations that use POST
	if r.Method == "POST" {
		// Search operations (POST is used for complex queries)
		if strings.Contains(path, "/_search") {
			return "search"
		}
		// Count operations
		if strings.Contains(path, "/_count") {
			return "search"
		}
		// Refresh, flush, and other index maintenance operations
		if strings.Contains(path, "/_refresh") ||
			strings.Contains(path, "/_flush") ||
			strings.Contains(path, "/_forcemerge") {
			return "maintenance"
		}
	}

	switch r.Method {
	case "GET", "HEAD":
		return "read"
	case "POST", "PUT":
		return "write"
	case "DELETE":
		return "delete"
	default:
		return "other"
	}
}

// Health returns a health check handler
func Health() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(contentTypeHeader, contentTypeJSON)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy"}`))
	}
}

// Ready returns a readiness check handler
func Ready() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(contentTypeHeader, contentTypeJSON)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ready"}`))
	}
}
