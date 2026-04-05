package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/codifierr/go-scratchpad/es-bulk-proxy/internal/buffer"
	"github.com/codifierr/go-scratchpad/es-bulk-proxy/internal/config"
	"github.com/codifierr/go-scratchpad/es-bulk-proxy/internal/handler"
	"github.com/codifierr/go-scratchpad/es-bulk-proxy/internal/logger"
	"github.com/codifierr/go-scratchpad/es-bulk-proxy/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const version = "2.0.0"

func main() {
	// Initialize logger
	log := logger.New(isDevelopment())
	log.SetGlobal()

	log.InfoFields("starting elasticsearch proxy", map[string]interface{}{
		"version": version,
	})

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.FatalFields("failed to load configuration", map[string]interface{}{
			"error": err.Error(),
		})
	}

	log.InfoFields("configuration loaded", map[string]interface{}{
		"es_url":          cfg.Elasticsearch.URL,
		"flush_interval":  cfg.Buffer.FlushInterval.String(),
		"max_batch_size":  cfg.Buffer.MaxBatchSize,
		"max_buffer_size": cfg.Buffer.MaxBufferSize,
		"port":            cfg.Server.Port,
	})

	// Initialize metrics
	m := metrics.New()

	// Create bulk buffer manager (per-index buffers)
	bulkBuffer := buffer.NewManager(cfg, log, m)

	// Create proxy handler
	proxyHandler := handler.New(cfg, bulkBuffer, log, m)

	// Setup HTTP server
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", handler.Health())
	mux.HandleFunc("/ready", handler.Ready())
	mux.Handle("/", proxyHandler)

	server := &http.Server{
		Addr:         ":" + cfg.Server.Port,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Start server in goroutine
	go func() {
		log.InfoFields("server listening", map[string]any{
			"port": cfg.Server.Port,
		})
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.FatalFields("server failed to start", map[string]any{
				"error": err.Error(),
			})
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.InfoFields("shutting down server", map[string]any{})

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bulkBuffer.Shutdown()

	if err := server.Shutdown(ctx); err != nil {
		log.ErrorFields("server shutdown error", map[string]any{
			"error": err.Error(),
		})
	}

	log.InfoFields("server stopped", map[string]interface{}{})
}

// isDevelopment checks if running in development mode
func isDevelopment() bool {
	env := os.Getenv("ENVIRONMENT")
	return env == "development" || env == "dev" || env == ""
}
