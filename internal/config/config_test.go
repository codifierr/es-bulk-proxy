package config

import (
	"os"
	"testing"
	"time"
)

func TestLoad_Defaults(t *testing.T) {
	// Clear any existing env vars
	os.Clearenv()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Test default values
	if cfg.Server.Port != "8080" {
		t.Errorf("Server.Port = %s, want 8080", cfg.Server.Port)
	}

	if cfg.Elasticsearch.URL != "http://localhost:9200" {
		t.Errorf("Elasticsearch.URL = %s, want http://localhost:9200", cfg.Elasticsearch.URL)
	}

	if cfg.Buffer.FlushInterval != 30*time.Second {
		t.Errorf("Buffer.FlushInterval = %v, want 30s", cfg.Buffer.FlushInterval)
	}

	if cfg.Buffer.MaxBatchSize != 5242880 {
		t.Errorf("Buffer.MaxBatchSize = %d, want 5242880", cfg.Buffer.MaxBatchSize)
	}

	if cfg.Buffer.MaxBufferSize != 52428800 {
		t.Errorf("Buffer.MaxBufferSize = %d, want 52428800", cfg.Buffer.MaxBufferSize)
	}

	if cfg.Retry.Attempts != 3 {
		t.Errorf("Retry.Attempts = %d, want 3", cfg.Retry.Attempts)
	}

	if cfg.Retry.BackoffMin != 100*time.Millisecond {
		t.Errorf("Retry.BackoffMin = %v, want 100ms", cfg.Retry.BackoffMin)
	}
}

func TestLoad_FromEnvVars(t *testing.T) {
	// Set environment variables
	os.Clearenv()
	_ = os.Setenv("PORT", "9090")
	_ = os.Setenv("ES_URL", "http://es.example.com:9200")
	_ = os.Setenv("FLUSH_INTERVAL", "5s")
	_ = os.Setenv("MAX_BATCH_SIZE", "1048576")
	_ = os.Setenv("MAX_BUFFER_SIZE", "10485760")
	_ = os.Setenv("RETRY_ATTEMPTS", "5")
	_ = os.Setenv("RETRY_BACKOFF_MIN", "200ms")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Test env var values
	if cfg.Server.Port != "9090" {
		t.Errorf("Server.Port = %s, want 9090", cfg.Server.Port)
	}

	if cfg.Elasticsearch.URL != "http://es.example.com:9200" {
		t.Errorf("Elasticsearch.URL = %s, want http://es.example.com:9200", cfg.Elasticsearch.URL)
	}

	if cfg.Buffer.FlushInterval != 5*time.Second {
		t.Errorf("Buffer.FlushInterval = %v, want 5s", cfg.Buffer.FlushInterval)
	}

	if cfg.Buffer.MaxBatchSize != 1048576 {
		t.Errorf("Buffer.MaxBatchSize = %d, want 1048576", cfg.Buffer.MaxBatchSize)
	}

	if cfg.Buffer.MaxBufferSize != 10485760 {
		t.Errorf("Buffer.MaxBufferSize = %d, want 10485760", cfg.Buffer.MaxBufferSize)
	}

	if cfg.Retry.Attempts != 5 {
		t.Errorf("Retry.Attempts = %d, want 5", cfg.Retry.Attempts)
	}

	if cfg.Retry.BackoffMin != 200*time.Millisecond {
		t.Errorf("Retry.BackoffMin = %v, want 200ms", cfg.Retry.BackoffMin)
	}

	// Cleanup
	os.Clearenv()
}

func TestLoad_PartialEnvVars(t *testing.T) {
	// Set only some environment variables
	os.Clearenv()
	_ = os.Setenv("PORT", "3000")
	_ = os.Setenv("ES_URL", "http://custom-es:9200")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Test overridden values
	if cfg.Server.Port != "3000" {
		t.Errorf("Server.Port = %s, want 3000", cfg.Server.Port)
	}

	if cfg.Elasticsearch.URL != "http://custom-es:9200" {
		t.Errorf("Elasticsearch.URL = %s, want http://custom-es:9200", cfg.Elasticsearch.URL)
	}

	// Test default values (not overridden)
	if cfg.Buffer.FlushInterval != 30*time.Second {
		t.Errorf("Buffer.FlushInterval = %v, want 30s (default)", cfg.Buffer.FlushInterval)
	}

	if cfg.Retry.Attempts != 3 {
		t.Errorf("Retry.Attempts = %d, want 3 (default)", cfg.Retry.Attempts)
	}

	// Cleanup
	os.Clearenv()
}

func TestLoad_InvalidDuration(t *testing.T) {
	os.Clearenv()
	_ = os.Setenv("FLUSH_INTERVAL", "invalid-duration")

	_, err := Load()
	if err == nil {
		t.Error("Load() should fail with invalid duration")
	}

	os.Clearenv()
}

func TestLoad_ConfigFileNotFound(t *testing.T) {
	// Should not error when config file is not found
	// It should use defaults and env vars
	os.Clearenv()

	cfg, err := Load()
	if err != nil {
		t.Errorf("Load() should not fail when config file is missing, got: %v", err)
	}

	if cfg == nil {
		t.Error("Load() should return config even without file")
	}
}

func TestConfig_Structure(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{
			Port: "8080",
		},
		Elasticsearch: ElasticsearchConfig{
			URL: "http://localhost:9200",
		},
		Buffer: BufferConfig{
			FlushInterval: 30 * time.Second,
			MaxBatchSize:  5 * 1024 * 1024,
			MaxBufferSize: 50 * 1024 * 1024,
		},
		Retry: RetryConfig{
			Attempts:   3,
			BackoffMin: 100 * time.Millisecond,
		},
	}

	if cfg.Server.Port == "" {
		t.Error("Server.Port should not be empty")
	}

	if cfg.Elasticsearch.URL == "" {
		t.Error("Elasticsearch.URL should not be empty")
	}

	if cfg.Buffer.FlushInterval == 0 {
		t.Error("Buffer.FlushInterval should not be zero")
	}

	if cfg.Buffer.MaxBatchSize == 0 {
		t.Error("Buffer.MaxBatchSize should not be zero")
	}

	if cfg.Buffer.MaxBufferSize == 0 {
		t.Error("Buffer.MaxBufferSize should not be zero")
	}

	if cfg.Retry.Attempts == 0 {
		t.Error("Retry.Attempts should not be zero")
	}

	if cfg.Retry.BackoffMin == 0 {
		t.Error("Retry.BackoffMin should not be zero")
	}
}

func TestLoad_NumericEnvVars(t *testing.T) {
	os.Clearenv()
	_ = os.Setenv("MAX_BATCH_SIZE", "2097152")   // 2MB
	_ = os.Setenv("MAX_BUFFER_SIZE", "20971520") // 20MB
	_ = os.Setenv("RETRY_ATTEMPTS", "10")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.Buffer.MaxBatchSize != 2097152 {
		t.Errorf("Buffer.MaxBatchSize = %d, want 2097152", cfg.Buffer.MaxBatchSize)
	}

	if cfg.Buffer.MaxBufferSize != 20971520 {
		t.Errorf("Buffer.MaxBufferSize = %d, want 20971520", cfg.Buffer.MaxBufferSize)
	}

	if cfg.Retry.Attempts != 10 {
		t.Errorf("Retry.Attempts = %d, want 10", cfg.Retry.Attempts)
	}

	os.Clearenv()
}

func TestLoad_DurationParsing(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		expected time.Duration
		wantErr  bool
	}{
		{
			name:     "seconds",
			envValue: "45s",
			expected: 45 * time.Second,
			wantErr:  false,
		},
		{
			name:     "minutes",
			envValue: "2m",
			expected: 2 * time.Minute,
			wantErr:  false,
		},
		{
			name:     "milliseconds",
			envValue: "500ms",
			expected: 500 * time.Millisecond,
			wantErr:  false,
		},
		{
			name:     "combined",
			envValue: "1m30s",
			expected: 90 * time.Second,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Clearenv()
			_ = os.Setenv("FLUSH_INTERVAL", tt.envValue)

			cfg, err := Load()
			if (err != nil) != tt.wantErr {
				t.Errorf("Load() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && cfg.Buffer.FlushInterval != tt.expected {
				t.Errorf("Buffer.FlushInterval = %v, want %v", cfg.Buffer.FlushInterval, tt.expected)
			}

			os.Clearenv()
		})
	}
}

func TestLoad_ZeroValues(t *testing.T) {
	os.Clearenv()
	_ = os.Setenv("MAX_BATCH_SIZE", "0")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Zero values should be loaded as-is (validation is separate concern)
	if cfg.Buffer.MaxBatchSize != 0 {
		t.Errorf("Buffer.MaxBatchSize = %d, want 0", cfg.Buffer.MaxBatchSize)
	}

	os.Clearenv()
}

func TestServerConfig(t *testing.T) {
	cfg := ServerConfig{
		Port: "8080",
	}

	if cfg.Port != "8080" {
		t.Errorf("Port = %s, want 8080", cfg.Port)
	}
}

func TestElasticsearchConfig(t *testing.T) {
	cfg := ElasticsearchConfig{
		URL: "http://es:9200",
	}

	if cfg.URL != "http://es:9200" {
		t.Errorf("URL = %s, want http://es:9200", cfg.URL)
	}
}

func TestBufferConfig(t *testing.T) {
	cfg := BufferConfig{
		FlushInterval: 5 * time.Second,
		MaxBatchSize:  1024,
		MaxBufferSize: 10240,
	}

	if cfg.FlushInterval != 5*time.Second {
		t.Errorf("FlushInterval = %v, want 5s", cfg.FlushInterval)
	}

	if cfg.MaxBatchSize != 1024 {
		t.Errorf("MaxBatchSize = %d, want 1024", cfg.MaxBatchSize)
	}

	if cfg.MaxBufferSize != 10240 {
		t.Errorf("MaxBufferSize = %d, want 10240", cfg.MaxBufferSize)
	}
}

func TestRetryConfig(t *testing.T) {
	cfg := RetryConfig{
		Attempts:   5,
		BackoffMin: 250 * time.Millisecond,
	}

	if cfg.Attempts != 5 {
		t.Errorf("Attempts = %d, want 5", cfg.Attempts)
	}

	if cfg.BackoffMin != 250*time.Millisecond {
		t.Errorf("BackoffMin = %v, want 250ms", cfg.BackoffMin)
	}
}
