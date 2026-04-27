package config

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/spf13/viper"
)

const (
	defaultMaxBatchSize  int64 = 5 * 1024 * 1024
	defaultMaxBufferSize int64 = 50 * 1024 * 1024
	defaultRetryAttempts       = 3
)

// Config holds all application configuration.
type Config struct {
	Server        ServerConfig
	Elasticsearch ElasticsearchConfig
	Buffer        BufferConfig
	Retry         RetryConfig
	Logger        LoggerConfig
}

// ServerConfig holds HTTP server configuration.
type ServerConfig struct {
	Port         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

// ElasticsearchConfig holds Elasticsearch connection configuration.
type ElasticsearchConfig struct {
	URL            string
	RequestTimeout time.Duration
}

// BufferConfig holds bulk buffer configuration.
type BufferConfig struct {
	FlushInterval time.Duration
	MaxBatchSize  int64
	MaxBufferSize int64
}

// RetryConfig holds retry configuration.
type RetryConfig struct {
	Attempts   int
	BackoffMin time.Duration
}

// LoggerConfig holds logger configuration.
type LoggerConfig struct {
	Syslog SyslogConfig
}

// SyslogConfig holds syslog configuration.
type SyslogConfig struct {
	Enabled bool
	Network string // "udp", "tcp", etc.
	Address string // "localhost:514"
}

// Load loads configuration from environment variables and config files.
func Load() (*Config, error) {
	v := viper.New()

	// Set default values
	setDefaults(v)

	// Set config name and paths
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath("./configs")
	v.AddConfigPath(".")
	v.AddConfigPath("/etc/es-bulk-proxy")

	// Read config file (optional)
	if err := v.ReadInConfig(); err != nil {
		var configFileNotFoundError viper.ConfigFileNotFoundError
		if !errors.As(err, &configFileNotFoundError) {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
		// Config file not found; using defaults and env vars
	}

	// Enable automatic env variable binding
	v.AutomaticEnv()

	// Bind specific environment variables
	bindEnvVars(v)

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// Validate checks that configuration values are internally consistent.
func (c *Config) Validate() error {
	if err := c.Server.validate(); err != nil {
		return err
	}

	if c.Elasticsearch.RequestTimeout <= 0 {
		return errors.New("elasticsearch.requesttimeout must be greater than 0")
	}

	if _, err := url.ParseRequestURI(c.Elasticsearch.URL); err != nil {
		return fmt.Errorf("elasticsearch.url must be a valid URL: %w", err)
	}

	if c.Buffer.FlushInterval <= 0 {
		return errors.New("buffer.flushinterval must be greater than 0")
	}

	if c.Buffer.MaxBatchSize <= 0 {
		return errors.New("buffer.maxbatchsize must be greater than 0")
	}

	if c.Buffer.MaxBufferSize <= 0 {
		return errors.New("buffer.maxbuffersize must be greater than 0")
	}

	if c.Buffer.MaxBatchSize > c.Buffer.MaxBufferSize {
		return errors.New("buffer.maxbatchsize must be less than or equal to buffer.maxbuffersize")
	}

	if c.Retry.Attempts < 0 {
		return errors.New("retry.attempts must be greater than or equal to 0")
	}

	if c.Retry.BackoffMin <= 0 {
		return errors.New("retry.backoffmin must be greater than 0")
	}

	return nil
}

// validate checks server configuration values.
func (s *ServerConfig) validate() error {
	if s.Port == "" {
		return errors.New("server.port must not be empty")
	}

	if s.ReadTimeout <= 0 {
		return errors.New("server.readtimeout must be greater than 0")
	}

	if s.WriteTimeout <= 0 {
		return errors.New("server.writetimeout must be greater than 0")
	}

	if s.IdleTimeout <= 0 {
		return errors.New("server.idletimeout must be greater than 0")
	}

	return nil
}

// setDefaults sets default configuration values.
func setDefaults(v *viper.Viper) {
	// Server defaults
	v.SetDefault("server.port", "8080")
	v.SetDefault("server.readtimeout", "30s")
	v.SetDefault("server.writetimeout", "30s")
	v.SetDefault("server.idletimeout", "2m")

	// Elasticsearch defaults
	v.SetDefault("elasticsearch.url", "http://localhost:9200")
	v.SetDefault("elasticsearch.requesttimeout", "30s")

	// Buffer defaults
	v.SetDefault("buffer.flushinterval", "30s")
	v.SetDefault("buffer.maxbatchsize", defaultMaxBatchSize)
	v.SetDefault("buffer.maxbuffersize", defaultMaxBufferSize)

	// Retry defaults
	v.SetDefault("retry.attempts", defaultRetryAttempts)
	v.SetDefault("retry.backoffmin", "100ms")

	// Logger defaults
	v.SetDefault("logger.syslog.enabled", false)
	v.SetDefault("logger.syslog.network", "udp")
	v.SetDefault("logger.syslog.address", "localhost:514")
}

// bindEnvVars binds environment variables to config keys.
func bindEnvVars(v *viper.Viper) {
	bindings := map[string]string{
		"server.port":                  "PORT",
		"server.readtimeout":           "SERVER_READ_TIMEOUT",
		"server.writetimeout":          "SERVER_WRITE_TIMEOUT",
		"server.idletimeout":           "SERVER_IDLE_TIMEOUT",
		"elasticsearch.url":            "ES_URL",
		"elasticsearch.requesttimeout": "ES_REQUEST_TIMEOUT",
		"buffer.flushinterval":         "FLUSH_INTERVAL",
		"buffer.maxbatchsize":          "MAX_BATCH_SIZE",
		"buffer.maxbuffersize":         "MAX_BUFFER_SIZE",
		"retry.attempts":               "RETRY_ATTEMPTS",
		"retry.backoffmin":             "RETRY_BACKOFF_MIN",
		"logger.syslog.enabled":        "SYSLOG_ENABLED",
		"logger.syslog.network":        "SYSLOG_NETWORK",
		"logger.syslog.address":        "SYSLOG_ADDRESS",
	}

	for key, env := range bindings {
		if err := v.BindEnv(key, env); err != nil {
			panic(fmt.Sprintf("failed to bind env variable %s: %v", env, err))
		}
	}
}
