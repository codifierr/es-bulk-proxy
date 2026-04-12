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
	Port string
}

// ElasticsearchConfig holds Elasticsearch connection configuration.
type ElasticsearchConfig struct {
	URL string
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
	if c.Server.Port == "" {
		return errors.New("server.port must not be empty")
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

// setDefaults sets default configuration values.
func setDefaults(v *viper.Viper) {
	// Server defaults
	v.SetDefault("server.port", "8080")

	// Elasticsearch defaults
	v.SetDefault("elasticsearch.url", "http://localhost:9200")

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
	// Use uppercase env vars for compatibility
	err := v.BindEnv("server.port", "PORT")
	if err != nil {
		panic(fmt.Sprintf("failed to bind env variable: %v", err))
	}

	err = v.BindEnv("elasticsearch.url", "ES_URL")
	if err != nil {
		panic(fmt.Sprintf("failed to bind env variable: %v", err))
	}

	err = v.BindEnv("buffer.flushinterval", "FLUSH_INTERVAL")
	if err != nil {
		panic(fmt.Sprintf("failed to bind env variable: %v", err))
	}

	err = v.BindEnv("buffer.maxbatchsize", "MAX_BATCH_SIZE")
	if err != nil {
		panic(fmt.Sprintf("failed to bind env variable: %v", err))
	}

	err = v.BindEnv("buffer.maxbuffersize", "MAX_BUFFER_SIZE")
	if err != nil {
		panic(fmt.Sprintf("failed to bind env variable: %v", err))
	}

	err = v.BindEnv("retry.attempts", "RETRY_ATTEMPTS")
	if err != nil {
		panic(fmt.Sprintf("failed to bind env variable: %v", err))
	}

	err = v.BindEnv("retry.backoffmin", "RETRY_BACKOFF_MIN")
	if err != nil {
		panic(fmt.Sprintf("failed to bind env variable: %v", err))
	}

	err = v.BindEnv("logger.syslog.enabled", "SYSLOG_ENABLED")
	if err != nil {
		panic(fmt.Sprintf("failed to bind env variable: %v", err))
	}

	err = v.BindEnv("logger.syslog.network", "SYSLOG_NETWORK")
	if err != nil {
		panic(fmt.Sprintf("failed to bind env variable: %v", err))
	}

	err = v.BindEnv("logger.syslog.address", "SYSLOG_ADDRESS")
	if err != nil {
		panic(fmt.Sprintf("failed to bind env variable: %v", err))
	}
}
