package logger

import (
	"io"
	"log/syslog"
	"os"
	"time"

	"github.com/codifierr/es-bulk-proxy/internal/config"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Logger wraps zerolog.Logger.
type Logger struct {
	logger *zerolog.Logger
}

// New creates a new logger instance.
func New(cfg *config.LoggerConfig, development bool) *Logger {
	var output io.Writer = os.Stdout

	var writers []io.Writer

	// Console output
	if development {
		// Pretty console output for development
		output = zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
		}

		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		// JSON output for production
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	writers = append(writers, output)

	// Syslog output
	if cfg != nil && cfg.Syslog.Enabled {
		var w *syslog.Writer

		var err error

		if cfg.Syslog.Network != "" && cfg.Syslog.Address != "" {
			// Remote syslog
			w, err = syslog.Dial(
				cfg.Syslog.Network,
				cfg.Syslog.Address,
				syslog.LOG_INFO|syslog.LOG_LOCAL0,
				"es-bulk-proxy",
			)
		} else {
			// Local syslog (unix socket)
			w, err = syslog.New(
				syslog.LOG_INFO|syslog.LOG_LOCAL0,
				"es-bulk-proxy",
			)
		}

		if err == nil {
			// Use SyslogLevelWriter to preserve log levels
			writers = append(writers, zerolog.SyslogLevelWriter(w))
			// 👉 If you want structured logs in syslog, use this instead:
			// writers = append(writers, zerolog.SyslogCEEWriter(w))
		}
		// If syslog fails, fallback silently to stdout
	}

	// Create multi-writer (level-aware)
	var finalOutput io.Writer
	if len(writers) > 1 {
		finalOutput = zerolog.MultiLevelWriter(writers...)
	} else {
		finalOutput = output
	}

	// Create logger
	logger := zerolog.New(finalOutput).
		With().
		Timestamp().
		Caller().
		Logger()

	return &Logger{
		logger: &logger,
	}
}

// SetGlobal sets the logger as the global logger.
func (l *Logger) SetGlobal() {
	log.Logger = *l.logger
}

// InfoFields logs an info message with fields.
func (l *Logger) InfoFields(msg string, fields map[string]any) {
	event := l.logger.Info()
	for k, v := range fields {
		event = event.Interface(k, v)
	}

	event.Msg(msg)
}

// ErrorFields logs an error message with fields.
func (l *Logger) ErrorFields(msg string, fields map[string]any) {
	event := l.logger.Error()
	for k, v := range fields {
		event = event.Interface(k, v)
	}

	event.Msg(msg)
}

// WarnFields logs a warning message with fields.
func (l *Logger) WarnFields(msg string, fields map[string]any) {
	event := l.logger.Warn()
	for k, v := range fields {
		event = event.Interface(k, v)
	}

	event.Msg(msg)
}

// DebugFields logs a debug message with fields.
func (l *Logger) DebugFields(msg string, fields map[string]any) {
	event := l.logger.Debug()
	for k, v := range fields {
		event = event.Interface(k, v)
	}

	event.Msg(msg)
}

// FatalFields logs a fatal message with fields and exits.
func (l *Logger) FatalFields(msg string, fields map[string]any) {
	event := l.logger.Fatal()
	for k, v := range fields {
		event = event.Interface(k, v)
	}

	event.Msg(msg)
}

// With creates a child logger with additional context.
func (l *Logger) With() zerolog.Context {
	return l.logger.With()
}

// GetZerolog returns the underlying zerolog.Logger.
func (l *Logger) GetZerolog() *zerolog.Logger {
	return l.logger
}
