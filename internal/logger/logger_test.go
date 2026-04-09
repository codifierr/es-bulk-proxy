package logger

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestNew_Development(t *testing.T) {
	log := New(true)

	if log == nil {
		t.Fatal("New() returned nil")
	}

	if log.logger == nil {
		t.Error("logger not initialized")
	}
}

func TestNew_Production(t *testing.T) {
	log := New(false)

	if log == nil {
		t.Fatal("New() returned nil")
	}

	if log.logger == nil {
		t.Error("logger not initialized")
	}
}

func TestLogger_InfoFields(t *testing.T) {
	// Capture log output
	var buf bytes.Buffer
	testLogger := zerolog.New(&buf).With().Timestamp().Logger()

	log := &Logger{
		logger: &testLogger,
	}

	fields := map[string]interface{}{
		"key1": "value1",
		"key2": 42,
		"key3": true,
	}

	log.InfoFields("test message", fields)

	output := buf.String()
	if !strings.Contains(output, "test message") {
		t.Error("Log output should contain message")
	}

	// Verify JSON structure
	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON log: %v", err)
	}

	if logEntry["level"] != "info" {
		t.Errorf("Level = %v, want info", logEntry["level"])
	}

	if logEntry["message"] != "test message" {
		t.Errorf("Message = %v, want 'test message'", logEntry["message"])
	}

	if logEntry["key1"] != "value1" {
		t.Errorf("key1 = %v, want 'value1'", logEntry["key1"])
	}

	if logEntry["key2"] != float64(42) {
		t.Errorf("key2 = %v, want 42", logEntry["key2"])
	}

	if logEntry["key3"] != true {
		t.Errorf("key3 = %v, want true", logEntry["key3"])
	}
}

func TestLogger_ErrorFields(t *testing.T) {
	var buf bytes.Buffer
	testLogger := zerolog.New(&buf).With().Timestamp().Logger()

	log := &Logger{
		logger: &testLogger,
	}

	fields := map[string]interface{}{
		"error": "something went wrong",
		"code":  500,
	}

	log.ErrorFields("error occurred", fields)

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON log: %v", err)
	}

	if logEntry["level"] != "error" {
		t.Errorf("Level = %v, want error", logEntry["level"])
	}

	if logEntry["message"] != "error occurred" {
		t.Errorf("Message = %v, want 'error occurred'", logEntry["message"])
	}
}

func TestLogger_WarnFields(t *testing.T) {
	var buf bytes.Buffer
	testLogger := zerolog.New(&buf).With().Timestamp().Logger()

	log := &Logger{
		logger: &testLogger,
	}

	fields := map[string]interface{}{
		"warning": "potential issue",
	}

	log.WarnFields("warning message", fields)

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON log: %v", err)
	}

	if logEntry["level"] != "warn" {
		t.Errorf("Level = %v, want warn", logEntry["level"])
	}

	if logEntry["message"] != "warning message" {
		t.Errorf("Message = %v, want 'warning message'", logEntry["message"])
	}
}

func TestLogger_DebugFields(t *testing.T) {
	var buf bytes.Buffer
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	testLogger := zerolog.New(&buf).With().Timestamp().Logger()

	log := &Logger{
		logger: &testLogger,
	}

	fields := map[string]interface{}{
		"debug_info": "detailed information",
	}

	log.DebugFields("debug message", fields)

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON log: %v", err)
	}

	if logEntry["level"] != "debug" {
		t.Errorf("Level = %v, want debug", logEntry["level"])
	}

	if logEntry["message"] != "debug message" {
		t.Errorf("Message = %v, want 'debug message'", logEntry["message"])
	}
}

func TestLogger_EmptyFields(t *testing.T) {
	var buf bytes.Buffer
	testLogger := zerolog.New(&buf).With().Timestamp().Logger()

	log := &Logger{
		logger: &testLogger,
	}

	// Should not panic with empty fields
	log.InfoFields("message with empty fields", map[string]interface{}{})

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON log: %v", err)
	}

	if logEntry["message"] != "message with empty fields" {
		t.Errorf("Message = %v, want 'message with empty fields'", logEntry["message"])
	}
}

func TestLogger_NilFields(t *testing.T) {
	var buf bytes.Buffer
	testLogger := zerolog.New(&buf).With().Timestamp().Logger()

	log := &Logger{
		logger: &testLogger,
	}

	// Should not panic with nil fields
	log.InfoFields("message with nil fields", nil)

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON log: %v", err)
	}

	if logEntry["message"] != "message with nil fields" {
		t.Errorf("Message = %v, want 'message with nil fields'", logEntry["message"])
	}
}

func TestLogger_ComplexFields(t *testing.T) {
	var buf bytes.Buffer
	testLogger := zerolog.New(&buf).With().Timestamp().Logger()

	log := &Logger{
		logger: &testLogger,
	}

	fields := map[string]interface{}{
		"string": "text",
		"int":    42,
		"float":  3.14,
		"bool":   true,
		"slice":  []string{"a", "b", "c"},
		"map":    map[string]string{"nested": "value"},
		"nil":    nil,
	}

	log.InfoFields("complex fields", fields)

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON log: %v", err)
	}

	// Verify all field types are present
	if logEntry["string"] != "text" {
		t.Errorf("string field not logged correctly")
	}

	if logEntry["int"] != float64(42) {
		t.Errorf("int field not logged correctly")
	}

	if logEntry["bool"] != true {
		t.Errorf("bool field not logged correctly")
	}
}

func TestLogger_With(t *testing.T) {
	var buf bytes.Buffer
	testLogger := zerolog.New(&buf).With().Timestamp().Logger()

	log := &Logger{
		logger: &testLogger,
	}

	// Test With method
	ctx := log.With()
	// Can't compare zerolog.Context directly, so we test by using it
	childLogger := ctx.Str("component", "test").Logger()
	childLog := &Logger{logger: &childLogger}

	childLog.InfoFields("child message", map[string]interface{}{"key": "value"})

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON log: %v", err)
	}

	if logEntry["component"] != "test" {
		t.Error("Child logger should inherit context")
	}
}

func TestLogger_SetGlobal(t *testing.T) {
	log := New(false)

	// Should not panic
	log.SetGlobal()

	// Global logger should be set (we can't easily test this directly)
	// but we can verify the method doesn't panic
}

func TestLogger_MultipleInstances(t *testing.T) {
	log1 := New(true)
	log2 := New(false)

	if log1 == nil || log2 == nil {
		t.Fatal("Failed to create logger instances")
	}

	if log1.logger == log2.logger {
		t.Error("Different logger instances should have different underlying loggers")
	}
}

func TestLogger_ConcurrentLogging(t *testing.T) {
	var buf bytes.Buffer
	testLogger := zerolog.New(&buf).With().Timestamp().Logger()

	log := &Logger{
		logger: &testLogger,
	}

	done := make(chan bool)

	// Multiple goroutines logging concurrently
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				log.InfoFields("concurrent message", map[string]interface{}{
					"goroutine": id,
					"iteration": j,
				})
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should not panic and should have logged messages
	output := buf.String()
	if len(output) == 0 {
		t.Error("No log output from concurrent logging")
	}
}

func TestLogger_SpecialCharacters(t *testing.T) {
	var buf bytes.Buffer
	testLogger := zerolog.New(&buf).With().Timestamp().Logger()

	log := &Logger{
		logger: &testLogger,
	}

	fields := map[string]interface{}{
		"special": "quotes\"and\\slashes/and<tags>",
		"unicode": "こんにちは世界 🌍",
		"newline": "line1\nline2",
	}

	log.InfoFields("special characters", fields)

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON log with special characters: %v", err)
	}

	// JSON should escape special characters properly
	if logEntry["special"] == nil {
		t.Error("Special characters field should be present")
	}

	if logEntry["unicode"] == nil {
		t.Error("Unicode field should be present")
	}
}

func TestLogger_LargeFields(t *testing.T) {
	var buf bytes.Buffer
	testLogger := zerolog.New(&buf).With().Timestamp().Logger()

	log := &Logger{
		logger: &testLogger,
	}

	// Create large field map
	fields := make(map[string]interface{})
	for i := 0; i < 100; i++ {
		fields[string(rune('a'+i%26))+strconv.Itoa(i)] = strings.Repeat("x", 100)
	}

	// Should not panic with large fields
	log.InfoFields("large fields", fields)

	if buf.Len() == 0 {
		t.Error("Should have logged large fields")
	}
}

func TestLogger_NumericFieldTypes(t *testing.T) {
	var buf bytes.Buffer
	testLogger := zerolog.New(&buf).With().Timestamp().Logger()

	log := &Logger{
		logger: &testLogger,
	}

	fields := map[string]interface{}{
		"int":     int(42),
		"int8":    int8(8),
		"int16":   int16(16),
		"int32":   int32(32),
		"int64":   int64(64),
		"uint":    uint(42),
		"uint8":   uint8(8),
		"uint16":  uint16(16),
		"uint32":  uint32(32),
		"uint64":  uint64(64),
		"float32": float32(3.14),
		"float64": float64(2.71828),
	}

	log.InfoFields("numeric types", fields)

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON log: %v", err)
	}

	// All numeric types should be serialized
	for key := range fields {
		if logEntry[key] == nil {
			t.Errorf("Field %s should be present", key)
		}
	}
}
