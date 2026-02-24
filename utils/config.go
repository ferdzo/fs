package utils

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	DataPath                  string
	Address                   string
	Port                      int
	ChunkSize                 int
	LogLevel                  string
	LogFormat                 string
	AuditLog                  bool
	GcInterval                time.Duration
	GcEnabled                 bool
	MultipartCleanupRetention time.Duration
}

func NewConfig() *Config {
	_ = godotenv.Load()

	config := &Config{
		DataPath:   sanitizeDataPath(os.Getenv("DATA_PATH")),
		Address:    firstNonEmpty(strings.TrimSpace(os.Getenv("ADDRESS")), "0.0.0.0"),
		Port:       envIntRange("PORT", 3000, 1, 65535),
		ChunkSize:  envIntRange("CHUNK_SIZE", 8192000, 1, 64*1024*1024),
		LogLevel:   strings.ToLower(firstNonEmpty(strings.TrimSpace(os.Getenv("LOG_LEVEL")), "info")),
		LogFormat:  strings.ToLower(firstNonEmpty(strings.TrimSpace(os.Getenv("LOG_FORMAT")), strings.TrimSpace(os.Getenv("LOG_TYPE")), "text")),
		AuditLog:   envBool("AUDIT_LOG", true),
		GcInterval: time.Duration(envIntRange("GC_INTERVAL", 10, 1, 60)) * time.Minute,
		GcEnabled:  envBool("GC_ENABLED", true),
		MultipartCleanupRetention: time.Duration(
			envIntRange("MULTIPART_RETENTION_HOURS", 24, 1, 24*30),
		) * time.Hour,
	}

	if config.LogFormat != "json" && config.LogFormat != "text" {
		config.LogFormat = "text"
	}

	return config

}

func envIntRange(key string, defaultValue, minValue, maxValue int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return defaultValue
	}
	if value < minValue || value > maxValue {
		return defaultValue
	}
	return value
}

func envBool(key string, defaultValue bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return defaultValue
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func sanitizeDataPath(raw string) string {
	cleaned := strings.TrimSpace(raw)
	if cleaned == "" {
		cleaned = "."
	}
	cleaned = filepath.Clean(cleaned)
	if abs, err := filepath.Abs(cleaned); err == nil {
		return abs
	}
	return cleaned
}
