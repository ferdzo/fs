package utils

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	DataPath  string
	Address   string
	Port      int
	ChunkSize int
	LogLevel  string
	LogFormat string
	AuditLog  bool
}

func NewConfig() *Config {
	_ = godotenv.Load()

	config := &Config{
		DataPath:  sanitizeDataPath(os.Getenv("DATA_PATH")),
		Address:   firstNonEmpty(strings.TrimSpace(os.Getenv("ADDRESS")), "0.0.0.0"),
		Port:      envInt("PORT", 3000),
		ChunkSize: envInt("CHUNK_SIZE", 8192000),
		LogLevel:  strings.ToLower(firstNonEmpty(strings.TrimSpace(os.Getenv("LOG_LEVEL")), "info")),
		LogFormat: strings.ToLower(firstNonEmpty(strings.TrimSpace(os.Getenv("LOG_FORMAT")), strings.TrimSpace(os.Getenv("LOG_TYPE")), "text")),
		AuditLog:  envBool("AUDIT_LOG", true),
	}

	if config.LogFormat != "json" && config.LogFormat != "text" {
		config.LogFormat = "text"
	}

	return config

}

func envInt(key string, defaultValue int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
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
