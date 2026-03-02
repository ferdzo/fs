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
	AuthEnabled               bool
	AuthRegion                string
	AuthSkew                  time.Duration
	AuthMaxPresign            time.Duration
	AuthMasterKey             string
	AuthBootstrapAccessKey    string
	AuthBootstrapSecretKey    string
	AuthBootstrapPolicy       string
	AdminAPIEnabled           bool
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
		AuthEnabled:            envBool("AUTH_ENABLED", true),
		AuthRegion:             firstNonEmpty(strings.TrimSpace(os.Getenv("AUTH_REGION")), "us-east-1"),
		AuthSkew:               time.Duration(envIntRange("AUTH_SKEW_SECONDS", 300, 30, 3600)) * time.Second,
		AuthMaxPresign:         time.Duration(envIntRange("AUTH_MAX_PRESIGN_SECONDS", 86400, 60, 86400)) * time.Second,
		AuthMasterKey:          strings.TrimSpace(os.Getenv("AUTH_MASTER_KEY")),
		AuthBootstrapAccessKey: strings.TrimSpace(os.Getenv("AUTH_BOOTSTRAP_ACCESS_KEY")),
		AuthBootstrapSecretKey: strings.TrimSpace(os.Getenv("AUTH_BOOTSTRAP_SECRET_KEY")),
		AuthBootstrapPolicy:    strings.TrimSpace(os.Getenv("AUTH_BOOTSTRAP_POLICY")),
		AdminAPIEnabled:        envBool("ADMIN_API_ENABLED", true),
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
