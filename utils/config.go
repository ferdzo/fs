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
	MaxObjectUploadBytes      int64
	LogLevel                  string
	LogFormat                 string
	AuditLog                  bool
	BodyReadTimeoutSeconds    int64
	GcInterval                time.Duration
	GcEnabled                 bool
	MultipartCleanupRetention time.Duration
	AuthEnabled               bool
	AuthRegion                string
	AuthFailureLimitPerMin    int
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
		DataPath:             sanitizeDataPath(os.Getenv("DATA_PATH")),
		Address:              firstNonEmpty(strings.TrimSpace(os.Getenv("ADDRESS")), "0.0.0.0"),
		Port:                 envIntRange("PORT", 2600, 1, 65535),
		ChunkSize:            envIntRange("CHUNK_SIZE", 8192000, 1, 64*1024*1024),
		MaxObjectUploadBytes: envInt64Range("FS_MAX_OBJECT_UPLOAD_BYTES", 5*1024*1024*1024, 1, 5*1024*1024*1024),
		LogLevel:             strings.ToLower(firstNonEmpty(strings.TrimSpace(os.Getenv("LOG_LEVEL")), "info")),
		LogFormat:            strings.ToLower(firstNonEmpty(strings.TrimSpace(os.Getenv("LOG_FORMAT")), strings.TrimSpace(os.Getenv("LOG_TYPE")), "text")),
		AuditLog:             envBool("AUDIT_LOG", true),
		GcInterval:           time.Duration(envIntRange("GC_INTERVAL", 10, 1, 60)) * time.Minute,
		GcEnabled:            envBool("GC_ENABLED", true),
		MultipartCleanupRetention: time.Duration(
			envIntRange("MULTIPART_RETENTION_HOURS", 24, 1, 24*30),
		) * time.Hour,
		AuthEnabled:            envBool("FS_AUTH_ENABLED", false),
		AuthRegion:             firstNonEmpty(strings.TrimSpace(os.Getenv("FS_AUTH_REGION")), "us-east-1"),
		AuthSkew:               time.Duration(envIntRange("FS_AUTH_CLOCK_SKEW_SECONDS", 300, 30, 3600)) * time.Second,
		AuthMaxPresign:         time.Duration(envIntRange("FS_AUTH_MAX_PRESIGN_SECONDS", 86400, 60, 86400)) * time.Second,
		AuthFailureLimitPerMin: int(envInt64Range("FS_AUTH_FAILURE_LIMIT_PER_MIN", 600, 0, 100000)),
		AuthMasterKey:          strings.TrimSpace(os.Getenv("FS_MASTER_KEY")),
		AuthBootstrapAccessKey: strings.TrimSpace(os.Getenv("FS_ROOT_USER")),
		AuthBootstrapSecretKey: strings.TrimSpace(os.Getenv("FS_ROOT_PASSWORD")),
		AuthBootstrapPolicy:    strings.TrimSpace(os.Getenv("FS_ROOT_POLICY_JSON")),
		AdminAPIEnabled:        envBool("ADMIN_API_ENABLED", true),
		BodyReadTimeoutSeconds: envInt64Range("FS_BODY_READ_TIMEOUT_SECONDS", 60, 0, 86400),
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

func envInt64Range(key string, defaultValue, minValue, maxValue int64) int64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.ParseInt(raw, 10, 64)
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

// fallback: parsed in LoadConfig caller
