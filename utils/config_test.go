package utils

import (
	"testing"
	"time"
)

func TestEnvInt64Range(t *testing.T) {
	t.Setenv("TEST_INT64_RANGE", "42")
	if got := envInt64Range("TEST_INT64_RANGE", 10, 1, 100); got != 42 {
		t.Fatalf("envInt64Range valid = %d, want 42", got)
	}
}

func TestEnvInt64RangeFallsBackForInvalidValues(t *testing.T) {
	t.Setenv("TEST_INT64_RANGE", "invalid")
	if got := envInt64Range("TEST_INT64_RANGE", 10, 1, 100); got != 10 {
		t.Fatalf("envInt64Range invalid = %d, want 10", got)
	}
	t.Setenv("TEST_INT64_RANGE", "101")
	if got := envInt64Range("TEST_INT64_RANGE", 10, 1, 100); got != 10 {
		t.Fatalf("envInt64Range too large = %d, want 10", got)
	}
}

func TestNewConfigBodyReadTimeoutZero(t *testing.T) {
	t.Setenv("FS_BODY_READ_TIMEOUT_SECONDS", "0")
	cfg := NewConfig()
	if cfg.BodyReadTimeoutSeconds != 0 {
		t.Fatalf("BodyReadTimeoutSeconds = %d, want 0", cfg.BodyReadTimeoutSeconds)
	}
}

func TestNewConfigMetricsDefaultsAndDisable(t *testing.T) {
	t.Setenv("METRICS_ADDRESS", "")
	t.Setenv("METRICS_PORT", "")
	cfg := NewConfig()
	if cfg.MetricsAddress != "127.0.0.1" {
		t.Fatalf("MetricsAddress = %q, want 127.0.0.1", cfg.MetricsAddress)
	}
	if cfg.MetricsPort != 9091 {
		t.Fatalf("MetricsPort = %d, want 9091", cfg.MetricsPort)
	}

	t.Setenv("METRICS_ADDRESS", "0.0.0.0")
	t.Setenv("METRICS_PORT", "0")
	cfgDisabled := NewConfig()
	if cfgDisabled.MetricsAddress != "0.0.0.0" {
		t.Fatalf("MetricsAddress = %q, want 0.0.0.0", cfgDisabled.MetricsAddress)
	}
	if cfgDisabled.MetricsPort != 0 {
		t.Fatalf("MetricsPort = %d, want 0", cfgDisabled.MetricsPort)
	}
}

func TestNewConfigGCSettings(t *testing.T) {
	t.Setenv("GC_INTERVAL", "")
	t.Setenv("GC_INTERVAL_SECONDS", "")
	t.Setenv("GC_GRACE_SECONDS", "")
	cfgDefault := NewConfig()
	if cfgDefault.GcInterval != 10*time.Minute {
		t.Fatalf("default GcInterval = %v, want 10m", cfgDefault.GcInterval)
	}
	if cfgDefault.GcGrace != 15*time.Minute {
		t.Fatalf("default GcGrace = %v, want 15m", cfgDefault.GcGrace)
	}

	t.Setenv("GC_INTERVAL", "5")
	t.Setenv("GC_INTERVAL_SECONDS", "7")
	t.Setenv("GC_GRACE_SECONDS", "0")
	cfgOverride := NewConfig()
	if cfgOverride.GcInterval != 7*time.Second {
		t.Fatalf("override GcInterval = %v, want 7s", cfgOverride.GcInterval)
	}
	if cfgOverride.GcGrace != 0 {
		t.Fatalf("override GcGrace = %v, want 0s", cfgOverride.GcGrace)
	}

	t.Setenv("GC_INTERVAL_SECONDS", "invalid")
	t.Setenv("GC_GRACE_SECONDS", "30")
	cfgFallback := NewConfig()
	if cfgFallback.GcInterval != 5*time.Minute {
		t.Fatalf("fallback GcInterval = %v, want 5m", cfgFallback.GcInterval)
	}
	if cfgFallback.GcGrace != 30*time.Second {
		t.Fatalf("GcGrace = %v, want 30s", cfgFallback.GcGrace)
	}
}
