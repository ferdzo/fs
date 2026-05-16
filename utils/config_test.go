package utils

import "testing"

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
