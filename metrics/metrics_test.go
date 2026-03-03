package metrics

import (
	"strings"
	"testing"
)

func TestRenderPrometheusHistogramNoEmptyLabelSet(t *testing.T) {
	reg := NewRegistry()
	reg.ObserveBatchSize(3)
	reg.ObserveGC(0, 0, 0, 0, true)

	out := reg.RenderPrometheus()
	if strings.Contains(out, "fs_batch_size_histogram_sum{}") {
		t.Fatalf("unexpected empty label set for batch sum metric")
	}
	if strings.Contains(out, "fs_batch_size_histogram_count{}") {
		t.Fatalf("unexpected empty label set for batch count metric")
	}
	if strings.Contains(out, "fs_gc_duration_seconds_sum{}") {
		t.Fatalf("unexpected empty label set for gc sum metric")
	}
	if strings.Contains(out, "fs_gc_duration_seconds_count{}") {
		t.Fatalf("unexpected empty label set for gc count metric")
	}
}

func TestEscapeLabelValueEscapesSingleBackslash(t *testing.T) {
	got := escapeLabelValue(`a\b`)
	want := `a\\b`
	if got != want {
		t.Fatalf("escapeLabelValue returned %q, want %q", got, want)
	}
}
