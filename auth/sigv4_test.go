package auth

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestCanonicalPathEncodesEquals(t *testing.T) {
	u := &url.URL{Path: "/test-bucket/jsp-data-raw/year=2026/month=03/day=12/vehicle_positions.parquet"}
	got := canonicalPath(u)
	want := "/test-bucket/jsp-data-raw/year%3D2026/month%3D03/day%3D12/vehicle_positions.parquet"
	if got != want {
		t.Fatalf("unexpected canonical path: got %q want %q", got, want)
	}
}

func TestCanonicalPathPreservesExistingEscapes(t *testing.T) {
	u, err := url.Parse("http://localhost:2600/test-bucket/jsp-data-raw/year%3d2026/file%2Eparquet")
	if err != nil {
		t.Fatalf("url.Parse failed: %v", err)
	}
	got := canonicalPath(u)
	want := "/test-bucket/jsp-data-raw/year%3D2026/file%2Eparquet"
	if got != want {
		t.Fatalf("unexpected canonical path: got %q want %q", got, want)
	}
}

func TestBuildCanonicalRequestUsesAwsEncodedPath(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://localhost:2600/test-bucket/jsp-data-raw/year=2026/month=03/day=12/vehicle_positions.parquet", nil)
	req.Header.Set("x-amz-date", "20260313T120000Z")
	req.Header.Set("x-amz-content-sha256", "UNSIGNED-PAYLOAD")

	canonical, err := buildCanonicalRequest(req, []string{"host", "x-amz-content-sha256", "x-amz-date"}, "UNSIGNED-PAYLOAD", false)
	if err != nil {
		t.Fatalf("buildCanonicalRequest failed: %v", err)
	}

	lines := strings.Split(canonical, "\n")
	if len(lines) < 2 {
		t.Fatalf("canonical request has unexpected format: %q", canonical)
	}
	wantPath := "/test-bucket/jsp-data-raw/year%3D2026/month%3D03/day%3D12/vehicle_positions.parquet"
	if lines[1] != wantPath {
		t.Fatalf("unexpected canonical path line: got %q want %q", lines[1], wantPath)
	}
}
