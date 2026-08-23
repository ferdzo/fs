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

func TestParsePresignedSigV4RejectsInvalidExpires(t *testing.T) {
	cases := []string{"0", "-5", "604801", "notanumber"}
	for _, expires := range cases {
		target := "http://localhost:2600/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ak/20260822/us-east-1/s3/aws4_request&X-Amz-Date=20260822T120000Z&X-Amz-Expires=" + expires + "&X-Amz-SignedHeaders=host&X-Amz-Signature=abc"
		req := httptest.NewRequest(http.MethodGet, target, nil)
		if _, err := parsePresignedSigV4(req); err == nil {
			t.Fatalf("expected X-Amz-Expires=%q to be rejected", expires)
		}
	}
}

func TestParsePresignedSigV4AcceptsMaxValidExpires(t *testing.T) {
	target := "http://localhost:2600/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ak/20260822/us-east-1/s3/aws4_request&X-Amz-Date=20260822T120000Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=abc"
	req := httptest.NewRequest(http.MethodGet, target, nil)
	input, err := parsePresignedSigV4(req)
	if err != nil {
		t.Fatalf("expected valid presign to parse: %v", err)
	}
	if input.ExpiresSeconds != 604800 {
		t.Fatalf("unexpected expires seconds: %d", input.ExpiresSeconds)
	}
}

func TestParsePresignedSigV4RequiresHostInSignedHeaders(t *testing.T) {
	target := "http://localhost:2600/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ak/20260822/us-east-1/s3/aws4_request&X-Amz-Date=20260822T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=x-amz-date&X-Amz-Signature=abc"
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.Header.Set("x-amz-date", "20260822T120000Z")
	if _, err := parsePresignedSigV4(req); err == nil {
		t.Fatalf("expected presign without host in SignedHeaders to be rejected")
	}
}
