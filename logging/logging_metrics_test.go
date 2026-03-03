package logging

import (
	"net/http/httptest"
	"testing"
)

func TestMetricRouteLabelFallbacks(t *testing.T) {
	testCases := []struct {
		name string
		path string
		want string
	}{
		{name: "root", path: "/", want: "/"},
		{name: "health", path: "/healthz", want: "/healthz"},
		{name: "metrics", path: "/metrics", want: "/metrics"},
		{name: "bucket", path: "/some-bucket", want: "/{bucket}"},
		{name: "object", path: "/some-bucket/private/path/file.jpg", want: "/{bucket}/*"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tc.path, nil)
			got := metricRouteLabel(req)
			if got != tc.want {
				t.Fatalf("metricRouteLabel(%q) = %q, want %q", tc.path, got, tc.want)
			}
		})
	}
}
