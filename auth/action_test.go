package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestResolveTargetIncludesListBucketPrefix(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://example.com/test-bucket?list-type=2&prefix=allowed/", nil)

	target := resolveTarget(req)

	if target.Action != ActionListBucket {
		t.Fatalf("action = %q, want %q", target.Action, ActionListBucket)
	}
	if target.Bucket != "test-bucket" {
		t.Fatalf("bucket = %q, want test-bucket", target.Bucket)
	}
	if target.Prefix != "allowed/" {
		t.Fatalf("prefix = %q, want allowed/", target.Prefix)
	}
	if target.Key != "" {
		t.Fatalf("key = %q, want empty", target.Key)
	}
}

func TestResolveTargetListBucketWithoutPrefix(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://example.com/test-bucket", nil)

	target := resolveTarget(req)

	if target.Action != ActionListBucket {
		t.Fatalf("action = %q, want %q", target.Action, ActionListBucket)
	}
	if target.Prefix != "" {
		t.Fatalf("prefix = %q, want empty", target.Prefix)
	}
}

func TestRequiresHandlerAuthorizationOnlyForMultiDelete(t *testing.T) {
	cases := []struct {
		url    string
		method string
		want   bool
	}{
		{"/bucket?delete", http.MethodPost, true},
		{"/bucket?delete&other=1", http.MethodPost, true},
		{"/bucket/key?uploads&delete=1", http.MethodPost, false},
		{"/bucket/key?uploadId=x&delete=1", http.MethodPost, false},
		{"/bucket?key=delete", http.MethodPost, false},
		{"/bucket?delete", http.MethodGet, false},
		{"/bucket", http.MethodPost, false},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(tc.method, tc.url, nil)
		if got := RequiresHandlerAuthorization(req); got != tc.want {
			t.Errorf("RequiresHandlerAuthorization(%s %s) = %v, want %v", tc.method, tc.url, got, tc.want)
		}
	}
}
