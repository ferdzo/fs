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
