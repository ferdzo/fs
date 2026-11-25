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

func TestResolveTargetGranularBucketAndVersionActions(t *testing.T) {
	cases := []struct {
		name       string
		method     string
		url        string
		wantAction Action
		wantBucket string
		wantKey    string
		wantPrefix string
	}{
		{"get versioning", http.MethodGet, "/b?versioning", ActionGetBucketVersioning, "b", "", ""},
		{"put versioning", http.MethodPut, "/b?versioning", ActionPutBucketVersioning, "b", "", ""},
		{"get location", http.MethodGet, "/b?location", ActionGetBucketLocation, "b", "", ""},
		{"list versions", http.MethodGet, "/b?versions&prefix=logs/", ActionListBucketVersions, "b", "", "logs/"},
		{"list uploads", http.MethodGet, "/b?uploads&prefix=part/", ActionListBucketMultipart, "b", "", "part/"},
		{"get object version", http.MethodGet, "/b/k?versionId=v1", ActionGetObjectVersion, "b", "k", ""},
		{"head object version", http.MethodHead, "/b/k?versionId=v1", ActionGetObjectVersion, "b", "k", ""},
		{"get object latest", http.MethodGet, "/b/k", ActionGetObject, "b", "k", ""},
		{"delete object version", http.MethodDelete, "/b/k?versionId=v1", ActionDeleteObjectVersion, "b", "k", ""},
		{"delete object unversioned", http.MethodDelete, "/b/k", ActionDeleteObject, "b", "k", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, "http://example.com"+tc.url, nil)
			target := resolveTarget(req)
			if target.Action != tc.wantAction || target.Bucket != tc.wantBucket || target.Key != tc.wantKey || target.Prefix != tc.wantPrefix {
				t.Fatalf("resolveTarget(%s %s) = %+v, want action=%q bucket=%q key=%q prefix=%q",
					tc.method, tc.url, target, tc.wantAction, tc.wantBucket, tc.wantKey, tc.wantPrefix)
			}
		})
	}
}
