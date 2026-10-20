package webui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeProvider struct {
	summary   Summary
	buckets   []Bucket
	objects   []Object
	gotBucket string
	gotPrefix string
}

func (f *fakeProvider) Summary(context.Context) (Summary, error) { return f.summary, nil }

func (f *fakeProvider) Buckets(context.Context) ([]Bucket, error) { return f.buckets, nil }

func (f *fakeProvider) Objects(_ context.Context, bucket, prefix string) ([]Object, error) {
	f.gotBucket, f.gotPrefix = bucket, prefix
	return f.objects, nil
}

func newTestHandler(t *testing.T, cfg Config) (*fakeProvider, http.Handler) {
	t.Helper()
	provider := &fakeProvider{
		summary: Summary{Buckets: 2, Objects: 3, LogicalBytes: 2048, PhysicalBytes: 1024},
		buckets: []Bucket{{Name: "photos", Objects: 2, Bytes: 1024}},
		objects: []Object{{Key: "a.txt", Size: 10, ETag: "etag", LastModified: "2026-01-01"}},
	}
	handler, err := NewHandler(cfg, provider)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return provider, handler
}

func get(handler http.Handler, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestIndexServed(t *testing.T) {
	_, handler := newTestHandler(t, Config{})
	rec := get(handler, "/ui/")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "<title>fs</title>") {
		t.Fatal("index page not served")
	}
}

func TestSummary(t *testing.T) {
	_, handler := newTestHandler(t, Config{})
	rec := get(handler, "/ui/api/summary")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var summary Summary
	if err := json.Unmarshal(rec.Body.Bytes(), &summary); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if summary.LogicalBytes != 2048 || summary.PhysicalBytes != 1024 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestObjectsForwardPrefix(t *testing.T) {
	provider, handler := newTestHandler(t, Config{})
	rec := get(handler, "/ui/api/objects?bucket=photos&prefix=2026/")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if provider.gotBucket != "photos" || provider.gotPrefix != "2026/" {
		t.Fatalf("provider got bucket=%q prefix=%q", provider.gotBucket, provider.gotPrefix)
	}
	var objects []Object
	if err := json.Unmarshal(rec.Body.Bytes(), &objects); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(objects) != 1 || objects[0].Key != "a.txt" {
		t.Fatalf("objects = %+v", objects)
	}
}

func TestTokenRequired(t *testing.T) {
	_, handler := newTestHandler(t, Config{Token: "secret"})
	if rec := get(handler, "/ui/api/summary"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token: status = %d, want 401", rec.Code)
	}
	req := httptest.NewRequest(http.MethodGet, "/ui/api/summary", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("with token: status = %d, want 200", rec.Code)
	}
}

func TestMissingBucket(t *testing.T) {
	_, handler := newTestHandler(t, Config{})
	if rec := get(handler, "/ui/api/objects"); rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestUnknownPath(t *testing.T) {
	_, handler := newTestHandler(t, Config{})
	if rec := get(handler, "/ui/nope"); rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}
