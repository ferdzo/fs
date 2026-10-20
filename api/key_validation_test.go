package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestObjectKeyWithNULIsRejected(t *testing.T) {
	handler, _ := newTestObjectHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/test-bucket/nul%00key", strings.NewReader("payload"))
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body: %s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "<Code>InvalidArgument</Code>") {
		t.Fatalf("expected InvalidArgument XML, got: %s", rec.Body.String())
	}
}
