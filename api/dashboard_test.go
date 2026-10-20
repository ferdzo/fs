package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fs/webui"
)

func TestDashboardMountedWithToken(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	if err := svc.CreateBucket("dash-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if _, err := svc.PutObject("dash-bucket", "a.txt", "text/plain", strings.NewReader("hello")); err != nil {
		t.Fatalf("put object: %v", err)
	}
	handler.EnableDashboard("secret")

	recorder := httptest.NewRecorder()
	handler.router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ui", nil))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "<title>fs</title>") {
		t.Fatalf("dashboard page: status %d", recorder.Code)
	}

	recorder = httptest.NewRecorder()
	handler.router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ui/api/summary", nil))
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("summary without token: status %d, want 401", recorder.Code)
	}

	request := httptest.NewRequest(http.MethodGet, "/ui/api/summary", nil)
	request.Header.Set("Authorization", "Bearer secret")
	recorder = httptest.NewRecorder()
	handler.router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("summary: status %d body %s", recorder.Code, recorder.Body.String())
	}
	var summary webui.Summary
	if err := json.Unmarshal(recorder.Body.Bytes(), &summary); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if summary.Buckets != 1 || summary.Objects != 1 || summary.LogicalBytes != 5 {
		t.Fatalf("summary = %+v", summary)
	}

	request = httptest.NewRequest(http.MethodGet, "/ui/api/objects?bucket=dash-bucket", nil)
	request.Header.Set("Authorization", "Bearer secret")
	recorder = httptest.NewRecorder()
	handler.router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("objects: status %d body %s", recorder.Code, recorder.Body.String())
	}
	var objects []webui.Object
	if err := json.Unmarshal(recorder.Body.Bytes(), &objects); err != nil {
		t.Fatalf("decode objects: %v", err)
	}
	if len(objects) != 1 || objects[0].Key != "a.txt" || objects[0].Size != 5 {
		t.Fatalf("objects = %+v", objects)
	}
}

func TestDashboardDisabledWithoutToken(t *testing.T) {
	handler, _ := newTestObjectHandler(t)
	recorder := httptest.NewRecorder()
	handler.router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ui", nil))
	if recorder.Code == http.StatusOK {
		t.Fatal("dashboard served without a configured token")
	}
}
