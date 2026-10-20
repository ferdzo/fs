package api

import (
	"encoding/base64"
	"encoding/json"
	"fs/auth"
	"fs/logging"
	"fs/metadata"
	"fs/models"
	"fs/service"
	"fs/storage"
	"fs/webui"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

func newAuthDashboardHandler(t *testing.T) (*Handler, *service.ObjectService, *auth.Service) {
	t.Helper()
	root := t.TempDir()
	md, err := metadata.NewMetadataHandler(filepath.Join(root, "metadata.db"))
	if err != nil {
		t.Fatalf("new metadata handler: %v", err)
	}
	blob, err := storage.NewBlobStore(root, 1024)
	if err != nil {
		t.Fatalf("new blob store: %v", err)
	}
	svc := service.NewObjectService(md, blob, time.Hour)
	t.Cleanup(func() {
		_ = svc.Close()
	})
	masterKey := base64.StdEncoding.EncodeToString(make([]byte, 32))
	authSvc, err := auth.NewService(auth.ConfigFromValues(
		true, "us-east-1", 0, 0, masterKey, "root-user", "root-secret-123", "", 0,
	), md)
	if err != nil {
		t.Fatalf("new auth service: %v", err)
	}
	if err := authSvc.EnsureBootstrap(); err != nil {
		t.Fatalf("ensure bootstrap: %v", err)
	}
	handler := NewHandler(svc, slog.New(slog.NewTextHandler(io.Discard, nil)), logging.Config{}, authSvc, false)
	handler.setupRoutes()
	return handler, svc, authSvc
}

func TestDashboardUsesExistingAccounts(t *testing.T) {
	handler, svc, authSvc := newAuthDashboardHandler(t)
	for _, bucket := range []string{"allowed-bucket", "other-bucket"} {
		if err := svc.CreateBucket(bucket); err != nil {
			t.Fatalf("create bucket %q: %v", bucket, err)
		}
	}
	created, err := authSvc.CreateUser(auth.CreateUserInput{
		AccessKeyID: "limited-user",
		Policy: models.AuthPolicy{Statements: []models.AuthPolicyStatement{{
			Effect:  "allow",
			Actions: []string{"s3:ListBucket"},
			Bucket:  "allowed-bucket",
		}}},
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	handler.EnableDashboard("")

	recorder := httptest.NewRecorder()
	handler.router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ui/api/summary", nil))
	if recorder.Code != http.StatusUnauthorized || recorder.Header().Get("WWW-Authenticate") == "" {
		t.Fatalf("no credentials: status %d headers %v", recorder.Code, recorder.Header())
	}

	request := httptest.NewRequest(http.MethodGet, "/ui/api/summary", nil)
	request.SetBasicAuth("limited-user", created.SecretKey)
	recorder = httptest.NewRecorder()
	handler.router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("limited user summary: status %d body %s", recorder.Code, recorder.Body.String())
	}
	var summary webui.Summary
	if err := json.Unmarshal(recorder.Body.Bytes(), &summary); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if summary.Buckets != 1 {
		t.Fatalf("limited user sees %d buckets, want 1", summary.Buckets)
	}

	request = httptest.NewRequest(http.MethodGet, "/ui/api/summary", nil)
	request.SetBasicAuth("limited-user", "wrong-secret")
	recorder = httptest.NewRecorder()
	handler.router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("wrong secret: status %d, want 401", recorder.Code)
	}
}
