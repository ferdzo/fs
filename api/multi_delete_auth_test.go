package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fs/auth"
	"fs/logging"
	"fs/metadata"
	"fs/models"
	"fs/service"
	"fs/storage"

	"github.com/go-chi/chi/v5"
)

func newAuthorizedDeleteHandler(t *testing.T) (*Handler, *service.ObjectService, *auth.Service) {
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
		true,
		"us-east-1",
		0,
		0,
		masterKey,
		"",
		"",
		"",
	), md)
	if err != nil {
		t.Fatalf("new auth service: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := NewHandler(svc, logger, logging.Config{}, authSvc, false)
	return handler, svc, authSvc
}

func newBucketPostRequest(bucket, body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/"+bucket+"?delete", strings.NewReader(body))
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("bucket", bucket)
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
}

func withAuthContext(req *http.Request, accessKeyID string) *http.Request {
	authCtx := auth.RequestContext{
		Authenticated: true,
		AccessKeyID:   accessKeyID,
		AuthType:      "test",
	}
	return req.WithContext(auth.WithRequestContext(req.Context(), authCtx))
}

func createDeleteUser(t *testing.T, authSvc *auth.Service, prefix string) {
	t.Helper()
	_, err := authSvc.CreateUser(auth.CreateUserInput{
		AccessKeyID: "delete-user",
		SecretKey:   "delete-secret-1",
		Policy: models.AuthPolicy{
			Statements: []models.AuthPolicyStatement{
				{
					Effect:  "allow",
					Actions: []string{"s3:DeleteObject"},
					Bucket:  "test-bucket",
					Prefix:  prefix,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("create delete user: %v", err)
	}
}

func putTestObject(t *testing.T, svc *service.ObjectService, key string) {
	t.Helper()
	_, err := svc.PutObject("test-bucket", key, "text/plain", bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("put object %q: %v", key, err)
	}
}

func TestMultiDeleteAuthorizesEveryKey(t *testing.T) {
	handler, svc, authSvc := newAuthorizedDeleteHandler(t)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	createDeleteUser(t, authSvc, "allowed/")
	putTestObject(t, svc, "allowed/file.txt")
	putTestObject(t, svc, "private/file.txt")

	body := `<Delete><Object><Key>allowed/file.txt</Key></Object><Object><Key>private/file.txt</Key></Object></Delete>`
	req := withAuthContext(newBucketPostRequest("test-bucket", body), "delete-user")
	rec := httptest.NewRecorder()

	handler.handlePostBucket(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d body=%s", rec.Code, rec.Body.String())
	}
	responseBody := rec.Body.String()
	if !strings.Contains(responseBody, "<Deleted>") || !strings.Contains(responseBody, "allowed/file.txt") {
		t.Fatalf("expected allowed key to be deleted, body=%s", responseBody)
	}
	if !strings.Contains(responseBody, "<Error>") || !strings.Contains(responseBody, "private/file.txt") || !strings.Contains(responseBody, "AccessDenied") {
		t.Fatalf("expected denied key error, body=%s", responseBody)
	}
	if _, err := svc.HeadObject("test-bucket", "allowed/file.txt"); !errors.Is(err, metadata.ErrObjectNotFound) {
		t.Fatalf("allowed object should be deleted, got err=%v", err)
	}
	if _, err := svc.HeadObject("test-bucket", "private/file.txt"); err != nil {
		t.Fatalf("private object should remain: %v", err)
	}
}

func TestMultiDeleteAllowsScopedKeys(t *testing.T) {
	handler, svc, authSvc := newAuthorizedDeleteHandler(t)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	createDeleteUser(t, authSvc, "allowed/")
	putTestObject(t, svc, "allowed/file.txt")

	body := `<Delete><Object><Key>allowed/file.txt</Key></Object></Delete>`
	req := withAuthContext(newBucketPostRequest("test-bucket", body), "delete-user")
	rec := httptest.NewRecorder()

	handler.handlePostBucket(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d body=%s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "<Error>") {
		t.Fatalf("unexpected delete error body=%s", rec.Body.String())
	}
	if _, err := svc.HeadObject("test-bucket", "allowed/file.txt"); !errors.Is(err, metadata.ErrObjectNotFound) {
		t.Fatalf("allowed object should be deleted, got err=%v", err)
	}
}
