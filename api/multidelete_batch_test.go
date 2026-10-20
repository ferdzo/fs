package api

import (
	"encoding/base64"
	"fmt"
	"fs/auth"
	"fs/logging"
	"fs/metadata"
	"fs/models"
	"fs/service"
	"fs/storage"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// countingPolicyStore wraps the metadata store and counts policy lookups so
// handler tests can prove multi-delete reads the caller's policy once.
type countingPolicyStore struct {
	auth.Store
	policyReads atomic.Int64
}

func (s *countingPolicyStore) GetAuthPolicy(accessKeyID string) (*models.AuthPolicy, error) {
	s.policyReads.Add(1)
	return s.Store.GetAuthPolicy(accessKeyID)
}

func (s *countingPolicyStore) policyReadCount() int64 {
	return s.policyReads.Load()
}

func newCountingDeleteHandler(t *testing.T) (*Handler, *service.ObjectService, *auth.Service, *countingPolicyStore) {
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

	store := &countingPolicyStore{Store: md}
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
		0,
	), store)
	if err != nil {
		t.Fatalf("new auth service: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := NewHandler(svc, logger, logging.Config{}, authSvc, false)
	return handler, svc, authSvc, store
}

func deleteObjectsBody(keys []string) string {
	var body strings.Builder
	body.WriteString("<Delete>")
	for _, key := range keys {
		body.WriteString("<Object><Key>")
		body.WriteString(key)
		body.WriteString("</Key></Object>")
	}
	body.WriteString("</Delete>")
	return body.String()
}

func TestMultiDeleteReadsPolicyOnceForManyKeys(t *testing.T) {
	handler, svc, authSvc, store := newCountingDeleteHandler(t)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	createDeleteUserWithStatements(t, authSvc, []models.AuthPolicyStatement{
		{Effect: "allow", Actions: []string{"s3:DeleteObject"}, Bucket: "test-bucket", Prefix: "batch/"},
	})

	const keyCount = 64
	keys := make([]string, keyCount)
	for i := range keys {
		keys[i] = fmt.Sprintf("batch/file-%03d.txt", i)
		putTestObject(t, svc, keys[i])
	}

	req := withAuthContext(newBucketPostRequest("test-bucket", deleteObjectsBody(keys)), "delete-user")
	rec := httptest.NewRecorder()

	handler.handlePostBucket(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d body=%s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "<Error>") {
		t.Fatalf("unexpected delete error body=%s", rec.Body.String())
	}
	if got := store.policyReadCount(); got != 1 {
		t.Fatalf("policy reads = %d, want 1 for %d keys", got, keyCount)
	}
	for _, key := range keys {
		if _, err := svc.HeadObject("test-bucket", key); err == nil {
			t.Fatalf("expected %q to be deleted", key)
		}
	}
}

func TestMultiDeleteBatchPreservesDeniedKeys(t *testing.T) {
	handler, svc, authSvc, store := newCountingDeleteHandler(t)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	createDeleteUserWithStatements(t, authSvc, []models.AuthPolicyStatement{
		{Effect: "allow", Actions: []string{"s3:DeleteObject"}, Bucket: "test-bucket", Prefix: "batch/"},
		{Effect: "deny", Actions: []string{"s3:DeleteObject"}, Bucket: "test-bucket", Prefix: "batch/secret/"},
	})

	allowed := []string{"batch/a.txt", "batch/b.txt"}
	denied := []string{"batch/secret/a.txt", "outside/c.txt"}
	for _, key := range append(append([]string{}, allowed...), denied...) {
		putTestObject(t, svc, key)
	}
	secondAllowed := "batch/secretish.txt"
	putTestObject(t, svc, secondAllowed)
	allowed = append(allowed, secondAllowed)

	body := deleteObjectsBody(append(append([]string{}, allowed...), denied...))
	req := withAuthContext(newBucketPostRequest("test-bucket", body), "delete-user")
	rec := httptest.NewRecorder()

	handler.handlePostBucket(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d body=%s", rec.Code, rec.Body.String())
	}
	responseBody := rec.Body.String()
	for _, key := range allowed {
		if !strings.Contains(responseBody, key) || !strings.Contains(responseBody, "<Deleted>") {
			t.Fatalf("expected %q in deleted response, body=%s", key, responseBody)
		}
		if _, err := svc.HeadObject("test-bucket", key); err == nil {
			t.Fatalf("expected allowed key %q to be deleted", key)
		}
	}
	for _, key := range denied {
		if !strings.Contains(responseBody, key) || !strings.Contains(responseBody, "AccessDenied") {
			t.Fatalf("expected %q to be denied, body=%s", key, responseBody)
		}
		if _, err := svc.HeadObject("test-bucket", key); err != nil {
			t.Fatalf("denied key %q must remain: %v", key, err)
		}
	}
	if got := store.policyReadCount(); got != 1 {
		t.Fatalf("policy reads = %d, want 1", got)
	}
}
