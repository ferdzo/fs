package api

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sort"
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
	createDeleteUserWithStatements(t, authSvc, []models.AuthPolicyStatement{
		{
			Effect:  "allow",
			Actions: []string{"s3:DeleteObject"},
			Bucket:  "test-bucket",
			Prefix:  prefix,
		},
	})
}

func createDeleteUserWithStatements(t *testing.T, authSvc *auth.Service, statements []models.AuthPolicyStatement) {
	t.Helper()
	_, err := authSvc.CreateUser(auth.CreateUserInput{
		AccessKeyID: "delete-user",
		SecretKey:   "delete-secret-1",
		Policy: models.AuthPolicy{
			Statements: statements,
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

func TestMultiDeleteRouteAuthorizesKeysAfterMiddleware(t *testing.T) {
	handler, svc, authSvc := newAuthorizedDeleteHandler(t)
	handler.setupRoutes()
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	createDeleteUserWithStatements(t, authSvc, []models.AuthPolicyStatement{
		{Effect: "allow", Actions: []string{"s3:DeleteObject"}, Bucket: "test-bucket", Prefix: "allowed/"},
		{Effect: "deny", Actions: []string{"s3:DeleteObject"}, Bucket: "test-bucket", Prefix: "private/"},
	})
	putTestObject(t, svc, "allowed/file.txt")
	putTestObject(t, svc, "private/file.txt")

	body := `<Delete><Object><Key>allowed/file.txt</Key></Object><Object><Key>private/file.txt</Key></Object></Delete>`
	req := httptest.NewRequest(http.MethodPost, "/test-bucket?delete", strings.NewReader(body))
	signTestSigV4Request(t, req, "delete-user", "delete-secret-1")
	rec := httptest.NewRecorder()

	handler.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d body=%s", rec.Code, rec.Body.String())
	}
	responseBody := rec.Body.String()
	if !strings.Contains(responseBody, "allowed/file.txt") || !strings.Contains(responseBody, "<Deleted>") {
		t.Fatalf("expected allowed key deletion, body=%s", responseBody)
	}
	if !strings.Contains(responseBody, "private/file.txt") || !strings.Contains(responseBody, "AccessDenied") {
		t.Fatalf("expected per-key AccessDenied, body=%s", responseBody)
	}
	if _, err := svc.HeadObject("test-bucket", "allowed/file.txt"); !errors.Is(err, metadata.ErrObjectNotFound) {
		t.Fatalf("allowed object should be deleted, got err=%v", err)
	}
	if _, err := svc.HeadObject("test-bucket", "private/file.txt"); err != nil {
		t.Fatalf("private object should remain: %v", err)
	}
}

func signTestSigV4Request(t *testing.T, req *http.Request, accessKeyID, secretKey string) {
	t.Helper()

	amzDate := time.Now().UTC().Format("20060102T150405Z")
	date := amzDate[:8]
	region := "us-east-1"
	serviceName := "s3"
	scope := strings.Join([]string{date, region, serviceName, "aws4_request"}, "/")
	signedHeaders := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	signedHeadersRaw := strings.Join(signedHeaders, ";")
	payloadHash := "UNSIGNED-PAYLOAD"

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	canonicalRequest := strings.Join([]string{
		req.Method,
		req.URL.EscapedPath(),
		canonicalTestQuery(req.URL.RawQuery),
		"host:" + strings.TrimSpace(req.Host) + "\n" +
			"x-amz-content-sha256:" + payloadHash + "\n" +
			"x-amz-date:" + amzDate + "\n",
		signedHeadersRaw,
		payloadHash,
	}, "\n")
	canonicalHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hex.EncodeToString(canonicalHash[:]),
	}, "\n")
	signingKey := testHMAC(testHMAC(testHMAC(testHMAC([]byte("AWS4"+secretKey), date), region), serviceName), "aws4_request")
	signature := hex.EncodeToString(testHMAC(signingKey, stringToSign))

	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 "+
		"Credential="+accessKeyID+"/"+scope+", "+
		"SignedHeaders="+signedHeadersRaw+", "+
		"Signature="+signature)
}

func canonicalTestQuery(rawQuery string) string {
	values, _ := url.ParseQuery(rawQuery)
	pairs := make([]string, 0)
	for key, valueList := range values {
		if len(valueList) == 0 {
			pairs = append(pairs, awsTestQueryEscape(key)+"=")
			continue
		}
		for _, value := range valueList {
			pairs = append(pairs, awsTestQueryEscape(key)+"="+awsTestQueryEscape(value))
		}
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "&")
}

func awsTestQueryEscape(value string) string {
	encoded := url.QueryEscape(value)
	encoded = strings.ReplaceAll(encoded, "+", "%20")
	encoded = strings.ReplaceAll(encoded, "*", "%2A")
	encoded = strings.ReplaceAll(encoded, "%7E", "~")
	return encoded
}

func testHMAC(key []byte, value string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(value))
	return mac.Sum(nil)
}
