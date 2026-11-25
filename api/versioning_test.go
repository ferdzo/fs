package api

import (
	"encoding/xml"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"fs/metadata"
	"fs/models"
)

func doVersioningRequest(handler *Handler, method, target, body string) *httptest.ResponseRecorder {
	var reader *strings.Reader
	if body != "" {
		reader = strings.NewReader(body)
	} else {
		reader = strings.NewReader("")
	}
	request := httptest.NewRequest(method, target, reader)
	recorder := httptest.NewRecorder()
	handler.router.ServeHTTP(recorder, request)
	return recorder
}

func enableVersioning(t *testing.T, handler *Handler, bucket string) {
	t.Helper()
	recorder := doVersioningRequest(handler, http.MethodPut, "/"+bucket+"?versioning",
		`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("enable versioning: status %d body %s", recorder.Code, recorder.Body.String())
	}
}

func putVersionedObject(t *testing.T, handler *Handler, bucket, key, body string) string {
	t.Helper()
	recorder := doVersioningRequest(handler, http.MethodPut, "/"+bucket+"/"+key, body)
	if recorder.Code != http.StatusOK {
		t.Fatalf("put object: status %d body %s", recorder.Code, recorder.Body.String())
	}
	return recorder.Header().Get("x-amz-version-id")
}

func TestVersioningSubresourceAndObjectVersions(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	if err := svc.CreateBucket("versioned-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	enableVersioning(t, handler, "versioned-bucket")

	recorder := doVersioningRequest(handler, http.MethodGet, "/versioned-bucket?versioning", "")
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "<Status>Enabled</Status>") {
		t.Fatalf("get versioning: status %d body %s", recorder.Code, recorder.Body.String())
	}

	v1 := putVersionedObject(t, handler, "versioned-bucket", "key", "first")
	v2 := putVersionedObject(t, handler, "versioned-bucket", "key", "second")
	if v1 == "" || v2 == "" || v1 == v2 {
		t.Fatalf("version ids = %q, %q", v1, v2)
	}

	recorder = doVersioningRequest(handler, http.MethodGet, "/versioned-bucket/key", "")
	if recorder.Code != http.StatusOK || recorder.Body.String() != "second" {
		t.Fatalf("get latest: status %d body %q", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("x-amz-version-id"); got != v2 {
		t.Fatalf("latest version header = %q, want %q", got, v2)
	}

	recorder = doVersioningRequest(handler, http.MethodGet, "/versioned-bucket/key?versionId="+v1, "")
	if recorder.Code != http.StatusOK || recorder.Body.String() != "first" {
		t.Fatalf("get v1: status %d body %q", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("x-amz-version-id"); got != v1 {
		t.Fatalf("v1 version header = %q, want %q", got, v1)
	}

	recorder = doVersioningRequest(handler, http.MethodHead, "/versioned-bucket/key?versionId="+v1, "")
	if recorder.Code != http.StatusOK || recorder.Header().Get("x-amz-version-id") != v1 {
		t.Fatalf("head v1: status %d headers %v", recorder.Code, recorder.Header())
	}

	recorder = doVersioningRequest(handler, http.MethodDelete, "/versioned-bucket/key", "")
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("delete: status %d", recorder.Code)
	}
	markerID := recorder.Header().Get("x-amz-version-id")
	if recorder.Header().Get("x-amz-delete-marker") != "true" || markerID == "" {
		t.Fatalf("delete headers = %v", recorder.Header())
	}

	recorder = doVersioningRequest(handler, http.MethodGet, "/versioned-bucket/key", "")
	if recorder.Code != http.StatusNotFound || recorder.Header().Get("x-amz-delete-marker") != "true" {
		t.Fatalf("latest marker: status %d headers %v", recorder.Code, recorder.Header())
	}

	recorder = doVersioningRequest(handler, http.MethodGet, "/versioned-bucket/key?versionId="+markerID, "")
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("explicit marker: status %d, want 405", recorder.Code)
	}

	recorder = doVersioningRequest(handler, http.MethodGet, "/versioned-bucket/key?versionId="+v2, "")
	if recorder.Code != http.StatusOK || recorder.Body.String() != "second" {
		t.Fatalf("v2 under marker: status %d body %q", recorder.Code, recorder.Body.String())
	}

	recorder = doVersioningRequest(handler, http.MethodGet, "/versioned-bucket?versions", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("list versions: status %d body %s", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	for _, want := range []string{v1, v2, markerID, "<DeleteMarker>", "<IsLatest>"} {
		if !strings.Contains(body, want) {
			t.Fatalf("list versions missing %q in %s", want, body)
		}
	}
}

func TestMultiDeleteExplicitVersion(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	if err := svc.CreateBucket("versioned-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	enableVersioning(t, handler, "versioned-bucket")

	v1 := putVersionedObject(t, handler, "versioned-bucket", "key", "first")
	putVersionedObject(t, handler, "versioned-bucket", "key", "second")

	body := `<Delete><Object><Key>key</Key><VersionId>` + v1 + `</VersionId></Object></Delete>`
	recorder := doVersioningRequest(handler, http.MethodPost, "/versioned-bucket?delete", body)
	if recorder.Code != http.StatusOK {
		t.Fatalf("multi-delete: status %d body %s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), v1) {
		t.Fatalf("multi-delete response missing version %q: %s", v1, recorder.Body.String())
	}

	if recorder := doVersioningRequest(handler, http.MethodGet, "/versioned-bucket/key?versionId="+v1, ""); recorder.Code != http.StatusNotFound {
		t.Fatalf("deleted version: status %d, want 404", recorder.Code)
	}
	recorder = doVersioningRequest(handler, http.MethodGet, "/versioned-bucket/key", "")
	if recorder.Code != http.StatusOK || recorder.Body.String() != "second" {
		t.Fatalf("latest after version delete: status %d body %q", recorder.Code, recorder.Body.String())
	}
}

func TestCopyFromExplicitVersion(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	if err := svc.CreateBucket("versioned-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	enableVersioning(t, handler, "versioned-bucket")

	v1 := putVersionedObject(t, handler, "versioned-bucket", "source", "original")
	putVersionedObject(t, handler, "versioned-bucket", "source", "replacement")

	request := httptest.NewRequest(http.MethodPut, "/versioned-bucket/copy", strings.NewReader(""))
	request.Header.Set("x-amz-copy-source", "/versioned-bucket/source?versionId="+v1)
	recorder := httptest.NewRecorder()
	handler.router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("copy: status %d body %s", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("x-amz-copy-source-version-id"); got != v1 {
		t.Fatalf("copy source version = %q, want %q", got, v1)
	}

	recorder = doVersioningRequest(handler, http.MethodGet, "/versioned-bucket/copy", "")
	if recorder.Code != http.StatusOK || recorder.Body.String() != "original" {
		t.Fatalf("copied object: status %d body %q", recorder.Code, recorder.Body.String())
	}
}

func TestMultiDeleteUnversionedRequestOnVersionedBucket(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	if err := svc.CreateBucket("versioned-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	enableVersioning(t, handler, "versioned-bucket")

	v1 := putVersionedObject(t, handler, "versioned-bucket", "keep-history", "v1-data")
	_ = putVersionedObject(t, handler, "versioned-bucket", "keep-history", "v2-data")

	body := `<Delete><Object><Key>keep-history</Key></Object></Delete>`
	rec := doVersioningRequest(handler, http.MethodPost, "/versioned-bucket?delete", body)
	if rec.Code != http.StatusOK {
		t.Fatalf("multi-delete status = %d body=%s", rec.Code, rec.Body.String())
	}
	var delRes models.DeleteObjectsResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &delRes); err != nil {
		t.Fatalf("unmarshal DeleteResult: %v", err)
	}
	if len(delRes.Deleted) != 1 || !delRes.Deleted[0].DeleteMarker || delRes.Deleted[0].DeleteMarkerVersionID == "" {
		t.Fatalf("expected delete marker entry, got %+v", delRes.Deleted)
	}

	// Historical version must still exist and block DeleteBucket until purged.
	getOld := doVersioningRequest(handler, http.MethodGet, "/versioned-bucket/keep-history?versionId="+v1, "")
	if getOld.Code != http.StatusOK || getOld.Body.String() != "v1-data" {
		t.Fatalf("historical version lost after multi-delete: status=%d body=%q", getOld.Code, getOld.Body.String())
	}
	delBucket := doVersioningRequest(handler, http.MethodDelete, "/versioned-bucket", "")
	if delBucket.Code != http.StatusConflict {
		t.Fatalf("DeleteBucket with remaining versions = %d, want 409", delBucket.Code)
	}
}

func TestListObjectsV1AndV2ExcludeDeleteMarkers(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	if err := svc.CreateBucket("versioned-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	enableVersioning(t, handler, "versioned-bucket")

	putVersionedObject(t, handler, "versioned-bucket", "alive.txt", "alive")
	putVersionedObject(t, handler, "versioned-bucket", "dir-deleted/item.txt", "gone")
	putVersionedObject(t, handler, "versioned-bucket", "z-deleted.txt", "gone")

	for _, key := range []string{"dir-deleted/item.txt", "z-deleted.txt"} {
		rec := doVersioningRequest(handler, http.MethodDelete, "/versioned-bucket/"+key, "")
		if rec.Code != http.StatusNoContent {
			t.Fatalf("delete %s status = %d", key, rec.Code)
		}
	}

	v1Rec := doVersioningRequest(handler, http.MethodGet, "/versioned-bucket?delimiter=/&max-keys=1", "")
	if v1Rec.Code != http.StatusOK {
		t.Fatalf("V1 status = %d body=%s", v1Rec.Code, v1Rec.Body.String())
	}
	var v1 models.ListBucketResultV1
	if err := xml.Unmarshal(v1Rec.Body.Bytes(), &v1); err != nil {
		t.Fatalf("unmarshal V1: %v", err)
	}
	if len(v1.Contents) != 1 || v1.Contents[0].Key != "alive.txt" {
		t.Fatalf("V1 Contents = %+v, want only alive.txt", v1.Contents)
	}
	if len(v1.CommonPrefixes) != 0 {
		t.Fatalf("V1 CommonPrefixes = %+v, want empty (dir-deleted/ only has delete marker)", v1.CommonPrefixes)
	}
	if v1.IsTruncated {
		t.Fatalf("V1 IsTruncated = true, want false (trailing delete marker must not truncate)")
	}

	v2Rec := doVersioningRequest(handler, http.MethodGet, "/versioned-bucket?list-type=2&delimiter=/&max-keys=1", "")
	if v2Rec.Code != http.StatusOK {
		t.Fatalf("V2 status = %d body=%s", v2Rec.Code, v2Rec.Body.String())
	}
	var v2 models.ListBucketResultV2
	if err := xml.Unmarshal(v2Rec.Body.Bytes(), &v2); err != nil {
		t.Fatalf("unmarshal V2: %v", err)
	}
	if v2.KeyCount != 1 || len(v2.Contents) != 1 || v2.Contents[0].Key != "alive.txt" {
		t.Fatalf("V2 Contents = %+v, want only alive.txt", v2.Contents)
	}
	if len(v2.CommonPrefixes) != 0 || v2.IsTruncated {
		t.Fatalf("V2 CommonPrefixes=%+v IsTruncated=%v, want empty/false", v2.CommonPrefixes, v2.IsTruncated)
	}
}

func TestIfNoneMatchOnDeleteMarkerAndConcurrentPuts(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	if err := svc.CreateBucket("versioned-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	enableVersioning(t, handler, "versioned-bucket")

	putVersionedObject(t, handler, "versioned-bucket", "item", "first")
	if rec := doVersioningRequest(handler, http.MethodDelete, "/versioned-bucket/item", ""); rec.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d", rec.Code)
	}

	// PUT with If-None-Match: * over a delete marker must succeed (not 500).
	req := httptest.NewRequest(http.MethodPut, "/versioned-bucket/item", strings.NewReader("recreated"))
	req.Header.Set("If-None-Match", "*")
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT If-None-Match:* over delete marker = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	// PUT with If-None-Match: * over a live object must return 412.
	req2 := httptest.NewRequest(http.MethodPut, "/versioned-bucket/item", strings.NewReader("conflict"))
	req2.Header.Set("If-None-Match", "*")
	rec2 := httptest.NewRecorder()
	handler.router.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusPreconditionFailed {
		t.Fatalf("PUT If-None-Match:* over live object = %d, want 412; body=%s", rec2.Code, rec2.Body.String())
	}

	// Concurrent PUTs with If-None-Match: * on an absent key: exactly one wins.
	const workers = 6
	start := make(chan struct{})
	var wins atomic.Int32
	var conflicts atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			r := httptest.NewRequest(http.MethodPut, "/versioned-bucket/race-key", strings.NewReader("payload"))
			r.Header.Set("If-None-Match", "*")
			w := httptest.NewRecorder()
			handler.router.ServeHTTP(w, r)
			switch w.Code {
			case http.StatusOK:
				wins.Add(1)
			case http.StatusPreconditionFailed:
				conflicts.Add(1)
			default:
				t.Errorf("worker %d unexpected status %d: %s", id, w.Code, w.Body.String())
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if wins.Load() != 1 || conflicts.Load() != workers-1 {
		t.Fatalf("concurrent If-None-Match:* wins=%d conflicts=%d, want 1 and %d", wins.Load(), conflicts.Load(), workers-1)
	}
}

func TestListObjectVersionsLegacyNullAndMarkers(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	if err := svc.CreateBucket("legacy-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if _, err := svc.PutObject("legacy-bucket", "a-legacy", "text/plain", strings.NewReader("legacy")); err != nil {
		t.Fatalf("PutObject legacy: %v", err)
	}
	enableVersioning(t, handler, "legacy-bucket")
	v2 := putVersionedObject(t, handler, "legacy-bucket", "logs/b", "v2")

	p1Rec := doVersioningRequest(handler, http.MethodGet, "/legacy-bucket?versions&max-keys=1", "")
	if p1Rec.Code != http.StatusOK {
		t.Fatalf("page 1 status = %d body=%s", p1Rec.Code, p1Rec.Body.String())
	}
	var p1 listVersionsResult
	if err := xml.Unmarshal(p1Rec.Body.Bytes(), &p1); err != nil {
		t.Fatalf("unmarshal p1: %v", err)
	}
	if len(p1.Versions) != 1 || p1.Versions[0].VersionID != "null" || !p1.IsTruncated || p1.NextVersionIDMarker != "null" {
		t.Fatalf("unexpected page 1: %+v", p1)
	}

	p2Rec := doVersioningRequest(handler, http.MethodGet,
		"/legacy-bucket?versions&key-marker="+p1.NextKeyMarker+"&version-id-marker="+p1.NextVersionIDMarker, "")
	if p2Rec.Code != http.StatusOK {
		t.Fatalf("page 2 status = %d body=%s", p2Rec.Code, p2Rec.Body.String())
	}
	var p2 listVersionsResult
	if err := xml.Unmarshal(p2Rec.Body.Bytes(), &p2); err != nil {
		t.Fatalf("unmarshal p2: %v", err)
	}
	if len(p2.Versions) != 1 || p2.Versions[0].Key != "logs/b" || p2.Versions[0].VersionID != v2 {
		t.Fatalf("unexpected page 2: %+v", p2)
	}

	prefRec := doVersioningRequest(handler, http.MethodGet, "/legacy-bucket?versions&prefix=logs/&key-marker=a-legacy", "")
	if prefRec.Code != http.StatusOK {
		t.Fatalf("prefixed status = %d body=%s", prefRec.Code, prefRec.Body.String())
	}
	if !strings.Contains(prefRec.Body.String(), "<Key>logs/b</Key>") {
		t.Fatalf("expected logs/b when key-marker precedes prefix, body=%s", prefRec.Body.String())
	}
}

func TestVersioningAuthPermissions(t *testing.T) {
	handler, svc, authSvc := newAuthorizedDeleteHandler(t)
	handler.setupRoutes()
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if err := svc.SetBucketVersioning("test-bucket", "Enabled"); err != nil {
		t.Fatalf("SetBucketVersioning: %v", err)
	}
	m, err := svc.PutObject("test-bucket", "doc.txt", "text/plain", strings.NewReader("v1"))
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	v1 := m.VersionID

	// Principal has CreateBucket, PutObject, DeleteObject, GetObject — NOT
	// PutBucketVersioning, DeleteObjectVersion, or GetObjectVersion.
	createDeleteUserWithStatements(t, authSvc, []models.AuthPolicyStatement{
		{
			Effect:  "allow",
			Actions: []string{"s3:CreateBucket", "s3:PutObject", "s3:DeleteObject", "s3:GetObject"},
			Bucket:  "test-bucket",
			Prefix:  "*",
		},
	})

	putVerReq := httptest.NewRequest(http.MethodPut, "/test-bucket?versioning",
		strings.NewReader(`<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>`))
	putVerReq.Host = "127.0.0.1"
	signTestSigV4Request(t, putVerReq, "delete-user", "delete-secret-1")
	putVerRec := httptest.NewRecorder()
	handler.router.ServeHTTP(putVerRec, putVerReq)
	if putVerRec.Code != http.StatusForbidden {
		t.Fatalf("PUT ?versioning without s3:PutBucketVersioning = %d, want 403", putVerRec.Code)
	}

	delVerReq := httptest.NewRequest(http.MethodDelete, "/test-bucket/doc.txt?versionId="+v1, nil)
	delVerReq.Host = "127.0.0.1"
	signTestSigV4Request(t, delVerReq, "delete-user", "delete-secret-1")
	delVerRec := httptest.NewRecorder()
	handler.router.ServeHTTP(delVerRec, delVerReq)
	if delVerRec.Code != http.StatusForbidden {
		t.Fatalf("DELETE ?versionId without s3:DeleteObjectVersion = %d, want 403", delVerRec.Code)
	}

	batchBody := `<Delete><Object><Key>doc.txt</Key><VersionId>` + v1 + `</VersionId></Object></Delete>`
	batchReq := httptest.NewRequest(http.MethodPost, "/test-bucket?delete", strings.NewReader(batchBody))
	batchReq.Host = "127.0.0.1"
	signTestSigV4Request(t, batchReq, "delete-user", "delete-secret-1")
	batchRec := httptest.NewRecorder()
	handler.router.ServeHTTP(batchRec, batchReq)
	if batchRec.Code != http.StatusOK || !strings.Contains(batchRec.Body.String(), "AccessDenied") {
		t.Fatalf("batch delete ?versionId expected AccessDenied entry, code=%d body=%s", batchRec.Code, batchRec.Body.String())
	}

	copyReq := httptest.NewRequest(http.MethodPut, "/test-bucket/copied.txt", nil)
	copyReq.Host = "127.0.0.1"
	copyReq.Header.Set("x-amz-copy-source", "/test-bucket/doc.txt?versionId="+v1)
	signTestSigV4Request(t, copyReq, "delete-user", "delete-secret-1")
	copyRec := httptest.NewRecorder()
	handler.router.ServeHTTP(copyRec, copyReq)
	if copyRec.Code != http.StatusForbidden {
		t.Fatalf("copy from explicit versionId without s3:GetObjectVersion = %d, want 403", copyRec.Code)
	}

	if _, err := svc.HeadObjectVersion("test-bucket", "doc.txt", v1); errors.Is(err, metadata.ErrVersionNotFound) {
		t.Fatal("v1 must still exist after denied deletes")
	}
}
