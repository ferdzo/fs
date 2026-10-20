package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
