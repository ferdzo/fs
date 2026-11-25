package api

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
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
)

func TestShouldDecodeAWSChunkedPayloadUnsignedTrailerMode(t *testing.T) {
	t.Parallel()

	req, err := http.NewRequest(http.MethodPut, "http://example.com/b/k", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
	if !shouldDecodeAWSChunkedPayload(req) {
		t.Fatalf("expected shouldDecodeAWSChunkedPayload to return true for STREAMING-UNSIGNED-PAYLOAD-TRAILER")
	}
}

func TestUnsupportedAWSChunkedContentEncodingWithoutStreamingMode(t *testing.T) {
	t.Parallel()

	req, err := http.NewRequest(http.MethodPut, "http://example.com/b/k", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("x-amz-content-sha256", "UNSIGNED-PAYLOAD")

	if !hasUnsupportedAWSChunkedPayload(req) {
		t.Fatalf("expected aws-chunked content encoding without streaming mode to be unsupported")
	}
	if shouldDecodeAWSChunkedPayload(req) {
		t.Fatalf("non-streaming aws-chunked content encoding must not trigger decoding")
	}
}

func TestPutObjectRejectsUnsignedAWSChunkedContentEncoding(t *testing.T) {
	handler, svc := newUploadLimitHandler(t, 1024)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, "/test-bucket/object.txt", strings.NewReader("4\r\nWiki\r\n0\r\n\r\n"))
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
	rec := httptest.NewRecorder()

	handler.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "InvalidArgument") {
		t.Fatalf("expected InvalidArgument response, body=%s", rec.Body.String())
	}
}

func TestAWSChunkedReaderPassThroughForPlainPayload(t *testing.T) {
	t.Parallel()

	plain := "PAR1\x00\x01\x02\x03binary-without-aws-chunked-header"
	reader := newAWSChunkedDecodingReader(strings.NewReader(plain))
	defer reader.Close()

	out, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(out) != plain {
		t.Fatalf("unexpected passthrough result: got %q want %q", string(out), plain)
	}
}

func TestAWSChunkedReaderDecodesChunkedPayload(t *testing.T) {
	t.Parallel()

	encoded := "" +
		"4\r\nWiki\r\n" +
		"5\r\npedia\r\n" +
		"0\r\n" +
		"x-amz-checksum-crc32:xxxx\r\n" +
		"\r\n"

	reader := newAWSChunkedDecodingReader(strings.NewReader(encoded))
	defer reader.Close()

	out, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(out) != "Wikipedia" {
		t.Fatalf("decoded payload mismatch: got %q want %q", string(out), "Wikipedia")
	}
}

func TestAWSChunkedReaderRejectsOversizedChunkHeader(t *testing.T) {
	t.Parallel()

	encoded := strings.Repeat("f", maxAWSChunkedLineBytes+1) + "\n"
	reader := newAWSChunkedDecodingReader(strings.NewReader(encoded))
	defer reader.Close()

	_, err := io.ReadAll(reader)
	if !errors.Is(err, service.ErrEntityTooLarge) {
		t.Fatalf("read error = %v, want ErrEntityTooLarge", err)
	}
}

func newDisabledAuthHandler(t *testing.T) (*Handler, *service.ObjectService) {
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
	authSvc, err := auth.NewService(auth.Config{Enabled: false}, md)
	if err != nil {
		t.Fatalf("new disabled auth service: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := NewHandler(svc, logger, logging.Config{}, authSvc, false)
	handler.setupRoutes()
	return handler, svc
}

func TestSignedStreamingDecodedWhenAuthDisabled(t *testing.T) {
	modes := []struct {
		name    string
		trailer bool
	}{
		{name: "payload", trailer: false},
		{name: "payload-trailer", trailer: true},
	}

	handlers := []struct {
		name string
		make func(t *testing.T) (*Handler, *service.ObjectService)
	}{
		{name: "nil-auth", make: newTestObjectHandler},
		{name: "disabled-auth", make: newDisabledAuthHandler},
	}

	payload := []byte("hello signed aws-chunked world without auth")
	chunks := [][]byte{payload[:12], payload[12:28], payload[28:]}
	sum := md5.Sum(payload)
	wantETag := `"` + hex.EncodeToString(sum[:]) + `"`

	for _, h := range handlers {
		for _, m := range modes {
			t.Run(h.name+"/"+m.name, func(t *testing.T) {
				handler, svc := h.make(t)
				if err := svc.CreateBucket("test-bucket"); err != nil {
					t.Fatalf("CreateBucket: %v", err)
				}

				req := httptest.NewRequest(http.MethodPut, "/test-bucket/obj.bin", nil)
				req.Host = "127.0.0.1"
				signChunkedPutRequest(t, req, "unused-secret", chunks, m.trailer)
				rec := httptest.NewRecorder()
				handler.router.ServeHTTP(rec, req)

				if rec.Code != http.StatusOK {
					t.Fatalf("PUT status = %d, want 200; body=%s", rec.Code, rec.Body.String())
				}
				if gotETag := rec.Header().Get("ETag"); gotETag != wantETag {
					t.Fatalf("ETag = %q, want %q", gotETag, wantETag)
				}

				getReq := httptest.NewRequest(http.MethodGet, "/test-bucket/obj.bin", nil)
				getRec := httptest.NewRecorder()
				handler.router.ServeHTTP(getRec, getReq)
				if getRec.Code != http.StatusOK {
					t.Fatalf("GET status = %d, want 200; body=%s", getRec.Code, getRec.Body.String())
				}
				if got := getRec.Body.String(); got != string(payload) {
					t.Fatalf("decoded object = %q, want %q", got, string(payload))
				}
			})
		}
	}
}

func TestSignedStreamingMultipartAndMultiDeleteWhenAuthDisabled(t *testing.T) {
	handler, svc := newDisabledAuthHandler(t)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// Multipart initiate
	initReq := httptest.NewRequest(http.MethodPost, "/test-bucket/mp.bin?uploads", nil)
	initRec := httptest.NewRecorder()
	handler.router.ServeHTTP(initRec, initReq)
	if initRec.Code != http.StatusOK {
		t.Fatalf("initiate status = %d, body=%s", initRec.Code, initRec.Body.String())
	}
	var initRes models.InitiateMultipartUploadResult
	if err := xml.Unmarshal(initRec.Body.Bytes(), &initRes); err != nil {
		t.Fatalf("unmarshal initiate result: %v", err)
	}

	// Signed streaming UploadPart while auth is disabled
	partPayload := []byte("multipart signed-streaming part payload")
	partReq := httptest.NewRequest(http.MethodPut, "/test-bucket/mp.bin?uploadId="+initRes.UploadID+"&partNumber=1", nil)
	partReq.Host = "127.0.0.1"
	signChunkedPutRequest(t, partReq, "unused-secret", [][]byte{partPayload[:15], partPayload[15:]}, false)
	partRec := httptest.NewRecorder()
	handler.router.ServeHTTP(partRec, partReq)
	if partRec.Code != http.StatusOK {
		t.Fatalf("upload part status = %d, body=%s", partRec.Code, partRec.Body.String())
	}

	completeBody := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + partRec.Header().Get("ETag") + `</ETag></Part></CompleteMultipartUpload>`
	compReq := httptest.NewRequest(http.MethodPost, "/test-bucket/mp.bin?uploadId="+initRes.UploadID, strings.NewReader(completeBody))
	compRec := httptest.NewRecorder()
	handler.router.ServeHTTP(compRec, compReq)
	if compRec.Code != http.StatusOK {
		t.Fatalf("complete status = %d, body=%s", compRec.Code, compRec.Body.String())
	}

	getRec := httptest.NewRecorder()
	handler.router.ServeHTTP(getRec, httptest.NewRequest(http.MethodGet, "/test-bucket/mp.bin", nil))
	if getRec.Code != http.StatusOK || getRec.Body.String() != string(partPayload) {
		t.Fatalf("multipart GET = (%d, %q), want (200, %q)", getRec.Code, getRec.Body.String(), string(partPayload))
	}

	// Signed trailer streaming multi-delete while auth is disabled
	delBody := `<Delete><Object><Key>mp.bin</Key></Object></Delete>`
	delReq := httptest.NewRequest(http.MethodPost, "/test-bucket?delete", nil)
	delReq.Host = "127.0.0.1"
	signChunkedPutRequest(t, delReq, "unused-secret", [][]byte{[]byte(delBody)}, true)
	delRec := httptest.NewRecorder()
	handler.router.ServeHTTP(delRec, delReq)
	if delRec.Code != http.StatusOK {
		t.Fatalf("multi-delete status = %d, body=%s", delRec.Code, delRec.Body.String())
	}
	if _, err := svc.HeadObject("test-bucket", "mp.bin"); !errors.Is(err, metadata.ErrObjectNotFound) {
		t.Fatalf("expected mp.bin deleted, got err=%v", err)
	}
}

func TestSignedStreamingRejectsUnsupportedModeWhenAuthDisabled(t *testing.T) {
	handler, svc := newDisabledAuthHandler(t)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, "/test-bucket/bad.bin", strings.NewReader("4\r\nWiki\r\n0\r\n\r\n"))
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-UNKNOWN")
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "AuthorizationHeaderMalformed") {
		t.Fatalf("expected AuthorizationHeaderMalformed, body=%s", rec.Body.String())
	}
}
