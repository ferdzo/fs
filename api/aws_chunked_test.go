package api

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fs/service"
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
