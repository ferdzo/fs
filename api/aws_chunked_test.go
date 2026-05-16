package api

import (
	"io"
	"net/http"
	"strings"
	"testing"
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
		"\r\n" +
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
