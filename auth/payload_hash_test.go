package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestPayloadHashVerifierAllowsMatchingBody(t *testing.T) {
	body := "payload"
	req := newPayloadHashRequest(t, body, body)

	if err := wrapPayloadHashVerifier(req); err != nil {
		t.Fatalf("wrapPayloadHashVerifier returned error: %v", err)
	}
	got, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}
	if string(got) != body {
		t.Fatalf("unexpected body: got %q want %q", string(got), body)
	}
}

func TestPayloadHashVerifierRejectsMismatchedBody(t *testing.T) {
	req := newPayloadHashRequest(t, "signed-payload", "actual-payload")

	if err := wrapPayloadHashVerifier(req); err != nil {
		t.Fatalf("wrapPayloadHashVerifier returned error: %v", err)
	}
	_, err := io.ReadAll(req.Body)
	if !errors.Is(err, ErrSignatureDoesNotMatch) {
		t.Fatalf("ReadAll error = %v, want ErrSignatureDoesNotMatch", err)
	}
}

func TestPayloadSigningRejectsSignedStreamingMode(t *testing.T) {
	req, err := http.NewRequest(http.MethodPut, "http://example.com/b/k", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")

	err = validatePayloadSigningMode(req, &sigV4Input{})
	if !errors.Is(err, ErrAuthorizationHeaderMalformed) {
		t.Fatalf("validatePayloadSigningMode error = %v, want ErrAuthorizationHeaderMalformed", err)
	}
}

func TestPayloadSigningAllowsUnsignedStreamingMode(t *testing.T) {
	req, err := http.NewRequest(http.MethodPut, "http://example.com/b/k", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")

	if err := validatePayloadSigningMode(req, &sigV4Input{}); err != nil {
		t.Fatalf("validatePayloadSigningMode returned error: %v", err)
	}
}

func newPayloadHashRequest(t *testing.T, signedBody, actualBody string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, "http://example.com/b/k", strings.NewReader(actualBody))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(signedBody))
	req.Header.Set("x-amz-content-sha256", hex.EncodeToString(sum[:]))
	return req
}
