package api

import (
	"fs/auth"
	"net/http"
	"testing"
)

func TestMapToS3ErrorDecodedContentLengthMismatch(t *testing.T) {
	got := mapToS3Error(auth.ErrDecodedContentLengthMismatch)
	if got.Status != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", got.Status, http.StatusBadRequest)
	}
	if got.Code != s3ErrInvalidArgument.Code {
		t.Fatalf("code = %q, want %q", got.Code, s3ErrInvalidArgument.Code)
	}
}

func TestMapToS3ErrorChunkSignatureMismatchStaysForbidden(t *testing.T) {
	got := mapToS3Error(auth.ErrChunkSignatureMismatch)
	if got.Status != http.StatusForbidden || got.Code != "SignatureDoesNotMatch" {
		t.Fatalf("got status=%d code=%q, want 403 SignatureDoesNotMatch", got.Status, got.Code)
	}
}
