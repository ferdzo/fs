package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fs/metrics"
	"hash"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5/middleware"
)

func Middleware(
	svc *Service,
	logger *slog.Logger,
	auditEnabled bool,
	onError func(http.ResponseWriter, *http.Request, error),
) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authCtx := RequestContext{Authenticated: false, AuthType: "none"}
			if svc == nil || !svc.Config().Enabled {
				metrics.Default.ObserveAuth("bypass", "disabled", "auth_disabled")
				next.ServeHTTP(w, r.WithContext(WithRequestContext(r.Context(), authCtx)))
				return
			}

			if r.URL.Path == "/healthz" {
				metrics.Default.ObserveAuth("bypass", "none", "public_endpoint")
				next.ServeHTTP(w, r.WithContext(WithRequestContext(r.Context(), authCtx)))
				return
			}

			resolvedCtx, err := svc.AuthenticateRequest(r)
			if err != nil {
				metrics.Default.ObserveAuth("error", "sigv4", authErrorClass(err))
				if auditEnabled && logger != nil {
					requestID := middleware.GetReqID(r.Context())
					attrs := []any{
						"method", r.Method,
						"path", r.URL.Path,
						"remote_ip", clientIP(r.RemoteAddr),
						"error", err.Error(),
					}
					if requestID != "" {
						attrs = append(attrs, "request_id", requestID)
					}
					logger.Warn("auth_failed", attrs...)
				}
				if onError != nil {
					onError(w, r, err)
					return
				}
				http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
				return
			}

			if err := wrapPayloadHashVerifier(r); err != nil {
				metrics.Default.ObserveAuth("error", "sigv4", authErrorClass(err))
				if onError != nil {
					onError(w, r, err)
					return
				}
				http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
				return
			}

			metrics.Default.ObserveAuth("ok", resolvedCtx.AuthType, "none")
			if auditEnabled && logger != nil {
				requestID := middleware.GetReqID(r.Context())
				attrs := []any{
					"method", r.Method,
					"path", r.URL.Path,
					"remote_ip", clientIP(r.RemoteAddr),
					"access_key_id", resolvedCtx.AccessKeyID,
					"auth_type", resolvedCtx.AuthType,
				}
				if requestID != "" {
					attrs = append(attrs, "request_id", requestID)
				}
				logger.Info("auth_success", attrs...)
			}
			next.ServeHTTP(w, r.WithContext(WithRequestContext(r.Context(), resolvedCtx)))
		})
	}
}

func wrapPayloadHashVerifier(r *http.Request) error {
	if r == nil || r.Body == nil || r.Body == http.NoBody {
		return nil
	}
	payloadHash := resolvePayloadHash(r, false)
	if !payloadHashRequiresVerification(payloadHash) {
		return nil
	}
	if !isHexSHA256(payloadHash) {
		return ErrAuthorizationHeaderMalformed
	}
	expected, err := hex.DecodeString(strings.ToLower(payloadHash))
	if err != nil {
		return ErrAuthorizationHeaderMalformed
	}
	r.Body = &payloadHashVerifyingReadCloser{
		inner:    r.Body,
		hasher:   sha256.New(),
		expected: expected,
	}
	return nil
}

type payloadHashVerifyingReadCloser struct {
	inner    io.ReadCloser
	hasher   hash.Hash
	expected []byte
	done     bool
}

func (r *payloadHashVerifyingReadCloser) Read(p []byte) (int, error) {
	n, err := r.inner.Read(p)
	if n > 0 {
		_, _ = r.hasher.Write(p[:n])
	}
	if err == io.EOF && !r.done {
		r.done = true
		if !equalBytes(r.hasher.Sum(nil), r.expected) {
			return n, ErrSignatureDoesNotMatch
		}
	}
	return n, err
}

func (r *payloadHashVerifyingReadCloser) Close() error {
	return r.inner.Close()
}

func equalBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var diff byte
	for i := range left {
		diff |= left[i] ^ right[i]
	}
	return diff == 0
}

func authErrorClass(err error) string {
	switch {
	case errors.Is(err, ErrInvalidAccessKeyID):
		return "invalid_access_key"
	case errors.Is(err, ErrSignatureDoesNotMatch):
		return "signature_mismatch"
	case errors.Is(err, ErrAuthorizationHeaderMalformed):
		return "auth_header_malformed"
	case errors.Is(err, ErrRequestTimeTooSkewed):
		return "time_skew"
	case errors.Is(err, ErrExpiredToken):
		return "expired_token"
	case errors.Is(err, ErrNoAuthCredentials):
		return "missing_credentials"
	case errors.Is(err, ErrUnsupportedAuthScheme):
		return "unsupported_auth_scheme"
	case errors.Is(err, ErrInvalidPresign):
		return "invalid_presign"
	case errors.Is(err, ErrCredentialDisabled):
		return "credential_disabled"
	case errors.Is(err, ErrAccessDenied):
		return "access_denied"
	default:
		return "other"
	}
}

func clientIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err == nil && host != "" {
		return host
	}
	return remoteAddr
}
