package auth

import (
	"log/slog"
	"net"
	"net/http"

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
				next.ServeHTTP(w, r.WithContext(WithRequestContext(r.Context(), authCtx)))
				return
			}

			if r.URL.Path == "/healthz" {
				next.ServeHTTP(w, r.WithContext(WithRequestContext(r.Context(), authCtx)))
				return
			}

			resolvedCtx, err := svc.AuthenticateRequest(r)
			if err != nil {
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

func clientIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err == nil && host != "" {
		return host
	}
	return remoteAddr
}
