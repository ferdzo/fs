package logging

import (
	"fs/metrics"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type Config struct {
	Level     slog.Level
	LevelName string
	Format    string
	Audit     bool
	AddSource bool
	DebugMode bool
}

func ConfigFromEnv() Config {
	levelName := strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL")))
	format := strings.ToLower(strings.TrimSpace(os.Getenv("LOG_FORMAT")))
	return ConfigFromValues(levelName, format, envBool("AUDIT_LOG", true))
}

func ConfigFromValues(levelName, format string, audit bool) Config {
	levelName = strings.ToLower(strings.TrimSpace(levelName))
	if levelName == "" {
		levelName = "info"
	}
	level := parseLevel(levelName)
	levelName = strings.ToUpper(level.String())

	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" {
		format = "text"
	}
	if format != "json" && format != "text" {
		format = "text"
	}

	debugMode := level <= slog.LevelDebug
	return Config{
		Level:     level,
		LevelName: levelName,
		Format:    format,
		Audit:     audit,
		AddSource: debugMode,
		DebugMode: debugMode,
	}
}

func NewLogger(cfg Config) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level:     cfg.Level,
		AddSource: cfg.AddSource,
	}
	opts.ReplaceAttr = func(_ []string, attr slog.Attr) slog.Attr {
		if attr.Key == slog.SourceKey {
			if src, ok := attr.Value.Any().(*slog.Source); ok && src != nil {
				attr.Key = "src"
				attr.Value = slog.StringValue(filepath.Base(src.File) + ":" + strconv.Itoa(src.Line))
			}
		}
		return attr
	}

	var handler slog.Handler
	if cfg.Format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}

func HTTPMiddleware(logger *slog.Logger, cfg Config) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			metrics.Default.IncHTTPInFlight()
			defer metrics.Default.DecHTTPInFlight()
			requestID := middleware.GetReqID(r.Context())
			if requestID != "" {
				ww.Header().Set("x-amz-request-id", requestID)
			}

			next.ServeHTTP(ww, r)

			elapsed := time.Since(start)
			status := ww.Status()
			if status == 0 {
				status = http.StatusOK
			}
			route := metricRouteLabel(r)
			metrics.Default.ObserveHTTPRequest(r.Method, route, status, elapsed, ww.BytesWritten())

			if !cfg.Audit && !cfg.DebugMode {
				return
			}

			attrs := []any{
				"method", r.Method,
				"path", r.URL.Path,
				"status", status,
				"bytes", ww.BytesWritten(),
				"duration_ms", float64(elapsed.Nanoseconds()) / 1_000_000.0,
				"remote_addr", r.RemoteAddr,
			}
			if requestID != "" {
				attrs = append(attrs, "request_id", requestID)
			}

			if cfg.DebugMode {
				attrs = append(attrs,
					"query", r.URL.RawQuery,
					"user_agent", r.UserAgent(),
					"content_length", r.ContentLength,
					"content_type", r.Header.Get("Content-Type"),
					"x_amz_sha256", r.Header.Get("x-amz-content-sha256"),
				)
				logger.Debug("http_request", attrs...)
				return
			}

			logger.Info("http_request", attrs...)
		})
	}
}

func metricRouteLabel(r *http.Request) string {
	if r == nil || r.URL == nil {
		return "/unknown"
	}

	if routeCtx := chi.RouteContext(r.Context()); routeCtx != nil {
		if pattern := strings.TrimSpace(routeCtx.RoutePattern()); pattern != "" {
			return pattern
		}
	}

	path := strings.TrimSpace(r.URL.Path)
	if path == "" || path == "/" {
		return "/"
	}
	if path == "/healthz" || path == "/metrics" {
		return path
	}

	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return "/"
	}
	if !strings.Contains(trimmed, "/") {
		return "/{bucket}"
	}
	return "/{bucket}/*"
}

func envBool(key string, defaultValue bool) bool {
	raw := os.Getenv(key)
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return defaultValue
	}
	return value
}

func parseLevel(levelName string) slog.Level {
	switch levelName {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
