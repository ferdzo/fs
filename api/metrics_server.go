package api

import (
	"context"
	"errors"
	"fs/metrics"
	"fs/utils"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// NewMetricsHandler returns an HTTP handler serving only Prometheus metrics
// at GET/HEAD /metrics.
func NewMetricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		payload := metrics.Default.RenderPrometheus()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodHead {
			return
		}
		_, _ = w.Write([]byte(payload))
	})
	return mux
}

// MetricsServer serves /metrics on a dedicated internal listener separate from
// the S3 API router.
type MetricsServer struct {
	server   *http.Server
	listener net.Listener
	logger   *slog.Logger

	shutdownOnce sync.Once
	shutdownErr  error
}

// StartMetricsServer binds address synchronously and serves /metrics until ctx
// is cancelled or Shutdown is called.
func StartMetricsServer(ctx context.Context, address string, logger *slog.Logger) (*MetricsServer, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if logger == nil {
		logger = slog.Default()
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	srv := &http.Server{
		Addr:              listener.Addr().String(),
		Handler:           NewMetricsHandler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ms := &MetricsServer{
		server:   srv,
		listener: listener,
		logger:   logger,
	}

	logger.Info("metrics_server_starting", "address", ms.Addr())
	utils.Go("metrics-http-serve", func() {
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics_server_failed", "error", err)
		}
	})
	utils.Go("metrics-http-shutdown", func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = ms.Shutdown(shutdownCtx)
	})

	return ms, nil
}

// Addr returns the bound listener address.
func (s *MetricsServer) Addr() string {
	return s.listener.Addr().String()
}

// Shutdown gracefully stops the metrics HTTP server. It is safe to call more
// than once.
func (s *MetricsServer) Shutdown(ctx context.Context) error {
	s.shutdownOnce.Do(func() {
		if ctx == nil {
			ctx = context.Background()
		}
		s.shutdownErr = s.server.Shutdown(ctx)
	})
	return s.shutdownErr
}
