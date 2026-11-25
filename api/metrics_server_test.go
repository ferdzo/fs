package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsServerServesPrometheusOnly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv, err := StartMetricsServer(ctx, "127.0.0.1:0", logger)
	if err != nil {
		t.Fatalf("StartMetricsServer: %v", err)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}()

	baseURL := "http://" + srv.Addr()

	resp, err := http.Get(baseURL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read /metrics body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(resp.Header.Get("Content-Type"), "text/plain") {
		t.Fatalf("unexpected Content-Type: %q", resp.Header.Get("Content-Type"))
	}
	if !strings.Contains(string(body), "fs_uptime_seconds") {
		t.Fatalf("metrics body missing fs_uptime_seconds: %s", string(body))
	}

	headResp, err := http.Head(baseURL + "/metrics")
	if err != nil {
		t.Fatalf("HEAD /metrics: %v", err)
	}
	headBody, err := io.ReadAll(headResp.Body)
	headResp.Body.Close()
	if err != nil {
		t.Fatalf("read HEAD /metrics body: %v", err)
	}
	if headResp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD /metrics status = %d, want 200", headResp.StatusCode)
	}
	if len(headBody) != 0 {
		t.Fatalf("HEAD /metrics returned non-empty body: %q", string(headBody))
	}

	notFoundResp, err := http.Get(baseURL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz on metrics server: %v", err)
	}
	notFoundResp.Body.Close()
	if notFoundResp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /healthz status = %d, want 404", notFoundResp.StatusCode)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("explicit Shutdown: %v", err)
	}
}

func TestS3RouterDoesNotReserveMetricsBucketName(t *testing.T) {
	handler, _ := newTestObjectHandler(t)

	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT /metrics (create bucket) status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	putObj := httptest.NewRecorder()
	handler.router.ServeHTTP(putObj, httptest.NewRequest(http.MethodPut, "/metrics/obj.txt", strings.NewReader("ok")))
	if putObj.Code != http.StatusOK {
		t.Fatalf("PUT /metrics/obj.txt status = %d, want 200; body=%s", putObj.Code, putObj.Body.String())
	}

	listRec := httptest.NewRecorder()
	handler.router.ServeHTTP(listRec, httptest.NewRequest(http.MethodGet, "/metrics?list-type=2", nil))
	if listRec.Code != http.StatusOK {
		t.Fatalf("GET /metrics?list-type=2 status = %d, want 200; body=%s", listRec.Code, listRec.Body.String())
	}
	if !strings.Contains(listRec.Body.String(), "<Key>obj.txt</Key>") {
		t.Fatalf("expected obj.txt in metrics bucket listing, body=%s", listRec.Body.String())
	}
}
