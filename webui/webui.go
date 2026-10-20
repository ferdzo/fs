// Package webui serves a minimal read-only dashboard for the object store.
package webui

import (
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

//go:embed index.html
var files embed.FS

// Summary is the dashboard overview payload.
type Summary struct {
	Buckets       int64 `json:"buckets"`
	Objects       int64 `json:"objects"`
	LogicalBytes  int64 `json:"logicalBytes"`
	PhysicalBytes int64 `json:"physicalBytes"`
}

// Bucket describes one bucket in the overview.
type Bucket struct {
	Name    string `json:"name"`
	Objects int64  `json:"objects"`
	Bytes   int64  `json:"bytes"`
}

// Object describes one object in a listing.
type Object struct {
	Key          string `json:"key"`
	Size         int64  `json:"size"`
	ETag         string `json:"etag"`
	LastModified string `json:"lastModified"`
}

// Provider supplies dashboard data. Implementations must be safe for concurrent use.
type Provider interface {
	Summary(context.Context) (Summary, error)
	Buckets(context.Context) ([]Bucket, error)
	Objects(ctx context.Context, bucket, prefix string) ([]Object, error)
}

// Config controls where the dashboard is mounted and how API calls are authenticated.
type Config struct {
	BasePath string // defaults to /ui
	Token    string // when set, /api/* requires "Authorization: Bearer <token>"
	// Authorize replaces the static token check. It may return a request with an
	// enriched context; Provider calls see that context.
	Authorize func(*http.Request) (*http.Request, bool)
}

type handler struct {
	base      string
	token     string
	authorize func(*http.Request) (*http.Request, bool)
	provider  Provider
	page      []byte
}

// NewHandler returns the dashboard handler.
func NewHandler(cfg Config, provider Provider) (http.Handler, error) {
	if provider == nil {
		return nil, errors.New("webui: provider is required")
	}
	base := "/" + strings.Trim(strings.TrimSpace(cfg.BasePath), "/")
	if base == "/" {
		base = "/ui"
	}
	page, err := files.ReadFile("index.html")
	if err != nil {
		return nil, err
	}
	return &handler{base: base, token: cfg.Token, authorize: cfg.Authorize, provider: provider, page: page}, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	switch {
	case r.URL.Path == h.base, r.URL.Path == h.base+"/":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		_, _ = w.Write(h.page)
	case strings.HasPrefix(r.URL.Path, h.base+"/api/"):
		request, ok := h.authorizeRequest(r)
		if !ok {
			if h.authorize != nil {
				w.Header().Set("WWW-Authenticate", `Basic realm="fs"`)
			}
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		h.api(w, request)
	default:
		http.NotFound(w, r)
	}
}

func (h *handler) api(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case h.base + "/api/summary":
		h.summary(w, r)
	case h.base + "/api/buckets":
		h.buckets(w, r)
	case h.base + "/api/objects":
		h.objects(w, r)
	default:
		http.NotFound(w, r)
	}
}

// authorizeRequest applies the configured Authorize hook, falling back to the
// static bearer token.
func (h *handler) authorizeRequest(r *http.Request) (*http.Request, bool) {
	if h.authorize != nil {
		return h.authorize(r)
	}
	return r, h.authorized(r)
}

func (h *handler) summary(w http.ResponseWriter, r *http.Request) {
	summary, err := h.provider.Summary(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	writeJSON(w, http.StatusOK, summary)
}

func (h *handler) buckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := h.provider.Buckets(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	writeJSON(w, http.StatusOK, buckets)
}

func (h *handler) objects(w http.ResponseWriter, r *http.Request) {
	bucket := r.URL.Query().Get("bucket")
	if bucket == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bucket is required"})
		return
	}
	objects, err := h.provider.Objects(r.Context(), bucket, r.URL.Query().Get("prefix"))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	writeJSON(w, http.StatusOK, objects)
}

func (h *handler) authorized(r *http.Request) bool {
	if h.token == "" {
		return true
	}
	const prefix = "Bearer "
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(header[len(prefix):]), []byte(h.token)) == 1
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
