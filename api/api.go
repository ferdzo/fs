package api

import (
	"fmt"
	"fs/service"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type Handler struct {
	router *chi.Mux
	svc    *service.ObjectService
}

func NewHandler(svc *service.ObjectService) *Handler {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)

	h := &Handler{
		router: r,
		svc:    svc,
	}
	return h
}

func (h *Handler) setupRoutes() {
	h.router.Use(middleware.Logger)
	h.router.Get("/", h.handleWelcome)
	h.router.Get("/*", h.handleGetObject)
	h.router.Put("/*", h.handlePutObject)
}

func (h *Handler) handleWelcome(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("Welcome to the Object Storage API!"))
	if err != nil {
		return
	}
}

func (h *Handler) handleGetObject(w http.ResponseWriter, r *http.Request) {
	urlParams := chi.URLParam(r, "*")
	bucket := strings.Split(urlParams, "/")[0]
	key := strings.Join(strings.Split(urlParams, "/")[1:], "/")

	stream, manifest, err := h.svc.GetObject(bucket, key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", manifest.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(manifest.Size, 10))
	w.Header().Set("ETag", manifest.ETag)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Last-Modified", time.Unix(manifest.CreatedAt, 0).UTC().Format(time.RFC1123))
	w.WriteHeader(http.StatusOK)
	_, err = io.Copy(w, stream)

}

func (h *Handler) handlePutObject(w http.ResponseWriter, r *http.Request) {
	urlParams := chi.URLParam(r, "*")
	bucket := strings.Split(urlParams, "/")[0]
	key := strings.Join(strings.Split(urlParams, "/")[1:], "/")

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	manifest, err := h.svc.PutObject(bucket, key, contentType, r.Body)
	defer r.Body.Close()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("ETag", manifest.ETag)
	w.Header().Set("Content-Length", "0")

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) Start(address string) error {
	fmt.Printf("Starting API server on %s\n", address)
	h.setupRoutes()
	return http.ListenAndServe(address, h.router)
}
