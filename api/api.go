package api

import (
	"fmt"
	"fs/service"
	"fs/utils"
	"io"
	"net/http"
	"strconv"
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

	h.router.Get("/", h.handleGetBuckets)

	h.router.Get("/{bucket}/", h.handleGetBucket)
	h.router.Get("/{bucket}", h.handleGetBucket)
	h.router.Put("/{bucket}", h.handlePutBucket)
	h.router.Put("/{bucket}/", h.handlePutBucket)
	h.router.Delete("/{bucket}", h.handleDeleteBucket)
	h.router.Delete("/{bucket}/", h.handleDeleteBucket)
	h.router.Head("/{bucket}", h.handleHeadBucket)
	h.router.Head("/{bucket}/", h.handleHeadBucket)

	h.router.Get("/{bucket}/*", h.handleGetObject)
	h.router.Put("/{bucket}/*", h.handlePutObject)
	h.router.Head("/{bucket}/*", h.handleHeadObject)
}

func (h *Handler) handleWelcome(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("Welcome to the Object Storage API!"))
	if err != nil {
		return
	}
}

func (h *Handler) handleGetObject(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key := chi.URLParam(r, "*")
	if key == "" {
		http.Error(w, "object key is required", http.StatusBadRequest)
		return
	}

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
	bucket := chi.URLParam(r, "bucket")
	key := chi.URLParam(r, "*")
	if key == "" {
		http.Error(w, "object key is required", http.StatusBadRequest)
		return
	}

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

func (h *Handler) handleHeadObject(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key := chi.URLParam(r, "*")
	if key == "" {
		http.Error(w, "object key is required", http.StatusBadRequest)
		return
	}

	manifest, err := h.svc.HeadObject(bucket, key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("ETag", manifest.ETag)
	w.Header().Set("Content-Length", "0")
	w.Header().Set("Last-Modified", time.Unix(manifest.CreatedAt, 0).UTC().Format(time.RFC1123))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handlePutBucket(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	if h.svc.CreateBucket(bucket) != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (h *Handler) handleDeleteBucket(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	if h.svc.DeleteBucket(bucket) != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) handleHeadBucket(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	if h.svc.HeadBucket(bucket) != nil {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleGetBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := h.svc.ListBuckets()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-yaml")
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
	for _, bucket := range buckets {
		w.Write([]byte(bucket))
	}
}

func (h *Handler) handleGetBucket(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")

	if r.URL.Query().Get("list-type") == "2" {
		h.handleListObjectsV2(w, r, bucket)
		return
	}

	if r.URL.Query().Has("location") {
		return
	}

}

func (h *Handler) handleListObjectsV2(w http.ResponseWriter, r *http.Request, bucket string) {
	objects, err := h.svc.ListObjects(bucket, "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	xmlResponse, err := utils.ConstructXMLResponseForObjectList(bucket, objects)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(xmlResponse)))
	w.WriteHeader(http.StatusOK)
	_, err = w.Write([]byte(xmlResponse))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

func (h *Handler) Start(address string) error {
	fmt.Printf("Starting API server on %s\n", address)
	h.setupRoutes()
	return http.ListenAndServe(address, h.router)
}
