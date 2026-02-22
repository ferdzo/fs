package api

import (
	"encoding/xml"
	"errors"
	"fmt"
	"fs/metadata"
	"fs/models"
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
	h.router.Post("/{bucket}/*", h.handlePostObject)
	h.router.Head("/{bucket}/*", h.handleHeadObject)
	h.router.Delete("/{bucket}/*", h.handleDeleteObject)
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
		writeS3Error(w, r, s3ErrInvalidObjectKey, r.URL.Path)
		return
	}

	stream, manifest, err := h.svc.GetObject(bucket, key)
	if err != nil {
		writeMappedS3Error(w, r, err)
		return
	}

	w.Header().Set("Content-Type", manifest.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(manifest.Size, 10))
	w.Header().Set("ETag", `"`+manifest.ETag+`"`)
	w.Header().Set("Last-Modified", time.Unix(manifest.CreatedAt, 0).UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(http.StatusOK)
	_, err = io.Copy(w, stream)

}

func (h *Handler) handlePostObject(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key := chi.URLParam(r, "*")
	if key == "" {
		writeS3Error(w, r, s3ErrInvalidObjectKey, r.URL.Path)
		return
	}

	if _, ok := r.URL.Query()["uploads"]; ok {
		upload, err := h.svc.CreateMultipartUpload(bucket, key)
		if err != nil {
			writeMappedS3Error(w, r, err)
			return
		}
		response := models.InitiateMultipartUploadResult{
			Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
			Bucket:   upload.Bucket,
			Key:      upload.Key,
			UploadID: upload.UploadID,
		}
		payload, err := xml.MarshalIndent(response, "", "    ")
		if err != nil {
			writeMappedS3Error(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(xml.Header))
		_, _ = w.Write(payload)
		return
	}

	writeS3Error(w, r, s3ErrNotImplemented, r.URL.Path)
}

func (h *Handler) handlePutObject(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key := chi.URLParam(r, "*")
	if key == "" {
		writeS3Error(w, r, s3ErrInvalidObjectKey, r.URL.Path)
		return
	}
	if r.URL.Query().Get("uploads") != "" {
		if r.URL.Query().Get("partNumber") != "" {
		}
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	manifest, err := h.svc.PutObject(bucket, key, contentType, r.Body)
	defer r.Body.Close()

	if err != nil {
		writeMappedS3Error(w, r, err)
		return
	}

	w.Header().Set("ETag", `"`+manifest.ETag+`"`)
	w.Header().Set("Content-Length", "0")

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleHeadObject(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key := chi.URLParam(r, "*")
	if key == "" {
		writeS3Error(w, r, s3ErrInvalidObjectKey, r.URL.Path)
		return
	}

	manifest, err := h.svc.HeadObject(bucket, key)
	if err != nil {
		writeMappedS3Error(w, r, err)
		return
	}

	w.Header().Set("ETag", `"`+manifest.ETag+`"`)
	w.Header().Set("Content-Length", "0")
	w.Header().Set("Last-Modified", time.Unix(manifest.CreatedAt, 0).UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handlePutBucket(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	if err := h.svc.CreateBucket(bucket); err != nil {
		writeMappedS3Error(w, r, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (h *Handler) handleDeleteBucket(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	if err := h.svc.DeleteBucket(bucket); err != nil {
		writeMappedS3Error(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleDeleteObject(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key := chi.URLParam(r, "*")
	if key == "" {
		writeS3Error(w, r, s3ErrInvalidObjectKey, r.URL.Path)
		return
	}

	err := h.svc.DeleteObject(bucket, key)
	if err != nil {
		if errors.Is(err, metadata.ErrObjectNotFound) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeMappedS3Error(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleHeadBucket(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	if err := h.svc.HeadBucket(bucket); err != nil {
		writeMappedS3Error(w, r, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleGetBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := h.svc.ListBuckets()
	if err != nil {
		writeMappedS3Error(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	for _, bucket := range buckets {
		w.Write([]byte(bucket))
	}
}

func (h *Handler) handleGetBucket(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")

	if r.URL.Query().Get("list-type") == "2" {
		prefix := r.URL.Query().Get("prefix")
		if prefix == "" {
			prefix = ""
		}
		h.handleListObjectsV2(w, r, bucket, prefix)
		return
	}
	writeS3Error(w, r, s3ErrNotImplemented, r.URL.Path)

}

func (h *Handler) handleListObjectsV2(w http.ResponseWriter, r *http.Request, bucket, prefix string) {
	objects, err := h.svc.ListObjects(bucket, prefix)
	if err != nil {
		writeMappedS3Error(w, r, err)
		return
	}

	xmlResponse, err := utils.ConstructXMLResponseForObjectList(bucket, objects)
	if err != nil {
		writeMappedS3Error(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(xmlResponse)))
	w.WriteHeader(http.StatusOK)
	_, err = w.Write([]byte(xmlResponse))
	if err != nil {
		return
	}

}

func (h *Handler) Start(address string) error {
	fmt.Printf("Starting API server on %s\n", address)
	h.setupRoutes()
	return http.ListenAndServe(address, h.router)
}
