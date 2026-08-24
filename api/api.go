package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"fs/auth"
	"fs/logging"
	"fs/metadata"
	"fs/metrics"
	"fs/models"
	"fs/service"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type Handler struct {
	router    *chi.Mux
	svc       *service.ObjectService
	logger    *slog.Logger
	logConfig logging.Config
	authSvc   *auth.Service
	adminAPI  bool

	bodyReadTimeout time.Duration
}

const (
	maxXMLBodyBytes         int64 = 1 << 20
	maxDeleteObjects              = 1000
	maxObjectKeyBytes             = 1024
	maxAWSChunkedLineBytes        = 8 << 10
	serverReadHeaderTimeout       = 5 * time.Second
	// serverReadTimeout and serverWriteTimeout are disabled on purpose: both
	// cover the full request/response lifetime including streaming bodies, so a
	// finite value truncates large uploads and downloads mid-flight. Read and
	// idle liveness are still enforced by serverReadHeaderTimeout (header
	// phase) and serverIdleTimeout (between keepalive requests), and concurrent
	// load is bounded by serverMaxConnections.
	serverReadTimeout    = 0
	serverWriteTimeout   = 0
	serverIdleTimeout    = 120 * time.Second
	serverMaxHeaderBytes = 1 << 20
	serverMaxConnections = 1024
)

func NewHandler(svc *service.ObjectService, logger *slog.Logger, logConfig logging.Config, authSvc *auth.Service, adminAPI bool, bodyReadTimeout ...time.Duration) *Handler {
	timeout := time.Minute
	if len(bodyReadTimeout) > 0 && bodyReadTimeout[0] > 0 {
		timeout = bodyReadTimeout[0]
	}
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(s3Recoverer)
	if logger == nil {
		logger = slog.Default()
	}

	h := &Handler{
		router:    r,
		svc:       svc,
		logger:    logger,
		logConfig: logConfig,
		authSvc:   authSvc,
		adminAPI:  adminAPI,

		bodyReadTimeout: timeout,
	}
	return h
}

// s3Recoverer converts handler panics into S3-style XML InternalError
// responses; plain-text 500s break AWS SDK error parsing.
func s3Recoverer(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil && rec != http.ErrAbortHandler {
				slog.Error("panic_recovered",
					"panic", fmt.Sprintf("%v", rec),
					"method", r.Method,
					"path", r.URL.Path,
					"request_id", middleware.GetReqID(r.Context()),
				)
				writeS3Error(w, r, s3ErrInternal, r.URL.Path)
			}
		}()
		next.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func (h *Handler) setupRoutes() {
	h.router.Use(bodyReadTimeoutMiddleware(h.bodyReadTimeout))
	h.router.Use(logging.HTTPMiddleware(h.logger, h.logConfig))
	h.router.Use(auth.Middleware(h.authSvc, h.logger, h.logConfig.Audit, writeMappedS3Error))

	h.router.Get("/healthz", h.handleHealth)
	h.router.Head("/healthz", h.handleHealth)
	h.router.Get("/metrics", h.handleMetrics)
	h.router.Head("/metrics", h.handleMetrics)
	h.router.Get("/", h.handleGetBuckets)
	if h.adminAPI {
		h.registerAdminRoutes()
	}

	h.router.Get("/{bucket}/", h.handleGetBucket)
	h.router.Get("/{bucket}", h.handleGetBucket)
	h.router.Put("/{bucket}", h.handlePutBucket)
	h.router.Put("/{bucket}/", h.handlePutBucket)
	h.router.Post("/{bucket}", h.handlePostBucket)
	h.router.Post("/{bucket}/", h.handlePostBucket)
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

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if _, err := h.svc.ListBuckets(); err != nil {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		if r.Method != http.MethodHead {
			_, _ = w.Write([]byte("unhealthy"))
		}
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write([]byte("ok"))
	}
}

func (h *Handler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	payload := metrics.Default.RenderPrometheus()
	w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(payload))
}

func validateObjectKey(key string) *s3APIError {
	if key == "" {
		err := s3ErrInvalidObjectKey
		return &err
	}
	if len(key) > maxObjectKeyBytes {
		err := s3ErrKeyTooLong
		return &err
	}
	return nil
}

func objectKeyFromRequest(r *http.Request) (string, *s3APIError) {
	rawKey := rawObjectKeyFromRequest(r)
	key, err := normalizeObjectKey(rawKey)
	if err != nil {
		apiErr := s3ErrInvalidArgument
		return "", &apiErr
	}
	if apiErr := validateObjectKey(key); apiErr != nil {
		return "", apiErr
	}
	return key, nil
}

func rawObjectKeyFromRequest(r *http.Request) string {
	if r == nil || r.URL == nil {
		return ""
	}
	bucket := chi.URLParam(r, "bucket")
	if bucket == "" {
		return chi.URLParam(r, "*")
	}
	escapedPath := r.URL.EscapedPath()
	prefix := "/" + bucket + "/"
	if strings.HasPrefix(escapedPath, prefix) {
		return strings.TrimPrefix(escapedPath, prefix)
	}
	return chi.URLParam(r, "*")
}

func normalizeObjectKey(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	return url.PathUnescape(raw)
}

func parseCopySource(raw string) (string, string, error) {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "/")
	if idx := strings.IndexByte(raw, '?'); idx >= 0 {
		raw = raw[:idx]
	}
	bucket, rawKey, found := strings.Cut(raw, "/")
	if !found || strings.TrimSpace(bucket) == "" || rawKey == "" {
		return "", "", errors.New("invalid copy source")
	}
	key, err := normalizeObjectKey(rawKey)
	if err != nil {
		return "", "", err
	}
	if apiErr := validateObjectKey(key); apiErr != nil {
		return "", "", errors.New(apiErr.Code)
	}
	return bucket, key, nil
}

func (h *Handler) authorizeCopySource(r *http.Request, bucket, key string) error {
	return h.authorizeObjectAction(r, auth.ActionGetObject, bucket, key)
}

func (h *Handler) authorizeObjectAction(r *http.Request, action auth.Action, bucket, key string) error {
	if h.authSvc == nil || !h.authSvc.Config().Enabled {
		return nil
	}

	authCtx, ok := auth.GetRequestContext(r.Context())
	if !ok || !authCtx.Authenticated {
		return auth.ErrAccessDenied
	}

	return h.authSvc.Authorize(authCtx.AccessKeyID, auth.RequestTarget{
		Action: action,
		Bucket: bucket,
		Key:    key,
	})
}

func (h *Handler) handleGetObject(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key, apiErr := objectKeyFromRequest(r)
	if apiErr != nil {
		writeS3Error(w, r, *apiErr, r.URL.Path)
		return
	}

	if uploadID := r.URL.Query().Get("uploadId"); uploadID != "" {
		h.handleListMultipartParts(w, r, bucket, key, uploadID)
		return
	}

	stream, manifest, err := h.svc.GetObject(bucket, key)
	if err != nil {
		writeMappedS3Error(w, r, err)
		return
	}
	defer stream.Close()

	rangeHeader := strings.TrimSpace(r.Header.Get("Range"))
	if rangeHeader != "" {
		start, end, err := parseSingleByteRange(rangeHeader, manifest.Size)
		if err != nil {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", manifest.Size))
			writeS3Error(w, r, s3ErrInvalidRange, r.URL.Path)
			return
		}

		if start > 0 {
			if _, err := io.CopyN(io.Discard, stream, start); err != nil {
				writeMappedS3Error(w, r, err)
				return
			}
		}

		length := end - start + 1
		w.Header().Set("Content-Type", manifest.ContentType)
		w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, manifest.Size))
		w.Header().Set("ETag", `"`+manifest.ETag+`"`)
		w.Header().Set("Last-Modified", time.Unix(manifest.CreatedAt, 0).UTC().Format(http.TimeFormat))
		w.Header().Set("Accept-Ranges", "bytes")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.CopyN(w, stream, length)
		return
	}

	w.Header().Set("Content-Type", manifest.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(manifest.Size, 10))
	w.Header().Set("ETag", `"`+manifest.ETag+`"`)
	w.Header().Set("Last-Modified", time.Unix(manifest.CreatedAt, 0).UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(http.StatusOK)
	if _, err = io.Copy(w, stream); err != nil && !errors.Is(err, context.Canceled) {
		h.logger.Warn("get_object_stream_failed",
			"bucket", bucket,
			"key", key,
			"etag", manifest.ETag,
			"error", err,
		)
	}
}

func (h *Handler) handlePostObject(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key, apiErr := objectKeyFromRequest(r)
	if apiErr != nil {
		writeS3Error(w, r, *apiErr, r.URL.Path)
		return
	}
	defer r.Body.Close()

	if _, ok := r.URL.Query()["uploads"]; ok {
		if err := h.authorizeObjectAction(r, auth.ActionCreateMultipartUpload, bucket, key); err != nil {
			writeMappedS3Error(w, r, err)
			return
		}
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
		w.Header().Set("Content-Length", strconv.Itoa(len(xml.Header)+len(payload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(xml.Header))
		_, _ = w.Write(payload)
		return
	}

	if uploadID := r.URL.Query().Get("uploadId"); uploadID != "" {
		if err := h.authorizeObjectAction(r, auth.ActionCompleteMultipart, bucket, key); err != nil {
			writeMappedS3Error(w, r, err)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxXMLBodyBytes)
		var req models.CompleteMultipartUploadRequest
		if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				writeS3Error(w, r, s3ErrEntityTooLarge, r.URL.Path)
				return
			}
			writeS3Error(w, r, s3ErrMalformedXML, r.URL.Path)
			return
		}
		metrics.Default.ObserveBatchSize(len(req.Parts))

		manifest, err := h.svc.CompleteMultipartUpload(bucket, key, uploadID, req.Parts)
		if err != nil {
			writeMappedS3Error(w, r, err)
			return
		}

		response := models.CompleteMultipartUploadResult{
			Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
			Bucket:   bucket,
			Key:      key,
			ETag:     `"` + manifest.ETag + `"`,
			Location: r.URL.Path,
		}
		payload, err := xml.MarshalIndent(response, "", "    ")
		if err != nil {
			writeMappedS3Error(w, r, err)
			return
		}

		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.Header().Set("Content-Length", strconv.Itoa(len(xml.Header)+len(payload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(xml.Header))
		_, _ = w.Write(payload)
		return
	}

	writeS3Error(w, r, s3ErrNotImplemented, r.URL.Path)
}

// hasSignedStreamingPayload reports whether the request declares a
// chunk-signature streaming mode that fs cannot verify yet.
func hasSignedStreamingPayload(r *http.Request) bool {
	return strings.HasPrefix(
		strings.ToUpper(strings.TrimSpace(r.Header.Get("x-amz-content-sha256"))),
		"STREAMING-AWS4-HMAC-SHA256",
	)
}

func (h *Handler) handlePutObject(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key, apiErr := objectKeyFromRequest(r)
	if apiErr != nil {
		writeS3Error(w, r, *apiErr, r.URL.Path)
		return
	}
	defer r.Body.Close()

	uploadID := r.URL.Query().Get("uploadId")
	partNumberRaw := r.URL.Query().Get("partNumber")
	if uploadID != "" || partNumberRaw != "" {
		if uploadID == "" || partNumberRaw == "" {
			writeS3Error(w, r, s3ErrInvalidPart, r.URL.Path)
			return
		}
		if strings.TrimSpace(r.Header.Get("x-amz-copy-source")) != "" {
			writeS3Error(w, r, s3ErrNotImplemented, r.URL.Path)
			return
		}

		partNumber, err := strconv.Atoi(partNumberRaw)
		if err != nil {
			writeS3Error(w, r, s3ErrInvalidPart, r.URL.Path)
			return
		}
		if partNumber < 1 || partNumber > 10000 {
			writeS3Error(w, r, s3ErrInvalidPart, r.URL.Path)
			return
		}

		var decodeStream io.ReadCloser
		bodyReader := selectBodyReader(r, &decodeStream)
		if bodyReader == nil {
			writeS3Error(w, r, s3ErrInvalidArgument, r.URL.Path)
			return
		}
		if decodeStream != nil {
			defer decodeStream.Close()
		}

		etag, err := h.svc.UploadPart(bucket, key, uploadID, partNumber, bodyReader)
		if err != nil {
			writeMappedS3Error(w, r, err)
			return
		}
		w.Header().Set("ETag", `"`+etag+`"`)
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
		return
	}
	metrics.Default.ObserveBatchSize(1)

	if ifNoneMatch := strings.TrimSpace(r.Header.Get("If-None-Match")); ifNoneMatch != "" {
		manifest, err := h.svc.HeadObject(bucket, key)
		if err != nil {
			if !errors.Is(err, metadata.ErrObjectNotFound) {
				writeMappedS3Error(w, r, err)
				return
			}
		} else if ifNoneMatchPreconditionFailed(ifNoneMatch, manifest.ETag) {
			writeS3Error(w, r, s3ErrPreconditionFailed, r.URL.Path)
			return
		}
	}

	if copySourceRaw := strings.TrimSpace(r.Header.Get("x-amz-copy-source")); copySourceRaw != "" {
		srcBucket, srcKey, err := parseCopySource(copySourceRaw)
		if err != nil {
			writeS3Error(w, r, s3ErrInvalidArgument, r.URL.Path)
			return
		}
		if err := h.authorizeCopySource(r, srcBucket, srcKey); err != nil {
			writeMappedS3Error(w, r, err)
			return
		}

		manifest, err := h.svc.CopyObject(srcBucket, srcKey, bucket, key)
		if err != nil {
			writeMappedS3Error(w, r, err)
			return
		}

		response := models.CopyObjectResult{
			Xmlns:        "http://s3.amazonaws.com/doc/2006-03-01/",
			LastModified: time.Unix(manifest.CreatedAt, 0).UTC().Format("2006-01-02T15:04:05.000Z"),
			ETag:         `"` + manifest.ETag + `"`,
		}
		payload, err := xml.MarshalIndent(response, "", "    ")
		if err != nil {
			writeMappedS3Error(w, r, err)
			return
		}

		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.Header().Set("ETag", `"`+manifest.ETag+`"`)
		w.Header().Set("Content-Length", strconv.Itoa(len(xml.Header)+len(payload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(xml.Header))
		_, _ = w.Write(payload)
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	var decodeStream io.ReadCloser
	bodyReader := selectBodyReader(r, &decodeStream)
	if bodyReader == nil {
		writeS3Error(w, r, s3ErrInvalidArgument, r.URL.Path)
		return
	}
	if decodeStream != nil {
		defer decodeStream.Close()
	}

	manifest, err := h.svc.PutObject(bucket, key, contentType, bodyReader)

	if err != nil {
		writeMappedS3Error(w, r, err)
		return
	}

	w.Header().Set("ETag", `"`+manifest.ETag+`"`)
	w.Header().Set("Content-Length", "0")

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleListMultipartParts(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string) {
	parts, err := h.svc.ListMultipartParts(bucket, key, uploadID)
	if err != nil {
		writeMappedS3Error(w, r, err)
		return
	}

	response := models.ListPartsResult{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
		Parts:    make([]models.PartItem, 0, len(parts)),
	}
	for _, part := range parts {
		response.Parts = append(response.Parts, models.PartItem{
			PartNumber:   part.PartNumber,
			LastModified: time.Unix(part.CreatedAt, 0).UTC().Format("2006-01-02T15:04:05.000Z"),
			ETag:         `"` + part.ETag + `"`,
			Size:         part.Size,
		})
	}

	payload, err := xml.MarshalIndent(response, "", "    ")
	if err != nil {
		writeMappedS3Error(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(xml.Header)+len(payload)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(payload)
}

// selectBodyReader picks the decoded stream for a PUT: aws-chunked
// unsigned decoder or the raw body. Signed-stream bodies were already
// wrapped by the auth middleware.
func selectBodyReader(r *http.Request, decodeOut *io.ReadCloser) io.Reader {
	if hasUnsupportedAWSChunkedPayload(r) {
		return nil
	}
	if shouldDecodeAWSChunkedPayload(r) {
		ds := newAWSChunkedDecodingReader(r.Body)
		*decodeOut = ds
		return ds
	}
	return r.Body
}

func shouldDecodeAWSChunkedPayload(r *http.Request) bool {
	signingMode := strings.ToLower(r.Header.Get("x-amz-content-sha256"))
	return strings.HasPrefix(signingMode, "streaming-unsigned-payload")
}

func hasUnsupportedAWSChunkedPayload(r *http.Request) bool {
	contentEncoding := strings.ToLower(r.Header.Get("Content-Encoding"))
	if !strings.Contains(contentEncoding, "aws-chunked") {
		return false
	}
	return !shouldDecodeAWSChunkedPayload(r) && !hasSignedStreamingPayload(r)
}

func newAWSChunkedDecodingReader(src io.Reader) io.ReadCloser {
	probedReader, isAWSChunked := probeAWSChunkedPayload(src)
	if !isAWSChunked {
		return io.NopCloser(probedReader)
	}

	pr, pw := io.Pipe()
	go func() {
		if err := decodeAWSChunkedPayload(probedReader, pw); err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		_ = pw.Close()
	}()
	return pr
}

func probeAWSChunkedPayload(src io.Reader) (io.Reader, bool) {
	reader := bufio.NewReaderSize(src, maxAWSChunkedLineBytes)
	headerLine, err := reader.ReadSlice('\n')
	replay := io.MultiReader(bytes.NewReader(headerLine), reader)
	if errors.Is(err, bufio.ErrBufferFull) {
		return replay, true
	}
	if err != nil {
		return replay, false
	}

	line := strings.TrimRight(string(headerLine), "\r\n")
	chunkSizeToken := line
	if idx := strings.IndexByte(chunkSizeToken, ';'); idx >= 0 {
		chunkSizeToken = chunkSizeToken[:idx]
	}
	chunkSizeToken = strings.TrimSpace(chunkSizeToken)
	if chunkSizeToken == "" {
		return replay, false
	}
	size, parseErr := strconv.ParseInt(chunkSizeToken, 16, 64)
	if parseErr != nil || size < 0 {
		return replay, false
	}
	return replay, true
}

func decodeAWSChunkedPayload(src io.Reader, dst io.Writer) error {
	reader := bufio.NewReaderSize(src, maxAWSChunkedLineBytes)
	for {
		headerLine, err := readAWSChunkedLine(reader)
		if err != nil {
			return err
		}
		headerLine = strings.TrimRight(headerLine, "\r\n")
		chunkSizeToken := headerLine
		if idx := strings.IndexByte(chunkSizeToken, ';'); idx >= 0 {
			chunkSizeToken = chunkSizeToken[:idx]
		}
		chunkSizeToken = strings.TrimSpace(chunkSizeToken)
		chunkSize, err := strconv.ParseInt(chunkSizeToken, 16, 64)
		if err != nil {
			return fmt.Errorf("invalid aws-chunked header %q: %w", headerLine, err)
		}
		if chunkSize < 0 {
			return fmt.Errorf("invalid aws-chunked size: %d", chunkSize)
		}
		if chunkSize == 0 {
			for {
				line, err := readAWSChunkedLine(reader)
				if err != nil {
					return err
				}
				if line == "\r\n" || line == "\n" {
					return nil
				}
			}
		}
		if chunkSize > 0 {
			if _, err := io.CopyN(dst, reader, chunkSize); err != nil {
				return err
			}
		}

		crlf := make([]byte, 2)
		if _, err := io.ReadFull(reader, crlf); err != nil {
			return err
		}
		if crlf[0] != '\r' || crlf[1] != '\n' {
			return errors.New("invalid aws-chunked payload terminator")
		}
	}
}

func readAWSChunkedLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		return "", service.ErrEntityTooLarge
	}
	if len(line) > maxAWSChunkedLineBytes {
		return "", service.ErrEntityTooLarge
	}
	return string(line), err
}

func ifNoneMatchPreconditionFailed(headerValue, etag string) bool {
	for _, rawToken := range strings.Split(headerValue, ",") {
		token := strings.TrimSpace(rawToken)
		if token == "" {
			continue
		}
		if token == "*" {
			return true
		}

		token = strings.TrimPrefix(token, "W/")
		token = strings.Trim(token, `"`)
		if strings.EqualFold(token, etag) {
			return true
		}
	}
	return false
}

func (h *Handler) handlePutBucket(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	if err := h.svc.CreateBucket(bucket); err != nil {
		writeMappedS3Error(w, r, err)
		return
	}
	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleDeleteBucket(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	if err := h.svc.DeleteBucket(bucket); err != nil {
		writeMappedS3Error(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handlePostBucket(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	if _, ok := r.URL.Query()["delete"]; !ok {
		writeS3Error(w, r, s3ErrNotImplemented, r.URL.Path)
		return
	}
	defer r.Body.Close()
	r.Body = http.MaxBytesReader(w, r.Body, maxXMLBodyBytes)

	bodyReader := io.Reader(r.Body)
	var decodeStream io.ReadCloser
	if shouldDecodeAWSChunkedPayload(r) {
		decodeStream = newAWSChunkedDecodingReader(r.Body)
		defer decodeStream.Close()
		bodyReader = decodeStream
	}

	var req models.DeleteObjectsRequest
	if err := xml.NewDecoder(bodyReader).Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeS3Error(w, r, s3ErrEntityTooLarge, r.URL.Path)
			return
		}
		writeS3Error(w, r, s3ErrMalformedXML, r.URL.Path)
		return
	}
	if len(req.Objects) > maxDeleteObjects {
		writeS3Error(w, r, s3ErrTooManyDeleteObjects, r.URL.Path)
		return
	}
	metrics.Default.ObserveBatchSize(len(req.Objects))

	keys := make([]string, 0, len(req.Objects))
	response := models.DeleteObjectsResult{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
	}
	for _, obj := range req.Objects {
		if obj.Key == "" {
			response.Errors = append(response.Errors, models.DeleteError{
				Key:     obj.Key,
				Code:    s3ErrInvalidObjectKey.Code,
				Message: s3ErrInvalidObjectKey.Message,
			})
			continue
		}
		if len(obj.Key) > maxObjectKeyBytes {
			response.Errors = append(response.Errors, models.DeleteError{
				Key:     obj.Key,
				Code:    s3ErrKeyTooLong.Code,
				Message: s3ErrKeyTooLong.Message,
			})
			continue
		}
		if err := h.authorizeObjectAction(r, auth.ActionDeleteObject, bucket, obj.Key); err != nil {
			apiErr := mapToS3Error(err)
			response.Errors = append(response.Errors, models.DeleteError{
				Key:     obj.Key,
				Code:    apiErr.Code,
				Message: apiErr.Message,
			})
			continue
		}
		keys = append(keys, obj.Key)
	}

	deleted, err := h.svc.DeleteObjects(bucket, keys)
	if err != nil {
		writeMappedS3Error(w, r, err)
		return
	}

	if !req.Quiet {
		response.Deleted = make([]models.DeletedEntry, 0, len(deleted))
		for _, key := range deleted {
			response.Deleted = append(response.Deleted, models.DeletedEntry{Key: key})
		}
	}

	payload, err := xml.MarshalIndent(response, "", "    ")
	if err != nil {
		writeMappedS3Error(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(xml.Header)+len(payload)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(payload)
}

func (h *Handler) handleDeleteObject(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key, apiErr := objectKeyFromRequest(r)
	if apiErr != nil {
		writeS3Error(w, r, *apiErr, r.URL.Path)
		return
	}
	if uploadId := r.URL.Query().Get("uploadId"); uploadId != "" {
		err := h.svc.AbortMultipartUpload(bucket, key, uploadId)
		if err != nil {
			writeMappedS3Error(w, r, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
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
	if h.authSvc != nil {
		if region := strings.TrimSpace(h.authSvc.Config().Region); region != "" {
			w.Header().Set("x-amz-bucket-region", region)
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleHeadObject(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key, apiErr := objectKeyFromRequest(r)
	if apiErr != nil {
		writeS3Error(w, r, *apiErr, r.URL.Path)
		return
	}

	manifest, err := h.svc.HeadObject(bucket, key)
	if err != nil {
		writeMappedS3Error(w, r, err)
		return
	}
	etag := manifest.ETag
	size := strconv.FormatInt(manifest.Size, 10)

	contentType := manifest.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("ETag", `"`+etag+`"`)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", size)
	w.Header().Set("Last-Modified", time.Unix(manifest.CreatedAt, 0).UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

type limitedListener struct {
	net.Listener
	slots chan struct{}
}

func newLimitedListener(inner net.Listener, maxConns int) net.Listener {
	if maxConns <= 0 {
		return inner
	}
	metrics.Default.SetConnectionPoolMax(maxConns)
	return &limitedListener{
		Listener: inner,
		slots:    make(chan struct{}, maxConns),
	}
}

func (l *limitedListener) Accept() (net.Conn, error) {
	select {
	case l.slots <- struct{}{}:
	default:
		metrics.Default.IncConnectionPoolWait()
		metrics.Default.IncRequestQueueLength()
		l.slots <- struct{}{}
		metrics.Default.DecRequestQueueLength()
	}
	conn, err := l.Listener.Accept()
	if err != nil {
		<-l.slots
		return nil, err
	}
	metrics.Default.IncConnectionPoolActive()
	return &limitedConn{
		Conn: conn,
		done: func() {
			<-l.slots
			metrics.Default.DecConnectionPoolActive()
		},
	}, nil
}

type limitedConn struct {
	net.Conn
	once sync.Once
	done func()
}

func (c *limitedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.done)
	return err
}

func (h *Handler) handleGetBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := h.svc.ListBuckets()
	if err != nil {
		writeMappedS3Error(w, r, err)
		return
	}

	response := models.ListAllMyBucketsResult{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner: models.BucketsOwner{
			ID:          "local",
			DisplayName: "local",
		},
		Buckets: models.BucketsElement{
			Items: make([]models.BucketItem, 0, len(buckets)),
		},
	}

	for _, bucket := range buckets {
		manifest, err := h.svc.GetBucketManifest(bucket)
		if err != nil {
			h.logger.Warn("bucket_manifest_read_failed", "bucket", bucket, "error", err)
			continue
		}
		response.Buckets.Items = append(response.Buckets.Items, models.BucketItem{
			Name:         bucket,
			CreationDate: manifest.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		})
	}

	payload, err := xml.MarshalIndent(response, "", "    ")
	if err != nil {
		writeMappedS3Error(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(xml.Header)+len(payload)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(payload)
}

func (h *Handler) handleGetBucket(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	query := r.URL.Query()

	if query.Has("location") {
		region := "us-east-1"
		if h.authSvc != nil {
			candidate := strings.TrimSpace(h.authSvc.Config().Region)
			if candidate != "" {
				region = candidate
			}
		}
		xmlResponse := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">%s</LocationConstraint>`, region)

		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.Header().Set("Content-Length", strconv.Itoa(len(xmlResponse)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(xmlResponse))
		return
	}

	listType := strings.TrimSpace(query.Get("list-type"))
	if listType == "2" {
		h.handleListObjectsV2(w, r, bucket)
		return
	}
	if listType != "" {
		writeS3Error(w, r, s3ErrInvalidArgument, r.URL.Path)
		return
	}

	if shouldUseListObjectsV1(query) {
		h.handleListObjectsV1(w, r, bucket)
		return
	}

	writeS3Error(w, r, s3ErrNotImplemented, r.URL.Path)
}

func shouldUseListObjectsV1(query url.Values) bool {
	if len(query) == 0 {
		return true
	}

	listingParams := map[string]struct{}{
		"delimiter":     {},
		"encoding-type": {},
		"marker":        {},
		"max-keys":      {},
		"prefix":        {},
	}
	for key := range query {
		if _, ok := listingParams[key]; !ok {
			return false
		}
	}
	return true
}

func (h *Handler) handleListObjectsV1(w http.ResponseWriter, r *http.Request, bucket string) {
	prefix := r.URL.Query().Get("prefix")
	delimiter := r.URL.Query().Get("delimiter")
	marker := r.URL.Query().Get("marker")
	encodingType := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("encoding-type")))
	if encodingType != "" && encodingType != "url" {
		writeS3Error(w, r, s3ErrInvalidArgument, r.URL.Path)
		return
	}

	maxKeys := 1000
	if rawMaxKeys := strings.TrimSpace(r.URL.Query().Get("max-keys")); rawMaxKeys != "" {
		parsed, err := strconv.Atoi(rawMaxKeys)
		if err != nil || parsed < 0 {
			writeS3Error(w, r, s3ErrInvalidArgument, r.URL.Path)
			return
		}
		if parsed > 1000 {
			parsed = 1000
		}
		maxKeys = parsed
	}

	result := models.ListBucketResultV1{
		Xmlns:        "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:         bucket,
		Prefix:       s3EncodeIfNeeded(prefix, encodingType),
		Marker:       s3EncodeIfNeeded(marker, encodingType),
		Delimiter:    s3EncodeIfNeeded(delimiter, encodingType),
		MaxKeys:      maxKeys,
		EncodingType: encodingType,
	}

	type pageEntry struct {
		Object       *models.ObjectManifest
		CommonPrefix string
	}

	entries := make([]pageEntry, 0, maxKeys)
	seenCommonPrefixes := make(map[string]struct{})
	truncated := false
	stopErr := errors.New("list_v1_page_complete")

	startKey := prefix
	if marker != "" && marker > startKey {
		startKey = marker
	}

	if maxKeys > 0 {
		err := h.svc.ForEachObjectFrom(bucket, startKey, func(object *models.ObjectManifest) error {
			if object == nil {
				return nil
			}
			key := object.Key

			if prefix != "" {
				if key < prefix {
					return nil
				}
				if !strings.HasPrefix(key, prefix) {
					return stopErr
				}
			}
			if marker != "" && key <= marker {
				return nil
			}

			if delimiter != "" {
				relative := strings.TrimPrefix(key, prefix)
				if idx := strings.Index(relative, delimiter); idx >= 0 {
					commonPrefix := prefix + relative[:idx+len(delimiter)]
					if marker != "" && commonPrefix <= marker {
						return nil
					}
					if _, exists := seenCommonPrefixes[commonPrefix]; exists {
						return nil
					}
					seenCommonPrefixes[commonPrefix] = struct{}{}
					if len(entries) >= maxKeys {
						truncated = true
						return stopErr
					}
					entries = append(entries, pageEntry{
						CommonPrefix: commonPrefix,
					})
					return nil
				}
			}

			if len(entries) >= maxKeys {
				truncated = true
				return stopErr
			}
			entries = append(entries, pageEntry{Object: object})
			return nil
		})
		if err != nil && !errors.Is(err, stopErr) {
			writeMappedS3Error(w, r, err)
			return
		}
	}

	for _, entry := range entries {
		if entry.Object != nil {
			result.Contents = append(result.Contents, models.Contents{
				Key:          s3EncodeIfNeeded(entry.Object.Key, encodingType),
				LastModified: time.Unix(entry.Object.CreatedAt, 0).UTC().Format("2006-01-02T15:04:05.000Z"),
				ETag:         `"` + entry.Object.ETag + `"`,
				Size:         entry.Object.Size,
				StorageClass: "STANDARD",
			})
		} else {
			result.CommonPrefixes = append(result.CommonPrefixes, models.CommonPrefixes{
				Prefix: s3EncodeIfNeeded(entry.CommonPrefix, encodingType),
			})
		}
	}

	result.IsTruncated = truncated
	if result.IsTruncated && result.NextMarker == "" && len(entries) > 0 {
		last := entries[len(entries)-1]
		if last.Object != nil {
			result.NextMarker = s3EncodeIfNeeded(last.Object.Key, encodingType)
		} else {
			result.NextMarker = s3EncodeIfNeeded(last.CommonPrefix, encodingType)
		}
	}

	xmlResponse, err := xml.MarshalIndent(result, "", "    ")
	if err != nil {
		writeMappedS3Error(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(xml.Header)+len(xmlResponse)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(xmlResponse)
}

func (h *Handler) handleListObjectsV2(w http.ResponseWriter, r *http.Request, bucket string) {
	prefix := r.URL.Query().Get("prefix")
	delimiter := r.URL.Query().Get("delimiter")
	startAfter := r.URL.Query().Get("start-after")
	encodingType := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("encoding-type")))
	if encodingType != "" && encodingType != "url" {
		writeS3Error(w, r, s3ErrInvalidArgument, r.URL.Path)
		return
	}

	maxKeys := 1000
	if rawMaxKeys := strings.TrimSpace(r.URL.Query().Get("max-keys")); rawMaxKeys != "" {
		parsed, err := strconv.Atoi(rawMaxKeys)
		if err != nil || parsed < 0 {
			writeS3Error(w, r, s3ErrInvalidArgument, r.URL.Path)
			return
		}
		if parsed > 1000 {
			parsed = 1000
		}
		maxKeys = parsed
	}

	continuationToken := strings.TrimSpace(r.URL.Query().Get("continuation-token"))
	continuationMarker := ""
	continuationType := ""
	continuationValue := ""
	if continuationToken != "" {
		decoded, err := base64.StdEncoding.DecodeString(continuationToken)
		if err != nil || len(decoded) == 0 {
			writeS3Error(w, r, s3ErrInvalidArgument, r.URL.Path)
			return
		}
		continuationMarker = string(decoded)
		continuationType, continuationValue, _ = strings.Cut(continuationMarker, ":")
		if (continuationType != "K" && continuationType != "C") || continuationValue == "" {
			writeS3Error(w, r, s3ErrInvalidArgument, r.URL.Path)
			return
		}
	}

	result := models.ListBucketResultV2{
		Xmlns:             "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:              bucket,
		Prefix:            s3EncodeIfNeeded(prefix, encodingType),
		Delimiter:         s3EncodeIfNeeded(delimiter, encodingType),
		MaxKeys:           maxKeys,
		ContinuationToken: continuationToken,
		StartAfter:        s3EncodeIfNeeded(startAfter, encodingType),
		EncodingType:      encodingType,
	}

	type pageEntry struct {
		Marker       string
		Object       *models.ObjectManifest
		CommonPrefix string
	}

	entries := make([]pageEntry, 0, maxKeys)
	seenCommonPrefixes := make(map[string]struct{})
	truncated := false
	stopErr := errors.New("list_v2_page_complete")

	startKey := prefix
	if continuationToken != "" {
		startKey = continuationValue
	} else if startAfter != "" && startAfter > startKey {
		startKey = startAfter
	}

	if maxKeys > 0 {
		err := h.svc.ForEachObjectFrom(bucket, startKey, func(object *models.ObjectManifest) error {
			if object == nil {
				return nil
			}
			key := object.Key

			if prefix != "" {
				if key < prefix {
					return nil
				}
				if !strings.HasPrefix(key, prefix) {
					return stopErr
				}
			}

			if continuationToken != "" {
				if continuationType == "K" && key <= continuationValue {
					return nil
				}
				if continuationType == "C" && strings.HasPrefix(key, continuationValue) {
					return nil
				}
			} else if startAfter != "" && key <= startAfter {
				return nil
			}

			if delimiter != "" {
				relative := strings.TrimPrefix(key, prefix)
				if idx := strings.Index(relative, delimiter); idx >= 0 {
					commonPrefix := prefix + relative[:idx+len(delimiter)]
					if continuationToken == "" && startAfter != "" && commonPrefix <= startAfter {
						return nil
					}
					if _, exists := seenCommonPrefixes[commonPrefix]; exists {
						return nil
					}
					seenCommonPrefixes[commonPrefix] = struct{}{}
					if len(entries) >= maxKeys {
						truncated = true
						return stopErr
					}
					entries = append(entries, pageEntry{
						Marker:       "C:" + commonPrefix,
						CommonPrefix: commonPrefix,
					})
					return nil
				}
			}

			if len(entries) >= maxKeys {
				truncated = true
				return stopErr
			}
			entries = append(entries, pageEntry{
				Marker: "K:" + key,
				Object: object,
			})
			return nil
		})
		if err != nil && !errors.Is(err, stopErr) {
			writeMappedS3Error(w, r, err)
			return
		}
	}

	for _, entry := range entries {
		if entry.Object != nil {
			result.Contents = append(result.Contents, models.Contents{
				Key:          s3EncodeIfNeeded(entry.Object.Key, encodingType),
				LastModified: time.Unix(entry.Object.CreatedAt, 0).UTC().Format("2006-01-02T15:04:05.000Z"),
				ETag:         `"` + entry.Object.ETag + `"`,
				Size:         entry.Object.Size,
				StorageClass: "STANDARD",
			})
		} else {
			result.CommonPrefixes = append(result.CommonPrefixes, models.CommonPrefixes{
				Prefix: s3EncodeIfNeeded(entry.CommonPrefix, encodingType),
			})
		}
		result.KeyCount++
	}

	result.IsTruncated = truncated
	if result.IsTruncated && result.KeyCount > 0 {
		result.NextContinuationToken = base64.StdEncoding.EncodeToString([]byte(entries[result.KeyCount-1].Marker))
	}

	xmlResponse, err := xml.MarshalIndent(result, "", "    ")
	if err != nil {
		writeMappedS3Error(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(xml.Header)+len(xmlResponse)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(xmlResponse)

}

func s3EncodeIfNeeded(value, encodingType string) string {
	if encodingType != "url" || value == "" {
		return value
	}
	encoded := url.QueryEscape(value)
	return strings.ReplaceAll(encoded, "+", "%20")
}

func parseSingleByteRange(rangeHeader string, size int64) (int64, int64, error) {
	if size <= 0 || len(rangeHeader) < len("bytes=") || !strings.EqualFold(rangeHeader[:len("bytes=")], "bytes=") {
		return 0, 0, errors.New("invalid range")
	}
	spec := strings.TrimSpace(rangeHeader[len("bytes="):])
	if spec == "" || strings.Contains(spec, ",") {
		return 0, 0, errors.New("invalid range")
	}

	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, errors.New("invalid range")
	}

	if parts[0] == "" {
		suffixLength, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || suffixLength <= 0 {
			return 0, 0, errors.New("invalid range")
		}
		if suffixLength > size {
			suffixLength = size
		}
		start := size - suffixLength
		end := size - 1
		return start, end, nil
	}

	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, errors.New("invalid range")
	}

	var end int64
	if parts[1] == "" {
		end = size - 1
	} else {
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil || end < start {
			return 0, 0, errors.New("invalid range")
		}
		if end >= size {
			end = size - 1
		}
	}

	return start, end, nil
}

func (h *Handler) Start(ctx context.Context, address string) error {
	if ctx == nil {
		ctx = context.Background()
	}

	h.logger.Info("server_starting",
		"address", address,
		"log_format", h.logConfig.Format,
		"log_level", h.logConfig.LevelName,
		"audit_log", h.logConfig.Audit,
	)
	h.setupRoutes()

	server := http.Server{
		Addr:              address,
		Handler:           h.router,
		ReadHeaderTimeout: serverReadHeaderTimeout,
		ReadTimeout:       serverReadTimeout,
		WriteTimeout:      serverWriteTimeout,
		IdleTimeout:       serverIdleTimeout,
		MaxHeaderBytes:    serverMaxHeaderBytes,
	}
	errCh := make(chan error, 1)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	limitedListener := newLimitedListener(listener, serverMaxConnections)

	go func() {
		if err := server.Serve(limitedListener); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
			}
		}
	}()

	select {
	case <-ctx.Done():
		h.logger.Info("shutdown_context_done", "reason", ctx.Err())
	case err := <-errCh:
		h.logger.Error("server_listen_failed", "error", err)
		if closeErr := h.svc.Close(); closeErr != nil {
			h.logger.Error("service_close_failed", "error", closeErr)
		}
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		h.logger.Error("server_shutdown_failed", "error", err)
		return err
	}
	if err := h.svc.Close(); err != nil {
		h.logger.Error("service_close_failed", "error", err)
		return err
	}

	h.logger.Info("server_stopped")
	return nil
}
