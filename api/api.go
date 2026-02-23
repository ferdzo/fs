package api

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"fs/logging"
	"fs/metadata"
	"fs/models"
	"fs/service"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type Handler struct {
	router    *chi.Mux
	svc       *service.ObjectService
	logger    *slog.Logger
	logConfig logging.Config
}

func NewHandler(svc *service.ObjectService, logger *slog.Logger, logConfig logging.Config) *Handler {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	if logger == nil {
		logger = slog.Default()
	}

	h := &Handler{
		router:    r,
		svc:       svc,
		logger:    logger,
		logConfig: logConfig,
	}
	return h
}

func (h *Handler) setupRoutes() {
	h.router.Use(logging.HTTPMiddleware(h.logger, h.logConfig))

	h.router.Get("/", h.handleGetBuckets)

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

func (h *Handler) handleWelcome(w http.ResponseWriter) {
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
	_, err = io.Copy(w, stream)

}

func (h *Handler) handlePostObject(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key := chi.URLParam(r, "*")
	if key == "" {
		writeS3Error(w, r, s3ErrInvalidObjectKey, r.URL.Path)
		return
	}
	defer r.Body.Close()

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

	if uploadID := r.URL.Query().Get("uploadId"); uploadID != "" {
		var req models.CompleteMultipartUploadRequest
		if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
			writeS3Error(w, r, s3ErrMalformedXML, r.URL.Path)
			return
		}

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
	defer r.Body.Close()

	uploadID := r.URL.Query().Get("uploadId")
	partNumberRaw := r.URL.Query().Get("partNumber")
	if uploadID != "" || partNumberRaw != "" {
		if uploadID == "" || partNumberRaw == "" {
			writeS3Error(w, r, s3ErrInvalidPart, r.URL.Path)
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

		bodyReader := io.Reader(r.Body)
		var decodeStream io.ReadCloser
		if shouldDecodeAWSChunkedPayload(r) {
			decodeStream = newAWSChunkedDecodingReader(r.Body)
			defer decodeStream.Close()
			bodyReader = decodeStream
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

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	bodyReader := io.Reader(r.Body)
	var decodeStream io.ReadCloser
	if shouldDecodeAWSChunkedPayload(r) {
		decodeStream = newAWSChunkedDecodingReader(r.Body)
		defer decodeStream.Close()
		bodyReader = decodeStream
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
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(payload)
}

func shouldDecodeAWSChunkedPayload(r *http.Request) bool {
	contentEncoding := strings.ToLower(r.Header.Get("Content-Encoding"))
	if strings.Contains(contentEncoding, "aws-chunked") {
		return true
	}
	signingMode := strings.ToLower(r.Header.Get("x-amz-content-sha256"))
	return strings.HasPrefix(signingMode, "streaming-aws4-hmac-sha256-payload")
}

func newAWSChunkedDecodingReader(src io.Reader) io.ReadCloser {
	pr, pw := io.Pipe()
	go func() {
		if err := decodeAWSChunkedPayload(src, pw); err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		_ = pw.Close()
	}()
	return pr
}

func decodeAWSChunkedPayload(src io.Reader, dst io.Writer) error {
	reader := bufio.NewReader(src)
	for {
		headerLine, err := reader.ReadString('\n')
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

		if chunkSize == 0 {
			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					return err
				}
				if line == "\r\n" || line == "\n" {
					return nil
				}
			}
		}
	}
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
	etag := manifest.ETag
	size := strconv.FormatInt(manifest.Size, 10)

	w.Header().Set("ETag", `"`+etag+`"`)
	w.Header().Set("Content-Length", size)
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

func (h *Handler) handlePostBucket(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	if _, ok := r.URL.Query()["delete"]; !ok {
		writeS3Error(w, r, s3ErrNotImplemented, r.URL.Path)
		return
	}
	defer r.Body.Close()

	bodyReader := io.Reader(r.Body)
	var decodeStream io.ReadCloser
	if shouldDecodeAWSChunkedPayload(r) {
		decodeStream = newAWSChunkedDecodingReader(r.Body)
		defer decodeStream.Close()
		bodyReader = decodeStream
	}

	var req models.DeleteObjectsRequest
	if err := xml.NewDecoder(bodyReader).Decode(&req); err != nil {
		writeS3Error(w, r, s3ErrMalformedXML, r.URL.Path)
		return
	}

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
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(payload)
}

func (h *Handler) handleDeleteObject(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key := chi.URLParam(r, "*")
	if key == "" {
		writeS3Error(w, r, s3ErrInvalidObjectKey, r.URL.Path)
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
	w.WriteHeader(http.StatusOK)
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
			writeMappedS3Error(w, r, err)
			return
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
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(payload)
}

func (h *Handler) handleGetBucket(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")

	if r.URL.Query().Get("list-type") == "2" {
		h.handleListObjectsV2(w, r, bucket)
		return
	}
	if r.URL.Query().Has("location") {
		xmlResponse := `<?xml version="1.0" encoding="UTF-8"?>
						<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`

		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.Header().Set("Content-Length", strconv.Itoa(len(xmlResponse)))
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(xmlResponse))
		if err != nil {
			return
		}
		return
	}
	writeS3Error(w, r, s3ErrNotImplemented, r.URL.Path)

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
	if continuationToken != "" {
		decoded, err := base64.StdEncoding.DecodeString(continuationToken)
		if err != nil || len(decoded) == 0 {
			writeS3Error(w, r, s3ErrInvalidArgument, r.URL.Path)
			return
		}
		continuationMarker = string(decoded)
	}

	objects, err := h.svc.ListObjects(bucket, prefix)
	if err != nil {
		writeMappedS3Error(w, r, err)
		return
	}

	entries := buildListV2Entries(objects, prefix, delimiter)
	startIdx := 0
	if continuationMarker != "" {
		found := false
		for i, entry := range entries {
			if entry.Marker == continuationMarker {
				startIdx = i + 1
				found = true
				break
			}
		}
		if !found {
			writeS3Error(w, r, s3ErrInvalidArgument, r.URL.Path)
			return
		}
	} else if startAfter != "" {
		for startIdx < len(entries) && entries[startIdx].SortKey <= startAfter {
			startIdx++
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

	endIdx := startIdx
	for endIdx < len(entries) && result.KeyCount < maxKeys {
		entry := entries[endIdx]
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
		endIdx++
	}

	result.IsTruncated = endIdx < len(entries)
	if result.IsTruncated && result.KeyCount > 0 {
		result.NextContinuationToken = base64.StdEncoding.EncodeToString([]byte(entries[endIdx-1].Marker))
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

type listV2Entry struct {
	Marker       string
	SortKey      string
	Object       *models.ObjectManifest
	CommonPrefix string
}

func buildListV2Entries(objects []*models.ObjectManifest, prefix, delimiter string) []listV2Entry {
	sorted := make([]*models.ObjectManifest, 0, len(objects))
	sorted = append(sorted, objects...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Key < sorted[j].Key
	})

	entries := make([]listV2Entry, 0, len(sorted))
	seenCommonPrefixes := make(map[string]struct{})
	for _, object := range sorted {
		if object == nil {
			continue
		}
		if delimiter != "" {
			relative := strings.TrimPrefix(object.Key, prefix)
			if idx := strings.Index(relative, delimiter); idx >= 0 {
				commonPrefix := prefix + relative[:idx+len(delimiter)]
				if _, exists := seenCommonPrefixes[commonPrefix]; exists {
					continue
				}
				seenCommonPrefixes[commonPrefix] = struct{}{}
				entries = append(entries, listV2Entry{
					Marker:       "C:" + commonPrefix,
					SortKey:      commonPrefix,
					CommonPrefix: commonPrefix,
				})
				continue
			}
		}

		entries = append(entries, listV2Entry{
			Marker:  "K:" + object.Key,
			SortKey: object.Key,
			Object:  object,
		})
	}
	return entries
}

func s3EncodeIfNeeded(value, encodingType string) string {
	if encodingType != "url" || value == "" {
		return value
	}
	encoded := url.QueryEscape(value)
	return strings.ReplaceAll(encoded, "+", "%20")
}

func parseSingleByteRange(rangeHeader string, size int64) (int64, int64, error) {
	if size <= 0 || !strings.HasPrefix(rangeHeader, "bytes=") {
		return 0, 0, errors.New("invalid range")
	}
	spec := strings.TrimSpace(strings.TrimPrefix(rangeHeader, "bytes="))
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
		Addr:    address,
		Handler: h.router,
	}
	errCh := make(chan error, 1)

	go func() {
		if err := server.ListenAndServe(); err != nil {
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
