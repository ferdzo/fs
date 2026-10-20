package api

import (
	"encoding/xml"
	"fs/models"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const versioningNamespace = "http://s3.amazonaws.com/doc/2006-03-01/"

func versionIDFromRequest(r *http.Request) string {
	return strings.TrimSpace(r.URL.Query().Get("versionId"))
}

func setObjectVersionHeaders(w http.ResponseWriter, manifest *models.ObjectManifest) {
	if manifest != nil && manifest.VersionID != "" {
		w.Header().Set("x-amz-version-id", manifest.VersionID)
	}
}

// writeDeleteMarkerResponse renders an S3 delete-marker response: 405 when the
// client addressed the marker explicitly, 404 when the latest version is a
// marker. Both carry the marker metadata headers.
func (h *Handler) writeDeleteMarkerResponse(w http.ResponseWriter, r *http.Request, bucket, key, versionID string, explicit bool) {
	w.Header().Set("x-amz-delete-marker", "true")
	if manifest, err := h.svc.GetVersionManifest(bucket, key, versionID); err == nil {
		setObjectVersionHeaders(w, manifest)
		w.Header().Set("Last-Modified", time.Unix(manifest.CreatedAt, 0).UTC().Format(http.TimeFormat))
	}
	apiErr := s3APIError{Status: http.StatusNotFound, Code: "NoSuchKey", Message: "The specified key does not exist."}
	if explicit {
		apiErr = s3ErrMethodNotAllowed
	}
	writeS3Error(w, r, apiErr, r.URL.Path)
}

func (h *Handler) handleGetBucketVersioning(w http.ResponseWriter, r *http.Request, bucket string) {
	status, err := h.svc.GetBucketVersioning(bucket)
	if err != nil {
		writeMappedS3Error(w, r, err)
		return
	}
	configuration := models.VersioningConfiguration{Xmlns: versioningNamespace, Status: status}
	payload, err := xml.MarshalIndent(configuration, "", "    ")
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

func (h *Handler) handlePutBucketVersioning(w http.ResponseWriter, r *http.Request, bucket string) {
	defer r.Body.Close()
	r.Body = http.MaxBytesReader(w, r.Body, maxXMLBodyBytes)

	var configuration models.VersioningConfiguration
	if err := decodeBoundedXMLBody(r.Body, &configuration); err != nil {
		writeBoundedXMLBodyError(w, r, err)
		return
	}
	if strings.TrimSpace(configuration.MfaDelete) != "" {
		writeS3Error(w, r, s3ErrNotImplemented, r.URL.Path)
		return
	}
	configuration.Status = strings.TrimSpace(configuration.Status)
	if configuration.Status != "Enabled" && configuration.Status != "Suspended" {
		writeS3Error(w, r, s3ErrMalformedXML, r.URL.Path)
		return
	}
	if err := h.svc.SetBucketVersioning(bucket, configuration.Status); err != nil {
		writeMappedS3Error(w, r, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

type listVersionsResult struct {
	XMLName xml.Name `xml:"ListVersionsResult"`
	Xmlns   string   `xml:"xmlns,attr"`

	Name                string                   `xml:"Name"`
	Prefix              string                   `xml:"Prefix"`
	KeyMarker           string                   `xml:"KeyMarker"`
	VersionIDMarker     string                   `xml:"VersionIdMarker"`
	NextKeyMarker       string                   `xml:"NextKeyMarker,omitempty"`
	NextVersionIDMarker string                   `xml:"NextVersionIdMarker,omitempty"`
	Delimiter           string                   `xml:"Delimiter,omitempty"`
	MaxKeys             int                      `xml:"MaxKeys"`
	IsTruncated         bool                     `xml:"IsTruncated"`
	EncodingType        string                   `xml:"EncodingType,omitempty"`
	Versions            []listVersionEntry       `xml:"Version"`
	DeleteMarkers       []listDeleteMarkerEntry  `xml:"DeleteMarker"`
	CommonPrefixes      []listVersionsCommonPref `xml:"CommonPrefixes"`
}

type listVersionEntry struct {
	Key          string `xml:"Key"`
	VersionID    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type listDeleteMarkerEntry struct {
	Key          string `xml:"Key"`
	VersionID    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
}

type listVersionsCommonPref struct {
	Prefix string `xml:"Prefix"`
}

func (h *Handler) handleListObjectVersions(w http.ResponseWriter, r *http.Request, bucket string) {
	query := r.URL.Query()
	prefix := query.Get("prefix")
	delimiter := query.Get("delimiter")
	keyMarker := query.Get("key-marker")
	versionMarker := query.Get("version-id-marker")
	encodingType := strings.TrimSpace(query.Get("encoding-type"))
	if encodingType != "" && encodingType != "url" {
		writeS3Error(w, r, s3ErrInvalidArgument, r.URL.Path)
		return
	}
	maxKeys := 1000
	if raw := strings.TrimSpace(query.Get("max-keys")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 0 {
			writeS3Error(w, r, s3ErrInvalidArgument, r.URL.Path)
			return
		}
		maxKeys = value
	}

	page, err := h.svc.ListObjectVersions(bucket, prefix, delimiter, keyMarker, versionMarker, maxKeys)
	if err != nil {
		writeMappedS3Error(w, r, err)
		return
	}

	result := listVersionsResult{
		Xmlns:               versioningNamespace,
		Name:                bucket,
		Prefix:              s3EncodeIfNeeded(prefix, encodingType),
		KeyMarker:           s3EncodeIfNeeded(keyMarker, encodingType),
		VersionIDMarker:     versionMarker,
		NextKeyMarker:       s3EncodeIfNeeded(page.NextKeyMarker, encodingType),
		NextVersionIDMarker: page.NextVersionIDMarker,
		Delimiter:           s3EncodeIfNeeded(delimiter, encodingType),
		MaxKeys:             maxKeys,
		IsTruncated:         page.IsTruncated,
		EncodingType:        encodingType,
	}
	for _, entry := range page.Versions {
		manifest := entry.Manifest
		lastModified := time.Unix(manifest.CreatedAt, 0).UTC().Format("2006-01-02T15:04:05.000Z")
		if manifest.DeleteMarker {
			result.DeleteMarkers = append(result.DeleteMarkers, listDeleteMarkerEntry{
				Key:          s3EncodeIfNeeded(manifest.Key, encodingType),
				VersionID:    manifest.VersionID,
				IsLatest:     entry.IsLatest,
				LastModified: lastModified,
			})
			continue
		}
		result.Versions = append(result.Versions, listVersionEntry{
			Key:          s3EncodeIfNeeded(manifest.Key, encodingType),
			VersionID:    manifest.VersionID,
			IsLatest:     entry.IsLatest,
			LastModified: lastModified,
			ETag:         `"` + manifest.ETag + `"`,
			Size:         manifest.Size,
			StorageClass: "STANDARD",
		})
	}
	for _, commonPrefix := range page.CommonPrefixes {
		result.CommonPrefixes = append(result.CommonPrefixes, listVersionsCommonPref{
			Prefix: s3EncodeIfNeeded(commonPrefix, encodingType),
		})
	}

	payload, err := xml.MarshalIndent(result, "", "    ")
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
