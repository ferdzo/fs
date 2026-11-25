package auth

import (
	"net/http"
	"strings"
)

type Action string

const (
	ActionListAllMyBuckets      Action = "s3:ListAllMyBuckets"
	ActionCreateBucket          Action = "s3:CreateBucket"
	ActionHeadBucket            Action = "s3:HeadBucket"
	ActionDeleteBucket          Action = "s3:DeleteBucket"
	ActionGetBucketLocation     Action = "s3:GetBucketLocation"
	ActionGetBucketVersioning   Action = "s3:GetBucketVersioning"
	ActionPutBucketVersioning   Action = "s3:PutBucketVersioning"
	ActionListBucket            Action = "s3:ListBucket"
	ActionListBucketVersions    Action = "s3:ListBucketVersions"
	ActionListBucketMultipart   Action = "s3:ListBucketMultipartUploads"
	ActionGetObject             Action = "s3:GetObject"
	ActionGetObjectVersion      Action = "s3:GetObjectVersion"
	ActionPutObject             Action = "s3:PutObject"
	ActionDeleteObject          Action = "s3:DeleteObject"
	ActionDeleteObjectVersion   Action = "s3:DeleteObjectVersion"
	ActionCreateMultipartUpload Action = "s3:CreateMultipartUpload"
	ActionUploadPart            Action = "s3:UploadPart"
	ActionListMultipartParts    Action = "s3:ListMultipartUploadParts"
	ActionCompleteMultipart     Action = "s3:CompleteMultipartUpload"
	ActionAbortMultipartUpload  Action = "s3:AbortMultipartUpload"
)

type RequestTarget struct {
	Action Action
	Bucket string
	Key    string
	Prefix string
}

func RequiresHandlerAuthorization(r *http.Request) bool {
	if r == nil || r.URL == nil {
		return false
	}
	if r.Method != http.MethodPost {
		return false
	}
	query := r.URL.Query()
	if _, isDelete := query["delete"]; !isDelete {
		return false
	}
	// Multipart lifecycle requests are policy-checked here like every other
	// API call; only multi-object delete defers to per-key handler checks.
	if _, ok := query["uploads"]; ok {
		return false
	}
	if _, ok := query["uploadId"]; ok {
		return false
	}
	return true
}

func resolveTarget(r *http.Request) RequestTarget {
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		return RequestTarget{Action: ActionListAllMyBuckets}
	}

	parts := strings.SplitN(path, "/", 2)
	bucket := parts[0]
	key := ""
	if len(parts) > 1 {
		key = parts[1]
	}

	query := r.URL.Query()
	if key == "" {
		switch r.Method {
		case http.MethodPut:
			if query.Has("versioning") {
				return RequestTarget{Action: ActionPutBucketVersioning, Bucket: bucket}
			}
			return RequestTarget{Action: ActionCreateBucket, Bucket: bucket}
		case http.MethodHead:
			return RequestTarget{Action: ActionHeadBucket, Bucket: bucket}
		case http.MethodDelete:
			return RequestTarget{Action: ActionDeleteBucket, Bucket: bucket}
		case http.MethodGet:
			switch {
			case query.Has("versioning"):
				return RequestTarget{Action: ActionGetBucketVersioning, Bucket: bucket}
			case query.Has("location"):
				return RequestTarget{Action: ActionGetBucketLocation, Bucket: bucket}
			case query.Has("versions"):
				return RequestTarget{Action: ActionListBucketVersions, Bucket: bucket, Prefix: query.Get("prefix")}
			case query.Has("uploads"):
				return RequestTarget{Action: ActionListBucketMultipart, Bucket: bucket, Prefix: query.Get("prefix")}
			default:
				return RequestTarget{Action: ActionListBucket, Bucket: bucket, Prefix: query.Get("prefix")}
			}
		case http.MethodPost:
			if _, ok := query["delete"]; ok {
				return RequestTarget{Action: ActionDeleteObject, Bucket: bucket}
			}
		}
		return RequestTarget{Bucket: bucket}
	}

	uploadID := query.Get("uploadId")
	partNumber := query.Get("partNumber")
	if _, ok := query["uploads"]; ok && r.Method == http.MethodPost {
		return RequestTarget{Action: ActionCreateMultipartUpload, Bucket: bucket, Key: key}
	}
	if uploadID != "" {
		switch r.Method {
		case http.MethodPut:
			if partNumber != "" {
				return RequestTarget{Action: ActionUploadPart, Bucket: bucket, Key: key}
			}
		case http.MethodGet:
			return RequestTarget{Action: ActionListMultipartParts, Bucket: bucket, Key: key}
		case http.MethodPost:
			return RequestTarget{Action: ActionCompleteMultipart, Bucket: bucket, Key: key}
		case http.MethodDelete:
			return RequestTarget{Action: ActionAbortMultipartUpload, Bucket: bucket, Key: key}
		}
	}

	versionID := strings.TrimSpace(query.Get("versionId"))
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		if versionID != "" {
			return RequestTarget{Action: ActionGetObjectVersion, Bucket: bucket, Key: key}
		}
		return RequestTarget{Action: ActionGetObject, Bucket: bucket, Key: key}
	case http.MethodPut:
		return RequestTarget{Action: ActionPutObject, Bucket: bucket, Key: key}
	case http.MethodDelete:
		if versionID != "" {
			return RequestTarget{Action: ActionDeleteObjectVersion, Bucket: bucket, Key: key}
		}
		return RequestTarget{Action: ActionDeleteObject, Bucket: bucket, Key: key}
	}

	return RequestTarget{Bucket: bucket, Key: key}
}
