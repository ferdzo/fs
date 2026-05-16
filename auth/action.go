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
	ActionListBucket            Action = "s3:ListBucket"
	ActionGetObject             Action = "s3:GetObject"
	ActionPutObject             Action = "s3:PutObject"
	ActionDeleteObject          Action = "s3:DeleteObject"
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

	if key == "" {
		switch r.Method {
		case http.MethodPut:
			return RequestTarget{Action: ActionCreateBucket, Bucket: bucket}
		case http.MethodHead:
			return RequestTarget{Action: ActionHeadBucket, Bucket: bucket}
		case http.MethodDelete:
			return RequestTarget{Action: ActionDeleteBucket, Bucket: bucket}
		case http.MethodGet:
			return RequestTarget{Action: ActionListBucket, Bucket: bucket, Prefix: r.URL.Query().Get("prefix")}
		case http.MethodPost:
			if _, ok := r.URL.Query()["delete"]; ok {
				return RequestTarget{Action: ActionDeleteObject, Bucket: bucket}
			}
		}
		return RequestTarget{Bucket: bucket}
	}

	uploadID := r.URL.Query().Get("uploadId")
	partNumber := r.URL.Query().Get("partNumber")
	if _, ok := r.URL.Query()["uploads"]; ok && r.Method == http.MethodPost {
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

	switch r.Method {
	case http.MethodGet, http.MethodHead:
		return RequestTarget{Action: ActionGetObject, Bucket: bucket, Key: key}
	case http.MethodPut:
		return RequestTarget{Action: ActionPutObject, Bucket: bucket, Key: key}
	case http.MethodDelete:
		return RequestTarget{Action: ActionDeleteObject, Bucket: bucket, Key: key}
	}

	return RequestTarget{Bucket: bucket, Key: key}
}
