package api

import (
	"encoding/xml"
	"errors"
	"fs/metadata"
	"fs/models"
	"net/http"
)

type s3APIError struct {
	Status  int
	Code    string
	Message string
}

var (
	s3ErrInvalidObjectKey = s3APIError{
		Status:  http.StatusBadRequest,
		Code:    "InvalidArgument",
		Message: "Object key is required.",
	}
	s3ErrNotImplemented = s3APIError{
		Status:  http.StatusNotImplemented,
		Code:    "NotImplemented",
		Message: "A header you provided implies functionality that is not implemented.",
	}
	s3ErrInternal = s3APIError{
		Status:  http.StatusInternalServerError,
		Code:    "InternalError",
		Message: "We encountered an internal error. Please try again.",
	}
)

func mapToS3Error(err error) s3APIError {
	switch {
	case errors.Is(err, metadata.ErrInvalidBucketName):
		return s3APIError{
			Status:  http.StatusBadRequest,
			Code:    "InvalidBucketName",
			Message: "The specified bucket is not valid.",
		}
	case errors.Is(err, metadata.ErrBucketAlreadyExists):
		return s3APIError{
			Status:  http.StatusConflict,
			Code:    "BucketAlreadyOwnedByYou",
			Message: "Your previous request to create the named bucket succeeded and you already own it.",
		}
	case errors.Is(err, metadata.ErrBucketNotFound):
		return s3APIError{
			Status:  http.StatusNotFound,
			Code:    "NoSuchBucket",
			Message: "The specified bucket does not exist.",
		}
	case errors.Is(err, metadata.ErrBucketNotEmpty):
		return s3APIError{
			Status:  http.StatusConflict,
			Code:    "BucketNotEmpty",
			Message: "The bucket you tried to delete is not empty.",
		}
	case errors.Is(err, metadata.ErrObjectNotFound):
		return s3APIError{
			Status:  http.StatusNotFound,
			Code:    "NoSuchKey",
			Message: "The specified key does not exist.",
		}
	default:
		return s3ErrInternal
	}
}

func writeS3Error(w http.ResponseWriter, r *http.Request, apiErr s3APIError, resource string) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(apiErr.Status)

	if r != nil && r.Method == http.MethodHead {
		return
	}

	payload := models.S3ErrorResponse{
		Code:     apiErr.Code,
		Message:  apiErr.Message,
		Resource: resource,
	}

	out, err := xml.MarshalIndent(payload, "", "    ")
	if err != nil {
		return
	}

	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(out)
}

func writeMappedS3Error(w http.ResponseWriter, r *http.Request, err error) {
	writeS3Error(w, r, mapToS3Error(err), r.URL.Path)
}
