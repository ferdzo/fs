package api

import (
	"encoding/xml"
	"errors"
	"fs/metadata"
	"fs/models"
	"fs/service"
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
	s3ErrInvalidPart = s3APIError{
		Status:  http.StatusBadRequest,
		Code:    "InvalidPart",
		Message: "One or more of the specified parts could not be found.",
	}
	s3ErrInvalidPartOrder = s3APIError{
		Status:  http.StatusBadRequest,
		Code:    "InvalidPartOrder",
		Message: "The list of parts was not in ascending order.",
	}
	s3ErrMalformedXML = s3APIError{
		Status:  http.StatusBadRequest,
		Code:    "MalformedXML",
		Message: "The XML you provided was not well-formed or did not validate against our published schema.",
	}
	s3ErrInvalidArgument = s3APIError{
		Status:  http.StatusBadRequest,
		Code:    "InvalidArgument",
		Message: "Invalid argument.",
	}
	s3ErrInvalidRange = s3APIError{
		Status:  http.StatusRequestedRangeNotSatisfiable,
		Code:    "InvalidRange",
		Message: "The requested range is not satisfiable.",
	}
	s3ErrEntityTooSmall = s3APIError{
		Status:  http.StatusBadRequest,
		Code:    "EntityTooSmall",
		Message: "Your proposed upload is smaller than the minimum allowed object size.",
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
	case errors.Is(err, metadata.ErrMultipartNotFound):
		return s3APIError{
			Status:  http.StatusNotFound,
			Code:    "NoSuchUpload",
			Message: "The specified multipart upload does not exist.",
		}
	case errors.Is(err, metadata.ErrMultipartNotPending):
		return s3APIError{
			Status:  http.StatusBadRequest,
			Code:    "InvalidRequest",
			Message: "The multipart upload is not in a valid state for this operation.",
		}
	case errors.Is(err, service.ErrInvalidPart):
		return s3ErrInvalidPart
	case errors.Is(err, service.ErrInvalidPartOrder):
		return s3ErrInvalidPartOrder
	case errors.Is(err, service.ErrInvalidCompleteRequest):
		return s3ErrMalformedXML
	case errors.Is(err, service.ErrEntityTooSmall):
		return s3ErrEntityTooSmall
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
