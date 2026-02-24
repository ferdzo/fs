package models

import (
	"encoding/xml"
	"time"
)

type ObjectManifest struct {
	Bucket      string   `json:"bucket"`
	Key         string   `json:"key"`
	Size        int64    `json:"size"`
	ContentType string   `json:"content_type"`
	ETag        string   `json:"etag"`
	Chunks      []string `json:"chunks"`
	CreatedAt   int64    `json:"created_at"`
}
type BucketManifest struct {
	Name              string    `json:"name"`
	CreatedAt         time.Time `json:"created_at"`
	OwnerID           string    `json:"owner_id"`
	OwnerDisplayName  string    `json:"owner_display_name"`
	Region            string    `json:"region"`
	VersioningStatus  string    `json:"versioning_status"`
	PublicAccessBlock bool      `json:"public_access_block"`
}

type ListAllMyBucketsResult struct {
	XMLName xml.Name       `xml:"ListAllMyBucketsResult"`
	Xmlns   string         `xml:"xmlns,attr"`
	Owner   BucketsOwner   `xml:"Owner"`
	Buckets BucketsElement `xml:"Buckets"`
}

type BucketsOwner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName,omitempty"`
}

type BucketsElement struct {
	Items []BucketItem `xml:"Bucket"`
}

type BucketItem struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type S3ErrorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId,omitempty"`
	HostID    string   `xml:"HostId,omitempty"`
}

type ListBucketResult struct {
	XMLName xml.Name `xml:"ListBucketResult"`
	Xmlns   string   `xml:"xmlns,attr"`

	Name        string `xml:"Name"`
	Prefix      string `xml:"Prefix"`
	KeyCount    int    `xml:"KeyCount"`
	MaxKeys     int    `xml:"MaxKeys"`
	IsTruncated bool   `xml:"IsTruncated"`

	Contents       []Contents       `xml:"Contents"`
	CommonPrefixes []CommonPrefixes `xml:"CommonPrefixes,omitempty"`
}

type ListBucketResultV2 struct {
	XMLName xml.Name `xml:"ListBucketResult"`
	Xmlns   string   `xml:"xmlns,attr"`

	Name                  string `xml:"Name"`
	Prefix                string `xml:"Prefix"`
	Delimiter             string `xml:"Delimiter,omitempty"`
	MaxKeys               int    `xml:"MaxKeys"`
	KeyCount              int    `xml:"KeyCount"`
	IsTruncated           bool   `xml:"IsTruncated"`
	ContinuationToken     string `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string `xml:"NextContinuationToken,omitempty"`
	StartAfter            string `xml:"StartAfter,omitempty"`
	EncodingType          string `xml:"EncodingType,omitempty"`

	Contents       []Contents       `xml:"Contents,omitempty"`
	CommonPrefixes []CommonPrefixes `xml:"CommonPrefixes,omitempty"`
}

type Contents struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type CommonPrefixes struct {
	Prefix string `xml:"Prefix"`
}

type MultipartUpload struct {
	UploadID  string `json:"upload_id" xml:"UploadId"`
	Bucket    string `json:"bucket" xml:"Bucket"`
	Key       string `json:"key" xml:"Key"`
	CreatedAt string `json:"created_at" xml:"CreatedAt"`
	State     string `json:"state" xml:"State"`
}

type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}
type UploadedPart struct {
	PartNumber int      `json:"part_number" xml:"PartNumber"`
	ETag       string   `json:"etag" xml:"ETag"`
	Size       int64    `json:"size" xml:"Size"`
	Chunks     []string `json:"chunks"`
	CreatedAt  int64    `json:"created_at"`
}

type CompletedPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type CompleteMultipartUploadRequest struct {
	XMLName xml.Name        `xml:"CompleteMultipartUpload"`
	Parts   []CompletedPart `xml:"Part"`
}

type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
	Location string   `xml:"Location,omitempty"`
}

type ListPartsResult struct {
	XMLName  xml.Name   `xml:"ListPartsResult"`
	Xmlns    string     `xml:"xmlns,attr"`
	Bucket   string     `xml:"Bucket"`
	Key      string     `xml:"Key"`
	UploadID string     `xml:"UploadId"`
	Parts    []PartItem `xml:"Part"`
}

type PartItem struct {
	PartNumber   int    `xml:"PartNumber"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

type DeleteObjectsRequest struct {
	XMLName xml.Name               `xml:"Delete"`
	Objects []DeleteObjectIdentity `xml:"Object"`
	Quiet   bool                   `xml:"Quiet"`
}

type DeleteObjectIdentity struct {
	Key string `xml:"Key"`
}

type DeleteObjectsResult struct {
	XMLName xml.Name       `xml:"DeleteResult"`
	Xmlns   string         `xml:"xmlns,attr"`
	Deleted []DeletedEntry `xml:"Deleted,omitempty"`
	Errors  []DeleteError  `xml:"Error,omitempty"`
}

type DeletedEntry struct {
	Key string `xml:"Key"`
}

type DeleteError struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}
