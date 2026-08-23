# S3 Compatibility Matrix

This project is S3-compatible for a focused subset of operations.

## Implemented

### Service and account
- `GET /` list buckets

### Bucket
- `PUT /{bucket}` create bucket
- `HEAD /{bucket}` head bucket
- `DELETE /{bucket}` delete bucket (must be empty)
- `GET /{bucket}?list-type=2...` list objects v2
- `GET /{bucket}?location` get bucket location
- `POST /{bucket}?delete` delete multiple objects

### Object
- `PUT /{bucket}/{key}` put object
- `GET /{bucket}/{key}` get object
- `HEAD /{bucket}/{key}` head object
- `DELETE /{bucket}/{key}` delete object
- `GET /{bucket}/{key}` supports single-range requests

### Multipart upload
- `POST /{bucket}/{key}?uploads` initiate
- `PUT /{bucket}/{key}?uploadId=...&partNumber=N` upload part
- `GET /{bucket}/{key}?uploadId=...` list parts
- `POST /{bucket}/{key}?uploadId=...` complete
- `DELETE /{bucket}/{key}?uploadId=...` abort

### Authentication
- AWS SigV4 header auth
- AWS SigV4 presigned query auth
- `aws-chunked` payload decode for unsigned streaming upload modes
- SigV4 payload hash verification for fixed-size signed payloads

## Partially Implemented / Differences
- Exact parity with AWS S3 error codes/headers is still evolving.
- Some S3 edge-case behaviors may differ (especially uncommon query/header combinations).
- Admin API is custom JSON (`/_admin/v1/*`).
- Object and upload-part payloads are limited by `FS_MAX_OBJECT_UPLOAD_BYTES` (default 5 GiB).
- Signed `aws-chunked` payload modes (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD` and `-TRAILER`) are fully decoded with per-chunk signature chain verification; tampered chunks are rejected mid-upload with `SignatureDoesNotMatch`. When auth is disabled these modes are decoded without signature checks (no secret is available).

## Not Implemented (Current)
- Bucket versioning
- Lifecycle rules
- Replication
- Object lock / legal hold / retention
- SSE-S3 / SSE-KMS / SSE-C
- ACL APIs and IAM-compatible policy APIs
- STS / temporary credentials
- Event notifications
- Tagging APIs
- CORS APIs
- Website hosting APIs
