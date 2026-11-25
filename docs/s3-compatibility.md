# S3 Compatibility Matrix

This project is S3-compatible for a focused subset of operations.

## Implemented

### Service and account
- `GET /` list buckets

### Bucket
- `PUT /{bucket}` create bucket
- `HEAD /{bucket}` head bucket
- `DELETE /{bucket}` delete bucket (must be empty)
- `GET /{bucket}` list objects v1
- `GET /{bucket}?list-type=2...` list objects v2
- `GET /{bucket}?location` get bucket location
- `GET /{bucket}?uploads` list multipart uploads
- `GET /{bucket}?versioning` get bucket versioning status
- `PUT /{bucket}?versioning` enable or suspend bucket versioning
- `GET /{bucket}?versions` list object versions and delete markers
- `POST /{bucket}?delete` delete multiple objects

### Object
- `PUT /{bucket}/{key}` put object (supports `If-None-Match` precondition)
- `PUT /{bucket}/{key}` with `x-amz-copy-source` copy object (supports `?versionId=...`)
- `GET /{bucket}/{key}` get object (supports `?versionId=...`)
- `HEAD /{bucket}/{key}` head object (supports `?versionId=...`)
- `DELETE /{bucket}/{key}` delete object (supports `?versionId=...` and delete markers)
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
- `aws-chunked` payload decode for unsigned (`STREAMING-UNSIGNED-PAYLOAD*`) and signed (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`, `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`) streaming upload modes
- SigV4 payload hash verification for fixed-size signed payloads

## Partially Implemented / Differences
- Exact parity with AWS S3 error codes/headers is still evolving.
- Some S3 edge-case behaviors may differ (especially uncommon query/header combinations).
- Admin API is custom JSON (`/_admin/v1/*`).
- Write-only principals can infer key existence via `If-None-Match` preconditions (PUT returns 412 for existing vs proceeding for absent keys). This matches real S3 behaviour for write-only IAM policies.
- Repeated failed authentications from one source are throttled with `503 SlowDown` + `Retry-After` (tunable via `FS_AUTH_FAILURE_LIMIT_PER_MIN`, 0 disables).

- Object and upload-part payloads are limited by `FS_MAX_OBJECT_UPLOAD_BYTES` (default 5 GiB).
- Signed `aws-chunked` payload modes (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD` and `-TRAILER`) are fully decoded with per-chunk signature chain verification; tampered chunks are rejected mid-upload with `SignatureDoesNotMatch`. When auth is disabled these modes are decoded without signature checks (no secret is available).

## Not Implemented (Current)
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
