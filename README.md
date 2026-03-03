# fs

An experimental Object Storage written in Go that should be partially compatible with S3

## Features

Bucket operations:
- `PUT /{bucket}`
- `HEAD /{bucket}`
- `DELETE /{bucket}`
- `GET /` (list buckets)

Object operations:
- `PUT /{bucket}/{key}`
- `GET /{bucket}/{key}`
- `HEAD /{bucket}/{key}`
- `DELETE /{bucket}/{key}`
- `GET /{bucket}?list-type=2&prefix=...` (ListObjectsV2-style)

Multipart upload:
- `POST /{bucket}/{key}?uploads` (initiate)
- `PUT /{bucket}/{key}?uploadId=...&partNumber=N` (upload part)
- `GET /{bucket}/{key}?uploadId=...` (list parts)
- `POST /{bucket}/{key}?uploadId=...` (complete)
- `DELETE /{bucket}/{key}?uploadId=...` (abort)

Multi-object delete:
- `POST /{bucket}?delete` with S3-style XML body

AWS SigV4 streaming payload decoding for uploads (`aws-chunked` request bodies)

Authentication:
- AWS SigV4 request verification (header and presigned URL forms)
- Local credential/policy store in bbolt
- Bootstrap access key/secret via environment variables

Admin API (JSON):
- `POST /_admin/v1/users`
- `GET /_admin/v1/users`
- `GET /_admin/v1/users/{accessKeyId}`
- `PUT /_admin/v1/users/{accessKeyId}/policy`
- `PUT /_admin/v1/users/{accessKeyId}/status`
- `DELETE /_admin/v1/users/{accessKeyId}`

## Auth Setup

Required when `FS_AUTH_ENABLED=true`:
- `FS_MASTER_KEY` must be base64 for 32 decoded bytes (AES-256 key), e.g. `openssl rand -base64 32`
- `FS_ROOT_USER` and `FS_ROOT_PASSWORD` define initial credentials
- `ADMIN_API_ENABLED=true` enables `/_admin/v1/*` routes (bootstrap key only)

Reference: `auth/README.md`

Additional docs:
- Admin OpenAPI spec: `docs/admin-api-openapi.yaml`
- S3 compatibility matrix: `docs/s3-compatibility.md`

Health:
- `GET /healthz`
- `HEAD /healthz`
- `GET /metrics` (Prometheus exposition format)
- `HEAD /metrics`

## Limitations

- Not full S3 API coverage.
- No versioning or lifecycle policies.
- Error and edge-case behavior is still being refined for client compatibility.

## License

MIT License
