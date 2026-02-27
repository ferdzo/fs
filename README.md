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

## Auth Setup

Required when `AUTH_ENABLED=true`:
- `AUTH_MASTER_KEY` must be base64 for 32 decoded bytes (AES-256 key), e.g. `openssl rand -base64 32`
- `AUTH_BOOTSTRAP_ACCESS_KEY` and `AUTH_BOOTSTRAP_SECRET_KEY` define initial credentials

Reference: `docs/auth-spec.md`

Health:
- `GET /healthz`
- `HEAD /healthz`

## Limitations

- Not full S3 API coverage.
- No versioning or lifecycle policies.
- Error and edge-case behavior is still being refined for client compatibility.

## License

MIT License
