# fs

An experimental Object Storage written in Go that should be partially compatible with S3

## Features

- Bucket operations:
- `PUT /{bucket}`
- `HEAD /{bucket}`
- `DELETE /{bucket}`
- `GET /` (list buckets)
- Object operations:
- `PUT /{bucket}/{key}`
- `GET /{bucket}/{key}`
- `HEAD /{bucket}/{key}`
- `DELETE /{bucket}/{key}`
- `GET /{bucket}?list-type=2&prefix=...` (ListObjectsV2-style)
- Multipart upload:
- `POST /{bucket}/{key}?uploads` (initiate)
- `PUT /{bucket}/{key}?uploadId=...&partNumber=N` (upload part)
- `GET /{bucket}/{key}?uploadId=...` (list parts)
- `POST /{bucket}/{key}?uploadId=...` (complete)
- `DELETE /{bucket}/{key}?uploadId=...` (abort)
- Multi-object delete:
- `POST /{bucket}?delete` with S3-style XML body
- AWS SigV4 streaming payload decoding for uploads (`aws-chunked` request bodies)

## Limitations

- No authentication/authorization yet.
- Not full S3 API coverage.
- No garbage collection of unreferenced blob chunks yet.
- No versioning or lifecycle policies.
- Error and edge-case behavior is still being refined for client compatibility.

# License

MIT License
