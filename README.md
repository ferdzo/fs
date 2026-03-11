# fs

An experimental Object Storage written in Go that should be partially compatible with S3

## Running

Single binary, two modes:
- `fs` (no subcommand) starts the server (backward compatible)
- `fs server` starts the server explicitly
- `fs admin ...` runs admin CLI commands

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

Admin API policy examples (SigV4):
```bash
ENDPOINT="http://localhost:3000"
REGION="us-east-1"
ADMIN_ACCESS_KEY="${FS_ROOT_USER}"
ADMIN_SECRET_KEY="${FS_ROOT_PASSWORD}"
SIGV4="aws:amz:${REGION}:s3"
```

Replace user policy with one scoped statement:
```bash
curl --aws-sigv4 "$SIGV4" \
  --user "${ADMIN_ACCESS_KEY}:${ADMIN_SECRET_KEY}" \
  -H "Content-Type: application/json" \
  -X PUT "${ENDPOINT}/_admin/v1/users/test-user/policy" \
  -d '{
    "policy": {
      "statements": [
        {
          "effect": "allow",
          "actions": ["s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
          "bucket": "backup",
          "prefix": "restic/*"
        }
      ]
    }
  }'
```

Set multiple statements (for multiple buckets):
```bash
curl --aws-sigv4 "$SIGV4" \
  --user "${ADMIN_ACCESS_KEY}:${ADMIN_SECRET_KEY}" \
  -H "Content-Type: application/json" \
  -X PUT "${ENDPOINT}/_admin/v1/users/test-user/policy" \
  -d '{
    "policy": {
      "statements": [
        {
          "effect": "allow",
          "actions": ["s3:ListBucket", "s3:GetObject"],
          "bucket": "test-bucket",
          "prefix": "*"
        },
        {
          "effect": "allow",
          "actions": ["s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
          "bucket": "test-bucket-2",
          "prefix": "*"
        }
      ]
    }
  }'
```

Admin CLI:
- `fs admin user create --access-key backup-user --role readwrite`
- `fs admin user list`
- `fs admin user get backup-user`
- `fs admin user set-status backup-user --status disabled`
- `fs admin user set-role backup-user --role readonly --bucket backup-bucket --prefix restic/`
- `fs admin user set-role backup-user --role readwrite --bucket backups-2` (appends another statement)
- `fs admin user remove-role backup-user --role readonly --bucket backup-bucket --prefix restic/`
- `fs admin user set-role backup-user --role admin --replace` (replaces all statements)
- `fs admin user delete backup-user`
- `fs admin snapshot create --data-path /var/lib/fs --out /backup/fs-20260311.tar.gz`
- `fs admin snapshot inspect --file /backup/fs-20260311.tar.gz`
- `fs admin snapshot restore --file /backup/fs-20260311.tar.gz --data-path /var/lib/fs --force`
- `fs admin diag health`
- `fs admin diag version`

## Auth Setup

Required when `FS_AUTH_ENABLED=true`:
- `FS_MASTER_KEY` must be base64 for 32 decoded bytes (AES-256 key), e.g. `openssl rand -base64 32`
- `FS_ROOT_USER` and `FS_ROOT_PASSWORD` define initial credentials
- `ADMIN_API_ENABLED=true` enables `/_admin/v1/*` routes (bootstrap key only)

Reference: `auth/README.md`

Additional docs:
- Admin OpenAPI spec: `docs/admin-api-openapi.yaml`
- S3 compatibility matrix: `docs/s3-compatibility.md`

CLI credential/env resolution for `fs admin`:
- Flags: `--access-key`, `--secret-key`, `--endpoint`, `--region`
- Env fallback:
  - `FS_ROOT_USER` / `FS_ROOT_PASSWORD` (same defaults as server bootstrap)
  - `FSCLI_ACCESS_KEY` / `FSCLI_SECRET_KEY`
  - `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
  - `FSCLI_ENDPOINT` (fallback to `ADDRESS` + `PORT`, then `http://localhost:3000`)
  - `FSCLI_REGION` (fallback `FS_AUTH_REGION`, default `us-east-1`)

Note:
- `fs admin snapshot ...` commands operate locally on filesystem paths and do not require endpoint or auth credentials.

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
