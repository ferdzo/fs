# Authentication Design

This folder implements S3-compatible request authentication using AWS Signature Version 4 (SigV4), with local identity and policy data stored in bbolt.

## Goals
- Keep S3 client compatibility for request signing.
- Avoid external auth databases.
- Store secrets encrypted at rest (not plaintext in bbolt).
- Keep authorization simple and explicit.

## High-Level Architecture
- `auth/middleware.go`
  - HTTP middleware that enforces auth before API handlers.
  - Exempts `/healthz`.
  - Calls auth service and writes mapped S3 XML errors on failure.
- `auth/service.go`
  - Main auth orchestration:
    - parse SigV4 from request
    - validate timestamp/scope/service/region
    - load identity from metadata
    - decrypt secret
    - verify signature
    - evaluate policy against requested S3 action
- `auth/sigv4.go`
  - Canonical SigV4 parsing and verification helpers.
  - Supports header auth and presigned query auth.
- `auth/policy.go`
  - Authorization evaluator (deny overrides allow).
- `auth/action.go`
  - Maps HTTP method/path/query to logical S3 action + resource target.
- `auth/crypto.go`
  - AES-256-GCM encryption/decryption for stored secret keys.
- `auth/context.go`
  - Carries authentication result in request context for downstream logic.
- `auth/config.go`
  - Normalized auth configuration.
- `auth/errors.go`
  - Domain auth errors used by API S3 error mapping.

## Config Model
Auth is configured through env (read in `utils/config.go`, converted in `auth/config.go`):

- `FS_AUTH_ENABLED`
- `FS_AUTH_REGION`
- `FS_AUTH_CLOCK_SKEW_SECONDS`
- `FS_AUTH_MAX_PRESIGN_SECONDS`
- `FS_MASTER_KEY`
- `FS_ROOT_USER`
- `FS_ROOT_PASSWORD`
- `FS_ROOT_POLICY_JSON` (optional JSON)

Important:
- If `FS_AUTH_ENABLED=true`, `FS_MASTER_KEY` is required.
- `FS_MASTER_KEY` must be base64 that decodes to exactly 32 bytes (AES-256 key).

## Persistence Model (bbolt)
Implemented in metadata layer:
- `__AUTH_IDENTITIES__` bucket stores `models.AuthIdentity`
  - `access_key_id`
  - encrypted secret (`secret_enc`, `secret_nonce`)
  - status (`active`/disabled)
  - timestamps
- `__AUTH_POLICIES__` bucket stores `models.AuthPolicy`
  - `principal`
  - statements (`effect`, `actions`, `bucket`, `prefix`)

## Bootstrap Identity
On startup (`main.go`):
1. Build auth config.
2. Create auth service with metadata store.
3. Call `EnsureBootstrap()`.

If bootstrap env key/secret are set:
- identity is created/updated
- secret is encrypted with AES-GCM and stored
- policy is created:
  - default: full access (`s3:*`, `bucket=*`, `prefix=*`)
  - or overridden by `FS_ROOT_POLICY_JSON`

## Request Authentication Flow
For each non-health request:
1. Parse SigV4 input (header or presigned query).
2. Validate structural fields:
  - algorithm
  - credential scope
  - service must be `s3`
  - region must match config
3. Validate time:
  - `x-amz-date` format
  - skew within `FS_AUTH_CLOCK_SKEW_SECONDS`
  - presigned expiry within `FS_AUTH_MAX_PRESIGN_SECONDS`
4. Load identity by access key id.
5. Ensure identity status is active.
6. Decrypt stored secret using master key.
7. Recompute canonical request and expected signature.
8. Compare signatures.
9. Reject signed streaming payload modes that require per-chunk signature verification.
10. Wrap fixed-size signed payloads so the actual body must match `x-amz-content-sha256`.
11. Resolve target action from request.
12. Evaluate policy; deny overrides allow.
13. Store auth result in request context and continue.

## Authorization Semantics
Policy evaluator rules:
- No matching allow => denied.
- Any matching deny => denied (even if allow also matches).
- Wildcards supported:
  - action: `*` or `s3:*`
  - bucket: `*`
  - prefix: `*`
- Object actions apply `prefix` to the object key.
- `ListBucket` applies `prefix` to the requested list `prefix` query value; a scoped list policy such as `prefix=backups/` does not allow an empty-prefix or sibling-prefix bucket listing.
- Multi-object delete is authorized per object key after the XML body is parsed; denied keys are returned as per-key `AccessDenied` errors and are not deleted.

Action resolution includes:
- bucket APIs (`CreateBucket`, `ListBucket`, `HeadBucket`, `DeleteBucket`)
- object APIs (`GetObject`, `PutObject`, `DeleteObject`)
- multipart APIs (`CreateMultipartUpload`, `UploadPart`, `ListMultipartUploadParts`, `CompleteMultipartUpload`, `AbortMultipartUpload`)

## Error Behavior
Auth errors are mapped to S3-style XML errors in `api/s3_errors.go`, including:
- `AccessDenied`
- `InvalidAccessKeyId`
- `SignatureDoesNotMatch`
- `AuthorizationHeaderMalformed`
- `RequestTimeTooSkewed`
- `ExpiredToken`
- `AuthorizationQueryParametersError`

## Audit Logging
When `AUDIT_LOG=true` and auth is enabled:
- successful auth attempts emit `auth_success`
- failed auth attempts emit `auth_failed`

Each audit entry includes method, path, remote IP, and request ID (if present). Success logs also include access key ID and auth type.

## Security Notes
- Secret keys are recoverable by server design (required for SigV4 verification).
- They are encrypted at rest, not hashed.
- Master key rotation is not implemented yet.
- Keep `FS_MASTER_KEY` protected (secret manager/systemd env file/etc.).

## Current Scope / Limitations
- No STS/session-token auth yet.
- Signed aws-chunked streaming payloads are not accepted until per-chunk signature verification is implemented. Unsigned streaming payload modes can still be decoded by the API layer.
- Policy language is intentionally minimal, not full IAM.
- No automatic key rotation workflows.
- No key rotation endpoint for existing users yet.
