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

- `AUTH_ENABLED`
- `AUTH_REGION`
- `AUTH_SKEW_SECONDS`
- `AUTH_MAX_PRESIGN_SECONDS`
- `AUTH_MASTER_KEY`
- `AUTH_BOOTSTRAP_ACCESS_KEY`
- `AUTH_BOOTSTRAP_SECRET_KEY`
- `AUTH_BOOTSTRAP_POLICY` (optional JSON)

Important:
- If `AUTH_ENABLED=true`, `AUTH_MASTER_KEY` is required.
- `AUTH_MASTER_KEY` must be base64 that decodes to exactly 32 bytes (AES-256 key).

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
  - or overridden by `AUTH_BOOTSTRAP_POLICY`

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
  - skew within `AUTH_SKEW_SECONDS`
  - presigned expiry within `AUTH_MAX_PRESIGN_SECONDS`
4. Load identity by access key id.
5. Ensure identity status is active.
6. Decrypt stored secret using master key.
7. Recompute canonical request and expected signature.
8. Compare signatures.
9. Resolve target action from request.
10. Evaluate policy; deny overrides allow.
11. Store auth result in request context and continue.

## Authorization Semantics
Policy evaluator rules:
- No matching allow => denied.
- Any matching deny => denied (even if allow also matches).
- Wildcards supported:
  - action: `*` or `s3:*`
  - bucket: `*`
  - prefix: `*`

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
- Keep `AUTH_MASTER_KEY` protected (secret manager/systemd env file/etc.).

## Current Scope / Limitations
- No STS/session-token auth yet.
- No admin API for managing multiple users yet.
- Policy language is intentionally minimal, not full IAM.
- No automatic key rotation workflows.

## Practical Next Step
To support multiple users cleanly, add admin operations in auth service + API:
- create user
- rotate secret
- set policy
- disable/enable
- delete user
