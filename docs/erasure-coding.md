# Reed-Solomon Erasure Coding & Disaster Recovery

`fs` optionally stores chunk payloads across `K + M` local directories (typically separate disks or mount points) using `K` data shards and `M` parity shards via [`github.com/klauspost/reedsolomon`](https://github.com/klauspost/reedsolomon).

## What It Protects Against (and What It Does Not)

- **Survives loss or corruption of up to `M` disks/shards per chunk** with storage overhead `(K + M) / K` (e.g. `4+2` = 1.5x space, survives any 2 failed drives).
- **Detects silent bit-rot** on every shard read using a per-shard SHA-256 checksum inside each shard header, plus full-chunk SHA-256 verification against the content-addressed `chunkID`.
- **Does NOT replicate `metadata.db`**: bucket indexes, object manifests, version history, and credentials still live in `DATA_PATH/metadata.db` (bbolt). Back up `metadata.db` separately (e.g., `fs admin snapshot create`) or keep it on RAID-1. Losing `> M` shards for a chunk makes that chunk unrecoverable.

---

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `FS_STORAGE_BACKEND` | `disk` | Set to `erasure` to enable Reed-Solomon (`disk` keeps single-path `DATA_PATH/blobs`). |
| `FS_ERASURE_PATHS` | *(empty)* | Comma-separated list of `K + M` distinct root directories (slot `0 .. K+M-1` in listed order). |
| `FS_ERASURE_DATA_SHARDS` | `4` | `K` (`1..254`). Shards `0..K-1` store raw split data. |
| `FS_ERASURE_PARITY_SHARDS` | `2` | `M` (`1..254`, `K + M <= 255`). Shards `K..K+M-1` store RS parity. |
| `FS_ERASURE_AUTO_HEAL` | `true` | Automatically rewrite missing/corrupt shards during degraded reads. |
| `FS_ERASURE_STRICT_WRITE` | `false` | `false`: allow degraded writes & degraded startup as long as `>= K` disks succeed. `true`: require all `K + M` disks online. |

Example `4+2` `.env`:
```ini
DATA_PATH=/var/lib/fs-meta
CHUNK_SIZE=4194304
FS_STORAGE_BACKEND=erasure
FS_ERASURE_DATA_SHARDS=4
FS_ERASURE_PARITY_SHARDS=2
FS_ERASURE_PATHS=/mnt/d0,/mnt/d1,/mnt/d2,/mnt/d3,/mnt/d4,/mnt/d5
```

---

## How It Works

### Slot Binding (`.fs-erasure-slot.json`)
On initialization, each accessible root writes `<root>/.fs-erasure-slot.json`:
```json
{
  "version": 1,
  "slot": 0,
  "dataShards": 4,
  "parityShards": 2
}
```
If an existing root has a mismatched `slot`, `dataShards`, or `parityShards`, **startup fails immediately** to prevent silent disk-order swaps or geometry mismatches. Up to `M` roots may be missing at startup when `FS_ERASURE_STRICT_WRITE=false`.

### Write Path (`IngestStreamWithSizes`)
1. Incoming streams are sliced at `CHUNK_SIZE`, hashed for ETag (MD5) and `chunkID = hex(SHA-256(chunk))`, and pinned against GC via `onChunk(chunkID)`.
2. **Dedup check**: if all `K + M` shard files already exist, encoding is skipped. Missing shards are rewritten on re-upload, allowing re-uploading identical bytes to heal degraded chunks.
3. **Encode & atomic write**: plaintext is split into `K` equal shards + `M` parity shards, wrapped in 64-byte self-describing headers, and written concurrently across slots (`CreateTemp` -> `Write` -> `Sync` -> `Close` -> `Rename` -> `fsync(dir)`). Writes fail if `< K` shards succeed (or `< K + M` when `FS_ERASURE_STRICT_WRITE=true`).

### Read & Auto-Heal Path (`GetBlob` / `AssembleStream`)
1. **Healthy fast path**: reads only data shards `0..K-1` in parallel, verifies each 64-byte header and per-shard SHA-256, concatenates `OriginalChunkSize` bytes, and verifies `SHA-256(plaintext) == chunkID`. Parity disks are not read when healthy.
2. **Degraded reconstruction**: if any data shard is missing or corrupt, reads parity shards `K..K+M-1`. Any `>= K` valid shards with matching `(OriginalChunkSize, ShardPayloadSize)` reconstruct the missing shards and verify `SHA-256(plaintext) == chunkID`.
3. **Auto-heal**: when `FS_ERASURE_AUTO_HEAL=true`, reconstructed missing/corrupt shards are atomically written back to their slot roots (best-effort so a read still succeeds if a dead disk rejects writes).

### Preflight & Garbage Collection
- `StatChunk(chunkID)` returns `(newestModTime, presentShards >= K)`, so `GET`/`HEAD`/range reads preflight cleanly even with `M` failed disks and return `ErrChunkMissing` once `M+1` are gone.
- `DeleteBlob(chunkID)` deletes shard `i` from all `K + M` roots.

---

## On-Disk Binary Format (Zero-Guesswork Recovery Reference)

Everything needed to reconstruct a chunk without server config or `metadata.db` is stored inside every shard file.

- **Shard path**: `<root>/blobs/<chunkID[0:2]>/<chunkID[2:4]>/<chunkID>.s<idx:03d>` (e.g. `.../blobs/ab/cd/<64-hex>.s004`).
- **Reed-Solomon math**: `klauspost/reedsolomon.New(K, M)` over `GF(2^8)` (Vandermonde coding matrix, primitive polynomial `0x11d`).
  - If `OriginalChunkSize == 0`: all `K + M` shard payloads are empty (`ShardPayloadSize = 0`).
  - Otherwise: `ShardPayloadSize = ceil(OriginalChunkSize / K)`. Data is split sequentially across shards `0..K-1`; shard `K-1` is zero-padded at the end up to `ShardPayloadSize`. After reconstructing shards `0..K-1`, concatenate them and truncate to `OriginalChunkSize`. Verify `hex(SHA-256(plaintext)) == chunkID`.

### 64-Byte Shard Header (Big-Endian)

| Offset | Size | Field | Meaning |
| --- | --- | --- | --- |
| `0` | `4` | `Magic` | ASCII `FSER` (`0x46 0x53 0x45 0x52`) |
| `4` | `1` | `Version` | Format version: `1` |
| `5` | `1` | `Algorithm` | `1` = Reed-Solomon Vandermonde `GF(2^8)` |
| `6` | `1` | `DataShards` | `K` (`1..254`) |
| `7` | `1` | `ParityShards` | `M` (`1..254`) |
| `8` | `1` | `ShardIndex` | `0..K+M-1` (`0..K-1` data, `K..K+M-1` parity) |
| `9` | `3` | `Reserved` | Zeroes (`0x00 0x00 0x00`) |
| `12` | `4` | `ShardPayloadSize` | `uint32` byte length of payload immediately following header |
| `16` | `8` | `OriginalChunkSize` | `uint64` uncompressed chunk length in bytes |
| `24` | `32` | `ShardPayloadSHA256` | Raw SHA-256 digest of bytes `[64 .. 64+ShardPayloadSize]` |
| `56` | `8` | `Reserved` | Zeroes |
| `64` | `N` | `Payload` | `ShardPayloadSize` raw RS shard bytes |

---

## Failure & Recovery Runbook

All `fs admin erasure ...` commands work offline against disk paths (and read-only against `metadata.db`).

### 1. Check Cluster Shard Health (Offline Scrub)
```bash
fs admin erasure status \
  --paths /mnt/d0,/mnt/d1,/mnt/d2,/mnt/d3,/mnt/d4,/mnt/d5 \
  --data-shards 4 --parity-shards 2 \
  --metadata-db /var/lib/fs-meta/metadata.db
```
Returns non-zero if any chunk is unrecoverable or missing.

### 2. Replace 1..M Failed Disks and Rebuild Parity/Data
Stop writes (recommended) or mount the blank replacement disk(s) at the exact slot path(s) (e.g. `/mnt/d2`), then heal:
```bash
fs admin erasure heal \
  --paths /mnt/d0,/mnt/d1,/mnt/d2,/mnt/d3,/mnt/d4,/mnt/d5 \
  --data-shards 4 --parity-shards 2 \
  --metadata-db /var/lib/fs-meta/metadata.db
```
`NewErasureStore` initializes `.fs-erasure-slot.json` on the fresh disk and reconstructs every missing/corrupt shard onto it. Re-run `fs admin erasure status ...` to confirm `degraded=0 unrecoverable=0 missing=0`.

### 3. Extract Data Directly During an Outage (Order-Independent)
Even if mounts are in the wrong order, `M` disks are completely gone, or you copied surviving `*.sNNN` files into a single `/rescue/shards` directory, `recover` auto-detects `K`, `M`, shard index, and chunk size from the 64-byte headers:

- **Recover a full S3 object** using a read-only copy of `metadata.db`:
```bash
fs admin erasure recover \
  --paths /mnt/d4,/mnt/d0,/mnt/d5,/mnt/d2 \
  --metadata-db /backup/metadata.db \
  --bucket my-bucket --key path/to/file.bin \
  --out /tmp/file.bin
```
- **Recover a single chunk** without `metadata.db`:
```bash
fs admin erasure recover \
  --paths /rescue/shards \
  --chunk <64-hex-chunk-id> \
  --out /tmp/chunk.bin
```

### 4. Severe Failures
- **> M disks lost for a chunk**: Reed-Solomon cannot reconstruct with fewer than `K` valid shards. `status` exits non-zero listing unrecoverable chunk IDs; restore affected objects from external backups. Re-uploading the identical object repopulates missing shards if no overwrite occurred, or overwrites cleanly if versioning/replacing.
- **`metadata.db` lost, >= K shard disks intact**: restore `metadata.db` from `fs admin snapshot` (or backup) and run `fs admin erasure status --metadata-db ...` followed by `heal`. If no `metadata.db` backup exists, every unique chunk can still be decoded offline via `fs admin erasure recover --paths ... --chunk <id>`, though bucket/key names and multi-chunk object order require the manifest.
