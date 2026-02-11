# Zeppelin

[![CI](https://github.com/Ghatage/zeppelin/actions/workflows/ci.yml/badge.svg)](https://github.com/Ghatage/zeppelin/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Ghatage/zeppelin/branch/main/graph/badge.svg)](https://codecov.io/gh/Ghatage/zeppelin)

S3-native vector search engine. A self-hostable alternative to [turbopuffer](https://turbopuffer.com).

Object storage is the source of truth. Nodes are stateless — kill one, the only cost is cache re-warming. IVF-Flat indexing gives exactly **2 sequential S3 roundtrips per query**.

## Why?

Every existing open-source vector DB (Qdrant, Milvus, Weaviate) treats object storage as a backup tier. Their indexes (HNSW graphs) require low-latency random reads that object storage can't provide.

Zeppelin uses **IVF-Flat indexing** — probe centroids, then sequentially scan the top clusters — making S3 a first-class storage engine rather than an afterthought.

## Architecture

```
HTTP API (axum)
  │
  ├── WAL Writer ──── append-only fragments (bincode + xxHash)
  ├── Query Engine ── IVF centroid probe → cluster scan
  └── Namespace Mgr ─ CRUD, metadata
          │
      Cache Manager ── LRU, pinned centroids
          │
      Storage Layer ── object_store crate
          │
    ┌─────┼─────┐
    S3   GCS  Azure
   MinIO       Blob
    R2
```

**Key invariants:**
- WAL fragments and segments are immutable once written
- Manifest on S3 is the single source of truth
- Local cache is disposable — safe to blow away at any time
- Single writer per namespace, no distributed coordination needed

## S3 Layout

```
s3://{bucket}/{namespace}/
  meta.json              # dimensions, distance metric, vector count
  manifest.json          # fragment refs, segment refs, compaction watermark
  wal/
    {ulid}.wal           # bincode-serialized vectors + deletes
  segments/
    {segment_id}/
      centroids.bin      # IVF centroids
      cluster_{i}.bin    # vectors assigned to cluster i
      attrs_{i}.bin      # attributes for filtering
```

## API

```
# Vectors
POST   /v1/namespaces/{ns}/vectors    # upsert
DELETE /v1/namespaces/{ns}/vectors    # delete by IDs

# Search
POST   /v1/namespaces/{ns}/query      # vector search with filters

# Namespaces
GET    /v1/namespaces                  # list (supports ?prefix=)
GET    /v1/namespaces/{ns}             # get metadata
DELETE /v1/namespaces/{ns}             # delete namespace + all data
POST   /v1/namespaces/{ns}/warm        # async cache warming

# Operations
GET    /healthz                        # health check
GET    /metrics                        # prometheus format
```

## Quickstart

### Prerequisites

- Rust 1.84+
- An S3-compatible bucket (AWS S3, MinIO, R2, GCS)

### Docker quickstart

```bash
# Start Zeppelin + MinIO (local dev)
docker compose up -d

# Verify it's running
curl http://localhost:8080/healthz

# For production S3 — override env vars, skip MinIO
S3_ENDPOINT= AWS_ACCESS_KEY_ID=AKIA... AWS_SECRET_ACCESS_KEY=... S3_BUCKET=my-bucket \
  docker compose up zeppelin
```

### Run from source

```bash
cp .env.example .env
# Fill in AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET
cargo run
```

### Python client

```bash
pip install ./python
```

```python
from zeppelin import ZeppelinClient

with ZeppelinClient() as client:
    client.create_namespace("demo", dimensions=4)
    client.upsert_vectors("demo", [{"id": "v1", "values": [1, 0, 0, 0]}])
    result = client.query("demo", [1, 0, 0, 0], top_k=5)
    for r in result.results:
        print(f"{r.id}: {r.score:.4f}")
    client.delete_namespace("demo")
```

See [`python/README.md`](python/README.md) for full API docs.

### Run tests

```bash
# Against real S3 (default)
cargo test

# Against MinIO
TEST_BACKEND=minio cargo test
```

### View code coverage

```bash
cargo install cargo-llvm-cov
cargo llvm-cov --html --lib --tests
open target/llvm-cov/html/index.html
```

## Configuration

Zeppelin reads from `zeppelin.toml` with environment variable overrides:

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| `storage.backend` | `STORAGE_BACKEND` | `s3` | `s3`, `gcs`, `azure`, `local` |
| `storage.bucket` | `S3_BUCKET` | `zeppelin` | Bucket name |
| `storage.s3_region` | `AWS_REGION` | `us-east-1` | AWS region |
| `storage.s3_endpoint` | `S3_ENDPOINT` | *(auto)* | Custom endpoint for MinIO/R2 |
| `cache.dir` | — | `/var/cache/zeppelin` | Local cache directory |
| `cache.max_size_gb` | — | `50` | Max cache size |
| `indexing.default_nprobe` | — | `16` | Clusters to probe per query |
| `compaction.interval_secs` | — | `30` | Compaction check interval |

See `.env.example` for the full list.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Storage abstraction | `object_store` crate | Multi-cloud from day 1. Battle-tested (Apache Arrow ecosystem). |
| Index | IVF-Flat | S3-friendly: 2 sequential roundtrips. Pluggable trait for future IVF-PQ, ScaNN. |
| WAL | ULID fragments + JSON manifest | Simple single-writer. No conditional writes needed. |
| Filtering | Post-filter + oversampling | Correct for v1. Filter-aware routing deferred. |
| Cache | LRU + pinned centroids | Simple, correct. Per-namespace budgets deferred. |
| Coordination | None — S3 consistency | No ZooKeeper/etcd. S3 read-after-write handles it. |
| Serialization | bincode (WAL) + JSON (manifests) | bincode for speed on hot path, JSON for debuggability. |

## License

Apache-2.0
