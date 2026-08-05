<p align="center">
  <img src="assets/icon.svg" alt="Zeppelin" width="128" height="128">
</p>
<h1 align="center">Zeppelin</h1>
<p align="center">
  <a href="https://www.rust-lang.org"><img src="https://img.shields.io/badge/Rust-1.85+-DEA584?logo=rust&logoColor=white" alt="Rust: 1.85+"></a>
  <a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/License-Apache--2.0-blue.svg" alt="License: Apache-2.0"></a>
  <a href="https://github.com/zepdb/zeppelin/actions/workflows/ci.yml"><img src="https://github.com/zepdb/zeppelin/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://codecov.io/gh/zepdb/zeppelin"><img src="https://codecov.io/gh/zepdb/zeppelin/branch/main/graph/badge.svg" alt="codecov"></a>
</p>
<p align="center">
  An S3-native vector search engine. Object storage is the source of truth. Nodes are stateless.
</p>

---

## Features

- **S3-native** -- Object storage is the single source of truth
- **Stateless nodes** -- Any node can serve any query
- **IVF indexing** -- IVF-Flat, IVF-SQ8 (4x compression), IVF-PQ (16-32x), and Hierarchical ANN
- **BM25 full-text search** -- Inverted indexes with configurable tokenization, stemming, and multi-field `rank_by` expressions (opt-in, see below)
- **Bitmap pre-filters** -- RoaringBitmap indexes for sub-millisecond attribute filtering
- **Write-ahead log** -- Durable writes with compaction into indexed segments
- **Namespace forks** -- Disabled-by-default, licensed copy-on-write branching ([operator guide](docs/branching-operations.md)); fork only, no merge
- **Strong & eventual consistency** -- Choose per-query (see [Consistency semantics](#consistency-semantics))
- **Object storage** -- S3, MinIO, and S3-compatible backends. GCS and Azure planned

## Performance

Measured on the production default query path — TwoBit (2-bit Extended-RaBitQ)
coarse scoring with exact f32 rerank and the scale-aware `nprobe` policy — over
dbpedia-100k (100,000 x 1,536-dim cosine vectors; 256 clusters, so an omitted
`nprobe` resolves to 48), strong consistency, no filter, single warm node
against loopback MinIO (Apple M3 Max, rustc 1.93, release build):

| Warm repeat query (n=200) | `top_k = 10` | `top_k = 100` |
| --- | --- | --- |
| mean / p50 | 8.0 ms / 8.0 ms | 11.5 ms / 11.0 ms |
| p95 / p99 | 9.3 ms / 9.7 ms | 15.8 ms / -- |
| recall vs exact ground truth | 0.989 @10 | 0.982 @100 |

Every warm query stays honest to the S3-native design: one conditional
manifest GET (strong consistency re-verifies the authoritative manifest) plus
~37 parallel segment range GETs (~16 MB) per query — nothing above is served
from index state that bypasses object storage. On byte-dominated deployments
(local disk / MinIO), setting `query.cost_latency_profile = "low_latency"`
trades more range requests for fewer bytes and cuts warm mean latency a
further ~7%; the default profile stays request-optimized for real S3 pricing.

These are loopback-MinIO measurements, not cloud-S3 latency claims: real S3
adds its per-request round-trip floors to the manifest, coarse, and rerank
waves. Methodology, per-slice attribution, and bit-exactness evidence for the
2026-08 optimization pass (-30% warm latency at identical results) are in
[`tasks/optimization_2bit_results/summary.md`](tasks/optimization_2bit_results/summary.md).

## Quick Start

Spin up Zeppelin with MinIO locally using Docker Compose:

```bash
docker compose up
```

This starts Zeppelin on port `8080` and MinIO on port `9000` with a pre-created `zeppelin` bucket.

### Create a namespace

```bash
# Server generates a UUID name — save it!
curl -s http://localhost:8080/v1/namespaces \
  -H "Content-Type: application/json" \
  -d '{"dimensions": 384}' | jq
# Response: {"name": "550e8400-e29b-41d4-a716-446655440000", "dimensions": 384, ..., "warning": "Save this namespace name..."}
```

### Upsert vectors

```bash
# Replace <ns> with the UUID from the create response
curl -s http://localhost:8080/v1/namespaces/<ns>/vectors \
  -H "Content-Type: application/json" \
  -d '{
    "vectors": [
      {"id": "vec-1", "values": [0.1, 0.2, 0.3, "...384 dims..."]},
      {"id": "vec-2", "values": [0.4, 0.5, 0.6, "...384 dims..."]}
    ]
  }' | jq
```

### Query

```bash
curl -s http://localhost:8080/v1/namespaces/<ns>/query \
  -H "Content-Type: application/json" \
  -d '{
    "vector": [0.1, 0.2, 0.3, "...384 dims..."],
    "top_k": 10
  }' | jq
```

For a `late_interaction_fde` namespace with an active embedding profile, send
query text through the retrieval algebra:

```bash
curl -s http://localhost:8080/v1/namespaces/<ns>/query \
  -H "Content-Type: application/json" \
  -d '{
    "sources": [{
      "type": "late_interaction",
      "text": "find the section about storage ownership",
      "top_k": 100,
      "semantic_wait_ms": 5000
    }],
    "consistency": "strong",
    "top_k": 10
  }' | jq
```

Strong queries wait for every visible live record to have an exact semantic
overlay. If the budget expires, Zeppelin returns `SEMANTIC_INDEX_LAG` with
`requested_generation`, `covered_sequence`, `pending_records`, and
`failed_records`. Eventual queries score only covered live versions and
suppress newer pending versions and tombstones. Their response reports
`"semantic_coverage": "complete"` when every live version was covered and
`"semantic_coverage": "partial"` when any live version was omitted.
Late-interaction sources can participate in RRF fusion; weighted fusion is
rejected because raw MaxSim is not calibrated against dense or BM25 scores.

### Delete vectors

```bash
curl -s -X DELETE http://localhost:8080/v1/namespaces/<ns>/vectors \
  -H "Content-Type: application/json" \
  -d '{"ids": ["vec-1"]}' | jq
```

## API Reference

The canonical definition is the [OpenAPI 3.1 spec](api/zeppelin-api.yaml). The
tables below are a complete index of the served routes.

### Operational

| Method   | Path                | Description                                   |
|----------|---------------------|-----------------------------------------------|
| `GET`    | `/healthz`          | Liveness probe                                |
| `GET`    | `/readyz`           | Readiness probe                               |
| `GET`    | `/metrics`          | Prometheus metrics                            |
| `GET`    | `/debug/pprof/cpu`  | CPU profile — `profiling` build feature only  |

### Namespaces

| Method            | Path                                   | Description                                      |
|-------------------|----------------------------------------|--------------------------------------------------|
| `POST`            | `/v1/namespaces`                       | Create a namespace (returns UUID)                |
| `GET`             | `/v1/namespaces/:ns`                   | Get namespace metadata                           |
| `DELETE`          | `/v1/namespaces/:ns`                   | Delete a namespace                               |
| `POST`            | `/v1/namespaces/:ns/clone`             | Create an independent copy clone                 |
| `GET/POST`        | `/v1/namespaces/:ns/branches`          | List/create direct branches — registered only when branching is enabled |
| `PATCH`           | `/v1/namespaces/:ns/index_config`      | Update index configuration                       |
| `POST`            | `/v1/namespaces/:ns/compact`           | Trigger compaction                               |
| `GET`             | `/v1/namespaces/:ns/compact/status`    | Compaction status                                |
| `POST`            | `/v1/namespaces/:ns/hydrate`           | Trigger cache hydration                          |
| `GET`             | `/v1/namespaces/:ns/snapshots`         | List snapshots                                   |
| `GET/PUT/DELETE`  | `/v1/namespaces/:ns/snapshots/:name`   | Read, create, or delete one named snapshot       |

### Data

| Method   | Path                                | Description             |
|----------|-------------------------------------|-------------------------|
| `POST`   | `/v1/namespaces/:ns/vectors`        | Upsert vectors          |
| `DELETE` | `/v1/namespaces/:ns/vectors`        | Delete vectors          |
| `POST`   | `/v1/namespaces/:ns/vectors/get`    | Fetch vectors by ID     |
| `POST`   | `/v1/namespaces/:ns/query`          | Query nearest neighbors |
| `POST`   | `/v1/namespaces/:ns/query/batch`    | Batch query             |

### Runtime configuration

| Method            | Path                | Description                              |
|-------------------|---------------------|------------------------------------------|
| `GET`             | `/v1/config/query`  | Read live query configuration            |
| `PATCH`/`PUT`     | `/v1/config/query`  | Update live query configuration          |

### Security

These routes are always registered, but each is gated by a licensed feature and
returns a not-licensed error without it. See the
[security deployment guide](docs/security-deployment.md).

| Method             | Path                                            | Entitlement  |
|--------------------|-------------------------------------------------|--------------|
| `GET/POST`         | `/v1/security/principals`                       | RBAC         |
| `GET/POST`         | `/v1/security/keys`                             | RBAC         |
| `DELETE`           | `/v1/security/keys/:key_id`                     | RBAC         |
| `POST`             | `/v1/security/keys/:key_id/rotate`              | RBAC         |
| `GET/POST/DELETE`  | `/v1/security/grants`                           | RBAC         |
| `GET`              | `/v1/security/policy`                           | RBAC         |
| `POST`             | `/v1/security/tokens`                           | Delegation   |
| `GET/POST`         | `/v1/security/preservation`                     | Preservation |
| `POST`             | `/v1/security/preservation/:lock_id/release`    | Preservation |

## API Clients

Use the HTTP API directly or generate a client from the canonical [OpenAPI 3.1 spec](api/zeppelin-api.yaml). This repository does not maintain generated client SDK packages.

## Development

### Prerequisites

- [Rust 1.85+](https://www.rust-lang.org/tools/install)
- [Docker](https://docs.docker.com/get-docker/) (for MinIO in tests)

### Build

```bash
cargo build
```

### Run tests

Run the default in-memory test suite:

```bash
cargo test
```

For the MinIO-backed integration pass:

```bash
docker compose -f docker-compose.test.yml up -d
TEST_BACKEND=minio \
TEST_S3_BUCKET=stormcrow-test \
MINIO_ENDPOINT=http://localhost:9000 \
MINIO_ACCESS_KEY=minioadmin \
MINIO_SECRET_KEY=minioadmin \
cargo test --tests
```

### Run locally against MinIO

```bash
# Start MinIO
docker compose -f docker-compose.test.yml up -d

# Run Zeppelin against local MinIO
STORAGE_BACKEND=s3 \
S3_BUCKET=stormcrow-test \
S3_ENDPOINT=http://localhost:9000 \
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
AWS_REGION=us-east-1 \
S3_ALLOW_HTTP=true \
cargo run
```

### Lint and format

```bash
cargo fmt --all -- --check
cargo clippy -- -D warnings
```

## Architecture

```
src/
  storage/     Object store abstraction (S3, S3-compatible)
  wal/         Write-ahead log: fragments, manifest, reader/writer
  namespace/   Namespace CRUD, metadata, and the branch graph
  index/       Vector indexing (IVF-Flat with k-means, quantization)
  fts/         BM25 lexical retrieval: tokenizer, inverted indexes
  cache/       Local disk and memory cache with LRU eviction
  compaction/  Background WAL-to-segment compaction
  security/    Authorization kernel, policy, entitlements, audit
  server/      Axum HTTP handlers, routes, middleware
```

Writes land in the WAL as immutable fragments. Background compaction merges fragments into indexed segments (IVF, bitmap pre-filters, BM25 inverted indexes). Queries probe the closest centroids and merge results from any un-compacted WAL fragments.

Flat IVF segments scale their centroid count with segment size: one centroid
per 3,000 logical rows, bounded by a 256-centroid floor and a 4,096-centroid
resident-memory cap. An omitted flat `nprobe` searches 3/16 of the active
segment's clusters with a runtime-configurable floor of 32. Each logical row
is stored in exactly one cluster. The ignored `ivf_recall_gate` integration
test is the binding recall, scan, storage, full-probe, and determinism check for
changes to this policy.

### Consistency semantics

Consistency is selected per-query via the `consistency` field:

- **`strong`** scans un-compacted WAL fragments in addition to indexed
  segments, so it always reflects writes committed to S3 — with one caveat:
  each node caches the namespace manifest for up to 500 ms. A write through
  node A is immediately visible to strong queries on node A (write-through
  cache), but a strong query on node B may miss it for up to the cache TTL.
  In other words: **same-node read-your-writes; cross-node bounded staleness
  (≤ 500 ms)**. Single-node deployments get true strong consistency. If you
  need cross-node read-your-writes, sticky-route each client to one node.
- **`eventual`** reads indexed segments and applies delete tombstones from
  un-compacted WAL fragments, but skips WAL vector/BM25 scoring. Deletes are
  hidden immediately on the same node after the delete returns. Recent upserts
  and updates can still be stale until the next compaction cycle, so use it
  when write freshness within one compaction interval does not matter.

### Multi-node coordination

Writes are safe under concurrency without any coordinator: every manifest
commit is an ETag-guarded compare-and-swap, so concurrent upserts from
multiple nodes serialize correctly. Background compaction additionally
acquires a per-namespace lease (`compaction.lease_duration_secs`, default
300 s) so only one node compacts a namespace at a time; a fencing token +
CAS make even an expired-lease holder unable to commit stale state.

### Full-text search (opt-in)

BM25 `rank_by` queries work out of the box against un-compacted WAL data.
For segment data, per-cluster and global inverted indexes are built during
compaction only when `indexing.fts_index = true` (or `ZEPPELIN_FTS_INDEX=true`)
— it is **off by default** because it adds compaction cost for namespaces
that never use FTS. Without it, segment BM25 falls back to a full scan,
which is rejected above `bm25_max_full_scan_clusters` (default 500) to
protect latency. Enable `fts_index` if you use `rank_by` at scale.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

Copyright 2026 Anup Ghatage. See [NOTICE](NOTICE) — redistributions and
derivative works must retain the attribution notices it contains, per
Section 4(d) of the license.
