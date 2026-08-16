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

## Why Zeppelin

Most vector databases replicate your data across SSD-backed nodes and keep
indexes memory-resident — you pay for that fleet whether you query it or
not. Zeppelin stores everything (vectors, indexes, WAL, manifest) in object
storage and keeps nodes stateless. A warm dense query is one conditional
manifest GET plus two bounded waves of parallel range reads — a quantized
coarse scan, then an exact rerank — so compute scales with query volume,
storage scales at object-store prices, and any node, including one that
just booted, can serve any namespace. There is no rebalancing, no replica
fleet, and nothing to lose when a node dies. The design target is recall
parity with memory-resident engines at structurally lower cost, not peak
QPS.

## Features

- **S3-native** -- Object storage is the single source of truth
- **Stateless nodes** -- Any node can serve any query
- **IVF indexing** -- Scale-aware IVF-Flat partitioning and Hierarchical ANN
- **Quantization** -- 2-bit Extended-RaBitQ with exact f32 rerank (production default), SQ8 (4x), PQ (16-32x), and f16 storage
- **BM25 full-text search** -- Inverted indexes with configurable tokenization, stemming, and multi-field `rank_by` expressions (opt-in, see below)
- **Late-interaction retrieval** -- Multi-vector MaxSim with asynchronous semantic enrichment and RRF fusion
- **Bitmap pre-filters** -- RoaringBitmap indexes for sub-millisecond attribute filtering
- **Write-ahead log** -- Durable writes with compaction into indexed segments
- **Namespace forks** -- Disabled-by-default copy-on-write branching; fork only, no merge
- **Strong & eventual consistency** -- Choose per-query (see [Consistency semantics](#consistency-semantics))
- **Security suite** -- Fail-closed authorization kernel, RBAC, durable audit log, delegation tokens, and preservation holds
- **Object storage** -- S3, MinIO, and S3-compatible backends; GCS and Azure Blob, selected by `[storage] backend`
- **Sizing advisor** -- Ranked hardware recommendations and validated, tuned configs from an embedded cloud pricing catalog (see [Sizing advisor](#sizing-advisor))

## Status

Zeppelin is pre-1.0 software under active development:

- **No on-disk format stability guarantee yet.** Stored artifacts may
  change between pre-1.0 versions without migration tooling. Artifacts are
  immutable within a version.
- **Namespace branching is disabled by default** (`branching.enabled`), and
  its release validation gates are not yet complete.
- **GCS and Azure Blob backends are implemented with emulator-backed test
  gates** (patched fake-gcs-server, Azurite — see `scripts/emulators/`) —
  no gate has run against real GCS or Azure yet, a deliberate emulators-only
  decision, and per-substrate performance is unmeasured. S3 and
  S3-compatible stores remain the battle-tested substrates.
- Published performance numbers are loopback-MinIO measurements, not
  cloud-S3 latency claims — see [Performance](#performance) for exactly
  what was measured.

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

(p99 for the `top_k = 100` cell was not recorded in the measurement ledger.)

Every warm query stays honest to the S3-native design: one conditional
manifest GET (strong consistency re-verifies the authoritative manifest) plus
~37 parallel segment range GETs (~16 MB) per query — nothing above is served
from index state that bypasses object storage. On byte-dominated deployments
(local disk / MinIO), setting `query.cost_latency_profile = "low_latency"`
trades more range requests for fewer bytes and cuts warm mean latency a
further ~7%; the default profile stays request-optimized for real S3 pricing.

Late-interaction (`late_interaction_fde`) queries, measured on the same
host: warm truth-wave p50 of 59 ms and end-to-end p50 of 102 ms on a
50k-unit heavy-tail corpus (1,109 queries, K = 1000, f16 token matrices);
~42 ms truth wave on SciFact (5,183 docs). The 2026-08 optimization ladder
cut the 50k truth wave 7x (417 ms to 59 ms) via streamed range reads, f16
decode acceleration, and parallel scoped scoring.

These are loopback-MinIO measurements, not cloud-S3 latency claims: real S3
adds its per-request round-trip floors to the manifest, coarse, and rerank
waves.

## Quick Start

Spin up Zeppelin with MinIO locally using Docker Compose:

```bash
docker compose up
```

This starts Zeppelin on port `8080` and MinIO on port `9000` with a pre-created `zeppelin` bucket. The bundled [`zeppelin.dev.toml`](zeppelin.dev.toml) boots the server in `open_unsafe` security mode (no authentication) — local development only; set `security.mode = "enforced"` (see the `[security]` section of [`zeppelin.toml.example`](zeppelin.toml.example)) before deploying anywhere real.

Every example below is copy-pasteable end-to-end (4-dimensional vectors
keep them short — real embeddings just have more numbers).

### Create a namespace

```bash
# Server generates a UUID name — save it!
NS=$(curl -s http://localhost:8080/v1/namespaces \
  -H "Content-Type: application/json" \
  -d '{"dimensions": 4, "full_text_search": {"content": {}}}' | jq -r .name)
echo "$NS"
```

The `full_text_search` block enables BM25 over the `content` attribute; omit
it if you only need vector search.

### Upsert vectors

```bash
curl -s http://localhost:8080/v1/namespaces/$NS/vectors \
  -H "Content-Type: application/json" \
  -d '{
    "vectors": [
      {"id": "vec-1", "values": [0.10, 0.20, 0.30, 0.40],
       "attributes": {"genre": "systems", "year": 2026,
                      "content": "object storage is the source of truth"}},
      {"id": "vec-2", "values": [0.40, 0.30, 0.20, 0.10],
       "attributes": {"genre": "databases", "year": 2025,
                      "content": "stateless nodes serve any namespace"}},
      {"id": "vec-3", "values": [0.11, 0.21, 0.31, 0.41],
       "attributes": {"genre": "systems", "year": 2024,
                      "content": "the write-ahead log compacts into segments"}}
    ]
  }' | jq
```

### Query

```bash
curl -s http://localhost:8080/v1/namespaces/$NS/query \
  -H "Content-Type: application/json" \
  -d '{"vector": [0.1, 0.2, 0.3, 0.4], "top_k": 2}' | jq
```

```json
{
  "results": [
    {"id": "vec-1", "score": 0.0,
     "attributes": {"genre": "systems", "year": 2026,
                    "content": "object storage is the source of truth"}},
    {"id": "vec-3", "score": 0.000104129314,
     "attributes": {"genre": "systems", "year": 2024,
                    "content": "the write-ahead log compacts into segments"}}
  ],
  "scanned_fragments": 1,
  "scanned_segments": 0
}
```

Scores are distances for vector search (lower is better) and relevance for
BM25 (higher is better).

### Query with an attribute filter

```bash
curl -s http://localhost:8080/v1/namespaces/$NS/query \
  -H "Content-Type: application/json" \
  -d '{
    "vector": [0.1, 0.2, 0.3, 0.4],
    "top_k": 10,
    "filter": {"op": "eq", "field": "genre", "value": "systems"}
  }' | jq
```

Filters compose with `and`, `or`, `not`, `in`, range, and `contains`
operators — see the [OpenAPI spec](api/zeppelin-api.yaml) for the full
grammar.

### BM25 full-text query

```bash
curl -s http://localhost:8080/v1/namespaces/$NS/query \
  -H "Content-Type: application/json" \
  -d '{"rank_by": ["content", "BM25", "storage truth"], "top_k": 2}' | jq
```

For multi-vector late-interaction retrieval (query text scored with MaxSim
against token matrices), see the late-interaction `rank_by` forms in the
[OpenAPI spec](api/zeppelin-api.yaml).

### Delete vectors

```bash
curl -s -X DELETE http://localhost:8080/v1/namespaces/$NS/vectors \
  -H "Content-Type: application/json" \
  -d '{"ids": ["vec-1"]}' | jq
```

### Clean up

```bash
curl -s -X DELETE http://localhost:8080/v1/namespaces/$NS | jq
```

## Sizing advisor

`zeppelin_advisor` turns a data shape (vectors, dims, quantization, filters,
FTS) into ranked hardware options and a production-ready config. It embeds a
snapshot-dated cloud pricing catalog (AWS/GCP/Azure instances, block storage,
object storage) and a cost/latency model calibrated against measured
perf-contract runs. Request/byte inputs are contract-gated, while QPS and
latency are calibrated on loopback MinIO only. The S3 in-region TTFB profile
is an assumed anchor, not a project measurement, and is labeled as such in the
output banner; non-AWS rows are extrapolated.

```bash
# Rank instance / cache / nprobe combinations for your data shape
cargo run --release --bin zeppelin_advisor -- plan \
  --cloud aws --region us-east-1 --vectors 21000000 --dims 768 --replicas 3

# Inspect the embedded pricing snapshot
cargo run --release --bin zeppelin_advisor -- catalog --cloud aws --region us-east-1

# Emit a tuned zeppelin.toml for the selected hardware
cargo run --release --bin zeppelin_advisor -- emit-config \
  --cloud aws --region us-east-1 --instance i4i.2xlarge --replicas 3 \
  --vectors 21000000 --dims 768 --quantization rabitq-2bit --nprobe 256 \
  --bucket my-bucket --security-mode enforced --out zeppelin.toml
```

`plan` ranks viable candidates by monthly cost with predicted QPS, p50/p99,
$/query, and the per-row bottleneck, and lists every rejected row with its
reason. `emit-config` renders a fully commented config, validates it through
the real config loader before writing anything, recomputes the GC safety
floor from the values it emits, and generates a fresh random HMAC key in
enforced mode (move it to a secret manager before rollout). Refresh the
pricing snapshot with
[`scripts/refresh_cloud_catalog.py`](scripts/refresh_cloud_catalog.py).

## Configuration

Zeppelin boots from built-in defaults, overridden by an optional
`zeppelin.toml` (path via `ZEPPELIN_CONFIG`), overridden in turn by
environment variables. [`zeppelin.toml.example`](zeppelin.toml.example)
documents the commonly tuned knobs with their defaults and env-var names.
For a hardware-tuned config validated through the real loader, use
`zeppelin_advisor emit-config` (above).

Object storage is selected by `[storage] backend`:

| `backend` | Fields | Notes |
| --- | --- | --- |
| `s3` (default) | `s3_region`, `s3_endpoint`, `s3_access_key_id`, `s3_secret_access_key`, `s3_allow_http` | AWS S3, MinIO, R2, any S3-compatible store |
| `gcs` | `gcs_service_account_path` **or** `gcs_service_account_key`, `gcs_endpoint` (emulator only) | conditional writes key on the object generation |
| `azure` | `azure_account_name`, `azure_access_key`, `azure_endpoint`, `azure_use_emulator`, `azure_allow_http` | `bucket` names the container |
| `local` | — | development/testing only; no conditional PUT |

Fields from a backend family other than the selected one are a
configuration error, never silently ignored. With `fail_fast = true`
(default) every boot verifies the substrate's declared capabilities live —
conditional PUT with fresh and stale tokens, LIST/GET identity, delete of
an absent key — and refuses to start if the store cannot enforce them.

## API Reference

The canonical definition is the [OpenAPI 3.1 spec](api/zeppelin-api.yaml). The
tables below are a complete index of the served routes. Before exposing them in
production, configure the `[security]` section using the annotated
[`zeppelin.toml.example`](zeppelin.toml.example).

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

These routes are always registered; each is backed by a service composed from
configuration and returns a 403 `feature_disabled` error when its surface is
not enabled. Configure via the `[security]` section in
[`zeppelin.toml.example`](zeppelin.toml.example).

| Method             | Path                                            | Enabled by                              |
|--------------------|-------------------------------------------------|-----------------------------------------|
| `GET/POST`         | `/v1/security/principals`                       | `security.rbac`                         |
| `GET/POST`         | `/v1/security/keys`                             | `security.rbac`                         |
| `DELETE`           | `/v1/security/keys/:key_id`                     | `security.rbac`                         |
| `POST`             | `/v1/security/keys/:key_id/rotate`              | `security.rbac`                         |
| `GET/POST/DELETE`  | `/v1/security/grants`                           | `security.rbac`                         |
| `GET`              | `/v1/security/policy`                           | `security.rbac`                         |
| `POST`             | `/v1/security/tokens`                           | `security.rbac` + `token_signing_key_path` |
| `GET/POST`         | `/v1/security/preservation`                     | `security.rbac`                         |
| `POST`             | `/v1/security/preservation/:lock_id/release`    | `security.rbac`                         |

## API Clients

Use the HTTP API directly or generate a client from the canonical [OpenAPI 3.1 spec](api/zeppelin-api.yaml). This repository does not maintain generated client SDK packages.

## Documentation

- [OpenAPI 3.1 spec](api/zeppelin-api.yaml) — the canonical API contract
  (versioned independently of the crate; see the spec's `info.version`)

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
TEST_S3_BUCKET=zeppelin-test \
MINIO_ENDPOINT=http://localhost:9000 \
MINIO_ACCESS_KEY=minioadmin \
MINIO_SECRET_KEY=minioadmin \
cargo test --tests
```

`TEST_BACKEND=gcs` (patched fake-gcs-server) and `TEST_BACKEND=azurite`
(Azurite) run the same suites against the emulated non-S3 substrates; setup
and pinned versions are in [`scripts/emulators/`](scripts/emulators/README.md).

### Run locally against MinIO

```bash
# Start MinIO
docker compose -f docker-compose.test.yml up -d

# Run Zeppelin against local MinIO (open_unsafe dev security posture)
ZEPPELIN_CONFIG=zeppelin.dev.toml \
STORAGE_BACKEND=s3 \
S3_BUCKET=zeppelin-test \
S3_ENDPOINT=http://localhost:9000 \
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
AWS_REGION=us-east-1 \
S3_ALLOW_HTTP=true \
cargo run --bin zeppelin
```

### Lint and format

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
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

```
Write path                              Query path
──────────                              ──────────
client ── upsert ─▶ node                client ── query ─▶ node
                     │                                      │
        PUT WAL fragment ─────▶ S3      conditional GET manifest ──▶ S3
                     │                                      │
        CAS manifest (ETag) ──▶ S3      coarse wave: parallel range GETs
                                        (quantized cluster scans)
background compaction                                       │
  WAL fragments ─▶ IVF segments         rerank wave: coalesced range GETs
  + bitmap / BM25 indexes               (exact f32 rows)
  (one owner via lease + CAS)                               │
                                        merged, reranked top-k
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

- **`strong`** remotely verifies the manifest instead of trusting its cache
  age, then searches the active segment and scores uncompacted WAL data
  ([`src/query.rs:833-845`](src/query.rs#L833-L845),
  [`src/query.rs:1485-1505`](src/query.rs#L1485-L1505)). A committed write is
  therefore visible to a strong query on the same node or another node: the
  `strong_query_within_ttl_observes_manifest_advanced_on_s3` test primes a
  60-second stale cache, advances S3 through an independent writer, and asserts
  that the next strong query sees the new WAL vector
  ([`tests/strong_freshness_tests.rs:176-215`](tests/strong_freshness_tests.rs#L176-L215)).
  When the manifest is unchanged, the normal cost is one conditional manifest
  GET with no body bytes; concurrent readers may reuse a verification completed
  after their own request began
  (`strong_query_with_unchanged_manifest_uses_one_bodyless_freshness_get`,
  [`tests/strong_freshness_tests.rs:247-290`](tests/strong_freshness_tests.rs#L247-L290);
  [`src/cache/manifest_cache.rs:694-732`](src/cache/manifest_cache.rs#L694-L732)).
- **`eventual`** may reuse a manifest cached within the configured TTL, which
  defaults to 500 ms
  ([`src/cache/manifest_cache.rs:590-633`](src/cache/manifest_cache.rs#L590-L633),
  [`src/config.rs:2865-2867`](src/config.rs#L2865-L2867)). It skips WAL
  vector/BM25 scoring but still applies WAL tombstones from the selected
  manifest, so its recent upserts wait for compaction while its deletes remain
  suppressed
  ([`src/query.rs:1485-1519`](src/query.rs#L1485-L1519)). Another node may keep
  its previous manifest until that TTL expires; the writing node installs the
  returned manifest in its local cache
  (`eventual_query_within_ttl_keeps_zero_manifest_get_fast_path`,
  [`tests/strong_freshness_tests.rs:217-245`](tests/strong_freshness_tests.rs#L217-L245);
  [`src/server/handlers/vectors.rs:726-727`](src/server/handlers/vectors.rs#L726-L727),
  [`src/server/handlers/vectors.rs:1517-1518`](src/server/handlers/vectors.rs#L1517-L1518)).

### Running more than one node

#### Reads

Read requests need no node affinity: strong reads verify object-storage state,
and eventual reads use the receiving node's TTL-bounded cache
([`src/query.rs:833-845`](src/query.rs#L833-L845)). In particular, sticky
routing is not required for cross-node strong read-your-writes; that path is
covered by `strong_query_within_ttl_observes_manifest_advanced_on_s3`
([`tests/strong_freshness_tests.rs:176-215`](tests/strong_freshness_tests.rs#L176-L215)).

#### Writes

HTTP vector writes do not acquire the compaction lease: ordinary upserts and
deletes call `WalWriter::append`, while guarded variants also pass no fencing
token. Each writer's group-commit queue and last-committed manifest/ETag memo
are process-local
([`src/server/handlers/vectors.rs:690-723`](src/server/handlers/vectors.rs#L690-L723),
[`src/server/handlers/vectors.rs:1446-1474`](src/server/handlers/vectors.rs#L1446-L1474),
[`src/wal/writer.rs:404-417`](src/wal/writer.rs#L404-L417),
[`src/wal/writer.rs:576-583`](src/wal/writer.rs#L576-L583),
[`src/wal/writer.rs:805-842`](src/wal/writer.rs#L805-L842)). Manifest ETag CAS
prevents a stale writer from silently overwriting a newer manifest; under
moderate contention, `test_concurrent_writers_backoff_absorbs_conflicts` uses
three independent writers and verifies that all 24 successful fragments remain
referenced
([`tests/write_path_tests.rs:197-250`](tests/write_path_tests.rs#L197-L250)).

Contention is nevertheless visible in latency and can reach clients. An
ordinary unguarded batch makes at most eight manifest-CAS attempts, backing off
from a 10 ms base after each conflict; exhaustion returns a manifest conflict
([`src/wal/writer.rs:113-150`](src/wal/writer.rs#L113-L150),
[`src/wal/writer.rs:1047-1056`](src/wal/writer.rs#L1047-L1056),
[`src/wal/writer.rs:1233-1240`](src/wal/writer.rs#L1233-L1240)). Guarded filter
deletes, constrained ID deletes, and scoped upserts instead recompute their
selection against a fresh manifest up to four times and return a retryable 409
only after all four guard attempts conflict
([`src/server/handlers/vectors.rs:172`](src/server/handlers/vectors.rs#L172),
[`src/server/handlers/vectors.rs:739-800`](src/server/handlers/vectors.rs#L739-L800),
[`src/server/handlers/vectors.rs:1548-1626`](src/server/handlers/vectors.rs#L1548-L1626);
`test_guarded_filter_delete_reevaluates_and_succeeds_within_bound` and
`test_guarded_filter_delete_exhaustion_returns_409`,
[`tests/write_path_tests.rs:701-886`](tests/write_path_tests.rs#L701-L886)).
OpenAPI 0.2.1 publishes the 409 `CONFLICT_RETRY` plus `Retry-After` contract for
both [`upsertVectors` (`api/zeppelin-api.yaml:1275-1324`)](api/zeppelin-api.yaml#L1275-L1324)
and [`deleteVectors` (`api/zeppelin-api.yaml:1330-1360`)](api/zeppelin-api.yaml#L1330-L1360)
([`api/zeppelin-api.yaml:6`](api/zeppelin-api.yaml#L6),
[`api/zeppelin-api.yaml:1698-1715`](api/zeppelin-api.yaml#L1698-L1715),
`openapi_documents_vector_write_conflicts_and_process_local_controls`,
[`tests/contract_tests.rs:310-356`](tests/contract_tests.rs#L310-L356)).

The v1 operating rule is therefore **one writer process per namespace**. When
possible, route namespace-scoped data mutations whose URL contains
`/v1/namespaces/{ns}/...` to the same node—for example, hash that path at the
load balancer—while leaving reads free to use any ready node. On a backend that
reports version identities, this preserves process-local batching and avoids
the stale-memo conflict that strict round-robin routing creates on attempt zero
([`src/server/mod.rs:2557-2614`](src/server/mod.rs#L2557-L2614),
[`src/wal/writer.rs:975-989`](src/wal/writer.rs#L975-L989),
[`src/wal/writer.rs:1047-1056`](src/wal/writer.rs#L1047-L1056)).

#### Compaction and GC

Compaction acquires a per-namespace lease (300 seconds by default), renews it,
and carries its fencing token into manifest publication
([`src/config.rs:3151-3157`](src/config.rs#L3151-L3157),
[`src/compaction/background.rs:1148-1168`](src/compaction/background.rs#L1148-L1168),
[`src/compaction/background.rs:1191-1234`](src/compaction/background.rs#L1191-L1234)).
The first acquisition of a missing lease uses a create-only PUT; a creation
collision re-reads the authoritative lease within a five-attempt bound
([`src/wal/lease.rs:81`](src/wal/lease.rs#L81),
[`src/wal/lease.rs:293-357`](src/wal/lease.rs#L293-L357)).

GC is not covered by that lease. Its deletion path still requires candidates to
survive the configured horizon and a fresh reachability check; deleting an
already absent artifact is an idempotent completion
([`src/compaction/gc.rs:67-85`](src/compaction/gc.rs#L67-L85)). Every process
starts its own background loop, and that loop invokes its `GcRunner` before the
separately leased compaction branch
([`src/startup.rs:600-618`](src/startup.rs#L600-L618),
[`src/compaction/background.rs:2036-2041`](src/compaction/background.rs#L2036-L2041),
[`src/compaction/background.rs:2071-2087`](src/compaction/background.rs#L2071-L2087)).
A warm runner performs a namespace inventory LIST before it can skip an
unchanged, not-yet-due full cycle, so that inventory traffic scales with node
count and full-cycle work is repeated when due
([`src/compaction/gc.rs:2791-2856`](src/compaction/gc.rs#L2791-L2856)).
A dead compaction loop withholds readiness: the
`compaction_loop_death_withholds_readiness` test aborts the loop and verifies
that `/readyz` changes from 200 to 503
([`tests/compaction_liveness_tests.rs:12-78`](tests/compaction_liveness_tests.rs#L12-L78)).

#### Per-process state

`GET`/`PATCH`/`PUT /v1/config/query` read or replace only the receiving process's
snapshot; updates neither write object storage nor survive restart
([`src/runtime_config.rs:1-12`](src/runtime_config.rs#L1-L12),
[`src/server/mod.rs:2541-2550`](src/server/mod.rs#L2541-L2550)). Fan out an
operational query-config change to every node that should serve it.

Rate-limit buckets are also process-local. A client that is balanced across N
nodes can consume a separate configured quota at each node
([`src/server/mod.rs:343-344`](src/server/mod.rs#L343-L344),
[`src/server/mod.rs:2138-2188`](src/server/mod.rs#L2138-L2188);
`openapi_documents_vector_write_conflicts_and_process_local_controls`,
[`tests/contract_tests.rs:338-356`](tests/contract_tests.rs#L338-L356)).

There is no operator `node_id` field in the boot configuration
([`src/config.rs:217-253`](src/config.rs#L217-L253)). Startup takes the lease
holder ID from the audit runtime: tracing-only mode generates a fresh
`zeppelin-{UUID}`, while durable-audit mode uses the authoritative signer-scoped
audit stream ID
([`src/startup.rs:457-467`](src/startup.rs#L457-L467),
[`src/security/audit_sink.rs:551-585`](src/security/audit_sink.rs#L551-L585)).

### Full-text search (opt-in)

BM25 `rank_by` queries work out of the box against un-compacted WAL data.
For segment data, per-cluster and global inverted indexes are built during
compaction only when `indexing.fts_index = true` (or `ZEPPELIN_FTS_INDEX=true`)
— it is **off by default** because it adds compaction cost for namespaces
that never use FTS. Without it, segment BM25 falls back to a full scan,
which is rejected above `bm25_max_full_scan_clusters` (default 500) to
protect latency. Enable `fts_index` if you use `rank_by` at scale.

## Contributing

Issues and pull requests are welcome — see
[CONTRIBUTING.md](CONTRIBUTING.md) for the gates to run before sending a PR
and the ground rules (no silent fallbacks, tests hit real object storage).

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

Copyright 2026 Anup Ghatage. See [NOTICE](NOTICE) — redistributions and
derivative works must retain the attribution notices it contains, per
Section 4(d) of the license.
