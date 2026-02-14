<p align="center">
  <img src="assets/icon.svg" alt="Zeppelin" width="128" height="128">
</p>
<h1 align="center">Zeppelin</h1>
<p align="center">
  <a href="https://www.rust-lang.org"><img src="https://img.shields.io/badge/Rust-1.84+-DEA584?logo=rust&logoColor=white" alt="Rust: 1.84+"></a>
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
- **BM25 full-text search** -- Inverted indexes with configurable tokenization, stemming, and multi-field `rank_by` expressions
- **Bitmap pre-filters** -- RoaringBitmap indexes for sub-millisecond attribute filtering
- **Write-ahead log** -- Durable writes with compaction into indexed segments
- **Strong & eventual consistency** -- Choose per-query
- **Object storage** -- S3, MinIO, and S3-compatible backends. GCS and Azure planned

## Quick Start

Spin up Zeppelin with MinIO locally using Docker Compose:

```bash
docker compose up
```

This starts Zeppelin on port `8080` and MinIO on port `9000` with a pre-created `zeppelin` bucket.

### Create a namespace

```bash
curl -s http://localhost:8080/v1/namespaces \
  -H "Content-Type: application/json" \
  -d '{"name": "my-vectors", "dimensions": 384}' | jq
```

### Upsert vectors

```bash
curl -s http://localhost:8080/v1/namespaces/my-vectors/vectors \
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
curl -s http://localhost:8080/v1/namespaces/my-vectors/query \
  -H "Content-Type: application/json" \
  -d '{
    "vector": [0.1, 0.2, 0.3, "...384 dims..."],
    "top_k": 10
  }' | jq
```

### Delete vectors

```bash
curl -s -X DELETE http://localhost:8080/v1/namespaces/my-vectors/vectors \
  -H "Content-Type: application/json" \
  -d '{"ids": ["vec-1"]}' | jq
```

## API Reference

| Method   | Path                              | Description            |
|----------|-----------------------------------|------------------------|
| `GET`    | `/healthz`                        | Liveness probe         |
| `GET`    | `/readyz`                         | Readiness probe        |
| `GET`    | `/metrics`                        | Prometheus metrics     |
| `POST`   | `/v1/namespaces`                  | Create a namespace     |
| `GET`    | `/v1/namespaces`                  | List namespaces        |
| `GET`    | `/v1/namespaces/:ns`              | Get namespace metadata |
| `DELETE` | `/v1/namespaces/:ns`              | Delete a namespace     |
| `POST`   | `/v1/namespaces/:ns/vectors`      | Upsert vectors         |
| `DELETE` | `/v1/namespaces/:ns/vectors`      | Delete vectors         |
| `POST`   | `/v1/namespaces/:ns/query`        | Query nearest neighbors|

## Client SDKs

Official client libraries for Zeppelin:

| SDK | Package | Install | Repo |
|-----|---------|---------|------|
| Python | `zeppelin-python` | `pip install zeppelin-python` | [zepdb/zeppelin-py](https://github.com/zepdb/zeppelin-py) |
| TypeScript | `zeppelin-typescript` | `npm install zeppelin-typescript` | [zepdb/zeppelin-typescript](https://github.com/zepdb/zeppelin-typescript) |

Both SDKs cover all 9 API endpoints with full support for:
- Vector similarity search (ANN) with filters
- BM25 full-text search with `rank_by` S-expressions
- Composable filter builders (eq, range, in, contains, and/or/not, ...)
- FTS namespace configuration (`FtsFieldConfig`)
- Typed error hierarchy (Validation, NotFound, Conflict, Server)

The Python SDK also provides an async client (`AsyncZeppelinClient`).

An [OpenAPI 3.1 spec](api/zeppelin-api.yaml) is available as the canonical API reference.

## Development

### Prerequisites

- [Rust 1.84+](https://www.rust-lang.org/tools/install)
- [Docker](https://docs.docker.com/get-docker/) (for MinIO in tests)

### Build

```bash
cargo build
```

### Run tests

Start MinIO for the test suite, then run tests:

```bash
docker compose -f docker-compose.test.yml up -d
TEST_BACKEND=minio cargo test
```

### Run locally (without Docker)

```bash
# Start MinIO
docker compose -f docker-compose.test.yml up -d

# Run Zeppelin against local MinIO
STORAGE_BACKEND=s3 \
S3_BUCKET=zeppelin \
S3_ENDPOINT=http://localhost:9000 \
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
AWS_REGION=us-east-1 \
S3_ALLOW_HTTP=true \
cargo run
```

### Lint and format

```bash
cargo fmt --check
cargo clippy -- -D warnings
```

## Architecture

```
src/
  storage/     Object store abstraction (S3, S3-compatible)
  wal/         Write-ahead log: fragments, manifest, reader/writer
  namespace/   Namespace CRUD and metadata
  index/       Vector indexing (IVF-Flat with k-means)
  cache/       Local disk cache with LRU eviction
  compaction/  Background WAL-to-segment compaction
  server/      Axum HTTP handlers, routes, middleware
```

Writes land in the WAL as immutable fragments. Background compaction merges fragments into indexed segments (IVF, bitmap pre-filters, BM25 inverted indexes). Queries probe the closest centroids and merge results from any un-compacted WAL fragments.

## License

[Apache-2.0](https://opensource.org/licenses/Apache-2.0)
