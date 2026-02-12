# zeppelin-typescript

TypeScript SDK for the [Zeppelin](https://github.com/Ghatage/zeppelin) vector search engine. Uses native `fetch` with zero runtime dependencies. Full TypeScript types for all API shapes.

## Installation

```bash
npm install zeppelin-typescript
```

Or install from source:

```bash
cd clients/typescript
npm install
npm run build
```

## Connecting to Zeppelin

### Local development (MinIO)

```typescript
import { ZeppelinClient } from "zeppelin-typescript";

// Default: connects to http://localhost:8080
// Start Zeppelin locally with MinIO as the S3 backend:
//   docker run -p 9000:9000 minio/minio server /data
//   ZEPPELIN_S3_ENDPOINT=http://localhost:9000 cargo run

const client = new ZeppelinClient();
// or explicitly:
const client2 = new ZeppelinClient({ baseUrl: "http://localhost:8080" });
```

### Production (AWS S3)

```typescript
import { ZeppelinClient } from "zeppelin-typescript";

// Zeppelin server running against real S3:
//   AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
//   AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
//   ZEPPELIN_S3_BUCKET=my-zeppelin-bucket \
//   ZEPPELIN_S3_REGION=us-east-1 \
//   cargo run --release

const client = new ZeppelinClient({
  baseUrl: "https://zeppelin.example.com",
  timeout: 60_000,
  headers: { Authorization: "Bearer eyJhbGciOiJIUzI1NiIs..." },
});
```

### Custom Fetch (Node.js < 18, SSR, testing)

```typescript
import { ZeppelinClient } from "zeppelin-typescript";

const client = new ZeppelinClient({
  baseUrl: "http://localhost:8080",
  fetch: myCustomFetchImpl,
});
```

## Full API Reference

### Health Checks

```typescript
import { ZeppelinClient } from "zeppelin-typescript";

const client = new ZeppelinClient();

// GET /healthz — always 200 if the process is running
const health = await client.health();
console.log(health); // { status: "ok" }

// GET /readyz — verifies S3 connectivity (throws on 503)
const ready = await client.ready();
console.log(ready); // { status: "ready", s3_connected: true }
```

### Namespace Management

```typescript
import { ZeppelinClient } from "zeppelin-typescript";
import type { Namespace, FtsFieldConfig } from "zeppelin-typescript";

const client = new ZeppelinClient();

// POST /v1/namespaces — create a namespace
const ns: Namespace = await client.createNamespace("products", 768, {
  distanceMetric: "cosine", // "cosine" | "euclidean" | "dot_product"
});
console.log(ns.name);            // "products"
console.log(ns.dimensions);      // 768
console.log(ns.distance_metric); // "cosine"
console.log(ns.vector_count);    // 0
console.log(ns.created_at);      // "2025-06-15T10:30:00Z"
console.log(ns.updated_at);      // "2025-06-15T10:30:00Z"

// Create a namespace with full-text search fields
const nsFts = await client.createNamespace("articles", 768, {
  fullTextSearch: {
    title: {
      language: "english",
      stemming: true,
      remove_stopwords: true,
      case_sensitive: false,
      k1: 1.2,   // BM25 term frequency saturation
      b: 0.75,   // BM25 length normalization
      max_token_length: 40,
    },
    body: {}, // all defaults
  },
});
console.log(Object.keys(nsFts.full_text_search!)); // ["title", "body"]

// GET /v1/namespaces — list all namespaces
const namespaces = await client.listNamespaces();
for (const n of namespaces) {
  console.log(`${n.name}: ${n.vector_count} vectors, ${n.dimensions}d`);
}

// GET /v1/namespaces/:ns — get a single namespace
const fetched = await client.getNamespace("products");
console.log(fetched.vector_count);

// DELETE /v1/namespaces/:ns — delete namespace and all data
await client.deleteNamespace("products");
```

### Vector Operations

```typescript
import { ZeppelinClient } from "zeppelin-typescript";
import type { VectorEntry } from "zeppelin-typescript";

const client = new ZeppelinClient();

// POST /v1/namespaces/:ns/vectors — upsert vectors
const vectors: VectorEntry[] = [
  {
    id: "prod-001",
    values: [0.12, -0.34, 0.56, ...new Array(765).fill(0.0)],
    attributes: {
      name: "Wireless Headphones",
      category: "electronics",
      price: 79.99,
      in_stock: true,
      tags: ["audio", "bluetooth", "wireless"],
    },
  },
  {
    id: "prod-002",
    values: [0.78, 0.23, -0.45, ...new Array(765).fill(0.0)],
    attributes: {
      name: "USB-C Cable",
      category: "accessories",
      price: 12.99,
      in_stock: true,
      tags: ["cable", "usb-c"],
    },
  },
];

const upserted = await client.upsertVectors("products", vectors);
console.log(`Upserted ${upserted} vectors`); // "Upserted 2 vectors"

// DELETE /v1/namespaces/:ns/vectors — delete vectors by ID
const deleted = await client.deleteVectors("products", ["prod-002"]);
console.log(`Deleted ${deleted} vectors`); // "Deleted 1 vectors"
```

### Vector Search (ANN)

```typescript
import { ZeppelinClient, Filters } from "zeppelin-typescript";

const client = new ZeppelinClient();

// POST /v1/namespaces/:ns/query — vector similarity search
const result = await client.query("products", {
  vector: [0.12, -0.34, 0.56, ...new Array(765).fill(0.0)],
  topK: 10,
});
console.log(`Found ${result.results.length} results`);
console.log(`Scanned ${result.scanned_fragments} WAL fragments`);
console.log(`Scanned ${result.scanned_segments} index segments`);

for (const r of result.results) {
  console.log(`  ${r.id}: score=${r.score.toFixed(4)}`);
  if (r.attributes) {
    console.log(`    name=${r.attributes.name}`);
  }
}

// With consistency level
const strongResult = await client.query("products", {
  vector: new Array(768).fill(0.1),
  topK: 5,
  consistency: "strong",   // "strong" (default) scans WAL + index
                            // "eventual" reads index only (faster)
});

// With nprobe (number of IVF clusters to probe)
const preciseResult = await client.query("products", {
  vector: new Array(768).fill(0.1),
  topK: 5,
  nprobe: 8, // higher = more accurate but slower
});
```

### Filtered Search

```typescript
import { ZeppelinClient, Filters } from "zeppelin-typescript";

const client = new ZeppelinClient();
const vec = new Array(768).fill(0.1);

// Exact match
await client.query("products", {
  vector: vec,
  topK: 5,
  filter: Filters.eq("category", "electronics"),
});

// Not equal
await client.query("products", {
  vector: vec,
  filter: Filters.notEq("category", "accessories"),
});

// Numeric range (any combination of gte/lte/gt/lt)
await client.query("products", {
  vector: vec,
  filter: Filters.range("price", { gte: 20.0, lte: 100.0 }),
});

// IN — value matches any in the list
await client.query("products", {
  vector: vec,
  filter: Filters.in("category", ["electronics", "accessories"]),
});

// NOT IN
await client.query("products", {
  vector: vec,
  filter: Filters.notIn("category", ["spam", "test"]),
});

// CONTAINS — array field contains a value
await client.query("products", {
  vector: vec,
  filter: Filters.contains("tags", "bluetooth"),
});

// FTS token filters
await client.query("products", {
  vector: vec,
  filter: Filters.containsAllTokens("name", ["wireless", "headphones"]),
});
await client.query("products", {
  vector: vec,
  filter: Filters.containsTokenSequence("name", ["usb", "c", "cable"]),
});

// Boolean combinators — AND, OR, NOT
await client.query("products", {
  vector: vec,
  filter: Filters.and(
    Filters.eq("in_stock", true),
    Filters.range("price", { lte: 50.0 }),
  ),
});

await client.query("products", {
  vector: vec,
  filter: Filters.or(
    Filters.eq("category", "electronics"),
    Filters.eq("category", "audio"),
  ),
});

await client.query("products", {
  vector: vec,
  filter: Filters.not(Filters.eq("in_stock", false)),
});

// Deep nesting
await client.query("products", {
  vector: vec,
  filter: Filters.and(
    Filters.or(
      Filters.eq("category", "electronics"),
      Filters.eq("category", "accessories"),
    ),
    Filters.not(Filters.eq("in_stock", false)),
    Filters.range("price", { gte: 10.0, lte: 200.0 }),
  ),
});
```

### BM25 Full-Text Search

```typescript
import { ZeppelinClient, RankBy, Filters } from "zeppelin-typescript";

const client = new ZeppelinClient();

// Single-field BM25 search
const result = await client.query("articles", {
  rankBy: RankBy.bm25("body", "vector database performance"),
  topK: 10,
});
for (const r of result.results) {
  console.log(`  ${r.id}: relevance=${r.score.toFixed(4)}`); // higher = more relevant
}

// Prefix search (autocomplete) — last token treated as prefix
await client.query("articles", {
  rankBy: RankBy.bm25("title", "vec"),
  topK: 5,
  lastAsPrefix: true, // matches "vector", "vectors", "vectorize", etc.
});

// Multi-field search with Sum
await client.query("articles", {
  rankBy: RankBy.sum(
    RankBy.bm25("title", "rust async"),
    RankBy.bm25("body", "rust async"),
  ),
  topK: 10,
});

// Weighted multi-field (title 3x more important)
await client.query("articles", {
  rankBy: RankBy.sum(
    RankBy.product(3.0, RankBy.bm25("title", "search engine")),
    RankBy.bm25("body", "search engine"),
  ),
  topK: 10,
});

// Max across fields (take the best field score per document)
await client.query("articles", {
  rankBy: RankBy.max(
    RankBy.bm25("title", "performance"),
    RankBy.bm25("body", "performance"),
  ),
  topK: 10,
});

// BM25 with filters
await client.query("articles", {
  rankBy: RankBy.bm25("body", "machine learning"),
  topK: 10,
  filter: Filters.eq("published", true),
});
```

### Error Handling

```typescript
import { ZeppelinClient } from "zeppelin-typescript";
import {
  ZeppelinError,    // base class for all errors
  ValidationError,  // 400 — bad request (dimensions=0, empty vectors, etc.)
  NotFoundError,    // 404 — namespace or resource not found
  ConflictError,    // 409 — namespace already exists, manifest conflict, lease held
  ServerError,      // 5xx — internal server error
} from "zeppelin-typescript";

const client = new ZeppelinClient();

try {
  await client.getNamespace("nonexistent");
} catch (e) {
  if (e instanceof NotFoundError) {
    console.log(`Not found: ${e.message}`);
    console.log(`HTTP status: ${e.statusCode}`); // 404
  }
}

try {
  await client.createNamespace("existing-ns", 128);
  await client.createNamespace("existing-ns", 128); // duplicate
} catch (e) {
  if (e instanceof ConflictError) {
    console.log(`Conflict: ${e.message}`); // "namespace already exists"
  }
}

try {
  await client.createNamespace("bad", 0);
} catch (e) {
  if (e instanceof ValidationError) {
    console.log(`Validation failed: ${e.message}`);
  }
}

// Catch all Zeppelin errors
try {
  await client.getNamespace("maybe");
} catch (e) {
  if (e instanceof ZeppelinError) {
    console.log(`Error ${e.statusCode}: ${e.message}`);
  }
}
```

## End-to-End Example: Local MinIO

```typescript
/**
 * Full lifecycle against a local Zeppelin + MinIO setup.
 *
 * Start MinIO:
 *   docker run -p 9000:9000 -p 9001:9001 \
 *     -e MINIO_ROOT_USER=minioadmin \
 *     -e MINIO_ROOT_PASSWORD=minioadmin \
 *     minio/minio server /data --console-address ":9001"
 *
 * Start Zeppelin:
 *   ZEPPELIN_S3_ENDPOINT=http://localhost:9000 \
 *   ZEPPELIN_S3_BUCKET=zeppelin \
 *   AWS_ACCESS_KEY_ID=minioadmin \
 *   AWS_SECRET_ACCESS_KEY=minioadmin \
 *   cargo run
 */

import { ZeppelinClient, Filters, RankBy } from "zeppelin-typescript";
import type { VectorEntry } from "zeppelin-typescript";

const client = new ZeppelinClient({ baseUrl: "http://localhost:8080" });

// 1. Verify connectivity
const ready = await client.ready();
console.assert(ready.s3_connected === true);

// 2. Create namespace with FTS
const ns = await client.createNamespace("blog-posts", 4, {
  distanceMetric: "cosine",
  fullTextSearch: {
    title: {},
    body: {},
  },
});

// 3. Upsert documents
const docs: VectorEntry[] = [
  {
    id: "post-1",
    values: [1.0, 0.0, 0.0, 0.0],
    attributes: {
      title: "Introduction to Vector Databases",
      body: "Vector databases store and search high-dimensional embeddings efficiently.",
      author: "alice",
      published: true,
    },
  },
  {
    id: "post-2",
    values: [0.0, 1.0, 0.0, 0.0],
    attributes: {
      title: "BM25 Ranking Explained",
      body: "BM25 is a probabilistic ranking function used in information retrieval.",
      author: "bob",
      published: true,
    },
  },
  {
    id: "post-3",
    values: [0.0, 0.0, 1.0, 0.0],
    attributes: {
      title: "Draft: Advanced Indexing",
      body: "IVF-Flat, scalar quantization, and product quantization techniques.",
      author: "alice",
      published: false,
    },
  },
];
await client.upsertVectors("blog-posts", docs);

// 4. Vector similarity search
const vecResult = await client.query("blog-posts", {
  vector: [1.0, 0.1, 0.0, 0.0],
  topK: 2,
});
console.log("Vector search:", vecResult.results.map((r) => r.id));

// 5. Filtered vector search
const filtered = await client.query("blog-posts", {
  vector: [1.0, 0.1, 0.0, 0.0],
  topK: 10,
  filter: Filters.and(
    Filters.eq("published", true),
    Filters.eq("author", "alice"),
  ),
});
console.log("Filtered:", filtered.results.map((r) => r.id));

// 6. BM25 full-text search
const bm25Result = await client.query("blog-posts", {
  rankBy: RankBy.bm25("body", "vector database"),
  topK: 5,
});
console.log(
  "BM25:",
  bm25Result.results.map((r) => `${r.id} (${r.score.toFixed(2)})`),
);

// 7. Weighted multi-field BM25
const weightedResult = await client.query("blog-posts", {
  rankBy: RankBy.sum(
    RankBy.product(2.0, RankBy.bm25("title", "ranking")),
    RankBy.bm25("body", "ranking"),
  ),
  topK: 5,
});
console.log(
  "Weighted BM25:",
  weightedResult.results.map((r) => `${r.id} (${r.score.toFixed(2)})`),
);

// 8. Cleanup
await client.deleteVectors("blog-posts", ["post-3"]);
await client.deleteNamespace("blog-posts");
console.log("Done!");
```

## End-to-End Example: AWS S3

```typescript
/**
 * Full lifecycle against Zeppelin running with AWS S3.
 *
 * Start Zeppelin:
 *   AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
 *   AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
 *   ZEPPELIN_S3_BUCKET=my-zeppelin-prod \
 *   ZEPPELIN_S3_REGION=us-east-1 \
 *   cargo run --release
 */

import { ZeppelinClient, Filters, RankBy } from "zeppelin-typescript";

const client = new ZeppelinClient({
  baseUrl: "https://zeppelin.example.com",
  timeout: 60_000,
  headers: {
    Authorization:
      "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example",
  },
});

// Create namespace for 1536-dim OpenAI embeddings
await client.createNamespace("knowledge-base", 1536, {
  distanceMetric: "cosine",
  fullTextSearch: {
    content: { k1: 1.5, b: 0.8 },
  },
});

// Upsert embeddings from your pipeline
const embedding = new Array(1536).fill(0.0123); // placeholder for real embedding
await client.upsertVectors("knowledge-base", [
  {
    id: "doc-a1b2c3",
    values: embedding,
    attributes: {
      content: "Zeppelin is an S3-native vector search engine.",
      source: "docs",
      section: "overview",
    },
  },
]);

// Hybrid search: vector + filter
const result = await client.query("knowledge-base", {
  vector: embedding,
  topK: 20,
  nprobe: 16,
  consistency: "strong",
  filter: Filters.eq("source", "docs"),
});

// BM25 search with autocomplete
const autocomplete = await client.query("knowledge-base", {
  rankBy: RankBy.bm25("content", "S3 nat"),
  lastAsPrefix: true,
  topK: 5,
});
```

## Running Tests

```bash
cd clients/typescript

# Install dependencies
npm install

# Build
npm run build

# Run unit tests (no server needed)
npm test

# Run integration tests (requires running Zeppelin server)
ZEPPELIN_URL=http://localhost:8080 npx vitest run tests/integration.test.ts
```
