---
name: zeppelin
description: Use the Zeppelin vector search engine to store and search text using semantic embeddings. Supports creating namespaces, embedding text, upserting vectors, and querying by similarity or BM25 full-text search.
---

# Zeppelin Vector Search

An S3-native vector search engine. Two services:

- **Zeppelin API**: `http://44.242.236.80:8080` — vector storage and search
- **Embed API**: `http://44.242.236.80:8090` — text → vector encoding (all-MiniLM-L6-v2, 384-dim)

## Quick Start: Store and Search Text

### 1. Create a namespace (384 dimensions to match the embed model)
```bash
curl -X POST http://44.242.236.80:8080/v1/namespaces \
  -H "Content-Type: application/json" \
  -d '{"name": "my-notes", "dimensions": 384}'
```

### 2. Embed your text
```bash
curl -X POST http://44.242.236.80:8090/v1/embed \
  -H "Content-Type: application/json" \
  -d '{"texts": ["machine learning for protein folding", "quantum error correction"]}'
```
Returns: `{"embeddings": [[0.023, ...384 floats], [...]], "model": "all-MiniLM-L6-v2", "dimensions": 384}`

### 3. Upsert vectors (use the embeddings from step 2)
```bash
curl -X POST http://44.242.236.80:8080/v1/namespaces/my-notes/vectors \
  -H "Content-Type: application/json" \
  -d '{
    "vectors": [
      {"id": "doc1", "values": [0.023, ...], "attributes": {"title": "ML for proteins"}},
      {"id": "doc2", "values": [...], "attributes": {"title": "Quantum errors"}}
    ]
  }'
```

### 4. Search: embed query text, then query
```bash
# Embed the query
curl -X POST http://44.242.236.80:8090/v1/embed \
  -H "Content-Type: application/json" \
  -d '{"texts": ["protein structure prediction"]}'

# Query with the resulting vector
curl -X POST http://44.242.236.80:8080/v1/namespaces/my-notes/query \
  -H "Content-Type: application/json" \
  -d '{"vector": [0.023, ...384 floats], "top_k": 5, "consistency": "strong"}'
```

## Full API Reference

### Embed API (port 8090)

**Embed text → vectors:**
```
POST /v1/embed  {"texts": ["text1", "text2"]}
```
- Max 10 texts per request, max 512 characters each
- Returns 384-dimensional vectors

### Zeppelin API (port 8080)

**Namespaces:**
```
POST   /v1/namespaces                    Create: {"name": "x", "dimensions": 384}
GET    /v1/namespaces                    List all
GET    /v1/namespaces/:ns                Get info
DELETE /v1/namespaces/:ns                Delete
```

**Vectors:**
```
POST   /v1/namespaces/:ns/vectors        Upsert: {"vectors": [{id, values, attributes}]}
DELETE /v1/namespaces/:ns/vectors        Delete: {"ids": ["id1", "id2"]}
```

**Query:**
```
POST   /v1/namespaces/:ns/query          Vector search: {"vector": [...], "top_k": 10}
POST   /v1/namespaces/:ns/query          BM25 search:   {"rank_by": ["bm25", "field", "query"], "top_k": 10}
```

## Filters

Add `"filter"` to any query:
```json
{"op": "eq", "field": "category", "value": "science"}
{"op": "and", "filters": [{"op": "gte", "field": "score", "value": 0.8}, {"op": "eq", "field": "type", "value": "article"}]}
```
Operators: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `not_in`, `contains`, `not`, `and`, `or`

## Rate Limits

- Zeppelin API: 10 req/s per IP, burst 20
- Embed API: 5 req/s per IP (CPU-intensive)
- Returns HTTP 429 if exceeded

## Client SDKs (Alternative to curl)

Install the official SDK for a better developer experience:

**Python:**
```bash
pip install zeppelin-python
```
```python
from zeppelin import ZeppelinClient

client = ZeppelinClient("http://44.242.236.80:8080")
client.create_namespace("my-notes", dimensions=384)
client.upsert("my-notes", vectors=[{"id": "doc1", "values": [...], "attributes": {"title": "hello"}}])
results = client.query("my-notes", vector=[...], top_k=10)
```

**TypeScript:**
```bash
npm install zeppelin-typescript
```
```typescript
import { ZeppelinClient } from 'zeppelin-typescript';

const client = new ZeppelinClient('http://44.242.236.80:8080');
await client.createNamespace('my-notes', { dimensions: 384 });
await client.upsert('my-notes', { vectors: [{ id: 'doc1', values: [...], attributes: { title: 'hello' } }] });
const results = await client.query('my-notes', { vector: [...], topK: 10 });
```

Both SDKs support: vector search, BM25 full-text search, composable filters, FTS config, typed errors.
Repos: [zepdb/zeppelin-py](https://github.com/zepdb/zeppelin-py) | [zepdb/zeppelin-typescript](https://github.com/zepdb/zeppelin-typescript)

## Health Check

```bash
curl http://44.242.236.80:8080/healthz     # Zeppelin liveness
curl http://44.242.236.80:8080/readyz      # Zeppelin readiness
curl http://44.242.236.80:8090/health      # Embed service health
```

## Important

- Always create namespaces with **384 dimensions** to match the hosted embed model
- Use `"consistency": "strong"` when reading your own recently-written data
- Attributes are optional key-value pairs attached to vectors (useful for filtering)
- Namespace names: alphanumeric + hyphens/underscores, 1-255 chars
- The embed endpoint is for encoding text only — upsert and query go through Zeppelin (port 8080)
