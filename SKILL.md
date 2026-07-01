---
name: zeppelin
description: Use the Zeppelin vector search engine to store and search text using semantic embeddings. Supports creating namespaces, embedding text, upserting vectors, and querying by similarity or BM25 full-text search.
---

# Zeppelin Vector Search

An S3-native vector search engine. This repository implements the Zeppelin API (Rust, `src/`) and a small Embed API (Python, `embed_service/`). The hosted deployment exposes both:

- **Zeppelin API**: `http://44.242.236.80:8080` — vector storage and search
- **Embed API**: `http://44.242.236.80:8090` — text → vector encoding (all-MiniLM-L6-v2, 384-dim)

## Quick Start: Store and Search Text

### 1. Create a namespace (384 dimensions to match the embed model)
```bash
curl -X POST http://44.242.236.80:8080/v1/namespaces \
  -H "Content-Type: application/json" \
  -d '{"dimensions": 384}'
# Response includes "name": "<uuid>" — SAVE THIS! It cannot be recovered if lost.
```

### 2. Embed your text
```bash
curl -X POST http://44.242.236.80:8090/v1/embed \
  -H "Content-Type: application/json" \
  -d '{"texts": ["machine learning for protein folding", "quantum error correction"]}'
```
Returns: `{"embeddings": [[0.023, ...384 floats], [...]], "model": "all-MiniLM-L6-v2", "dimensions": 384}`

### 3. Upsert vectors (use the embeddings from step 2, replace <uuid> with your namespace name)
```bash
curl -X POST http://44.242.236.80:8080/v1/namespaces/<uuid>/vectors \
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
curl -X POST http://44.242.236.80:8080/v1/namespaces/<uuid>/query \
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
POST   /v1/namespaces                    Create: {"dimensions": 384} → returns UUID name
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
POST   /v1/namespaces/:ns/query          BM25 search:   {"rank_by": ["field", "BM25", "query"], "top_k": 10}
```

## Filters

Add `"filter"` to any query:
```json
{"op": "eq", "field": "category", "value": "science"}
{"op": "and", "filters": [{"op": "range", "field": "score", "gte": 0.8}, {"op": "eq", "field": "type", "value": "article"}]}
```
Operators: `eq`, `not_eq`, `range` (`gt`, `gte`, `lt`, `lte` fields), `in`, `not_in`, `contains`, `contains_all_tokens`, `contains_token_sequence`, `not`, `and`, `or`

## Rate Limits

- Zeppelin API: 10 req/s per IP, burst 20
- Embed API: 5 req/s per IP (CPU-intensive)
- Returns HTTP 429 if exceeded

## Client Generation

Use curl directly or generate a client from `api/zeppelin-api.yaml`. This repository does not maintain generated Python or TypeScript SDK packages.

## Health Check

```bash
curl http://44.242.236.80:8080/healthz     # Zeppelin liveness
curl http://44.242.236.80:8080/readyz      # Zeppelin readiness
curl http://44.242.236.80:8090/health      # Embed service health
```

## Important

- Always create namespaces with **384 dimensions** to match the hosted embed model
- **Namespace names are server-generated UUIDs** — save the name from the create response, it cannot be recovered
- Use `"consistency": "strong"` when reading your own recently-written data
- Attributes are optional key-value pairs attached to vectors (useful for filtering)
- The embed endpoint is for encoding text only — upsert and query go through Zeppelin (port 8080)
