# zeppelin-python

Python SDK for the [Zeppelin](https://github.com/Ghatage/zeppelin) vector search engine. Supports sync and async clients, vector similarity search, BM25 full-text search, filtered queries, and composable filter/rank builders.

## Installation

```bash
pip install zeppelin-python
```

Or install from source:

```bash
cd clients/python
pip install -e ".[dev]"
```

## Connecting to Zeppelin

### Local development (MinIO)

```python
from zeppelin import ZeppelinClient

# Default: connects to http://localhost:8080
# Start Zeppelin locally with MinIO as the S3 backend:
#   docker run -p 9000:9000 minio/minio server /data
#   ZEPPELIN_S3_ENDPOINT=http://localhost:9000 cargo run

client = ZeppelinClient("http://localhost:8080")
```

### Production (AWS S3)

```python
from zeppelin import ZeppelinClient

# Zeppelin server running against real S3:
#   AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
#   AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
#   ZEPPELIN_S3_BUCKET=my-zeppelin-bucket \
#   ZEPPELIN_S3_REGION=us-east-1 \
#   cargo run

client = ZeppelinClient(
    base_url="https://zeppelin.example.com",
    timeout=60.0,
    headers={"Authorization": "Bearer eyJhbGciOiJIUzI1NiIs..."},
)
```

## Full API Reference

### Health Checks

```python
from zeppelin import ZeppelinClient

with ZeppelinClient("http://localhost:8080") as client:
    # GET /healthz — always 200 if the process is running
    health = client.health()
    print(health)  # {"status": "ok"}

    # GET /readyz — verifies S3 connectivity (503 if unreachable)
    ready = client.ready()
    print(ready)  # {"status": "ready", "s3_connected": True}
```

### Namespace Management

```python
from zeppelin import ZeppelinClient, FtsFieldConfig

with ZeppelinClient("http://localhost:8080") as client:

    # POST /v1/namespaces — create a namespace
    ns = client.create_namespace(
        name="products",
        dimensions=768,
        distance_metric="cosine",  # "cosine" | "euclidean" | "dot_product"
    )
    print(ns.name)             # "products"
    print(ns.dimensions)       # 768
    print(ns.distance_metric)  # "cosine"
    print(ns.vector_count)     # 0
    print(ns.created_at)       # "2025-06-15T10:30:00Z"
    print(ns.updated_at)       # "2025-06-15T10:30:00Z"

    # Create a namespace with full-text search fields
    ns_fts = client.create_namespace(
        name="articles",
        dimensions=768,
        full_text_search={
            "title": FtsFieldConfig(
                language="english",
                stemming=True,
                remove_stopwords=True,
                case_sensitive=False,
                k1=1.2,    # BM25 term frequency saturation
                b=0.75,    # BM25 length normalization
                max_token_length=40,
            ),
            "body": FtsFieldConfig(),  # all defaults
        },
    )
    print(ns_fts.full_text_search.keys())  # dict_keys(["title", "body"])

    # GET /v1/namespaces — list all namespaces
    namespaces = client.list_namespaces()
    for n in namespaces:
        print(f"{n.name}: {n.vector_count} vectors, {n.dimensions}d")

    # GET /v1/namespaces/:ns — get a single namespace
    ns = client.get_namespace("products")
    print(ns.vector_count)

    # DELETE /v1/namespaces/:ns — delete namespace and all data
    client.delete_namespace("products")
```

### Vector Operations

```python
from zeppelin import ZeppelinClient, Vector

with ZeppelinClient("http://localhost:8080") as client:

    # POST /v1/namespaces/:ns/vectors — upsert vectors
    # Accepts Vector objects or plain dicts

    # Using Vector objects
    count = client.upsert_vectors("products", [
        Vector(
            id="prod-001",
            values=[0.12, -0.34, 0.56] + [0.0] * 765,
            attributes={
                "name": "Wireless Headphones",
                "category": "electronics",
                "price": 79.99,
                "in_stock": True,
                "tags": ["audio", "bluetooth", "wireless"],
            },
        ),
        Vector(
            id="prod-002",
            values=[0.78, 0.23, -0.45] + [0.0] * 765,
            attributes={
                "name": "USB-C Cable",
                "category": "accessories",
                "price": 12.99,
                "in_stock": True,
                "tags": ["cable", "usb-c"],
            },
        ),
    ])
    print(f"Upserted {count} vectors")  # "Upserted 2 vectors"

    # Using plain dicts (same wire format)
    count = client.upsert_vectors("products", [
        {
            "id": "prod-003",
            "values": [0.0] * 768,
            "attributes": {"name": "Laptop Stand", "price": 45.00},
        },
    ])

    # DELETE /v1/namespaces/:ns/vectors — delete vectors by ID
    deleted = client.delete_vectors("products", ["prod-003"])
    print(f"Deleted {deleted} vectors")  # "Deleted 1 vectors"
```

### Vector Search (ANN)

```python
from zeppelin import ZeppelinClient, Filter

with ZeppelinClient("http://localhost:8080") as client:

    # POST /v1/namespaces/:ns/query — vector similarity search
    result = client.query(
        "products",
        vector=[0.12, -0.34, 0.56] + [0.0] * 765,
        top_k=10,
    )
    print(f"Found {len(result.results)} results")
    print(f"Scanned {result.scanned_fragments} WAL fragments")
    print(f"Scanned {result.scanned_segments} index segments")

    for r in result.results:
        print(f"  {r.id}: score={r.score:.4f}")
        if r.attributes:
            print(f"    name={r.attributes.get('name')}")

    # With consistency level
    result = client.query(
        "products",
        vector=[0.12, -0.34, 0.56] + [0.0] * 765,
        top_k=5,
        consistency="strong",    # "strong" (default) scans WAL + index
                                 # "eventual" reads index only (faster)
    )

    # With nprobe (number of IVF clusters to probe)
    result = client.query(
        "products",
        vector=[0.12, -0.34, 0.56] + [0.0] * 765,
        top_k=5,
        nprobe=8,  # higher = more accurate but slower
    )
```

### Filtered Search

```python
from zeppelin import ZeppelinClient, Filter

with ZeppelinClient("http://localhost:8080") as client:

    # Exact match
    result = client.query(
        "products",
        vector=[0.1] * 768,
        top_k=5,
        filter=Filter.eq("category", "electronics"),
    )

    # Not equal
    result = client.query(
        "products",
        vector=[0.1] * 768,
        filter=Filter.not_eq("category", "accessories"),
    )

    # Numeric range (any combination of gte/lte/gt/lt)
    result = client.query(
        "products",
        vector=[0.1] * 768,
        filter=Filter.range("price", gte=20.0, lte=100.0),
    )

    # IN — value matches any in the list
    result = client.query(
        "products",
        vector=[0.1] * 768,
        filter=Filter.in_("category", ["electronics", "accessories"]),
    )

    # NOT IN
    result = client.query(
        "products",
        vector=[0.1] * 768,
        filter=Filter.not_in("category", ["spam", "test"]),
    )

    # CONTAINS — array field contains a value
    result = client.query(
        "products",
        vector=[0.1] * 768,
        filter=Filter.contains("tags", "bluetooth"),
    )

    # FTS token filters
    result = client.query(
        "products",
        vector=[0.1] * 768,
        filter=Filter.contains_all_tokens("name", ["wireless", "headphones"]),
    )
    result = client.query(
        "products",
        vector=[0.1] * 768,
        filter=Filter.contains_token_sequence("name", ["usb", "c", "cable"]),
    )

    # Boolean combinators — AND, OR, NOT
    result = client.query(
        "products",
        vector=[0.1] * 768,
        filter=Filter.and_(
            Filter.eq("in_stock", True),
            Filter.range("price", lte=50.0),
        ),
    )

    result = client.query(
        "products",
        vector=[0.1] * 768,
        filter=Filter.or_(
            Filter.eq("category", "electronics"),
            Filter.eq("category", "audio"),
        ),
    )

    result = client.query(
        "products",
        vector=[0.1] * 768,
        filter=Filter.not_(Filter.eq("in_stock", False)),
    )

    # Deep nesting
    result = client.query(
        "products",
        vector=[0.1] * 768,
        filter=Filter.and_(
            Filter.or_(
                Filter.eq("category", "electronics"),
                Filter.eq("category", "accessories"),
            ),
            Filter.not_(Filter.eq("in_stock", False)),
            Filter.range("price", gte=10.0, lte=200.0),
        ),
    )
```

### BM25 Full-Text Search

```python
from zeppelin import ZeppelinClient, RankBy, Filter

with ZeppelinClient("http://localhost:8080") as client:

    # Single-field BM25 search
    result = client.query(
        "articles",
        rank_by=RankBy.bm25("body", "vector database performance"),
        top_k=10,
    )
    for r in result.results:
        print(f"  {r.id}: relevance={r.score:.4f}")  # higher = more relevant

    # Prefix search (autocomplete) — last token treated as prefix
    result = client.query(
        "articles",
        rank_by=RankBy.bm25("title", "vec"),
        top_k=5,
        last_as_prefix=True,  # matches "vector", "vectors", "vectorize", etc.
    )

    # Multi-field search with Sum
    result = client.query(
        "articles",
        rank_by=RankBy.sum(
            RankBy.bm25("title", "rust async"),
            RankBy.bm25("body", "rust async"),
        ),
        top_k=10,
    )

    # Weighted multi-field (title 3x more important)
    result = client.query(
        "articles",
        rank_by=RankBy.sum(
            RankBy.product(3.0, RankBy.bm25("title", "search engine")),
            RankBy.bm25("body", "search engine"),
        ),
        top_k=10,
    )

    # Max across fields (take the best field score per document)
    result = client.query(
        "articles",
        rank_by=RankBy.max(
            RankBy.bm25("title", "performance"),
            RankBy.bm25("body", "performance"),
        ),
        top_k=10,
    )

    # BM25 with filters
    result = client.query(
        "articles",
        rank_by=RankBy.bm25("body", "machine learning"),
        top_k=10,
        filter=Filter.eq("published", True),
    )
```

### Async Client

Every method available on `ZeppelinClient` has an `async` equivalent on `AsyncZeppelinClient`.

```python
import asyncio
from zeppelin import AsyncZeppelinClient, Vector, Filter, RankBy, FtsFieldConfig

async def main():
    async with AsyncZeppelinClient("http://localhost:8080") as client:
        # Health
        health = await client.health()
        ready = await client.ready()

        # Namespaces
        ns = await client.create_namespace(
            "async-demo",
            dimensions=128,
            full_text_search={"content": FtsFieldConfig()},
        )
        namespaces = await client.list_namespaces()
        ns = await client.get_namespace("async-demo")

        # Vectors
        count = await client.upsert_vectors("async-demo", [
            Vector("v1", [0.1] * 128, attributes={"content": "hello world"}),
            Vector("v2", [0.2] * 128, attributes={"content": "goodbye world"}),
        ])

        # Vector search
        result = await client.query(
            "async-demo",
            vector=[0.1] * 128,
            top_k=5,
            filter=Filter.eq("content", "hello world"),
        )

        # BM25 search
        result = await client.query(
            "async-demo",
            rank_by=RankBy.bm25("content", "hello"),
            top_k=5,
        )

        # Delete vectors
        deleted = await client.delete_vectors("async-demo", ["v1"])

        # Delete namespace
        await client.delete_namespace("async-demo")

asyncio.run(main())
```

### Error Handling

```python
from zeppelin import ZeppelinClient
from zeppelin.exceptions import (
    ZeppelinError,    # base class for all errors
    ValidationError,  # 400 — bad request (dimensions=0, empty vectors, etc.)
    NotFoundError,    # 404 — namespace or resource not found
    ConflictError,    # 409 — namespace already exists, manifest conflict, lease held
    ServerError,      # 5xx — internal server error
)

with ZeppelinClient("http://localhost:8080") as client:
    try:
        client.get_namespace("nonexistent")
    except NotFoundError as e:
        print(f"Not found: {e.message}")
        print(f"HTTP status: {e.status_code}")  # 404

    try:
        client.create_namespace("existing-ns", dimensions=128)
        client.create_namespace("existing-ns", dimensions=128)  # duplicate
    except ConflictError as e:
        print(f"Conflict: {e.message}")  # "namespace already exists"

    try:
        client.create_namespace("bad", dimensions=0)
    except ValidationError as e:
        print(f"Validation failed: {e.message}")

    try:
        client.health()
    except ServerError as e:
        print(f"Server error ({e.status_code}): {e.message}")

    # Catch all Zeppelin errors
    try:
        client.get_namespace("maybe")
    except ZeppelinError as e:
        print(f"Error {e.status_code}: {e.message}")
```

## End-to-End Example: Local MinIO

```python
"""
Full lifecycle against a local Zeppelin + MinIO setup.

Start MinIO:
    docker run -p 9000:9000 -p 9001:9001 \
        -e MINIO_ROOT_USER=minioadmin \
        -e MINIO_ROOT_PASSWORD=minioadmin \
        minio/minio server /data --console-address ":9001"

Start Zeppelin:
    ZEPPELIN_S3_ENDPOINT=http://localhost:9000 \
    ZEPPELIN_S3_BUCKET=zeppelin \
    AWS_ACCESS_KEY_ID=minioadmin \
    AWS_SECRET_ACCESS_KEY=minioadmin \
    cargo run
"""

from zeppelin import ZeppelinClient, Vector, Filter, RankBy, FtsFieldConfig

with ZeppelinClient("http://localhost:8080") as client:
    # 1. Verify connectivity
    assert client.ready()["s3_connected"] is True

    # 2. Create namespace with FTS
    ns = client.create_namespace(
        "blog-posts",
        dimensions=4,
        distance_metric="cosine",
        full_text_search={"title": FtsFieldConfig(), "body": FtsFieldConfig()},
    )

    # 3. Upsert documents
    client.upsert_vectors("blog-posts", [
        Vector("post-1", [1.0, 0.0, 0.0, 0.0], attributes={
            "title": "Introduction to Vector Databases",
            "body": "Vector databases store and search high-dimensional embeddings efficiently.",
            "author": "alice",
            "published": True,
        }),
        Vector("post-2", [0.0, 1.0, 0.0, 0.0], attributes={
            "title": "BM25 Ranking Explained",
            "body": "BM25 is a probabilistic ranking function used in information retrieval.",
            "author": "bob",
            "published": True,
        }),
        Vector("post-3", [0.0, 0.0, 1.0, 0.0], attributes={
            "title": "Draft: Advanced Indexing",
            "body": "IVF-Flat, scalar quantization, and product quantization techniques.",
            "author": "alice",
            "published": False,
        }),
    ])

    # 4. Vector similarity search
    result = client.query("blog-posts", vector=[1.0, 0.1, 0.0, 0.0], top_k=2)
    print("Vector search:", [r.id for r in result.results])

    # 5. Filtered vector search
    result = client.query(
        "blog-posts",
        vector=[1.0, 0.1, 0.0, 0.0],
        top_k=10,
        filter=Filter.and_(
            Filter.eq("published", True),
            Filter.eq("author", "alice"),
        ),
    )
    print("Filtered:", [r.id for r in result.results])

    # 6. BM25 full-text search
    result = client.query(
        "blog-posts",
        rank_by=RankBy.bm25("body", "vector database"),
        top_k=5,
    )
    print("BM25:", [(r.id, f"{r.score:.2f}") for r in result.results])

    # 7. Weighted multi-field BM25
    result = client.query(
        "blog-posts",
        rank_by=RankBy.sum(
            RankBy.product(2.0, RankBy.bm25("title", "ranking")),
            RankBy.bm25("body", "ranking"),
        ),
        top_k=5,
    )
    print("Weighted BM25:", [(r.id, f"{r.score:.2f}") for r in result.results])

    # 8. Cleanup
    client.delete_vectors("blog-posts", ["post-3"])
    client.delete_namespace("blog-posts")
    print("Done!")
```

## End-to-End Example: AWS S3

```python
"""
Full lifecycle against Zeppelin running with AWS S3.

Start Zeppelin:
    AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
    AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
    ZEPPELIN_S3_BUCKET=my-zeppelin-prod \
    ZEPPELIN_S3_REGION=us-east-1 \
    cargo run --release
"""

from zeppelin import ZeppelinClient, Vector, Filter, RankBy, FtsFieldConfig

client = ZeppelinClient(
    base_url="https://zeppelin.example.com",
    timeout=60.0,
    headers={"Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example"},
)

# Create namespace for 1536-dim OpenAI embeddings
ns = client.create_namespace(
    "knowledge-base",
    dimensions=1536,
    distance_metric="cosine",
    full_text_search={"content": FtsFieldConfig(k1=1.5, b=0.8)},
)

# Upsert embeddings from your pipeline
embedding = [0.0123] * 1536  # placeholder for real embedding
client.upsert_vectors("knowledge-base", [
    Vector("doc-a1b2c3", embedding, attributes={
        "content": "Zeppelin is an S3-native vector search engine.",
        "source": "docs",
        "section": "overview",
    }),
])

# Hybrid search: vector + filter
result = client.query(
    "knowledge-base",
    vector=embedding,
    top_k=20,
    nprobe=16,
    consistency="strong",
    filter=Filter.eq("source", "docs"),
)

# BM25 search with autocomplete
result = client.query(
    "knowledge-base",
    rank_by=RankBy.bm25("content", "S3 nat"),
    last_as_prefix=True,
    top_k=5,
)

client.close()
```

## Running Tests

```bash
cd clients/python

# Install dev dependencies
pip install -e ".[dev]"

# Run unit tests (no server needed)
pytest tests/ -v --ignore=tests/test_integration.py

# Run integration tests (requires running Zeppelin server)
ZEPPELIN_URL=http://localhost:8080 pytest tests/test_integration.py -v
```
