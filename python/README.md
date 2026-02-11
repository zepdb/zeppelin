# Zeppelin Python Client

Python client for the [Zeppelin](https://github.com/Ghatage/stormcrow) vector search engine.

## Install

```bash
pip install ./python
```

## Quickstart

```python
from zeppelin import ZeppelinClient

with ZeppelinClient("http://localhost:8080") as client:
    # Create a namespace
    ns = client.create_namespace("my-vectors", dimensions=128, distance_metric="cosine")

    # Upsert vectors
    vectors = [
        {"id": "vec-1", "values": [0.1] * 128, "attributes": {"color": "red"}},
        {"id": "vec-2", "values": [0.2] * 128, "attributes": {"color": "blue"}},
    ]
    count = client.upsert_vectors("my-vectors", vectors)
    print(f"Upserted {count} vectors")

    # Query
    result = client.query("my-vectors", vector=[0.1] * 128, top_k=5)
    for r in result.results:
        print(f"  {r.id}: {r.score:.4f}")

    # Query with filters
    result = client.query(
        "my-vectors",
        vector=[0.1] * 128,
        top_k=5,
        filter={"op": "eq", "field": "color", "value": "red"},
    )

    # Delete vectors
    client.delete_vectors("my-vectors", ids=["vec-1"])

    # Delete namespace
    client.delete_namespace("my-vectors")
```

## Error Handling

```python
from zeppelin import ZeppelinClient, NotFoundError, ValidationError

with ZeppelinClient() as client:
    try:
        client.get_namespace("nonexistent")
    except NotFoundError:
        print("Namespace not found")

    try:
        client.create_namespace("bad", dimensions=-1)
    except ValidationError as e:
        print(f"Invalid request: {e.message}")
```

## API Reference

### `ZeppelinClient(base_url="http://localhost:8080", timeout=30.0)`

| Method | Description |
|--------|-------------|
| `health()` | Health check |
| `ready()` | Readiness check |
| `create_namespace(name, dimensions, distance_metric="cosine")` | Create namespace |
| `list_namespaces(prefix=None)` | List namespaces |
| `get_namespace(name)` | Get namespace metadata |
| `delete_namespace(name)` | Delete namespace |
| `upsert_vectors(namespace, vectors)` | Upsert vectors (returns count) |
| `delete_vectors(namespace, ids)` | Delete vectors by ID (returns count) |
| `query(namespace, vector, top_k=10, filter=None, consistency=None, nprobe=None)` | Vector search |
