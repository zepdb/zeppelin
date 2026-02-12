"""Zeppelin Python client — sync and async."""

from __future__ import annotations

from typing import Any

import httpx

from .exceptions import (
    ConflictError,
    NotFoundError,
    ServerError,
    ValidationError,
    ZeppelinError,
)
from .types import FtsFieldConfig, Namespace, QueryResponse, SearchResult, Vector


def _handle_response(resp: httpx.Response) -> Any:
    """Shared response handler for sync and async clients."""
    if resp.status_code < 300:
        if resp.status_code == 204 or not resp.content:
            return None
        return resp.json()

    try:
        body = resp.json()
        message = body.get("error", resp.text)
    except Exception:
        message = resp.text

    status = resp.status_code
    if status == 400:
        raise ValidationError(message, status_code=status)
    if status == 404:
        raise NotFoundError(message, status_code=status)
    if status == 409:
        raise ConflictError(message, status_code=status)
    if status >= 500:
        raise ServerError(message, status_code=status)
    raise ZeppelinError(message, status_code=status)


def _parse_namespace(data: dict) -> Namespace:
    fts_raw = data.get("full_text_search", {})
    fts = {k: FtsFieldConfig.from_dict(v) for k, v in fts_raw.items()} if fts_raw else {}
    return Namespace(
        name=data["name"],
        dimensions=data["dimensions"],
        distance_metric=data["distance_metric"],
        vector_count=data["vector_count"],
        created_at=data["created_at"],
        updated_at=data["updated_at"],
        full_text_search=fts,
    )


def _build_query_body(
    vector: list[float] | None,
    rank_by: list | None,
    top_k: int,
    filter: dict | None,
    consistency: str | None,
    nprobe: int | None,
    last_as_prefix: bool,
) -> dict[str, Any]:
    body: dict[str, Any] = {"top_k": top_k}
    if vector is not None:
        body["vector"] = vector
    if rank_by is not None:
        body["rank_by"] = rank_by
    if last_as_prefix:
        body["last_as_prefix"] = True
    if filter is not None:
        body["filter"] = filter
    if consistency is not None:
        body["consistency"] = consistency
    if nprobe is not None:
        body["nprobe"] = nprobe
    return body


def _parse_query_response(data: dict) -> QueryResponse:
    return QueryResponse(
        results=[
            SearchResult(
                id=r["id"],
                score=r["score"],
                attributes=r.get("attributes"),
            )
            for r in data["results"]
        ],
        scanned_fragments=data["scanned_fragments"],
        scanned_segments=data["scanned_segments"],
    )


def _build_create_ns_body(
    name: str,
    dimensions: int,
    distance_metric: str,
    full_text_search: dict[str, FtsFieldConfig | dict] | None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "name": name,
        "dimensions": dimensions,
        "distance_metric": distance_metric,
    }
    if full_text_search:
        body["full_text_search"] = {
            k: v.to_dict() if isinstance(v, FtsFieldConfig) else v
            for k, v in full_text_search.items()
        }
    return body


class ZeppelinClient:
    """Synchronous client for the Zeppelin vector search API.

    Usage::

        with ZeppelinClient("http://localhost:8080") as client:
            client.create_namespace("my-ns", dimensions=128)
            client.upsert_vectors("my-ns", [Vector("v1", [0.1] * 128)])
            result = client.query("my-ns", vector=[0.1] * 128, top_k=5)
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        timeout: float = 30.0,
        headers: dict[str, str] | None = None,
    ):
        self._client = httpx.Client(
            base_url=base_url,
            timeout=timeout,
            headers=headers or {},
        )

    def __enter__(self) -> ZeppelinClient:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    # -- Health --

    def health(self) -> dict:
        """Check server health."""
        resp = self._client.get("/healthz")
        return _handle_response(resp)

    def ready(self) -> dict:
        """Check server readiness."""
        resp = self._client.get("/readyz")
        return _handle_response(resp)

    # -- Namespaces --

    def create_namespace(
        self,
        name: str,
        dimensions: int,
        distance_metric: str = "cosine",
        full_text_search: dict[str, FtsFieldConfig | dict] | None = None,
    ) -> Namespace:
        """Create a new namespace."""
        body = _build_create_ns_body(name, dimensions, distance_metric, full_text_search)
        resp = self._client.post("/v1/namespaces", json=body)
        data = _handle_response(resp)
        return _parse_namespace(data)

    def list_namespaces(self) -> list[Namespace]:
        """List all namespaces."""
        resp = self._client.get("/v1/namespaces")
        data = _handle_response(resp)
        return [_parse_namespace(ns) for ns in data]

    def get_namespace(self, name: str) -> Namespace:
        """Get namespace metadata."""
        resp = self._client.get(f"/v1/namespaces/{name}")
        data = _handle_response(resp)
        return _parse_namespace(data)

    def delete_namespace(self, name: str) -> None:
        """Delete a namespace and all its data."""
        resp = self._client.delete(f"/v1/namespaces/{name}")
        _handle_response(resp)

    # -- Vectors --

    def upsert_vectors(
        self,
        namespace: str,
        vectors: list[dict | Vector],
    ) -> int:
        """Upsert vectors. Returns the number of upserted vectors."""
        payload = [v.to_dict() if isinstance(v, Vector) else v for v in vectors]
        resp = self._client.post(
            f"/v1/namespaces/{namespace}/vectors",
            json={"vectors": payload},
        )
        data = _handle_response(resp)
        return data["upserted"]

    def delete_vectors(self, namespace: str, ids: list[str]) -> int:
        """Delete vectors by ID. Returns the number of deleted vectors."""
        resp = self._client.request(
            "DELETE",
            f"/v1/namespaces/{namespace}/vectors",
            json={"ids": ids},
        )
        data = _handle_response(resp)
        return data["deleted"]

    # -- Query --

    def query(
        self,
        namespace: str,
        *,
        vector: list[float] | None = None,
        rank_by: list | None = None,
        top_k: int = 10,
        filter: dict | None = None,
        consistency: str | None = None,
        nprobe: int | None = None,
        last_as_prefix: bool = False,
    ) -> QueryResponse:
        """Run a vector similarity search or BM25 full-text search.

        Exactly one of ``vector`` or ``rank_by`` must be provided.
        """
        body = _build_query_body(vector, rank_by, top_k, filter, consistency, nprobe, last_as_prefix)
        resp = self._client.post(f"/v1/namespaces/{namespace}/query", json=body)
        data = _handle_response(resp)
        return _parse_query_response(data)


class AsyncZeppelinClient:
    """Async client for the Zeppelin vector search API.

    Usage::

        async with AsyncZeppelinClient("http://localhost:8080") as client:
            await client.create_namespace("my-ns", dimensions=128)
            await client.upsert_vectors("my-ns", [Vector("v1", [0.1] * 128)])
            result = await client.query("my-ns", vector=[0.1] * 128, top_k=5)
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        timeout: float = 30.0,
        headers: dict[str, str] | None = None,
    ):
        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            headers=headers or {},
        )

    async def __aenter__(self) -> AsyncZeppelinClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    # -- Health --

    async def health(self) -> dict:
        """Check server health."""
        resp = await self._client.get("/healthz")
        return _handle_response(resp)

    async def ready(self) -> dict:
        """Check server readiness."""
        resp = await self._client.get("/readyz")
        return _handle_response(resp)

    # -- Namespaces --

    async def create_namespace(
        self,
        name: str,
        dimensions: int,
        distance_metric: str = "cosine",
        full_text_search: dict[str, FtsFieldConfig | dict] | None = None,
    ) -> Namespace:
        """Create a new namespace."""
        body = _build_create_ns_body(name, dimensions, distance_metric, full_text_search)
        resp = await self._client.post("/v1/namespaces", json=body)
        data = _handle_response(resp)
        return _parse_namespace(data)

    async def list_namespaces(self) -> list[Namespace]:
        """List all namespaces."""
        resp = await self._client.get("/v1/namespaces")
        data = _handle_response(resp)
        return [_parse_namespace(ns) for ns in data]

    async def get_namespace(self, name: str) -> Namespace:
        """Get namespace metadata."""
        resp = await self._client.get(f"/v1/namespaces/{name}")
        data = _handle_response(resp)
        return _parse_namespace(data)

    async def delete_namespace(self, name: str) -> None:
        """Delete a namespace and all its data."""
        resp = await self._client.delete(f"/v1/namespaces/{name}")
        _handle_response(resp)

    # -- Vectors --

    async def upsert_vectors(
        self,
        namespace: str,
        vectors: list[dict | Vector],
    ) -> int:
        """Upsert vectors. Returns the number of upserted vectors."""
        payload = [v.to_dict() if isinstance(v, Vector) else v for v in vectors]
        resp = await self._client.post(
            f"/v1/namespaces/{namespace}/vectors",
            json={"vectors": payload},
        )
        data = _handle_response(resp)
        return data["upserted"]

    async def delete_vectors(self, namespace: str, ids: list[str]) -> int:
        """Delete vectors by ID. Returns the number of deleted vectors."""
        resp = await self._client.request(
            "DELETE",
            f"/v1/namespaces/{namespace}/vectors",
            json={"ids": ids},
        )
        data = _handle_response(resp)
        return data["deleted"]

    # -- Query --

    async def query(
        self,
        namespace: str,
        *,
        vector: list[float] | None = None,
        rank_by: list | None = None,
        top_k: int = 10,
        filter: dict | None = None,
        consistency: str | None = None,
        nprobe: int | None = None,
        last_as_prefix: bool = False,
    ) -> QueryResponse:
        """Run a vector similarity search or BM25 full-text search.

        Exactly one of ``vector`` or ``rank_by`` must be provided.
        """
        body = _build_query_body(vector, rank_by, top_k, filter, consistency, nprobe, last_as_prefix)
        resp = await self._client.post(f"/v1/namespaces/{namespace}/query", json=body)
        data = _handle_response(resp)
        return _parse_query_response(data)
