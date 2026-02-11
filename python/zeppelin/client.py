"""Zeppelin Python client."""

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
from .types import Namespace, QueryResponse, SearchResult, Vector


class ZeppelinClient:
    """Synchronous client for the Zeppelin vector search API.

    Usage::

        with ZeppelinClient("http://localhost:8080") as client:
            client.create_namespace("my-ns", dimensions=128)
            client.upsert_vectors("my-ns", [{"id": "v1", "values": [0.1] * 128}])
            result = client.query("my-ns", [0.1] * 128, top_k=5)
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        timeout: float = 30.0,
    ):
        self._client = httpx.Client(base_url=base_url, timeout=timeout)

    def __enter__(self) -> ZeppelinClient:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    # -- Health --

    def health(self) -> dict:
        """Check server health."""
        return self._get("/healthz")

    def ready(self) -> dict:
        """Check server readiness."""
        return self._get("/readyz")

    # -- Namespaces --

    def create_namespace(
        self,
        name: str,
        dimensions: int,
        distance_metric: str = "cosine",
    ) -> Namespace:
        """Create a new namespace."""
        data = self._post(
            "/v1/namespaces",
            json={
                "name": name,
                "dimensions": dimensions,
                "distance_metric": distance_metric,
            },
        )
        return _parse_namespace(data)

    def list_namespaces(self, prefix: str | None = None) -> list[Namespace]:
        """List all namespaces, optionally filtered by prefix."""
        params = {}
        if prefix is not None:
            params["prefix"] = prefix
        data = self._get("/v1/namespaces", params=params)
        return [_parse_namespace(ns) for ns in data]

    def get_namespace(self, name: str) -> Namespace:
        """Get namespace metadata."""
        data = self._get(f"/v1/namespaces/{name}")
        return _parse_namespace(data)

    def delete_namespace(self, name: str) -> None:
        """Delete a namespace and all its data."""
        self._delete(f"/v1/namespaces/{name}")

    # -- Vectors --

    def upsert_vectors(
        self,
        namespace: str,
        vectors: list[dict | Vector],
    ) -> int:
        """Upsert vectors into a namespace. Returns the number of upserted vectors."""
        payload = [
            v.to_dict() if isinstance(v, Vector) else v for v in vectors
        ]
        data = self._put(
            f"/v1/namespaces/{namespace}/vectors",
            json={"vectors": payload},
        )
        return data["upserted"]

    def delete_vectors(self, namespace: str, ids: list[str]) -> int:
        """Delete vectors by ID. Returns the number of deleted vectors."""
        url = f"/v1/namespaces/{namespace}/vectors"
        resp = self._client.request("DELETE", url, json={"ids": ids})
        data = self._handle_response(resp)
        return data["deleted"]

    # -- Query --

    def query(
        self,
        namespace: str,
        vector: list[float],
        top_k: int = 10,
        filter: dict | None = None,
        consistency: str | None = None,
        nprobe: int | None = None,
    ) -> QueryResponse:
        """Run a vector similarity search."""
        body: dict[str, Any] = {"vector": vector, "top_k": top_k}
        if filter is not None:
            body["filter"] = filter
        if consistency is not None:
            body["consistency"] = consistency
        if nprobe is not None:
            body["nprobe"] = nprobe
        data = self._post(f"/v1/namespaces/{namespace}/query", json=body)
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

    # -- Internal helpers --

    def _get(self, path: str, params: dict | None = None) -> Any:
        resp = self._client.get(path, params=params)
        return self._handle_response(resp)

    def _post(self, path: str, json: dict) -> Any:
        resp = self._client.post(path, json=json)
        return self._handle_response(resp)

    def _put(self, path: str, json: dict) -> Any:
        resp = self._client.put(path, json=json)
        return self._handle_response(resp)

    def _delete(self, path: str) -> Any:
        resp = self._client.delete(path)
        return self._handle_response(resp)

    @staticmethod
    def _handle_response(resp: httpx.Response) -> Any:
        if resp.status_code < 300:
            if resp.status_code == 204 or not resp.content:
                return None
            return resp.json()

        # Try to extract error message from JSON body
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
    return Namespace(
        name=data["name"],
        dimensions=data["dimensions"],
        distance_metric=data["distance_metric"],
        vector_count=data["vector_count"],
        created_at=data["created_at"],
        updated_at=data["updated_at"],
    )
