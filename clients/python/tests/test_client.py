"""Unit tests for ZeppelinClient and AsyncZeppelinClient (mocked httpx)."""

import json

import httpx
import pytest
import pytest_asyncio

from zeppelin import (
    AsyncZeppelinClient,
    ZeppelinClient,
    FtsFieldConfig,
    Vector,
)
from zeppelin.exceptions import (
    ConflictError,
    NotFoundError,
    ServerError,
    ValidationError,
)
from zeppelin.filters import Filter
from zeppelin.rank_by import RankBy

# --- Fixtures ---

NS_RESPONSE = {
    "name": "test-ns",
    "dimensions": 128,
    "distance_metric": "cosine",
    "vector_count": 0,
    "created_at": "2025-01-01T00:00:00Z",
    "updated_at": "2025-01-01T00:00:00Z",
}

NS_RESPONSE_WITH_FTS = {
    **NS_RESPONSE,
    "full_text_search": {
        "content": {
            "language": "english",
            "stemming": True,
            "remove_stopwords": True,
            "case_sensitive": False,
            "k1": 1.2,
            "b": 0.75,
            "max_token_length": 40,
        }
    },
}

QUERY_RESPONSE = {
    "results": [
        {"id": "v1", "score": 0.95, "attributes": {"color": "red"}},
        {"id": "v2", "score": 0.80},
    ],
    "scanned_fragments": 3,
    "scanned_segments": 1,
}


# --- Sync Client Tests ---


class TestSyncHealth:
    def test_health(self, httpx_mock):
        httpx_mock.add_response(url="http://test:8080/healthz", json={"status": "ok"})
        with ZeppelinClient("http://test:8080") as client:
            result = client.health()
        assert result == {"status": "ok"}

    def test_ready(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/readyz",
            json={"status": "ready", "s3_connected": True},
        )
        with ZeppelinClient("http://test:8080") as client:
            result = client.ready()
        assert result["s3_connected"] is True


class TestSyncNamespaces:
    def test_create_namespace(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces",
            method="POST",
            json=NS_RESPONSE,
            status_code=201,
        )
        with ZeppelinClient("http://test:8080") as client:
            ns = client.create_namespace("test-ns", 128)
        assert ns.name == "test-ns"
        assert ns.dimensions == 128
        assert ns.distance_metric == "cosine"

    def test_create_namespace_with_fts(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces",
            method="POST",
            json=NS_RESPONSE_WITH_FTS,
            status_code=201,
        )
        with ZeppelinClient("http://test:8080") as client:
            ns = client.create_namespace(
                "test-ns",
                128,
                full_text_search={"content": FtsFieldConfig()},
            )
        assert "content" in ns.full_text_search
        assert ns.full_text_search["content"].stemming is True

    def test_list_namespaces(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces",
            method="GET",
            json=[NS_RESPONSE],
        )
        with ZeppelinClient("http://test:8080") as client:
            nss = client.list_namespaces()
        assert len(nss) == 1
        assert nss[0].name == "test-ns"

    def test_get_namespace(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces/test-ns",
            json=NS_RESPONSE,
        )
        with ZeppelinClient("http://test:8080") as client:
            ns = client.get_namespace("test-ns")
        assert ns.name == "test-ns"

    def test_delete_namespace(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces/test-ns",
            method="DELETE",
            status_code=204,
        )
        with ZeppelinClient("http://test:8080") as client:
            client.delete_namespace("test-ns")


class TestSyncVectors:
    def test_upsert_vectors_with_dicts(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces/test-ns/vectors",
            method="POST",
            json={"upserted": 2},
        )
        with ZeppelinClient("http://test:8080") as client:
            count = client.upsert_vectors("test-ns", [
                {"id": "v1", "values": [1.0, 2.0]},
                {"id": "v2", "values": [3.0, 4.0]},
            ])
        assert count == 2

    def test_upsert_vectors_with_objects(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces/test-ns/vectors",
            method="POST",
            json={"upserted": 1},
        )
        with ZeppelinClient("http://test:8080") as client:
            count = client.upsert_vectors("test-ns", [
                Vector("v1", [1.0, 2.0], attributes={"color": "red"}),
            ])
        assert count == 1

        # Verify the request body was serialized correctly
        request = httpx_mock.get_requests()[0]
        body = json.loads(request.content)
        assert body["vectors"][0]["attributes"] == {"color": "red"}

    def test_delete_vectors(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces/test-ns/vectors",
            method="DELETE",
            json={"deleted": 3},
        )
        with ZeppelinClient("http://test:8080") as client:
            count = client.delete_vectors("test-ns", ["v1", "v2", "v3"])
        assert count == 3


class TestSyncQuery:
    def test_vector_query(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces/test-ns/query",
            method="POST",
            json=QUERY_RESPONSE,
        )
        with ZeppelinClient("http://test:8080") as client:
            result = client.query("test-ns", vector=[0.1, 0.2], top_k=5)
        assert len(result.results) == 2
        assert result.results[0].id == "v1"
        assert result.results[0].score == 0.95
        assert result.results[0].attributes == {"color": "red"}
        assert result.results[1].attributes is None
        assert result.scanned_fragments == 3
        assert result.scanned_segments == 1

    def test_vector_query_with_filter(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces/test-ns/query",
            method="POST",
            json=QUERY_RESPONSE,
        )
        with ZeppelinClient("http://test:8080") as client:
            client.query(
                "test-ns",
                vector=[0.1, 0.2],
                filter=Filter.eq("color", "red"),
            )
        request = httpx_mock.get_requests()[0]
        body = json.loads(request.content)
        assert body["filter"] == {"op": "eq", "field": "color", "value": "red"}

    def test_bm25_query(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces/test-ns/query",
            method="POST",
            json=QUERY_RESPONSE,
        )
        with ZeppelinClient("http://test:8080") as client:
            client.query(
                "test-ns",
                rank_by=RankBy.bm25("content", "search query"),
            )
        request = httpx_mock.get_requests()[0]
        body = json.loads(request.content)
        assert body["rank_by"] == ["content", "BM25", "search query"]
        assert "vector" not in body

    def test_bm25_query_with_prefix(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces/test-ns/query",
            method="POST",
            json=QUERY_RESPONSE,
        )
        with ZeppelinClient("http://test:8080") as client:
            client.query(
                "test-ns",
                rank_by=RankBy.bm25("content", "sea"),
                last_as_prefix=True,
            )
        request = httpx_mock.get_requests()[0]
        body = json.loads(request.content)
        assert body["last_as_prefix"] is True

    def test_query_with_consistency(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces/test-ns/query",
            method="POST",
            json=QUERY_RESPONSE,
        )
        with ZeppelinClient("http://test:8080") as client:
            client.query(
                "test-ns",
                vector=[0.1],
                consistency="eventual",
            )
        request = httpx_mock.get_requests()[0]
        body = json.loads(request.content)
        assert body["consistency"] == "eventual"

    def test_query_with_nprobe(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces/test-ns/query",
            method="POST",
            json=QUERY_RESPONSE,
        )
        with ZeppelinClient("http://test:8080") as client:
            client.query("test-ns", vector=[0.1], nprobe=4)
        request = httpx_mock.get_requests()[0]
        body = json.loads(request.content)
        assert body["nprobe"] == 4


class TestSyncErrors:
    def test_400_raises_validation_error(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces",
            method="POST",
            json={"error": "dimensions must be > 0", "status": 400},
            status_code=400,
        )
        with ZeppelinClient("http://test:8080") as client:
            with pytest.raises(ValidationError) as exc_info:
                client.create_namespace("bad", 0)
        assert exc_info.value.status_code == 400
        assert "dimensions" in exc_info.value.message

    def test_404_raises_not_found(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces/missing",
            json={"error": "namespace 'missing' not found", "status": 404},
            status_code=404,
        )
        with ZeppelinClient("http://test:8080") as client:
            with pytest.raises(NotFoundError) as exc_info:
                client.get_namespace("missing")
        assert exc_info.value.status_code == 404

    def test_409_raises_conflict(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces",
            method="POST",
            json={"error": "namespace already exists", "status": 409},
            status_code=409,
        )
        with ZeppelinClient("http://test:8080") as client:
            with pytest.raises(ConflictError):
                client.create_namespace("dup", 128)

    def test_500_raises_server_error(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces",
            method="GET",
            json={"error": "internal error", "status": 500},
            status_code=500,
        )
        with ZeppelinClient("http://test:8080") as client:
            with pytest.raises(ServerError):
                client.list_namespaces()


# --- Async Client Tests ---


class TestAsyncHealth:
    @pytest.mark.asyncio
    async def test_health(self, httpx_mock):
        httpx_mock.add_response(url="http://test:8080/healthz", json={"status": "ok"})
        async with AsyncZeppelinClient("http://test:8080") as client:
            result = await client.health()
        assert result == {"status": "ok"}


class TestAsyncNamespaces:
    @pytest.mark.asyncio
    async def test_create_namespace(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces",
            method="POST",
            json=NS_RESPONSE,
            status_code=201,
        )
        async with AsyncZeppelinClient("http://test:8080") as client:
            ns = await client.create_namespace("test-ns", 128)
        assert ns.name == "test-ns"

    @pytest.mark.asyncio
    async def test_list_namespaces(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces",
            method="GET",
            json=[NS_RESPONSE],
        )
        async with AsyncZeppelinClient("http://test:8080") as client:
            nss = await client.list_namespaces()
        assert len(nss) == 1

    @pytest.mark.asyncio
    async def test_delete_namespace(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces/test-ns",
            method="DELETE",
            status_code=204,
        )
        async with AsyncZeppelinClient("http://test:8080") as client:
            await client.delete_namespace("test-ns")


class TestAsyncVectors:
    @pytest.mark.asyncio
    async def test_upsert_vectors(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces/test-ns/vectors",
            method="POST",
            json={"upserted": 1},
        )
        async with AsyncZeppelinClient("http://test:8080") as client:
            count = await client.upsert_vectors("test-ns", [
                Vector("v1", [1.0, 2.0]),
            ])
        assert count == 1

    @pytest.mark.asyncio
    async def test_delete_vectors(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces/test-ns/vectors",
            method="DELETE",
            json={"deleted": 2},
        )
        async with AsyncZeppelinClient("http://test:8080") as client:
            count = await client.delete_vectors("test-ns", ["v1", "v2"])
        assert count == 2


class TestAsyncQuery:
    @pytest.mark.asyncio
    async def test_vector_query(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces/test-ns/query",
            method="POST",
            json=QUERY_RESPONSE,
        )
        async with AsyncZeppelinClient("http://test:8080") as client:
            result = await client.query("test-ns", vector=[0.1, 0.2])
        assert len(result.results) == 2

    @pytest.mark.asyncio
    async def test_bm25_query(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces/test-ns/query",
            method="POST",
            json=QUERY_RESPONSE,
        )
        async with AsyncZeppelinClient("http://test:8080") as client:
            result = await client.query(
                "test-ns",
                rank_by=RankBy.bm25("content", "search"),
            )
        assert len(result.results) == 2


class TestAsyncErrors:
    @pytest.mark.asyncio
    async def test_404_raises_not_found(self, httpx_mock):
        httpx_mock.add_response(
            url="http://test:8080/v1/namespaces/missing",
            json={"error": "not found", "status": 404},
            status_code=404,
        )
        async with AsyncZeppelinClient("http://test:8080") as client:
            with pytest.raises(NotFoundError):
                await client.get_namespace("missing")
