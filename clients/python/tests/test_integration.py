"""Integration tests for the Zeppelin Python client.

Requires a running Zeppelin server. Set ZEPPELIN_URL to enable.

    ZEPPELIN_URL=http://localhost:8080 pytest tests/test_integration.py -v
"""

from __future__ import annotations

import os
import uuid

import pytest
import pytest_asyncio

from zeppelin import (
    AsyncZeppelinClient,
    ZeppelinClient,
    FtsFieldConfig,
    Vector,
)
from zeppelin.exceptions import ConflictError, NotFoundError
from zeppelin.filters import Filter
from zeppelin.rank_by import RankBy

ZEPPELIN_URL = os.environ.get("ZEPPELIN_URL")

pytestmark = pytest.mark.skipif(
    ZEPPELIN_URL is None,
    reason="ZEPPELIN_URL not set",
)


def unique_ns() -> str:
    return f"test-py-{uuid.uuid4().hex[:8]}"


class TestSyncIntegration:
    def test_health(self):
        with ZeppelinClient(ZEPPELIN_URL) as client:
            result = client.health()
            assert result["status"] == "ok"

    def test_readiness(self):
        with ZeppelinClient(ZEPPELIN_URL) as client:
            result = client.ready()
            assert result["s3_connected"] is True

    def test_namespace_lifecycle(self):
        ns_name = unique_ns()
        with ZeppelinClient(ZEPPELIN_URL) as client:
            # Create
            ns = client.create_namespace(ns_name, 4)
            assert ns.name == ns_name
            assert ns.dimensions == 4

            # Get
            ns2 = client.get_namespace(ns_name)
            assert ns2.name == ns_name

            # List
            nss = client.list_namespaces()
            assert any(n.name == ns_name for n in nss)

            # Duplicate fails
            with pytest.raises(ConflictError):
                client.create_namespace(ns_name, 4)

            # Delete
            client.delete_namespace(ns_name)

            # Get after delete
            with pytest.raises(NotFoundError):
                client.get_namespace(ns_name)

    def test_vector_upsert_and_query(self):
        ns_name = unique_ns()
        with ZeppelinClient(ZEPPELIN_URL) as client:
            client.create_namespace(ns_name, 4)

            # Upsert
            count = client.upsert_vectors(ns_name, [
                Vector("v1", [1.0, 0.0, 0.0, 0.0], attributes={"color": "red"}),
                Vector("v2", [0.0, 1.0, 0.0, 0.0], attributes={"color": "blue"}),
                Vector("v3", [0.0, 0.0, 1.0, 0.0], attributes={"color": "red"}),
            ])
            assert count == 3

            # Query
            result = client.query(ns_name, vector=[1.0, 0.0, 0.0, 0.0], top_k=2)
            assert len(result.results) <= 2
            assert result.results[0].id == "v1"

            # Query with filter
            result = client.query(
                ns_name,
                vector=[1.0, 0.0, 0.0, 0.0],
                top_k=10,
                filter=Filter.eq("color", "blue"),
            )
            assert all(
                r.attributes and r.attributes.get("color") == "blue"
                for r in result.results
            )

            # Delete
            deleted = client.delete_vectors(ns_name, ["v1"])
            assert deleted == 1

            # Cleanup
            client.delete_namespace(ns_name)

    def test_fts_namespace_and_bm25(self):
        ns_name = unique_ns()
        with ZeppelinClient(ZEPPELIN_URL) as client:
            ns = client.create_namespace(
                ns_name,
                4,
                full_text_search={"content": FtsFieldConfig()},
            )
            assert "content" in ns.full_text_search

            # Upsert with text attributes
            client.upsert_vectors(ns_name, [
                Vector("d1", [1.0, 0.0, 0.0, 0.0], attributes={"content": "the quick brown fox"}),
                Vector("d2", [0.0, 1.0, 0.0, 0.0], attributes={"content": "lazy dog sleeps"}),
                Vector("d3", [0.0, 0.0, 1.0, 0.0], attributes={"content": "quick fox jumps over lazy dog"}),
            ])

            # BM25 query
            result = client.query(
                ns_name,
                rank_by=RankBy.bm25("content", "quick fox"),
                top_k=3,
            )
            assert len(result.results) > 0

            client.delete_namespace(ns_name)


class TestAsyncIntegration:
    @pytest.mark.asyncio
    async def test_async_lifecycle(self):
        ns_name = unique_ns()
        async with AsyncZeppelinClient(ZEPPELIN_URL) as client:
            result = await client.health()
            assert result["status"] == "ok"

            ns = await client.create_namespace(ns_name, 4)
            assert ns.name == ns_name

            await client.upsert_vectors(ns_name, [
                Vector("v1", [1.0, 0.0, 0.0, 0.0]),
            ])

            result = await client.query(ns_name, vector=[1.0, 0.0, 0.0, 0.0], top_k=1)
            assert len(result.results) == 1

            await client.delete_namespace(ns_name)
