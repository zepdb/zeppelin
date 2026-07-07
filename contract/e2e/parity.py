"""Cross-language client parity check for a live Zeppelin engine."""

from __future__ import annotations

import json
import math
import os
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


ENGINE_ROOT = Path(__file__).resolve().parents[2]
PY_REPO = Path(require_env("ZEPPELIN_PY_REPO")).resolve()
TS_REPO = Path(require_env("ZEPPELIN_TS_REPO")).resolve()
ZEPPELIN_URL = require_env("ZEPPELIN_URL")

sys.path.insert(0, str(PY_REPO))

from zeppelin import FtsFieldConfig, Vector, ZeppelinClient  # noqa: E402


PARITY_QUERY: dict[str, Any] = {
    "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
    "rerank": {"type": "vector", "vector": [10.0, 0.0]},
    "candidate_k": 6,
    "top_k": 4,
    "facets": ["category"],
    "consistency": "strong",
    "projection": {"include_attributes": False},
}


def canonical_vectors() -> list[Vector]:
    return [
        Vector(
            "seed",
            [0.0, 0.0],
            attributes={
                "tenant": "seed",
                "doc_id": "seed",
                "category": "seed",
                "tags": ["seed"],
                "content": "anchor search",
            },
        ),
        Vector(
            "doc-a-1",
            [0.1, 0.0],
            attributes={
                "tenant": "keep",
                "doc_id": "doc-a",
                "category": "alpha",
                "tags": ["fresh", "red"],
                "content": "alpha search",
            },
        ),
        Vector(
            "doc-a-2",
            [0.2, 0.0],
            attributes={
                "tenant": "keep",
                "doc_id": "doc-a",
                "category": "alpha",
                "tags": ["fresh", "blue"],
                "content": "beta search rerank",
            },
        ),
        Vector(
            "doc-b-1",
            [0.3, 0.0],
            attributes={
                "tenant": "keep",
                "doc_id": "doc-b",
                "category": "beta",
                "tags": ["red"],
                "content": "rerank rerank rerank",
            },
        ),
        Vector(
            "doc-c-1",
            [0.4, 0.0],
            attributes={
                "tenant": "keep",
                "doc_id": "doc-c",
                "category": "gamma",
                "tags": ["outside"],
                "content": "plain text",
            },
        ),
        Vector(
            "rerank-near",
            [10.0, 0.0],
            attributes={
                "tenant": "keep",
                "doc_id": "doc-r",
                "category": "zeta",
                "tags": ["rerank"],
                "content": "plain text",
            },
        ),
    ]


def summarize_py(response: Any) -> dict[str, Any]:
    return {
        "ids": [result.id for result in response.results],
        "scores": [result.score for result in response.results],
        "facets": response.facets,
    }


def query_typescript(namespace: str) -> dict[str, Any]:
    env = {
        **os.environ,
        "ZEPPELIN_TS_REPO": str(TS_REPO),
        "ZEPPELIN_URL": ZEPPELIN_URL,
        "ZEPPELIN_PARITY_NAMESPACE": namespace,
        "ZEPPELIN_PARITY_QUERY": json.dumps(PARITY_QUERY, sort_keys=True),
    }
    result = subprocess.run(
        ["node", str(ENGINE_ROOT / "contract" / "e2e" / "query-ts.mjs")],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )
    return json.loads(result.stdout)


def assert_same(py_result: dict[str, Any], ts_result: dict[str, Any]) -> None:
    if py_result["ids"] != ts_result["ids"]:
        raise AssertionError(
            "ranked id parity mismatch\n"
            f"python:     {py_result['ids']}\n"
            f"typescript: {ts_result['ids']}"
        )
    if py_result["facets"] != ts_result["facets"]:
        raise AssertionError(
            "facet parity mismatch\n"
            f"python:     {py_result['facets']}\n"
            f"typescript: {ts_result['facets']}"
        )
    for index, (py_score, ts_score) in enumerate(
        zip(py_result["scores"], ts_result["scores"], strict=True)
    ):
        if not math.isclose(py_score, ts_score, rel_tol=0.0, abs_tol=1e-6):
            raise AssertionError(
                "score parity mismatch at rank "
                f"{index}: python={py_score}, typescript={ts_score}"
            )


def main() -> None:
    namespace = f"parity-{uuid.uuid4().hex[:12]}"
    with ZeppelinClient(ZEPPELIN_URL, timeout=60.0) as client:
        client.create_namespace(
            namespace,
            2,
            distance_metric="euclidean",
            full_text_search={
                "content": FtsFieldConfig(
                    stemming=False,
                    remove_stopwords=False,
                )
            },
            index_config={"nlist": 4, "quantization": "none"},
        )
        try:
            client.upsert_vectors(namespace, canonical_vectors())
            py_result = summarize_py(client.query(namespace, **PARITY_QUERY))
            ts_result = query_typescript(namespace)
            assert_same(py_result, ts_result)
        finally:
            client.delete_namespace(namespace)

    print(
        json.dumps(
            {
                "namespace": namespace,
                "ids": py_result["ids"],
                "scores": py_result["scores"],
                "facets": py_result["facets"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
