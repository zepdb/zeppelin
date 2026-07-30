"""Generate small cross-language fixtures for the Phase-1 FDE kernel."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt

from fde_reference import (
    Algorithm,
    FdeTransform,
    FinalProjection,
    InnerProjection,
    deserialize_transform,
    encode_document,
    encode_query,
    max_sim,
    serialize_transform,
    simhash_bucket,
)


FIXTURE_SEED = 20_260_729
FIXTURE_PATH = (
    Path(__file__).resolve().parents[2]
    / "tests"
    / "fixtures"
    / "mmli"
    / "fde_fixtures.json"
)


def _empty_f32() -> npt.NDArray[np.float32]:
    return np.empty((0,), dtype=np.float32)


def _empty_u32() -> npt.NDArray[np.uint32]:
    return np.empty((0,), dtype=np.uint32)


def _materialize_transform(
    rng: np.random.Generator,
    *,
    algorithm: Algorithm,
    repetitions: int,
    simhash_bits: int,
    input_dimension: int,
    inner: InnerProjection,
    d_proj: int = 0,
    final: FinalProjection = FinalProjection.NONE,
    d_final: int = 0,
    simhash_planes: npt.ArrayLike | None = None,
) -> FdeTransform:
    if simhash_planes is None:
        planes = rng.standard_normal(
            (repetitions, simhash_bits, input_dimension),
            dtype=np.float32,
        )
    else:
        planes = np.asarray(simhash_planes, dtype=np.float32)

    if inner is InnerProjection.IDENTITY:
        inner_matrix = _empty_f32()
    elif inner is InnerProjection.RADEMACHER:
        scale = np.float32(
            np.float32(1.0) / np.sqrt(np.float32(d_proj))
        )
        signs = rng.integers(
            0,
            2,
            size=(repetitions, d_proj, input_dimension),
            dtype=np.int8,
        )
        inner_matrix = np.where(signs == 0, -scale, scale).astype(np.float32)
    elif inner is InnerProjection.AMS_SKETCH:
        inner_matrix = np.zeros(
            (repetitions, d_proj, input_dimension),
            dtype=np.float32,
        )
        for repetition in range(repetitions):
            for input_coordinate in range(input_dimension):
                target = int(rng.integers(0, d_proj))
                sign = np.float32(
                    -1.0 if int(rng.integers(0, 2)) == 0 else 1.0
                )
                inner_matrix[repetition, target, input_coordinate] = sign
    else:
        raise AssertionError(f"unknown inner projection: {inner}")

    bucket_count = 1 << simhash_bits
    projected_dimension = (
        input_dimension if inner is InnerProjection.IDENTITY else d_proj
    )
    pre_final_dimension = (
        repetitions * bucket_count * projected_dimension
    )
    if final is FinalProjection.NONE:
        count_targets = _empty_u32()
        count_signs = _empty_f32()
    elif final is FinalProjection.COUNT_SKETCH:
        count_targets = rng.integers(
            0,
            d_final,
            size=pre_final_dimension,
            dtype=np.uint32,
        )
        count_signs = np.where(
            rng.integers(0, 2, size=pre_final_dimension, dtype=np.int8) == 0,
            np.float32(-1.0),
            np.float32(1.0),
        ).astype(np.float32)
    else:
        raise AssertionError(f"unknown final projection: {final}")

    transform = FdeTransform(
        algorithm=algorithm,
        repetitions=repetitions,
        simhash_bits=simhash_bits,
        input_dimension=input_dimension,
        inner=inner,
        d_proj=d_proj,
        final=final,
        d_final=d_final,
        simhash_planes=np.ascontiguousarray(planes, dtype=np.float32),
        inner_matrix=np.ascontiguousarray(inner_matrix, dtype=np.float32),
        count_targets=np.ascontiguousarray(count_targets, dtype=np.uint32),
        count_signs=np.ascontiguousarray(count_signs, dtype=np.float32),
    )
    transform.validate()
    return transform


def _matrix_payload(matrix: npt.NDArray[np.float32]) -> dict[str, Any]:
    return {
        "values": [float(value) for value in matrix.reshape(-1)],
        "vector_count": int(matrix.shape[0]),
        "vector_dimension": int(matrix.shape[1]),
    }


def _query_empty_coordinates(
    transform: FdeTransform,
    query: npt.NDArray[np.float32],
    document_fde: npt.NDArray[np.float32],
) -> list[int]:
    # CountSketch erases the pre-final block coordinates, so it has no stable
    # "empty bucket coordinate" to expose in the fixture.
    if transform.final is FinalProjection.COUNT_SKETCH:
        return []

    coordinates: list[int] = []
    block_dimension = transform.projected_dimension
    repetition_stride = transform.bucket_count * block_dimension
    for repetition in range(transform.repetitions):
        populated = {
            simhash_bucket(transform, row, repetition) for row in query
        }
        for bucket in range(transform.bucket_count):
            if bucket in populated:
                continue
            block_start = (
                repetition * repetition_stride + bucket * block_dimension
            )
            for coordinate in range(
                block_start,
                block_start + block_dimension,
            ):
                # Rust's invariant assertion also checks that the document
                # coordinate demonstrates a non-zero filled value.
                if document_fde[coordinate] != np.float32(0.0):
                    coordinates.append(coordinate)
    return coordinates


def _fixture(
    name: str,
    transform: FdeTransform,
    query: npt.NDArray[np.float32],
    document: npt.NDArray[np.float32],
) -> dict[str, Any]:
    query_fde = encode_query(transform, query)
    document_fde = encode_document(transform, document)
    score = max_sim(query, document)
    return {
        "name": name,
        "transform_hex": serialize_transform(transform).hex(),
        "query": _matrix_payload(query),
        "document": _matrix_payload(document),
        "expected_query_fde": [float(value) for value in query_fde],
        "expected_document_fde": [
            float(value) for value in document_fde
        ],
        "expected_maxsim": float(score),
        "query_zero_coordinates": _query_empty_coordinates(
            transform,
            query,
            document_fde,
        ),
    }


def build_fixtures() -> dict[str, Any]:
    """Build four cases spanning every Phase-1 transform operation."""

    rng = np.random.default_rng(FIXTURE_SEED)

    paper_one_row = _materialize_transform(
        rng,
        algorithm=Algorithm.PAPER_V1,
        repetitions=2,
        simhash_bits=2,
        input_dimension=3,
        inner=InnerProjection.RADEMACHER,
        d_proj=2,
    )
    one_row_query = np.asarray(
        [[1.0, -0.5, 0.25], [-0.75, 0.5, 1.25]],
        dtype=np.float32,
    )
    one_row_document = np.asarray(
        [[0.5, -1.0, 2.0]],
        dtype=np.float32,
    )

    # Rows 0 and 1 hash to bucket 01; row 2 hashes to bucket 10.
    # Empty bucket 00 is distance one from both, so bucket 01 wins.
    # The filled value comes from row 0, not the populated-bucket average.
    tie_transform = _materialize_transform(
        rng,
        algorithm=Algorithm.PAPER_V1,
        repetitions=1,
        simhash_bits=2,
        input_dimension=2,
        inner=InnerProjection.RADEMACHER,
        d_proj=2,
        simhash_planes=np.asarray(
            [[[1.0, 0.0], [0.0, 1.0]]],
            dtype=np.float32,
        ),
    )
    tie_query = np.asarray([[1.0, 2.0]], dtype=np.float32)
    tie_document = np.asarray(
        [[-1.0, 2.0], [-2.0, 1.0], [1.0, -1.0]],
        dtype=np.float32,
    )

    reference_identity = _materialize_transform(
        rng,
        algorithm=Algorithm.REFERENCE_V1,
        repetitions=1,
        simhash_bits=2,
        input_dimension=2,
        inner=InnerProjection.IDENTITY,
        simhash_planes=np.asarray(
            [[[1.0, 0.0], [0.0, 1.0]]],
            dtype=np.float32,
        ),
    )
    negative_query = np.asarray(
        [[1.0, 0.0], [0.0, 1.0]],
        dtype=np.float32,
    )
    negative_document = np.asarray(
        [[-3.0, -1.0], [-1.0, -3.0]],
        dtype=np.float32,
    )

    reference_sketch = _materialize_transform(
        rng,
        algorithm=Algorithm.REFERENCE_V1,
        repetitions=2,
        simhash_bits=2,
        input_dimension=3,
        inner=InnerProjection.AMS_SKETCH,
        d_proj=2,
        final=FinalProjection.COUNT_SKETCH,
        d_final=5,
    )
    sketch_query = np.asarray(
        [[1.0, -1.0, 0.5], [-0.5, 0.25, 1.5]],
        dtype=np.float32,
    )
    sketch_document = np.asarray(
        [
            [0.75, -0.5, 1.0],
            [-1.25, 0.5, 0.25],
            [0.5, 1.25, -0.75],
        ],
        dtype=np.float32,
    )

    fixtures = [
        _fixture(
            "paper_rademacher_one_row_document",
            paper_one_row,
            one_row_query,
            one_row_document,
        ),
        _fixture(
            "paper_hamming_tie_lowest_bucket_then_row",
            tie_transform,
            tie_query,
            tie_document,
        ),
        _fixture(
            "reference_identity_negative_maxsim",
            reference_identity,
            negative_query,
            negative_document,
        ),
        _fixture(
            "reference_ams_with_countsketch",
            reference_sketch,
            sketch_query,
            sketch_document,
        ),
    ]
    _self_check(fixtures, tie_transform, tie_query, tie_document)
    return {"fixtures": fixtures}


def _self_check(
    fixtures: list[dict[str, Any]],
    tie_transform: FdeTransform,
    tie_query: npt.NDArray[np.float32],
    tie_document: npt.NDArray[np.float32],
) -> None:
    names = {fixture["name"] for fixture in fixtures}
    if len(fixtures) != 4 or len(names) != 4:
        raise AssertionError("expected four uniquely named fixtures")

    algorithms: set[Algorithm] = set()
    for fixture in fixtures:
        encoded = bytes.fromhex(fixture["transform_hex"])
        decoded = deserialize_transform(encoded)
        algorithms.add(decoded.algorithm)
        if serialize_transform(decoded) != encoded:
            raise AssertionError("transform bytes did not round-trip")

        query_payload = fixture["query"]
        document_payload = fixture["document"]
        query = np.asarray(query_payload["values"], dtype=np.float32).reshape(
            query_payload["vector_count"],
            query_payload["vector_dimension"],
        )
        document = np.asarray(
            document_payload["values"],
            dtype=np.float32,
        ).reshape(
            document_payload["vector_count"],
            document_payload["vector_dimension"],
        )
        np.testing.assert_array_equal(
            encode_query(decoded, query),
            np.asarray(fixture["expected_query_fde"], dtype=np.float32),
        )
        np.testing.assert_array_equal(
            encode_document(decoded, document),
            np.asarray(fixture["expected_document_fde"], dtype=np.float32),
        )
        np.testing.assert_equal(
            max_sim(query, document),
            np.float32(fixture["expected_maxsim"]),
        )

    if algorithms != {Algorithm.PAPER_V1, Algorithm.REFERENCE_V1}:
        raise AssertionError("fixtures must contain both algorithm variants")

    tie_buckets = [
        simhash_bucket(tie_transform, row, 0) for row in tie_document
    ]
    if tie_buckets != [1, 1, 2]:
        raise AssertionError(f"unexpected Hamming fixture buckets: {tie_buckets}")
    if simhash_bucket(tie_transform, tie_query[0], 0) != 3:
        raise AssertionError("tie query must occupy bucket 3")
    tie_document_fde = encode_document(tie_transform, tie_document)
    first_row_projection = np.asarray(
        [
            np.sum(
                tie_document[0] * projection,
                dtype=np.float32,
            )
            for projection in tie_transform.inner_matrix[0]
        ],
        dtype=np.float32,
    )
    np.testing.assert_array_equal(
        tie_document_fde[: tie_transform.projected_dimension],
        first_row_projection,
    )

    negative = next(
        fixture
        for fixture in fixtures
        if fixture["name"] == "reference_identity_negative_maxsim"
    )
    if negative["expected_maxsim"] != -2.0:
        raise AssertionError("negative MaxSim fixture must score -2")


def _render(payload: dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, allow_nan=False) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail unless the committed fixture matches a fresh generation",
    )
    args = parser.parse_args()

    payload = build_fixtures()
    rendered = _render(payload)
    if args.check:
        if not FIXTURE_PATH.exists():
            raise SystemExit(f"fixture does not exist: {FIXTURE_PATH}")
        if FIXTURE_PATH.read_text(encoding="utf-8") != rendered:
            raise SystemExit("fde_fixtures.json is stale; rerun generator")
        action = "checked"
    else:
        FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
        FIXTURE_PATH.write_text(rendered, encoding="utf-8")
        action = "wrote"

    transform_bytes = sum(
        len(bytes.fromhex(fixture["transform_hex"]))
        for fixture in payload["fixtures"]
    )
    output_coordinates = sum(
        len(fixture["expected_query_fde"])
        for fixture in payload["fixtures"]
    )
    print(
        f"{action} {FIXTURE_PATH}: {len(payload['fixtures'])} cases, "
        f"{transform_bytes} transform bytes, "
        f"{output_coordinates} query FDE coordinates"
    )


if __name__ == "__main__":
    main()
