"""Independent NumPy reference for the two Phase-1 MUVERA FDE variants.

This module follows the scalar equations directly.  It intentionally does not
share transform generation or implementation code with the Rust kernel:
fixtures persist every plane, projection coefficient, and sketch assignment.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
import struct

import numpy as np
import numpy.typing as npt


FloatMatrix = npt.NDArray[np.float32]
IntVector = npt.NDArray[np.uint32]
FloatVector = npt.NDArray[np.float32]

TRANSFORM_MAGIC = b"ZFT1"
TRANSFORM_FORMAT_VERSION = 1
_HEADER = struct.Struct("<4sBBIIIBIBI")


class Algorithm(IntEnum):
    """Persisted algorithm tags."""

    PAPER_V1 = 1
    REFERENCE_V1 = 2


class InnerProjection(IntEnum):
    """Persisted per-bucket projection tags."""

    IDENTITY = 0
    RADEMACHER = 1
    AMS_SKETCH = 2


class FinalProjection(IntEnum):
    """Persisted post-concatenation projection tags."""

    NONE = 0
    COUNT_SKETCH = 1


@dataclass(frozen=True)
class FdeTransform:
    """One fully materialized transform; the arrays are its identity."""

    algorithm: Algorithm
    repetitions: int
    simhash_bits: int
    input_dimension: int
    inner: InnerProjection
    d_proj: int
    final: FinalProjection
    d_final: int
    simhash_planes: FloatMatrix
    inner_matrix: npt.NDArray[np.float32]
    count_targets: IntVector
    count_signs: FloatVector

    @property
    def bucket_count(self) -> int:
        """Number of SimHash buckets in each repetition."""

        return 1 << self.simhash_bits

    @property
    def projected_dimension(self) -> int:
        """Coordinate count in one bucket block."""

        if self.inner is InnerProjection.IDENTITY:
            return self.input_dimension
        return self.d_proj

    @property
    def pre_final_dimension(self) -> int:
        """FDE dimension after concatenation and before CountSketch."""

        return (
            self.repetitions
            * self.bucket_count
            * self.projected_dimension
        )

    @property
    def output_dimension(self) -> int:
        """Persisted FDE dimension."""

        if self.final is FinalProjection.COUNT_SKETCH:
            return self.d_final
        return self.pre_final_dimension

    def validate(self) -> None:
        """Reject malformed transforms before equations or serialization."""

        if self.repetitions <= 0:
            raise ValueError("repetitions must be positive")
        if self.simhash_bits < 0:
            raise ValueError("simhash_bits must be non-negative")
        if self.input_dimension <= 0:
            raise ValueError("input_dimension must be positive")
        expected_planes = (
            self.repetitions,
            self.simhash_bits,
            self.input_dimension,
        )
        if self.simhash_planes.shape != expected_planes:
            raise ValueError(
                "simhash plane shape mismatch: "
                f"expected {expected_planes}, got {self.simhash_planes.shape}"
            )
        _require_f32_finite("simhash_planes", self.simhash_planes)

        if self.inner is InnerProjection.IDENTITY:
            if self.d_proj != 0 or self.inner_matrix.size != 0:
                raise ValueError("identity projection must have no matrix")
        else:
            if self.d_proj <= 0 or self.d_proj > self.input_dimension:
                raise ValueError("d_proj must be in 1..=input_dimension")
            expected_inner = (
                self.repetitions,
                self.d_proj,
                self.input_dimension,
            )
            if self.inner_matrix.shape != expected_inner:
                raise ValueError(
                    "inner matrix shape mismatch: "
                    f"expected {expected_inner}, got {self.inner_matrix.shape}"
                )
            _require_f32_finite("inner_matrix", self.inner_matrix)

        if self.algorithm is Algorithm.PAPER_V1:
            if self.inner is not InnerProjection.RADEMACHER:
                raise ValueError("PaperV1 requires Rademacher projection")
            if self.final is not FinalProjection.NONE:
                raise ValueError("PaperV1 has no final projection")
        elif self.algorithm is Algorithm.REFERENCE_V1:
            if self.inner not in (
                InnerProjection.IDENTITY,
                InnerProjection.AMS_SKETCH,
            ):
                raise ValueError("ReferenceV1 requires identity or AMS")

        if self.final is FinalProjection.NONE:
            if (
                self.d_final != 0
                or self.count_targets.size != 0
                or self.count_signs.size != 0
            ):
                raise ValueError("no final projection must have no assignments")
        else:
            if self.d_final <= 0 or self.d_final > self.pre_final_dimension:
                raise ValueError("d_final is outside the valid dimension range")
            if self.count_targets.shape != (self.pre_final_dimension,):
                raise ValueError("CountSketch target count mismatch")
            if self.count_signs.shape != (self.pre_final_dimension,):
                raise ValueError("CountSketch sign count mismatch")
            if np.any(self.count_targets >= self.d_final):
                raise ValueError("CountSketch target is out of range")
            if np.any(
                (self.count_signs != np.float32(-1.0))
                & (self.count_signs != np.float32(1.0))
            ):
                raise ValueError("CountSketch signs must be exactly -1 or 1")


def _require_f32_finite(name: str, values: npt.NDArray[np.float32]) -> None:
    if values.dtype != np.dtype(np.float32):
        raise ValueError(f"{name} must have float32 dtype")
    if not np.all(np.isfinite(values)):
        raise ValueError(f"{name} contains a non-finite value")


def _checked_matrix(values: npt.ArrayLike, input_dimension: int) -> FloatMatrix:
    matrix = np.asarray(values, dtype=np.float32)
    if matrix.ndim != 2:
        raise ValueError("multi-vector input must be a two-dimensional matrix")
    if matrix.shape[0] == 0:
        raise ValueError("multi-vector input must contain at least one row")
    if matrix.shape[1] != input_dimension:
        raise ValueError(
            f"matrix dimension mismatch: expected {input_dimension}, "
            f"got {matrix.shape[1]}"
        )
    if not np.all(np.isfinite(matrix)):
        raise ValueError("multi-vector input contains a non-finite value")
    return np.ascontiguousarray(matrix, dtype=np.float32)


def _dot_f32(left: FloatVector, right: FloatVector) -> np.float32:
    """Evaluate a dot product in scalar row order with f32 rounding."""

    total = np.float32(0.0)
    for left_value, right_value in zip(left, right, strict=True):
        product = np.float32(left_value * right_value)
        total = np.float32(total + product)
    return total


def simhash_bucket(
    transform: FdeTransform,
    row: FloatVector,
    repetition: int,
) -> int:
    """Map one row to a bucket, shifting sign bits left in bit order."""

    bucket = 0
    for bit in range(transform.simhash_bits):
        plane = transform.simhash_planes[repetition, bit]
        bucket = (bucket << 1) | int(_dot_f32(row, plane) > 0.0)
    return bucket


def _project_rows(
    transform: FdeTransform,
    matrix: FloatMatrix,
    repetition: int,
) -> FloatMatrix:
    if transform.inner is InnerProjection.IDENTITY:
        return matrix.copy()

    projected = np.zeros(
        (matrix.shape[0], transform.projected_dimension),
        dtype=np.float32,
    )
    for row_index, row in enumerate(matrix):
        for output_index in range(transform.projected_dimension):
            projected[row_index, output_index] = _dot_f32(
                row,
                transform.inner_matrix[repetition, output_index],
            )
    return projected


def _encode(
    transform: FdeTransform,
    values: npt.ArrayLike,
    *,
    document: bool,
) -> FloatVector:
    transform.validate()
    matrix = _checked_matrix(values, transform.input_dimension)
    if document and np.any(np.all(matrix == np.float32(0.0), axis=1)):
        raise ValueError("document rows must be non-zero")

    repetitions = np.zeros(
        (
            transform.repetitions,
            transform.bucket_count,
            transform.projected_dimension,
        ),
        dtype=np.float32,
    )

    for repetition in range(transform.repetitions):
        projected_rows = _project_rows(transform, matrix, repetition)
        row_buckets = [
            simhash_bucket(transform, row, repetition) for row in matrix
        ]
        counts = np.zeros(transform.bucket_count, dtype=np.int64)
        first_rows: list[int | None] = [None] * transform.bucket_count

        for row_index, bucket in enumerate(row_buckets):
            counts[bucket] += 1
            if first_rows[bucket] is None:
                first_rows[bucket] = row_index
            for coordinate in range(transform.projected_dimension):
                repetitions[repetition, bucket, coordinate] = np.float32(
                    repetitions[repetition, bucket, coordinate]
                    + projected_rows[row_index, coordinate]
                )

        if not document:
            continue

        populated = [bucket for bucket, count in enumerate(counts) if count]
        for bucket in range(transform.bucket_count):
            if counts[bucket]:
                denominator = np.float32(counts[bucket])
                repetitions[repetition, bucket] = (
                    repetitions[repetition, bucket] / denominator
                ).astype(np.float32)
                continue

            source_bucket = min(
                populated,
                key=lambda candidate: (
                    (bucket ^ candidate).bit_count(),
                    candidate,
                ),
            )
            source_row = first_rows[source_bucket]
            if source_row is None:
                raise AssertionError("selected source bucket is not populated")
            repetitions[repetition, bucket] = projected_rows[source_row]

    concatenated = repetitions.reshape(-1)
    if transform.final is FinalProjection.NONE:
        return concatenated

    final = np.zeros(transform.d_final, dtype=np.float32)
    for source, value in enumerate(concatenated):
        target = int(transform.count_targets[source])
        final[target] = np.float32(
            final[target] + transform.count_signs[source] * value
        )
    return final


def encode_query(
    transform: FdeTransform,
    values: npt.ArrayLike,
) -> FloatVector:
    """Encode query rows as per-bucket sums; empty blocks stay zero."""

    return _encode(transform, values, document=False)


def encode_document(
    transform: FdeTransform,
    values: npt.ArrayLike,
) -> FloatVector:
    """Encode document rows as averages plus deterministic empty fill."""

    return _encode(transform, values, document=True)


def max_sim(query: npt.ArrayLike, document: npt.ArrayLike) -> np.float32:
    """Compute scalar asymmetric Chamfer similarity with first-row ties."""

    query_matrix = np.asarray(query, dtype=np.float32)
    document_matrix = np.asarray(document, dtype=np.float32)
    if query_matrix.ndim != 2 or document_matrix.ndim != 2:
        raise ValueError("MaxSim inputs must be two-dimensional")
    if query_matrix.shape[0] == 0 or document_matrix.shape[0] == 0:
        raise ValueError("MaxSim inputs must contain at least one row")
    if query_matrix.shape[1] != document_matrix.shape[1]:
        raise ValueError("MaxSim dimensions must match")
    if not np.all(np.isfinite(query_matrix)) or not np.all(
        np.isfinite(document_matrix)
    ):
        raise ValueError("MaxSim inputs must be finite")

    score = np.float32(0.0)
    for query_row in query_matrix:
        best = np.float32(-np.inf)
        for document_row in document_matrix:
            similarity = _dot_f32(query_row, document_row)
            if similarity > best:
                best = similarity
        score = np.float32(score + best)
    return score


def serialize_transform(transform: FdeTransform) -> bytes:
    """Serialize the exact Rust-facing ZFT1 little-endian format."""

    transform.validate()
    encoded = bytearray(
        _HEADER.pack(
            TRANSFORM_MAGIC,
            TRANSFORM_FORMAT_VERSION,
            int(transform.algorithm),
            transform.repetitions,
            transform.simhash_bits,
            transform.input_dimension,
            int(transform.inner),
            transform.d_proj,
            int(transform.final),
            transform.d_final,
        )
    )
    encoded.extend(_little_endian_f32(transform.simhash_planes))
    if transform.inner is not InnerProjection.IDENTITY:
        encoded.extend(_little_endian_f32(transform.inner_matrix))
    if transform.final is FinalProjection.COUNT_SKETCH:
        for target, sign in zip(
            transform.count_targets,
            transform.count_signs,
            strict=True,
        ):
            encoded.extend(struct.pack("<If", int(target), float(sign)))
    return bytes(encoded)


def deserialize_transform(encoded: bytes) -> FdeTransform:
    """Decode ZFT1 bytes for generator self-checking."""

    if len(encoded) < _HEADER.size:
        raise ValueError("truncated transform header")
    (
        magic,
        format_version,
        algorithm,
        repetitions,
        simhash_bits,
        input_dimension,
        inner,
        d_proj,
        final,
        d_final,
    ) = _HEADER.unpack_from(encoded)
    if magic != TRANSFORM_MAGIC or format_version != TRANSFORM_FORMAT_VERSION:
        raise ValueError("invalid transform header")

    cursor = _HEADER.size
    plane_count = repetitions * simhash_bits * input_dimension
    planes, cursor = _read_f32(
        encoded,
        cursor,
        plane_count,
        (repetitions, simhash_bits, input_dimension),
    )

    inner_kind = InnerProjection(inner)
    if inner_kind is InnerProjection.IDENTITY:
        inner_matrix = np.empty((0,), dtype=np.float32)
    else:
        inner_count = repetitions * d_proj * input_dimension
        inner_matrix, cursor = _read_f32(
            encoded,
            cursor,
            inner_count,
            (repetitions, d_proj, input_dimension),
        )

    final_kind = FinalProjection(final)
    if final_kind is FinalProjection.NONE:
        count_targets = np.empty((0,), dtype=np.uint32)
        count_signs = np.empty((0,), dtype=np.float32)
    else:
        projected_dimension = (
            input_dimension
            if inner_kind is InnerProjection.IDENTITY
            else d_proj
        )
        assignment_count = (
            repetitions * (1 << simhash_bits) * projected_dimension
        )
        byte_count = assignment_count * 8
        if cursor + byte_count > len(encoded):
            raise ValueError("truncated CountSketch assignments")
        count_targets = np.empty(assignment_count, dtype=np.uint32)
        count_signs = np.empty(assignment_count, dtype=np.float32)
        for index in range(assignment_count):
            target, sign = struct.unpack_from("<If", encoded, cursor)
            cursor += 8
            count_targets[index] = target
            count_signs[index] = sign

    if cursor != len(encoded):
        raise ValueError("trailing transform bytes")
    transform = FdeTransform(
        algorithm=Algorithm(algorithm),
        repetitions=repetitions,
        simhash_bits=simhash_bits,
        input_dimension=input_dimension,
        inner=inner_kind,
        d_proj=d_proj,
        final=final_kind,
        d_final=d_final,
        simhash_planes=planes,
        inner_matrix=inner_matrix,
        count_targets=count_targets,
        count_signs=count_signs,
    )
    transform.validate()
    return transform


def _little_endian_f32(values: npt.NDArray[np.float32]) -> bytes:
    return np.asarray(values, dtype="<f4").tobytes(order="C")


def _read_f32(
    encoded: bytes,
    cursor: int,
    count: int,
    shape: tuple[int, ...],
) -> tuple[npt.NDArray[np.float32], int]:
    byte_count = count * 4
    if cursor + byte_count > len(encoded):
        raise ValueError("truncated f32 transform payload")
    values = np.frombuffer(
        encoded,
        dtype="<f4",
        count=count,
        offset=cursor,
    ).astype(np.float32, copy=True)
    return values.reshape(shape), cursor + byte_count
