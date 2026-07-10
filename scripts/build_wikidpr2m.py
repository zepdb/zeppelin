#!/usr/bin/env python3
"""Build a deterministic wiki_dpr_e5 prefix with exact sliced ground truth.

The source files are the immutable Parquet objects published for Elastic's
wiki-dpr-e5-768 benchmark.  Corpus extraction uses HTTP range reads and a
column projection, so the 85 GB corpus object is never downloaded in full.

The output layout matches zeppelin-devbench's raw dataset loader:

    corpus_vectors.f32
    corpus_ids.txt
    query_vectors.f32
    query_ids.txt
    ground_truth_top100.u32
    meta.json

All vectors are explicitly L2-normalized and written as little-endian f32.
Ground truth is recomputed against the selected prefix; the full-corpus
``closest_ids`` column is deliberately never read.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Iterator, Sequence


GIB = 1024**3
MIB = 1024**2
MIN_FREE_BYTES = 8 * GIB
DIMENSIONS = 768
DEFAULT_CORPUS_ROWS = 2_000_000
ALLOWED_CORPUS_ROWS = (1_000_000, 2_000_000)
DEFAULT_QUERY_ROWS = 1_000
GROUND_TRUTH_K = 100

DEFAULT_DATA_ROOT = Path(
    "/Users/aghatage/Documents/code/zeppelin-devbench/data"
)


@dataclass(frozen=True)
class RemoteSource:
    name: str
    url: str
    generation: str
    content_length: int
    crc32c: str
    md5: str | None
    rows: int


DATA_SOURCE = RemoteSource(
    name="data",
    url=(
        "https://storage.googleapis.com/elastic-benchmark-datasets/"
        "wiki-dpr-e5-768/data.parquet?generation=1778486380300287"
    ),
    generation="1778486380300287",
    content_length=85_227_520_544,
    crc32c="aKpK4A==",
    md5=None,
    rows=21_015_300,
)

QUERIES_SOURCE = RemoteSource(
    name="queries",
    url=(
        "https://storage.googleapis.com/elastic-benchmark-datasets/"
        "wiki-dpr-e5-768/queries.parquet?generation=1778486223485507"
    ),
    generation="1778486223485507",
    content_length=58_962_846,
    crc32c="5kCzNg==",
    md5="H1ME9B2983rxbOyiL9TUeg==",
    rows=10_000,
)


class DatasetBuildError(RuntimeError):
    """A fatal dataset integrity or build error."""


@dataclass(frozen=True)
class ArtifactInfo:
    bytes: int
    sha256: str

    def as_json(self) -> dict[str, int | str]:
        return {"bytes": self.bytes, "sha256": self.sha256}


@dataclass(frozen=True)
class SourceInspection:
    source: RemoteSource
    schema: dict[str, str]
    row_groups: int
    prefix_row_groups: tuple[int, ...]
    prefix_rows: int
    projected_compressed_bytes: int

    def as_json(self) -> dict[str, Any]:
        return {
            "url": self.source.url,
            "generation": self.source.generation,
            "content_length": self.source.content_length,
            "crc32c": self.source.crc32c,
            "md5": self.source.md5,
            "rows": self.source.rows,
            "row_groups": self.row_groups,
            "schema": self.schema,
            "prefix_row_group_first": self.prefix_row_groups[0],
            "prefix_row_group_count": len(self.prefix_row_groups),
            "prefix_rows": self.prefix_rows,
            "projected_compressed_bytes": self.projected_compressed_bytes,
        }


@dataclass(frozen=True)
class ExtractionStats:
    rows: int
    input_norm_min: float
    input_norm_max: float
    output_norm_max_abs_error: float

    def as_json(self) -> dict[str, int | float]:
        return {
            "rows": self.rows,
            "input_norm_min": self.input_norm_min,
            "input_norm_max": self.input_norm_max,
            "output_norm_max_abs_error": self.output_norm_max_abs_error,
        }


class HashedWriter:
    """Buffered, fsyncing writer that hashes exactly the persisted bytes."""

    def __init__(self, path: Path, buffer_bytes: int = 8 * MIB) -> None:
        self.path = path
        self._file: BinaryIO = path.open("xb", buffering=buffer_bytes)
        self._hasher = hashlib.sha256()
        self._bytes = 0
        self._closed = False
        self._info: ArtifactInfo | None = None

    def write(self, data: bytes) -> None:
        if self._closed:
            raise DatasetBuildError(f"attempted write after closing {self.path}")
        written = self._file.write(data)
        if written != len(data):
            raise DatasetBuildError(
                f"short write to {self.path}: wrote {written} of {len(data)} bytes"
            )
        self._hasher.update(data)
        self._bytes += written

    def close(self) -> ArtifactInfo:
        if not self._closed:
            self._file.flush()
            os.fsync(self._file.fileno())
            self._file.close()
            self._closed = True
            actual_bytes = self.path.stat().st_size
            if actual_bytes != self._bytes:
                raise DatasetBuildError(
                    f"size changed while writing {self.path}: "
                    f"tracked {self._bytes}, stat reports {actual_bytes}"
                )
            self._info = ArtifactInfo(
                bytes=self._bytes,
                sha256=self._hasher.hexdigest(),
            )
        if self._info is None:
            raise DatasetBuildError(f"writer for {self.path} closed without metadata")
        return self._info

    def __enter__(self) -> "HashedWriter":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()


# Third-party modules are imported only after CLI parsing and the disk-space
# preflight.  This also lets us set deterministic thread counts before NumPy
# initializes its BLAS backend.
np: Any = None
pa: Any = None
pc: Any = None
pq: Any = None
fsspec: Any = None


def import_dependencies(workers: int) -> None:
    global np, pa, pc, pq, fsspec

    thread_count = str(workers)
    for variable in (
        "VECLIB_MAXIMUM_THREADS",
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
    ):
        os.environ[variable] = thread_count

    try:
        import fsspec as loaded_fsspec
        import numpy as loaded_numpy
        import pyarrow as loaded_pyarrow
        import pyarrow.compute as loaded_compute
        import pyarrow.parquet as loaded_parquet
    except ImportError as error:
        raise DatasetBuildError(
            "required existing Python package is missing; expected "
            "numpy, pyarrow, fsspec, and aiohttp to be installed"
        ) from error

    np = loaded_numpy
    pa = loaded_pyarrow
    pc = loaded_compute
    pq = loaded_parquet
    fsspec = loaded_fsspec
    pa.set_cpu_count(workers)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a normalized deterministic wiki_dpr_e5 prefix and "
            "brute-force exact cosine top-100 ground truth."
        )
    )
    parser.add_argument(
        "--corpus-rows",
        type=int,
        choices=ALLOWED_CORPUS_ROWS,
        default=DEFAULT_CORPUS_ROWS,
        help="deterministic corpus prefix size (default: 2000000)",
    )
    parser.add_argument(
        "--query-rows",
        type=int,
        default=DEFAULT_QUERY_ROWS,
        help="must be exactly 1000 for the Phase 1 gate",
    )
    parser.add_argument(
        "--gt-k",
        type=int,
        default=GROUND_TRUTH_K,
        help="must be exactly 100 for the Phase 1 gate",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help=(
            "output directory; defaults to zeppelin-devbench/data/"
            "wikidpr2m (or wikidpr1m for a 1M slice)"
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=min(16, os.cpu_count() or 1),
        help="fixed NumPy/Arrow worker count (default: min(16, CPU count))",
    )
    parser.add_argument(
        "--extract-batch-rows",
        type=int,
        default=8_000,
        help="Parquet extraction batch size (default: 8000)",
    )
    parser.add_argument(
        "--corpus-block-rows",
        type=int,
        default=32_768,
        help="brute-force corpus score block size (default: 32768)",
    )
    parser.add_argument(
        "--query-block-rows",
        type=int,
        default=64,
        help="brute-force query score block size (default: 64)",
    )
    parser.add_argument(
        "--http-block-mib",
        type=int,
        default=16,
        help="in-memory HTTP range-read cache block size (default: 16 MiB)",
    )
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument(
        "--dry-run",
        action="store_true",
        help="perform local validation and print the plan; no network or writes",
    )
    modes.add_argument(
        "--metadata-only",
        action="store_true",
        help=(
            "after the disk preflight, range-read only Parquet metadata and "
            "print the validated source plan; do not write dataset files"
        ),
    )
    args = parser.parse_args(argv)

    if args.query_rows != DEFAULT_QUERY_ROWS:
        parser.error("--query-rows must be exactly 1000")
    if args.gt_k != GROUND_TRUTH_K:
        parser.error("--gt-k must be exactly 100")
    for name in (
        "workers",
        "extract_batch_rows",
        "corpus_block_rows",
        "query_block_rows",
        "http_block_mib",
    ):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")

    if args.out is None:
        suffix = "wikidpr2m" if args.corpus_rows == 2_000_000 else "wikidpr1m"
        args.out = DEFAULT_DATA_ROOT / suffix
    args.out = args.out.expanduser().resolve()
    return args


def nearest_existing_ancestor(path: Path) -> Path:
    current = path
    while not current.exists():
        parent = current.parent
        if parent == current:
            raise DatasetBuildError(
                f"cannot find an existing ancestor for output path {path}"
            )
        current = parent
    if not current.is_dir():
        current = current.parent
    return current


def preflight(args: argparse.Namespace) -> dict[str, int | str]:
    if args.out.exists():
        raise DatasetBuildError(
            f"output path already exists; refusing to overwrite: {args.out}"
        )

    filesystem_path = nearest_existing_ancestor(args.out.parent)
    usage = shutil.disk_usage(filesystem_path)
    known_output_bytes = (
        args.corpus_rows * DIMENSIONS * 4
        + args.query_rows * DIMENSIONS * 4
        + args.query_rows * args.gt_k * 4
    )
    if usage.free < MIN_FREE_BYTES:
        raise DatasetBuildError(
            "disk preflight failed before network access: "
            f"{filesystem_path} has {usage.free / GIB:.2f} GiB free, "
            f"but at least {MIN_FREE_BYTES / GIB:.0f} GiB is required; "
            f"known binary outputs alone require "
            f"{known_output_bytes / GIB:.2f} GiB"
        )

    return {
        "filesystem_path": str(filesystem_path),
        "free_bytes": usage.free,
        "minimum_free_bytes": MIN_FREE_BYTES,
        "known_output_bytes": known_output_bytes,
    }


def local_plan(args: argparse.Namespace, disk: dict[str, int | str]) -> dict[str, Any]:
    return {
        "mode": (
            "dry-run"
            if args.dry_run
            else "metadata-only"
            if args.metadata_only
            else "build"
        ),
        "output": str(args.out),
        "corpus_rows": args.corpus_rows,
        "query_rows": args.query_rows,
        "dimensions": DIMENSIONS,
        "ground_truth_k": args.gt_k,
        "workers": args.workers,
        "extract_batch_rows": args.extract_batch_rows,
        "corpus_block_rows": args.corpus_block_rows,
        "query_block_rows": args.query_block_rows,
        "http_block_bytes": args.http_block_mib * MIB,
        "disk": disk,
        "sources": {
            "data": remote_source_json(DATA_SOURCE),
            "queries": remote_source_json(QUERIES_SOURCE),
        },
    }


def remote_source_json(source: RemoteSource) -> dict[str, Any]:
    return {
        "url": source.url,
        "generation": source.generation,
        "content_length": source.content_length,
        "crc32c": source.crc32c,
        "md5": source.md5,
        "rows": source.rows,
    }


@contextmanager
def open_remote_parquet(
    source: RemoteSource,
    http_block_bytes: int,
) -> Iterator[tuple[Any, int]]:
    # ``readahead`` is fsspec's in-memory range cache.  No simplecache or
    # filecache layer is used, so source Parquet bytes never land on disk.
    filesystem = fsspec.filesystem("http", block_size=http_block_bytes)
    with filesystem.open(
        source.url,
        "rb",
        block_size=http_block_bytes,
        cache_type="readahead",
    ) as remote_file:
        remote_size = remote_file.size
        if remote_size is None:
            raise DatasetBuildError(f"server did not report size for {source.url}")
        if remote_size != source.content_length:
            raise DatasetBuildError(
                f"{source.name} source size changed: expected "
                f"{source.content_length}, got {remote_size}"
            )
        yield pq.ParquetFile(remote_file), remote_size


def validate_source_schema(source: RemoteSource, schema: Any) -> dict[str, str]:
    fields = {field.name: field for field in schema}
    expected_names = (
        ("id", "text", "title", "embedding")
        if source is DATA_SOURCE
        else ("id", "query", "embedding", "passage", "closest_ids")
    )
    missing = [name for name in expected_names if name not in fields]
    if missing:
        raise DatasetBuildError(
            f"{source.name} Parquet schema is missing fields: {missing}"
        )
    if not pa.types.is_int64(fields["id"].type):
        raise DatasetBuildError(
            f"{source.name}.id must be int64, got {fields['id'].type}"
        )

    embedding_type = fields["embedding"].type
    if not (
        pa.types.is_list(embedding_type)
        or pa.types.is_large_list(embedding_type)
        or pa.types.is_fixed_size_list(embedding_type)
    ):
        raise DatasetBuildError(
            f"{source.name}.embedding must be a list, got {embedding_type}"
        )
    value_type = embedding_type.value_type
    expected_value_type = pa.float32() if source is DATA_SOURCE else pa.float64()
    if value_type != expected_value_type:
        raise DatasetBuildError(
            f"{source.name}.embedding values must be {expected_value_type}, "
            f"got {value_type}"
        )

    if source is QUERIES_SOURCE:
        closest_type = fields["closest_ids"].type
        if not (
            pa.types.is_list(closest_type)
            or pa.types.is_large_list(closest_type)
        ) or not pa.types.is_string(closest_type.value_type):
            raise DatasetBuildError(
                "queries.closest_ids must be list<string>, got "
                f"{closest_type}"
            )

    return {name: str(fields[name].type) for name in expected_names}


def prefix_row_groups(metadata: Any, target_rows: int) -> tuple[int, ...]:
    selected: list[int] = []
    rows = 0
    for group_id in range(metadata.num_row_groups):
        group_rows = metadata.row_group(group_id).num_rows
        if rows + group_rows > target_rows:
            raise DatasetBuildError(
                f"corpus prefix {target_rows} would split row group {group_id}; "
                "the pinned source layout changed"
            )
        selected.append(group_id)
        rows += group_rows
        if rows == target_rows:
            break
    if rows != target_rows:
        raise DatasetBuildError(
            f"source has only {rows} whole-row-group prefix rows, "
            f"expected {target_rows}"
        )
    return tuple(selected)


def compressed_projection_bytes(
    metadata: Any,
    row_groups: Sequence[int],
    columns: set[str],
) -> int:
    total = 0
    for group_id in row_groups:
        group = metadata.row_group(group_id)
        for column_id in range(group.num_columns):
            column = group.column(column_id)
            top_level_name = column.path_in_schema.split(".", 1)[0]
            if top_level_name in columns:
                total += column.total_compressed_size
    return total


def inspect_source(
    source: RemoteSource,
    prefix_rows: int,
    http_block_bytes: int,
) -> SourceInspection:
    with open_remote_parquet(source, http_block_bytes) as (parquet, _):
        metadata = parquet.metadata
        if metadata.num_rows != source.rows:
            raise DatasetBuildError(
                f"{source.name} source row count changed: expected "
                f"{source.rows}, got {metadata.num_rows}"
            )
        schema = validate_source_schema(source, parquet.schema_arrow)

        if source is DATA_SOURCE:
            groups = prefix_row_groups(metadata, prefix_rows)
            projected_bytes = compressed_projection_bytes(
                metadata,
                groups,
                {"id", "embedding"},
            )
            selected_rows = sum(metadata.row_group(i).num_rows for i in groups)
        else:
            if metadata.num_row_groups != 1:
                raise DatasetBuildError(
                    "queries source must contain exactly one row group; got "
                    f"{metadata.num_row_groups}"
                )
            if prefix_rows > metadata.row_group(0).num_rows:
                raise DatasetBuildError(
                    f"queries source has fewer than {prefix_rows} rows"
                )
            groups = (0,)
            projected_bytes = compressed_projection_bytes(
                metadata,
                groups,
                {"id", "embedding"},
            )
            selected_rows = prefix_rows

        return SourceInspection(
            source=source,
            schema=schema,
            row_groups=metadata.num_row_groups,
            prefix_row_groups=groups,
            prefix_rows=selected_rows,
            projected_compressed_bytes=projected_bytes,
        )


def inspect_sources(args: argparse.Namespace) -> dict[str, SourceInspection]:
    block_bytes = args.http_block_mib * MIB
    print("validating pinned Parquet metadata", file=sys.stderr, flush=True)
    data = inspect_source(DATA_SOURCE, args.corpus_rows, block_bytes)
    queries = inspect_source(QUERIES_SOURCE, args.query_rows, block_bytes)
    return {"data": data, "queries": queries}


def vectors_from_batch(batch: Any, source_name: str) -> tuple[list[int], Any]:
    id_index = batch.schema.get_field_index("id")
    embedding_index = batch.schema.get_field_index("embedding")
    if id_index < 0 or embedding_index < 0:
        raise DatasetBuildError(
            f"{source_name} projected batch lacks id or embedding"
        )

    ids_array = batch.column(id_index)
    embeddings = batch.column(embedding_index)
    if ids_array.null_count != 0:
        raise DatasetBuildError(f"{source_name} contains null ids")
    if embeddings.null_count != 0:
        raise DatasetBuildError(f"{source_name} contains null embeddings")

    lengths_arrow = pc.list_value_length(embeddings)
    if lengths_arrow.null_count != 0:
        raise DatasetBuildError(
            f"{source_name} contains an embedding with unknown length"
        )
    lengths = lengths_arrow.to_numpy(zero_copy_only=False)
    if lengths.size != batch.num_rows or not np.all(lengths == DIMENSIONS):
        bad = np.flatnonzero(lengths != DIMENSIONS)
        first_bad = int(bad[0]) if bad.size else -1
        bad_length = int(lengths[first_bad]) if first_bad >= 0 else -1
        raise DatasetBuildError(
            f"{source_name} embedding row {first_bad} has dimension "
            f"{bad_length}, expected {DIMENSIONS}"
        )

    flattened = pc.list_flatten(embeddings)
    values = flattened.to_numpy(zero_copy_only=False)
    expected_values = batch.num_rows * DIMENSIONS
    if values.size != expected_values:
        raise DatasetBuildError(
            f"{source_name} flattened embedding count is {values.size}, "
            f"expected {expected_values}"
        )

    # Both source types are cast to f32 before normalization.  The query
    # Parquet stores doubles, but the bake-off consumes exactly the f32 bytes
    # written here.
    vectors = np.asarray(values, dtype=np.float32).reshape(
        batch.num_rows,
        DIMENSIONS,
    )
    if not np.isfinite(vectors).all():
        raise DatasetBuildError(f"{source_name} contains NaN or infinity")

    source_ids = ids_array.to_pylist()
    if any(type(value) is not int for value in source_ids):
        raise DatasetBuildError(f"{source_name} contains a non-integer id")
    return source_ids, vectors


def normalize_vectors(vectors: Any, source_name: str) -> tuple[Any, Any, float]:
    work = vectors.astype(np.float64, copy=True)
    norms = np.sqrt(np.sum(work * work, axis=1, dtype=np.float64))
    if not np.isfinite(norms).all():
        raise DatasetBuildError(f"{source_name} produced a non-finite norm")
    if np.any(norms <= 0.0):
        first = int(np.flatnonzero(norms <= 0.0)[0])
        raise DatasetBuildError(
            f"{source_name} embedding row {first} has zero norm"
        )

    normalized = np.asarray(
        work / norms[:, np.newaxis],
        dtype=np.dtype("<f4"),
        order="C",
    )
    if not np.isfinite(normalized).all():
        raise DatasetBuildError(
            f"{source_name} normalization produced NaN or infinity"
        )
    output_norms = np.sqrt(
        np.sum(
            normalized.astype(np.float64) ** 2,
            axis=1,
            dtype=np.float64,
        )
    )
    max_abs_error = float(np.max(np.abs(output_norms - 1.0)))
    if max_abs_error > 1.0e-5:
        raise DatasetBuildError(
            f"{source_name} normalized f32 norm error {max_abs_error} "
            "exceeds 1e-5"
        )
    return normalized, norms, max_abs_error


def extract_vector_prefix(
    inspection: SourceInspection,
    target_rows: int,
    vector_path: Path,
    ids_path: Path,
    batch_rows: int,
    http_block_bytes: int,
) -> tuple[ArtifactInfo, ArtifactInfo, ExtractionStats]:
    vector_writer = HashedWriter(vector_path)
    ids_writer = HashedWriter(ids_path)
    seen_ids: set[int] = set()
    rows_written = 0
    input_norm_min = math.inf
    input_norm_max = -math.inf
    output_norm_max_abs_error = 0.0

    try:
        with open_remote_parquet(
            inspection.source,
            http_block_bytes,
        ) as (parquet, _):
            batches = parquet.iter_batches(
                batch_size=batch_rows,
                row_groups=list(inspection.prefix_row_groups),
                columns=["id", "embedding"],
                use_threads=True,
            )
            for batch in batches:
                if rows_written == target_rows:
                    break
                remaining = target_rows - rows_written
                if batch.num_rows > remaining:
                    batch = batch.slice(0, remaining)

                source_ids, raw_vectors = vectors_from_batch(
                    batch,
                    inspection.source.name,
                )
                duplicate = next(
                    (source_id for source_id in source_ids if source_id in seen_ids),
                    None,
                )
                if duplicate is not None:
                    raise DatasetBuildError(
                        f"{inspection.source.name} contains duplicate id {duplicate}"
                    )
                if len(set(source_ids)) != len(source_ids):
                    raise DatasetBuildError(
                        f"{inspection.source.name} contains duplicate ids in a batch"
                    )
                seen_ids.update(source_ids)

                normalized, norms, max_abs_error = normalize_vectors(
                    raw_vectors,
                    inspection.source.name,
                )
                vector_writer.write(normalized.tobytes(order="C"))
                ids_writer.write(
                    ("".join(f"{source_id}\n" for source_id in source_ids)).encode(
                        "utf-8"
                    )
                )

                rows_written += batch.num_rows
                input_norm_min = min(input_norm_min, float(np.min(norms)))
                input_norm_max = max(input_norm_max, float(np.max(norms)))
                output_norm_max_abs_error = max(
                    output_norm_max_abs_error,
                    max_abs_error,
                )
                if (
                    rows_written % 100_000 == 0
                    or rows_written == target_rows
                    or target_rows <= 10_000
                ):
                    print(
                        f"{inspection.source.name}: extracted "
                        f"{rows_written}/{target_rows} rows",
                        file=sys.stderr,
                        flush=True,
                    )
    finally:
        vector_info = vector_writer.close()
        ids_info = ids_writer.close()

    if rows_written != target_rows:
        raise DatasetBuildError(
            f"{inspection.source.name} extraction wrote {rows_written} rows, "
            f"expected {target_rows}"
        )
    if len(seen_ids) != target_rows:
        raise DatasetBuildError(
            f"{inspection.source.name} has {len(seen_ids)} unique ids, "
            f"expected {target_rows}"
        )
    expected_vector_bytes = target_rows * DIMENSIONS * 4
    if vector_info.bytes != expected_vector_bytes:
        raise DatasetBuildError(
            f"{vector_path} is {vector_info.bytes} bytes, "
            f"expected {expected_vector_bytes}"
        )

    return (
        vector_info,
        ids_info,
        ExtractionStats(
            rows=rows_written,
            input_norm_min=input_norm_min,
            input_norm_max=input_norm_max,
            output_norm_max_abs_error=output_norm_max_abs_error,
        ),
    )


def block_top_k_exact(scores: Any, row_start: int, k: int) -> tuple[Any, Any]:
    query_rows, width = scores.shape
    take = min(k, width)
    output_scores = np.empty((query_rows, take), dtype=np.float32)
    output_rows = np.empty((query_rows, take), dtype=np.uint32)

    if width <= take:
        partitions = np.broadcast_to(
            np.arange(width, dtype=np.int64),
            (query_rows, width),
        )
    else:
        partitions = np.argpartition(scores, width - take, axis=1)[:, -take:]

    for query_index in range(query_rows):
        row_scores = scores[query_index]
        candidates = partitions[query_index]
        if width > take:
            threshold = np.min(row_scores[candidates])
            greater = np.flatnonzero(row_scores > threshold)
            needed = take - greater.size
            if needed < 0:
                raise DatasetBuildError(
                    "argpartition threshold admitted too many greater scores"
                )
            ties = np.flatnonzero(row_scores == threshold)
            # Row indices in a corpus block are already ascending, so taking
            # the first threshold ties implements the global row-ascending
            # secondary key exactly.
            selected = np.concatenate((greater, ties[:needed]))
        else:
            selected = candidates

        if selected.size != take:
            raise DatasetBuildError(
                f"selected {selected.size} block candidates, expected {take}"
            )
        selected_rows = selected.astype(np.uint64) + row_start
        selected_scores = row_scores[selected]
        order = np.lexsort((selected_rows, -selected_scores))
        output_scores[query_index] = selected_scores[order]
        output_rows[query_index] = selected_rows[order].astype(np.uint32)

    return output_scores, output_rows


def merge_top_k_exact(
    best_scores: Any | None,
    best_rows: Any | None,
    block_scores: Any,
    block_rows: Any,
    k: int,
) -> tuple[Any, Any]:
    if best_scores is None or best_rows is None:
        return block_scores, block_rows

    merged_scores = np.concatenate((best_scores, block_scores), axis=1)
    merged_rows = np.concatenate((best_rows, block_rows), axis=1)
    query_rows = merged_scores.shape[0]
    take = min(k, merged_scores.shape[1])
    output_scores = np.empty((query_rows, take), dtype=np.float32)
    output_rows = np.empty((query_rows, take), dtype=np.uint32)
    for query_index in range(query_rows):
        order = np.lexsort(
            (merged_rows[query_index], -merged_scores[query_index])
        )[:take]
        output_scores[query_index] = merged_scores[query_index, order]
        output_rows[query_index] = merged_rows[query_index, order]
    return output_scores, output_rows


def validate_ground_truth(best_scores: Any, best_rows: Any, corpus_rows: int) -> None:
    if best_rows.shape != (DEFAULT_QUERY_ROWS, GROUND_TRUTH_K):
        raise DatasetBuildError(
            f"ground truth shape is {best_rows.shape}, expected "
            f"({DEFAULT_QUERY_ROWS}, {GROUND_TRUTH_K})"
        )
    if not np.isfinite(best_scores).all():
        raise DatasetBuildError("ground truth contains a non-finite score")
    if np.any(best_rows >= corpus_rows):
        raise DatasetBuildError("ground truth contains an out-of-range row")

    for query_index in range(best_rows.shape[0]):
        rows = best_rows[query_index]
        scores = best_scores[query_index]
        if np.unique(rows).size != GROUND_TRUTH_K:
            raise DatasetBuildError(
                f"ground truth query {query_index} contains duplicate rows"
            )
        if np.any(scores[:-1] < scores[1:]):
            raise DatasetBuildError(
                f"ground truth query {query_index} is not score-descending"
            )
        ties = scores[:-1] == scores[1:]
        if np.any(rows[:-1][ties] > rows[1:][ties]):
            raise DatasetBuildError(
                f"ground truth query {query_index} violates row-ascending ties"
            )


def compute_ground_truth(
    corpus_path: Path,
    queries_path: Path,
    output_path: Path,
    corpus_rows: int,
    query_rows: int,
    k: int,
    corpus_block_rows: int,
    query_block_rows: int,
) -> tuple[ArtifactInfo, dict[str, Any]]:
    corpus = np.memmap(
        corpus_path,
        mode="r",
        dtype=np.dtype("<f4"),
        shape=(corpus_rows, DIMENSIONS),
    )
    queries = np.memmap(
        queries_path,
        mode="r",
        dtype=np.dtype("<f4"),
        shape=(query_rows, DIMENSIONS),
    )
    best_scores = np.empty((query_rows, 0), dtype=np.float32)
    best_rows = np.empty((query_rows, 0), dtype=np.uint32)
    block_count = math.ceil(corpus_rows / corpus_block_rows)
    started = time.monotonic()
    covered_rows = 0

    print(
        f"ground truth: scanning {corpus_rows} corpus rows for "
        f"{query_rows} queries in {block_count} blocks",
        file=sys.stderr,
        flush=True,
    )
    for block_index, corpus_start in enumerate(
        range(0, corpus_rows, corpus_block_rows),
        start=1,
    ):
        corpus_end = min(corpus_rows, corpus_start + corpus_block_rows)
        # A contiguous in-memory block avoids repeatedly faulting memmap pages
        # while each query block is scored. NumPy delegates matmul to the BLAS
        # backend configured with the fixed --workers count.
        corpus_block = np.ascontiguousarray(
            corpus[corpus_start:corpus_end],
            dtype=np.float32,
        )
        for query_start in range(0, query_rows, query_block_rows):
            query_end = min(query_rows, query_start + query_block_rows)
            query_block = np.ascontiguousarray(
                queries[query_start:query_end],
                dtype=np.float32,
            )
            scores = np.matmul(query_block, corpus_block.T)
            if scores.dtype != np.float32:
                raise DatasetBuildError(
                    f"NumPy matmul returned {scores.dtype}, expected float32"
                )
            if not np.isfinite(scores).all():
                raise DatasetBuildError(
                    f"non-finite score in corpus block {corpus_start}:{corpus_end}"
                )
            block_scores, block_rows = block_top_k_exact(
                scores,
                corpus_start,
                k,
            )
            if block_index == 1:
                merged_scores, merged_rows = block_scores, block_rows
            else:
                merged_scores, merged_rows = merge_top_k_exact(
                    best_scores[query_start:query_end],
                    best_rows[query_start:query_end],
                    block_scores,
                    block_rows,
                    k,
                )
            if block_index == 1 and query_start == 0:
                # All query blocks have the same width; allocate full result
                # arrays once after the first block's candidate count is known.
                best_scores = np.empty(
                    (query_rows, merged_scores.shape[1]),
                    dtype=np.float32,
                )
                best_rows = np.empty(
                    (query_rows, merged_rows.shape[1]),
                    dtype=np.uint32,
                )
            best_scores[query_start:query_end] = merged_scores
            best_rows[query_start:query_end] = merged_rows

        covered_rows += corpus_end - corpus_start
        elapsed = time.monotonic() - started
        rate = covered_rows / elapsed if elapsed > 0.0 else 0.0
        remaining = corpus_rows - covered_rows
        eta = remaining / rate if rate > 0.0 else math.inf
        print(
            f"ground truth: block {block_index}/{block_count}, "
            f"rows {covered_rows}/{corpus_rows}, elapsed {elapsed:.1f}s, "
            f"ETA {eta:.1f}s",
            file=sys.stderr,
            flush=True,
        )

    if covered_rows != corpus_rows:
        raise DatasetBuildError(
            f"ground truth scanned {covered_rows} rows, expected {corpus_rows}"
        )
    validate_ground_truth(best_scores, best_rows, corpus_rows)

    ground_truth = np.asarray(best_rows, dtype=np.dtype("<u4"), order="C")
    with HashedWriter(output_path) as writer:
        writer.write(ground_truth.tobytes(order="C"))
        artifact = writer.close()
    expected_bytes = query_rows * k * 4
    if artifact.bytes != expected_bytes:
        raise DatasetBuildError(
            f"{output_path} is {artifact.bytes} bytes, expected {expected_bytes}"
        )

    return artifact, {
        "algorithm": "blocked exhaustive NumPy float32 matrix multiplication",
        "distance": "cosine via dot product of stored normalized f32 vectors",
        "examined_corpus_rows_per_query": corpus_rows,
        "corpus_block_rows": corpus_block_rows,
        "query_block_rows": query_block_rows,
        "tie_break": "score descending, then corpus row index ascending",
        "output": "little-endian u32 corpus row indices",
    }


def write_meta(path: Path, meta: dict[str, Any]) -> ArtifactInfo:
    payload = (json.dumps(meta, indent=2, sort_keys=True) + "\n").encode("utf-8")
    with HashedWriter(path) as writer:
        writer.write(payload)
        return writer.close()


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def build_dataset(
    args: argparse.Namespace,
    disk: dict[str, int | str],
    inspections: dict[str, SourceInspection],
) -> None:
    args.out.parent.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(
        tempfile.mkdtemp(
            prefix=f".{args.out.name}.tmp-",
            dir=args.out.parent,
        )
    )
    completed = False
    try:
        artifacts: dict[str, ArtifactInfo] = {}
        corpus_info, corpus_ids_info, corpus_stats = extract_vector_prefix(
            inspections["data"],
            args.corpus_rows,
            temp_dir / "corpus_vectors.f32",
            temp_dir / "corpus_ids.txt",
            args.extract_batch_rows,
            args.http_block_mib * MIB,
        )
        artifacts["corpus_vectors.f32"] = corpus_info
        artifacts["corpus_ids.txt"] = corpus_ids_info

        query_info, query_ids_info, query_stats = extract_vector_prefix(
            inspections["queries"],
            args.query_rows,
            temp_dir / "query_vectors.f32",
            temp_dir / "query_ids.txt",
            min(args.extract_batch_rows, args.query_rows),
            args.http_block_mib * MIB,
        )
        artifacts["query_vectors.f32"] = query_info
        artifacts["query_ids.txt"] = query_ids_info

        ground_truth_info, ground_truth_meta = compute_ground_truth(
            temp_dir / "corpus_vectors.f32",
            temp_dir / "query_vectors.f32",
            temp_dir / "ground_truth_top100.u32",
            args.corpus_rows,
            args.query_rows,
            args.gt_k,
            args.corpus_block_rows,
            args.query_block_rows,
        )
        artifacts["ground_truth_top100.u32"] = ground_truth_info

        script_sha256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        meta = {
            "format_version": 1,
            "corpus_n": args.corpus_rows,
            "query_n": args.query_rows,
            "dims": DIMENSIONS,
            "metric": "cosine",
            "gt_k": args.gt_k,
            "slice": {
                "corpus": (
                    f"first {args.corpus_rows} rows in pinned data Parquet order"
                ),
                "queries": (
                    f"first {args.query_rows} rows in pinned queries Parquet order"
                ),
                "full_corpus_closest_ids_used": False,
            },
            "ids": {
                "corpus_ids.txt": "source data.id rendered as decimal UTF-8",
                "query_ids.txt": "source queries.id rendered as decimal UTF-8",
                "ground_truth": "zero-based row offsets into corpus_vectors.f32",
            },
            "normalization": {
                "input_cast": "cast each source coordinate to float32 first",
                "norm_accumulator": "float64",
                "output": "L2-normalized little-endian float32",
                "corpus": corpus_stats.as_json(),
                "queries": query_stats.as_json(),
            },
            "ground_truth": ground_truth_meta,
            "sources": {
                name: inspection.as_json()
                for name, inspection in inspections.items()
            },
            "artifacts": {
                name: info.as_json() for name, info in sorted(artifacts.items())
            },
            "builder": {
                "script": "scripts/build_wikidpr2m.py",
                "script_sha256": script_sha256,
                "python": sys.version.split()[0],
                "numpy": np.__version__,
                "pyarrow": pa.__version__,
                "fsspec": fsspec.__version__,
                "workers": args.workers,
                "blas_thread_environment": {
                    variable: os.environ[variable]
                    for variable in (
                        "VECLIB_MAXIMUM_THREADS",
                        "OMP_NUM_THREADS",
                        "OPENBLAS_NUM_THREADS",
                        "MKL_NUM_THREADS",
                        "NUMEXPR_NUM_THREADS",
                    )
                },
                "disk_preflight": {
                    "filesystem_path": disk["filesystem_path"],
                    "minimum_free_bytes": disk["minimum_free_bytes"],
                    "known_output_bytes": disk["known_output_bytes"],
                },
            },
        }
        meta_info = write_meta(temp_dir / "meta.json", meta)
        print(
            f"meta.json: {meta_info.bytes} bytes, sha256={meta_info.sha256}",
            file=sys.stderr,
            flush=True,
        )

        expected_names = {
            "corpus_vectors.f32",
            "corpus_ids.txt",
            "query_vectors.f32",
            "query_ids.txt",
            "ground_truth_top100.u32",
            "meta.json",
        }
        actual_names = {entry.name for entry in temp_dir.iterdir()}
        if actual_names != expected_names:
            raise DatasetBuildError(
                f"temporary dataset contains {sorted(actual_names)}, "
                f"expected {sorted(expected_names)}"
            )

        fsync_directory(temp_dir)
        if args.out.exists():
            raise DatasetBuildError(
                f"output appeared during build; refusing to overwrite: {args.out}"
            )
        os.rename(temp_dir, args.out)
        fsync_directory(args.out.parent)
        completed = True
        print(f"dataset finalized atomically at {args.out}", flush=True)
    finally:
        if not completed and temp_dir.exists():
            shutil.rmtree(temp_dir)


def run(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    # This is intentionally before dependency import, fsspec construction, or
    # any operation that could contact a remote server.
    disk = preflight(args)
    plan = local_plan(args, disk)

    if args.dry_run:
        print(json.dumps(plan, indent=2, sort_keys=True))
        return 0

    import_dependencies(args.workers)
    inspections = inspect_sources(args)
    inspected_plan = {
        **plan,
        "validated_sources": {
            name: inspection.as_json()
            for name, inspection in inspections.items()
        },
    }
    if args.metadata_only:
        print(json.dumps(inspected_plan, indent=2, sort_keys=True))
        return 0

    build_dataset(args, disk, inspections)
    return 0


def main() -> None:
    try:
        raise SystemExit(run())
    except DatasetBuildError as error:
        print(f"build_wikidpr2m.py: {error}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
