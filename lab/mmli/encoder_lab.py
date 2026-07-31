#!/usr/bin/env python3
"""Pinned, offline encoder qualification driver for MMLI-2 Phase 2."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import resource
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import torch
from datasets import Dataset, concatenate_datasets
from huggingface_hub import snapshot_download
from PIL import Image, ImageDraw
from safetensors.torch import load_file
from transformers import (
    AutoModel,
    AutoTokenizer,
    ColModernVBertProcessor,
    ModernVBertModel,
    ModernVBertPreTrainedModel,
)
from transformers.conversion_mapping import (
    get_checkpoint_conversion_mapping,
    register_checkpoint_conversion_mapping,
)
from transformers.core_model_loading import WeightRenaming

TEXT_REPO = "lightonai/GTE-ModernColBERT-v1"
TEXT_REVISION = "cbbe53366e564450558f5e639dd499171f127538"
SCIFACT_REPO = "BeIR/scifact"
SCIFACT_REVISION = "b3b5335604bf5ee3c4447671af975ea25143d4f5"
VISUAL_BASE_REPO = "ModernVBERT/colmodernvbert-base"
VISUAL_BASE_REVISION = "17604b47f51828a5e904557094552bf23fdd9fca"
VISUAL_ADAPTER_REPO = "ModernVBERT/colmodernvbert"
VISUAL_ADAPTER_REVISION = "810a3ed07222eed11376ec516a5744394c7e0a0b"
VIDORE = (
    (
        "hr",
        "vidore/vidore_v3_hr",
        "95f2f83a5a09590a89e34960479f9438e48bca77",
    ),
    (
        "computer_science",
        "vidore/vidore_v3_computer_science",
        "7b91f10e18b72a763dd17a0c05d66bf985b98f1d",
    ),
)
DIMENSION = 128
BATCH_SIZE = 8
PAIR_COUNT = 50
VISUAL_TASK_CAP = 1_000
PUNCTUATION = list('!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~')
DIAGNOSTIC_CANDIDATE_KS = (50, 100, 300, 500, 600, 700, 1_000, 2_000)
PRECISION_RANKING_BASELINES = (
    "f16",
    "int8_global_scale",
    "int8_per_row_calibrated",
)
PRECISION_RANKING_NEW_CANDIDATES = (
    "int8_per_row_calibrated_renormalized",
    "int8_symmetric_per_row_renormalized",
    "int8_groupwise_32_symmetric_renormalized",
    "int8_groupwise_16_symmetric_renormalized",
)
PRECISION_RANKING_ALL_CANDIDATES = (
    *PRECISION_RANKING_BASELINES,
    *PRECISION_RANKING_NEW_CANDIDATES,
)
PRODUCTION_INT8_GROUP_SIZES = (16, 32, 128)
DEFAULT_PRODUCTION_INT8_GROUP_SIZES = (32,)


@dataclass
class EncodingResult:
    matrices: list[np.ndarray]
    ids: list[str]
    batch_one_cpu_seconds_per_item: float
    batch_eight_cpu_seconds_per_item: float
    batch_eight_wall_seconds_per_item: float
    peak_rss_mib: float


class ColModernVBert(ModernVBertPreTrainedModel):
    """Official ColPali inference wrapper, kept local to forbid remote code."""

    _checkpoint_conversion_mapping = {
        r"^base_model\.model\.model\.text_model": "model.text_model",
        r"^base_model\.model\.custom_text_proj": "custom_text_proj",
    }

    def __init__(self, config, mask_non_image_embeddings: bool = False, **kwargs):
        super().__init__(config=config)
        self.model = ModernVBertModel(config, **kwargs)
        self.dim = DIMENSION
        self.custom_text_proj = torch.nn.Linear(
            self.model.config.text_config.hidden_size, self.dim
        )
        self.mask_non_image_embeddings = mask_non_image_embeddings
        self.main_input_name = "doc_input_ids"
        self.post_init()

    @classmethod
    def from_pretrained(cls, *args, **kwargs):
        key_mapping = kwargs.pop("key_mapping", None)
        if key_mapping is None:
            key_mapping = dict(
                getattr(super(), "_checkpoint_conversion_mapping", {})
            )
            key_mapping.update(cls._checkpoint_conversion_mapping)
        return super().from_pretrained(*args, **kwargs, key_mapping=key_mapping)

    def forward(self, *args, **kwargs):
        hidden = self.model(*args, **kwargs)[0]
        projected = self.custom_text_proj(hidden)
        projected = projected / projected.norm(dim=-1, keepdim=True).clamp_min(
            1.0e-12
        )
        projected = projected * kwargs["attention_mask"].unsqueeze(-1)
        if "pixel_values" in kwargs and self.mask_non_image_embeddings:
            image_mask = (
                kwargs["input_ids"] == self.config.image_token_id
            ).unsqueeze(-1)
            projected = projected * image_mask
        return projected


if get_checkpoint_conversion_mapping("modernvbert") is None:
    register_checkpoint_conversion_mapping(
        "modernvbert",
        [
            WeightRenaming(source_patterns=source, target_patterns=target)
            for source, target in ColModernVBert._checkpoint_conversion_mapping.items()
        ],
    )


def snapshot_path(hf_home: Path, repo_id: str, revision: str, repo_type: str) -> Path:
    prefix = "datasets" if repo_type == "dataset" else "models"
    encoded = repo_id.replace("/", "--")
    path = hf_home / "hub" / f"{prefix}--{encoded}" / "snapshots" / revision
    if not path.is_dir():
        raise FileNotFoundError(f"missing pinned snapshot: {path}")
    return path


def download_pins(hf_home: Path) -> None:
    jobs = (
        (VISUAL_BASE_REPO, VISUAL_BASE_REVISION, "model", None),
        (VISUAL_ADAPTER_REPO, VISUAL_ADAPTER_REVISION, "model", None),
        (
            TEXT_REPO,
            TEXT_REVISION,
            "model",
            ["*.json", "*.safetensors", "README.md"],
        ),
        (
            SCIFACT_REPO,
            SCIFACT_REVISION,
            "dataset",
            ["README.md", "corpus/*.parquet", "queries/*.parquet"],
        ),
        *(
            (
                repo,
                revision,
                "dataset",
                ["README.md", "corpus/*.parquet", "queries/*.parquet"],
            )
            for _, repo, revision in VIDORE
        ),
    )
    for repo_id, revision, repo_type, patterns in jobs:
        path = snapshot_download(
            repo_id=repo_id,
            revision=revision,
            repo_type=repo_type,
            cache_dir=hf_home / "hub",
            allow_patterns=patterns,
        )
        print(f"downloaded {repo_id}@{revision}: {path}", flush=True)


def require_offline() -> None:
    required = {"HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1"}
    for name, expected in required.items():
        if os.environ.get(name) != expected:
            raise RuntimeError(f"{name} must be {expected} for encoder execution")


def load_parquet_directory(path: Path) -> Dataset:
    shards = [
        Dataset.from_parquet(str(parquet))
        for parquet in sorted(path.glob("*.parquet"))
    ]
    if not shards:
        raise FileNotFoundError(f"no parquet shards under {path}")
    return shards[0] if len(shards) == 1 else concatenate_datasets(shards)


def peak_rss_mib() -> float:
    raw = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    divisor = 1024.0 * 1024.0 if platform.system() == "Darwin" else 1024.0
    return raw / divisor


def batches(values: Sequence, size: int) -> Iterable[Sequence]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def insert_prefix(values: torch.Tensor, prefix_id: int) -> torch.Tensor:
    prefix = torch.full(
        (values.shape[0], 1),
        prefix_id,
        dtype=values.dtype,
        device=values.device,
    )
    return torch.cat((values[:, :1], prefix, values[:, 1:]), dim=1)


class TextEncoder:
    def __init__(self, snapshot: Path):
        self.model = AutoModel.from_pretrained(
            snapshot,
            trust_remote_code=False,
            local_files_only=True,
            torch_dtype=torch.float32,
        ).eval()
        self.tokenizer = AutoTokenizer.from_pretrained(
            snapshot,
            trust_remote_code=False,
            local_files_only=True,
        )
        projection = load_file(snapshot / "1_Dense" / "model.safetensors")
        self.projection = torch.nn.Linear(768, DIMENSION, bias=False)
        self.projection.load_state_dict({"weight": projection["linear.weight"]})
        self.projection.eval()
        self.query_prefix_id = self.tokenizer.convert_tokens_to_ids("[Q] ")
        self.document_prefix_id = self.tokenizer.convert_tokens_to_ids("[D] ")
        if self.query_prefix_id == self.tokenizer.unk_token_id:
            raise RuntimeError("pinned tokenizer does not contain [Q] prefix")
        if self.document_prefix_id == self.tokenizer.unk_token_id:
            raise RuntimeError("pinned tokenizer does not contain [D] prefix")
        self.skip_ids = {
            self.tokenizer.convert_tokens_to_ids(value) for value in PUNCTUATION
        }

    def encode_batch(self, texts: Sequence[str], is_query: bool) -> list[np.ndarray]:
        limit = 47 if is_query else 299
        prefix_id = self.query_prefix_id if is_query else self.document_prefix_id
        tokens = self.tokenizer(
            list(texts),
            padding=True,
            truncation=True,
            max_length=limit,
            return_tensors="pt",
        )
        tokens["input_ids"] = insert_prefix(tokens["input_ids"], prefix_id)
        tokens["attention_mask"] = insert_prefix(tokens["attention_mask"], 1)
        if "token_type_ids" in tokens:
            tokens["token_type_ids"] = insert_prefix(tokens["token_type_ids"], 0)
        with torch.inference_mode():
            hidden = self.model(**tokens).last_hidden_state
            projected = torch.nn.functional.normalize(
                self.projection(hidden), p=2, dim=-1
            )
        results = []
        for index in range(projected.shape[0]):
            mask = tokens["attention_mask"][index].bool()
            if not is_query:
                for skip_id in self.skip_ids:
                    mask &= tokens["input_ids"][index] != skip_id
            matrix = projected[index][mask].cpu().numpy().astype(np.float32)
            if matrix.shape[0] == 0:
                raise RuntimeError("text encoder produced an empty matrix")
            results.append(matrix)
        return results


def timed_text_encoding(
    encoder: TextEncoder,
    texts: list[str],
    ids: list[str],
    is_query: bool,
) -> EncodingResult:
    probe_count = min(BATCH_SIZE, len(texts))
    start = time.process_time()
    for text in texts[:probe_count]:
        encoder.encode_batch([text], is_query)
    batch_one = (time.process_time() - start) / probe_count

    matrices: list[np.ndarray] = []
    start = time.process_time()
    wall_start = time.perf_counter()
    for ordinal, batch in enumerate(batches(texts, BATCH_SIZE), start=1):
        matrices.extend(encoder.encode_batch(batch, is_query))
        if ordinal % 25 == 0:
            print(
                f"text {'queries' if is_query else 'documents'}: "
                f"{len(matrices)}/{len(texts)}",
                flush=True,
            )
    batch_eight = (time.process_time() - start) / len(texts)
    batch_eight_wall = (time.perf_counter() - wall_start) / len(texts)
    return EncodingResult(
        matrices,
        ids,
        batch_one,
        batch_eight,
        batch_eight_wall,
        peak_rss_mib(),
    )


def text_encoding_once(
    encoder: TextEncoder,
    texts: list[str],
    ids: list[str],
    is_query: bool,
    progress_label: str = "int8 f32",
) -> EncodingResult:
    matrices: list[np.ndarray] = []
    for ordinal, batch in enumerate(batches(texts, BATCH_SIZE), start=1):
        matrices.extend(encoder.encode_batch(batch, is_query))
        if ordinal % 25 == 0:
            print(
                f"{progress_label} text "
                f"{'queries' if is_query else 'documents'}: "
                f"{len(matrices)}/{len(texts)}",
                flush=True,
            )
    return EncodingResult(matrices, ids, 0.0, 0.0, 0.0, peak_rss_mib())


def redirected_visual_adapter(adapter: Path, base: Path, destination: Path) -> Path:
    shutil.copytree(adapter, destination, symlinks=False)
    config_path = destination / "adapter_config.json"
    config = json.loads(config_path.read_text())
    config["base_model_name_or_path"] = str(base)
    config_path.write_text(json.dumps(config, indent=2) + "\n")
    return destination


def load_visual_model(base: Path, adapter: Path, scratch: Path):
    local_adapter = redirected_visual_adapter(
        adapter, base, scratch / "visual-adapter-local"
    )
    model = ColModernVBert.from_pretrained(
        local_adapter,
        trust_remote_code=False,
        local_files_only=True,
        torch_dtype=torch.float32,
    ).eval()
    lora_modules = [
        name for name, _ in model.named_modules() if "lora_A.default" in name
    ]
    if len(lora_modules) != 89:
        raise RuntimeError(f"expected 89 active LoRA modules, got {len(lora_modules)}")
    processor = ColModernVBertProcessor.from_pretrained(
        local_adapter,
        trust_remote_code=False,
        local_files_only=True,
    )
    return model, processor


def visual_batch(
    model,
    processor,
    values: Sequence[Image.Image | str],
    is_query: bool,
) -> list[np.ndarray]:
    inputs = (
        processor.process_queries(list(values), return_tensors="pt")
        if is_query
        else processor.process_images(list(values), return_tensors="pt")
    )
    with torch.inference_mode():
        embeddings = model(**inputs)
    results = []
    for index in range(embeddings.shape[0]):
        mask = inputs["attention_mask"][index].bool()
        matrix = embeddings[index][mask].cpu().numpy().astype(np.float32)
        if matrix.shape[0] == 0:
            raise RuntimeError("visual encoder produced an empty matrix")
        results.append(matrix)
    return results


def timed_visual_encoding(
    model,
    processor,
    values: list[Image.Image | str],
    ids: list[str],
    is_query: bool,
) -> EncodingResult:
    probe_count = min(BATCH_SIZE, len(values))
    start = time.process_time()
    for value in values[:probe_count]:
        visual_batch(model, processor, [value], is_query)
    batch_one = (time.process_time() - start) / probe_count

    matrices: list[np.ndarray] = []
    start = time.process_time()
    wall_start = time.perf_counter()
    for ordinal, batch in enumerate(batches(values, BATCH_SIZE), start=1):
        matrices.extend(visual_batch(model, processor, batch, is_query))
        if ordinal % 10 == 0:
            print(
                f"visual {'queries' if is_query else 'pages'}: "
                f"{len(matrices)}/{len(values)}",
                flush=True,
            )
    batch_eight = (time.process_time() - start) / len(values)
    batch_eight_wall = (time.perf_counter() - wall_start) / len(values)
    return EncodingResult(
        matrices,
        ids,
        batch_one,
        batch_eight,
        batch_eight_wall,
        peak_rss_mib(),
    )


def visual_encoding_once(
    model,
    processor,
    values: list[Image.Image | str],
    ids: list[str],
    is_query: bool,
    progress_label: str = "int8 f32",
) -> EncodingResult:
    matrices: list[np.ndarray] = []
    for ordinal, batch in enumerate(batches(values, BATCH_SIZE), start=1):
        matrices.extend(visual_batch(model, processor, batch, is_query))
        if ordinal % 10 == 0:
            print(
                f"{progress_label} visual "
                f"{'queries' if is_query else 'pages'}: "
                f"{len(matrices)}/{len(values)}",
                flush=True,
            )
    return EncodingResult(matrices, ids, 0.0, 0.0, 0.0, peak_rss_mib())


def write_tensor(
    prefix: Path, result: EncodingResult, dtype: str = "f16"
) -> tuple[Path, Path]:
    if dtype not in {"f16", "f32"}:
        raise RuntimeError(f"unsupported tensor dtype: {dtype}")
    raw_path = prefix.with_suffix(f".{dtype}")
    sidecar_path = prefix.with_suffix(".json")
    numpy_dtype = "<f2" if dtype == "f16" else "<f4"
    with raw_path.open("wb") as output:
        for matrix in result.matrices:
            np.asarray(matrix, dtype=numpy_dtype).tofile(output)
    sidecar = {
        "rows": [int(matrix.shape[0]) for matrix in result.matrices],
        "dim": DIMENSION,
        "dtype": dtype,
        "ids": result.ids,
    }
    sidecar_path.write_text(json.dumps(sidecar, separators=(",", ":")))
    return raw_path, sidecar_path


def read_tensor(prefix: Path, timing: dict) -> EncodingResult:
    sidecar_path = prefix.with_suffix(".json")
    sidecar = json.loads(sidecar_path.read_text())
    if sidecar["dim"] != DIMENSION or sidecar["dtype"] not in {"f16", "f32"}:
        raise RuntimeError(f"unexpected cached tensor format in {sidecar_path}")
    raw_path = prefix.with_suffix(f".{sidecar['dtype']}")
    rows = [int(value) for value in sidecar["rows"]]
    ids = [str(value) for value in sidecar["ids"]]
    if len(rows) != len(ids) or len(ids) != len(set(ids)):
        raise RuntimeError(f"invalid cached tensor IDs in {sidecar_path}")
    expected_scalars = sum(rows) * DIMENSION
    numpy_dtype = "<f2" if sidecar["dtype"] == "f16" else "<f4"
    values = np.fromfile(raw_path, dtype=numpy_dtype)
    if values.size != expected_scalars:
        raise RuntimeError(
            f"{raw_path} has {values.size} scalars, expected {expected_scalars}"
        )
    values = values.astype(np.float32)
    matrices = []
    offset = 0
    for row_count in rows:
        next_offset = offset + row_count * DIMENSION
        matrices.append(values[offset:next_offset].reshape(row_count, DIMENSION))
        offset = next_offset
    return EncodingResult(
        matrices=matrices,
        ids=ids,
        batch_one_cpu_seconds_per_item=float(timing["batch_1"]),
        batch_eight_cpu_seconds_per_item=float(timing["batch_8"]),
        batch_eight_wall_seconds_per_item=float(timing["batch_8_wall"]),
        peak_rss_mib=float(timing["peak_rss_mib"]),
    )


def validate_precision_reference(
    primary: EncodingResult,
    reference: EncodingResult,
    lane: str,
    role: str,
) -> None:
    if primary.ids != reference.ids:
        raise RuntimeError(
            f"{lane} {role} f16/f32 tensor IDs differ"
        )
    if len(primary.matrices) != len(reference.matrices):
        raise RuntimeError(
            f"{lane} {role} f16/f32 matrix counts differ"
        )
    for index, (primary_matrix, reference_matrix) in enumerate(
        zip(primary.matrices, reference.matrices)
    ):
        if primary_matrix.shape != reference_matrix.shape:
            raise RuntimeError(
                f"{lane} {role} f16/f32 shape differs at index {index}: "
                f"{primary_matrix.shape} != {reference_matrix.shape}"
            )
        if (
            primary_matrix.ndim != 2
            or primary_matrix.shape[1] != DIMENSION
        ):
            raise RuntimeError(
                f"{lane} {role} tensor at index {index} has invalid shape "
                f"{primary_matrix.shape}"
            )


def require_count(value, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RuntimeError(f"{label} must be a non-negative integer")


def require_fraction(value, label: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or not 0.0 <= float(value) <= 1.0
    ):
        raise RuntimeError(f"{label} must be a finite fraction")


def resolve_precision_ranking_candidates(
    lane: str,
    selected: list[str] | None,
) -> tuple[list[str], bool]:
    if lane == "text":
        if selected:
            raise RuntimeError(
                "text ranking-retention always runs all seven candidates"
            )
        return list(PRECISION_RANKING_ALL_CANDIDATES), False
    if not selected:
        raise RuntimeError(
            "visual ranking-retention requires one or two --candidate "
            "selections"
        )
    if len(selected) != len(set(selected)):
        raise RuntimeError("ranking-retention candidates must be unique")
    if len(selected) > 2:
        raise RuntimeError(
            "visual ranking-retention accepts at most two --candidate "
            "selections"
        )
    return [*PRECISION_RANKING_BASELINES, *selected], True


def validate_precision_ranking_audit(
    result: dict,
    lane: str,
    queries: EncodingResult,
    expected_candidates: Sequence[str],
) -> None:
    if not isinstance(result, dict) or result.get("lane") != lane:
        raise RuntimeError("Rust precision-ranking result has the wrong lane")
    audit = result.get("precision_ranking_audit")
    if not isinstance(audit, dict):
        raise RuntimeError("Rust result omitted precision_ranking_audit")
    if audit.get("reference") != "f32_documents_f32_queries":
        raise RuntimeError("precision-ranking audit has an unknown reference")
    for field in ("query_count", "gold_count"):
        require_count(audit.get(field), f"precision_ranking_audit.{field}")
    if (
        audit["query_count"] != len(queries.ids)
        or audit["gold_count"] != len(queries.ids) * 10
    ):
        raise RuntimeError("precision-ranking audit count is incomplete")
    candidates = audit.get("candidates")
    if not isinstance(candidates, list):
        raise RuntimeError("precision_ranking_audit.candidates must be a list")
    actual_candidates = [
        candidate.get("candidate")
        for candidate in candidates
        if isinstance(candidate, dict)
    ]
    if actual_candidates != list(expected_candidates):
        raise RuntimeError(
            "precision-ranking audit candidate set or order differs: "
            f"expected {list(expected_candidates)}, got {actual_candidates}"
        )
    fraction_fields = {
        "top_10_set_exactly_equal_query_fraction",
        "f32_top_10_recovered_in_candidate_top_10",
        "ordered_top_10_exactly_equal_query_fraction",
        "f32_top_1_same_document_fraction",
        "f32_top_1_in_candidate_top_10_fraction",
        "saving_fraction_vs_f16",
    }
    for candidate in candidates:
        label = f"precision_ranking_audit.{candidate['candidate']}"
        for field in fraction_fields:
            require_fraction(candidate.get(field), f"{label}.{field}")
        for field in ("coordinate_bytes_total", "metadata_bytes_per_row"):
            require_count(candidate.get(field), f"{label}.{field}")
        if (
            isinstance(candidate.get("mean_payload_bytes_per_unit"), bool)
            or not isinstance(
                candidate.get("mean_payload_bytes_per_unit"),
                (int, float),
            )
            or not math.isfinite(
                float(candidate["mean_payload_bytes_per_unit"])
            )
            or candidate["mean_payload_bytes_per_unit"] < 0
        ):
            raise RuntimeError(
                f"{label}.mean_payload_bytes_per_unit must be non-negative"
            )
        ranks = candidate.get("per_rank_same_document_fractions")
        if not isinstance(ranks, list) or len(ranks) != 10:
            raise RuntimeError(f"{label} must report ranks 1 through 10")
        for expected_rank, rank in enumerate(ranks, start=1):
            if not isinstance(rank, dict) or rank.get("rank") != expected_rank:
                raise RuntimeError(f"{label} rank sequence is incomplete")
            require_fraction(
                rank.get("fraction"),
                f"{label}.per_rank[{expected_rank}].fraction",
            )


def resolve_production_int8_group_sizes(
    selected: list[int] | None,
) -> tuple[int, ...]:
    group_sizes = tuple(selected or DEFAULT_PRODUCTION_INT8_GROUP_SIZES)
    if len(group_sizes) != len(set(group_sizes)):
        raise RuntimeError("production INT8 group sizes must be unique")
    if any(size not in PRODUCTION_INT8_GROUP_SIZES for size in group_sizes):
        raise RuntimeError(
            "production INT8 group size must be one of 16, 32, or 128"
        )
    return group_sizes


def require_sha256(value, label: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise RuntimeError(f"{label} must be a lowercase SHA-256 digest")


def validate_ranking_agreement(
    agreement: dict,
    label: str,
    query_count: int,
) -> None:
    if not isinstance(agreement, dict):
        raise RuntimeError(f"{label} must be an object")
    if (
        agreement.get("query_count") != query_count
        or agreement.get("gold_count") != query_count * 10
    ):
        raise RuntimeError(f"{label} does not cover the complete query set")
    for field in (
        "top_10_set_exactly_equal_query_fraction",
        "top_10_recovered_in_candidate_top_10",
        "ordered_top_10_exactly_equal_query_fraction",
        "top_1_same_document_fraction",
        "top_1_in_candidate_top_10_fraction",
    ):
        require_fraction(agreement.get(field), f"{label}.{field}")
    require_count(
        agreement.get("missed_top_10_memberships"),
        f"{label}.missed_top_10_memberships",
    )
    ranks = agreement.get("per_rank_same_document_fractions")
    if not isinstance(ranks, list) or len(ranks) != 10:
        raise RuntimeError(f"{label} must report ranks 1 through 10")
    for expected_rank, rank in enumerate(ranks, start=1):
        if not isinstance(rank, dict) or rank.get("rank") != expected_rank:
            raise RuntimeError(f"{label} rank sequence is incomplete")
        require_fraction(
            rank.get("fraction"),
            f"{label}.per_rank[{expected_rank}].fraction",
        )


def validate_production_int8_qualification(
    result: dict,
    lane: str,
    queries: EncodingResult,
    group_sizes: Sequence[int],
    artifact_directory: Path,
) -> None:
    if (
        not isinstance(result, dict)
        or result.get("schema_version") != 1
        or result.get("mode") != "production_int8_matrix_qualification"
        or result.get("lane") != lane
    ):
        raise RuntimeError("Rust production INT8 result has the wrong identity")
    if result.get("decision_state") != "measured_awaiting_operator_threshold":
        raise RuntimeError("production INT8 result invented a gate decision")
    if (
        result.get("f32_reference")
        != "same_pass_f32_documents_and_queries"
        or result.get("f16_reference")
        != "same_pass_f16_documents_and_queries"
        or result.get("candidate_query_dtype") != "f16"
        or result.get("f16_f32_id_parity_verified") is not True
    ):
        raise RuntimeError("production INT8 result has an invalid reference")
    require_sha256(
        result.get("evidence_digest_sha256"),
        "evidence_digest_sha256",
    )
    input_digests = result.get("input_tensor_digests")
    if not isinstance(input_digests, dict):
        raise RuntimeError("production INT8 result omitted input digests")
    for field in (
        "f16_documents_sha256",
        "f16_queries_sha256",
        "f32_documents_sha256",
        "f32_queries_sha256",
    ):
        require_sha256(input_digests.get(field), f"input_tensor_digests.{field}")
    validate_ranking_agreement(
        result.get("f16_vs_f32_ranking"),
        "f16_vs_f32_ranking",
        len(queries.ids),
    )
    groups = result.get("group_results")
    if not isinstance(groups, list):
        raise RuntimeError("production INT8 result omitted group cells")
    actual_group_sizes = [
        group.get("group_size") for group in groups if isinstance(group, dict)
    ]
    if actual_group_sizes != list(group_sizes):
        raise RuntimeError(
            "production INT8 result group order differs: "
            f"expected {list(group_sizes)}, got {actual_group_sizes}"
        )
    for group_size, group in zip(group_sizes, groups):
        label = f"production_int8.group_{group_size}"
        if not isinstance(group, dict):
            raise RuntimeError(f"{label} must be an object")
        expected_state = (
            "measurement_only_no_durable_visual_baseline"
            if group_size == 128
            else "measured_awaiting_operator_threshold"
        )
        if (
            group.get("dtype")
            != {"type": "int8_sym_v1", "group_size": group_size}
            or group.get("decision_state") != expected_state
        ):
            raise RuntimeError(f"{label} has an invalid dtype or state")
        for field in (
            "artifact_write_read_verified",
            "authoritative_dtype_verified",
            "id_parity_verified",
        ):
            if group.get(field) is not True:
                raise RuntimeError(f"{label}.{field} must be true")
        for field in (
            "artifact_file_bytes",
            "matrix_payload_bytes",
            "f16_matrix_payload_bytes",
        ):
            require_count(group.get(field), f"{label}.{field}")
            if group[field] == 0:
                raise RuntimeError(f"{label}.{field} must be positive")
        require_fraction(
            group.get("saving_fraction_vs_f16"),
            f"{label}.saving_fraction_vs_f16",
        )
        mean_bytes = group.get("mean_matrix_payload_bytes_per_document")
        if (
            isinstance(mean_bytes, bool)
            or not isinstance(mean_bytes, (int, float))
            or not math.isfinite(float(mean_bytes))
            or mean_bytes <= 0
        ):
            raise RuntimeError(f"{label} has invalid mean payload bytes")
        expected_file = f"{lane}-int8-sym-v1-g{group_size}.zme1"
        if group.get("artifact_file") != expected_file:
            raise RuntimeError(f"{label} has an unexpected artifact name")
        artifact_path = artifact_directory / expected_file
        if (
            not artifact_path.is_file()
            or artifact_path.stat().st_size != group["artifact_file_bytes"]
        ):
            raise RuntimeError(f"{label} artifact write did not persist")
        require_sha256(
            group.get("artifact_checksum_sha256"),
            f"{label}.artifact_checksum_sha256",
        )
        if sha256_file(artifact_path) != group["artifact_checksum_sha256"]:
            raise RuntimeError(f"{label} artifact checksum differs on disk")
        validate_ranking_agreement(
            group.get("int8_vs_f32_ranking"),
            f"{label}.int8_vs_f32_ranking",
            len(queries.ids),
        )
        validate_ranking_agreement(
            group.get("int8_vs_f16_ranking"),
            f"{label}.int8_vs_f16_ranking",
            len(queries.ids),
        )
        if "passed" in group:
            raise RuntimeError(f"{label} must not auto-decide an unsigned gate")


def analytic_retrieval_decode_cost(
    corpus_stats: dict,
    candidate: str,
) -> dict:
    if not isinstance(corpus_stats, dict):
        raise RuntimeError("ranking lane omitted corpus_stats")
    scalar_count = corpus_stats.get("scalar_count")
    row_count = corpus_stats.get("total_rows")
    dimension = corpus_stats.get("dim")
    for value, label in (
        (scalar_count, "corpus_stats.scalar_count"),
        (row_count, "corpus_stats.total_rows"),
        (dimension, "corpus_stats.dim"),
    ):
        require_count(value, label)
    if dimension == 0 or scalar_count != row_count * dimension:
        raise RuntimeError("ranking corpus stats have inconsistent dimensions")

    cost = {
        "basis": "analytic_exact_counts",
        "scope": "payload_to_f32_before_maxsim",
        "coordinate_conversions_total": scalar_count,
        "scale_values_read_total": 0,
        "offset_values_read_total": 0,
        "dequantize_multiplications_total": 0,
        "dequantize_additions_total": 0,
        "renormalized_rows_total": 0,
        "norm_accumulations_total": 0,
        "square_roots_total": 0,
        "normalization_divisions_total": 0,
        "maxsim_scoring_included": False,
    }
    if candidate == "f16":
        return cost
    cost["dequantize_multiplications_total"] = scalar_count
    if candidate == "int8_global_scale":
        return cost
    if candidate in {
        "int8_per_row_calibrated",
        "int8_per_row_calibrated_renormalized",
    }:
        cost["scale_values_read_total"] = row_count
        cost["offset_values_read_total"] = row_count
        cost["dequantize_additions_total"] = scalar_count
    elif candidate == "int8_symmetric_per_row_renormalized":
        cost["scale_values_read_total"] = row_count
    elif candidate in {
        "int8_groupwise_32_symmetric_renormalized",
        "int8_groupwise_16_symmetric_renormalized",
    }:
        group_size = 32 if "groupwise_32" in candidate else 16
        cost["scale_values_read_total"] = (
            row_count * ((dimension + group_size - 1) // group_size)
        )
    else:
        raise RuntimeError(f"unknown ranking candidate: {candidate}")

    if candidate != "int8_per_row_calibrated":
        cost["renormalized_rows_total"] = row_count
        cost["norm_accumulations_total"] = scalar_count
        cost["square_roots_total"] = row_count
        cost["normalization_divisions_total"] = scalar_count
    return cost


def backfill_retrieval_decode_cost(lane_result: dict) -> None:
    if not isinstance(lane_result, dict):
        raise RuntimeError("ranking lane result must be an object")
    audit = lane_result.get("precision_ranking_audit")
    candidates = audit.get("candidates") if isinstance(audit, dict) else None
    if not isinstance(candidates, list):
        raise RuntimeError("ranking lane omitted candidate results")
    for candidate in candidates:
        if not isinstance(candidate, dict):
            raise RuntimeError("ranking candidate result must be an object")
        expected = analytic_retrieval_decode_cost(
            lane_result.get("corpus_stats"),
            candidate.get("candidate"),
        )
        existing = candidate.get("retrieval_decode_cost")
        if existing is None:
            candidate["retrieval_decode_cost"] = expected
        elif existing != expected:
            raise RuntimeError(
                f"{candidate.get('candidate')} retrieval_decode_cost "
                "differs from the analytic contract"
            )


def quantized_matrices(result: EncodingResult) -> list[torch.Tensor]:
    return [
        torch.from_numpy(matrix.astype(np.float16).astype(np.float32))
        for matrix in result.matrices
    ]


def text_official_pair_scores(
    queries: EncodingResult, documents: EncodingResult
) -> list[float]:
    """Apply the pinned model's documented PyLate MaxSim pairwise formula."""
    q_matrices = quantized_matrices(queries)
    d_matrices = quantized_matrices(documents)
    scores = []
    for query, document in zip(
        q_matrices[:PAIR_COUNT], d_matrices[:PAIR_COUNT]
    ):
        scores.append(
            torch.einsum("sh,th->st", query, document)
            .max(dim=-1)
            .values.sum()
            .item()
        )
    return scores


def visual_official_pair_scores(
    processor,
    queries: EncodingResult,
    documents: EncodingResult,
) -> list[float]:
    """Use the model-native processor scoring seam after f16 exchange."""
    q_matrices = quantized_matrices(queries)[:PAIR_COUNT]
    d_matrices = quantized_matrices(documents)[:PAIR_COUNT]
    score_matrix = processor.score_retrieval(
        q_matrices,
        d_matrices,
        batch_size=BATCH_SIZE,
        output_dtype=torch.float32,
        output_device="cpu",
    )
    return [
        float(score_matrix[index, index].item()) for index in range(PAIR_COUNT)
    ]


def write_pair_scores(
    path: Path,
    queries: EncodingResult,
    documents: EncodingResult,
    official_scores: Sequence[float],
) -> None:
    if len(official_scores) != PAIR_COUNT:
        raise RuntimeError(
            f"expected {PAIR_COUNT} official pair scores, got "
            f"{len(official_scores)}"
        )
    pairs = []
    for index, score in enumerate(official_scores):
        pairs.append(
            {
                "query_id": queries.ids[index],
                "document_id": documents.ids[index],
                "score": score,
            }
        )
    path.write_text(json.dumps(pairs, separators=(",", ":")))


def encoding_metadata(documents: EncodingResult, queries: EncodingResult) -> dict:
    return {
        "documents": {
            "count": len(documents.ids),
            "batch_1_cpu_seconds_per_item": documents.batch_one_cpu_seconds_per_item,
            "batch_8_cpu_seconds_per_item": documents.batch_eight_cpu_seconds_per_item,
            "batch_8_wall_seconds_per_item": (
                documents.batch_eight_wall_seconds_per_item
            ),
            "peak_rss_mib": documents.peak_rss_mib,
        },
        "queries": {
            "count": len(queries.ids),
            "batch_1_cpu_seconds_per_item": queries.batch_one_cpu_seconds_per_item,
            "batch_8_cpu_seconds_per_item": queries.batch_eight_cpu_seconds_per_item,
            "batch_8_wall_seconds_per_item": (
                queries.batch_eight_wall_seconds_per_item
            ),
            "peak_rss_mib": queries.peak_rss_mib,
        },
    }


def run_rust(
    binary: Path,
    work_dir: Path,
    lane: str,
    documents: EncodingResult,
    queries: EncodingResult,
    official_scores: Sequence[float],
    chosen_algorithm: str | None = None,
    chosen_centering: str | None = None,
    full_precision_documents: EncodingResult | None = None,
    full_precision_queries: EncodingResult | None = None,
    int8_probe: bool = False,
    reuse_primary_exchange: bool = False,
) -> dict:
    if reuse_primary_exchange:
        doc_raw = work_dir / f"{lane}-documents.f16"
        doc_sidecar = work_dir / f"{lane}-documents.json"
        query_raw = work_dir / f"{lane}-queries.f16"
        query_sidecar = work_dir / f"{lane}-queries.json"
        for path in (doc_raw, doc_sidecar, query_raw, query_sidecar):
            if not path.is_file():
                raise FileNotFoundError(
                    f"missing retained primary tensor exchange file: {path}"
                )
    else:
        doc_raw, doc_sidecar = write_tensor(
            work_dir / f"{lane}-documents", documents
        )
        query_raw, query_sidecar = write_tensor(
            work_dir / f"{lane}-queries", queries
        )
    pair_path = work_dir / f"{lane}-pairs.json"
    write_pair_scores(pair_path, queries, documents, official_scores)
    job = {
        "lane": lane,
        "documents": {"raw": str(doc_raw), "sidecar": str(doc_sidecar)},
        "queries": {"raw": str(query_raw), "sidecar": str(query_sidecar)},
        "official_scores": str(pair_path),
        "int8_probe": int8_probe,
    }
    if lane == "visual":
        job["chosen_algorithm"] = chosen_algorithm
        job["chosen_centering"] = chosen_centering
    if (full_precision_documents is None) != (
        full_precision_queries is None
    ):
        raise RuntimeError(
            "full-precision documents and queries must be supplied together"
        )
    if int8_probe and full_precision_documents is None:
        raise RuntimeError("int8 probe requires fresh f32 reference tensors")
    full_precision_paths: list[Path] = []
    if full_precision_documents is not None:
        full_doc_raw, full_doc_sidecar = write_tensor(
            work_dir / f"{lane}-documents-f32",
            full_precision_documents,
            "f32",
        )
        full_query_raw, full_query_sidecar = write_tensor(
            work_dir / f"{lane}-queries-f32",
            full_precision_queries,
            "f32",
        )
        job["full_precision_documents"] = {
            "raw": str(full_doc_raw),
            "sidecar": str(full_doc_sidecar),
        }
        job["full_precision_queries"] = {
            "raw": str(full_query_raw),
            "sidecar": str(full_query_sidecar),
        }
        full_precision_paths = [
            full_doc_raw,
            full_doc_sidecar,
            full_query_raw,
            full_query_sidecar,
        ]
    job_path = work_dir / f"{lane}-job.json"
    job_path.write_text(json.dumps(job, indent=2) + "\n")
    completed = subprocess.run(
        [binary, job_path],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    result = json.loads(completed.stdout)
    if int8_probe:
        probe = result.get("int8_probe")
        probe_fields = {
            "candidate_config",
            "candidate_centering",
            "candidate_document_pooling_factor",
            "global_scale_passed",
            "variants",
        }
        variant_fields = {
            "variant",
            "f32_top_1_same_rank_fraction",
            "f32_top_10_recall_in_int8_top_10",
            "max_sim_50_pair_max_absolute_error",
            "first_100_document_row_l2_error_p50",
            "first_100_document_row_l2_error_p95",
            "first_100_document_row_l2_error_p99",
            "mean_payload_bytes_per_document",
            "candidate_recall_at_50",
            "candidate_recall_at_100",
            "candidate_recall_at_300",
            "passed",
        }
        if (
            not isinstance(probe, dict)
            or not probe_fields.issubset(probe)
            or not isinstance(probe.get("variants"), list)
            or len(probe["variants"]) != 2
            or not isinstance(probe.get("global_scale_passed"), bool)
            or {
                row.get("variant")
                for row in probe["variants"]
                if isinstance(row, dict)
            }
            != {"global_scale", "per_row_calibrated"}
            or any(
                not isinstance(row, dict)
                or not variant_fields.issubset(row)
                for row in probe["variants"]
            )
        ):
            raise RuntimeError(
                f"{lane} Rust result omitted a complete int8 probe"
            )
    for path in full_precision_paths:
        path.unlink()
    return result


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def artifact_hashes(snapshots: Sequence[tuple[str, str, Path]]) -> list[dict]:
    rows = []
    for label, revision, snapshot in snapshots:
        for path in sorted(value for value in snapshot.rglob("*") if value.is_file()):
            rows.append(
                {
                    "artifact": f"{label}/{path.relative_to(snapshot)}",
                    "revision": revision,
                    "sha256": sha256_file(path),
                }
            )
    return rows


def percentile(values: Sequence[int], quantile: float) -> float:
    return float(np.quantile(np.asarray(values, dtype=np.float64), quantile))


def pearson(left: np.ndarray, right: np.ndarray) -> float:
    if left.shape != right.shape or left.size < 2:
        raise RuntimeError("correlation inputs must have the same non-trivial shape")
    left_centered = left - left.mean()
    right_centered = right - right.mean()
    denominator = np.linalg.norm(left_centered) * np.linalg.norm(right_centered)
    if denominator == 0.0:
        raise RuntimeError("correlation input has zero variance")
    return float(np.dot(left_centered, right_centered) / denominator)


def diagnostic_summary(cell: dict) -> dict:
    gold_ranks = cell["gold_ranks"]
    missed_ranks = np.asarray(
        [
            row["fde_rank"]
            for row in gold_ranks
            if row["fde_rank"] > 100
        ],
        dtype=np.float64,
    )
    if missed_ranks.size == 0:
        raise RuntimeError("diagnostic expected at least one K=100 miss")

    pairs = cell["score_pairs"]
    query_rows = np.asarray(
        [row["query_rows"] for row in pairs], dtype=np.float64
    )
    document_rows = np.asarray(
        [row["document_rows"] for row in pairs], dtype=np.float64
    )
    semantic_exact = (
        np.asarray([row["exact_score"] for row in pairs], dtype=np.float64)
        / query_rows
    )
    transformed_exact = (
        np.asarray(
            [row["transformed_exact_score"] for row in pairs],
            dtype=np.float64,
        )
        / query_rows
    )
    fde = (
        np.asarray(
            [row["fde_score_per_repetition"] for row in pairs],
            dtype=np.float64,
        )
        / query_rows
    )

    def fit(exact: np.ndarray) -> dict:
        design = np.column_stack((exact, np.ones(exact.size)))
        slope, intercept = np.linalg.lstsq(design, fde, rcond=None)[0]
        residual = fde - (slope * exact + intercept)
        return {
            "score_pearson": pearson(exact, fde),
            "affine_slope": float(slope),
            "affine_intercept": float(intercept),
            "residual_rmse": float(np.sqrt(np.mean(residual * residual))),
            "signed_residual_vs_document_rows_pearson": pearson(
                residual, document_rows
            ),
            "absolute_residual_vs_document_rows_pearson": pearson(
                np.abs(residual), document_rows
            ),
        }

    recall_by_frontier = {}
    k_for_95 = {}
    for frontier in (1, 5, 10):
        ranks = sorted(
            row["fde_rank"]
            for row in gold_ranks
            if row["exact_rank"] <= frontier
        )
        recall_by_frontier[str(frontier)] = {
            str(candidate_k): sum(rank <= candidate_k for rank in ranks)
            / len(ranks)
            for candidate_k in DIAGNOSTIC_CANDIDATE_KS
        }
        k_for_95[str(frontier)] = ranks[int(np.ceil(0.95 * len(ranks))) - 1]

    return {
        "config": cell["config"],
        "repetitions": cell["repetitions"],
        "simhash_bits": cell["simhash_bits"],
        "d_proj": cell["d_proj"],
        "output_dimension": cell["output_dimension"],
        "transform_checksum_sha256": cell["transform_checksum_sha256"],
        "recall_at_50": cell["recall_at_50"],
        "recall_at_100": cell["recall_at_100"],
        "recall_at_300": cell["recall_at_300"],
        "gold_count": len(cell["gold_ranks"]),
        "missed_at_100": int(missed_ranks.size),
        "missed_rank_p50": float(np.quantile(missed_ranks, 0.50)),
        "missed_rank_p90": float(np.quantile(missed_ranks, 0.90)),
        "missed_rank_p95": float(np.quantile(missed_ranks, 0.95)),
        "missed_rank_p99": float(np.quantile(missed_ranks, 0.99)),
        "missed_rank_max": int(missed_ranks.max()),
        "missed_rank_bins": {
            "101_400": int(np.count_nonzero(missed_ranks <= 400)),
            "401_1000": int(
                np.count_nonzero((missed_ranks > 400) & (missed_ranks <= 1000))
            ),
            "1001_2000": int(
                np.count_nonzero(
                    (missed_ranks > 1000) & (missed_ranks <= 2000)
                )
            ),
            "2001_plus": int(np.count_nonzero(missed_ranks > 2000)),
        },
        "score_pair_count": len(pairs),
        "recall_by_exact_frontier": recall_by_frontier,
        "candidate_k_for_95_percent": k_for_95,
        "semantic_raw_exact_fit": fit(semantic_exact),
        "construction_transformed_exact_fit": fit(transformed_exact),
    }


def exact_gap_summary(
    rows: list[dict], baseline_cell: dict, construction_rmse: float
) -> dict:
    if not rows:
        raise RuntimeError("exact frontier-gap diagnostic is empty")
    query_rows = np.asarray(
        [row["query_rows"] for row in rows], dtype=np.float64
    )
    rank_10 = (
        np.asarray([row["rank_10_score"] for row in rows], dtype=np.float64)
        / query_rows
    )
    rank_100 = (
        np.asarray([row["rank_100_score"] for row in rows], dtype=np.float64)
        / query_rows
    )
    raw_gap = np.asarray(
        [row["rank_10_to_rank_100_gap"] for row in rows],
        dtype=np.float64,
    )
    gap = raw_gap / query_rows
    if np.any(gap < -1.0e-7):
        raise RuntimeError("rank-100 exact score exceeds rank-10 exact score")
    relative_gap = gap / np.maximum(np.abs(rank_10), 1.0e-12)
    recovered_by_query: dict[int, int] = {}
    for row in baseline_cell["gold_ranks"]:
        query_index = int(row["query_index"])
        recovered_by_query.setdefault(query_index, 0)
        recovered_by_query[query_index] += int(row["fde_rank"] <= 100)
    if set(recovered_by_query) != {
        int(row["query_index"]) for row in rows
    }:
        raise RuntimeError("frontier gaps and baseline recovery queries differ")
    recovery = np.asarray(
        [
            recovered_by_query[int(row["query_index"])] / 10.0
            for row in rows
        ],
        dtype=np.float64,
    )
    gap_order = np.argsort(gap, kind="stable")
    gap_deciles = np.array_split(gap_order, 10)

    def distribution(values: np.ndarray) -> dict:
        return {
            "min": float(values.min()),
            "p01": float(np.quantile(values, 0.01)),
            "p05": float(np.quantile(values, 0.05)),
            "p50": float(np.quantile(values, 0.50)),
            "p95": float(np.quantile(values, 0.95)),
            "p99": float(np.quantile(values, 0.99)),
            "max": float(values.max()),
        }

    return {
        "query_count": len(rows),
        "normalization": "exact MaxSim divided by query rows",
        "rank_10_score": distribution(rank_10),
        "rank_100_score": distribution(rank_100),
        "rank_10_to_rank_100_gap": distribution(gap),
        "relative_gap_over_rank_10": distribution(relative_gap),
        "gap_vs_top10_recovery_at_100_pearson": pearson(gap, recovery),
        "lowest_gap_decile_recall_at_100": float(
            recovery[gap_deciles[0]].mean()
        ),
        "highest_gap_decile_recall_at_100": float(
            recovery[gap_deciles[-1]].mean()
        ),
        "construction_residual_rmse": construction_rmse,
        "fraction_gap_below_1x_construction_rmse": float(
            np.mean(gap <= construction_rmse)
        ),
        "fraction_gap_below_2x_construction_rmse": float(
            np.mean(gap <= 2.0 * construction_rmse)
        ),
        "fraction_gap_below_3x_construction_rmse": float(
            np.mean(gap <= 3.0 * construction_rmse)
        ),
    }


def diagnostic_axis(
    values: np.ndarray, start: int, end: int
) -> tuple[np.ndarray, float, float]:
    low, high = np.quantile(values, [0.01, 0.99])
    if high <= low:
        raise RuntimeError("diagnostic scatter axis has no range")
    clipped = np.clip(values, low, high)
    scaled = start + (clipped - low) * (end - start) / (high - low)
    return scaled, float(low), float(high)


def render_diagnostic_scatter(cells: list[dict], path: Path) -> None:
    width = 1_400
    row_height = 380
    image = Image.new("RGB", (width, row_height * len(cells)), "white")
    draw = ImageDraw.Draw(image)
    for row_index, cell in enumerate(cells):
        pairs = cell["score_pairs"]
        query_rows = np.asarray(
            [row["query_rows"] for row in pairs], dtype=np.float64
        )
        document_rows = np.asarray(
            [row["document_rows"] for row in pairs], dtype=np.float64
        )
        exact = (
            np.asarray([row["exact_score"] for row in pairs], dtype=np.float64)
            / query_rows
        )
        transformed_exact = (
            np.asarray(
                [row["transformed_exact_score"] for row in pairs],
                dtype=np.float64,
            )
            / query_rows
        )
        fde = (
            np.asarray(
                [row["fde_score_per_repetition"] for row in pairs],
                dtype=np.float64,
            )
            / query_rows
        )
        design = np.column_stack(
            (transformed_exact, np.ones(transformed_exact.size))
        )
        slope, intercept = np.linalg.lstsq(design, fde, rcond=None)[0]
        residual = np.abs(fde - (slope * transformed_exact + intercept))

        top = row_index * row_height + 45
        bottom = top + 285
        left_x0, left_x1 = 70, 650
        right_x0, right_x1 = 770, 1_350
        exact_x, exact_low, exact_high = diagnostic_axis(
            exact, left_x0, left_x1
        )
        fde_y, fde_low, fde_high = diagnostic_axis(fde, bottom, top)
        length_x, length_low, length_high = diagnostic_axis(
            document_rows, right_x0, right_x1
        )
        residual_y, residual_low, residual_high = diagnostic_axis(
            residual, bottom, top
        )

        draw.text(
            (20, row_index * row_height + 12),
            (
                f"{cell['config']}: R={cell['repetitions']} "
                f"k={cell['simhash_bits']} d={cell['d_proj']}"
            ),
            fill="black",
        )
        draw.rectangle((left_x0, top, left_x1, bottom), outline="black")
        draw.rectangle((right_x0, top, right_x1, bottom), outline="black")
        for x, y in zip(exact_x, fde_y):
            draw.ellipse((x - 1, y - 1, x + 1, y + 1), fill="#2455a4")
        for x, y in zip(length_x, residual_y):
            draw.ellipse((x - 1, y - 1, x + 1, y + 1), fill="#b33a3a")
        draw.text(
            (left_x0, bottom + 8),
            (
                "raw exact MaxSim/query rows "
                f"[p1={exact_low:.3g}, p99={exact_high:.3g}]"
            ),
            fill="black",
        )
        draw.text(
            (left_x0, top - 18),
            (
                "FDE IP/(R*query rows) "
                f"[p1={fde_low:.3g}, p99={fde_high:.3g}]"
            ),
            fill="black",
        )
        draw.text(
            (right_x0, bottom + 8),
            (
                "document rows "
                f"[p1={length_low:.0f}, p99={length_high:.0f}]"
            ),
            fill="black",
        )
        draw.text(
            (right_x0, top - 18),
            (
                "|construction residual| "
                f"[p1={residual_low:.3g}, p99={residual_high:.3g}]"
            ),
            fill="black",
        )
    image.save(path)


def write_diagnostic_artifacts(report_path: Path, result: dict) -> dict:
    cells = result.get("diagnostics")
    probes = result.get("diagnostic_probes") or []
    exact_gaps = result.get("exact_frontier_gaps") or []
    if not cells and not probes and not exact_gaps:
        return {}
    payload = {
        "schema_version": 2,
        "seed": result["seed"],
        "diagnostic_cell_score_normalization": (
            "FDE inner product divided by repetitions and query rows; exact "
            "MaxSim divided by query rows"
        ),
        "exact_frontier_gap_normalization": (
            "per-query JSON fields are raw MaxSim sums; report summaries "
            "divide scores and gaps by query rows"
        ),
        "cells": cells or [],
        "fixed_budget_probes": probes,
        "exact_frontier_gaps": exact_gaps,
    }
    json_path = report_path.with_name("lab-diagnostics.json")
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    if cells:
        render_diagnostic_scatter(
            cells, report_path.with_name("lab-diagnostics.png")
        )
    summaries = [diagnostic_summary(cell) for cell in cells or []]
    baseline_cell = next(
        cell for cell in cells or [] if cell["config"] == "A"
    )
    baseline_summary = next(
        summary for summary in summaries if summary["config"] == "A"
    )
    construction_rmse = baseline_summary[
        "construction_transformed_exact_fit"
    ]["residual_rmse"]
    return {
        "cells": summaries,
        "probes": probes,
        "exact_gap": (
            exact_gap_summary(
                exact_gaps, baseline_cell, construction_rmse
            )
            if exact_gaps
            else None
        ),
    }


def grouped_candidate_recall(
    gold_ranks: list[dict], candidate_k: int, group
) -> dict:
    totals: dict[str, list[int]] = {}
    for row in gold_ranks:
        label = str(group(row))
        counts = totals.setdefault(label, [0, 0])
        counts[0] += 1
        counts[1] += int(row["fde_rank"] <= candidate_k)
    return {
        label: {
            "gold_count": counts[0],
            "recall": counts[1] / counts[0],
        }
        for label, counts in sorted(totals.items())
    }


def visual_diagnostic_summary(cell: dict) -> dict:
    summary = diagnostic_summary(cell)
    gold_ranks = cell["gold_ranks"]
    query_rows = sorted(
        {
            int(row["query_index"]): int(row["query_rows"])
            for row in gold_ranks
        }.values()
    )
    quartile_bounds = [
        query_rows[max(0, math.ceil(len(query_rows) * fraction) - 1)]
        for fraction in (0.25, 0.50, 0.75)
    ]

    def query_row_quartile(row: dict) -> str:
        value = int(row["query_rows"])
        if value <= quartile_bounds[0]:
            return f"Q1 <= {quartile_bounds[0]}"
        if value <= quartile_bounds[1]:
            return f"Q2 <= {quartile_bounds[1]}"
        if value <= quartile_bounds[2]:
            return f"Q3 <= {quartile_bounds[2]}"
        return f"Q4 > {quartile_bounds[2]}"

    summary["query_row_quartile_boundaries"] = quartile_bounds
    summary["recall_at_300_splits"] = {
        "corpus": grouped_candidate_recall(
            gold_ranks,
            300,
            lambda row: row["query_id"].split(":", 1)[0],
        ),
        "document_rows": grouped_candidate_recall(
            gold_ranks,
            300,
            lambda row: row["document_rows"],
        ),
        "query_rows": grouped_candidate_recall(
            gold_ranks,
            300,
            query_row_quartile,
        ),
    }
    return summary


def write_visual_diagnostic_artifacts(
    report_path: Path, result: dict
) -> dict:
    cells = result.get("visual_diagnostics") or []
    pooling = result.get("pooling_probes") or []
    precision = result.get("precision_retention")
    json_path = report_path.with_name("lab-visual-diagnostics.json")
    if precision is None and json_path.is_file():
        previous = json.loads(json_path.read_text())
        previous_precision = previous.get("precision_retention")
        expected_queries = int(result["query_stats"]["matrix_count"])
        if (
            previous.get("seed") == result["seed"]
            and previous_precision is not None
            and previous_precision.get("query_count") == expected_queries
            and previous_precision.get("gold_count") == expected_queries * 10
        ):
            precision = previous_precision
    if not cells and not pooling and not precision:
        return {}
    approved_pooling_factor = int(
        result["winner"]["document_pooling_factor"]
    )
    payload = {
        "schema_version": 2,
        "seed": result["seed"],
        "gate_truth": "unpooled f16-roundtripped exhaustive MaxSim",
        "approved_candidate_document_pooling_factor": (
            approved_pooling_factor
        ),
        "pooling_semantics": (
            "contiguous document-row arithmetic means; tail divided by its "
            "actual row count; no renormalization; query and exact truth "
            "remain unpooled"
        ),
        "cells": cells,
        "pooling_probes": pooling,
        "precision_retention": precision,
    }
    json_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n"
    )
    return {
        "cells": [visual_diagnostic_summary(cell) for cell in cells],
        "pooling_probes": pooling,
        "precision_retention": precision,
        "approved_candidate_document_pooling_factor": (
            approved_pooling_factor
        ),
    }


def render_visual_diagnostics(evidence: dict) -> list[str]:
    if not evidence:
        return []
    summaries = evidence["cells"]
    lines = [
        "",
        "### Visual fixed-D diagnostic",
        "",
        "- Raw per-gold ranks and probes: "
        "[lab-visual-diagnostics.json](lab-visual-diagnostics.json).",
        "- All k-line cells use identity, PaperV1, d=16, and D=10,240.",
        "",
        "| Config | R/k/d | K | Exact top-1 | Exact top-5 | Exact top-10 |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for summary in summaries:
        frontiers = summary["recall_by_exact_frontier"]
        for candidate_k in map(str, DIAGNOSTIC_CANDIDATE_KS):
            lines.append(
                f"| {summary['config']} | {summary['repetitions']}/"
                f"{summary['simhash_bits']}/{summary['d_proj']} | "
                f"{candidate_k} | {frontiers['1'][candidate_k]:.6f} | "
                f"{frontiers['5'][candidate_k]:.6f} | "
                f"{frontiers['10'][candidate_k]:.6f} |"
            )
        k_for_95 = summary["candidate_k_for_95_percent"]
        lines.append(
            f"| {summary['config']} K needed for 95% | — | — | "
            f"{k_for_95['1']} | {k_for_95['5']} | {k_for_95['10']} |"
        )
        lines.append(
            f"| {summary['config']} query-row quartiles | — | — | "
            f"{summary['query_row_quartile_boundaries']} | — | — |"
        )
    lines.extend(
        [
            "",
            "| Config | Split | Group | Golds | R@300 |",
            "| --- | --- | --- | ---: | ---: |",
        ]
    )
    for summary in summaries:
        for split, groups in summary["recall_at_300_splits"].items():
            for group, values in groups.items():
                lines.append(
                    f"| {summary['config']} | {split} | {group} | "
                    f"{values['gold_count']} | {values['recall']:.6f} |"
                )
    pooling = evidence["pooling_probes"]
    if pooling:
        lines.extend(
            [
                "",
                "| Pool factor | Config | Mean rows before/after | "
                "R@50 | R@100 | R@300 |",
                "| ---: | --- | --- | ---: | ---: | ---: |",
            ]
        )
        for probe in pooling:
            result = probe["result"]
            lines.append(
                f"| {probe['factor']}× | {result['config']} "
                f"({result['repetitions']}/{result['simhash_bits']}/"
                f"{result['d_proj']}) | {probe['original_mean_rows']:.3f}/"
                f"{probe['pooled_mean_rows']:.3f} | "
                f"{result['recall_at_50']:.6f} | "
                f"{result['recall_at_100']:.6f} | "
                f"{result['recall_at_300']:.6f} |"
            )
        lines.extend(
            [
                "",
                f"- Approved visual candidate pooling: "
                f"{evidence['approved_candidate_document_pooling_factor']}× "
                "contiguous document-row arithmetic means, no "
                "renormalization. Queries and exact truth remain unpooled; "
                "4× remains diagnostic-only.",
            ]
        )
    precision = evidence["precision_retention"]
    if precision:
        lines.extend(
            [
                "",
                "### Visual f32 → f16 exact-ranking retention",
                "",
                f"- Queries: {precision['query_count']}; exact top-10 golds: "
                f"{precision['gold_count']}.",
                "- f32/f16 exact top-1 same-rank fraction: "
                f"`{precision['f32_top_1_same_rank_fraction']:.6f}`.",
                "- f32 top-1 present in f16 top-10: "
                f"`{precision['f32_top_1_in_f16_top_10_fraction']:.6f}`.",
                "- f32 exact top-10 recovered by f16 exact top-10: "
                f"`{precision['f32_top_10_recall_in_f16_top_10']:.6f}`.",
                "- This is diagnostic evidence only; no f16 qualification "
                "threshold was introduced.",
            ]
        )
    return lines


def render_diagnostics(evidence: dict) -> list[str]:
    if not evidence:
        return []
    summaries = evidence["cells"]
    probes = evidence["probes"]
    gap = evidence["exact_gap"]
    lines = [
        "",
        "### FDE failure diagnostic",
        "",
        "- Configs C and D are diagnostic-only. Config E is the selected "
        "same-budget operating-point probe and its measured cutoff curve "
        "drives the text candidate K.",
        "- Exact per-gold ranks and score pairs: "
        "[lab-diagnostics.json](lab-diagnostics.json).",
        "- Score/residual scatter for A, C, and E: "
        "[lab-diagnostics.png](lab-diagnostics.png).",
        "",
        "| Config | R/k/d | R@100 | Missed | Rank p50/p95/p99/max | "
        "101–400 | 401–1000 | 1001–2000 | 2001+ |",
        "| --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |",
    ]
    for summary in summaries:
        bins = summary["missed_rank_bins"]
        lines.append(
            f"| {summary['config']} | {summary['repetitions']}/"
            f"{summary['simhash_bits']}/{summary['d_proj']} | "
            f"{summary['recall_at_100']:.6f} | "
            f"{summary['missed_at_100']} | "
            f"{summary['missed_rank_p50']:.0f}/"
            f"{summary['missed_rank_p95']:.0f}/"
            f"{summary['missed_rank_p99']:.0f}/"
            f"{summary['missed_rank_max']} | "
            f"{bins['101_400']} | {bins['401_1000']} | "
            f"{bins['1001_2000']} | {bins['2001_plus']} |"
        )
    baseline = next(
        summary for summary in summaries if summary["config"] == "A"
    )
    lines.extend(
        [
            "",
            "| Fixed-budget probe | R/k/d | D | R@50 | R@100 | R@300 | "
            "R@100 delta vs A |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    selected_probe = next(
        summary
        for summary in summaries
        if summary["config"] == "E"
    )
    for probe in [*probes, selected_probe]:
        lines.append(
            f"| {probe['config']} | "
            f"{probe['repetitions']}/{probe['simhash_bits']}/"
            f"{probe['d_proj']} "
            f"| {probe['output_dimension']} | "
            f"{probe['recall_at_50']:.6f} | "
            f"{probe['recall_at_100']:.6f} | "
            f"{probe['recall_at_300']:.6f} | "
            f"{probe['recall_at_100'] - baseline['recall_at_100']:+.6f} |"
        )
    lines.extend(
        [
            "",
            "| Config | K | Exact top-1 recovered | Exact top-5 recovered | "
            "Exact top-10 recovered |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for summary in summaries:
        frontiers = summary["recall_by_exact_frontier"]
        for candidate_k in map(str, DIAGNOSTIC_CANDIDATE_KS):
            lines.append(
                f"| {summary['config']} | {candidate_k} | "
                f"{frontiers['1'][candidate_k]:.6f} | "
                f"{frontiers['5'][candidate_k]:.6f} | "
                f"{frontiers['10'][candidate_k]:.6f} |"
            )
        k_for_95 = summary["candidate_k_for_95_percent"]
        lines.append(
            f"| {summary['config']} K needed for 95% | — | "
            f"{k_for_95['1']} | {k_for_95['5']} | {k_for_95['10']} |"
        )
    lines.extend(
        [
            "",
            "| Config | Score pairs | Raw-exact r | Raw abs-residual/length r | "
            "Transformed-exact r | Construction abs-residual/length r |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for summary in summaries:
        semantic = summary["semantic_raw_exact_fit"]
        construction = summary["construction_transformed_exact_fit"]
        lines.append(
            f"| {summary['config']} | {summary['score_pair_count']} | "
            f"{semantic['score_pearson']:.6f} | "
            f"{semantic['absolute_residual_vs_document_rows_pearson']:.6f} | "
            f"{construction['score_pearson']:.6f} | "
            f"{construction['absolute_residual_vs_document_rows_pearson']:.6f} |"
        )
    lines.append("")
    for summary in summaries:
        lines.append(
            f"- {summary['config']} transform SHA-256: "
            f"`{summary['transform_checksum_sha256']}`."
        )
    diagnostic = next(
        summary
        for summary in summaries
        if summary["config"] == "C-diagnostic"
    )
    bins = baseline["missed_rank_bins"]
    missed = baseline["missed_at_100"]
    semantic = baseline["semantic_raw_exact_fit"]
    construction = baseline["construction_transformed_exact_fit"]
    d_proj_probe = next(
        probe
        for probe in probes
        if probe["config"] == "D-dproj-diagnostic"
    )
    reps_probe = selected_probe
    lines.extend(
        [
            f"- Rank shape: for A, {bins['101_400'] / missed:.1%} of K=100 "
            "misses land at ranks 101–400, "
            f"{(bins['101_400'] + bins['401_1000']) / missed:.1%} at ranks "
            "≤1,000, "
            f"and {bins['2001_plus'] / missed:.1%} beyond rank 2,000. The "
            "ordering is mostly noisy near the candidate frontier with a "
            "smaller long tail; it is not near-random.",
            "- Document-length bias is not supported: A's absolute "
            "residual/document-row correlation is "
            f"`{semantic['absolute_residual_vs_document_rows_pearson']:.6f}` "
            "against raw exact MaxSim and "
            f"`{construction['absolute_residual_vs_document_rows_pearson']:.6f}` "
            "against transformed exact MaxSim.",
            "- Raising `k_sim` while cutting `d_proj` did not cure the "
            "failure: C-diagnostic reduced top-10 R@100 from "
            f"`{baseline['recall_at_100']:.6f}` to "
            f"`{diagnostic['recall_at_100']:.6f}`.",
            "- At the same 10,240-D budget, coarser buckets plus wider inner "
            "projection changed top-10 R@100 by "
            f"`{d_proj_probe['recall_at_100'] - baseline['recall_at_100']:+.6f}`; "
            "coarser buckets plus more repetitions changed it by "
            f"`{reps_probe['recall_at_100'] - baseline['recall_at_100']:+.6f}`.",
            "- Metric provenance: the Phase 2 gate measures the fraction of "
            "every exact top-10 frontier recovered. The MUVERA paper's "
            "offline `1Recall@N` measures recovery of the single exact "
            "Chamfer nearest neighbor. Under that paper metric A reaches "
            f"`{baseline['recall_by_exact_frontier']['1']['100']:.6f}` at "
            "K=100 and "
            f"`{baseline['recall_by_exact_frontier']['1']['300']:.6f}` at "
            "K=300; 95% recovery requires K="
            f"{baseline['candidate_k_for_95_percent']['1']}. Recovering 95% "
            "of the entire exact top-10 requires K="
            f"{baseline['candidate_k_for_95_percent']['10']}.",
            "- Parameter provenance: A (`R=20`, `k_sim=5`, `d_proj=16`) is "
            "the paper's direct 10,240-D Pareto cell. Its headline "
            "final-projection experiment first builds `R=40`, `k_sim=6`, "
            "`d_proj=128` (327,680-D), then projects to 10,240-D; "
            "C-diagnostic is not a paper operating point.",
            "- Budget provenance: 10,240 is the selected Phase 2 paper point, "
            "not a hard product ceiling. The source design explicitly lists "
            "20,480 dimensions and frames affordability of dimension/K as "
            "the constraint.",
            "- Gate correction: the quality threshold remains 0.95 full "
            "exact-top-10 recovery. Candidate K is the smallest measured "
            "cutoff meeting it, bounded by the approved K=700 hard maximum; "
            "K=100 is a cost point, not the quality threshold.",
            "- Query augmentation: the pinned checkpoint sets "
            "`do_query_expansion=false` and "
            "`attend_to_expansion_tokens=false`; the lab therefore retains "
            "only attention-mask query rows. `[MASK]` padding is not an "
            "enabled semantic expansion for this checkpoint.",
        ]
    )
    if gap:
        lines.extend(
            [
                "",
                "| Exact-score quantity | p1 | p5 | p50 | p95 | p99 |",
                "| --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for label, key in (
            ("Rank 10 / query rows", "rank_10_score"),
            ("Rank 100 / query rows", "rank_100_score"),
            ("Rank 10 − rank 100 / query rows", "rank_10_to_rank_100_gap"),
            ("Gap / rank-10 score", "relative_gap_over_rank_10"),
        ):
            distribution = gap[key]
            lines.append(
                f"| {label} | {distribution['p01']:.6f} | "
                f"{distribution['p05']:.6f} | "
                f"{distribution['p50']:.6f} | "
                f"{distribution['p95']:.6f} | "
                f"{distribution['p99']:.6f} |"
            )
        lines.extend(
            [
                "",
                f"- Exact frontier-gap sample: {gap['query_count']} queries; "
                "scores are normalized by query rows.",
                "- Gap/recovery relationship: Pearson r="
                f"`{gap['gap_vs_top10_recovery_at_100_pearson']:.6f}`; "
                "top-10 R@100 is "
                f"`{gap['lowest_gap_decile_recall_at_100']:.6f}` in the "
                "smallest-gap decile and "
                f"`{gap['highest_gap_decile_recall_at_100']:.6f}` in the "
                "largest-gap decile.",
                "- Against A's centered-exact/FDE construction residual RMSE "
                f"of `{gap['construction_residual_rmse']:.6f}`, "
                f"{gap['fraction_gap_below_1x_construction_rmse']:.1%}/"
                f"{gap['fraction_gap_below_2x_construction_rmse']:.1%}/"
                f"{gap['fraction_gap_below_3x_construction_rmse']:.1%} of "
                "rank-10→rank-100 gaps are below 1×/2×/3× that scale.",
                "- Encoder checkpoint/seed stability is not measured by this "
                "one-checkpoint, one-seed phase. The gap distribution alone "
                "cannot justify changing the gate.",
            ]
        )
    return lines


def render_report(
    report_path: Path,
    pins: list[dict],
    preflight: dict,
    text_result: dict | None,
    text_cost: dict | None,
    visual_result: dict | None,
    visual_cost: dict | None,
) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    diagnostic_evidence = (
        write_diagnostic_artifacts(report_path, text_result)
        if text_result is not None
        else {}
    )
    visual_diagnostic_evidence = (
        write_visual_diagnostic_artifacts(report_path, visual_result)
        if visual_result is not None
        else {}
    )
    selected_int8_variant = selected_int8_variant_name(
        text_result, visual_result
    )
    lines = [
        "# MMLI-2 Phase 2 encoder qualification",
        "",
        "One seed and one repeat were used. Model and dataset files were "
        "downloaded at exact revisions once; encoder execution then used "
        "`HF_HUB_OFFLINE=1`, `TRANSFORMERS_OFFLINE=1`, "
        "`local_files_only=True`, and `trust_remote_code=False`.",
        "",
        "## Pinned artifacts",
        "",
        "| Artifact | Revision | SHA-256 |",
        "| --- | --- | --- |",
    ]
    lines.extend(
        f"| `{row['artifact']}` | `{row['revision']}` | `{row['sha256']}` |"
        for row in pins
    )
    lines.extend(
        [
            "",
            "## Visual loader preflight",
            "",
            f"- Base: `{preflight['base_revision']}`",
            f"- Unmerged LoRA: `{preflight['adapter_revision']}`",
            "- Adapter config base locator redirected to the exact local pinned base.",
            f"- Active LoRA modules: {preflight['lora_modules']} (178 tensors).",
            "- Query and image forward plus native `score_retrieval`: passed.",
            "- Remote code: disabled.",
        ]
    )
    if text_result is not None:
        lines.extend(
            render_lane(
                "Text",
                text_result,
                text_cost,
                selected_int8_variant,
            )
        )
        lines.extend(render_diagnostics(diagnostic_evidence))
    if visual_result is not None:
        lines.extend(
            render_lane(
                "Visual",
                visual_result,
                visual_cost,
                selected_int8_variant,
            )
        )
        lines.extend(render_visual_diagnostics(visual_diagnostic_evidence))
    if text_result is not None and visual_result is not None:
        lines.extend(render_int8_matrix_probe(text_result, visual_result))
    if text_result is not None:
        lines.extend(render_decisions(text_result, visual_result))
    report_path.write_text("\n".join(lines) + "\n")


def render_lane(
    name: str,
    result: dict,
    cost: dict | None,
    selected_int8_variant: str | None,
) -> list[str]:
    lines = ["", f"## {name} lane", ""]
    if name == "Text":
        lines.extend(
            [
                "- Official scorer: pinned PyLate `MaxSim` pairwise formula "
                "(sum over query rows of maximum token dot product).",
                "- Row normalization: encoder L2-normalizes every retained row "
                "before f16 exchange; documents retain attention-mask rows "
                "except punctuation, queries retain attention-mask rows; no "
                "post-f16 renormalization. The pinned checkpoint disables "
                "query expansion, so masked padding rows are excluded.",
            ]
        )
    else:
        lines.extend(
            [
                "- Official scorer: native "
                "`ColModernVBertProcessor.score_retrieval`.",
                "- Row normalization: the pinned model L2-normalizes rows in "
                "its forward pass before f16 exchange; attention-mask rows are "
                "retained; no post-f16 renormalization.",
            ]
        )
    lines.append(f"- Gate passed: `{str(result['gate_passed']).lower()}`")
    parity = result.get("parity")
    if parity:
        lines.extend(
            [
                f"- Official-score pairs: {parity['pair_count']}",
                f"- MaxSim absolute error: "
                f"`{parity['max_absolute_error']:.8g}`",
            ]
        )
    lines.append(
        f"- MaxSim parity maximum relative error: "
        f"`{result['parity_max_relative_error']:.8g}`"
    )
    lines.extend(
        [
            "",
            "| Config | Algorithm | Centering | R@50 | R@100 | R@300 |",
            "| --- | --- | --- | ---: | ---: | ---: |",
        ]
    )
    for cell in result["cells"]:
        lines.append(
            f"| {cell['config']} | {cell['algorithm']} | "
            f"{cell['centering']} | {cell['recall_at_50']:.6f} | "
            f"{cell['recall_at_100']:.6f} | "
            f"{cell['recall_at_300']:.6f} |"
        )
    winner = result.get("winner")
    if winner:
        lines.extend(
            [
                "",
                "### Decision",
                "",
                f"- Algorithm: `{winner['algorithm']}`",
                f"- FDE config: `{winner['config']}`",
                f"- VectorTransformRecipe: `{winner['centering']}`",
                "- Candidate document pooling: "
                f"`{winner['document_pooling_factor']}×`",
                f"- Candidate K: `{winner['candidate_k']}`",
            ]
        )
    if cost:
        lines.extend(
            [
                "",
                "### Encoder cost",
                "",
                "| Role | Count | CPU s/item batch 1 | CPU s/item batch 8 | "
                "Wall s/item batch 8 | Peak RSS MiB |",
                "| --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for role in ("documents", "queries"):
            row = cost[role]
            lines.append(
                f"| {role} | {row['count']} | "
                f"{row['batch_1_cpu_seconds_per_item']:.6f} | "
                f"{row['batch_8_cpu_seconds_per_item']:.6f} | "
                f"{row['batch_8_wall_seconds_per_item']:.6f} | "
                f"{row['peak_rss_mib']:.1f} |"
            )
    corpus = result.get("corpus_stats")
    if corpus:
        unit = "document" if name == "Text" else "page"
        truth_bytes = corpus["mean_rows"] * corpus["dim"] * 4
        fde_dimensions = {}
        for cell in result["cells"]:
            fde_dimensions.setdefault(cell["config"], cell["output_dimension"])
        lines.extend(
            [
                "",
                "### Storage cost",
                "",
                f"- Multi-vector f32 truth: `{truth_bytes:.1f}` bytes/{unit} "
                f"(mean {corpus['mean_rows']:.3f} rows × "
                f"{corpus['dim']} × 4 bytes).",
            ]
        )
        probe = result.get("int8_probe")
        if probe is not None and selected_int8_variant is not None:
            variant = next(
                (
                    row
                    for row in probe["variants"]
                    if row["variant"] == selected_int8_variant
                ),
                None,
            )
            if variant is None:
                raise RuntimeError(
                    f"{name} int8 probe omitted selected variant "
                    f"{selected_int8_variant}"
                )
            lines.append(
                f"- Selected int8 matrix payload "
                f"(`{selected_int8_variant}`): "
                f"`{variant['mean_payload_bytes_per_document']:.1f}` "
                f"bytes/{unit}."
            )
        for config in sorted(fde_dimensions):
            dimension = fde_dimensions[config]
            lines.append(
                f"- Config {config} FDE: `{dimension * 4}` bytes/retrieval "
                f"unit ({dimension} f32 coordinates)."
            )
    for key in ("corpus_stats", "query_stats", "geometry", "routing"):
        if key in result and result[key]:
            lines.extend(
                [
                    "",
                    f"### {key.replace('_', ' ').title()}",
                    "",
                    "```json",
                    json.dumps(result[key], indent=2, sort_keys=True),
                    "```",
                ]
            )
    return lines


def selected_int8_variant_name(
    text_result: dict | None,
    visual_result: dict | None,
) -> str | None:
    if text_result is None or visual_result is None:
        return None
    text_probe = text_result.get("int8_probe")
    visual_probe = visual_result.get("int8_probe")
    if text_probe is None or visual_probe is None:
        return None
    text_by_name = {
        row["variant"]: bool(row["passed"])
        for row in text_probe["variants"]
    }
    visual_by_name = {
        row["variant"]: bool(row["passed"])
        for row in visual_probe["variants"]
    }
    if set(text_by_name) != set(visual_by_name):
        raise RuntimeError("text and visual int8 probe variants differ")
    expected_names = {"global_scale", "per_row_calibrated"}
    if set(text_by_name) != expected_names:
        raise RuntimeError("int8 probe variant names are not recognized")
    global_name = "global_scale"
    if text_by_name[global_name] and visual_by_name[global_name]:
        return global_name
    calibrated_name = "per_row_calibrated"
    if (
        text_by_name[calibrated_name]
        and visual_by_name[calibrated_name]
    ):
        return calibrated_name
    return None


def render_int8_matrix_probe(
    text_result: dict,
    visual_result: dict,
) -> list[str]:
    text_probe = text_result.get("int8_probe")
    visual_probe = visual_result.get("int8_probe")
    if text_probe is None and visual_probe is None:
        return []
    if text_probe is None or visual_probe is None:
        raise RuntimeError("int8 probe result must exist for both lanes")
    selected = selected_int8_variant_name(text_result, visual_result)
    overall_passed = selected is not None
    global_scale_overall_passed = bool(
        text_probe["global_scale_passed"]
        and visual_probe["global_scale_passed"]
    )
    calibrated_overall_passed = all(
        next(
            row["passed"]
            for row in probe["variants"]
            if row["variant"] == "per_row_calibrated"
        )
        for probe in (text_probe, visual_probe)
    )
    lines = [
        "",
        "## int8 matrix payload probe",
        "",
        f"- Text candidate recipe: `{text_probe['candidate_config']}`, "
        f"`{text_probe['candidate_centering']}`, document pooling "
        f"`{text_probe['candidate_document_pooling_factor']}×`.",
        f"- Visual candidate recipe: `{visual_probe['candidate_config']}`, "
        f"`{visual_probe['candidate_centering']}`, document pooling "
        f"`{visual_probe['candidate_document_pooling_factor']}×`.",
        "",
        "| Lane | Variant | f32/int8 top-1 same | "
        "f32 top-10 recovered | Max error (50 pairs) | "
        "Row L2 p50/p95/p99 | R@50 | R@100 | R@300 | "
        "Bytes/unit | Passed |",
        "| --- | --- | ---: | ---: | ---: | --- | ---: | ---: | "
        "---: | ---: | --- |",
    ]
    for lane, probe in (("Text", text_probe), ("Visual", visual_probe)):
        for variant in probe["variants"]:
            lines.append(
                f"| {lane} | `{variant['variant']}` | "
                f"{variant['f32_top_1_same_rank_fraction']:.6f} | "
                f"{variant['f32_top_10_recall_in_int8_top_10']:.6f} | "
                f"{variant['max_sim_50_pair_max_absolute_error']:.8g} | "
                f"{variant['first_100_document_row_l2_error_p50']:.6f}/"
                f"{variant['first_100_document_row_l2_error_p95']:.6f}/"
                f"{variant['first_100_document_row_l2_error_p99']:.6f} | "
                f"{variant['candidate_recall_at_50']:.6f} | "
                f"{variant['candidate_recall_at_100']:.6f} | "
                f"{variant['candidate_recall_at_300']:.6f} | "
                f"{variant['mean_payload_bytes_per_document']:.1f} | "
                f"`{str(variant['passed']).lower()}` |"
            )
    lines.extend(
        [
            "",
            "- Global-scale overall passed: "
            f"`{str(global_scale_overall_passed).lower()}`.",
            "- Per-row calibrated overall passed: "
            f"`{str(calibrated_overall_passed).lower()}`.",
            f"- Overall passed: `{str(overall_passed).lower()}`.",
            (
                f"- Decision: approve `{selected}` int8 document matrix "
                "payloads for Phases 6, 7, and 9; query matrices remain f16."
                if overall_passed
                else "- Decision: reject int8 document matrix payloads; "
                "retain f16 for Phase 6."
            ),
            "- The binding candidate gate remains measured against f32 exact "
            "truth. These probe readouts keep the FDE ranking unchanged and "
            "use each int8 variant's exact top-10 as the diagnostic truth.",
        ]
    )
    return lines


def render_decisions(
    text_result: dict, visual_result: dict | None
) -> list[str]:
    text_winner = text_result["winner"]
    lines = [
        "",
        "## Named decisions and resolved lateon unknowns",
        "",
    ]
    if text_result["gate_passed"]:
        lines.append(f"- Candidate algorithm: `{text_winner['algorithm']}`.")
    else:
        lines.append(
            "- No-go: no candidate operating point is authorized because the "
            "text recall gate failed."
        )
        lines.append(
            f"- Best observed algorithm: `{text_winner['algorithm']}`."
        )
    lines.extend(
        [
        f"- Candidate transform: `{text_winner['centering']}`; the mean "
        "is computed from at most 5,000 evenly spaced document rows and "
        "the same frozen mean is applied to queries and documents. "
        "Centering is candidate-only; official exact MaxSim remains raw.",
        f"- Text operating point: config `{text_winner['config']}`, "
        f"D={text_winner['output_dimension']}, "
        f"K={text_winner['candidate_k']}, measured full-top-10 candidate "
        f"recall={text_winner['recall']:.6f}.",
        "- Exact-scoring transform: `Identity` over the model-normalized, "
        "row-filtered matrix.",
        "- Lab execution adapter: pinned Transformers CPU with remote code "
        "disabled; visual LoRA remains active and unmerged.",
        "- Artifact reproducibility: exact model/dataset revisions and every "
        "downloaded artifact SHA-256 are recorded above.",
        ]
    )
    routing = text_result.get("routing")
    if routing:
        best_readout = max(
            routing["readouts"],
            key=lambda row: (
                row["recall_at_100"],
                row["nprobe"],
                row["metric"] == "dot",
            ),
        )
        lines.append(
            f"- Routing metric: `{best_readout['metric']}` at "
            f"nprobe={best_readout['nprobe']} "
            f"(R@100={best_readout['recall_at_100']:.6f}, "
            f"nlist={routing['nlist']})."
        )
    if visual_result is None:
        lines.append(
            "- Visual-transfer decisions remain unresolved because the text "
            "gate stopped or the visual lane has not run."
        )
    else:
        visual_winner = visual_result["winner"]
        lines.extend(
            [
                f"- Visual operating point: config "
                f"`{visual_winner['config']}`, "
                f"candidate document pooling="
                f"{visual_winner['document_pooling_factor']}×, "
                f"D={visual_winner['output_dimension']}, "
                f"K={visual_winner['candidate_k']}, measured full-top-10 "
                f"candidate recall={visual_winner['recall']:.6f}.",
                "- Visual FDE geometry, centering, and candidate recall are "
                "reported above; PQ was not run (optional stretch skipped).",
                "- The pinned Hugging Face-native visual loader avoids remote "
                "code. Export to another runtime was not tested.",
                "- Exact ViDoRe task revisions and artifact hashes are recorded; "
                "no leaderboard scores from another revision were imported.",
            ]
        )
    return lines


def delete_transient_lab_artifacts(work_dir: Path) -> None:
    paths = [
        work_dir / "text-encoding-cost.json",
        work_dir / "visual-encoding-cost.json",
    ]
    for lane in ("text", "visual"):
        paths.extend(
            [
                work_dir / f"{lane}-documents.f16",
                work_dir / f"{lane}-documents.json",
                work_dir / f"{lane}-queries.f16",
                work_dir / f"{lane}-queries.json",
                work_dir / f"{lane}-pairs.json",
                work_dir / f"{lane}-job.json",
                work_dir / f"{lane}-result.json",
            ]
        )
    paths.extend(
        [
            work_dir / "visual-documents-f32.f32",
            work_dir / "visual-documents-f32.json",
            work_dir / "visual-queries-f32.f32",
            work_dir / "visual-queries-f32.json",
        ]
    )
    for path in paths:
        if path.exists():
            path.unlink()


def text_lane(
    hf_home: Path,
    work_dir: Path,
    binary: Path,
    int8_probe: bool,
):
    model_path = snapshot_path(hf_home, TEXT_REPO, TEXT_REVISION, "model")
    dataset_path = snapshot_path(
        hf_home, SCIFACT_REPO, SCIFACT_REVISION, "dataset"
    )
    corpus = load_parquet_directory(dataset_path / "corpus")
    queries = load_parquet_directory(dataset_path / "queries")
    document_ids = [str(value) for value in corpus["_id"]]
    document_texts = [
        f"{title} {text}".strip() for title, text in zip(corpus["title"], corpus["text"])
    ]
    query_ids = [str(value) for value in queries["_id"]]
    query_texts = [
        f"{title} {text}".strip()
        for title, text in zip(queries["title"], queries["text"])
    ]
    cost_path = work_dir / "text-encoding-cost.json"
    cached_paths = (
        work_dir / "text-documents.f16",
        work_dir / "text-documents.json",
        work_dir / "text-queries.f16",
        work_dir / "text-queries.json",
        cost_path,
    )
    cache_available = all(path.is_file() for path in cached_paths)
    if int8_probe and not cache_available:
        missing = [str(path) for path in cached_paths if not path.is_file()]
        raise FileNotFoundError(
            "int8 probe requires the retained text f16 cache; missing "
            + ", ".join(missing)
        )
    full_precision_documents = None
    full_precision_queries = None
    if cache_available:
        timing = json.loads(cost_path.read_text())
        documents = read_tensor(
            work_dir / "text-documents", timing["documents"]
        )
        text_queries = read_tensor(
            work_dir / "text-queries", timing["queries"]
        )
        if documents.ids != document_ids or text_queries.ids != query_ids:
            raise RuntimeError("cached text tensor IDs do not match pinned SciFact")
        print("reused validated pinned SciFact tensors", flush=True)
        if int8_probe:
            encoder = TextEncoder(model_path)
            full_precision_documents = text_encoding_once(
                encoder, document_texts, document_ids, is_query=False
            )
            full_precision_queries = text_encoding_once(
                encoder, query_texts, query_ids, is_query=True
            )
            del encoder
            validate_precision_reference(
                documents,
                full_precision_documents,
                "text",
                "documents",
            )
            validate_precision_reference(
                text_queries,
                full_precision_queries,
                "text",
                "queries",
            )
    else:
        encoder = TextEncoder(model_path)
        documents = timed_text_encoding(
            encoder, document_texts, document_ids, is_query=False
        )
        text_queries = timed_text_encoding(
            encoder, query_texts, query_ids, is_query=True
        )
        del encoder
        cost_path.write_text(
            json.dumps(
                {
                    "documents": {
                        "batch_1": documents.batch_one_cpu_seconds_per_item,
                        "batch_8": documents.batch_eight_cpu_seconds_per_item,
                        "batch_8_wall": (
                            documents.batch_eight_wall_seconds_per_item
                        ),
                        "peak_rss_mib": documents.peak_rss_mib,
                    },
                    "queries": {
                        "batch_1": text_queries.batch_one_cpu_seconds_per_item,
                        "batch_8": text_queries.batch_eight_cpu_seconds_per_item,
                        "batch_8_wall": (
                            text_queries.batch_eight_wall_seconds_per_item
                        ),
                        "peak_rss_mib": text_queries.peak_rss_mib,
                    },
                },
                indent=2,
            )
            + "\n"
        )
    result = run_rust(
        binary,
        work_dir,
        "text",
        documents,
        text_queries,
        text_official_pair_scores(text_queries, documents),
        full_precision_documents=full_precision_documents,
        full_precision_queries=full_precision_queries,
        int8_probe=int8_probe,
        reuse_primary_exchange=int8_probe,
    )
    return result, encoding_metadata(documents, text_queries)


def visual_lane(
    hf_home: Path,
    work_dir: Path,
    binary: Path,
    chosen_algorithm: str,
    chosen_centering: str,
    int8_probe: bool,
):
    base_path = snapshot_path(
        hf_home, VISUAL_BASE_REPO, VISUAL_BASE_REVISION, "model"
    )
    adapter_path = snapshot_path(
        hf_home, VISUAL_ADAPTER_REPO, VISUAL_ADAPTER_REVISION, "model"
    )
    images: list[Image.Image] = []
    document_ids: list[str] = []
    query_texts: list[str] = []
    query_ids: list[str] = []
    for label, repo, revision in VIDORE:
        dataset_path = snapshot_path(hf_home, repo, revision, "dataset")
        corpus = load_parquet_directory(dataset_path / "corpus")
        ordered = sorted(corpus, key=lambda row: int(row["corpus_id"]))[
            :VISUAL_TASK_CAP
        ]
        if len(ordered) != VISUAL_TASK_CAP:
            raise RuntimeError(
                f"{label} has only {len(ordered)} corpus rows after deterministic cap"
            )
        images.extend(row["image"] for row in ordered)
        document_ids.extend(
            f"{label}:{int(row['corpus_id'])}" for row in ordered
        )
        queries = load_parquet_directory(dataset_path / "queries")
        english = sorted(
            (row for row in queries if row["language"] == "english"),
            key=lambda row: int(row["query_id"]),
        )
        query_texts.extend(str(row["query"]) for row in english)
        query_ids.extend(f"{label}:{int(row['query_id'])}" for row in english)
    cost_path = work_dir / "visual-encoding-cost.json"
    cached_paths = (
        work_dir / "visual-documents.f16",
        work_dir / "visual-documents.json",
        work_dir / "visual-queries.f16",
        work_dir / "visual-queries.json",
        cost_path,
    )
    cache_available = all(path.is_file() for path in cached_paths)
    if int8_probe and not cache_available:
        missing = [str(path) for path in cached_paths if not path.is_file()]
        raise FileNotFoundError(
            "int8 probe requires the retained visual f16 cache; missing "
            + ", ".join(missing)
        )
    full_precision_documents = None
    full_precision_queries = None
    if cache_available:
        timing = json.loads(cost_path.read_text())
        documents = read_tensor(
            work_dir / "visual-documents", timing["documents"]
        )
        visual_queries = read_tensor(
            work_dir / "visual-queries", timing["queries"]
        )
        if documents.ids != document_ids or visual_queries.ids != query_ids:
            raise RuntimeError(
                "cached visual tensor IDs do not match pinned ViDoRe tasks"
            )
        with tempfile.TemporaryDirectory(dir=work_dir) as temporary:
            if int8_probe:
                model, processor = load_visual_model(
                    base_path, adapter_path, Path(temporary)
                )
                full_precision_documents = visual_encoding_once(
                    model,
                    processor,
                    images,
                    document_ids,
                    is_query=False,
                )
                full_precision_queries = visual_encoding_once(
                    model,
                    processor,
                    query_texts,
                    query_ids,
                    is_query=True,
                )
                del model
                validate_precision_reference(
                    documents,
                    full_precision_documents,
                    "visual",
                    "documents",
                )
                validate_precision_reference(
                    visual_queries,
                    full_precision_queries,
                    "visual",
                    "queries",
                )
            else:
                local_adapter = redirected_visual_adapter(
                    adapter_path,
                    base_path,
                    Path(temporary) / "visual-adapter-local",
                )
                processor = ColModernVBertProcessor.from_pretrained(
                    local_adapter,
                    trust_remote_code=False,
                    local_files_only=True,
                )
            official_scores = visual_official_pair_scores(
                processor, visual_queries, documents
            )
        print("reused validated pinned ViDoRe tensors", flush=True)
    else:
        with tempfile.TemporaryDirectory(dir=work_dir) as temporary:
            model, processor = load_visual_model(
                base_path, adapter_path, Path(temporary)
            )
            documents = timed_visual_encoding(
                model, processor, images, document_ids, is_query=False
            )
            visual_queries = timed_visual_encoding(
                model, processor, query_texts, query_ids, is_query=True
            )
            official_scores = visual_official_pair_scores(
                processor, visual_queries, documents
            )
            full_precision_documents = documents
            full_precision_queries = visual_queries
            del model, processor
        cost_path.write_text(
            json.dumps(
                {
                    "documents": {
                        "batch_1": documents.batch_one_cpu_seconds_per_item,
                        "batch_8": documents.batch_eight_cpu_seconds_per_item,
                        "batch_8_wall": (
                            documents.batch_eight_wall_seconds_per_item
                        ),
                        "peak_rss_mib": documents.peak_rss_mib,
                    },
                    "queries": {
                        "batch_1": visual_queries.batch_one_cpu_seconds_per_item,
                        "batch_8": visual_queries.batch_eight_cpu_seconds_per_item,
                        "batch_8_wall": (
                            visual_queries.batch_eight_wall_seconds_per_item
                        ),
                        "peak_rss_mib": visual_queries.peak_rss_mib,
                    },
                },
                indent=2,
            )
            + "\n"
        )
    result = run_rust(
        binary,
        work_dir,
        "visual",
        documents,
        visual_queries,
        official_scores,
        chosen_algorithm,
        chosen_centering,
        full_precision_documents=full_precision_documents,
        full_precision_queries=full_precision_queries,
        int8_probe=int8_probe,
        reuse_primary_exchange=int8_probe,
    )
    return result, encoding_metadata(documents, visual_queries)


def encode_text_precision_ranking(
    hf_home: Path,
) -> tuple[EncodingResult, EncodingResult]:
    model_path = snapshot_path(hf_home, TEXT_REPO, TEXT_REVISION, "model")
    dataset_path = snapshot_path(
        hf_home, SCIFACT_REPO, SCIFACT_REVISION, "dataset"
    )
    corpus = load_parquet_directory(dataset_path / "corpus")
    queries = load_parquet_directory(dataset_path / "queries")
    document_ids = [str(value) for value in corpus["_id"]]
    document_texts = [
        f"{title} {text}".strip()
        for title, text in zip(corpus["title"], corpus["text"])
    ]
    query_ids = [str(value) for value in queries["_id"]]
    query_texts = [
        f"{title} {text}".strip()
        for title, text in zip(queries["title"], queries["text"])
    ]
    encoder = TextEncoder(model_path)
    documents = text_encoding_once(
        encoder,
        document_texts,
        document_ids,
        is_query=False,
        progress_label="ranking f32",
    )
    encoded_queries = text_encoding_once(
        encoder,
        query_texts,
        query_ids,
        is_query=True,
        progress_label="ranking f32",
    )
    del encoder
    return documents, encoded_queries


def encode_visual_precision_ranking(
    hf_home: Path,
    work_dir: Path,
) -> tuple[EncodingResult, EncodingResult]:
    base_path = snapshot_path(
        hf_home, VISUAL_BASE_REPO, VISUAL_BASE_REVISION, "model"
    )
    adapter_path = snapshot_path(
        hf_home, VISUAL_ADAPTER_REPO, VISUAL_ADAPTER_REVISION, "model"
    )
    images: list[Image.Image] = []
    document_ids: list[str] = []
    query_texts: list[str] = []
    query_ids: list[str] = []
    for label, repo, revision in VIDORE:
        dataset_path = snapshot_path(hf_home, repo, revision, "dataset")
        corpus = load_parquet_directory(dataset_path / "corpus")
        ordered = sorted(corpus, key=lambda row: int(row["corpus_id"]))[
            :VISUAL_TASK_CAP
        ]
        if len(ordered) != VISUAL_TASK_CAP:
            raise RuntimeError(
                f"{label} has only {len(ordered)} corpus rows after "
                "deterministic cap"
            )
        images.extend(row["image"] for row in ordered)
        document_ids.extend(
            f"{label}:{int(row['corpus_id'])}" for row in ordered
        )
        queries = load_parquet_directory(dataset_path / "queries")
        english = sorted(
            (row for row in queries if row["language"] == "english"),
            key=lambda row: int(row["query_id"]),
        )
        query_texts.extend(str(row["query"]) for row in english)
        query_ids.extend(
            f"{label}:{int(row['query_id'])}" for row in english
        )
    with tempfile.TemporaryDirectory(dir=work_dir) as temporary:
        model, processor = load_visual_model(
            base_path,
            adapter_path,
            Path(temporary),
        )
        documents = visual_encoding_once(
            model,
            processor,
            images,
            document_ids,
            is_query=False,
            progress_label="ranking f32",
        )
        encoded_queries = visual_encoding_once(
            model,
            processor,
            query_texts,
            query_ids,
            is_query=True,
            progress_label="ranking f32",
        )
        del model, processor
    return documents, encoded_queries


def run_precision_ranking_job(
    binary: Path,
    work_dir: Path,
    lane: str,
    documents: EncodingResult,
    queries: EncodingResult,
    expected_candidates: Sequence[str],
    send_candidate_selection: bool,
) -> tuple[dict, tuple[Path, ...]]:
    prefix = f"ranking-{lane}"
    doc_raw, doc_sidecar = write_tensor(
        work_dir / f"{prefix}-documents",
        documents,
        "f16",
    )
    query_raw, query_sidecar = write_tensor(
        work_dir / f"{prefix}-queries",
        queries,
        "f16",
    )
    f32_doc_raw, f32_doc_sidecar = write_tensor(
        work_dir / f"{prefix}-documents-f32",
        documents,
        "f32",
    )
    f32_query_raw, f32_query_sidecar = write_tensor(
        work_dir / f"{prefix}-queries-f32",
        queries,
        "f32",
    )
    job = {
        "lane": lane,
        "documents": {"raw": str(doc_raw), "sidecar": str(doc_sidecar)},
        "queries": {"raw": str(query_raw), "sidecar": str(query_sidecar)},
        "precision_ranking_audit": True,
        "full_precision_documents": {
            "raw": str(f32_doc_raw),
            "sidecar": str(f32_doc_sidecar),
        },
        "full_precision_queries": {
            "raw": str(f32_query_raw),
            "sidecar": str(f32_query_sidecar),
        },
    }
    if send_candidate_selection:
        job["precision_ranking_candidates"] = list(expected_candidates)
    job_path = work_dir / f"{prefix}-job.json"
    job_path.write_text(json.dumps(job, indent=2) + "\n")
    completed = subprocess.run(
        [binary, job_path],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    result = json.loads(completed.stdout)
    validate_precision_ranking_audit(
        result,
        lane,
        queries,
        expected_candidates,
    )
    return result, (
        doc_raw,
        doc_sidecar,
        query_raw,
        query_sidecar,
        f32_doc_raw,
        f32_doc_sidecar,
        f32_query_raw,
        f32_query_sidecar,
        job_path,
    )


def run_production_int8_qualification_job(
    binary: Path,
    work_dir: Path,
    lane: str,
    documents: EncodingResult,
    queries: EncodingResult,
    group_sizes: Sequence[int],
    artifact_directory: Path,
) -> tuple[dict, tuple[Path, ...]]:
    prefix = f"production-int8-{lane}"
    doc_raw, doc_sidecar = write_tensor(
        work_dir / f"{prefix}-documents",
        documents,
        "f16",
    )
    query_raw, query_sidecar = write_tensor(
        work_dir / f"{prefix}-queries",
        queries,
        "f16",
    )
    f32_doc_raw, f32_doc_sidecar = write_tensor(
        work_dir / f"{prefix}-documents-f32",
        documents,
        "f32",
    )
    f32_query_raw, f32_query_sidecar = write_tensor(
        work_dir / f"{prefix}-queries-f32",
        queries,
        "f32",
    )
    job = {
        "lane": lane,
        "documents": {"raw": str(doc_raw), "sidecar": str(doc_sidecar)},
        "queries": {"raw": str(query_raw), "sidecar": str(query_sidecar)},
        "full_precision_documents": {
            "raw": str(f32_doc_raw),
            "sidecar": str(f32_doc_sidecar),
        },
        "full_precision_queries": {
            "raw": str(f32_query_raw),
            "sidecar": str(f32_query_sidecar),
        },
        "production_int8_qualification": {
            "group_sizes": list(group_sizes),
            "artifact_directory": str(artifact_directory),
        },
    }
    job_path = work_dir / f"{prefix}-job.json"
    job_path.write_text(json.dumps(job, indent=2) + "\n")
    completed = subprocess.run(
        [binary, job_path],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    result = json.loads(completed.stdout)
    validate_production_int8_qualification(
        result,
        lane,
        queries,
        group_sizes,
        artifact_directory,
    )
    artifact_paths = tuple(
        artifact_directory / f"{lane}-int8-sym-v1-g{size}.zme1"
        for size in group_sizes
    )
    return result, (
        doc_raw,
        doc_sidecar,
        query_raw,
        query_sidecar,
        f32_doc_raw,
        f32_doc_sidecar,
        f32_query_raw,
        f32_query_sidecar,
        job_path,
        *artifact_paths,
    )


def atomic_write_json(path: Path, payload: dict) -> None:
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as output:
            temporary_path = Path(output.name)
            json.dump(payload, output, indent=2, sort_keys=True)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_path, path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def write_precision_ranking_probe(
    path: Path,
    lane: str,
    result: dict,
) -> None:
    if path.is_file():
        payload = json.loads(path.read_text())
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version") != 1
            or not isinstance(payload.get("lanes"), dict)
        ):
            raise RuntimeError(
                f"existing ranking probe has an incompatible schema: {path}"
            )
    else:
        payload = {"schema_version": 1, "lanes": {}}
    payload["lanes"] = {**payload["lanes"], lane: result}
    for lane_result in payload["lanes"].values():
        backfill_retrieval_decode_cost(lane_result)
    atomic_write_json(path, payload)


def write_production_int8_qualification(
    path: Path,
    lane: str,
    result: dict,
) -> None:
    if path.is_file():
        payload = json.loads(path.read_text())
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version") != 1
            or payload.get("mode")
            != "production_int8_matrix_qualification"
            or not isinstance(payload.get("lanes"), dict)
        ):
            raise RuntimeError(
                f"existing production INT8 result has an incompatible schema: {path}"
            )
    else:
        payload = {
            "schema_version": 1,
            "mode": "production_int8_matrix_qualification",
            "decision_state": "measured_awaiting_operator_threshold",
            "lanes": {},
        }
    payload["lanes"] = {**payload["lanes"], lane: result}
    atomic_write_json(path, payload)


def run_precision_ranking_audit(args) -> None:
    require_offline()
    expected_candidates, send_candidate_selection = (
        resolve_precision_ranking_candidates(args.lane, args.candidates)
    )
    hf_home = args.hf_home.resolve()
    work_dir = args.work_dir.resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    probe_output = args.probe_output.resolve()
    if probe_output.suffix.lower() != ".json":
        raise RuntimeError("--probe-output must name a dedicated JSON file")
    prefix = f"ranking-{args.lane}"
    reserved_outputs = {
        (work_dir / f"{prefix}-{suffix}").resolve()
        for suffix in (
            "documents.json",
            "queries.json",
            "documents-f32.json",
            "queries-f32.json",
            "job.json",
        )
    }
    if probe_output in reserved_outputs:
        raise RuntimeError(
            "--probe-output must not overwrite a ranking-audit sidecar"
        )
    probe_output.parent.mkdir(parents=True, exist_ok=True)
    if args.lane == "text":
        documents, queries = encode_text_precision_ranking(hf_home)
    else:
        documents, queries = encode_visual_precision_ranking(
            hf_home,
            work_dir,
        )
    result, ranking_artifacts = run_precision_ranking_job(
        args.binary.resolve(),
        work_dir,
        args.lane,
        documents,
        queries,
        expected_candidates,
        send_candidate_selection,
    )
    write_precision_ranking_probe(probe_output, args.lane, result)
    if not args.retain_tensors:
        for path in ranking_artifacts:
            path.unlink()
    print(f"wrote {args.lane} ranking retention: {probe_output}", flush=True)


def run_production_int8_qualification(args) -> None:
    require_offline()
    group_sizes = resolve_production_int8_group_sizes(args.group_sizes)
    hf_home = args.hf_home.resolve()
    work_dir = args.work_dir.resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    probe_output = args.probe_output.resolve()
    if probe_output.suffix.lower() != ".json":
        raise RuntimeError("--probe-output must name a dedicated JSON file")
    prefix = f"production-int8-{args.lane}"
    reserved_outputs = {
        (work_dir / f"{prefix}-{suffix}").resolve()
        for suffix in (
            "documents.json",
            "queries.json",
            "documents-f32.json",
            "queries-f32.json",
            "job.json",
        )
    }
    if probe_output in reserved_outputs:
        raise RuntimeError(
            "--probe-output must not overwrite a production qualification sidecar"
        )
    probe_output.parent.mkdir(parents=True, exist_ok=True)
    artifact_directory = Path(
        tempfile.mkdtemp(
            prefix=f"{prefix}-artifacts-",
            dir=work_dir,
        )
    )
    if args.lane == "text":
        documents, queries = encode_text_precision_ranking(hf_home)
    else:
        documents, queries = encode_visual_precision_ranking(
            hf_home,
            work_dir,
        )
    result, transient_paths = run_production_int8_qualification_job(
        args.binary.resolve(),
        work_dir,
        args.lane,
        documents,
        queries,
        group_sizes,
        artifact_directory,
    )
    write_production_int8_qualification(
        probe_output,
        args.lane,
        result,
    )
    durable = json.loads(probe_output.read_text())
    durable_lane = durable.get("lanes", {}).get(args.lane)
    if (
        not isinstance(durable_lane, dict)
        or durable_lane.get("evidence_digest_sha256")
        != result["evidence_digest_sha256"]
    ):
        raise RuntimeError("durable production INT8 result verification failed")
    if not args.retain_tensors:
        for path in transient_paths:
            path.unlink()
        artifact_directory.rmdir()
    print(
        f"wrote {args.lane} production INT8 qualification "
        f"for groups {list(group_sizes)}: {probe_output}",
        flush=True,
    )
    if args.retain_tensors:
        print(
            f"retained production qualification artifacts: "
            f"{artifact_directory}",
            flush=True,
        )


def run(args) -> None:
    require_offline()
    hf_home = args.hf_home.resolve()
    work_dir = args.work_dir.resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    binary = args.binary.resolve()
    snapshots = [
        (
            TEXT_REPO,
            TEXT_REVISION,
            snapshot_path(hf_home, TEXT_REPO, TEXT_REVISION, "model"),
        ),
        (
            SCIFACT_REPO,
            SCIFACT_REVISION,
            snapshot_path(hf_home, SCIFACT_REPO, SCIFACT_REVISION, "dataset"),
        ),
        (
            VISUAL_BASE_REPO,
            VISUAL_BASE_REVISION,
            snapshot_path(
                hf_home, VISUAL_BASE_REPO, VISUAL_BASE_REVISION, "model"
            ),
        ),
        (
            VISUAL_ADAPTER_REPO,
            VISUAL_ADAPTER_REVISION,
            snapshot_path(
                hf_home, VISUAL_ADAPTER_REPO, VISUAL_ADAPTER_REVISION, "model"
            ),
        ),
        *[
            (
                repo,
                revision,
                snapshot_path(hf_home, repo, revision, "dataset"),
            )
            for _, repo, revision in VIDORE
        ],
    ]
    preflight = {
        "base_revision": VISUAL_BASE_REVISION,
        "adapter_revision": VISUAL_ADAPTER_REVISION,
        "lora_modules": 89,
    }
    text_result, text_cost = text_lane(
        hf_home,
        work_dir,
        binary,
        args.int8_probe,
    )
    pins = artifact_hashes(snapshots)
    render_report(
        args.report,
        pins,
        preflight,
        text_result,
        text_cost,
        None,
        None,
    )
    if not text_result["gate_passed"]:
        if not args.retain_tensors:
            delete_transient_lab_artifacts(work_dir)
        raise SystemExit("text candidate-recall gate failed; stopped")
    winner = text_result["winner"]
    visual_result, visual_cost = visual_lane(
        hf_home,
        work_dir,
        binary,
        winner["algorithm"],
        winner["centering"],
        args.int8_probe,
    )
    render_report(
        args.report,
        pins,
        preflight,
        text_result,
        text_cost,
        visual_result,
        visual_cost,
    )
    if not visual_result["gate_passed"]:
        if not args.retain_tensors:
            delete_transient_lab_artifacts(work_dir)
        raise SystemExit("visual candidate-recall gate failed; stopped")
    if not args.retain_tensors:
        delete_transient_lab_artifacts(work_dir)


def parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    download = subparsers.add_parser("download")
    download.add_argument("--hf-home", type=Path, required=True)
    execute = subparsers.add_parser("run")
    execute.add_argument("--hf-home", type=Path, required=True)
    execute.add_argument("--work-dir", type=Path, required=True)
    execute.add_argument("--binary", type=Path, required=True)
    execute.add_argument("--report", type=Path, required=True)
    execute.add_argument("--retain-tensors", action="store_true")
    execute.add_argument("--int8-probe", action="store_true")
    ranking = subparsers.add_parser("ranking-retention")
    ranking.add_argument("--lane", choices=("text", "visual"), required=True)
    ranking.add_argument(
        "--candidate",
        dest="candidates",
        action="append",
        choices=PRECISION_RANKING_NEW_CANDIDATES,
        help=(
            "add one new INT8 candidate after the automatic f16/global/"
            "per-row baselines; omit for text to run all seven, or repeat "
            "once or twice for visual in the requested order"
        ),
    )
    ranking.add_argument("--hf-home", type=Path, required=True)
    ranking.add_argument("--work-dir", type=Path, required=True)
    ranking.add_argument("--binary", type=Path, required=True)
    ranking.add_argument("--probe-output", type=Path, required=True)
    ranking.add_argument("--retain-tensors", action="store_true")
    qualification = subparsers.add_parser("production-int8-qualification")
    qualification.add_argument(
        "--lane",
        choices=("text", "visual"),
        required=True,
    )
    qualification.add_argument(
        "--group-size",
        dest="group_sizes",
        action="append",
        type=int,
        choices=PRODUCTION_INT8_GROUP_SIZES,
        help=(
            "production int8_sym_v1 group size; repeat for multiple cells; "
            "defaults to the mandatory group-32 attempt"
        ),
    )
    qualification.add_argument("--hf-home", type=Path, required=True)
    qualification.add_argument("--work-dir", type=Path, required=True)
    qualification.add_argument("--binary", type=Path, required=True)
    qualification.add_argument("--probe-output", type=Path, required=True)
    qualification.add_argument("--retain-tensors", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "download":
        download_pins(args.hf_home.resolve())
    elif args.command == "ranking-retention":
        run_precision_ranking_audit(args)
    elif args.command == "production-int8-qualification":
        run_production_int8_qualification(args)
    else:
        run(args)


if __name__ == "__main__":
    main()
