#!/usr/bin/env python3
"""Pinned offline encoder worker for MMLI-2.

The JSON control protocol is line-delimited. Matrix values never enter JSON:
each result is written as one raw little-endian f16 sidecar confined to the
session scratch directory. This process performs no downloads and accepts only
the two model layouts qualified by the Phase 2 lab.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

PROTOCOL_VERSION = 1
MANIFEST_NAME = "worker.json"
MAX_MANIFEST_BYTES = 1024 * 1024
DIMENSION = 128
PUNCTUATION = list('!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~')
MEDIA_FORMATS = {
    "image/jpeg": "JPEG",
    "image/png": "PNG",
    "image/webp": "WEBP",
}


class ProtocolError(RuntimeError):
    """A bounded, content-free diagnostic safe for the control channel."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class Limits:
    max_batch_units: int
    max_batch_input_bytes: int
    max_batch_pixels: int
    max_batch_rows: int
    max_tensor_bytes: int
    max_line_bytes: int


@dataclass(frozen=True)
class Identity:
    epoch_id: str
    implementation: str
    version: str
    preprocessing_digest: str
    supported_modalities: tuple[str, ...]
    artifact_digests: dict[str, str]
    output_dimension: int


class TextEncoder:
    """Exact GTE-ModernColBERT adapter qualified by the Phase 2 lab."""

    supported_modalities = ("text",)

    def __init__(self, snapshot: Path, dimension: int):
        import torch
        from safetensors.torch import load_file
        from transformers import AutoModel, AutoTokenizer

        if dimension != DIMENSION:
            raise ProtocolError(
                "unsupported_model",
                f"GTE-ModernColBERT requires output dimension {DIMENSION}",
            )
        self.torch = torch
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
        self.projection = torch.nn.Linear(768, dimension, bias=False)
        self.projection.load_state_dict({"weight": projection["linear.weight"]})
        self.projection.eval()
        self.query_prefix_id = self.tokenizer.convert_tokens_to_ids("[Q] ")
        self.document_prefix_id = self.tokenizer.convert_tokens_to_ids("[D] ")
        if self.query_prefix_id == self.tokenizer.unk_token_id:
            raise ProtocolError(
                "invalid_bundle", "pinned tokenizer does not contain [Q] prefix"
            )
        if self.document_prefix_id == self.tokenizer.unk_token_id:
            raise ProtocolError(
                "invalid_bundle", "pinned tokenizer does not contain [D] prefix"
            )
        self.skip_ids = {
            self.tokenizer.convert_tokens_to_ids(value) for value in PUNCTUATION
        }

    @staticmethod
    def _insert_prefix(values, prefix_id: int):
        import torch

        prefix = torch.full(
            (values.shape[0], 1),
            prefix_id,
            dtype=values.dtype,
            device=values.device,
        )
        return torch.cat((values[:, :1], prefix, values[:, 1:]), dim=1)

    def _encode(self, texts: Sequence[str], is_query: bool):
        limit = 47 if is_query else 299
        prefix_id = self.query_prefix_id if is_query else self.document_prefix_id
        tokens = self.tokenizer(
            list(texts),
            padding=True,
            truncation=True,
            max_length=limit,
            return_tensors="pt",
        )
        tokens["input_ids"] = self._insert_prefix(tokens["input_ids"], prefix_id)
        tokens["attention_mask"] = self._insert_prefix(tokens["attention_mask"], 1)
        if "token_type_ids" in tokens:
            tokens["token_type_ids"] = self._insert_prefix(
                tokens["token_type_ids"], 0
            )
        with self.torch.inference_mode():
            hidden = self.model(**tokens).last_hidden_state
            projected = self.torch.nn.functional.normalize(
                self.projection(hidden), p=2, dim=-1
            )
        results = []
        for index in range(projected.shape[0]):
            mask = tokens["attention_mask"][index].bool()
            if not is_query:
                for skip_id in self.skip_ids:
                    mask &= tokens["input_ids"][index] != skip_id
            matrix = projected[index][mask].cpu().numpy()
            if matrix.shape[0] == 0:
                raise ProtocolError(
                    "encoder_output", "text encoder produced an empty matrix"
                )
            results.append(matrix)
        return results

    def encode_documents(self, inputs: Sequence[dict[str, Any]]):
        texts = []
        for value in inputs:
            require_exact_keys(value, {"kind", "text"}, "text input")
            if value["kind"] != "text":
                raise ProtocolError(
                    "unsupported_modality",
                    "text encoder accepts only text documents",
                )
            texts.append(require_nonempty_text(value["text"], "document text"))
        return self._encode(texts, is_query=False)

    def encode_query(self, text: str):
        return self._encode([require_nonempty_text(text, "query text")], is_query=True)


class VisualEncoder:
    """Exact unmerged ColModernVBERT LoRA adapter qualified by Phase 2."""

    supported_modalities = ("image",)

    def __init__(
        self,
        base: Path,
        adapter: Path,
        scratch: Path,
        dimension: int,
        expected_lora_modules: int,
    ):
        import torch
        from transformers import (
            ColModernVBertProcessor,
            ModernVBertModel,
            ModernVBertPreTrainedModel,
        )
        from transformers.conversion_mapping import (
            get_checkpoint_conversion_mapping,
            register_checkpoint_conversion_mapping,
        )
        from transformers.core_model_loading import WeightRenaming

        if dimension != DIMENSION:
            raise ProtocolError(
                "unsupported_model",
                f"ColModernVBERT requires output dimension {DIMENSION}",
            )

        class ColModernVBert(ModernVBertPreTrainedModel):
            _checkpoint_conversion_mapping = {
                r"^base_model\.model\.model\.text_model": "model.text_model",
                r"^base_model\.model\.custom_text_proj": "custom_text_proj",
            }

            def __init__(self, config, mask_non_image_embeddings: bool = False, **kw):
                super().__init__(config=config)
                self.model = ModernVBertModel(config, **kw)
                self.dim = dimension
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
                return super().from_pretrained(
                    *args, **kwargs, key_mapping=key_mapping
                )

            def forward(self, *args, **kwargs):
                hidden = self.model(*args, **kwargs)[0]
                projected = self.custom_text_proj(hidden)
                projected = projected / projected.norm(
                    dim=-1, keepdim=True
                ).clamp_min(1.0e-12)
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

        local_adapter = scratch / "visual-adapter-local"
        shutil.copytree(adapter, local_adapter, symlinks=False)
        adapter_config_path = local_adapter / "adapter_config.json"
        adapter_config = bounded_json_file(adapter_config_path, MAX_MANIFEST_BYTES)
        adapter_config["base_model_name_or_path"] = str(base)
        adapter_config_path.write_text(
            json.dumps(adapter_config, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        self.torch = torch
        self.model = ColModernVBert.from_pretrained(
            local_adapter,
            trust_remote_code=False,
            local_files_only=True,
            torch_dtype=torch.float32,
        ).eval()
        lora_modules = [
            module
            for name, module in self.model.named_modules()
            if "lora_A.default" in name
        ]
        if len(lora_modules) != expected_lora_modules or not lora_modules:
            raise ProtocolError(
                "invalid_bundle",
                "visual adapter active LoRA module count does not match its pin",
            )
        for module in self.model.modules():
            if getattr(module, "merged", False):
                raise ProtocolError(
                    "invalid_bundle", "visual LoRA must remain active and unmerged"
                )
            if getattr(module, "merged_adapters", ()):
                raise ProtocolError(
                    "invalid_bundle", "visual LoRA must remain active and unmerged"
                )
        self.processor = ColModernVBertProcessor.from_pretrained(
            local_adapter,
            trust_remote_code=False,
            local_files_only=True,
        )

    def _encode(self, values, is_query: bool):
        inputs = (
            self.processor.process_queries(list(values), return_tensors="pt")
            if is_query
            else self.processor.process_images(list(values), return_tensors="pt")
        )
        with self.torch.inference_mode():
            embeddings = self.model(**inputs)
        results = []
        for index in range(embeddings.shape[0]):
            mask = inputs["attention_mask"][index].bool()
            matrix = embeddings[index][mask].cpu().numpy()
            if matrix.shape[0] == 0:
                raise ProtocolError(
                    "encoder_output", "visual encoder produced an empty matrix"
                )
            results.append(matrix)
        return results

    def encode_documents(self, inputs: Sequence[dict[str, Any]]):
        from PIL import Image

        images = []
        for value in inputs:
            require_exact_keys(
                value,
                {
                    "kind",
                    "path",
                    "media_type",
                    "width",
                    "height",
                    "encoded_size_bytes",
                },
                "image input",
            )
            if value["kind"] != "image":
                raise ProtocolError(
                    "unsupported_modality",
                    "visual encoder accepts only image documents",
                )
            path = confined_sidecar(value["path"])
            encoded_size = require_positive_int(
                value["encoded_size_bytes"], "encoded image size"
            )
            if path.stat().st_size != encoded_size:
                raise ProtocolError(
                    "invalid_image", "encoded image byte length does not match declaration"
                )
            media_type = value["media_type"]
            if media_type not in MEDIA_FORMATS:
                raise ProtocolError("invalid_image", "image media type is not supported")
            width = require_positive_int(value["width"], "image width")
            height = require_positive_int(value["height"], "image height")
            try:
                with Image.open(path) as image:
                    image.load()
                    if image.width != width or image.height != height:
                        raise ProtocolError(
                            "invalid_image",
                            "decoded image dimensions do not match declaration",
                        )
                    if image.format != MEDIA_FORMATS[media_type]:
                        raise ProtocolError(
                            "invalid_image",
                            "decoded image format does not match media type",
                        )
                    images.append(image.convert("RGB"))
            except ProtocolError:
                raise
            except (
                Image.DecompressionBombError,
                OSError,
                SyntaxError,
                ValueError,
            ) as error:
                raise ProtocolError(
                    "invalid_image", "encoded image cannot be decoded"
                ) from error
        return self._encode(images, is_query=False)

    def encode_query(self, text: str):
        return self._encode([require_nonempty_text(text, "query text")], is_query=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-bundle", type=Path, required=True)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--max-batch-units", type=int, required=True)
    parser.add_argument("--max-batch-input-bytes", type=int, required=True)
    parser.add_argument("--max-batch-pixels", type=int, required=True)
    parser.add_argument("--max-batch-rows", type=int, required=True)
    parser.add_argument("--max-tensor-bytes", type=int, required=True)
    parser.add_argument("--max-line-bytes", type=int, required=True)
    return parser.parse_args()


def require_offline_environment() -> None:
    for name in ("HF_HUB_OFFLINE", "TRANSFORMERS_OFFLINE", "HF_DATASETS_OFFLINE"):
        if os.environ.get(name) != "1":
            raise ProtocolError(
                "offline_required", f"{name} must be 1 for worker execution"
            )


def bounded_json_file(path: Path, max_bytes: int) -> dict[str, Any]:
    data = path.read_bytes()
    if len(data) > max_bytes:
        raise ProtocolError("invalid_bundle", "JSON file exceeds its byte limit")
    value = json.loads(data)
    if not isinstance(value, dict):
        raise ProtocolError("invalid_bundle", "JSON file must contain an object")
    return value


def require_absolute_directory(path: Path, label: str) -> Path:
    if not path.is_absolute():
        raise ProtocolError("invalid_path", f"{label} must be absolute")
    resolved = path.resolve(strict=True)
    if not resolved.is_dir():
        raise ProtocolError("invalid_path", f"{label} must be a directory")
    return resolved


def strict_relative(value: Any, label: str) -> Path:
    if not isinstance(value, str) or not value:
        raise ProtocolError("invalid_path", f"{label} must be a relative file name")
    path = Path(value)
    if path.is_absolute() or len(path.parts) != 1 or path.parts[0] in (".", ".."):
        raise ProtocolError("invalid_path", f"{label} must be a relative file name")
    return path


def confined_bundle_file(bundle: Path, value: Any, label: str) -> Path:
    if not isinstance(value, str) or not value:
        raise ProtocolError("invalid_bundle", f"{label} path must be non-empty")
    relative = Path(value)
    if relative.is_absolute() or ".." in relative.parts:
        raise ProtocolError("invalid_bundle", f"{label} path escapes the bundle")
    path = (bundle / relative).resolve(strict=True)
    if bundle not in path.parents or not path.is_file():
        raise ProtocolError("invalid_bundle", f"{label} is not a bundle file")
    return path


def confined_bundle_directory(bundle: Path, value: Any, label: str) -> Path:
    if not isinstance(value, str) or not value:
        raise ProtocolError("invalid_bundle", f"{label} path must be non-empty")
    relative = Path(value)
    if relative.is_absolute() or ".." in relative.parts:
        raise ProtocolError("invalid_bundle", f"{label} path escapes the bundle")
    path = (bundle / relative).resolve(strict=True)
    if bundle not in path.parents or not path.is_dir():
        raise ProtocolError("invalid_bundle", f"{label} is not a bundle directory")
    return path


def confined_sidecar(value: Any) -> Path:
    relative = strict_relative(value, "sidecar")
    path = (SCRATCH / relative).resolve(strict=True)
    if path.parent != SCRATCH or not path.is_file():
        raise ProtocolError("invalid_path", "sidecar escapes session scratch")
    return path


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            hasher.update(chunk)
    return hasher.hexdigest()


def require_exact_keys(value: Any, expected: set[str], label: str) -> None:
    if not isinstance(value, dict) or set(value) != expected:
        raise ProtocolError("invalid_request", f"{label} has an invalid shape")


def require_nonempty_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ProtocolError("invalid_request", f"{label} must be non-empty")
    return value


def require_positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ProtocolError("invalid_request", f"{label} must be a positive integer")
    return value


def load_bundle(bundle: Path, scratch: Path):
    manifest = bounded_json_file(bundle / MANIFEST_NAME, MAX_MANIFEST_BYTES)
    require_exact_keys(
        manifest,
        {
            "protocol_version",
            "epoch_id",
            "implementation",
            "version",
            "preprocessing_digest",
            "supported_modalities",
            "artifacts",
            "model",
        },
        "bundle manifest",
    )
    if manifest["protocol_version"] != PROTOCOL_VERSION:
        raise ProtocolError("invalid_bundle", "bundle protocol version is unsupported")
    artifacts = manifest["artifacts"]
    if not isinstance(artifacts, dict) or not artifacts:
        raise ProtocolError("invalid_bundle", "bundle artifacts must be non-empty")
    digests: dict[str, str] = {}
    for name, descriptor in artifacts.items():
        if not isinstance(name, str) or not name:
            raise ProtocolError("invalid_bundle", "artifact name must be non-empty")
        require_exact_keys(descriptor, {"path", "sha256"}, "artifact descriptor")
        path = confined_bundle_file(bundle, descriptor["path"], "artifact")
        expected = descriptor["sha256"]
        if (
            not isinstance(expected, str)
            or len(expected) != 64
            or any(character not in "0123456789abcdef" for character in expected)
        ):
            raise ProtocolError("invalid_bundle", "artifact digest must be lowercase SHA-256")
        if sha256_file(path) != expected:
            raise ProtocolError("invalid_bundle", f"artifact digest mismatch for {name}")
        digests[name] = expected

    model = manifest["model"]
    if not isinstance(model, dict) or "kind" not in model:
        raise ProtocolError("invalid_bundle", "bundle model declaration is invalid")
    dimension = require_positive_int(model.get("dimension"), "model dimension")
    kind = model["kind"]
    if kind == "gte_modern_colbert_v1":
        require_exact_keys(model, {"kind", "dimension", "model_path"}, "text model")
        adapter = TextEncoder(
            confined_bundle_directory(bundle, model["model_path"], "text model"),
            dimension,
        )
    elif kind == "colmodernvbert_v1":
        require_exact_keys(
            model,
            {
                "kind",
                "dimension",
                "base_path",
                "adapter_path",
                "active_lora_module_count",
            },
            "visual model",
        )
        adapter = VisualEncoder(
            confined_bundle_directory(bundle, model["base_path"], "visual base"),
            confined_bundle_directory(
                bundle, model["adapter_path"], "visual adapter"
            ),
            scratch,
            dimension,
            require_positive_int(
                model["active_lora_module_count"], "active LoRA module count"
            ),
        )
    else:
        raise ProtocolError("unsupported_model", "bundle model kind is not supported")

    modalities = manifest["supported_modalities"]
    if (
        not isinstance(modalities, list)
        or not modalities
        or any(not isinstance(value, str) for value in modalities)
        or tuple(sorted(set(modalities))) != tuple(sorted(adapter.supported_modalities))
    ):
        raise ProtocolError(
            "invalid_bundle", "bundle modalities do not match the loaded model"
        )
    identity = Identity(
        epoch_id=manifest["epoch_id"],
        implementation=manifest["implementation"],
        version=manifest["version"],
        preprocessing_digest=manifest["preprocessing_digest"],
        supported_modalities=tuple(sorted(set(modalities))),
        artifact_digests=digests,
        output_dimension=dimension,
    )
    return adapter, identity


def read_request(limits: Limits) -> dict[str, Any] | None:
    line = sys.stdin.buffer.readline(limits.max_line_bytes + 1)
    if not line:
        return None
    if len(line) > limits.max_line_bytes or not line.endswith(b"\n"):
        raise ProtocolError("frame_too_large", "request frame exceeds its byte limit")
    try:
        request = json.loads(line)
    except json.JSONDecodeError as error:
        raise ProtocolError("invalid_json", f"invalid request JSON: {error.msg}") from error
    if not isinstance(request, dict):
        raise ProtocolError("invalid_request", "request frame must be an object")
    return request


def validate_request_identity(request: dict[str, Any]) -> tuple[str, str]:
    request_type = request.get("type")
    request_id = request.get("request_id")
    if request.get("protocol_version") != PROTOCOL_VERSION:
        raise ProtocolError("invalid_request", "request protocol version mismatch")
    if not isinstance(request_id, str) or not request_id or len(request_id) > 128:
        raise ProtocolError("invalid_request", "request ID is invalid")
    if request_type not in ("encode_documents", "encode_query"):
        raise ProtocolError("invalid_request", "request type is unsupported")
    return request_type, request_id


def validate_document_bounds(inputs: Any, limits: Limits) -> list[dict[str, Any]]:
    if (
        not isinstance(inputs, list)
        or not inputs
        or len(inputs) > limits.max_batch_units
        or any(not isinstance(value, dict) for value in inputs)
    ):
        raise ProtocolError("resource_limit", "document batch count exceeds its limit")
    total_bytes = 0
    total_pixels = 0
    for value in inputs:
        kind = value.get("kind")
        if kind == "text":
            text = require_nonempty_text(value.get("text"), "document text")
            total_bytes += len(text.encode("utf-8"))
        elif kind in ("image", "image_text"):
            total_bytes += require_positive_int(
                value.get("encoded_size_bytes"), "encoded image size"
            )
            total_pixels += require_positive_int(
                value.get("width"), "image width"
            ) * require_positive_int(value.get("height"), "image height")
            if kind == "image_text":
                text = require_nonempty_text(value.get("text"), "document text")
                total_bytes += len(text.encode("utf-8"))
        else:
            raise ProtocolError("invalid_request", "document modality is unsupported")
    if total_bytes > limits.max_batch_input_bytes:
        raise ProtocolError("resource_limit", "document batch bytes exceed their limit")
    if total_pixels > limits.max_batch_pixels:
        raise ProtocolError("resource_limit", "document batch pixels exceed their limit")
    return inputs


def write_response(value: dict[str, Any], limits: Limits) -> None:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    if len(encoded) > limits.max_line_bytes:
        raise ProtocolError("frame_too_large", "response frame exceeds its byte limit")
    sys.stdout.buffer.write(encoded + b"\n")
    sys.stdout.buffer.flush()


def write_matrices(
    matrices: Sequence[Any],
    request_id: str,
    identity: Identity,
    limits: Limits,
) -> list[dict[str, Any]]:
    import numpy as np

    total_rows = 0
    outputs = []
    for ordinal, matrix in enumerate(matrices):
        values = np.asarray(matrix)
        if values.ndim != 2 or values.shape[1] != identity.output_dimension:
            raise ProtocolError("encoder_output", "encoder matrix shape is invalid")
        rows = int(values.shape[0])
        if rows <= 0:
            raise ProtocolError("encoder_output", "encoder matrix must be non-empty")
        total_rows += rows
        if total_rows > limits.max_batch_rows:
            raise ProtocolError("resource_limit", "encoder batch rows exceed their limit")
        if not np.isfinite(values).all():
            raise ProtocolError("encoder_output", "encoder matrix contains non-finite values")
        f16_values = values.astype("<f2", copy=False)
        if not np.isfinite(f16_values).all():
            raise ProtocolError(
                "encoder_output", "encoder matrix is not finite after f16 conversion"
            )
        encoded = f16_values.tobytes(order="C")
        if len(encoded) > limits.max_tensor_bytes:
            raise ProtocolError("resource_limit", "encoder tensor exceeds its byte limit")
        name = f"tensor-{request_id}-{ordinal}.f16le"
        relative = strict_relative(name, "tensor sidecar")
        path = SCRATCH / relative
        with path.open("xb") as destination:
            destination.write(encoded)
            destination.flush()
            os.fsync(destination.fileno())
        outputs.append(
            {
                "path": name,
                "dtype": "f16_le",
                "rows": rows,
                "columns": identity.output_dimension,
            }
        )
    return outputs


def main() -> int:
    args = parse_args()
    limits = Limits(
        max_batch_units=require_positive_int(args.max_batch_units, "batch units"),
        max_batch_input_bytes=require_positive_int(
            args.max_batch_input_bytes, "batch input bytes"
        ),
        max_batch_pixels=require_positive_int(args.max_batch_pixels, "batch pixels"),
        max_batch_rows=require_positive_int(args.max_batch_rows, "batch rows"),
        max_tensor_bytes=require_positive_int(args.max_tensor_bytes, "tensor bytes"),
        max_line_bytes=require_positive_int(args.max_line_bytes, "line bytes"),
    )
    require_offline_environment()
    global SCRATCH
    SCRATCH = require_absolute_directory(args.scratch, "scratch")
    bundle = require_absolute_directory(args.model_bundle, "model bundle")
    adapter, identity = load_bundle(bundle, SCRATCH)
    write_response(
        {
            "type": "hello",
            "protocol_version": PROTOCOL_VERSION,
            "epoch_id": identity.epoch_id,
            "implementation": identity.implementation,
            "version": identity.version,
            "preprocessing_digest": identity.preprocessing_digest,
            "supported_modalities": list(identity.supported_modalities),
            "artifact_digests": identity.artifact_digests,
            "output_dimension": identity.output_dimension,
        },
        limits,
    )

    while True:
        request = read_request(limits)
        if request is None:
            return 0
        request_id = request.get("request_id")
        try:
            request_type, request_id = validate_request_identity(request)
            if request_type == "encode_documents":
                require_exact_keys(
                    request,
                    {"type", "protocol_version", "request_id", "inputs"},
                    "document request",
                )
                inputs = validate_document_bounds(request["inputs"], limits)
                matrices = adapter.encode_documents(inputs)
            else:
                require_exact_keys(
                    request,
                    {"type", "protocol_version", "request_id", "text"},
                    "query request",
                )
                text = require_nonempty_text(request["text"], "query text")
                if len(text.encode("utf-8")) > limits.max_batch_input_bytes:
                    raise ProtocolError(
                        "resource_limit", "query bytes exceed their limit"
                    )
                matrices = adapter.encode_query(text)
            outputs = write_matrices(matrices, request_id, identity, limits)
            write_response(
                {
                    "type": "encoded",
                    "protocol_version": PROTOCOL_VERSION,
                    "request_id": request_id,
                    "epoch_id": identity.epoch_id,
                    "outputs": outputs,
                },
                limits,
            )
        except ProtocolError as error:
            if not isinstance(request_id, str) or not request_id:
                raise
            write_response(
                {
                    "type": "error",
                    "protocol_version": PROTOCOL_VERSION,
                    "request_id": request_id,
                    "epoch_id": identity.epoch_id,
                    "code": error.code,
                    "message": str(error),
                },
                limits,
            )


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProtocolError as error:
        print(f"worker fatal ({error.code}): {error}", file=sys.stderr, flush=True)
        raise SystemExit(2)
