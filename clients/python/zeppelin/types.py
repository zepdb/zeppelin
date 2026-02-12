"""Zeppelin API types."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class FtsFieldConfig:
    """Per-field configuration for full-text search."""

    language: str = "english"
    stemming: bool = True
    remove_stopwords: bool = True
    case_sensitive: bool = False
    k1: float = 1.2
    b: float = 0.75
    max_token_length: int = 40

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {}
        if self.language != "english":
            d["language"] = self.language
        if not self.stemming:
            d["stemming"] = False
        if not self.remove_stopwords:
            d["remove_stopwords"] = False
        if self.case_sensitive:
            d["case_sensitive"] = True
        if abs(self.k1 - 1.2) > 1e-6:
            d["k1"] = self.k1
        if abs(self.b - 0.75) > 1e-6:
            d["b"] = self.b
        if self.max_token_length != 40:
            d["max_token_length"] = self.max_token_length
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FtsFieldConfig:
        return cls(
            language=data.get("language", "english"),
            stemming=data.get("stemming", True),
            remove_stopwords=data.get("remove_stopwords", True),
            case_sensitive=data.get("case_sensitive", False),
            k1=data.get("k1", 1.2),
            b=data.get("b", 0.75),
            max_token_length=data.get("max_token_length", 40),
        )


@dataclass
class Namespace:
    """Namespace metadata."""

    name: str
    dimensions: int
    distance_metric: str
    vector_count: int
    created_at: str
    updated_at: str
    full_text_search: dict[str, FtsFieldConfig] = field(default_factory=dict)


@dataclass
class SearchResult:
    """A single search result."""

    id: str
    score: float
    attributes: dict[str, Any] | None = None


@dataclass
class QueryResponse:
    """Response from a query operation."""

    results: list[SearchResult]
    scanned_fragments: int
    scanned_segments: int


@dataclass
class Vector:
    """A vector entry for upsert operations."""

    id: str
    values: list[float]
    attributes: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"id": self.id, "values": self.values}
        if self.attributes is not None:
            d["attributes"] = self.attributes
        return d
