"""Zeppelin API response types."""

from dataclasses import dataclass, field


@dataclass
class Namespace:
    name: str
    dimensions: int
    distance_metric: str
    vector_count: int
    created_at: str
    updated_at: str


@dataclass
class SearchResult:
    id: str
    score: float
    attributes: dict | None = None


@dataclass
class QueryResponse:
    results: list[SearchResult]
    scanned_fragments: int
    scanned_segments: int


@dataclass
class Vector:
    id: str
    values: list[float]
    attributes: dict | None = None

    def to_dict(self) -> dict:
        d: dict = {"id": self.id, "values": self.values}
        if self.attributes is not None:
            d["attributes"] = self.attributes
        return d
