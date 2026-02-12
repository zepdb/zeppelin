"""Zeppelin — Python client for the Zeppelin vector search engine."""

from .client import AsyncZeppelinClient, ZeppelinClient
from .exceptions import (
    ConflictError,
    NotFoundError,
    ServerError,
    ValidationError,
    ZeppelinError,
)
from .filters import Filter
from .rank_by import RankBy
from .types import FtsFieldConfig, Namespace, QueryResponse, SearchResult, Vector

__version__ = "0.2.0"

__all__ = [
    "AsyncZeppelinClient",
    "ZeppelinClient",
    "ZeppelinError",
    "NotFoundError",
    "ConflictError",
    "ValidationError",
    "ServerError",
    "Filter",
    "RankBy",
    "FtsFieldConfig",
    "Namespace",
    "QueryResponse",
    "SearchResult",
    "Vector",
]
