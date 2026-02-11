"""Zeppelin — Python client for the Zeppelin vector search engine."""

from .client import ZeppelinClient
from .exceptions import (
    ConflictError,
    NotFoundError,
    ServerError,
    ValidationError,
    ZeppelinError,
)
from .types import Namespace, QueryResponse, SearchResult, Vector

__version__ = "0.1.0"

__all__ = [
    "ZeppelinClient",
    "ZeppelinError",
    "NotFoundError",
    "ConflictError",
    "ValidationError",
    "ServerError",
    "Namespace",
    "QueryResponse",
    "SearchResult",
    "Vector",
]
