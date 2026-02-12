"""Filter builder for Zeppelin queries.

Usage::

    from zeppelin.filters import Filter

    f = Filter.and_(
        Filter.eq("color", "red"),
        Filter.range("price", gte=10, lte=100),
    )
"""

from __future__ import annotations

from typing import Any


class Filter:
    """Static methods that produce filter dicts matching the Zeppelin wire format."""

    @staticmethod
    def eq(field: str, value: Any) -> dict:
        """Exact equality: ``{"op": "eq", "field": ..., "value": ...}``"""
        return {"op": "eq", "field": field, "value": value}

    @staticmethod
    def not_eq(field: str, value: Any) -> dict:
        """Not equal: ``{"op": "not_eq", "field": ..., "value": ...}``"""
        return {"op": "not_eq", "field": field, "value": value}

    @staticmethod
    def range(
        field: str,
        *,
        gte: float | None = None,
        lte: float | None = None,
        gt: float | None = None,
        lt: float | None = None,
    ) -> dict:
        """Numeric range filter. At least one bound must be provided."""
        d: dict[str, Any] = {"op": "range", "field": field}
        if gte is not None:
            d["gte"] = gte
        if lte is not None:
            d["lte"] = lte
        if gt is not None:
            d["gt"] = gt
        if lt is not None:
            d["lt"] = lt
        return d

    @staticmethod
    def in_(field: str, values: list[Any]) -> dict:
        """Field value is one of the given values."""
        return {"op": "in", "field": field, "values": values}

    @staticmethod
    def not_in(field: str, values: list[Any]) -> dict:
        """Field value is not one of the given values."""
        return {"op": "not_in", "field": field, "values": values}

    @staticmethod
    def contains(field: str, value: Any) -> dict:
        """Array field contains the given value."""
        return {"op": "contains", "field": field, "value": value}

    @staticmethod
    def contains_all_tokens(field: str, tokens: list[str]) -> dict:
        """All tokens must be present in the field (order-independent)."""
        return {"op": "contains_all_tokens", "field": field, "tokens": tokens}

    @staticmethod
    def contains_token_sequence(field: str, tokens: list[str]) -> dict:
        """Tokens must appear as an exact phrase (adjacent, in order)."""
        return {"op": "contains_token_sequence", "field": field, "tokens": tokens}

    @staticmethod
    def and_(*filters: dict) -> dict:
        """All sub-filters must match."""
        return {"op": "and", "filters": list(filters)}

    @staticmethod
    def or_(*filters: dict) -> dict:
        """Any sub-filter must match."""
        return {"op": "or", "filters": list(filters)}

    @staticmethod
    def not_(filter: dict) -> dict:
        """Negate a sub-filter."""
        return {"op": "not", "filter": filter}
