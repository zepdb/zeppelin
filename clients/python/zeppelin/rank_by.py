"""RankBy builder for BM25 full-text search queries.

Usage::

    from zeppelin.rank_by import RankBy

    # Single field
    r = RankBy.bm25("content", "search query")

    # Multi-field with weights
    r = RankBy.sum(
        RankBy.product(2.0, RankBy.bm25("title", "query")),
        RankBy.bm25("body", "query"),
    )
"""

from __future__ import annotations

from typing import Any

RankByExpr = list[Any]


class RankBy:
    """Static methods that produce rank_by S-expression arrays."""

    @staticmethod
    def bm25(field: str, query: str) -> RankByExpr:
        """Single-field BM25: ``["field", "BM25", "query"]``"""
        return [field, "BM25", query]

    @staticmethod
    def sum(*exprs: RankByExpr) -> RankByExpr:
        """Sum of multiple expressions: ``["Sum", [...exprs]]``"""
        return ["Sum", list(exprs)]

    @staticmethod
    def max(*exprs: RankByExpr) -> RankByExpr:
        """Max of multiple expressions: ``["Max", [...exprs]]``"""
        return ["Max", list(exprs)]

    @staticmethod
    def product(weight: float, expr: RankByExpr) -> RankByExpr:
        """Weighted expression: ``["Product", weight, expr]``"""
        return ["Product", weight, expr]
