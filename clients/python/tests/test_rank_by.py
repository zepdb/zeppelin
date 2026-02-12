"""Tests for the RankBy builder."""

from zeppelin.rank_by import RankBy


class TestRankByBm25:
    def test_simple(self):
        result = RankBy.bm25("content", "search query")
        assert result == ["content", "BM25", "search query"]

    def test_empty_query(self):
        result = RankBy.bm25("title", "")
        assert result == ["title", "BM25", ""]


class TestRankBySum:
    def test_two_fields(self):
        result = RankBy.sum(
            RankBy.bm25("title", "q"),
            RankBy.bm25("body", "q"),
        )
        assert result == [
            "Sum",
            [
                ["title", "BM25", "q"],
                ["body", "BM25", "q"],
            ],
        ]

    def test_three_fields(self):
        result = RankBy.sum(
            RankBy.bm25("a", "q"),
            RankBy.bm25("b", "q"),
            RankBy.bm25("c", "q"),
        )
        assert result[0] == "Sum"
        assert len(result[1]) == 3


class TestRankByMax:
    def test_max(self):
        result = RankBy.max(
            RankBy.bm25("title", "q"),
            RankBy.bm25("body", "q"),
        )
        assert result == [
            "Max",
            [
                ["title", "BM25", "q"],
                ["body", "BM25", "q"],
            ],
        ]


class TestRankByProduct:
    def test_product(self):
        result = RankBy.product(2.0, RankBy.bm25("title", "q"))
        assert result == ["Product", 2.0, ["title", "BM25", "q"]]


class TestRankByComposition:
    def test_weighted_sum(self):
        result = RankBy.sum(
            RankBy.product(2.0, RankBy.bm25("title", "query")),
            RankBy.bm25("body", "query"),
        )
        assert result == [
            "Sum",
            [
                ["Product", 2.0, ["title", "BM25", "query"]],
                ["body", "BM25", "query"],
            ],
        ]

    def test_nested_max_in_sum(self):
        result = RankBy.sum(
            RankBy.max(
                RankBy.bm25("title", "q"),
                RankBy.bm25("heading", "q"),
            ),
            RankBy.bm25("body", "q"),
        )
        assert result[0] == "Sum"
        assert result[1][0][0] == "Max"
        assert result[1][1] == ["body", "BM25", "q"]
