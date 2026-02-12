"""Tests for the Filter builder."""

from zeppelin.filters import Filter


class TestFilterEq:
    def test_eq_string(self):
        assert Filter.eq("color", "red") == {"op": "eq", "field": "color", "value": "red"}

    def test_eq_integer(self):
        assert Filter.eq("count", 42) == {"op": "eq", "field": "count", "value": 42}

    def test_eq_bool(self):
        assert Filter.eq("active", True) == {"op": "eq", "field": "active", "value": True}


class TestFilterNotEq:
    def test_not_eq(self):
        assert Filter.not_eq("status", "deleted") == {
            "op": "not_eq",
            "field": "status",
            "value": "deleted",
        }


class TestFilterRange:
    def test_range_gte_lte(self):
        result = Filter.range("price", gte=10, lte=100)
        assert result == {"op": "range", "field": "price", "gte": 10, "lte": 100}

    def test_range_gt_lt(self):
        result = Filter.range("price", gt=0, lt=50)
        assert result == {"op": "range", "field": "price", "gt": 0, "lt": 50}

    def test_range_single_bound(self):
        result = Filter.range("price", gte=10)
        assert result == {"op": "range", "field": "price", "gte": 10}
        assert "lte" not in result
        assert "gt" not in result
        assert "lt" not in result


class TestFilterIn:
    def test_in_strings(self):
        result = Filter.in_("tag", ["a", "b", "c"])
        assert result == {"op": "in", "field": "tag", "values": ["a", "b", "c"]}

    def test_in_integers(self):
        result = Filter.in_("level", [1, 2, 3])
        assert result == {"op": "in", "field": "level", "values": [1, 2, 3]}


class TestFilterNotIn:
    def test_not_in(self):
        result = Filter.not_in("category", ["spam", "junk"])
        assert result == {"op": "not_in", "field": "category", "values": ["spam", "junk"]}


class TestFilterContains:
    def test_contains(self):
        result = Filter.contains("tags", "rust")
        assert result == {"op": "contains", "field": "tags", "value": "rust"}


class TestFilterTokens:
    def test_contains_all_tokens(self):
        result = Filter.contains_all_tokens("content", ["rust", "async"])
        assert result == {
            "op": "contains_all_tokens",
            "field": "content",
            "tokens": ["rust", "async"],
        }

    def test_contains_token_sequence(self):
        result = Filter.contains_token_sequence("content", ["vector", "search"])
        assert result == {
            "op": "contains_token_sequence",
            "field": "content",
            "tokens": ["vector", "search"],
        }


class TestFilterBoolean:
    def test_and(self):
        result = Filter.and_(
            Filter.eq("color", "red"),
            Filter.range("price", gte=10),
        )
        assert result == {
            "op": "and",
            "filters": [
                {"op": "eq", "field": "color", "value": "red"},
                {"op": "range", "field": "price", "gte": 10},
            ],
        }

    def test_or(self):
        result = Filter.or_(
            Filter.eq("a", 1),
            Filter.eq("b", 2),
        )
        assert result == {
            "op": "or",
            "filters": [
                {"op": "eq", "field": "a", "value": 1},
                {"op": "eq", "field": "b", "value": 2},
            ],
        }

    def test_not(self):
        result = Filter.not_(Filter.eq("deleted", True))
        assert result == {
            "op": "not",
            "filter": {"op": "eq", "field": "deleted", "value": True},
        }

    def test_nested_boolean(self):
        result = Filter.and_(
            Filter.or_(
                Filter.eq("color", "red"),
                Filter.eq("color", "blue"),
            ),
            Filter.not_(Filter.eq("deleted", True)),
        )
        assert result["op"] == "and"
        assert len(result["filters"]) == 2
        assert result["filters"][0]["op"] == "or"
        assert result["filters"][1]["op"] == "not"
