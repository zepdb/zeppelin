import { describe, it, expect } from "vitest";
import { Filters } from "../src/filters.js";

describe("Filters.eq", () => {
  it("builds eq filter with string value", () => {
    expect(Filters.eq("color", "red")).toEqual({
      op: "eq",
      field: "color",
      value: "red",
    });
  });

  it("builds eq filter with integer value", () => {
    expect(Filters.eq("count", 42)).toEqual({
      op: "eq",
      field: "count",
      value: 42,
    });
  });

  it("builds eq filter with boolean value", () => {
    expect(Filters.eq("active", true)).toEqual({
      op: "eq",
      field: "active",
      value: true,
    });
  });
});

describe("Filters.notEq", () => {
  it("builds not_eq filter", () => {
    expect(Filters.notEq("status", "deleted")).toEqual({
      op: "not_eq",
      field: "status",
      value: "deleted",
    });
  });
});

describe("Filters.range", () => {
  it("builds range with gte and lte", () => {
    expect(Filters.range("price", { gte: 10, lte: 100 })).toEqual({
      op: "range",
      field: "price",
      gte: 10,
      lte: 100,
    });
  });

  it("builds range with gt and lt", () => {
    expect(Filters.range("price", { gt: 0, lt: 50 })).toEqual({
      op: "range",
      field: "price",
      gt: 0,
      lt: 50,
    });
  });

  it("builds range with single bound", () => {
    const result = Filters.range("price", { gte: 10 });
    expect(result.op).toBe("range");
    expect(result).toHaveProperty("gte", 10);
  });
});

describe("Filters.in", () => {
  it("builds in filter", () => {
    expect(Filters.in("tag", ["a", "b", "c"])).toEqual({
      op: "in",
      field: "tag",
      values: ["a", "b", "c"],
    });
  });
});

describe("Filters.notIn", () => {
  it("builds not_in filter", () => {
    expect(Filters.notIn("category", ["spam", "junk"])).toEqual({
      op: "not_in",
      field: "category",
      values: ["spam", "junk"],
    });
  });
});

describe("Filters.contains", () => {
  it("builds contains filter", () => {
    expect(Filters.contains("tags", "rust")).toEqual({
      op: "contains",
      field: "tags",
      value: "rust",
    });
  });
});

describe("Filters.containsAllTokens", () => {
  it("builds contains_all_tokens filter", () => {
    expect(Filters.containsAllTokens("content", ["rust", "async"])).toEqual({
      op: "contains_all_tokens",
      field: "content",
      tokens: ["rust", "async"],
    });
  });
});

describe("Filters.containsTokenSequence", () => {
  it("builds contains_token_sequence filter", () => {
    expect(
      Filters.containsTokenSequence("content", ["vector", "search"]),
    ).toEqual({
      op: "contains_token_sequence",
      field: "content",
      tokens: ["vector", "search"],
    });
  });
});

describe("Filters.and", () => {
  it("combines sub-filters", () => {
    const result = Filters.and(
      Filters.eq("color", "red"),
      Filters.range("price", { gte: 10 }),
    );
    expect(result).toEqual({
      op: "and",
      filters: [
        { op: "eq", field: "color", value: "red" },
        { op: "range", field: "price", gte: 10 },
      ],
    });
  });
});

describe("Filters.or", () => {
  it("combines sub-filters", () => {
    const result = Filters.or(
      Filters.eq("a", 1),
      Filters.eq("b", 2),
    );
    expect(result).toEqual({
      op: "or",
      filters: [
        { op: "eq", field: "a", value: 1 },
        { op: "eq", field: "b", value: 2 },
      ],
    });
  });
});

describe("Filters.not", () => {
  it("negates a sub-filter", () => {
    const result = Filters.not(Filters.eq("deleted", true));
    expect(result).toEqual({
      op: "not",
      filter: { op: "eq", field: "deleted", value: true },
    });
  });
});

describe("nested filters", () => {
  it("handles deep nesting", () => {
    const result = Filters.and(
      Filters.or(Filters.eq("color", "red"), Filters.eq("color", "blue")),
      Filters.not(Filters.eq("deleted", true)),
    );
    expect(result.op).toBe("and");
    const andFilter = result as { op: "and"; filters: unknown[] };
    expect(andFilter.filters).toHaveLength(2);
    expect((andFilter.filters[0] as { op: string }).op).toBe("or");
    expect((andFilter.filters[1] as { op: string }).op).toBe("not");
  });
});
