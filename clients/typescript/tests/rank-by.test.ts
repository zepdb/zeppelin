import { describe, it, expect } from "vitest";
import { RankBy } from "../src/rank-by.js";

describe("RankBy.bm25", () => {
  it("builds single-field BM25 expression", () => {
    expect(RankBy.bm25("content", "search query")).toEqual([
      "content",
      "BM25",
      "search query",
    ]);
  });

  it("handles empty query", () => {
    expect(RankBy.bm25("title", "")).toEqual(["title", "BM25", ""]);
  });
});

describe("RankBy.sum", () => {
  it("builds sum of two fields", () => {
    expect(
      RankBy.sum(RankBy.bm25("title", "q"), RankBy.bm25("body", "q")),
    ).toEqual([
      "Sum",
      [
        ["title", "BM25", "q"],
        ["body", "BM25", "q"],
      ],
    ]);
  });

  it("handles three fields", () => {
    const result = RankBy.sum(
      RankBy.bm25("a", "q"),
      RankBy.bm25("b", "q"),
      RankBy.bm25("c", "q"),
    );
    expect(result[0]).toBe("Sum");
    expect((result[1] as unknown[]).length).toBe(3);
  });
});

describe("RankBy.max", () => {
  it("builds max expression", () => {
    expect(
      RankBy.max(RankBy.bm25("title", "q"), RankBy.bm25("body", "q")),
    ).toEqual([
      "Max",
      [
        ["title", "BM25", "q"],
        ["body", "BM25", "q"],
      ],
    ]);
  });
});

describe("RankBy.product", () => {
  it("builds weighted expression", () => {
    expect(RankBy.product(2.0, RankBy.bm25("title", "q"))).toEqual([
      "Product",
      2.0,
      ["title", "BM25", "q"],
    ]);
  });
});

describe("composition", () => {
  it("builds weighted sum", () => {
    expect(
      RankBy.sum(
        RankBy.product(2.0, RankBy.bm25("title", "query")),
        RankBy.bm25("body", "query"),
      ),
    ).toEqual([
      "Sum",
      [
        ["Product", 2.0, ["title", "BM25", "query"]],
        ["body", "BM25", "query"],
      ],
    ]);
  });

  it("nests max inside sum", () => {
    const result = RankBy.sum(
      RankBy.max(RankBy.bm25("title", "q"), RankBy.bm25("heading", "q")),
      RankBy.bm25("body", "q"),
    );
    expect(result[0]).toBe("Sum");
    const inner = result[1] as unknown[];
    expect((inner[0] as unknown[])[0]).toBe("Max");
    expect(inner[1]).toEqual(["body", "BM25", "q"]);
  });
});
