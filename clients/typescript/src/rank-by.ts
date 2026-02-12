import type { RankByExpr } from "./types.js";

/**
 * RankBy builder with static methods that produce S-expression arrays
 * matching the Zeppelin wire format.
 *
 * @example
 * ```ts
 * import { RankBy } from "@zeppelin/client";
 *
 * // Single field
 * RankBy.bm25("content", "search query");
 *
 * // Weighted multi-field
 * RankBy.sum(
 *   RankBy.product(2.0, RankBy.bm25("title", "q")),
 *   RankBy.bm25("body", "q"),
 * );
 * ```
 */
export const RankBy = {
  /** Single-field BM25: `["field", "BM25", "query"]` */
  bm25(field: string, query: string): RankByExpr {
    return [field, "BM25", query];
  },

  /** Sum of multiple expressions: `["Sum", [...exprs]]` */
  sum(...exprs: RankByExpr[]): RankByExpr {
    return ["Sum", exprs];
  },

  /** Max of multiple expressions: `["Max", [...exprs]]` */
  max(...exprs: RankByExpr[]): RankByExpr {
    return ["Max", exprs];
  },

  /** Weighted expression: `["Product", weight, expr]` */
  product(weight: number, expr: RankByExpr): RankByExpr {
    return ["Product", weight, expr];
  },
} as const;
