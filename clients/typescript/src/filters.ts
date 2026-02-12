import type { AttributeValue, Filter } from "./types.js";

/**
 * Filter builder with static methods that produce filter objects
 * matching the Zeppelin wire format.
 *
 * @example
 * ```ts
 * import { Filters } from "@zeppelin/client";
 *
 * const f = Filters.and(
 *   Filters.eq("color", "red"),
 *   Filters.range("price", { gte: 10, lte: 100 }),
 * );
 * ```
 */
export const Filters = {
  /** Exact equality. */
  eq(field: string, value: AttributeValue): Filter {
    return { op: "eq", field, value };
  },

  /** Not equal. */
  notEq(field: string, value: AttributeValue): Filter {
    return { op: "not_eq", field, value };
  },

  /** Numeric range filter. */
  range(
    field: string,
    bounds: { gte?: number; lte?: number; gt?: number; lt?: number },
  ): Filter {
    return { op: "range", field, ...bounds };
  },

  /** Field value is one of the given values. */
  in(field: string, values: AttributeValue[]): Filter {
    return { op: "in", field, values };
  },

  /** Field value is not one of the given values. */
  notIn(field: string, values: AttributeValue[]): Filter {
    return { op: "not_in", field, values };
  },

  /** Array field contains the given value. */
  contains(field: string, value: AttributeValue): Filter {
    return { op: "contains", field, value };
  },

  /** All tokens must be present (order-independent). */
  containsAllTokens(field: string, tokens: string[]): Filter {
    return { op: "contains_all_tokens", field, tokens };
  },

  /** Tokens must appear as an exact phrase. */
  containsTokenSequence(field: string, tokens: string[]): Filter {
    return { op: "contains_token_sequence", field, tokens };
  },

  /** All sub-filters must match. */
  and(...filters: Filter[]): Filter {
    return { op: "and", filters };
  },

  /** Any sub-filter must match. */
  or(...filters: Filter[]): Filter {
    return { op: "or", filters };
  },

  /** Negate a sub-filter. */
  not(filter: Filter): Filter {
    return { op: "not", filter };
  },
} as const;
