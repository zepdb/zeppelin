/** Distance metric for vector comparison. */
export type DistanceMetric = "cosine" | "euclidean" | "dot_product";

/** Consistency level for queries. */
export type ConsistencyLevel = "strong" | "eventual";

/** Supported FTS languages. */
export type FtsLanguage = "english";

/** Attribute values that can be attached to vectors. */
export type AttributeValue =
  | string
  | number
  | boolean
  | string[]
  | number[];

/** Key-value attributes map. */
export type Attributes = Record<string, AttributeValue>;

/** Per-field configuration for full-text search. */
export interface FtsFieldConfig {
  language?: FtsLanguage;
  stemming?: boolean;
  remove_stopwords?: boolean;
  case_sensitive?: boolean;
  k1?: number;
  b?: number;
  max_token_length?: number;
}

/** A vector entry for upsert operations. */
export interface VectorEntry {
  id: string;
  values: number[];
  attributes?: Attributes;
}

/** A single search result. */
export interface SearchResult {
  id: string;
  score: number;
  attributes?: Attributes;
}

/** Namespace metadata. */
export interface Namespace {
  name: string;
  dimensions: number;
  distance_metric: DistanceMetric;
  vector_count: number;
  created_at: string;
  updated_at: string;
  full_text_search?: Record<string, FtsFieldConfig>;
}

/** Filter condition discriminated by the `op` field. */
export type Filter =
  | { op: "eq"; field: string; value: AttributeValue }
  | { op: "not_eq"; field: string; value: AttributeValue }
  | { op: "range"; field: string; gte?: number; lte?: number; gt?: number; lt?: number }
  | { op: "in"; field: string; values: AttributeValue[] }
  | { op: "not_in"; field: string; values: AttributeValue[] }
  | { op: "contains"; field: string; value: AttributeValue }
  | { op: "contains_all_tokens"; field: string; tokens: string[] }
  | { op: "contains_token_sequence"; field: string; tokens: string[] }
  | { op: "and"; filters: Filter[] }
  | { op: "or"; filters: Filter[] }
  | { op: "not"; filter: Filter };

/** RankBy S-expression (heterogeneous JSON array). */
export type RankByExpr = unknown[];

/** Request body for creating a namespace. */
export interface CreateNamespaceRequest {
  name: string;
  dimensions: number;
  distance_metric?: DistanceMetric;
  full_text_search?: Record<string, FtsFieldConfig>;
}

/** Request body for upserting vectors. */
export interface UpsertVectorsRequest {
  vectors: VectorEntry[];
}

/** Request body for deleting vectors. */
export interface DeleteVectorsRequest {
  ids: string[];
}

/** Request body for querying. */
export interface QueryRequest {
  vector?: number[];
  rank_by?: RankByExpr;
  last_as_prefix?: boolean;
  top_k?: number;
  filter?: Filter;
  consistency?: ConsistencyLevel;
  nprobe?: number;
}

/** Response from a query operation. */
export interface QueryResponse {
  results: SearchResult[];
  scanned_fragments: number;
  scanned_segments: number;
}

/** API error response. */
export interface ErrorResponse {
  error: string;
  status: number;
}

/** Client configuration options. */
export interface ZeppelinClientOptions {
  /** Base URL of the Zeppelin server. Defaults to http://localhost:8080. */
  baseUrl?: string;
  /** Request timeout in milliseconds. Defaults to 30000. */
  timeout?: number;
  /** Additional headers to send with every request. */
  headers?: Record<string, string>;
  /** Custom fetch implementation (for Node.js < 18 or testing). */
  fetch?: typeof globalThis.fetch;
}
