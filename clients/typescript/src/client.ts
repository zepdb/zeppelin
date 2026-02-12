import {
  ConflictError,
  NotFoundError,
  ServerError,
  ValidationError,
  ZeppelinError,
} from "./errors.js";
import type {
  ConsistencyLevel,
  DistanceMetric,
  Filter,
  FtsFieldConfig,
  Namespace,
  QueryResponse,
  RankByExpr,
  SearchResult,
  VectorEntry,
  ZeppelinClientOptions,
} from "./types.js";

/**
 * Client for the Zeppelin vector search API.
 *
 * Uses native `fetch` with zero runtime dependencies.
 *
 * @example
 * ```ts
 * const client = new ZeppelinClient({ baseUrl: "http://localhost:8080" });
 *
 * await client.createNamespace("my-ns", 128);
 * await client.upsertVectors("my-ns", [
 *   { id: "v1", values: new Array(128).fill(0.1) },
 * ]);
 * const result = await client.query("my-ns", { vector: new Array(128).fill(0.1) });
 * ```
 */
export class ZeppelinClient {
  private readonly baseUrl: string;
  private readonly timeout: number;
  private readonly headers: Record<string, string>;
  private readonly fetchFn: typeof globalThis.fetch;

  constructor(options: ZeppelinClientOptions = {}) {
    this.baseUrl = (options.baseUrl ?? "http://localhost:8080").replace(
      /\/$/,
      "",
    );
    this.timeout = options.timeout ?? 30_000;
    this.headers = options.headers ?? {};
    this.fetchFn = options.fetch ?? globalThis.fetch;
  }

  // -- Health --

  /** Check server health. */
  async health(): Promise<{ status: string }> {
    return this.get("/healthz");
  }

  /** Check server readiness. */
  async ready(): Promise<{
    status: string;
    s3_connected: boolean;
    error?: string;
  }> {
    return this.get("/readyz");
  }

  // -- Namespaces --

  /** Create a new namespace. */
  async createNamespace(
    name: string,
    dimensions: number,
    options?: {
      distanceMetric?: DistanceMetric;
      fullTextSearch?: Record<string, FtsFieldConfig>;
    },
  ): Promise<Namespace> {
    const body: Record<string, unknown> = {
      name,
      dimensions,
      distance_metric: options?.distanceMetric ?? "cosine",
    };
    if (options?.fullTextSearch) {
      body.full_text_search = options.fullTextSearch;
    }
    return this.post("/v1/namespaces", body);
  }

  /** List all namespaces. */
  async listNamespaces(): Promise<Namespace[]> {
    return this.get("/v1/namespaces");
  }

  /** Get namespace metadata. */
  async getNamespace(name: string): Promise<Namespace> {
    return this.get(`/v1/namespaces/${encodeURIComponent(name)}`);
  }

  /** Delete a namespace and all its data. */
  async deleteNamespace(name: string): Promise<void> {
    await this.delete(`/v1/namespaces/${encodeURIComponent(name)}`);
  }

  // -- Vectors --

  /** Upsert vectors. Returns the number upserted. */
  async upsertVectors(
    namespace: string,
    vectors: VectorEntry[],
  ): Promise<number> {
    const data = await this.post(
      `/v1/namespaces/${encodeURIComponent(namespace)}/vectors`,
      { vectors },
    );
    return (data as { upserted: number }).upserted;
  }

  /** Delete vectors by ID. Returns the number deleted. */
  async deleteVectors(namespace: string, ids: string[]): Promise<number> {
    const data = await this.request(
      "DELETE",
      `/v1/namespaces/${encodeURIComponent(namespace)}/vectors`,
      { ids },
    );
    return (data as { deleted: number }).deleted;
  }

  // -- Query --

  /** Run a vector similarity search or BM25 full-text search. */
  async query(
    namespace: string,
    options: {
      vector?: number[];
      rankBy?: RankByExpr;
      lastAsPrefix?: boolean;
      topK?: number;
      filter?: Filter;
      consistency?: ConsistencyLevel;
      nprobe?: number;
    },
  ): Promise<QueryResponse> {
    const body: Record<string, unknown> = {
      top_k: options.topK ?? 10,
    };
    if (options.vector !== undefined) body.vector = options.vector;
    if (options.rankBy !== undefined) body.rank_by = options.rankBy;
    if (options.lastAsPrefix) body.last_as_prefix = true;
    if (options.filter !== undefined) body.filter = options.filter;
    if (options.consistency !== undefined)
      body.consistency = options.consistency;
    if (options.nprobe !== undefined) body.nprobe = options.nprobe;

    return this.post(
      `/v1/namespaces/${encodeURIComponent(namespace)}/query`,
      body,
    );
  }

  // -- Internal --

  private async get<T>(path: string): Promise<T> {
    return this.request("GET", path);
  }

  private async post<T>(path: string, body: unknown): Promise<T> {
    return this.request("POST", path, body);
  }

  private async delete(path: string): Promise<void> {
    await this.request("DELETE", path);
  }

  private async request<T>(
    method: string,
    path: string,
    body?: unknown,
  ): Promise<T> {
    const url = `${this.baseUrl}${path}`;
    const init: RequestInit = {
      method,
      headers: {
        ...this.headers,
        ...(body !== undefined ? { "Content-Type": "application/json" } : {}),
      },
      body: body !== undefined ? JSON.stringify(body) : undefined,
      signal: AbortSignal.timeout(this.timeout),
    };

    const resp = await this.fetchFn(url, init);
    return this.handleResponse(resp);
  }

  private async handleResponse<T>(resp: Response): Promise<T> {
    if (resp.status < 300) {
      if (resp.status === 204) return undefined as T;
      const text = await resp.text();
      if (!text) return undefined as T;
      return JSON.parse(text) as T;
    }

    let message: string;
    try {
      const body = await resp.json();
      message = (body as { error?: string }).error ?? resp.statusText;
    } catch {
      message = resp.statusText;
    }

    const status = resp.status;
    if (status === 400) throw new ValidationError(message);
    if (status === 404) throw new NotFoundError(message);
    if (status === 409) throw new ConflictError(message);
    if (status >= 500) throw new ServerError(message, status);
    throw new ZeppelinError(message, status);
  }
}
