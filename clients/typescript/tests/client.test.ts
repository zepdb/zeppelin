import { describe, it, expect, vi, beforeEach } from "vitest";
import { ZeppelinClient } from "../src/client.js";
import {
  ValidationError,
  NotFoundError,
  ConflictError,
  ServerError,
} from "../src/errors.js";
import { Filters } from "../src/filters.js";
import { RankBy } from "../src/rank-by.js";

const NS_RESPONSE = {
  name: "test-ns",
  dimensions: 128,
  distance_metric: "cosine",
  vector_count: 0,
  created_at: "2025-01-01T00:00:00Z",
  updated_at: "2025-01-01T00:00:00Z",
};

const NS_RESPONSE_WITH_FTS = {
  ...NS_RESPONSE,
  full_text_search: {
    content: {
      language: "english",
      stemming: true,
      remove_stopwords: true,
      case_sensitive: false,
      k1: 1.2,
      b: 0.75,
      max_token_length: 40,
    },
  },
};

const QUERY_RESPONSE = {
  results: [
    { id: "v1", score: 0.95, attributes: { color: "red" } },
    { id: "v2", score: 0.8 },
  ],
  scanned_fragments: 3,
  scanned_segments: 1,
};

function mockFetch(
  status: number,
  body?: unknown,
): typeof globalThis.fetch {
  return vi.fn(async () => ({
    status,
    statusText: status === 200 ? "OK" : "Error",
    json: async () => body,
    text: async () => (body !== undefined ? JSON.stringify(body) : ""),
  })) as unknown as typeof globalThis.fetch;
}

function mockFetchCapture(
  status: number = 200,
  body: unknown = QUERY_RESPONSE,
): {
  fetch: typeof globalThis.fetch;
  lastRequest: () => { url: string; init: RequestInit };
} {
  let lastUrl = "";
  let lastInit: RequestInit = {};

  const fetchFn = vi.fn(async (url: string | URL | Request, init?: RequestInit) => {
    lastUrl = typeof url === "string" ? url : url.toString();
    lastInit = init ?? {};

    return {
      status,
      statusText: "OK",
      json: async () => body,
      text: async () => (body !== undefined ? JSON.stringify(body) : ""),
    };
  }) as unknown as typeof globalThis.fetch;

  return {
    fetch: fetchFn,
    lastRequest: () => ({ url: lastUrl, init: lastInit }),
  };
}

describe("health", () => {
  it("returns health status", async () => {
    const client = new ZeppelinClient({
      baseUrl: "http://test:8080",
      fetch: mockFetch(200, { status: "ok" }),
    });
    const result = await client.health();
    expect(result).toEqual({ status: "ok" });
  });

  it("returns readiness status", async () => {
    const client = new ZeppelinClient({
      baseUrl: "http://test:8080",
      fetch: mockFetch(200, { status: "ready", s3_connected: true }),
    });
    const result = await client.ready();
    expect(result.s3_connected).toBe(true);
  });
});

describe("namespaces", () => {
  it("creates a namespace", async () => {
    const client = new ZeppelinClient({
      baseUrl: "http://test:8080",
      fetch: mockFetch(201, NS_RESPONSE),
    });
    const ns = await client.createNamespace("test-ns", 128);
    expect(ns.name).toBe("test-ns");
    expect(ns.dimensions).toBe(128);
    expect(ns.distance_metric).toBe("cosine");
  });

  it("creates namespace with FTS config", async () => {
    const { fetch: fetchFn, lastRequest } = mockFetchCapture(201, NS_RESPONSE_WITH_FTS);

    const client = new ZeppelinClient({
      baseUrl: "http://test:8080",
      fetch: fetchFn,
    });
    const ns = await client.createNamespace("test-ns", 128, {
      fullTextSearch: { content: { language: "english" } },
    });
    expect(ns.full_text_search?.content?.stemming).toBe(true);

    const body = JSON.parse(lastRequest().init.body as string);
    expect(body.full_text_search).toEqual({
      content: { language: "english" },
    });
  });

  it("lists namespaces", async () => {
    const client = new ZeppelinClient({
      baseUrl: "http://test:8080",
      fetch: mockFetch(200, [NS_RESPONSE]),
    });
    const nss = await client.listNamespaces();
    expect(nss).toHaveLength(1);
    expect(nss[0].name).toBe("test-ns");
  });

  it("gets a namespace", async () => {
    const client = new ZeppelinClient({
      baseUrl: "http://test:8080",
      fetch: mockFetch(200, NS_RESPONSE),
    });
    const ns = await client.getNamespace("test-ns");
    expect(ns.name).toBe("test-ns");
  });

  it("deletes a namespace", async () => {
    const fetchFn = mockFetch(204);
    const client = new ZeppelinClient({
      baseUrl: "http://test:8080",
      fetch: fetchFn,
    });
    await client.deleteNamespace("test-ns");
    expect(fetchFn).toHaveBeenCalled();
  });
});

describe("vectors", () => {
  it("upserts vectors", async () => {
    const client = new ZeppelinClient({
      baseUrl: "http://test:8080",
      fetch: mockFetch(200, { upserted: 2 }),
    });
    const count = await client.upsertVectors("test-ns", [
      { id: "v1", values: [1.0, 2.0] },
      { id: "v2", values: [3.0, 4.0] },
    ]);
    expect(count).toBe(2);
  });

  it("upserts vectors with attributes", async () => {
    const { fetch: fetchFn, lastRequest } = mockFetchCapture(200, { upserted: 1 });

    const client = new ZeppelinClient({
      baseUrl: "http://test:8080",
      fetch: fetchFn,
    });
    await client.upsertVectors("test-ns", [
      { id: "v1", values: [1.0], attributes: { color: "red" } },
    ]);

    const body = JSON.parse(lastRequest().init.body as string);
    expect(body.vectors[0].attributes).toEqual({ color: "red" });
  });

  it("deletes vectors", async () => {
    const client = new ZeppelinClient({
      baseUrl: "http://test:8080",
      fetch: mockFetch(200, { deleted: 3 }),
    });
    const count = await client.deleteVectors("test-ns", ["v1", "v2", "v3"]);
    expect(count).toBe(3);
  });
});

describe("query", () => {
  it("runs vector query", async () => {
    const client = new ZeppelinClient({
      baseUrl: "http://test:8080",
      fetch: mockFetch(200, QUERY_RESPONSE),
    });
    const result = await client.query("test-ns", {
      vector: [0.1, 0.2],
      topK: 5,
    });
    expect(result.results).toHaveLength(2);
    expect(result.results[0].id).toBe("v1");
    expect(result.results[0].score).toBe(0.95);
    expect(result.results[0].attributes).toEqual({ color: "red" });
    expect(result.results[1].attributes).toBeUndefined();
    expect(result.scanned_fragments).toBe(3);
    expect(result.scanned_segments).toBe(1);
  });

  it("sends filter in request body", async () => {
    const { fetch: fetchFn, lastRequest } = mockFetchCapture();
    const client = new ZeppelinClient({
      baseUrl: "http://test:8080",
      fetch: fetchFn,
    });
    await client.query("test-ns", {
      vector: [0.1],
      filter: Filters.eq("color", "red"),
    });

    const body = JSON.parse(lastRequest().init.body as string);
    expect(body.filter).toEqual({
      op: "eq",
      field: "color",
      value: "red",
    });
  });

  it("sends BM25 rank_by", async () => {
    const { fetch: fetchFn, lastRequest } = mockFetchCapture();
    const client = new ZeppelinClient({
      baseUrl: "http://test:8080",
      fetch: fetchFn,
    });
    await client.query("test-ns", {
      rankBy: RankBy.bm25("content", "search query"),
    });

    const body = JSON.parse(lastRequest().init.body as string);
    expect(body.rank_by).toEqual(["content", "BM25", "search query"]);
    expect(body.vector).toBeUndefined();
  });

  it("sends last_as_prefix", async () => {
    const { fetch: fetchFn, lastRequest } = mockFetchCapture();
    const client = new ZeppelinClient({
      baseUrl: "http://test:8080",
      fetch: fetchFn,
    });
    await client.query("test-ns", {
      rankBy: RankBy.bm25("content", "sea"),
      lastAsPrefix: true,
    });

    const body = JSON.parse(lastRequest().init.body as string);
    expect(body.last_as_prefix).toBe(true);
  });

  it("sends consistency", async () => {
    const { fetch: fetchFn, lastRequest } = mockFetchCapture();
    const client = new ZeppelinClient({
      baseUrl: "http://test:8080",
      fetch: fetchFn,
    });
    await client.query("test-ns", {
      vector: [0.1],
      consistency: "eventual",
    });

    const body = JSON.parse(lastRequest().init.body as string);
    expect(body.consistency).toBe("eventual");
  });

  it("sends nprobe", async () => {
    const { fetch: fetchFn, lastRequest } = mockFetchCapture();
    const client = new ZeppelinClient({
      baseUrl: "http://test:8080",
      fetch: fetchFn,
    });
    await client.query("test-ns", { vector: [0.1], nprobe: 4 });

    const body = JSON.parse(lastRequest().init.body as string);
    expect(body.nprobe).toBe(4);
  });
});

describe("error handling", () => {
  it("throws ValidationError on 400", async () => {
    const client = new ZeppelinClient({
      baseUrl: "http://test:8080",
      fetch: mockFetch(400, {
        error: "dimensions must be > 0",
        status: 400,
      }),
    });
    await expect(
      client.createNamespace("bad", 0),
    ).rejects.toThrow(ValidationError);
  });

  it("throws NotFoundError on 404", async () => {
    const client = new ZeppelinClient({
      baseUrl: "http://test:8080",
      fetch: mockFetch(404, { error: "not found", status: 404 }),
    });
    await expect(
      client.getNamespace("missing"),
    ).rejects.toThrow(NotFoundError);
  });

  it("throws ConflictError on 409", async () => {
    const client = new ZeppelinClient({
      baseUrl: "http://test:8080",
      fetch: mockFetch(409, {
        error: "namespace already exists",
        status: 409,
      }),
    });
    await expect(
      client.createNamespace("dup", 128),
    ).rejects.toThrow(ConflictError);
  });

  it("throws ServerError on 500", async () => {
    const client = new ZeppelinClient({
      baseUrl: "http://test:8080",
      fetch: mockFetch(500, { error: "internal error", status: 500 }),
    });
    await expect(client.listNamespaces()).rejects.toThrow(ServerError);
  });

  it("includes error message", async () => {
    const client = new ZeppelinClient({
      baseUrl: "http://test:8080",
      fetch: mockFetch(404, { error: "namespace 'foo' not found", status: 404 }),
    });
    try {
      await client.getNamespace("foo");
    } catch (e) {
      expect((e as NotFoundError).message).toBe("namespace 'foo' not found");
      expect((e as NotFoundError).statusCode).toBe(404);
    }
  });
});

describe("URL encoding", () => {
  it("encodes namespace names in URL", async () => {
    const fetchFn = vi.fn(async () => ({
      status: 200,
      statusText: "OK",
      json: async () => NS_RESPONSE,
      text: async () => JSON.stringify(NS_RESPONSE),
    })) as unknown as typeof globalThis.fetch;

    const client = new ZeppelinClient({
      baseUrl: "http://test:8080",
      fetch: fetchFn,
    });
    await client.getNamespace("my ns");

    const calledUrl = (fetchFn as ReturnType<typeof vi.fn>).mock.calls[0][0];
    expect(calledUrl).toBe("http://test:8080/v1/namespaces/my%20ns");
  });
});
