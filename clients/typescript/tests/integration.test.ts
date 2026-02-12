/**
 * Integration tests for the Zeppelin TypeScript client.
 *
 * Requires a running Zeppelin server. Set ZEPPELIN_URL to enable.
 *
 *   ZEPPELIN_URL=http://localhost:8080 npx vitest run tests/integration.test.ts
 */

import { describe, it, expect, beforeAll } from "vitest";
import { ZeppelinClient } from "../src/client.js";
import { Filters } from "../src/filters.js";
import { RankBy } from "../src/rank-by.js";
import { ConflictError, NotFoundError } from "../src/errors.js";
import crypto from "node:crypto";

const ZEPPELIN_URL = process.env.ZEPPELIN_URL;

const describeIntegration = ZEPPELIN_URL
  ? describe
  : describe.skip;

function uniqueNs(): string {
  return `test-ts-${crypto.randomUUID().slice(0, 8)}`;
}

describeIntegration("integration", () => {
  let client: ZeppelinClient;

  beforeAll(() => {
    client = new ZeppelinClient({ baseUrl: ZEPPELIN_URL! });
  });

  it("health check", async () => {
    const result = await client.health();
    expect(result.status).toBe("ok");
  });

  it("readiness check", async () => {
    const result = await client.ready();
    expect(result.s3_connected).toBe(true);
  });

  it("namespace lifecycle", async () => {
    const name = uniqueNs();

    // Create
    const ns = await client.createNamespace(name, 4);
    expect(ns.name).toBe(name);
    expect(ns.dimensions).toBe(4);

    // Get
    const ns2 = await client.getNamespace(name);
    expect(ns2.name).toBe(name);

    // List
    const nss = await client.listNamespaces();
    expect(nss.some((n) => n.name === name)).toBe(true);

    // Duplicate fails
    await expect(client.createNamespace(name, 4)).rejects.toThrow(
      ConflictError,
    );

    // Delete
    await client.deleteNamespace(name);

    // Get after delete
    await expect(client.getNamespace(name)).rejects.toThrow(NotFoundError);
  });

  it("vector upsert and query", async () => {
    const name = uniqueNs();
    await client.createNamespace(name, 4);

    // Upsert
    const count = await client.upsertVectors(name, [
      { id: "v1", values: [1, 0, 0, 0], attributes: { color: "red" } },
      { id: "v2", values: [0, 1, 0, 0], attributes: { color: "blue" } },
      { id: "v3", values: [0, 0, 1, 0], attributes: { color: "red" } },
    ]);
    expect(count).toBe(3);

    // Query
    const result = await client.query(name, {
      vector: [1, 0, 0, 0],
      topK: 2,
    });
    expect(result.results.length).toBeLessThanOrEqual(2);
    expect(result.results[0].id).toBe("v1");

    // Query with filter
    const filtered = await client.query(name, {
      vector: [1, 0, 0, 0],
      topK: 10,
      filter: Filters.eq("color", "blue"),
    });
    for (const r of filtered.results) {
      expect(r.attributes?.color).toBe("blue");
    }

    // Delete vectors
    const deleted = await client.deleteVectors(name, ["v1"]);
    expect(deleted).toBe(1);

    // Cleanup
    await client.deleteNamespace(name);
  });

  it("FTS namespace and BM25 query", async () => {
    const name = uniqueNs();
    const ns = await client.createNamespace(name, 4, {
      fullTextSearch: { content: { language: "english" } },
    });
    expect(ns.full_text_search?.content).toBeDefined();

    await client.upsertVectors(name, [
      {
        id: "d1",
        values: [1, 0, 0, 0],
        attributes: { content: "the quick brown fox" },
      },
      {
        id: "d2",
        values: [0, 1, 0, 0],
        attributes: { content: "lazy dog sleeps" },
      },
      {
        id: "d3",
        values: [0, 0, 1, 0],
        attributes: { content: "quick fox jumps over lazy dog" },
      },
    ]);

    const result = await client.query(name, {
      rankBy: RankBy.bm25("content", "quick fox"),
      topK: 3,
    });
    expect(result.results.length).toBeGreaterThan(0);

    await client.deleteNamespace(name);
  });
});
