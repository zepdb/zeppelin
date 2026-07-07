import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

const repo = process.env.ZEPPELIN_TS_REPO;
const baseUrl = process.env.ZEPPELIN_URL;
const namespace = process.env.ZEPPELIN_PARITY_NAMESPACE;
const queryJson = process.env.ZEPPELIN_PARITY_QUERY;

if (!repo) throw new Error("ZEPPELIN_TS_REPO is required");
if (!baseUrl) throw new Error("ZEPPELIN_URL is required");
if (!namespace) throw new Error("ZEPPELIN_PARITY_NAMESPACE is required");
if (!queryJson) throw new Error("ZEPPELIN_PARITY_QUERY is required");

const entrypoint = path.join(repo, "dist", "index.js");
if (!fs.existsSync(entrypoint)) {
  throw new Error(`TypeScript client build output is missing: ${entrypoint}`);
}

const { ZeppelinClient } = await import(pathToFileURL(entrypoint).href);
const client = new ZeppelinClient({ baseUrl, timeout: 60_000 });
const response = await client.query(namespace, JSON.parse(queryJson));

console.log(JSON.stringify({
  ids: response.results.map((result) => result.id),
  scores: response.results.map((result) => result.score),
  facets: response.facets ?? null,
}));
