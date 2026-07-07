# Zeppelin Contract Fixtures

`contract/fixtures/v0.3.0/` contains engine-generated request and response
fixtures for the public HTTP contract documented in `api/zeppelin-api.yaml`.
Client SDK tests consume these files as golden wire examples without running
MinIO or a Zeppelin server.

Each version directory contains:

- `manifest.json` — ordered case metadata: `name`, `method`, `path`, `status`,
  request filename, and response filename.
- `<case>.req.json` — the JSON request body. Routes without a body use `null`.
- `<case>.resp.json` — the JSON response body. `204 No Content` uses `null`.

Fixtures are produced by the real axum handlers in
`tests/contract_tests.rs`. Regenerate them after intentional contract changes:

```sh
UPDATE_CONTRACT_FIXTURES=1 cargo test --test contract_tests \
  contract_fixtures_match_real_engine_output -- --nocapture
```

Then run the comparator normally:

```sh
cargo test --test contract_tests -- --nocapture
TEST_BACKEND=minio cargo test --test contract_tests -- --nocapture
```

The exporter canonicalizes only volatile values that clients must treat as
opaque or time-dependent: namespace names, timestamps, latency counters,
segment IDs, and cursor tokens. Response shapes, status codes, error envelopes,
query result bodies, facets, grouping, explain blocks, and batch entries remain
real engine output.
