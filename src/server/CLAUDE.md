# src/server — HTTP surface

Thin layer over domain logic. Handlers translate HTTP into namespace/WAL/
query/compaction calls; they must not reimplement domain rules or bypass the
manifest contract.

## Axum 0.7 — parameter syntax

Pinned to **axum 0.7**, which uses `:param`. Axum 0.8 uses `{param}`.

If a parameterized route 404s while static routes work, suspect this first.
It has bitten this repo before.

Related trap: **S3 keys and URL paths have different rules.** S3 keys may
contain `/`; a URL `:param` segment may not. Tests keep these separate —
`harness.key()` builds S3 keys, `api_ns()` builds URL paths. Do not use one
for the other.

## Router shape

Query endpoints get a **separate, lighter router** than everything else: no
request-id or `TraceLayer`, and direct `serde_json` deserialization rather
than Axum's `serde_path_to_error` wrapper. This is a deliberate hot-path
optimization. If you add a global middleware, decide explicitly whether the
query router should get it, and expect a QPS regression if it does.

Tower layers run from the last added toward the handler. The ordering in
`build_router` is load-bearing; read the ASCII diagram in `mod.rs`'s rustdoc
before reordering.

## Route authorization

Every protected route goes through `secure_route` + the central route map
(`classify_route` / `route_resource`). An unmapped route fails closed with
`UnmappedRoute` rather than defaulting to public.

`tests/contract_tests.rs` asserts the OpenAPI document matches the routed
surface exactly:

```bash
cargo test --test contract_tests openapi_documents_exact_routed_surface -- --exact
cargo test --test contract_tests openapi_documents_bearer_security_for_every_protected_operation -- --exact
```

**Adding a route means updating `api/zeppelin-api.yaml`**, or these fail.

## Conditionally registered routes

`/v1/namespaces/:ns/branches` is registered **only** when
`config.branching.enabled` (`mod.rs:2742`). It is the one route whose
existence depends on config. Handlers still re-check the flag as defense in
depth — keep that belt-and-suspenders check when adding gated routes.

## `/readyz` is cheap — it reads a snapshot

`readiness_check` reads a published `BranchGraphReadinessSnapshot` and issues
no object-store work of its own. The O(namespaces) scan
(`NamespaceGraph::inspect_readiness`) runs only on the budgeted background
maintenance pass, and not at all when `branching.enabled = false`. An
aggressive load-balancer health check is fine here. See
`../namespace/CLAUDE.md` for the lag/threshold trade.

> Corrected 2026-07-24. An earlier revision said the scan ran on every probe,
> unbudgeted. That was true when written and was fixed in `4f8583c`.

`security.readyz_public` controls whether the endpoint is unauthenticated and
whether repair identities are redacted from the body.

## Deletion

`delete_namespace` returns **202 Accepted** with `state: "deleting"` and drives
the governed state machine through `NamespaceGraph::delete`, then spawns
`spawn_namespace_delete_cleanup` to continue in the background. It is not a
synchronous delete; tests must poll or drive `resume_delete`.

## See also

- `../namespace/CLAUDE.md` — the governed delete state machine
- `../security/CLAUDE.md` — kernel admission, entitlements, audit
