# cleanup-zeppelin.md — Plan for Stabilizing & Legitimizing Zeppelin

Branch: `fable/legitimize-repo` (created from `main` @ d45d236, carrying prior uncommitted legitimization work)

## Context

A prior session left ~19 modified files uncommitted on `main` plus a `LEGITIMIZE_REPORT.md`
claiming full validation. Per the goal, all claims are treated as suspect until re-verified
against the implementation. Pre-existing untracked artifacts (`REPORT.md`, `result_*.txt`,
`states/`) are left untouched.

## Phases

### 1. Baseline (verify, don't trust)
- [ ] Record toolchain: rustc 1.93.0, cargo 1.93.0, cargo-deny 0.19.9, cargo-audit 0.22.2
- [ ] `cargo fmt --all -- --check`
- [ ] `cargo check --all-targets`
- [ ] `cargo clippy --all-targets -- -D warnings`
- [ ] `cargo test --lib`
- [ ] `cargo test --tests` (default backend)
- [ ] MinIO-backed `cargo test --tests` (`TEST_BACKEND=minio`, compose stack already up)
- [ ] `docker build`
- [ ] `cargo deny check`, `cargo audit`

### 2. Fix what baseline finds
- [ ] Build/clippy/fmt failures
- [ ] Flaky or failing tests (root cause, no sleeps-as-fixes)

### 3. Runtime validation
- [ ] Run service against MinIO; exercise healthz, readyz, namespace CRUD,
      upsert, query, delete-vector, delete-namespace end to end

### 4. Docs/spec drift audit
- [ ] README.md vs routes/types
- [ ] SKILL.md vs routes/types
- [ ] api/zeppelin-api.yaml vs axum router + serde types (filters, rank_by, UUID namespaces)
- [ ] Env vars, bucket names, Rust version, SDK claims

### 5. Hardening spot-checks
- [ ] Dimension mismatch, top_k/nprobe bounds, empty IDs, malformed JSON,
      missing namespace, duplicate upsert, delete semantics, BM25/vector exclusivity,
      readiness when S3 down

### 6. Maintainability sweep (conservative)
- [ ] Dead code, misleading names, unwrap/expect in request/storage paths, unused deps

### 7. CI reflects reality
- [ ] fmt + check + clippy + unit + MinIO integration + docker build, no secrets required

## Commit strategy

Logical checkpoints on `fable/legitimize-repo` once validation improves; no pushes.

## Deliverables

- This plan
- Updated `LEGITIMIZE_REPORT.md` with verified (not claimed) results
