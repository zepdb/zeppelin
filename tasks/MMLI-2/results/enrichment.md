# MMLI-2 Phase 6 — Background Enrichment

## Persisted formats

- Late-state section: version `3`.
- Multi-vector matrix artifact: `ZME1`, version `1`.
- FDE artifact: `ZFD1`, version `1`.
- Centering artifact: `ZCM1`, version `1`.
- Quarantine evidence: `ZEQ1`, version `1`.

The default and merge path is `f16`. `int8_sym_v1` encoding and decoding are
implemented, but activation remains fail-closed without a production
qualification stamp. The production writer/decoder ranking qualification in
`02a-int8-matrix.md` remains the next decision.

The canonical FDE recipe records candidate-only document pooling. Text uses
`Identity`; the approved visual operating point uses `ContiguousMean {
factor: 2 }`, with the final tail divided by its actual row count and no
renormalization. Exact-scoring matrices remain unpooled.

## Publication and lease measurement

The MinIO stale-output/lease-discipline test held the encoder behind a barrier
and observed that `{namespace}/lease.json` did not exist while encoding was in
flight. It then paused the immutable section create and root-manifest CAS
separately and observed the live lease at both boundaries. Two completed
overlay publications measured:

- lease PUTs: `8`;
- immutable late-section create PUTs: `2`;
- conditional root-manifest PUTs: `2`.

The same test published the stale physical version without counting it toward
the newer live version, then enriched the newer version and reached
`SemanticState::Ready`.

## Validation

- `CARGO_INCREMENTAL=0 cargo test --lib embedding --no-fail-fast`: PASS,
  `32` passed, `1` ignored real-model fixture.
- `TEST_BACKEND=minio CARGO_INCREMENTAL=0 cargo test --test
  enrichment_tests --no-fail-fast -- --nocapture`: PASS, `15/15`.
- `CARGO_INCREMENTAL=0 cargo clippy --all-targets -- -D warnings`: PASS.
- `CARGO_INCREMENTAL=0 cargo fmt --all -- --check`: PASS.
- `git diff --check`: PASS.
- Profile activation convergence regression: PASS, `1/1`.
- OpenAPI route and contract inventory checks: PASS, `2/2`.
- MinIO typed-ingest write-ack regression: PASS, `1/1`.

The minimum-bar scenarios cover production-seam matrix/FDE decode and direct
FDE equality, restart at all three checkpoints with create-only collision
rejection plus byte-identical matrix/FDE reuse, stale-output coverage, and
lease-free encoding with fenced publication. Durable row-specific poison
evidence additionally leaves healthy rows in the same fragment and later work
runnable, while keeping the coverage watermark below the failed record.
Bounded discovery skips settled historical input WALs without reading their
payloads and charges inspected bytes even when work is already in flight.
Exhausting all publication CAS attempts encodes the failed physical source
recipe exactly once, suppresses its rediscovery for the coordinator lifetime,
and still admits a newly published physical source.
The owning maintenance thread concurrently supervises the executor failure
signal, so an exhausted terminal work failure ends that thread immediately
rather than remaining buffered until shutdown.
