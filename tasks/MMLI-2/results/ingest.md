# MMLI-2 Phase 5 — Typed Multimodal Ingest

Pinned input revision: `592ee5398982a88d6303bb3003206425be5cc39a`

## Status

PASS. Phase 5 accepts, persists, replays, branches, snapshots, and garbage
collects typed text, image, and image-plus-text retrieval units. No encoder,
model runtime, or semantic query path is present.

The prerequisite Phase 1 and Phase 4 commits are ancestors of the pinned input:

- `3e8cc4a` — MUVERA FDE kernel and scalar MaxSim.
- `06d53ee` — content-addressed late-state manifest sections.

## What landed

- `IndexType::LateInteractionFde` and immutable namespace admission config.
  Late namespaces use `dimensions = 0`; dense writes fail with a typed error.
- Typed retrieval-unit, source-reference, content-hash, artifact-checksum, and
  semantic-coverage types under `src/embedding/`.
- A separately framed and versioned MessagePack input WAL with canonical map
  hashing and checked reads.
- Source create-only publication, late-section inventory publication, input
  fragment publication, then root-manifest CAS as the sole visibility point.
- `POST /v1/namespaces/:ns/retrieval-units` with fail-loud text, image,
  modality, dimensions, media-type, and request-size admission.
- Total-sequence FTS replay across dense and input WAL fragments.
- Origin-aware branch reads, root-release locality checks, retained-history and
  snapshot reachability, branch-local section rebasing, and exact-key
  source/input-WAL garbage collection.
- Full typed clones copy retained sources, rebuild a target-local v2 section,
  and rewrite checked input-WAL image keys without changing fragment IDs or
  replay order.

## Binding decision

The root manifest advances to binding projection `V6TypedIngest` whenever
`input_fragments` is non-empty or `semantic_coverage` is present. The
projection binds every input-fragment reference and the semantic-coverage
state. Dense manifests with both fields at their defaults retain their
pre-existing binding projection.

The two trailing root fields add exactly **2 bytes** beyond Phase 4 at each
tested shape. Combined with Phase 4's trailing `late_state: None`, the frozen
pre-Phase-4 fixture delta is the approved constant **3 bytes** at 0, 1, and 64
fragments: `[3, 3, 3]`. This planned root-shape pin change was explicitly
approved; no observed Tier-1 contract was rebaselined.

Source inventory remains outside the root. Its content is bound transitively
by the checksum-addressed `ManifestSectionRef`, whose key, checksum, size,
format version, and origin are already part of the Phase 4 projection.

## Section and WAL versions

- Late-state section format: **v2**.
- Legacy late-state section v1 decodes with an empty source inventory.
- Input WAL format: independent magic plus **v1** framing.
- Legacy root manifests decode `input_fragments = []` and
  `semantic_coverage = None`.

## Registry families

- `InputWal`: `{namespace}/input-wal/{fragment}.wal`
- `Source`: `{namespace}/sources/{content_hash}`

Both families are manifest-referenced, participate in physical-origin
resolution and locality, and use the registry's deferred-delete and exact-key
GC ownership decisions. Source reachability expands from the selected
late-state section.

## Measured write depth

The MinIO counting test measured total object-store PUTs for one admitted
write:

| Path | PUTs |
| --- | ---: |
| Dense vector write | 4 |
| Text-only retrieval-unit write | 4 |
| Image retrieval-unit write | 6 |

Text-only ingest therefore has the same write depth as dense ingest. Image
ingest adds exactly one create-only source PUT and one checksum-addressed
late-section PUT.

## Minimum-bar coverage

1. Lifecycle: text, image, and image-plus-text upsert/read/update/tombstone;
   checked source reads; restart and replay ordering.
2. GC: live source retention, orphan source collection, and snapshot-pinned
   source plus section retention. Warm memoized cycles preserve section-expanded
   history roots, while aged source keys in `pending_deletes` drain normally.
3. FTS: immediate BM25 visibility, update ordering, delete suppression, and
   image-only absence.
5. Branch/snapshot: zero-copy inherited input reads through physical origins
   and root-release blocking while source references are foreign. Republished
   target-local sections preserve inherited source owners, status remains
   non-materialized, and a full typed clone owns checked copies of every source
   and input fragment.

## Validation

- `CARGO_INCREMENTAL=0 cargo test --lib`: PASS, 683/683.
- `TEST_BACKEND=minio CARGO_INCREMENTAL=0 cargo test --test
  typed_ingest_tests`: PASS, 16/16.
- `CARGO_INCREMENTAL=0 cargo clippy --all-targets -- -D warnings`: PASS.
- `CARGO_INCREMENTAL=0 cargo fmt --all -- --check`: PASS.
- `git diff --check`: PASS.

Phase 5 has no separate dense-neutrality gate; the binding README assigns
those structural gates to Phases 3, 4, and 8. The full library suite and the
exact dense/text write-depth assertion cover the dense paths touched here.
