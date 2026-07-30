# MMLI-2 Phase 4 — Manifest Late Section

Pinned input revision: `0ae2bd227d471ddd1484090d31f1a6ad00ffdba4`

## Binding decision

`late_state: Some(_)` advances the execution binding to a new
`V5LateState` projection because the reference controls retention and branch
locality. Dense manifests with `late_state: None` retain their pre-existing
binding version. The V5 projection binds the section key, checksum, byte size,
format version, and physical artifact origin without extending any frozen
earlier binding projection.

## Dense byte constant

The trailing `late_state: None` adds exactly **1 byte** to the MessagePack root
manifest. The test compares a representative one-fragment/one-segment manifest
against a frozen exact pre-Phase-4 hex fixture, then repeats the measurement at
0, 1, and 64 fragments (with 0, 1, and 8 segments respectively). Every measured
delta is 1 byte.

## Tier-1 observation

The controlled same-machine pair used separately compiled executables:

- baseline source: `0ae2bd227d471ddd1484090d31f1a6ad00ffdba4`
- baseline executable SHA-256:
  `7e1b7776f749161eb77a843cd876d9d29cc07a280079873875b771ce7ebc4048`
- candidate executable SHA-256:
  `08cafeb66bfe85157429f1a1211f80ca2945f5ddb928683ebdf7a978b1d8c079`
- baseline report:
  `/private/tmp/mmli2-phase4-paired-v3/baseline/run-1785392219-198880000-83124/report.md`
- candidate report:
  `/private/tmp/mmli2-phase4-paired-v3/candidate/run-1785392253-432418000-83220/report.md`
- status: PASS, 20/20 scenarios in both runs
- manifest operations: 27 GET attempts and 5 PUTs in both runs
- aggregate manifest GET bytes: 38,006 before; 38,011 after
- aggregate manifest PUT bytes: 11,556 before; 11,562 after

The additive field accounts for +17 GET bytes (17 returned manifest bodies)
and +5 PUT bytes (five manifest bodies). The remaining paired-run deltas are
the independently randomized incarnation term described below: -12 GET bytes
and +1 PUT byte, yielding the observed +5 GET and +6 PUT totals. In
`upsert_single`, the manifest body was 1,273/1,308 bytes before and
1,276/1,311 after for GET/PUT: +1 byte from `late_state: None` and +2 bytes
from that run's different incarnation encoding. No `late/state` operation
occurred in either dense run, so the section object is neither written on
every upsert nor fetched on a dense read.

Only 17 of the 27 manifest GET attempts returned a body; the others were
conditional freshness responses with zero bytes. The raw cross-process GET
aggregate includes a separate pre-existing source of byte variation:
`NamespaceIncarnationId::new()` mints a random UUIDv4 per scenario, and the
manifest persists it as `[u8; 16]`. `rmp-serde` encodes those elements as
minimal MessagePack integers, so each random byte below 128 occupies one byte
and each byte at or above 128 occupies two. Independently generated
incarnations therefore move the root size by their differing high-bit-byte
counts. Two consecutive executions of the unchanged baseline executable
confirmed this: manifest GET bytes moved 37,986 to 37,993, PUT bytes moved
11,552 to 11,555, and `upsert_single` moved by three bytes with no code change.

The exact same-manifest fixture removes that random-input term and measures a
constant one-byte delta at every tested fragment/segment shape. Retained
history bodies use the same `Manifest` encoding, so each retained generation
has the same constant. The controlled Tier-1 pair changed no operation count,
added no section-object operation, and preserved all 20 pinned contracts. No
contract was rebaselined.

## Storage and cache

- Key shape:
  `{physical-namespace}/late/state/{sha256-of-canonical-section-bytes}`
- Cache: process-local immutable map keyed by the exact section ref tuple
  (object key, checksum, size, and format version), capped at 256 decoded
  sections; no TTL and no invalidation.

## Validation

Final acceptance:

- `CARGO_INCREMENTAL=0 cargo test --lib wal`: PASS, 104/104.
- `TEST_BACKEND=minio CARGO_INCREMENTAL=0 cargo test --test
  storage_gc_tests`: PASS, 78/78.
- `TEST_BACKEND=minio CARGO_INCREMENTAL=0 cargo test --test
  late_section_tests`: PASS, 11/11.
- `CARGO_INCREMENTAL=0 cargo clippy --all-targets -- -D warnings`: PASS.
- `CARGO_INCREMENTAL=0 cargo fmt --all -- --check`: PASS.
- `CARGO_INCREMENTAL=0 cargo test --lib
  late_section_horizons_use_authoritative_list_metadata`: PASS, 1/1.
- Controlled Tier-1 baseline/candidate runs: PASS, 20/20 each.
