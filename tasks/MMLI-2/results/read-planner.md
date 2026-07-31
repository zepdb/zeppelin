# MMLI-2 Phase 8 — Wave Read Planner

## Collapse measurement

The MinIO fixture wrote four 256 KiB immutable objects and issued 64 logical
1 KiB reads. The caller-order workload included adjacent ranges, duplicate
ranges, unsorted inputs, and two distant regions per object.

- Logical reads: `64`
- Planned and observed physical range GETs: `8`
- Planned and observed ranges per object:
  `4096..16024`, `131072..143024`
- Physical bytes including coalesced gaps: `95,520`
- Logical bytes before duplicate elimination: `65,536`

Every planned output was byte-identical to the corresponding naive MinIO
`get_range` result. `GetCounter` observed exactly the plan's eight physical
requests and exact planned ranges.

## Default bounds

- Gap budget: `64 KiB`. This amortizes object-store first-byte latency while
  keeping speculative gap reads much narrower than the dependency's default.
- Maximum request: `8 MiB`. This bounds one retained response buffer while
  allowing useful contiguous matrix-block reads.
- Maximum concurrency: `8`. This hides independent-object latency without
  allowing request count or retained buffers to grow without bound.

Every bound is validated as nonzero, and the gap budget must be smaller than
the maximum request size. A logical request larger than the request cap fails
explicitly because returning it as one no-copy `Bytes` slice cannot span two
physical buffers.

## Pinned object_store behavior

Zeppelin pins `object_store 0.11.2`. Its default `get_ranges` implementation
sorts ranges, merges successive gaps of at most `1 MiB`, and executes up to
ten merged requests concurrently. It has no maximum merged-request size and
slices a short response to the bytes actually returned.

The Phase 8 executor therefore submits each already-planned physical range as
a singleton `get_ranges` call. This preserves the planner's cumulative gap
budget, request-size cap, exact physical count, and outer concurrency bound
instead of letting the dependency re-plan them.

## Validation

- Planner unit suite: `5` passed, `0` failed.
- MinIO integration suite: `10` passed, `0` failed, including the required
  byte-exactness and physical-range proof.
- Clippy with all targets and warnings denied: PASS.
- Rustfmt and `git diff --check`: PASS.
- Independent acceptance rerun and dense-neutrality review: PASS, with the
  same `64 → 8 → 95,520` measurement and no findings.
