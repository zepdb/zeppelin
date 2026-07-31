# MMLI-2 Phase 2a — int8 Matrix Payload Validation & Implementation

Self-contained addendum to Phase 2. Extends the lab with an int8 retention
probe. If the probe passes, defines the implementation plan across Phases 6,
7, and 9. Run after the 2× candidate-side pooling gate is committed.

## Motivation

Visual multi-vector truth is ~521 KB/page at f32, ~260 KB at f16. At the
Phase-2 operating point (config E, K=300), wave 2 fetches ~300 matrices per
query — ~78 MB at f16. int8 halves it again to ~39 MB/query, directly
reducing S3 egress latency and cost.

The encoder L2-normalizes rows before writing f16 matrices — every row
vector lies on the unit sphere, range [-1, 1]. This makes naive int8
(scale=127, no per-row calibration) viable since dynamic range is bounded
and known a priori.

## The Probe

Reuse the existing f16 visual matrices and f32 exact truth on disk. No
re-encoding needed.

| Variant | Description |
| --- | --- |
| Global-scale | int8 via `round(f16 * 127)`, dequantize `int8 / 127` before MaxSim |
| Per-row calibrated | int8 with per-row `{scale, offset}`, dequantize before MaxSim |

Measurements:

1. **f32 exact top-10 retained by int8 exact top-10** (both lanes, both
   variants). Same methodology as the existing f16 retention probe. Report
   same-rank fraction and top-10 recovery fraction.
2. **MaxSim absolute error** — 50 pairs, same protocol as the encoder
   conformance check. Compare int8 MaxSim output vs f32 exact MaxSim.
3. **Per-row error distribution** — for the first 100 documents, compute
   per-row L2 error from int8 quantization and report p50/p95/p99.
4. **Cost table extension** — add int8 bytes/page to the existing storage
   cost table.
5. **Candidate recall delta** — rerun config E's cutoff curve at K ∈ {50,
   100, 300} using int8-quantized matrices for the wave-2 exact MaxSim. If
   the int8 exact ordering differs from f32 exact ordering, the candidate
   recall gate (measuring fraction of f32 exact top-10 in f32 FDE-ranked
   top-K) is unaffected — **this measurement documents whether the
   FDE-ranked frontier would differ if int8 were the scoring truth.**

## Probe Passing Criteria

Global-scale variant achieves:
- **f32 exact top-10 recovered by int8 exact top-10 ≥ 0.999** (visual
  lane).
- **MaxSim absolute error ≤ 1e-3** (50 pairs).
- Both lanes pass.

If global-scale fails but per-row calibrated passes, the implementation
carries per-row calibration cost. If both fail, int8 is rejected and f16
stays as the Phase 6 payload format.

## Authorized follow-up investigation

The initial probe rejected both variants. Before deciding whether the raw
MaxSim-error criterion is useful for an 8-bit payload, a later
operator-authorized audit asks only whether accepted f16 changes retrieval
results relative to f32.

For both text and visual, using one encoder output per lane, compare f32
against f16, global-scale INT8, and per-row calibrated INT8:

1. Compare the f32 exact top-10 set with the same tensors round-tripped
   through f16.
2. Report the fraction of queries whose complete top-10 set is identical.
3. Report the fraction whose ordered top-10 list is identical.
4. Report same-document fractions independently for ranks 1 through 10.

This audit does not measure or reinterpret MaxSim numerical parity. It
directly compares the product-visible ranking behavior of all three stored
representations. It does not itself change either frozen INT8 threshold.

After the audit, the operator-authorized fixed ranking experiment is:

1. Current affine per-row INT8 plus whole-row L2 renormalization.
2. Symmetric per-row INT8 plus whole-row L2 renormalization, with one f16
   scale and no offset per row.
3. Groupwise symmetric INT8 plus whole-row L2 renormalization at fixed group
   sizes 32 and 16, with f16 scales.

Run the four cells on text first, then run only the best one or two on visual.
Queries remain f16. Evaluate top-10 set equality, ordered top-10 equality,
per-rank agreement, payload bytes, and dequantization cost. The raw MaxSim
error cutoff is not a decision gate for this follow-up; no replacement
percentage is invented before the query-level ranking distribution is
measured.

The raw tensor snapshot may persist only until the ranking result is durable.

## Ranking experiment outcome — decision record (2026-07-30)

**Gate rebaseline (operator decision, Anup, 2026-07-30).** The INT8
decision gate is product-visible ranking agreement measured
f16-relative, replacing raw MaxSim numerical parity. The frozen
thresholds above (≥ 0.999 recovery, ≤ 1e-3 error) stand as
measurements — every variant still fails them — but they are no
longer the decision gate. This is an explicit rebaseline, not a
silent weakening: f16 itself, the accepted production format, agrees
with f32 on only 99.910% of top-10 memberships, so bitwise-style
parity was never the operative product bar.

**Text results** (all 1,109 SciFact queries × 5,183 docs, same-pass
f32 reference, 11,090 top-10 memberships):

| Format | Recovered | Same set | Same top-1 | Misses | Saving |
| --- | --- | --- | --- | --- | --- |
| f16 | 99.910% | 99.098% | 99.910% | 10 | — |
| Global int8 | 92.164% | 39.134% | 96.573% | 869 | 50.000% |
| Affine/row | 98.061% | 80.884% | 98.828% | 215 | 46.875% |
| Affine/row+renorm | 99.351% | 93.508% | 99.729% | 72 | 46.875% |
| Sym/row+renorm | 99.342% | 93.417% | 99.549% | 73 | 49.219% |
| G32 sym+renorm | 99.369% | 93.688% | 99.820% | 70 | 46.875% |
| G16 sym+renorm | 99.414% | 94.139% | 99.820% | 65 | 43.750% |

Findings: (1) **renormalization is the dominant lever** — 215→72
misses at identical payload; (2) the four renorm variants are
statistically indistinguishable (differences ≤ 8 misses against
±12 counting noise), so **group size is config, not code**; (3)
every changed query lost exactly one membership; near-tie churn.

**Visual results** (all 533 queries × 2,000 pages, same-pass f32
reference, 5,330 top-10 memberships):

| Format | Recovered | Same set | Same order | Same top-1 | Misses | Saving |
| --- | --- | --- | --- | --- | --- | --- |
| f16 | 100.000% | 100.000% | 98.874% | 99.625% | 0 | — |
| Global int8 | 98.180% | 82.176% | 37.523% | 98.124% | 97 | 50.000% |
| Affine/row | 99.587% | 95.872% | 78.049% | 99.250% | 22 | 46.875% |
| G32 sym+renorm | 99.737% | 97.373% | 80.675% | 98.874% | 14 | 46.875% |
| G16 sym+renorm | 99.737% | 97.373% | 82.927% | 98.687% | 14 | 43.750% |

Both groupwise cells improve membership, same-set, and same-order
agreement over affine/row. G16 has the best same-order result; G32
ties its membership result with fewer scale bytes and one fewer
top-1 flip. Every variant retains the f32 top result somewhere in
its top 10 for every query.

**Production decision**: dual format per README delta 14 —
`f16` (default) | `int8_sym_v1 { group_size }`, pinned at profile
activation. Group size is chosen at qualification time from
{16, 32, 128} (128 = per-row, currently the Pareto point at 49.219%
saving); do not hardcode a group size in product code.

### `int8_sym_v1` canonical spec (folded normalization)

Input: one encoder-L2-normalized f16 row, dim 128, upcast to f32.

1. Per group `g` of `group_size` coords: base scale
   `s_g = f16_rne(maxabs_g / 127)`.
2. `q_i = clamp(round_ties_away(x_i / s_g), -127, 127) as i8`.
3. `N = sqrt(Σ_row (f32(q_i) · s_g)²)`, accumulated in f64 over the
   reconstruction with the f16-rounded base scales.
4. Stored scale `s'_g = f16_rne(s_g / N)`. Payload = i8 coords +
   f16 stored scales. No offsets. No norms stored.
5. Decode: `x̂_i = f32(q_i) × f32(s'_g)`. Nothing else — no
   accumulation, sqrt, or division on the read path; `max_sim` is
   fed the reconstructed f32 rows.

Special case: an all-zero group stores `s'_g = 0` and zero coords
(exact). The lab errors on this; production defines it.

Determinism pins (binding): coordinate rounding is
ties-away-from-zero; every f32→f16 conversion is IEEE
round-to-nearest-even; exactly ONE implementation exists (Rust —
the worker always emits f16 and never quantizes; a bare `np.round`
is ties-to-even and would diverge); a golden-vector fixture pins
encode and decode bytes.

Known residual (why the exit gate below exists): f16 rounding of the
folded scales leaves decoded rows unit-norm only to ~±3e-4, and the
measured lab cells used runtime f64 renormalization with unfolded
scales — a different arithmetic path. Nobody has measured the exact
stored bytes yet. Predicted shift: single-digit misses; must be
measured, not assumed.

### Qualification gate (executed as the Phase 6 exit gate)

Before `int8_sym_v1` becomes selectable for a profile/epoch, rerun
the ranking comparison through the PRODUCTION writer + decoder on
actually-written artifacts (not the lab dequant path): text = all
1,109 SciFact queries vs same-pass f32 truth; visual = the full
533-query suite. The stamp requires the production-path numbers to
reproduce the durable lab cells within binomial counting noise
(misses within ~±15 of the lab cell for the same variant) and
same-top-1 ≥ 99.5%. Anup signs the final per-lane thresholds when
the visual lab numbers land; until the stamp exists, profile
activation MUST refuse `int8_sym_v1`. This gate is a precondition
for offering int8, not a merge blocker for the f16 path.

The visual lab numbers now exist, but neither groupwise cell meets
the draft same-top-1 threshold (G32 98.874%, G16 98.687%). The
folded-scale production path is a different arithmetic path and
must still be measured; no final per-lane threshold is signed by
this lab result. Until the operator resolves that gate and the
production stamp passes, `int8_sym_v1` activation remains refused
and f16 remains the usable default.

### Production writer/decoder measurement (2026-07-31)

The fixed G32 cell was measured on both full lanes through actual
production `MatrixArtifact::to_bytes` persistence and
`MatrixArtifact::from_bytes` decoding. Queries remained f16 and the
f32/f16 references came from the same encoder pass.

| Lane | Reference | Top-10 recovered | Misses | Same set | Same order | Same top-1 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Text | f32 | 99.296664% | 78/11090 | 93.056808% | 69.161407% | 99.819657% |
| Text | f16 | 99.269612% | 81/11090 | 92.786294% | 69.251578% | 99.909829% |
| Visual | f32 | 99.737336% | 14/5330 | 97.373358% | 81.801126% | 99.249531% |
| Visual | f16 | 99.737336% | 14/5330 | 97.373358% | 81.238274% | 99.249531% |

Text misses moved 70→78, inside the draft 55–85 band, and text
same-top-1 met the draft 99.5% bar. Visual misses stayed 14, inside
the draft 0–29 band, but visual same-top-1 was 99.250% and did not
meet the draft 99.5% bar. Both lanes saved 46.875% of matrix payload
bytes relative to f16.

Decision: **measured, awaiting operator threshold**. The final visual
threshold remains unsigned. No qualification tuple or stamp is
approved, `int8_sym_v1` activation remains fail-closed, and f16
remains the usable default. Full evidence is in
`results/int8-production-qualification.md` and
`results/int8-production-qualification.json`.

## Implementation Plan (dual-format, decided 2026-07-30)

### Phase 6 changes (enrichment)

- `MatrixDtype` closed enum: `F16 | Int8SymV1 { group_size }`. Stamped
  in the matrix fragment header (authoritative; checksum covers
  coordinates + stored scales together) with a copy in section
  metadata; disagreement is a hard typed error.
- The encoder worker ALWAYS emits f16. Rust quantizes at enrichment
  per the canonical spec above when the profile pins int8 — the
  single-implementation rule.
- Profile activation pins the dtype (README delta 14) and refuses
  `int8_sym_v1` without the qualification stamp. Default `f16`.
- Phase 6 exit gate = the qualification gate above.

### Phase 7 changes (exact-overlay query)

- The overlay scanner reads `MatrixDtype` from the fragment header
  and decodes through the shared decoder: f16 upcast, or the
  `int8_sym_v1` decode (one convert + one multiply per coordinate).
- `max_sim` stays the ONLY scorer, always fed reconstructed f32
  rows. No integer scorer variant. Unknown discriminant = typed
  error, never a fallback.
- No change to the candidate path (FDEs are unaffected). Query
  matrices stay f16 unconditionally.

### Phase 9 changes (segment IVF, two-wave)

- Late segment compaction carries the matrix payload in whatever dtype
  the source fragments use (no format conversion during compaction).
  Dtype is epoch-uniform, so mixing occurs only across epochs.
- Segments/truth blocks carry the `MatrixDtype` discriminant (+ scale
  layout) in their headers; the wave-2 planner passes it through and
  the wave-2 decode dispatches on it.

### What does NOT change

- FDE format (stays f32 for IVF scoring).
- Centering mean (stays f32).
- Query matrices (stays f16 — query count is small; the savings don't
  justify the complexity).
- Wave-1 candidate path (FDE IVF is untouched).
- The candidate recall gate (still measured against f32 exact).

## Minimum bar

(1) The probe results appended to `results/lab.md`, (2) int8 retention
numbers meeting the passing criteria, (3) this decision record committed.
If the probe fails, commit the rejection with numbers.

## Acceptance

```bash
CARGO_INCREMENTAL=0 cargo build --bin mmli_lab
CARGO_INCREMENTAL=0 cargo clippy --all-targets -- -D warnings
test -f tasks/MMLI-2/results/lab.md
# Probe section present with int8 retention numbers
grep -q "int8" tasks/MMLI-2/results/lab.md
```

## Commit

If passing: `feat(lab): qualify int8 matrix payload, approve for Phase 6`.
If failing: `lab(mmli): reject int8 matrix payload, f16 confirmed`.
For the authorized ranking follow-up:
`lab(mmli): record int8 ranking evidence`.
≤ 70 chars/line, imperative.
