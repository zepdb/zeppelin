# LateLatency — late-interaction query latency reduction

Created 2026-08-01. Owner: Anup. Orchestration: Claude writes phase
files and validates results; **Anup hands codex ONE phase file at a
time**; codex executes and STOPS. Nothing here is autonomous.

## The problem

Dense IVF search answers in ~20 ms. Late-interaction (MMLI-2 flat-SQ8
text lane, K=1000) answers in **463 ms p50** end-to-end (bench) /
**596 ms p50** (full harness). Verified breakdown from the pinned run
(`tasks/MMLI-2/results/phase9-flat-sq8-benchmark.json`, loopback
MinIO):

| Component | p50 |
| --- | ---: |
| query FDE encode | ~1.3 ms (mean) |
| resident SQ8 candidate scan (5,183 × 10,240) | 40 ms |
| **truth wave (fetch + decode + MaxSim, lumped)** | **423 ms** |
| end-to-end | 463 ms |

Truth wave cost shape: 2,000 logical ranges/query (matrix + attribute
per candidate) coalesced ~2.7× → **730 physical GETs**, 58.34 MB
planned bytes (mean).

## The diagnosis (measured, do not re-litigate)

**The truth wave is round-trip-bound, not byte-bound.**

- `execute_read_plan` runs `buffer_unordered(8)`
  (`src/storage/read_plan.rs:437`; `DEFAULT_MAX_CONCURRENT_REQUESTS
  = 8` at line 24). 730 ÷ 8 ≈ 91 serial waves; 423 ms ÷ 91 ≈ 4.6
  ms/wave ≈ per-GET loopback latency.
- Pure bandwidth floor would be 58 MB ÷ 410 MB/s ≈ 142 ms. We are 3×
  above it.
- The INT8-G32 confirm run proved the scaling: −38.4% bytes
  (35.95 MB) bought only −11% p50 (530 vs 596 ms full-harness),
  tracking the request ratio (622/730 = 0.85), not the byte ratio.
- `execute_read_plan` `try_collect`s ALL bytes before
  `score_truth_wave` runs (`read_plan.rs:438`,
  `segment_search.rs:237-240`) — fetch and decode+MaxSim are fully
  serialized, and the decode/score share of the 423 ms is
  uninstrumented.

Cost model to fit and carry through every phase:

```
T_truth ≈ ceil(R / C) · t_req  +  bytes / BW  +  T_decode + T_maxsim
```

R = physical requests, C = read concurrency. A phase result is the
model's parameters, not just a number — a knob that only wins at
SciFact density is a local minimum, not a finding.

Note: the visual lane has a DIFFERENT profile (154 requests,
75.77 MB, p50 694 ms → byte/CPU-bound, not request-bound). Sweeps run
on the text lane; visual is re-checked at productization.

## Phase ladder (order of levers)

| Phase | Lever | File |
| --- | --- | --- |
| 01 | Instrument the truth wave (fetch/decode/MaxSim split, per-request latency, model fit) | `01-instrument-truth-wave.md` |
| 02 | Read concurrency sweep (8→128; default is hardcoded 8) | `02-concurrency-sweep.md` |
| 03 | Request-count reduction: gap-budget / max-request sweep | `03-gap-budget-sweep.md` |
| 04 | Pipeline fetch with decode+MaxSim (kill the collect barrier) | `04-pipeline-fetch-score.md` |
| 05 | Shape check: ~50k-unit heavy-tailed synthetic corpus | `05-shape-check.md` |
| 06 | Productize defaults + full-harness acceptance (text + visual) | `06-productize-defaults.md` |
| 07 | Truth-wave CPU levers: parallel scoring + SIMD decode (from research report) | `07-cpu-levers.md` |
| 09 | RESEARCH: resident SQ8 scan parallelization (commit only on win) | `09-resident-scan-parallel.md` |
| 10 | RESEARCH: candidate pruning K sweep + recall curve (measurement only, no adoption) | `10-candidate-pruning.md` |

Fenced — designs only, NOT scheduled, blocked on workload evidence
(cannot be decided on SciFact):

- Candidate pruning (resident coarse MaxSim / truncated truth to cut
  K=1000 → ~300 truth fetches). Needs a recall gate on a
  representative workload.
- Cross-query truth-block caching (memoization track already has the
  deferred item).
- INT8 G32 revisit: only worth re-pricing after phases 02–04 push
  latency toward the bandwidth floor, where bytes finally matter;
  needs K≈1100–1200 or an INT8-specific gate (missed 0.975 at
  K=1000).
- Decode+MaxSim CPU optimization (phase 01 found score p50 353.9 ms =
  85% of the truth wave on loopback: decode 176 ms + MaxSim 178 ms,
  MaxSim tail drives p95) — research report
  `decode-maxsim-optimization-research.md` prices the levers; drafted
  as `07-cpu-levers.md`, awaiting Anup go.
- INT8-G32 default: PARKED 2026-08-01 (Anup). f16 stays default.
  Re-run the f16-vs-INT8 comparison AFTER the CPU levers + phases
  02–04 land, when decode is no longer the payer — the honest
  comparison is bytes/requests vs 60 hits of recall at that point.
- Interleave matrix+attribute into one range per candidate (phase 03
  observation: every query starts as exactly 2,000 logical ranges =
  2 per candidate; a layout fold would halve the request floor before
  any coalescing) — storage-format change, coordinates with
  tasks/new-format.md ZBP5 attrs-fold; design only.
- Resident SQ8 scan scaling: at 50k units the scan is ~391–414 ms
  and dominates e2e (phase 05) — scan parallelization/pruning is the
  next big payer at design-scope scale; needs its own phase + plan.

## Protocol (the pause is the point)

1. Anup gives codex exactly one phase file.
2. Codex executes it, writes results to `tasks/LateLatency/results/`,
   commits code on the phase branch, and **stops**. Every phase file
   ends with a hard stop; proceeding to the next lever is a protocol
   violation.
3. Claude validates the results against the phase's acceptance
   criteria, updates the ledger below, and reports to Anup.
4. Anup decides: advance / rerun / kill the lever. Only then does the
   next file go out.

Dataset caveat (standing): the inner-loop corpus is the pinned SciFact
text replay (5,183 docs / 1,109 queries) — cheap and digest-verified,
but its 19%-of-corpus-per-query candidate density inflates coalescing
(the 2.7× factor is a corpus-size artifact). Phases 01–04 measure
levers that are corpus-robust (concurrency, pipelining) or explicitly
model-parameterized (gap budget). **No production default changes
until phase 05 confirms out-of-regime and phase 06 runs the gates.**

## Ledger

| Phase | Status | Result | Decision |
| --- | --- | --- | --- |
| 01 | COMPLETE 2026-08-01 (6ff4df0 on ll-01-instrument) | Diagnosis overturned on loopback: fetch p50 62.7 ms, score p50 353.9 ms (decode 176.0 + MaxSim 177.6); per-GET 0.61 ms not 4.6; recall exact 10,869/11,090, tails within pins, observed==planned GETs | Anup 2026-08-01: split tracks — 02–04 in worktree, INT8 rerun in main thread |
| 01b | COMPLETE 2026-08-01 (validated; commit pending on ll-01b-int8-split) | e2e p50 455→387 ms (−15%); truth wave −69 ms: decode 176.0→116.5 (−33.8%), fetch 62.7→52.2 (−16.7%, tracks waves −15%), MaxSim unchanged (+0.7%); bytes −38.4%, GETs 730→622; recall exactly 10,809/11,090 (accepted baseline), tails within pins, observed==planned; INT8 per-byte decode slightly WORSE (307 vs 330 MB/s — dequant overhead), win is fewer bytes | Anup 2026-08-01: PARKED — f16 stays default; re-compare f16 vs INT8 after all other optimizations land |
| 02 | COMPLETE 2026-08-01 (0e3c24e on ll-02-concurrency, validated) | Knee at C=16: fetch p50 62.7→45.4 ms (−27.5%), e2e 455→436 ms (−4.2%); t_req rises with C (0.888→6.54 ms by C=128) — MinIO request-stack saturation floor ~43.6 ms at C=32–64, NOT the 410 MB/s wall (observed 1.33 GB/s); C=128 regresses; recommended prod value 16 (no default changed); all pins/parity pass | Auto-advanced to 03 per Anup's standing go |
| 03 | COMPLETE 2026-08-01 (3581a33 on ll-03-gap-budget, validated) | Knee at 256 KiB gap / 8 MiB max-request: GETs 730→461 (−37%), fetch 45.4→35.3 ms (−22%), e2e 436→427 ms, at +64% physical bytes (39.5 MiB/query gap waste); 1 MiB+ regresses (byte-dominated); 16 MiB cap identical plan (cap not binding); all pins/parity pass; CORPUS-SPECIFIC — knee position depends on SciFact's 19%-density, phase 05 must re-find it | Auto-advanced to 04 per Anup's standing go |
| 04 | COMPLETE 2026-08-01 (dc73ad7 on ll-04-pipeline, validated) | Streamed read plan + one spawn_blocking scorer (bench-gated, prod path unchanged): full confirm truth p50 389→363 ms, e2e 427→404 ms (−23 ms, 74% of the 35.3 ms overlap bound; residual is CPU contention +7.7 ms score); ordered top-10 identical old-vs-piped on all queries; pins exact; 762 lib tests pass; keep for phase 06 | Auto-advanced; worktree track 02–04 done — net e2e 483→404 ms (−16%) |
| 05 | COMPLETE 2026-08-01 (7849118 on ll-05-shape-check, validated) | 50k heavy-tail corpus: C16 TRANSFERS (fetch −19%, truth −18% at identical plan); 256 KiB gap PARTIAL (only −7.4% GETs at +83% bytes — SciFact knee is corpus-specific, as fenced); 8 MiB cap PARTIAL (not isolated, no sheds); GET drift 730→965 baseline / 461→894 recommended (coalescing holds at 2.07–2.24× via compact attrs); NEW: resident SQ8 scan ~391–414 ms dominates e2e at 50k scale | pending Anup: phase 06 Chosen values |
| 06 | RESULTS VALIDATED 2026-08-01 (commit pending on ll-06-productize) | Full-harness TEXT: p50 596→241 ms (−59.6%), p95 883→254 (−71.2%); all gates exact; C16 default + streamed truth wave now the sole production path; VISUAL REPLAY SKIPPED (pinned tensors absent — non-regression UNVERIFIED); 764 lib + 44 integration tests green; dense isolation empty | pending Anup: review + merge; visual replay owed when tensors restored |
| 07 | COMPLETE 2026-08-01 (06ce9be + d5f5140 on ll-07-cpu-levers, validated) | Lever 1 (parallel scoring, 16 workers): score wall 358→35.7 ms (10×), e2e 461→140 ms, bit-identical. Lever 2 (slice f16 decode + drop revalidation, half crate restored w/ Claude approval): e2e 140→136 ms only — residual decode is SHA-256 + attribute work, not widening. Cumulative e2e 461→136 ms (3.39×) at DEFAULT knobs (C8/64KiB/no pipeline); pins exact in all 3 arms; tails p95 614→150 ms | Anup 2026-08-01: combined confirm run, then phase 05 |
| 08 | COMPLETE 2026-08-01 (441600c + 53e4b44 on ll-08-combined, validated) | e2e p50 102 ms (truth 59, fetch 57.3, score wall 57.7 overlapped; SQ8 scan 42 now co-payer); 4.5× vs 455/461 baseline; 461 GETs / 95.7 MB at the knee; pins exact, obs==planned, bit-identity + module tests pass; 24 ms over ideal composition = worker/fetch contention, collect barrier stays dead (1.3 ms residual) | Anup: cherry-pick to main, then phase 05 |
| 09 | COMPLETE 2026-08-05 (`4f5ac17`, `b29de38`, `517f475`; scope extended with approved INT8 recomposition) | Exact parallel flat scan: 42→4 ms p50 (10.5x), production-profile e2e 109→72 ms with identical top-10. INT8-G32 curve: K1000 48 ms / 0.974662 recall; K1100 51 ms / 0.977818; K1200 53 ms / 0.980703, exceeding f16 K1000 quality 0.980072. All full runs observed==planned; tails stayed within/improved on pins. | ACCEPT scan. Quality-preserving recommendation: INT8 K1200 plus qualified dense read profile; speed recommendation: K1000 if the approved 0.54 percentage-point recall cost is acceptable. General dtype/gap defaults unchanged because dtype is a one-way epoch choice and large gaps did not transfer to sparse 50k shape. |
| 10 | DRAFTED 2026-08-01 (research, non-binding: measurement only, no adoption) | — | awaiting Anup go |

Baseline pins (all changes must preserve unless a phase says
otherwise): recall 10,869/11,090 = 0.980072 exactly at K=1000 (f16);
tails min 6/10, ≤12 queries <8/10, 0 <5/10; observed GETs == planned
on every query; zero per-query candidate-wave reads.
