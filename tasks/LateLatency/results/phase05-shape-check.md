# Phase 05 — 50k heavy-tail shape check

Status: **PASS**. Each prescribed arm ran once for 300 queries on
`ll-05-shape-check` from `fa0c18f`. No product default changed.

## Pre-build sizing gate

The estimate was written to this report before the first release build. The
requested lognormal shape uses median 120 and p99 1,500 tokens. Solving
`sigma = ln(1500 / 120) / 2.326347874` gives `sigma = 1.085705484` and an
uncapped expected mean of 216.344 tokens per unit. At 50,000 units and 128
f16 coordinates per token:

```text
expected vectors             = 50,000 * 216.344045 = 10,817,202
truth matrix payload         = 10,817,202 * 128 * 2 = 2,769,203,777 B
flat SQ8 codes               = 50,000 * 10,240     =   512,000,000 B
estimated total artifacts    ~= 3,314,718,112 B = 3.0871 GiB
```

The deterministic count sample contained 10,870,324 vectors. Before matrix
allocation, the harness conservatively estimated 3,358,958,668 artifact
bytes. Actual truth-plus-attribute blocks were 2,794,710,092 B, the flat SQ8
artifact was 532,518,855 B, and total upload was 3,327,228,947 B (3.0987
GiB). The conservative 8 GiB disk/MinIO allowance was below the 30 GiB stop
threshold.

The pre-run runtime estimate was five minutes per arm with a conservative
fifteen-minute bound. Actual elapsed times were 219.485 s, 212.354 s, and
220.913 s, below the one-hour stop threshold.

## Deterministic workload

The sampled document-token shape was min 1, p50 120, p95 719, p99 1,507,
max 9,545, and mean 217.40648. Queries used 20 random f16 token vectors at
the same 128-dimensional text-lane shape. Every query selected K=1,000.

The generator printed these namespace-independent values in every arm:

```text
document seed   0x4c4c3035444f4353
document SHA256 9386be37bef7fab99d07e46d6bb76e55999e4afe298e6d9bfb5f70080be7b6ea
query seed      0x4c4c303551525953
query SHA256    e0bebe63735b9cbdd7ddfeed3792df2e6aa30cd1243d7ce37eabf7a0a9cd838c
SQ8 code SHA256 33149f4480728b4296e97bd0f0beb900b8bd169386baa403823d6f4e1502e2f6
SQ8 cal SHA256  c284b7d96abde3354e05e68cb2d82f11a2e3f012e4bb232b4707a41feb426e23
```

Both seeds and all four digests matched across all three independent
generations. The whole
flat-artifact hash is intentionally not a determinism pin because its
locator keys contain the per-run UUID namespace.

## Pipeline choice

All arms ran with `MMLI_FLAT_BENCH_PIPELINE` unset. This keeps fetch and
parallel scoring serialized, avoiding the known fetch/scoring contention
from obscuring the request-shape comparison. The integrated scorer still
used 16 workers in every arm.

## Measurements

Requests, coalescing, and timings are p50. Byte columns are per-query means,
matching the Phase 03 convention.

| Arm | C | Gap | Physical GETs | Coalescing | Logical B | Planned B | Gap-waste B | Fetch ms | Truth ms | E2E ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| a — baseline | 8 | 64 KiB | 965 | 2.072539x | 10,827,235.2 | 12,355,437.2 | 1,528,202.0 | 82.189 | 91 | 482 |
| b — recommended | 16 | 256 KiB | 894 | 2.237136x | 10,827,235.2 | 22,630,495.9 | 11,803,260.7 | 55.910 | 64 | 458 |
| c — concurrency only | 16 | 64 KiB | 965 | 2.072539x | 10,827,235.2 | 12,355,437.2 | 1,528,202.0 | 66.531 | 75 | 492 |

Every query began with 2,000 logical ranges. The byte identity
`planned = logical + gap waste` holds in every arm. Observed GET counts equal
the plan for all 900 queries: totals were 289,544, 267,860, and 289,544 for
arms a, b, and c.

The selected random frontiers averaged about 42.24 matrix tokens per unit,
well below the 217.41-token corpus mean. That explains the 10.83 MB logical
truth payer and is a synthetic-frontier shape finding, not a semantic metric.

## Cost-model check with the new request count

The carried native-MinIO form is:

```text
T_truth ~= ceil(R / C) * t_req + T_score + T_driver
```

The 410 MB/s byte term is not added: Phase 01 established that per-GET wall
time already contains transfer on this backend, so adding it would count the
same bytes twice. The closest prior arm supplies `t_req` and the fitted fetch
residual. The carried truth prediction adds the prior 32.581250 ms parallel
score wall and 0.899958 ms truth-driver residual. Errors below are prediction
minus measurement.

| Arm | Waves | Raw fetch pred | Fitted fetch pred | Fetch measured | Fetch error | Fitted truth pred | Truth measured | Truth error |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| a | 121 | 72.560 | 78.909 | 82.189 | -3.280 (-3.99%) | 112.390 | 91 | +21.390 (+23.51%) |
| b | 56 | 60.174 | 64.170 | 55.910 | +8.260 (+14.77%) | 97.651 | 64 | +33.651 (+52.58%) |
| c | 61 | 54.509 | 59.446 | 66.531 | -7.086 (-10.65%) | 92.927 | 75 | +17.927 (+23.90%) |

The carried fetch fit remains within 14.8% after substituting the new R. The
truth fit overpredicts because its prior 32.581250 ms score payer measured
only 7.830333–8.006209 ms on these shorter selected frontiers.

The post-run wave fit remains structurally useful:

| Arm | In-arm per-GET p50 | `waves * t_req` | Fetch p50 | Residual |
| --- | ---: | ---: | ---: | ---: |
| a | 0.609958 | 73.804918 | 82.188958 | 8.384040 |
| b | 0.860042 | 48.162352 | 55.909833 | 7.747481 |
| c | 0.929041 | 56.671501 | 66.531375 | 9.859874 |

## Lever isolation

At the identical 64 KiB physical plan, a to c changes only concurrency.
Waves fall 121 to 61; per-GET p50 rises 52.31%, but fetch falls 15.658 ms
(19.05%) and truth falls 16 ms (17.58%). Raw E2E rises 10 ms because the
independent resident scan rises from 391 to 414 ms in that one run.

At C16, c to b isolates the gap budget. The 256 KiB budget removes 71 median
GETs (7.36%) and improves coalescing 7.94%, fetch 15.96%, truth 14.67%, and
E2E 6.91%. It does so by increasing mean planned bytes 83.16% and mean gap
waste 7.72x. The latency direction transfers, but the SciFact request/byte
knee does not transfer at the same efficiency.

## Execution and cleanup

The release build and each timed arm acquired
`/private/tmp/zeppelin-bench-lock` with the required mkdir loop and released
it afterward. Every Cargo invocation used `CARGO_INCREMENTAL=0`; each timed
test passed in release mode without a shed event. The final lock is absent.

Each timed UUID namespace was removed by the harness through
`ZeppelinStore::delete_prefix`, including explicit removed-object count
validation. Read-only listing confirmed all three run-owned prefixes absent.
Three bench prefixes from earlier in the day predated this task and were left
untouched.

`cargo fmt --all -- --check` and strict library clippy are clean. All-target
clippy is clean with only `clippy::needless-range-loop` allowed for the two
documented pre-existing warnings in the protected routing diagnostic. That
file, production defaults, `stash@{0}`, `.pi-subagents/`, and
`target-worktree/` are unchanged.

## Structural urgency

Request drift is material: the baseline shape moves from 730 GETs on SciFact
to 965 here (+32.19%), while the recommended shape moves from 461 to 894
(+93.93%). It does not approach 2,000 because the compact attribute artifact
still coalesces strongly; observed factors remain 2.07–2.24x. Attribute-fold
and candidate-pruning work are therefore more urgent than SciFact alone
suggested, because they remove logical work instead of buying a small GET
reduction with gap bytes, but less urgent than the near-2,000 forecast implied.

## Verdicts

| Knob | Verdict | Why |
| --- | --- | --- |
| Concurrency 16 | **TRANSFERS** | At identical request and byte shape, it halves waves and lowers fetch p50 19.05% and truth p50 17.58%; the one-run E2E reversal accompanies an unrelated 23 ms resident-scan increase. |
| Gap budget 256 KiB | **PARTIAL** | The latency direction transfers, but it removes only 7.36% of requests while increasing mean planned bytes 83.16%, so the SciFact knee does not transfer at the same efficiency. |
| Maximum request 8 MiB | **PARTIAL** | All arms complete without a shed event at this cap, but it is held constant and is not independently isolated by the prescribed arms. |
