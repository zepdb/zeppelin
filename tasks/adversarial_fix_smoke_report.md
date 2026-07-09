# Adversarial Fix Smoke Report

## Mixed smoke

Command:

```bash
TEST_BACKEND=minio \
ZEPPELIN_ADVERSARIAL_SECONDS=180 \
ZEPPELIN_ADVERSARIAL_MODE=mixed \
ZEPPELIN_ADVERSARIAL_PRESERVE=never \
ZEPPELIN_ADVERSARIAL_ARTIFACTS=target/adversarial/fix-smoke \
cargo test --test adversarial_workload_tests smoke -- --ignored --nocapture
```

Result: passed.

Artifact report:
`target/adversarial/fix-smoke/run-1783573979/report.md`

Smoke summary:

| seed | mode | status | ops | explicit compactions | background compactions |
| --- | --- | --- | ---: | ---: | ---: |
| 0 | deterministic | passed | 525 | 37 | 0 |
| 1 | chaos | passed | 525 | 5 | 10 |
| 2 | deterministic | passed | 515 | 48 | 0 |

Total: 3 seeds, 1565 ops, 0 failed seeds, 90 explicit
compactions, 10 background compactions.

No violations were recorded. This confirms zero I7FtsMembership and
zero I12StructuralSanity failures in the mixed smoke, no artifact-capture
panic, and visible background compaction activity for the chaos seed.

## Replays

I1StrongExact seeds replay clean on the current code:

| seed | max ops | replay result |
| --- | ---: | --- |
| 8589934592 | 14 | clean |
| 8589934593 | 26 | clean |
| 47244640257 | 14 | clean |

I6PaginationEquivalent report seeds replay clean on the current code:

`2`, `4294967297`, `4294967298`, `12884901890`, `17179869185`,
`17179869186`, `21474836481`, `25769803778`, `30064771073`,
`30064771074`, `34359738369`, `42949672961`, `47244640256`,
`51539607552`, `55834574849`.

## Remaining violations

No remaining adversarial violation class appeared in the mixed smoke.
No new repro command is needed.
