# IVF-Flat recall gate: RED baseline

Date: 2026-07-09

## Command

```text
ZEPPELIN_RECALL_GATE_DATA=$HOME/Documents/code/zeppelin-devbench/data \
  cargo test --release --test ivf_recall_gate -- --ignored --nocapture
```

## Result

The production partition seam is deterministic and satisfies every guard
except the binding recall target.

```text
wikidpr1m    clusters=256  nprobe=16   recall@10=0.939300 recall@100=0.916630 scan=0.12435 storage=1.00000 full=1.000000
wikidpr100k  clusters=256  nprobe=16   recall@10=0.897000 recall@100=0.853620 scan=0.11851 storage=1.00000 full=1.000000
wikidpr2m    clusters=256  nprobe=16   recall@10=0.944400 recall@100=0.927760 scan=0.12701 storage=1.00000 full=0.999990

thread 'ivf_recall_gate' panicked at tests/ivf_recall_gate.rs:479:5:
wikidpr1m recall@100 0.916630 is below 0.960000

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured;
0 filtered out; finished in 1169.72s
```

## Guard assessment

| Guard | 1M | 2M | Threshold | Result |
|---|---:|---:|---:|---|
| Recall@100 | 0.916630 | 0.927760 | >= 0.960000 | RED |
| Scan fraction | 0.12435 | 0.12701 | <= 0.20000 | PASS |
| Storage amplification | 1.00000 | 1.00000 | <= 1.50000 | PASS |
| Full-probe recall | 1.000000 | 0.999990 | >= 0.999000 | PASS |
| Assignment determinism | stable | stable | identical hashes | PASS |

The 100k prefix is diagnostic only. It is not used for the binding recall
decision.
