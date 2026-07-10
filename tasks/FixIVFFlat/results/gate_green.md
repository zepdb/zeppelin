# IVF-Flat recall gate: GREEN

Date: 2026-07-10

## Binding pinned-dataset gate

```text
ZEPPELIN_RECALL_GATE_DATA=$HOME/Documents/code/zeppelin-devbench/data \
  cargo test --release --test ivf_recall_gate -- --ignored --nocapture
```

```text
wikidpr1m    clusters=334  nprobe=63   recall@10=0.979900 recall@100=0.968820 scan=0.18603 storage=1.00000 full=1.000000
wikidpr100k  clusters=256  nprobe=48   recall@10=0.961800 recall@100=0.933260 scan=0.18272 storage=1.00000 full=1.000000
wikidpr2m    clusters=667  nprobe=126  recall@10=0.988200 recall@100=0.981440 scan=0.18226 storage=1.00000 full=0.999990

test result: ok. 1 passed; 0 failed; finished in 2045.59s
```

The 100k prefix is diagnostic only. The 1M and 2M rows are binding.

| Guard | 1M | 2M | Threshold | Result |
|---|---:|---:|---:|---|
| Recall@100 | 0.968820 | 0.981440 | >= 0.960000 | PASS |
| Scan fraction | 0.18603 | 0.18226 | <= 0.20000 | PASS |
| Storage amplification | 1.00000 | 1.00000 | <= 1.50000 | PASS |
| Full-probe recall | 1.000000 | 0.999990 | >= 0.999000 | PASS |
| Assignment determinism | stable | stable | identical hashes | PASS |

## MinIO production-path sentinel

The sibling runner needed three mechanical compatibility updates in a
detached temporary worktree because its checked-in API calls predated the
current compactor, query, and storage signatures. The benchmark still path
depended on this Zeppelin checkout. The source sibling remained unchanged.

```text
TEST_BACKEND=minio cargo run --release -- \
  --dataset dbpedia100k --nprobe 48,128 \
  --data-dir $HOME/Documents/code/zeppelin-devbench/data --json
```

Production compaction built 256 SQ8 clusters in 158.52 seconds. All 1,000
queries ran for both probe counts and cleanup deleted all 857 namespace
objects.

| Metric | Default policy, nprobe 48 | No-pruning sentinel, nprobe 128 |
|---|---:|---:|
| Recall@10 | 0.98960 | 0.99750 |
| Recall@100 | 0.98184 | 0.99703 |
| Mean ms/query | 277.05 | 444.50 |
| Mean GETs/query | 60.975 | 110.908 |
| Mean cluster GETs/query | 58.975 | 108.908 |
| Mean GET bytes/query | 82,542,431 | 128,808,293 |

The production-path default recall requirement of at least 0.95 passes.
The raw nprobe-48 versus nprobe-128 recall delta is 0.01519, above the old
plan's 0.01 cross-probe comparison. That delta is not sketch loss: the
adaptive budget is a structural no-op at nprobe 48, and the unit contract
pins full retention at 32, 48, 63, and 126 probes. The sentinel's additional
recall comes from its wider 128-cluster coarse frontier. No sketch constant or
the authorized 3/16 probe fraction was changed silently.

The default policy used 45.0% fewer total GETs and 35.9% fewer GET bytes than
the 128-probe sentinel. Its absolute GET count is higher than the historical
np16 estimate in the original plan; the measured trade-off is recorded here
instead of being hidden by an unmeasured tuning change.

## Focused validation

```text
cargo test --release --lib adaptive_sketch_cap_scales_monotonically
test result: ok. 1 passed; 0 failed

cargo test --release --lib
test result: ok. 360 passed; 0 failed

cargo check --release --tests
Finished release profile successfully

cargo clippy --release --all-targets -- -D warnings
Finished release profile successfully

cargo fmt --all -- --check
PASS
```

Additional required checks:

```text
cargo test --release --test proptest_ivf_recall
test result: ok. 3 passed; 0 failed

TEST_BACKEND=minio cargo test --release \
  --test adversarial_workload_tests smoke -- --ignored --nocapture
adversarial smoke: seeds=3 ops=1565 compactions=90 failed=0

```

## Full-suite audit

Plain debug `cargo test` could not reach execution on this host. Repeated
attempts left multiple `rustc` metadata processes asleep with zero CPU-time
growth, including a retry with incremental compilation and dev/test debuginfo
disabled. The release compiler path completed and ran the complete test
inventory, but three unrelated existing tests fail deterministically under
release timing/transport behavior:

```text
test_envelope_body_too_large
  reqwest body write failed because the server reset the connection

test_list_prefix_rejects_recursive_root_listing
  test did not panic as expected

test_group_commit_coalesces_manifest_puts
  expected <= 8 manifest CAS PUTs, observed 20
```

Each failure reproduced in isolation. A release run skipping the first two
continued through every later test binary; all remaining tests passed until
the final group-commit timing assertion above. These files and behaviors are
outside the IVF task and were not changed to make the battery look green.
