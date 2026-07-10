# Phase 0 result: no feasible declared operating point

Date: 2026-07-09

Original status: **STOP before Phase A.** The full declared grid has no single policy
satisfying recall@100 >= 0.965, scored-row fraction <= 0.15, and spill
storage inflation <= 1.5 on both `wikidpr1m` and `wikidpr2m`. The 2M
scale-aware/balanced cell passes, but its corresponding 1M cell does not.
Per `top2_experiment.md` and `fix_ivf_flat.md`, production wiring did not
begin and no `[P0.*]` constants were selected.

Decision: Anup subsequently authorized dropping spill and revising G2 to
0.20. The selected no-spill constants are tau=off, probe fraction=3/16,
nprobe floor=32, target rows per cluster=3000, balance ratio=4.0, and eight
repair rounds. The measured 1M/2M selection rows are 0.968810/0.981420
recall at 0.18603/0.18227 scan with 1.00000 storage inflation.

## Reproduction

- Repository commit: `75271612523250f04cdb466a683137fcbc709ba7`
- Host: 16 cores, 128 GiB RAM; 12 harness threads
- Dataset: `wikidpr1m`, 1,000,000 x 768 corpus, 1,000 queries
- Corpus SHA-256:
  `c692326532ffd0fc58379ed980243f157d895b0ad775968694cee337bc594116`
- Query SHA-256:
  `25fb72faf529d989c87fbc1dc7b076c9bd64590a72177854509277d5b7017d4b`
- Ground-truth SHA-256:
  `8d3067e921478c74b48c415d2b31574ae0ca0e3dbfb2edab7874e3f995f134e7`
- Dataset: `wikidpr2m`, 2,000,000 x 768 corpus, 1,000 queries
- Corpus SHA-256:
  `ecd281d4f2e255e48c68e6a8af0670faa4d07d39988880609b8b56654bff20b8`
- Query SHA-256:
  `25fb72faf529d989c87fbc1dc7b076c9bd64590a72177854509277d5b7017d4b`
- Ground-truth SHA-256:
  `a5e7399789cc53de9cac755a33ecc6245f648761cf2a3b82f3f0c2124ad47739`

The harness was copied to `/private/tmp/zeppelin-p0`, where the planned
mini-batch budget (`max(1024, 32*k)`), deterministic balance repair, spill,
dedup, and scored-row accounting were implemented without modifying main's
production `src/` tree.

Representative commands (balance repair was run both off and at 4x/8
rounds):

```bash
/private/tmp/zeppelin-p0/target/release/ivf_diag \
  --data-dir=$HOME/Documents/code/zeppelin-devbench/data/wikidpr1m \
  --rows=1000000 --nlist=256 --iters=25 --threads=12 \
  --gt-cache-dir=/private/tmp/zeppelin-p0 \
  --evals='l2:cosine:16,24,32,48' \
  --spill-ratio-sq=0,1.2,1.44,1.7,2.0 \
  --balance-max-ratio=4 --balance-rounds=8 \
  --batch-scale=true --label=p0-1m-k256-bal4

/private/tmp/zeppelin-p0/target/release/ivf_diag \
  --data-dir=$HOME/Documents/code/zeppelin-devbench/data/wikidpr1m \
  --rows=1000000 --nlist=334 --iters=25 --threads=12 \
  --gt-cache-dir=/private/tmp/zeppelin-p0 \
  --evals='l2:cosine:16,24,32,42,48,63' \
  --spill-ratio-sq=0,1.2,1.44,1.7,2.0 \
  --balance-max-ratio=4 --balance-rounds=8 \
  --batch-scale=true --label=p0-1m-k334-bal4

/private/tmp/zeppelin-p0/target/release/ivf_diag \
  --data-dir=$HOME/Documents/code/zeppelin-devbench/data/wikidpr2m \
  --rows=2000000 --nlist=667 --iters=25 --threads=12 \
  --gt-cache-dir=/private/tmp/zeppelin-p0 \
  --evals='l2:cosine:16,24,32,42,48,84,126' \
  --spill-ratio-sq=0,1.2,1.44,1.7,2.0 \
  --balance-max-ratio=4 --balance-rounds=8 \
  --batch-scale=true --label=p0-2m-k667-bal4
```

The extra scale-aware probes (42/63 at 1M and 42/84/126 at 2M) are the
exact values implied by the declared 1/16, 1/8, and 3/16 probe fractions.
They characterize declared policy values; they are not an additional
threshold search.

An unmodified reference-harness replay at the same commit reproduced the
historical RED baseline before the extended cells were trusted:

```text
k=256, nprobe=16, recall@100=0.916630,
scan_fraction=0.12435, occupancy_p50=2751, occupancy_max=19871
```

The earlier recorded value was 0.9166; the difference is only displayed
precision.

## Full binding 1M table

Balance-off and balance-4x results are identical: scaled mini-batches leave
maximum occupancy below 4x mean, so repair converges after zero split rounds.
The table therefore represents both balance settings. Scan includes spill
duplicates; storage is physical rows divided by logical rows.

| nlist | tau squared | nprobe | recall@100 | scan fraction | storage |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 0 | 16 | 0.876960 | 0.05980 | 1.00000 |
| 256 | 0 | 24 | 0.916210 | 0.08978 | 1.00000 |
| 256 | 0 | 32 | 0.938860 | 0.11961 | 1.00000 |
| 256 | 0 | 48 | 0.963570 | 0.17973 | 1.00000 |
| 256 | 1.2 | 16 | 0.942170 | 0.11998 | 1.90769 |
| 256 | 1.2 | 24 | 0.964450 | 0.18025 | 1.90769 |
| 256 | 1.2 | 32 | 0.976730 | 0.24030 | 1.90769 |
| 256 | 1.2 | 48 | 0.987900 | 0.36013 | 1.90769 |
| 256 | 1.44 | 16 | 0.943250 | 0.12458 | 1.98916 |
| 256 | 1.44 | 24 | 0.965330 | 0.18700 | 1.98916 |
| 256 | 1.44 | 32 | 0.977510 | 0.24914 | 1.98916 |
| 256 | 1.44 | 48 | 0.988370 | 0.37334 | 1.98916 |
| 256 | 1.7 | 16 | 0.943350 | 0.12467 | 1.99227 |
| 256 | 1.7 | 24 | 0.965430 | 0.18712 | 1.99227 |
| 256 | 1.7 | 32 | 0.977610 | 0.24930 | 1.99227 |
| 256 | 1.7 | 48 | 0.988420 | 0.37356 | 1.99227 |
| 256 | 2.0 | 16 | 0.943370 | 0.12470 | 1.99426 |
| 256 | 2.0 | 24 | 0.965450 | 0.18716 | 1.99426 |
| 256 | 2.0 | 32 | 0.977630 | 0.24934 | 1.99426 |
| 256 | 2.0 | 48 | 0.988440 | 0.37361 | 1.99426 |
| 334 | 0 | 16 | 0.861320 | 0.04780 | 1.00000 |
| 334 | 0 | 24 | 0.902710 | 0.07132 | 1.00000 |
| 334 | 0 | 32 | 0.927170 | 0.09503 | 1.00000 |
| 334 | 0 | 42 | 0.946550 | 0.12441 | 1.00000 |
| 334 | 0 | 48 | 0.954400 | 0.14192 | 1.00000 |
| 334 | 0 | 63 | 0.968810 | 0.18603 | 1.00000 |
| 334 | 1.2 | 16 | 0.933980 | 0.09561 | 1.90317 |
| 334 | 1.2 | 24 | 0.958410 | 0.14284 | 1.90317 |
| 334 | 1.2 | 32 | 0.971090 | 0.19033 | 1.90317 |
| 334 | 1.2 | 42 | 0.980500 | 0.24918 | 1.90317 |
| 334 | 1.2 | 48 | 0.984150 | 0.28410 | 1.90317 |
| 334 | 1.2 | 63 | 0.990410 | 0.37171 | 1.90317 |
| 334 | 1.44 | 16 | 0.935430 | 0.09956 | 1.98988 |
| 334 | 1.44 | 24 | 0.959590 | 0.14856 | 1.98988 |
| 334 | 1.44 | 32 | 0.972030 | 0.19785 | 1.98988 |
| 334 | 1.44 | 42 | 0.981230 | 0.25890 | 1.98988 |
| 334 | 1.44 | 48 | 0.984820 | 0.29517 | 1.98988 |
| 334 | 1.44 | 63 | 0.990960 | 0.38619 | 1.98988 |
| 334 | 1.7 | 16 | 0.935460 | 0.09966 | 1.99293 |
| 334 | 1.7 | 24 | 0.959610 | 0.14870 | 1.99293 |
| 334 | 1.7 | 32 | 0.972040 | 0.19802 | 1.99293 |
| 334 | 1.7 | 42 | 0.981250 | 0.25912 | 1.99293 |
| 334 | 1.7 | 48 | 0.984840 | 0.29541 | 1.99293 |
| 334 | 1.7 | 63 | 0.990980 | 0.38649 | 1.99293 |
| 334 | 2.0 | 16 | 0.935460 | 0.09968 | 1.99445 |
| 334 | 2.0 | 24 | 0.959610 | 0.14873 | 1.99445 |
| 334 | 2.0 | 32 | 0.972060 | 0.19806 | 1.99445 |
| 334 | 2.0 | 42 | 0.981250 | 0.25917 | 1.99445 |
| 334 | 2.0 | 48 | 0.984840 | 0.29547 | 1.99445 |
| 334 | 2.0 | 63 | 0.990980 | 0.38656 | 1.99445 |

## Full binding 2M table

At k=256, balance-off and balance-4x are identical because repair
converges before a split; the table labels that shared result
`off/4x`. At k=667, the balance cap required four repair rounds and
changed the partition, so both results are reported. The balanced k=667,
tau-off, nprobe-84 cell is the only 2M cell that passes all three Phase 0
selection constraints.

| nlist | balance | tau squared | nprobe | recall@100 | scan fraction | storage |
| ---: | :---: | ---: | ---: | ---: | ---: | ---: |
| 256 | off/4x | 0 | 16 | 0.891130 | 0.05993 | 1.00000 |
| 256 | off/4x | 0 | 24 | 0.925780 | 0.08962 | 1.00000 |
| 256 | off/4x | 0 | 32 | 0.946530 | 0.11939 | 1.00000 |
| 256 | off/4x | 0 | 48 | 0.968070 | 0.17878 | 1.00000 |
| 256 | off/4x | 1.2 | 16 | 0.950470 | 0.12016 | 1.88160 |
| 256 | off/4x | 1.2 | 24 | 0.969860 | 0.18006 | 1.88160 |
| 256 | off/4x | 1.2 | 32 | 0.979640 | 0.23978 | 1.88160 |
| 256 | off/4x | 1.2 | 48 | 0.989170 | 0.35861 | 1.88160 |
| 256 | off/4x | 1.44 | 16 | 0.951290 | 0.12468 | 1.96930 |
| 256 | off/4x | 1.44 | 24 | 0.970430 | 0.18662 | 1.96930 |
| 256 | off/4x | 1.44 | 32 | 0.980090 | 0.24835 | 1.96930 |
| 256 | off/4x | 1.44 | 48 | 0.989430 | 0.37123 | 1.96930 |
| 256 | off/4x | 1.7 | 16 | 0.951330 | 0.12484 | 1.98149 |
| 256 | off/4x | 1.7 | 24 | 0.970470 | 0.18682 | 1.98149 |
| 256 | off/4x | 1.7 | 32 | 0.980100 | 0.24861 | 1.98149 |
| 256 | off/4x | 1.7 | 48 | 0.989440 | 0.37159 | 1.98149 |
| 256 | off/4x | 2 | 16 | 0.951330 | 0.12490 | 1.99106 |
| 256 | off/4x | 2 | 24 | 0.970470 | 0.18690 | 1.99106 |
| 256 | off/4x | 2 | 32 | 0.980100 | 0.24870 | 1.99106 |
| 256 | off/4x | 2 | 48 | 0.989440 | 0.37171 | 1.99106 |
| 667 | off | 0 | 16 | 0.831610 | 0.02267 | 1.00000 |
| 667 | off | 0 | 24 | 0.875030 | 0.03378 | 1.00000 |
| 667 | off | 0 | 32 | 0.902340 | 0.04497 | 1.00000 |
| 667 | off | 0 | 42 | 0.924030 | 0.05898 | 1.00000 |
| 667 | off | 0 | 48 | 0.933050 | 0.06742 | 1.00000 |
| 667 | off | 0 | 84 | 0.963870 | 0.11791 | 1.00000 |
| 667 | off | 0 | 126 | 0.979670 | 0.17723 | 1.00000 |
| 667 | off | 1.2 | 16 | 0.909530 | 0.04541 | 1.88419 |
| 667 | off | 1.2 | 24 | 0.937700 | 0.06782 | 1.88419 |
| 667 | off | 1.2 | 32 | 0.954940 | 0.09042 | 1.88419 |
| 667 | off | 1.2 | 42 | 0.966600 | 0.11860 | 1.88419 |
| 667 | off | 1.2 | 48 | 0.971600 | 0.13562 | 1.88419 |
| 667 | off | 1.2 | 84 | 0.985760 | 0.23705 | 1.88419 |
| 667 | off | 1.2 | 126 | 0.992580 | 0.35510 | 1.88419 |
| 667 | off | 1.44 | 16 | 0.910750 | 0.04698 | 1.97037 |
| 667 | off | 1.44 | 24 | 0.938640 | 0.07011 | 1.97037 |
| 667 | off | 1.44 | 32 | 0.955590 | 0.09344 | 1.97037 |
| 667 | off | 1.44 | 42 | 0.967240 | 0.12249 | 1.97037 |
| 667 | off | 1.44 | 48 | 0.972110 | 0.14004 | 1.97037 |
| 667 | off | 1.44 | 84 | 0.986130 | 0.24463 | 1.97037 |
| 667 | off | 1.44 | 126 | 0.992750 | 0.36640 | 1.97037 |
| 667 | off | 1.7 | 16 | 0.910790 | 0.04705 | 1.98348 |
| 667 | off | 1.7 | 24 | 0.938650 | 0.07021 | 1.98348 |
| 667 | off | 1.7 | 32 | 0.955590 | 0.09356 | 1.98348 |
| 667 | off | 1.7 | 42 | 0.967240 | 0.12264 | 1.98348 |
| 667 | off | 1.7 | 48 | 0.972110 | 0.14020 | 1.98348 |
| 667 | off | 1.7 | 84 | 0.986130 | 0.24489 | 1.98348 |
| 667 | off | 1.7 | 126 | 0.992750 | 0.36676 | 1.98348 |
| 667 | off | 2 | 16 | 0.910800 | 0.04708 | 1.99185 |
| 667 | off | 2 | 24 | 0.938660 | 0.07024 | 1.99185 |
| 667 | off | 2 | 32 | 0.955600 | 0.09360 | 1.99185 |
| 667 | off | 2 | 42 | 0.967250 | 0.12269 | 1.99185 |
| 667 | off | 2 | 48 | 0.972120 | 0.14025 | 1.99185 |
| 667 | off | 2 | 84 | 0.986130 | 0.24496 | 1.99185 |
| 667 | off | 2 | 126 | 0.992750 | 0.36684 | 1.99185 |
| 667 | 4x | 0 | 16 | 0.839880 | 0.02308 | 1.00000 |
| 667 | 4x | 0 | 24 | 0.881970 | 0.03465 | 1.00000 |
| 667 | 4x | 0 | 32 | 0.908130 | 0.04617 | 1.00000 |
| 667 | 4x | 0 | 42 | 0.929330 | 0.06063 | 1.00000 |
| 667 | 4x | 0 | 48 | 0.938250 | 0.06928 | 1.00000 |
| 667 | 4x | 0 | 84 | 0.967880 | 0.12142 | 1.00000 |
| 667 | 4x | 0 | 126 | 0.981420 | 0.18227 | 1.00000 |
| 667 | 4x | 1.2 | 16 | 0.917430 | 0.04582 | 1.86287 |
| 667 | 4x | 1.2 | 24 | 0.944260 | 0.06884 | 1.86287 |
| 667 | 4x | 1.2 | 32 | 0.958630 | 0.09180 | 1.86287 |
| 667 | 4x | 1.2 | 42 | 0.969950 | 0.12059 | 1.86287 |
| 667 | 4x | 1.2 | 48 | 0.974690 | 0.13782 | 1.86287 |
| 667 | 4x | 1.2 | 84 | 0.987650 | 0.24111 | 1.86287 |
| 667 | 4x | 1.2 | 126 | 0.993220 | 0.36077 | 1.86287 |
| 667 | 4x | 1.44 | 16 | 0.919140 | 0.04783 | 1.96839 |
| 667 | 4x | 1.44 | 24 | 0.945510 | 0.07183 | 1.96839 |
| 667 | 4x | 1.44 | 32 | 0.959660 | 0.09575 | 1.96839 |
| 667 | 4x | 1.44 | 42 | 0.970940 | 0.12572 | 1.96839 |
| 667 | 4x | 1.44 | 48 | 0.975610 | 0.14363 | 1.96839 |
| 667 | 4x | 1.44 | 84 | 0.988390 | 0.25115 | 1.96839 |
| 667 | 4x | 1.44 | 126 | 0.993760 | 0.37579 | 1.96839 |
| 667 | 4x | 1.7 | 16 | 0.919180 | 0.04791 | 1.98182 |
| 667 | 4x | 1.7 | 24 | 0.945540 | 0.07195 | 1.98182 |
| 667 | 4x | 1.7 | 32 | 0.959680 | 0.09590 | 1.98182 |
| 667 | 4x | 1.7 | 42 | 0.971070 | 0.12592 | 1.98182 |
| 667 | 4x | 1.7 | 48 | 0.975740 | 0.14385 | 1.98182 |
| 667 | 4x | 1.7 | 84 | 0.988520 | 0.25150 | 1.98182 |
| 667 | 4x | 1.7 | 126 | 0.993890 | 0.37630 | 1.98182 |
| 667 | 4x | 2 | 16 | 0.919200 | 0.04794 | 1.98955 |
| 667 | 4x | 2 | 24 | 0.945560 | 0.07199 | 1.98955 |
| 667 | 4x | 2 | 32 | 0.959690 | 0.09594 | 1.98955 |
| 667 | 4x | 2 | 42 | 0.971120 | 0.12596 | 1.98955 |
| 667 | 4x | 2 | 48 | 0.975790 | 0.14390 | 1.98955 |
| 667 | 4x | 2 | 84 | 0.988560 | 0.25157 | 1.98955 |
| 667 | 4x | 2 | 126 | 0.993930 | 0.37638 | 1.98955 |

## Non-binding 100k context

Both context runs used k=256, scaled batches, and the 4x/8-round balance
setting. Wiki repair was a zero-split no-op. Dbpedia exhausted all eight
rounds and still ended at max occupancy 1,720 versus a 4x-mean limit of
1,562, so its balance target did not converge within the declared budget.

| dataset | tau squared | nprobe | recall@100 | scan fraction | storage |
| :--- | ---: | ---: | ---: | ---: | ---: |
| wiki100k | 0 | 16 | 0.804260 | 0.06143 | 1.00000 |
| wiki100k | 0 | 24 | 0.861000 | 0.09188 | 1.00000 |
| wiki100k | 0 | 32 | 0.894600 | 0.12237 | 1.00000 |
| wiki100k | 0 | 48 | 0.933260 | 0.18272 | 1.00000 |
| wiki100k | 1.2 | 16 | 0.894840 | 0.11994 | 1.82602 |
| wiki100k | 1.2 | 24 | 0.932430 | 0.17903 | 1.82602 |
| wiki100k | 1.2 | 32 | 0.952470 | 0.23772 | 1.82602 |
| wiki100k | 1.2 | 48 | 0.973770 | 0.35346 | 1.82602 |
| wiki100k | 1.44 | 16 | 0.899790 | 0.13023 | 1.99040 |
| wiki100k | 1.44 | 24 | 0.936000 | 0.19429 | 1.99040 |
| wiki100k | 1.44 | 32 | 0.955230 | 0.25788 | 1.99040 |
| wiki100k | 1.44 | 48 | 0.975600 | 0.38360 | 1.99040 |
| wiki100k | 1.7 | 16 | 0.900160 | 0.13083 | 1.99964 |
| wiki100k | 1.7 | 24 | 0.936300 | 0.19516 | 1.99964 |
| wiki100k | 1.7 | 32 | 0.955440 | 0.25903 | 1.99964 |
| wiki100k | 1.7 | 48 | 0.975720 | 0.38530 | 1.99964 |
| wiki100k | 2 | 16 | 0.900220 | 0.13087 | 2.00000 |
| wiki100k | 2 | 24 | 0.936370 | 0.19522 | 2.00000 |
| wiki100k | 2 | 32 | 0.955480 | 0.25910 | 2.00000 |
| wiki100k | 2 | 48 | 0.975740 | 0.38540 | 2.00000 |
| dbpedia100k | 0 | 16 | 0.925180 | 0.06610 | 1.00000 |
| dbpedia100k | 0 | 24 | 0.949120 | 0.09828 | 1.00000 |
| dbpedia100k | 0 | 32 | 0.962570 | 0.12958 | 1.00000 |
| dbpedia100k | 0 | 48 | 0.976200 | 0.19091 | 1.00000 |
| dbpedia100k | 1.2 | 16 | 0.959560 | 0.11889 | 1.61276 |
| dbpedia100k | 1.2 | 24 | 0.973150 | 0.17576 | 1.61276 |
| dbpedia100k | 1.2 | 32 | 0.980230 | 0.23025 | 1.61276 |
| dbpedia100k | 1.2 | 48 | 0.987460 | 0.33570 | 1.61276 |
| dbpedia100k | 1.44 | 16 | 0.967200 | 0.13769 | 1.89275 |
| dbpedia100k | 1.44 | 24 | 0.978700 | 0.20334 | 1.89275 |
| dbpedia100k | 1.44 | 32 | 0.984650 | 0.26621 | 1.89275 |
| dbpedia100k | 1.44 | 48 | 0.990280 | 0.38797 | 1.89275 |
| dbpedia100k | 1.7 | 16 | 0.968840 | 0.14118 | 1.97712 |
| dbpedia100k | 1.7 | 24 | 0.979970 | 0.20860 | 1.97712 |
| dbpedia100k | 1.7 | 32 | 0.985750 | 0.27336 | 1.97712 |
| dbpedia100k | 1.7 | 48 | 0.991130 | 0.39879 | 1.97712 |
| dbpedia100k | 2 | 16 | 0.969170 | 0.14181 | 1.99322 |
| dbpedia100k | 2 | 24 | 0.980320 | 0.20957 | 1.99322 |
| dbpedia100k | 2 | 32 | 0.986060 | 0.27467 | 1.99322 |
| dbpedia100k | 2 | 48 | 0.991480 | 0.40083 | 1.99322 |


## Pareto evidence and stop reason

No single declared policy passes all three selection constraints on both
binding datasets.

- The intended scale-aware/balanced, tau-off, 1/8-fraction policy is
  feasible at 2M: k=667, nprobe=84 reaches recall 0.967880 at scan
  0.12142 and storage 1.00000. Its corresponding 1M cell is not feasible:
  k=334, nprobe=42 reaches only recall 0.946550 at scan 0.12441.

- Best cost-compliant no-spill row: k=334, nprobe=48, recall 0.954400,
  scan 0.14192, storage 1.00000. It misses the 0.965 Phase-0 recall floor
  by 0.0106.
- Closest no-spill recall row: k=256, nprobe=48, recall 0.963570,
  scan 0.17973, storage 1.00000. It misses recall by 0.00143 and exceeds
  G2 by 0.02973.
- Smallest-spill near-cost row: k=334, tau squared=1.2, nprobe=24,
  recall 0.958410, scan 0.14284, storage 1.90317. It misses recall and
  exceeds G3 by 0.40317.
- The smallest non-zero threshold spills 907,685 of 1,000,000 rows at
  k=256 and 903,170 rows at k=334. Every larger threshold selects a
  monotonic superset and therefore cannot meet G3.
- At 2M, the smallest threshold also exceeds G3: storage is 1.88160 at
  k=256, 1.88419 at unbalanced k=667, and 1.86287 after 4x balance
  repair. The 100k contexts reach 1.82602 on wiki and 1.61276 on
  dbpedia, so the failure is not confined to one corpus size.

The documented escalation choices are required before implementation:
relax G2 toward 0.20, accept recall near 0.95, or design a probe-and-rescore
second stage. Expanding the spill grid below 1.2 would also be a new tuning
authorization because it is outside the declared experiment.
