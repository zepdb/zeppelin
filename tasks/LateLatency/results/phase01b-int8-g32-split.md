# Phase 01b — INT8-G32 truth-wave timing split

One release-mode run against native MinIO completed in 463.93 s. The f16
comparison is the Phase 01 run in `phase01-truth-wave-split.md`, which
completed in 542.38 s. Deltas are INT8-G32 minus f16, so negative latency,
request, byte, and wave deltas are improvements.

## Timing split

Times are per query except for Physical GET, which is per physical GET.

| Stage | Stat | f16 (ms) | INT8-G32 (ms) | Delta (ms) | Delta (%) |
| --- | --- | ---: | ---: | ---: | ---: |
| Truth wave | min | 318.000 | 249.000 | -69.000 | -21.70% |
| Truth wave | p5 | 353.000 | 284.000 | -69.000 | -19.55% |
| Truth wave | p50 | 417.000 | 348.000 | -69.000 | -16.55% |
| Truth wave | p95 | 574.000 | 508.000 | -66.000 | -11.50% |
| Truth wave | p99 | 629.000 | 566.000 | -63.000 | -10.02% |
| Truth wave | max | 692.000 | 615.000 | -77.000 | -11.13% |
| Truth wave | mean | 432.562 | 363.906 | -68.656 | -15.87% |
| Fetch | min | 59.561 | 47.454 | -12.107 | -20.33% |
| Fetch | p5 | 60.806 | 49.602 | -11.204 | -18.43% |
| Fetch | p50 | 62.685 | 52.195 | -10.490 | -16.73% |
| Fetch | p95 | 65.676 | 55.533 | -10.143 | -15.44% |
| Fetch | p99 | 67.510 | 58.953 | -8.558 | -12.68% |
| Fetch | max | 79.977 | 134.519 | +54.542 | +68.20% |
| Fetch | mean | 62.921 | 52.469 | -10.452 | -16.61% |
| Score | min | 255.314 | 195.377 | -59.937 | -23.48% |
| Score | p5 | 289.200 | 230.734 | -58.466 | -20.22% |
| Score | p50 | 353.902 | 295.717 | -58.185 | -16.44% |
| Score | p95 | 510.703 | 455.338 | -55.365 | -10.84% |
| Score | p99 | 565.241 | 508.123 | -57.118 | -10.11% |
| Score | max | 630.190 | 561.321 | -68.870 | -10.93% |
| Score | mean | 369.346 | 311.161 | -58.185 | -15.75% |
| Decode | min | 162.534 | 107.014 | -55.520 | -34.16% |
| Decode | p5 | 167.855 | 110.891 | -56.964 | -33.94% |
| Decode | p50 | 175.999 | 116.517 | -59.481 | -33.80% |
| Decode | p95 | 187.315 | 124.688 | -62.627 | -33.43% |
| Decode | p99 | 195.842 | 128.495 | -67.346 | -34.39% |
| Decode | max | 201.716 | 135.378 | -66.338 | -32.89% |
| Decode | mean | 176.511 | 116.902 | -59.609 | -33.77% |
| MaxSim | min | 80.201 | 80.402 | +0.202 | +0.25% |
| MaxSim | p5 | 113.323 | 113.605 | +0.283 | +0.25% |
| MaxSim | p50 | 177.595 | 178.811 | +1.217 | +0.69% |
| MaxSim | p95 | 329.198 | 332.547 | +3.349 | +1.02% |
| MaxSim | p99 | 383.566 | 390.082 | +6.517 | +1.70% |
| MaxSim | max | 445.716 | 441.965 | -3.751 | -0.84% |
| MaxSim | mean | 192.257 | 193.594 | +1.337 | +0.70% |
| Physical GET | min | 0.384 | 0.316 | -0.069 | -17.83% |
| Physical GET | p5 | 0.512 | 0.501 | -0.011 | -2.12% |
| Physical GET | p50 | 0.615 | 0.601 | -0.013 | -2.15% |
| Physical GET | p95 | 1.061 | 1.029 | -0.032 | -3.02% |
| Physical GET | p99 | 1.641 | 1.620 | -0.020 | -1.24% |
| Physical GET | max | 3.362 | 8.318 | +4.956 | +147.40% |
| Physical GET | mean | 0.677 | 0.661 | -0.016 | -2.32% |

At p50, INT8-G32 reduced the truth wave by 69 ms (16.55%), fetch by
10.490 ms (16.73%), and score by 58.185 ms (16.44%). The score reduction
came from decode, which fell 59.481 ms (33.80%); MaxSim was effectively
unchanged at +1.217 ms (+0.69%).

## Request shape

| Metric | Stat | f16 | INT8-G32 | Delta | Delta (%) |
| --- | --- | ---: | ---: | ---: | ---: |
| GETs/query | min | 687.000 | 587.000 | -100.000 | -14.56% |
| GETs/query | p5 | 710.000 | 604.000 | -106.000 | -14.93% |
| GETs/query | p50 | 730.000 | 622.000 | -108.000 | -14.79% |
| GETs/query | p95 | 748.000 | 639.000 | -109.000 | -14.57% |
| GETs/query | p99 | 755.000 | 647.000 | -108.000 | -14.30% |
| GETs/query | max | 768.000 | 655.000 | -113.000 | -14.71% |
| GETs/query | mean | 729.537 | 621.946 | -107.592 | -14.75% |
| Bytes/query | min | 53,492,510.000 | 32,967,596.000 | -20,524,914.000 | -38.37% |
| Bytes/query | p5 | 55,638,558.000 | 34,419,986.000 | -21,218,572.000 | -38.14% |
| Bytes/query | p50 | 58,068,192.000 | 35,795,006.000 | -22,273,186.000 | -38.36% |
| Bytes/query | p95 | 61,876,384.000 | 37,820,436.000 | -24,055,948.000 | -38.88% |
| Bytes/query | p99 | 64,084,966.000 | 39,279,454.000 | -24,805,512.000 | -38.71% |
| Bytes/query | max | 65,747,444.000 | 40,546,988.000 | -25,200,456.000 | -38.33% |
| Bytes/query | mean | 58,340,418.658 | 35,947,281.966 | -22,393,136.693 | -38.38% |
| Waves/query | min | 86.000 | 74.000 | -12.000 | -13.95% |
| Waves/query | p5 | 89.000 | 76.000 | -13.000 | -14.61% |
| Waves/query | p50 | 92.000 | 78.000 | -14.000 | -15.22% |
| Waves/query | p95 | 94.000 | 80.000 | -14.000 | -14.89% |
| Waves/query | p99 | 95.000 | 81.000 | -14.000 | -14.74% |
| Waves/query | max | 96.000 | 82.000 | -14.000 | -14.58% |
| Waves/query | mean | 91.619 | 78.188 | -13.431 | -14.66% |

Across 1,109 queries, total physical GETs fell from 809,057 to 689,738
(-119,319, -14.75%) and total request waves fell from 101,605 to 86,710
(-14,895, -14.66%). Every observed GET count matched its read plan. The p50
payload fell from 58,068,192 bytes to 35,795,006 bytes (-22,273,186,
-38.36%).

## Recall and run gates

K=1000 recall was exactly **10,809/11,090 = 0.9746618575**. This is the
accepted INT8-G32 baseline for this bench harness. It is five hits above the
prior 10,804 expectation derived from the separate `mmli_lab` pipeline; that
cross-harness difference is accepted. The f16 baseline remains exactly
10,869/11,090 = 0.9800721371.

The filtered-query checks, object cleanup, and test all passed. The one
surprising tail result was an isolated 8.318 ms maximum physical GET versus
3.362 ms for f16, which also raised the maximum fetch time to 134.519 ms from
79.977 ms. The GET p99 and mean still improved, so this did not extend across
the measured distribution.
