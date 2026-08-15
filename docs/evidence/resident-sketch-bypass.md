# Resident-sketch row-frontier bypass evidence

The rejected bypass selected exact-vector rows directly from the resident
two-bit sketch and skipped SQ8 coarse payloads. It is not in the current tree;
the implementation can be recovered from commit `212b689` for reproduction.

The fixture used 20,000 deterministic synthetic rows at 256 dimensions. The
control and bypass used the same unfiltered query, centroid probe set, frontier,
and exact-f32 rerank.

| Path | GETs/query | Objects touched | Coarse bytes/query | ID bytes/query | Rerank bytes/query | Recall@10 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SQ8 coarse path | 20 | 10 | 4,148,448 | 259,376 | 8,355,840 | 1.00 |
| Resident-row bypass | 18 | 9 | 0 | 184,560 | 8,625,152 | 0.80 |

At `top_k = 100`, recall fell from 1.00 to 0.94. Physical cluster-object bytes
fell only 1.45x, below the predeclared 2x threshold, while result quality moved
materially. A 40-row frontier over 3,750 probed rows still touched 9 of the 10
grouped objects touched by the coarse path, so winner dispersion—not row
addressing—limited the request reduction.

The retained fixed-stride work still removes one directory GET per grouped
object. The pinned two-object query falls from six cluster GETs to four, about a
33% reduction at the measured probe ratio, without using the rejected bypass.
