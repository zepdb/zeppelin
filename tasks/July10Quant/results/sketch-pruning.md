# Resident-sketch pruning finding

Inspected tree: `46bebaf`.

The production cap is:

```text
min(
    2 * max(6, ceil(0.3125 * nprobe + nprobe^2 / 150)),
    nprobe
)
```

The exact probe counts in `1..=256` where the cap is smaller than
`nprobe` are:

```text
13, 14, 15, 16, 17, 18, 19, 20, 21, 23, 25
```

This corrects the earlier list by adding `25`.

The scale-aware production checkpoints retain every probe: `32 -> 32`,
`48 -> 48`, `63 -> 63`, `126 -> 126`, and `256 -> 256`. The two tests
that pin the result are:

- `v4_scale_aware_sentinels_are_structural_noops`
- `adaptive_sketch_cap_scales_monotonically`

Conclusion: the resident sketch prunes zero clusters at default
production `nprobe`.

The sketch is not byte-for-byte unused. It still supplies
`cluster_has_attrs` and performs row scoring used for cluster evidence.
This finding does not recommend changing the budget constants.
