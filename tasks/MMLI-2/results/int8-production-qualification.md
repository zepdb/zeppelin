# MMLI-2 INT8 production qualification

Measured 2026-07-31 against source revision
`dd6aa2ff1fe25bc258db5765f9274aafb9d176ea` with the release
`mmli_lab` binary.

## Execution seam

Each lane was encoded once. The same-pass tensors supplied f32 and f16
exact rankings. Queries remained f16. Document matrices were written with
the production `MatrixArtifact::to_bytes` implementation as
`int8_sym_v1 { group_size: 32 }`, persisted as complete `ZME1` artifacts,
then read and decoded with `MatrixArtifact::from_bytes` before exhaustive
MaxSim scoring.

The harness verified the artifact checksum, authoritative header dtype,
document-ID parity, and f16/f32 tensor-ID parity. It atomically merged the
text and visual results into the durable JSON result before deleting the
raw tensors and artifact files.

## Ranking agreement

| Lane | Candidate / reference | Top-10 recovered | Misses | Same set/query | Same order/query | Same top-1 | Reference top-1 in top-10 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Text | f16 / f32 | 99.909829% | 10/11090 | 99.098287% | 95.852119% | 99.909829% | 100.000000% |
| Text | INT8 G32 / f32 | 99.296664% | 78/11090 | 93.056808% | 69.161407% | 99.819657% | 100.000000% |
| Text | INT8 G32 / f16 | 99.269612% | 81/11090 | 92.786294% | 69.251578% | 99.909829% | 100.000000% |
| Visual | f16 / f32 | 100.000000% | 0/5330 | 100.000000% | 98.874296% | 99.624765% | 100.000000% |
| Visual | INT8 G32 / f32 | 99.737336% | 14/5330 | 97.373358% | 81.801126% | 99.249531% | 100.000000% |
| Visual | INT8 G32 / f16 | 99.737336% | 14/5330 | 97.373358% | 81.238274% | 99.249531% | 100.000000% |

## Production result against the draft gate

| Lane | Lab G32 misses | Production misses | Delta | Draft miss band | Same top-1 | Draft top-1 bar | Result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Text | 70 | 78 | +8 | 55–85 | 99.819657% | ≥99.5% | Both draft checks met |
| Visual | 14 | 14 | 0 | 0–29 | 99.249531% | ≥99.5% | Top-1 check not met |

The production folded-scale arithmetic reproduced the durable lab
membership counts within the stated ±15 counting band in both lanes.
The text same-top-1 result also met the draft bar. The visual result did
not: 529 of 533 queries retained the same top document, while the draft
bar requires at least 99.5%.

## Same document at each rank

| Lane | Comparison | R1 | R2 | R3 | R4 | R5 | R6 | R7 | R8 | R9 | R10 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Text | f16 / f32 | .999098 | .999098 | .997295 | .994590 | .993688 | .992786 | .989179 | .988278 | .987376 | .982867 |
| Text | INT8 / f32 | .998197 | .987376 | .968440 | .955816 | .941389 | .929666 | .908927 | .885482 | .876465 | .874662 |
| Text | INT8 / f16 | .999098 | .988278 | .965735 | .955816 | .945897 | .929666 | .907124 | .880974 | .878269 | .878269 |
| Visual | f16 / f32 | .996248 | .994371 | .998124 | 1.000000 | 1.000000 | .996248 | .996248 | .998124 | .998124 | 1.000000 |
| Visual | INT8 / f32 | .992495 | .969981 | .962477 | .966229 | .962477 | .953096 | .960600 | .949343 | .947467 | .956848 |
| Visual | INT8 / f16 | .992495 | .968105 | .960600 | .966229 | .962477 | .953096 | .960600 | .947467 | .945591 | .956848 |

## Payload

| Lane | Documents | Rows | f16 payload | INT8 payload | Complete artifact | Mean INT8 payload/document | Saving vs f16 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Text | 5,183 | 1,191,618 | 305,054,208 B | 162,060,048 B | 162,288,164 B | 31,267.615 B | 46.875% |
| Visual | 2,000 | 2,033,949 | 520,690,944 B | 276,617,064 B | 276,705,128 B | 138,308.532 B | 46.875% |

## Evidence

- Durable JSON:
  `tasks/MMLI-2/results/int8-production-qualification.json`
- JSON SHA-256:
  `e91ef65c9c26a772a7a98e05985ceb7f310a094541d853559fc3aaee0a88794b`
- Text evidence digest:
  `ab7f8e61c0252d90e235759b47971a4750eece1d780ddaa67d427964f9ae8c12`
- Text artifact checksum:
  `2c531f23b98245ec35e8c857c7d1b96528747643020cb9aac73543f539cd9c74`
- Visual evidence digest:
  `47207bcdef5807d6de3e39b53ff7948ddf90b5cd20567c85c7bbdac34ee60631`
- Visual artifact checksum:
  `03af697f010180b81cccf7a33a0458f40336c813439f5e83352376283f8b9d99`

The JSON also records SHA-256 digests for all same-pass f32 and f16 input
tensors.

## Decision

This run is **measured, awaiting operator threshold**, not an INT8
qualification pass. The final visual threshold remains unsigned, and the
production visual same-top-1 result misses the draft 99.5% bar.

No qualification tuple is approved and no stamp is minted.
`int8_sym_v1` profile activation remains fail-closed. `f16` remains the
default usable matrix format.

## Verification

- `CARGO_INCREMENTAL=0 cargo build --bin mmli_lab`: passed.
- `CARGO_INCREMENTAL=0 cargo clippy --all-targets -- -D warnings`:
  passed.
- `CARGO_INCREMENTAL=0 cargo test --lib embedding`: 32 passed,
  0 failed, 1 ignored because it requires the separately pinned model
  bundle.
- Python syntax compilation and the production-qualification CLI help:
  passed.
- Result-file presence, INT8 result-section check, `cargo fmt --check`,
  and `git diff --check`: passed.
