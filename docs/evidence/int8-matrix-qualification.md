# INT8 matrix qualification evidence

This production writer/decoder measurement was recorded on 2026-07-31 at
source revision `dd6aa2ff1fe25bc258db5765f9274aafb9d176ea`. Each lane was encoded
as `int8_sym_v1 { group_size: 32 }`, written through the production `ZME1`
artifact encoder, read through the production decoder, and scored exhaustively
against the same-pass f16 reference. Queries remained f16.

| Lane | Documents / queries | Same top-1 vs f16 | Top-10 memberships recovered | Exact top-10 set/query | f16 / INT8 payload | Saving | Release status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Text | 5,183 / 1,109 | 99.909829% | 99.269612% | 92.786294% | 305,054,208 / 162,060,048 B | 46.875% | Approved |
| Visual | 2,000 / 533 | 99.249531% | 99.737336% | 97.373358% | 520,690,944 / 276,617,064 B | 46.875% | Held |

The production gate required at least 99.5% same-top-1 agreement. The text lane
met that bar and was subsequently approved; the visual lane did not and remains
held. The raw result was emitted before that operator decision, so its embedded
`decision_state` remains `measured_awaiting_operator_threshold`.

The harness also verified artifact checksums, authoritative header dtypes,
document-ID parity, f16/f32 tensor-ID parity, and a complete artifact
write/read. The SHA-256 of the durable JSON measurement is
`e91ef65c9c26a772a7a98e05985ceb7f310a094541d853559fc3aaee0a88794b`;
that digest is the evidence identity carried by the approved text tuple.
