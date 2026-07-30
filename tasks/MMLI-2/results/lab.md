# MMLI-2 Phase 2 encoder qualification

One seed and one repeat were used. Model and dataset files were downloaded at exact revisions once; encoder execution then used `HF_HUB_OFFLINE=1`, `TRANSFORMERS_OFFLINE=1`, `local_files_only=True`, and `trust_remote_code=False`.

## Pinned artifacts

| Artifact | Revision | SHA-256 |
| --- | --- | --- |
| `lightonai/GTE-ModernColBERT-v1/1_Dense/config.json` | `cbbe53366e564450558f5e639dd499171f127538` | `413492b3eb17ea85ecd66ed2ef02366d2d141169f3e96292611e130396a99612` |
| `lightonai/GTE-ModernColBERT-v1/1_Dense/model.safetensors` | `cbbe53366e564450558f5e639dd499171f127538` | `f1397b95dabca760e615a620cba165623137a0182bd4e4bd307e9da74fb964f3` |
| `lightonai/GTE-ModernColBERT-v1/README.md` | `cbbe53366e564450558f5e639dd499171f127538` | `2ef895fd76405fcc07778033a448ff93b69101c9b4d8d7990f0af4d5f422e13d` |
| `lightonai/GTE-ModernColBERT-v1/config.json` | `cbbe53366e564450558f5e639dd499171f127538` | `2402ec27c8ec3148b8bed3d27ca97c774d0a9d8c64c36d329821152b7a5cfba4` |
| `lightonai/GTE-ModernColBERT-v1/config_sentence_transformers.json` | `cbbe53366e564450558f5e639dd499171f127538` | `edbe6fd9b4ef756645baf9c979c3d8fe4307351fd8c1963018d0457deaedfed1` |
| `lightonai/GTE-ModernColBERT-v1/model.safetensors` | `cbbe53366e564450558f5e639dd499171f127538` | `a08c46f4ef7c9ffd8486d6531844bf6c47188d42b260ed42db72e86d5304f0e9` |
| `lightonai/GTE-ModernColBERT-v1/modules.json` | `cbbe53366e564450558f5e639dd499171f127538` | `02e279a7d7019ebac4183eb61ff776bb36688b0b01202849423bbc58850e99c5` |
| `lightonai/GTE-ModernColBERT-v1/onnx_config.json` | `cbbe53366e564450558f5e639dd499171f127538` | `73e4aac375a77036219a8b7b2f35f4febc31ec8e651996006254528c6c60ab35` |
| `lightonai/GTE-ModernColBERT-v1/sentence_bert_config.json` | `cbbe53366e564450558f5e639dd499171f127538` | `9238c37b0de481aab3ade5e78e063390a53ea7d9048b95df442e5b6f348e4387` |
| `lightonai/GTE-ModernColBERT-v1/special_tokens_map.json` | `cbbe53366e564450558f5e639dd499171f127538` | `6edfb9d64c0d7e5cbaa53516e90280fe1f42ba5ea7923d005a5f9b6e082142cf` |
| `lightonai/GTE-ModernColBERT-v1/tokenizer.json` | `cbbe53366e564450558f5e639dd499171f127538` | `23abe2a8f5640f8836c24cead7b76e77613b846824d7540151c6d90d7c2f4869` |
| `lightonai/GTE-ModernColBERT-v1/tokenizer_config.json` | `cbbe53366e564450558f5e639dd499171f127538` | `8a8d15adab97544d64822c7d50762d54a2fa15230c681e4055714d9b39a28a71` |
| `BeIR/scifact/README.md` | `b3b5335604bf5ee3c4447671af975ea25143d4f5` | `b5f57be1839fdb75867073986a019a78afe0a149950f79d1165509efc5e5cf2f` |
| `BeIR/scifact/corpus/corpus-00000-of-00001.parquet` | `b3b5335604bf5ee3c4447671af975ea25143d4f5` | `243324b35f03d82bd6d98a5f575966876e86cad7ce16e5333a35b1b793dc4f45` |
| `BeIR/scifact/queries/queries-00000-of-00001.parquet` | `b3b5335604bf5ee3c4447671af975ea25143d4f5` | `1c37956c5dc8b810b60302323c24d1a9e79e26411ba8f5ad9d0888642e2a9034` |
| `ModernVBERT/colmodernvbert-base/.gitattributes` | `17604b47f51828a5e904557094552bf23fdd9fca` | `11ad7efa24975ee4b0c3c3a38ed18737f0658a5f75a0a96787b576a78a023361` |
| `ModernVBERT/colmodernvbert-base/BUILD_INFO.json` | `17604b47f51828a5e904557094552bf23fdd9fca` | `da37ffd0e2116f585acddd6b906086cb557718fc6cbe1cb860fb4e62de72d2f3` |
| `ModernVBERT/colmodernvbert-base/README.md` | `17604b47f51828a5e904557094552bf23fdd9fca` | `ec7742ccb660fd3956bbc9d47627d95ae308fca0d3b8776c4f25a12af03ee90d` |
| `ModernVBERT/colmodernvbert-base/chat_template.jinja` | `17604b47f51828a5e904557094552bf23fdd9fca` | `fcbb6764942ea01454d80ccd9bd03365cd0ad41c31431208f7457f5cbb9ad69e` |
| `ModernVBERT/colmodernvbert-base/config.json` | `17604b47f51828a5e904557094552bf23fdd9fca` | `b732f7f984f68084fa2e4734128f5ec97dd1eb21ba21c542643b9709f47acb89` |
| `ModernVBERT/colmodernvbert-base/model.safetensors` | `17604b47f51828a5e904557094552bf23fdd9fca` | `a1723101fcb170e73fd30bd4a7d30e5747ac985206f7d561ce682108c5d671e2` |
| `ModernVBERT/colmodernvbert-base/preprocessor_config.json` | `17604b47f51828a5e904557094552bf23fdd9fca` | `2b2cad11a008b42c73c451398858fcbb6eb5e75b2ad5a55536b8994ea8711731` |
| `ModernVBERT/colmodernvbert-base/processor_config.json` | `17604b47f51828a5e904557094552bf23fdd9fca` | `0401bd1f5d81d93daf50349e3796b2866296c71544a1922ae50dc3028f20b0a5` |
| `ModernVBERT/colmodernvbert-base/special_tokens_map.json` | `17604b47f51828a5e904557094552bf23fdd9fca` | `8f23848b5d5e20595881fc5dd9884fd45ea60b1d9e42f68679393c6456ab7b46` |
| `ModernVBERT/colmodernvbert-base/tokenizer.json` | `17604b47f51828a5e904557094552bf23fdd9fca` | `65f95fb6ac63df1ec57aa2814f5c45c02a1b99d4cf6e59ec2bede0c158de4ec8` |
| `ModernVBERT/colmodernvbert-base/tokenizer_config.json` | `17604b47f51828a5e904557094552bf23fdd9fca` | `4474b6af80b05a7e5662b1fb865c19b5732952df001adeaf386af5672353adf6` |
| `ModernVBERT/colmodernvbert/.gitattributes` | `810a3ed07222eed11376ec516a5744394c7e0a0b` | `7fb9094947822ea495f22732f17e695308bcfe24775a408c1b6926327a6b5903` |
| `ModernVBERT/colmodernvbert/BUILD_INFO.json` | `810a3ed07222eed11376ec516a5744394c7e0a0b` | `55efb06c9841381e24e4f9083e52baec5e5ebc69589312d413d4a3d1f4925e63` |
| `ModernVBERT/colmodernvbert/README.md` | `810a3ed07222eed11376ec516a5744394c7e0a0b` | `c603dee94a4b435582a13ed93990c1e443a260bec2f1019de6acfb559960a9ad` |
| `ModernVBERT/colmodernvbert/adapter_config.json` | `810a3ed07222eed11376ec516a5744394c7e0a0b` | `7bf86d09ed51b02cb4241f76d19ffd96a22fb5ba85c35fadfdb47328a63764d6` |
| `ModernVBERT/colmodernvbert/adapter_model.safetensors` | `810a3ed07222eed11376ec516a5744394c7e0a0b` | `3462591f5124b4716692bbfff2c4c6e8857eb31b86878334dc6ca8f06d1322d3` |
| `ModernVBERT/colmodernvbert/bg.png` | `810a3ed07222eed11376ec516a5744394c7e0a0b` | `2f4df756454f1bd1261458a7e5fd186da2259e8e1548d09d43060bc3e5d419cb` |
| `ModernVBERT/colmodernvbert/chat_template.jinja` | `810a3ed07222eed11376ec516a5744394c7e0a0b` | `fcbb6764942ea01454d80ccd9bd03365cd0ad41c31431208f7457f5cbb9ad69e` |
| `ModernVBERT/colmodernvbert/preprocessor_config.json` | `810a3ed07222eed11376ec516a5744394c7e0a0b` | `2b2cad11a008b42c73c451398858fcbb6eb5e75b2ad5a55536b8994ea8711731` |
| `ModernVBERT/colmodernvbert/processor_config.json` | `810a3ed07222eed11376ec516a5744394c7e0a0b` | `0401bd1f5d81d93daf50349e3796b2866296c71544a1922ae50dc3028f20b0a5` |
| `ModernVBERT/colmodernvbert/special_tokens_map.json` | `810a3ed07222eed11376ec516a5744394c7e0a0b` | `8f23848b5d5e20595881fc5dd9884fd45ea60b1d9e42f68679393c6456ab7b46` |
| `ModernVBERT/colmodernvbert/table.png` | `810a3ed07222eed11376ec516a5744394c7e0a0b` | `cee9b1d779534d9b98bdaf17cc143015de764106559e762ff79a794190dcac5f` |
| `ModernVBERT/colmodernvbert/tokenizer.json` | `810a3ed07222eed11376ec516a5744394c7e0a0b` | `948b2ac5b46b1890f2fa4c43f41add4a6c04432e9cf529d788d1d7b10a3dea00` |
| `ModernVBERT/colmodernvbert/tokenizer_config.json` | `810a3ed07222eed11376ec516a5744394c7e0a0b` | `28ee34309f2fad3bc30514f6a15f743f612fa814d3d4d9ddcc3c79e810f29c79` |
| `vidore/vidore_v3_hr/README.md` | `95f2f83a5a09590a89e34960479f9438e48bca77` | `dac3c386c3e2a741dd8e9ff23fe1eb35812bbba30df8f25fb0ed3a33ab73c306` |
| `vidore/vidore_v3_hr/corpus/test-00000-of-00001.parquet` | `95f2f83a5a09590a89e34960479f9438e48bca77` | `79ae6980099204c7bc670317ef0ef31a0c9929c3e73476db64868a32f0594834` |
| `vidore/vidore_v3_hr/queries/test-00000-of-00001.parquet` | `95f2f83a5a09590a89e34960479f9438e48bca77` | `bbf0bfa70498e036e93b4b4567e37ce23ad3a658f2b7dd3567bbd70285c03606` |
| `vidore/vidore_v3_computer_science/README.md` | `7b91f10e18b72a763dd17a0c05d66bf985b98f1d` | `30c3a7add9f76ee5e5693dc6d1a0d0b3c6f54715c3fbb5b4c57240014201b02a` |
| `vidore/vidore_v3_computer_science/corpus/test-00000-of-00002.parquet` | `7b91f10e18b72a763dd17a0c05d66bf985b98f1d` | `0de0cecb23bf67ca40bb28df3d3677692f42f03cf538faa533ebe6435c1c3409` |
| `vidore/vidore_v3_computer_science/corpus/test-00001-of-00002.parquet` | `7b91f10e18b72a763dd17a0c05d66bf985b98f1d` | `00361cdf2aec14fcf35ec5a00ef63ae9b0c989e2d67417c926a44aeab220b135` |
| `vidore/vidore_v3_computer_science/queries/test-00000-of-00001.parquet` | `7b91f10e18b72a763dd17a0c05d66bf985b98f1d` | `86a800279c12f4a607378fc001e89e1d136d5568580c16cc0356eaa0092676f5` |

## Visual loader preflight

- Base: `17604b47f51828a5e904557094552bf23fdd9fca`
- Unmerged LoRA: `810a3ed07222eed11376ec516a5744394c7e0a0b`
- Adapter config base locator redirected to the exact local pinned base.
- Active LoRA modules: 89 (178 tensors).
- Query and image forward plus native `score_retrieval`: passed.
- Remote code: disabled.

## Text lane

- Official scorer: pinned PyLate `MaxSim` pairwise formula (sum over query rows of maximum token dot product).
- Row normalization: encoder L2-normalizes every retained row before f16 exchange; documents retain attention-mask rows except punctuation, queries retain attention-mask rows; no post-f16 renormalization.
- Gate passed: `false`
- Official-score pairs: 50
- MaxSim absolute error: `5.7220459e-06`
- MaxSim parity maximum relative error: `2.2933632e-07`

| Config | Algorithm | Centering | R@50 | R@100 | R@300 |
| --- | --- | --- | ---: | ---: | ---: |
| A | paper_v1 | identity | 0.234355 | 0.324977 | 0.511181 |
| B | paper_v1 | identity | 0.113796 | 0.185753 | 0.354283 |
| A | reference_v1 | identity | 0.182687 | 0.262489 | 0.458161 |
| B | reference_v1 | identity | 0.110730 | 0.175203 | 0.339946 |
| A | paper_v1 | subtract_global_mean | 0.624527 | 0.733814 | 0.877998 |
| A | paper_v1 | subtract_global_mean_renormalize | 0.588097 | 0.710730 | 0.866276 |

### Decision

- Algorithm: `paper_v1`
- FDE config: `A`
- VectorTransformRecipe: `subtract_global_mean`
- Candidate K: `100`

### Encoder cost

| Role | Count | CPU s/item batch 1 | CPU s/item batch 8 | Peak RSS MiB |
| --- | ---: | ---: | ---: | ---: |
| documents | 5183 | 0.260959 | 0.171419 | 1758.2 |
| queries | 1109 | 0.032722 | 0.020407 | 1791.6 |

### Storage cost

- Multi-vector f32 truth: `117713.4` bytes/document (mean 229.909 rows × 128 × 4 bytes).
- Config A FDE: `40960` bytes/retrieval unit (10240 f32 coordinates).
- Config B FDE: `8192` bytes/retrieval unit (2048 f32 coordinates).

### Corpus Stats

```json
{
  "dim": 128,
  "dtype": "f16",
  "matrix_count": 5183,
  "max_rows": 297,
  "mean_rows": 229.90893305035695,
  "min_rows": 53,
  "p50_rows_nearest_rank": 251,
  "p95_rows_nearest_rank": 277,
  "scalar_count": 152527104,
  "total_rows": 1191618
}
```

### Query Stats

```json
{
  "dim": 128,
  "dtype": "f16",
  "matrix_count": 1109,
  "max_rows": 48,
  "mean_rows": 21.226330027051397,
  "min_rows": 9,
  "p50_rows_nearest_rank": 20,
  "p95_rows_nearest_rank": 35,
  "scalar_count": 3013120,
  "total_rows": 23540
}
```

### Geometry

```json
[
  {
    "centering": "identity",
    "document_empty_bucket_fill_rate": 0.731658935546875,
    "document_mean_norm": 1.0000000056194271,
    "document_simhash_bucket_entropy_bits": 1.8291208927964848,
    "document_simhash_bucket_occupancy_rate": 0.5703125,
    "mean_pairwise_document_cosine": 0.8923661339288718,
    "query_mean_norm": 0.9999996994170752,
    "query_simhash_bucket_entropy_bits": 2.1535577238207098,
    "query_simhash_bucket_occupancy_rate": 0.6234375,
    "sampled_document_rows": 256,
    "simhash_sampled_document_rows": 5000,
    "simhash_sampled_documents": 256,
    "simhash_sampled_query_rows": 5000
  },
  {
    "centering": "subtract_global_mean",
    "document_empty_bucket_fill_rate": 0.080145263671875,
    "document_mean_norm": 0.3125849748690773,
    "document_simhash_bucket_entropy_bits": 4.907125141224361,
    "document_simhash_bucket_occupancy_rate": 1.0,
    "mean_pairwise_document_cosine": 0.013000690193172267,
    "query_mean_norm": 0.4211554332376544,
    "query_simhash_bucket_entropy_bits": 4.337497605406147,
    "query_simhash_bucket_occupancy_rate": 1.0,
    "sampled_document_rows": 256,
    "simhash_sampled_document_rows": 5000,
    "simhash_sampled_documents": 256,
    "simhash_sampled_query_rows": 5000
  },
  {
    "centering": "subtract_global_mean_renormalize",
    "document_empty_bucket_fill_rate": 0.080145263671875,
    "document_mean_norm": 0.9999999999983372,
    "document_simhash_bucket_entropy_bits": 4.907125141224361,
    "document_simhash_bucket_occupancy_rate": 1.0,
    "mean_pairwise_document_cosine": 0.013000690493754881,
    "query_mean_norm": 0.9999999999977021,
    "query_simhash_bucket_entropy_bits": 4.337497605406147,
    "query_simhash_bucket_occupancy_rate": 1.0,
    "sampled_document_rows": 256,
    "simhash_sampled_document_rows": 5000,
    "simhash_sampled_documents": 256,
    "simhash_sampled_query_rows": 5000
  }
]
```

### FDE failure diagnostic

- Diagnostic-only config C is outside the fixed gate and does not change the winner or go/no-go result.
- Exact per-gold ranks and score pairs: [lab-diagnostics.json](lab-diagnostics.json).
- Score/residual scatter: [lab-diagnostics.png](lab-diagnostics.png).

| Config | R/k/d | R@100 | Missed | Rank p50/p95/p99/max | 101–400 | 401–1000 | 1001–2000 | 2001+ |
| --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| A | 20/5/16 | 0.733814 | 2952 | 271/1454/2442/4666 | 1921 | 718 | 254 | 59 |
| C-diagnostic | 20/6/8 | 0.679531 | 3554 | 312/1873/3050/4827 | 2087 | 940 | 377 | 150 |

| Config | K | Exact top-1 recovered | Exact top-5 recovered | Exact top-10 recovered |
| --- | ---: | ---: | ---: | ---: |
| A | 50 | 0.922453 | 0.736519 | 0.624527 |
| A | 100 | 0.944995 | 0.820920 | 0.733814 |
| A | 300 | 0.979261 | 0.919387 | 0.877998 |
| A K needed for 95% | — | 109 | 489 | 700 |
| C-diagnostic | 50 | 0.891794 | 0.687106 | 0.572858 |
| C-diagnostic | 100 | 0.933273 | 0.773129 | 0.679531 |
| C-diagnostic | 300 | 0.969342 | 0.888909 | 0.835347 |
| C-diagnostic K needed for 95% | — | 162 | 726 | 955 |

| Config | Score pairs | Raw-exact r | Raw abs-residual/length r | Transformed-exact r | Construction abs-residual/length r |
| --- | ---: | ---: | ---: | ---: | ---: |
| A | 6104 | 0.447560 | 0.001263 | 0.847092 | -0.088864 |
| C-diagnostic | 6147 | 0.421344 | 0.018258 | 0.814834 | -0.012999 |

- A transform SHA-256: `31d3abb61a5362f3ce5a4986bfde7d3ad54f75b78be7174cb78a9970b5cf2e5d`.
- C-diagnostic transform SHA-256: `19a1a19ab3a722bd82cc9157c17d3ac571841f97709d47c6c1a22f07be44af20`.
- Rank shape: for A, 65.1% of K=100 misses land at ranks 101–400,
  89.4% at ranks ≤1,000, and 2.0% beyond rank 2,000. The ordering is
  mostly noisy near the candidate frontier with a smaller long tail; it is
  not near-random.
- Document-length bias is not supported: A's absolute residual/document-row
  correlation is `0.001263` against raw exact MaxSim and `-0.088864` against
  transformed exact MaxSim.
- Raising `k_sim` while cutting `d_proj` did not cure the failure:
  C-diagnostic reduced top-10 R@100 from `0.733814` to `0.679531`.
- Metric provenance: the Phase 2 gate measures the fraction of every exact
  top-10 frontier recovered. The MUVERA paper's offline `1Recall@N` measures
  recovery of the single exact Chamfer nearest neighbor. Under that paper
  metric A reaches `0.944995` at K=100 and `0.979261` at K=300; 95% recovery
  requires K=109. Recovering 95% of the entire exact top-10 requires K=700.
- Parameter provenance: A (`R=20`, `k_sim=5`, `d_proj=16`) is the paper's
  direct 10,240-D Pareto cell. Its headline final-projection experiment first
  builds `R=40`, `k_sim=6`, `d_proj=128` (327,680-D), then projects to
  10,240-D; C-diagnostic is not a paper operating point.

## Named decisions and resolved lateon unknowns

- No-go: no candidate operating point is authorized because the text recall gate failed.
- Best observed algorithm: `paper_v1`.
- Candidate transform: `subtract_global_mean`; the mean is computed from at most 5,000 evenly spaced document rows and the same frozen mean is applied to queries and documents. Centering is candidate-only; official exact MaxSim remains raw.
- Operating point: config `A`, D=10240, K=100, measured candidate recall=0.733814.
- Exact-scoring transform: `Identity` over the model-normalized, row-filtered matrix.
- Lab execution adapter: pinned Transformers CPU with remote code disabled; visual LoRA remains active and unmerged.
- Artifact reproducibility: exact model/dataset revisions and every downloaded artifact SHA-256 are recorded above.
- Visual-transfer decisions remain unresolved because the text gate stopped or the visual lane has not run.
