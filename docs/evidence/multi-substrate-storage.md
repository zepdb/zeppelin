# Multi-substrate storage evidence

The capability declaration was derived from the pinned `object_store` 0.11.2
implementation and validated against MinIO, patched fake-gcs-server v1.55.1,
Azurite 3.36.0, the local filesystem, and the in-memory test backend on
2026-08-14 and 2026-08-15.

## Declared capability matrix

| Substrate | Conditional PUT token | Create only | LIST/GET ETag comparable | Native batch delete | Atomic create-copy | Raw delete absent succeeds | User metadata | Identifier-only metadata names |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| S3 / MinIO | ETag | yes | yes | yes | yes | yes | yes | no |
| GCS | generation | yes | yes | no | yes | no | yes | no |
| Azure | ETag | yes | yes, after quote normalization | no | yes | no | yes | yes |
| Local | none | yes | yes | no | yes | no | no | no |
| In-memory tests | ETag | yes | yes | no | yes | no | yes | no |

The seam normalizes delete-of-absent to success even where the raw substrate
reports `NotFound`. Azure metadata names are lowered from logical hyphens to
wire underscores and normalized back on read. The `storage.fail_fast` boot
probe verifies create-only writes, fresh and stale conditional writes,
LIST/GET ETag comparison, and deletion behavior against the deployed backend
rather than trusting the static declaration alone.

## Conditional-write token-origin audit

The 2026-08-15 audit traced every production conditional-write token to its
origin. GCS LIST responses have an ETag but no generation, so a LIST-derived
token cannot authorize a GCS conditional PUT.

| Area | Direct conditional-write sites | Funnel callers | Result |
| --- | ---: | ---: | --- |
| WAL manifest and lease | 6 | — | All tokens came from GET or PUT responses |
| Namespace manager and branching | 2 | 14 | The metadata funnel received GET-derived tokens; branch-graph LIST consumers used only key and size |
| Security policy, publication, audit, and preservation | 13 | — | All tokens came through GET/PUT-derived loaded-head, claim, or existing-state funnels |
| Compaction and garbage collection | 0 | — | LIST tokens were comparison guards only; versioned manifest reads retained the GET token |

Overall, zero LIST-derived tokens reached a conditional write. Cross-request
LIST/GET ETag comparisons use a canonical form because Azure LIST responses
are unquoted while GET and PUT responses are quoted.

## Validation scope

| Gate | MinIO | Patched fake GCS | Azurite |
| --- | ---: | ---: | ---: |
| Substrate contract suite | 23 passed | 23 passed | 23 passed |
| Fail-fast boot plus ingest/query smoke | not re-recorded in this track | passed | passed |

GCS and Azure validation is emulator-backed only. No gate has run against real
GCS or Azure, and no per-substrate performance baseline has been recorded.
