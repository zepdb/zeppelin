# Stage 1.6 — branch deletion (EXECUTED 2026-08-08)

**47 of 56 local branches deleted.** Recovery SHAs for every one:
`tasks/branch-deletion-record-2026-08-08.txt` (`git branch <name> <sha>`).

Content was salvaged first: `50a819e` took the five tracked files that
existed only on `ll-01b-int8-split`, `ll-05-shape-check` and
`ll-09-scan-parallel-probe` — including `tasks/LateLatency/full_plan.md`,
which §0.0g had flagged as missing.

## Not deleted, and why

**Parked with a decision** (per §1.6, kept deliberately): `opt/sketch`,
`qps/f1-twophase-parked`, `opt/coalesce`, `opt/recall`,
`adversarial/phase1-coverage`.

**Has unique content**: `wip/query-api-surface-2026-07-06` — 1 patch not
upstream. Left for a decision rather than force-deleted.

**Blocked by a live worktree** — `git` refuses to delete a branch that is
checked out. All are zero-unique and deletable the moment their worktree
goes:

| Branch | Worktree | State |
|---|---|---|
| ~~`codex/branching`~~ | ~~`~/Documents/code/zeppelin-branching`~~ | **DISCARDED 2026-08-08** — see below |
| `ll-04-pipeline` | `~/Documents/code/zeppelin-ll-worktree` | clean |
| `perf/phase1-contract-core` | `~/Documents/code/zeppelin-perf` | clean |
| `quant/phase2-sketch` | `~/Documents/code/zeppelin-quant` | clean |

### `codex/branching` — investigated and discarded, 2026-08-08

Its worktree held **30 modified tracked files, +1,443/−121**, last touched
2026-07-16, committed nowhere. It was a **draft of the artifact-origins
work**: `ArtifactOrigin`, `ManifestExecutionBindingV2`, origin
validation/canonicalization/admission, `local_origin`/`fragment_origin`/
`segment_origin` — 1,350 of the 1,443 lines in `src/wal/manifest.rs`.

Discarded because it was superseded three ways:

1. **Every symbol it adds is on `main`**, landed by `fb333ac` (2026-07-17,
   the day after the worktree was last touched). Five of its seven new
   tests are on `main` verbatim.
2. **The other two tests target deleted code.** Both are receipt tests, and
   receipts were removed by `9ff6fbd` and `d83ad82`. Three files the diff
   edits — `src/security/receipt.rs`, `tests/security_receipt_tests.rs`,
   `tests/manifest_receipt_storage_tests.rs` — **no longer exist**.
3. **It did not apply.** Plain `git apply` failed on the first file and
   every one after; three-way conflicted in 6 of 12 source files and
   hard-errored on `src/security/receipt.rs: does not exist in index`.
   The base was 318 commits behind.

Its two untracked paths (`src/namespace/branching/`, `src/namespace/types.rs`)
were also earlier drafts — `main`'s versions are strictly larger
(`branching/error.rs` 41 → 326 lines) and carry the same symbols.

**Both archived before removal**, so the discard is reversible:

- `~/Documents/code/zep-temp/branching-worktree-discarded-2026-08-08.patch`
  (the tracked diff, with provenance header; `git apply --3way`)
- `~/Documents/code/zep-temp/branching-worktree-untracked-2026-08-08.tar.gz`
  (the untracked drafts)

Five stale worktree registrations were pruned (their directories no
longer existed, so nothing on disk was touched).

**Remotes untouched**: `origin/2bit-quant`,
`origin/docs/comprehensive-comment-pass` still need a decision. Remote
deletion is a separate, more consequential action.

---

## Original census (2026-08-07), retained for the reasoning


**Anup executes every deletion in this file. Claude produced the list and
deleted nothing.** Census taken at `main` = `1a48f08`.

Method, so each row is auditable:

- `ahead` / `behind` from `git rev-list --left-right --count main...<branch>`.
- **`unique`** from `git cherry -v main <branch>`, which compares *patch ids*,
  not SHAs. A branch that was rebased, reworded, or landed by
  re-authoring still shows `0 unique` when its content is on `main`. This is
  the column that matters: `ahead > 0` alone does **not** mean owed work.

---

## A. Safe to delete — zero unique patches (49 branches)

Every commit on these is patch-identical to something already on `main`.
Deleting them loses nothing.

### A1. Merged zero-ahead heads (35)

`g6-repair`, `g6-repair-f4`, `g6-repair-lifecycle`, `g6-repair-runner`,
`ll-01-instrument`, `ll-02-concurrency`, `ll-03-gap-budget`,
`ll-04-pipeline`, `ll-06-productize`, `ll-07-cpu-levers`, `ll-08-combined`,
`ll-09-scan-parallel`, `ll-worktree-track`, `mmadv-01-harness-entry`,
`mmadv-02-stream-faults`, `mmadv-03-late-content`, `mmadv-04-pathological`,
`mmadv-05-interleave-crash`, `s12-fence-seam`, `s12-modality-cache`,
`quant/phase1-bakeoff`, `quant/phase2-sketch`, `wt/create-by-name`,
`wt/gc-cas-delta`, `wt/gc-horizon`, `wt/gc-upload-window`,
`wt/namespace-stats`.

**Correction to the 2026-08-06 census in `execution_order.md`:** it listed
`ll-01…08` as uniformly zero-ahead. Two are not — `ll-01b-int8-split` and
`ll-05-shape-check` are each **1 ahead with 1 unique patch** and carry
evidence artifacts that never landed. They are in section C, not here.
`g6-repair` itself (not just its three `-f4`/`-lifecycle`/`-runner` children)
is also zero-ahead and can join the list.

### A2. Ahead but fully absorbed — `0 unique` (12)

| Branch | ahead | Evidence |
|---|---|---|
| `bycatch-B1` | 5 | `git cherry`: 0 of 5 unique |
| `bycatch-B2` | 1 | 0 of 1 unique |
| `bycatch-B3` | 1 | 0 of 1 unique |
| `bycatch-B4` | 1 | 0 of 1 unique |
| `bycatch-B5a` | 1 | 0 of 1 unique |
| `bycatch-B6` | 1 | 0 of 1 unique |
| `bycatch-B7` | 1 | 0 of 1 unique |
| `bycatch-B8` | 1 | 0 of 1 unique |
| `bycatch-B9a` | 1 | 0 of 1 unique |
| `bycatch-B9b` | 1 | 0 of 1 unique |
| `codex/branching` | 1 | 0 of 1 unique — its one commit `3b14787` "Model namespace branching lifecycle and GC" is on `main` as `475d5cf` |
| `perf/phase1-contract-core` | 2 | 0 of 2 unique |

The ten `bycatch-B*` heads are **new since the last census** — the 2026-08-06
list does not mention them at all. They are the two-bit flip bycatch from
2026-07-28/29 and are fully absorbed.

### A3. Retire by content — the MMLI branch triage (2)

| Branch | ahead | Verdict |
|---|---|---|
| `codex/mmli2-phase2` | 2 | **RETIRE.** Both commits `0 unique` — the lab diagnostics and the encoder-qualification no-go are on `main`. |
| `codex/mmli2-phase5` | 1 | **RETIRE.** `e99d355` "Add typed multimodal ingestion" is `0 unique` — landed on `main` as MMLI-2 P5 `aaf7b86`. |

---

## B. Named in the old list, still present — Anup's judgement, not a content
question

These have real unique work and are kept-or-killed on intent, not on
absorption. Listed for completeness because the 2026-07-27 list named them
and none has been deleted.

| Branch | ahead | behind | Note |
|---|---|---|---|
| `codex/branching-active` | 135 | 300 | Superseded by the ten landed branching phases |
| `codex/legitimize-repo` | 83 | 710 | Legitimize merged 2026-07-02 (`4f3bf4d`) |
| `fable/legitimize-repo` | 94 | 710 | same |
| `feat/dynamic-config` | 26 | 564 | |
| `main-bak` | 16 | 538 | |
| `pre-msg-reflow-backup` | 24 | 186 | |
| `wip/query-api-surface-2026-07-06` | 1 | 505 | 1 unique, explicitly WIP |
| `adversarial/phase1-coverage` | 7 | 391 | Stage 1.1 disposition — still undispositioned |

Remotes needing a decision: `origin/2bit-quant`,
`origin/docs/comprehensive-comment-pass`.

---

## C. **Do not delete until salvaged** — carries evidence not on `main` (3)

These are the only branches in the whole census with content `main` lacks.

| Branch | Unique content | Why it matters |
|---|---|---|
| `ll-01b-int8-split` | `tasks/LateLatency/results/phase01b-int8-g32-split.{json,md}` | The INT8-G32 truth-wave split measurement. Not tracked on `main`. |
| `ll-05-shape-check` | `tasks/LateLatency/results/phase05-shape-check.{json,md}` | The heavy-tail fetch-shape measurement. `src/config.rs:382` on `main` **states phase 05's finding in prose** ("phase 05 proved larger gaps corpus-specific: at 50k units, 256 KiB removed only 7.4% of GETs while adding 83% planned bytes") while citing `phase09-scan-int8.md` as its evidence path. The underlying phase-05 artifact is branch-only. |
| `ll-09-scan-parallel-probe` | `tasks/LateLatency/full_plan.md` | The LateLatency plan of record. `execution_order.md` §0.0g already flags it: "referenced by the phase files but absent from the directory". This branch is the only copy. |

Each branch's *code* is superseded — `main` carries a strictly newer
`flat_candidate.rs` and `phase9_flat_sq8_bench.rs`. Only the `tasks/`
artifacts are owed.

### `ll-09-scan-parallel-probe` — merge or retire?

**RETIRE, after salvaging `full_plan.md`.** Merging it would be a
**regression**. `git diff main ll-09-scan-parallel-probe` (two-dot, current
trees) shows the branch would delete all of `src/sizing/` (16 files, ~5,000
lines), `tests/advisor_emit_tests.rs`, `tests/config_tuning_doc_tests.rs`, the
`Config::load_from_str` seam, and the `BinaryHeap` selection path in
`flat_candidate.rs` — every one of which landed on `main` *after* the branch
forked. The branch is 15 behind.

Its own unique work is already on `main` in re-authored form:
`b0d4602` carries the parallel scan (`main` has `FLAT_SCAN_MIN_ROWS_PER_WORKER`
at `flat_candidate.rs:479` and `thread::scope` at `:595`), `f815bcf` +
`1a48f08` carry the 53 ms INT8 profile and its config documentation, and
`main`'s `tasks/LateLatency/results/` is a **superset** of the branch's —
`main` additionally has `phase09-scanpar-gap655360.json`,
`phase09-scanpar-knee.json`, and `phase09-scan-parallel.md`.

`git cherry` reports 6 of 7 as unique only because they were re-authored
during landing, not because their content is missing.

---

## Suggested execution order for Anup

1. Salvage the five files in section C onto `main` first (Claude can do this
   on request; it is a `git checkout <branch> -- <path>` plus a commit).
2. Delete section A (49 branches) — zero risk, zero content loss.
3. Decide section B individually.
4. Delete section C only after step 1 is committed.
