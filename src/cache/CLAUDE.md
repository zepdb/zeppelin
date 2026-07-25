# src/cache — disposable tiers

**Nothing here is authoritative.** A cache hit can never publish, resurrect, or
hide an artifact. S3 is the source of truth; local state is disposable.

Tiers: `MemoryCache` → `DiskCache` → S3 fetch closure, entered via
`DiskCache::get_or_fetch`. `manifest_cache.rs` is separate because manifests
are *mutable* visibility records with TTL and freshness rules, unlike the
write-once artifacts the byte cache holds.

## Correctness rule

Reuse is correct only when the caller supplies the exact key of an **immutable**
object selected by its current manifest snapshot. Zeppelin's write-once artifact
contract is what makes the cache safe; code that caches mutable bytes must
invalidate explicitly and is outside that contract.

For branches this matters: cache identity must come from the **physical**
origin, not the logical namespace. Two branches sharing a source segment must
share one cache entry, and neither may reconstruct a key from its own name.

## Traps

- **Use unique temp filenames for atomic writes.** `{file}.{uuid}.tmp`, never a
  fixed `.tmp` suffix — concurrent tasks writing the same cache key otherwise
  race and corrupt each other.
- **Keep `TempDir` alive** for as long as anything uses its path.
  `TempDir::drop()` deletes the directory, so setup helpers must *return* the
  handle rather than dropping it.
- Eviction is Redis-style **16-entry sampled LRU** (O(1)), in both
  `MemoryCache` and `DiskCache`. It is approximate by design.
- Manifest fetches are coalesced by a per-namespace singleflight mutex, and the
  write path is **write-through** (`ManifestCache::insert`), not
  invalidate-then-refetch.

## Hydration

`hydration.rs` speculatively warms the byte cache. It is a pure optimization —
failure must never change authoritative data or query visibility.

Branch-safety contract, added by the branching work: a queued job must
re-prove that its snapshot is still authoritative — exact logical manifest
generation, namespace incarnation, active segment, and resolved physical origin
— **before any physical HEAD or GET**. If the branch was fenced, rotated, or
deleted meanwhile, the job aborts having touched nothing.

Jobs have a hard `job_timeout` covering authority refresh, planning,
downloads, retries, and backoff. It is wired to the server request timeout, so
that governed deletion's reader-safety grace floor provably outlives any
detached hydration reader. **If you change either value, re-check that
relationship** — it is what keeps a hydration reader from outliving the grace
window.

## See also

- `../wal/CLAUDE.md` — artifact origins and manifest generations
- `../namespace/CLAUDE.md` — the grace window hydration must not outlive
