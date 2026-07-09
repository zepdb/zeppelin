# Rust documentation style

Zeppelin's documentation should let an engineer open any Rust file and answer
three questions quickly:

1. Where am I in the system?
2. What contract does this code provide?
3. Which Rust ideas make that contract safe or efficient?

The intended reader is an experienced Java or C engineer who is learning Rust
while learning Zeppelin. Write in high-level, plain English. Explain domain
intent, observable behavior, and correctness constraints; do not translate the
implementation into English one statement at a time.

This guide applies to production code, binaries, tests, benchmarks, fuzz
targets, and test infrastructure.

## Coverage and depth

Documentation coverage is exhaustive. Documentation depth is proportional.

- Every Rust source file or inline module has a `//!` orientation.
- Every function and method has a `///` contract, including private helpers and
  test helpers.
- Every struct, enum, trait, type alias, constant, static, variant, and
  significant field is documented.
- Public fields and enum variants are always documented. Document private
  fields when their role, units, ownership, or invariant is not obvious.
- Every non-trivial operation has a realistic example.
- Every file and non-trivial operation is reviewed for an applicable ASCII
  diagram and for Rust concepts worth teaching to a Java/C engineer.

`#![deny(missing_docs)]` is only a compiler-enforced floor: it checks reachable
public items, not whether private code is understandable or whether a comment
is useful. Review private items deliberately and build documentation with
`--document-private-items`.

Use this depth guide:

| Kind of code | Expected depth |
| --- | --- |
| Trivial getter or conversion | A precise summary; parameters, return meaning, and a short scenario when useful |
| Private helper | Purpose in its caller's workflow, assumptions, inputs, output, and a concise example |
| Domain operation or public API | Full contract, meaningful failures, side effects, consistency, performance, and example |
| Storage, manifest, WAL, compaction, query, cache, index, lease, or concurrency code | Full contract plus authority/visibility rules, failure state, cost, diagram when applicable, and Rust learning notes |

Do not make a small function look complicated merely to fill a template. Do
not omit its documentation because the implementation looks obvious.

## Comment forms

Use Rust's line-oriented comment forms consistently:

- `//!` documents the containing crate, file, or module. Put it before imports
  and item declarations.
- `///` documents the item immediately below it and appears in generated
  rustdoc.
- `//` explains a non-obvious implementation choice within a function.
- Prefer repeated line comments. Avoid `/* ... */` and `/** ... */` unless a
  rare formatting constraint makes them necessary.

An internal `//` comment should explain why an operation is ordered, why a
clone is intentional, why a lock is released before `.await`, or why an
apparently simpler implementation would violate an invariant. It should not
say `// increment the counter` above `counter += 1`.

## File and module orientation

A reader should not have to inspect imports to discover a file's architectural
role. Each `//!` overview explains:

- the responsibility this file owns and what it deliberately does not own;
- where the file sits in Zeppelin's architecture;
- which layer or caller enters it and which important modules it calls;
- its major types and entry points;
- the state or immutable artifacts it reads, creates, publishes, caches, or
  deletes;
- the authority, visibility, ordering, and concurrency invariants it protects;
- where a new reader should begin;
- notable Rust techniques used here and why they suit the responsibility.

Use a `## Reading map` in a large or multi-phase file. Link to types and
functions rather than citing line numbers, which become stale as code moves.

```rust
//! Loads and publishes the manifest that defines a namespace's visible data.
//!
//! The object stored in S3 or MinIO is authoritative. Memory and disk may cache
//! a previously loaded manifest, but they cannot make a WAL fragment or segment
//! visible to readers. Callers enter through [`load_manifest`] to observe state
//! and [`publish_manifest`] to replace it conditionally.
//!
//! ## Reading map
//!
//! 1. Start with [`Manifest`] for the persistent data model.
//! 2. Read [`load_manifest`] for the read and cache path.
//! 3. Read [`publish_manifest`] for ETag compare-and-swap publication.
//!
//! ## Invariants
//!
//! - Published artifacts are immutable.
//! - A stale ETag never overwrites a newer manifest.
//! - Local cache never overrides object-store state.
//!
//! ## Rust concepts used here
//!
//! [`Arc`] shares the store client between async tasks without giving any task
//! unique ownership of it. See [`publish_manifest`] for the difference between
//! sharing a client and conditionally mutating the remote manifest.
```

The example names above are illustrative. Link only to items that actually
exist and use qualified links when necessary.

### File-level diagram decision

Record an ASCII diagram in the `//!` overview when understanding the file
requires tracking any of the following:

- three or more phases;
- more than one store or representation;
- authoritative state versus cached state;
- success and failure branches;
- ownership transfer or borrow lifetime;
- multiple tasks, locks, channels, or concurrent actors.

For a simple leaf module, a diagram may add noise. The decision is explicit,
not automatic: review every file and omit a diagram only when prose is clearer.

## Item documentation

Document an item from the caller's point of view. Start with a one-sentence
summary that completes the thought "This item ..." without repeating its name.
Follow with enough context to explain why it exists and how it fits the
surrounding subsystem.

### Types, traits, variants, and fields

For a struct or enum, describe the domain concept, valid states, ownership, and
serialization or compatibility constraints. For a trait, describe the
abstraction boundary, who implements it, and which behavior every
implementation must preserve. For a field or variant, explain its meaning,
units, sentinel values, and relationship to other fields.

Document derives when they have a meaningful consequence. For example, explain
why a persisted type is `Serialize`, why a cheap identifier is `Copy`, or why a
shared configuration snapshot must be `Send + Sync`. Do not list derives that
need no interpretation.

### Functions and methods

At minimum, every function or method documents:

- what it does and what its caller can observe;
- each parameter in domain terms, including units, validation, and ownership
  expectations;
- the exact return meaning, including what `None` or an empty collection means;
- a short, high-level scenario when it clarifies behavior.

Add the following sections when applicable. Keep this order so readers can scan
contracts consistently:

1. `# Parameters`
2. `# Returns`
3. `# Errors`
4. `# Panics`
5. `# Side Effects`
6. `# Consistency`
7. `# Performance`
8. `# Examples`
9. `# Rust Notes for Java/C Engineers`
10. `# Safety`

The sections mean:

- `# Parameters`: Describe the domain value, constraints, units, validation
  state, and whether ownership is borrowed, moved, or shared. Omit only when
  there are no parameters.
- `# Returns`: Describe the success value, ordering, ownership, and special
  cases. For `Result`, describe the success value here and failures under
  `# Errors`. Omit only for functions returning `()` when the side-effect
  contract already makes that clear.
- `# Errors`: Required for `Result`. Group meaningful failure classes and say
  whether remote or local partial work can already exist. Do not merely say
  "returns an error on failure."
- `# Panics`: Required when callers can trigger a panic. State the exact
  precondition. A panic that would expose corrupted state or an invariant
  violation still deserves documentation.
- `# Side Effects`: Describe S3/MinIO requests, artifact creation, manifest
  publication, cache or metric mutation, task spawning, and shared-state
  mutation.
- `# Consistency`: Explain authority, visibility, ETag CAS, lease and fencing
  checks, cache invalidation, and ordering assumptions.
- `# Performance`: Explain cost in domain terms: sequential object-store
  roundtrips, GETs/PUTs, ranges and bytes read, allocation, CPU complexity,
  locking, cache behavior, or spawned work.
- `# Examples`: Show the starting state, operation, and result. Include an
  important failure case when it is part of the contract.
- `# Rust Notes for Java/C Engineers`: Teach a context-specific, non-obvious
  Rust choice. See the dedicated section below.
- `# Safety`: Required for unsafe functions and meaningful unsafe blocks.
  State the complete invariant that makes the operation sound.

For a small private helper, proportional documentation can stay concise:

```rust
/// Converts a published artifact entry into the segment descriptor used by the
/// query planner.
///
/// # Parameters
///
/// - `entry`: Manifest entry for a segment already visible to readers.
///
/// # Returns
///
/// An owned descriptor containing the object-store location and vector count.
///
/// # Examples
///
/// A visible entry for a 10,000-vector segment becomes the planner-facing
/// descriptor used to decide which immutable object ranges to read.
```

For an important operation, document the complete lifecycle:

```rust
/// Publishes a namespace manifest with an object-store compare-and-swap write.
///
/// This operation is the visibility boundary. Uploading an immutable WAL
/// fragment or segment does not make it queryable; a successful manifest
/// publication does.
///
/// # Parameters
///
/// - `previous_etag`: Version of the authoritative manifest on which the caller
///   based this update.
/// - `next_manifest`: Complete set of artifacts that should be visible after
///   publication.
///
/// # Returns
///
/// The ETag assigned to the newly published manifest.
///
/// # Errors
///
/// Returns a storage error if the conditional PUT fails. An ETag mismatch means
/// another publication won; the caller must reload and must not overwrite it.
/// Artifacts uploaded before this call may remain in object storage but are not
/// visible through the manifest.
///
/// # Side Effects
///
/// Performs one conditional object-store PUT when validation succeeds.
///
/// # Consistency
///
/// The S3/MinIO manifest remains authoritative. Cache state cannot satisfy or
/// bypass the ETag precondition.
///
/// # Examples
///
/// A writer based on version 12 uploads a fragment and attempts publication. If
/// version 12 is still current, version 13 exposes the fragment. If version 13
/// already exists, publication fails and the fragment remains unreferenced.
```

Again, adapt names and behavior to the real code. Never paste a plausible
contract without checking the implementation and its callers.

## High-level examples

Examples should let a reader mentally execute the contract without first
learning the implementation.

- Prefer a Zeppelin scenario over placeholder values such as `foo` and `bar`.
- State the before state, the operation, and the after state or return value.
- For manifests, WAL, segments, compaction, cache invalidation, deletes, and
  leases, make visibility and ordering explicit.
- Include failure examples when they explain retry, idempotency, partial work,
  a CAS miss, corruption, or rejected input.
- Keep the example local. Do not invent a large fake API or duplicate an
  integration test inside rustdoc.
- A tiny getter, constant, or obvious constructor does not need a ceremonial
  example. A one-sentence inline scenario is enough when useful.

Choose the fence by what the example teaches:

- Use a normal `rust` fence for self-contained code that rustdoc should compile
  and test.
- Use `no_run` when the code should compile but requires S3, MinIO, a network,
  a Tokio runtime setup, or other external state to execute.
- Use `text` for conceptual before/after examples, flows, and ASCII diagrams.
- Use `ignore` only when compilation is impossible for a documented reason.
  Prefer a conceptual `text` example over pretend Rust.

Code examples should use `?` when error propagation is the lesson and may hide
setup lines with `#` only when the visible example stays understandable. Do not
use `unwrap()` or `expect()` in examples; Zeppelin denies them in code because
failures must remain explicit.

## Rust Notes for Java/C Engineers

Add `## Rust concepts used here` at module level or `# Rust Notes for Java/C
Engineers` at item level when Rust contributes meaningfully to the design.
Teach the feature in the context of the Zeppelin operation, not as an isolated
language tutorial.

High-value topics include:

- ownership, borrowing, moves, lifetimes, `Copy`, `Clone`, and allocation;
- `Arc`, locks, atomics, `DashMap`, channels, and `Send`/`Sync`;
- `Option`, `Result`, `?`, and exhaustive `match`;
- enums and newtypes that make invalid states unrepresentable;
- traits, generics, associated types, and `dyn Trait` dispatch;
- iterator pipelines, closures, and zero-cost abstraction;
- async functions, Tokio tasks, cancellation, and lock lifetime around `.await`;
- RAII and `Drop` for cleanup;
- slices and `bytes::Bytes` as borrowed or shared byte views;
- serde attributes, derives, and persisted-format compatibility;
- `#[must_use]`, visibility, and module boundaries;
- unsafe code and its safety invariant.

A useful note answers the relevant subset of these questions:

1. Which Rust feature is used here?
2. Why does it fit this Zeppelin operation?
3. What is the nearest Java mental model?
4. What is the nearest C mental model?
5. What does the Rust compiler prevent or guarantee?
6. Does the operation borrow, move, clone, copy, allocate, share, or lock?
7. What short scenario or snippet makes that concrete?

Comparisons are analogies, not equivalences. Say where the analogy stops. A
Rust `&T` resembles a Java reference in how it names an existing value and a C
`const T *` in read-only use, but unlike either comparison it is statically
non-null, lifetime-checked, and restricted by Rust's aliasing rules. Java has no
direct equivalent of a move that makes the original binding unusable; ordinary
C pointer assignment does not transfer compiler-enforced ownership.

Example:

```rust
/// # Rust Notes for Java/C Engineers
///
/// `manifest: &Manifest` is a temporary shared borrow. In Java it resembles
/// receiving an object reference, but this function cannot retain it beyond
/// its declared lifetime. In C it resembles `const Manifest *`, with compiler
/// guarantees that the reference is non-null and remains valid during use.
///
/// The returned `SegmentRef` is owned. The caller can keep it after the borrow
/// ends because this function copies or clones only the descriptor data it
/// needs; it does not return a pointer into `manifest`.
```

Explain the actual data cost. Do not casually call `.clone()` a copy: cloning
an `Arc<T>` increments a reference count, cloning `Bytes` shares a buffer, and
cloning `Vec<T>` allocates and clones its elements. Note intentional allocation
or refcount work on performance-sensitive paths.

Avoid repeating the same ownership lesson on every method in a file. Put the
shared explanation in the module or type documentation, then link back to it
from items whose contracts depend on it.

## ASCII diagrams

Use ASCII diagrams when a relationship is materially faster to understand
visually than through prose. Strong candidates include:

- overall architecture and layer boundaries;
- query and write paths;
- manifest CAS publication and artifact visibility;
- WAL-to-segment compaction;
- memory to disk to S3 cache lookup;
- metadata bitmap filtering followed by ANN search;
- BM25 and vector score fusion;
- lease acquisition, fencing, takeover, and release;
- ownership transfer, borrow lifetime, and usable values after a move;
- spawned task, channel, lock, and shared-state relationships;
- error branches whose partial state matters.

Put diagrams in fenced `text` blocks so rustdoc does not compile them as Rust.
Prefer top-to-bottom flow, keep them readable in a terminal, and label authority,
cache, visibility, and failure boundaries.

```text
upload immutable WAL fragment
              |
              | object exists, but readers cannot see it
              v
publish manifest with ETag CAS -------- CAS miss
              |                            |
              | success                    v
              v                      reload; do not overwrite
readers discover fragment
```

A Rust-focused diagram can make ownership concrete:

```text
caller owns Manifest
       |
       | temporary shared borrow: &Manifest
       v
build_descriptor(...)
       |
       | returns owned SegmentRef; borrow ends
       v
caller can keep and use both owned values
```

Do not draw diagrams for trivial accessors, one-step conversions, or a flow
already clearer in one sentence. Diagrams describe stable relationships and
lifecycle, not individual statements. Update or remove them when code changes.

## Intra-doc links and navigation

Use intra-doc links to connect contracts across module boundaries:

- `[`Type`]` for an item in scope;
- `[`module::Type`]` or `[`crate::module::Type`]` when qualification removes
  ambiguity;
- `[`method`][Type::method]` when descriptive link text reads better;
- links from thin HTTP handlers to the domain operation they expose;
- links from cache views to the authoritative storage type;
- links from implementations to the trait contract they preserve.

Link the first useful mention, not every occurrence. Prefer links over copied
explanations; centralized invariants are easier to keep current. Never guess a
path: build rustdoc with broken-link checking enabled.

## Zeppelin truths comments must preserve

Documentation describes current code, but every local explanation should make
the relevant architectural consequence explicit:

- **Fail loudly.** Do not describe a fallback or default unless the code and
  configuration contract intentionally provide one. Errors are not silent
  degradation.
- **S3/MinIO is authoritative.** Memory and local disk are disposable caches.
  Higher layers access object storage through `src/storage/`.
- **The manifest controls visibility.** An uploaded artifact is not visible
  merely because its object exists. A conditional manifest publication makes
  it visible.
- **Artifacts are immutable.** WAL fragments and segments are write-once;
  replacement happens by creating artifacts and publishing a new manifest.
- **Concurrency has explicit defenses.** Document the current single-writer
  assumption and, where implemented, both lease/fencing checks and ETag CAS.
- **Async work must not block runtime threads.** Describe task spawning,
  cancellation, shared state, and any lock held across `.await`.
- **The type system carries invariants.** Explain newtypes, enums, and
  `#[must_use]` where they prevent invalid states or ignored results.

Explain subsystem-specific concepts where they occur:

- Storage: GET/PUT/list/range behavior, retries, idempotency, and roundtrips.
- WAL: immutable fragments, checksums, ordering, replay, corruption, and the
  gap between upload and publication.
- Segments: contents, indexing role, compaction origin, and visibility.
- Compaction: fragment selection, CPU isolation, centroid reuse, output
  creation, publication, and cleanup.
- Indexing: define IVF-Flat, hierarchy, quantization, centroids, clusters,
  `nprobe`, recall, latency, and size tradeoffs before relying on those terms.
- Retrieval: vector search, bitmap prefiltering, BM25, hybrid score fusion, and
  result merging.
- Cache: memory/disk/S3 lookup order, stale versus authoritative state, pinned
  centroids, hydration, and speculative prefetch.
- HTTP: request/response meaning, idempotency, validation, rate limiting, and
  error mapping; link to domain logic instead of duplicating it.

When persisted types use serde, document format compatibility and obey the
repository's serialization constraints. In particular, a type tree containing
`#[serde(untagged)]` or `#[serde(skip_serializing_if)]` requires a
self-describing format, and checksum input must be deterministic.

## Good and bad comments

Bad comments repeat syntax, omit the domain contract, or promise behavior the
code does not establish:

```rust
/// Gets data.
fn get_data(key: &str) -> Result<Bytes> { /* ... */ }

// Loop over segments.
for segment in segments { /* ... */ }
```

Better comments identify authority, result meaning, cost, and intent:

```rust
/// Loads the immutable segment bytes referenced by the current manifest.
///
/// # Parameters
///
/// - `key`: Object-store key copied from a published segment descriptor. This
///   is an S3 key, not an HTTP namespace path.
///
/// # Returns
///
/// Shared [`Bytes`] containing the complete segment object.
///
/// # Errors
///
/// Returns an error when the object is missing or the storage request fails.
/// Missing data is an invariant violation; this function does not substitute
/// cached or empty bytes.
///
/// # Performance
///
/// Performs one object-store GET after a cache miss.
```

An implementation comment should capture a non-obvious reason:

```rust
// Drop the guard before awaiting S3 so unrelated cache reads can continue.
drop(cache_guard);
let bytes = store.get(key).await?;
```

Do not add a comment that claims this property unless the guard really is
released and the types permit the described behavior.

## Accuracy and uncertainty

Read the implementation, callers, tests, persisted structures, and error
mapping before documenting behavior. Comments describe what the code does now,
not an intended future architecture.

If behavior remains genuinely unclear, leave a specific marker rather than
inventing a fact:

```rust
/// TODO(doc): Verify whether a timed-out publication can leave an object that
/// became authoritative even though this caller observed an error.
```

Use `TODO(doc)` sparingly and report every occurrence for maintainer review.
When code changes, update nearby examples and diagrams in the same change.

## Tests and support code

Test documentation should explain the property being protected and the failure
it would catch, not restate the test name. Test helpers document setup,
ownership, cleanup, and backend assumptions. In particular:

- integration tests use the shared `TestHarness` and real S3 or MinIO;
- each test's random prefix provides object-store isolation;
- a returned `TempDir` handle must remain alive while its path is in use;
- vector fixtures used for merge or dedup scenarios require unique ID prefixes;
- assertions should explain the authoritative S3 state they verify.

Document benchmarks in terms of the workload and measured boundary. Document
fuzz and property tests in terms of generated input, invariant, and minimized
failure. Document concurrency tests in terms of actors, allowed outcomes, and
forbidden interleavings.

## Review checklist

For each in-scope file:

- [ ] The `//!` overview answers where the reader is, who calls this module,
      what it calls, which state it touches, and where reading should start.
- [ ] Large files have an intra-doc-linked `## Reading map`.
- [ ] Every item and every function, including private and test helpers, is
      documented at proportional depth.
- [ ] Parameters, return values, errors, panics, side effects, consistency, and
      cost are explicit where applicable.
- [ ] Non-trivial functions have high-level normal examples and important
      failure examples.
- [ ] Diagram applicability was considered; applicable architecture, flow,
      ownership, and concurrency diagrams are present.
- [ ] Intelligent Rust usage has a contextual Java/C learning note, without
      repetitive language-tutorial material.
- [ ] Intra-doc links connect important types, callers, and downstream flows.
- [ ] Comments preserve Zeppelin's authority, immutability, and fail-loud
      rules and match the current implementation.
- [ ] No runtime behavior changed as part of the documentation pass.
- [ ] Every `TODO(doc)` is specific and included in the final report.

## Lints and validation

The crate currently denies missing public docs. The desired crate-level policy
is:

```rust
#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![warn(clippy::missing_errors_doc)]
#![warn(clippy::missing_panics_doc)]
#![deny(clippy::missing_safety_doc)]
```

Introduce new warnings before making them errors if the repository is not yet
clean. Do not weaken an existing denial merely to make a documentation pass
green.

Run the following from the repository root:

```sh
cargo fmt --check
cargo doc --no-deps
cargo doc --no-deps --document-private-items
cargo test --doc
cargo clippy --all-targets -- -D warnings
```

`cargo doc --document-private-items` is essential because Zeppelin requires
documentation for private implementation seams as well as public APIs.
`cargo test --doc` compiles runnable examples. The two rustdoc builds catch
different navigation and private-item problems; inspect every warning rather
than treating a successful exit alone as proof that a comment is accurate.

The completion report for each documentation run records:

- files and modules documented;
- major invariants clarified;
- reading maps and ASCII diagrams added;
- Rust concepts explained for Java/C engineers;
- normal and failure flows illustrated with examples;
- validation commands and results;
- all remaining `TODO(doc)` questions.
