//! Strong physical-artifact origin identities.
//!
//! Copy-on-write branching splits a question that used to have one answer: the
//! namespace a manifest *belongs to* is no longer necessarily the namespace an
//! artifact's bytes *live in*. This module owns the vocabulary for that second,
//! physical answer — which namespace lifetime actually stores a given immutable
//! WAL fragment or segment — plus the deterministic construction of the
//! per-manifest lookup table that encodes it.
//!
//! It owns nothing about how that table is stored or used. The persisted field,
//! the descriptor resolution, and the loud failures on a dangling index all live
//! in [`manifest::Manifest`](crate::wal::manifest::Manifest); object keys are built and fetched in
//! `src/storage/`. Nothing here performs I/O.
//!
//! ## Where this sits
//!
//! [`manifest::Manifest::artifact_origins`](crate::wal::manifest::Manifest::artifact_origins) is the canonical table;
//! each fragment and segment descriptor holds an optional
//! [`ArtifactOriginIndex`] into it. Manifest code builds that table through
//! `ArtifactOriginSetBuilder` (crate-private) when it normalizes a manifest or
//! prepares a zero-copy fork, and resolves it back into [`ArtifactOrigin`]
//! values for the read paths in `src/query.rs`, `src/retrieval_scope.rs`,
//! `src/cache/hydration.rs`, and `src/wal/fragment_cache.rs`.
//!
//! ## The absent-index rule
//!
//! ```text
//! descriptor.artifact_origin == None
//!     -> the bytes are owned by THIS manifest's own namespace lifetime
//!        ("local"); the key is built from the reading namespace.
//!
//! descriptor.artifact_origin == Some(index)
//!     -> the bytes live in artifact_origins[index], a possibly FOREIGN
//!        namespace lifetime; the key is built from that origin, and the
//!        reading namespace must not be substituted for it.
//! ```
//!
//! That single bit is what makes a fresh fork free. A new branch copies **no
//! artifacts**: its generation-1 manifest simply points every visible ref at the
//! source namespace's lifetime. The consequence is the cost that must be
//! budgeted elsewhere — the **first compaction of a foreign-backed branch fully
//! materializes it**, reading the entire logical view and writing target-owned
//! segments. Once every visible ref is `None` again the branch is materialized,
//! and later compactions are ordinary incremental local ones.
//!
//! ## Invariants
//!
//! - **An origin names a lifetime, not a name.** [`ArtifactOrigin`] pairs a
//!   `NamespaceId` with a `NamespaceIncarnationId` precisely so a
//!   deleted-and-recreated namespace of the same name can never silently satisfy
//!   a foreign reference. A nil incarnation is rejected at collection time
//!   rather than being treated as "unknown".
//! - **The table is canonical, so the manifest bytes are deterministic.**
//!   `ArtifactOriginSetBuilder` accumulates into a `BTreeSet` and assigns
//!   indices strictly in sorted order at `finish`. Two manifests describing the
//!   same set of origins therefore serialize identically and hash identically,
//!   regardless of the order descriptors were visited. This is the same
//!   canonicalize-before-you-digest rule the repository applies to every
//!   checksum input.
//! - **Index assignment is not stable across rebuilds.** Indices are positions
//!   in one manifest's frozen table, never durable identifiers. Anything that
//!   must survive across manifests carries a full [`ArtifactOrigin`], and the
//!   builder deliberately refuses to hand out an index before `finish`.
//! - **Failures are structural and loud.** Both a nil incarnation and a table
//!   that would exceed the `u32` index space return
//!   [`BranchError::ArtifactOriginInvalid`] with a secret-free diagnostic. There
//!   is no default origin and no "assume local" fallback; an unresolvable
//!   descriptor is an integrity failure, because guessing would read another
//!   tenant's objects or silently return nothing.
//! - **Origins describe immutable artifacts.** A published fragment or segment
//!   is write-once, so its origin never changes. Materialization does not
//!   rewrite an origin — it creates new target-owned artifacts and publishes a
//!   manifest whose refs no longer need one.
//!
//! ## Serialization
//!
//! Both public types are persisted inside the MessagePack manifest.
//! [`ArtifactOriginIndex`] is `#[serde(transparent)]`, so it encodes as a bare
//! `u32` with no wrapper overhead, and [`ArtifactOrigin`] encodes as its two
//! identity fields. Neither uses `skip_serializing_if`, which would be unsafe in
//! this format; the manifest's own `#[serde(default)]` on the table is what lets
//! pre-branching manifests decode with no origins at all.
//!
//! ## Rust concepts used here
//!
//! **A `Copy` newtype for the index.** [`ArtifactOriginIndex`] wraps a `u32` and
//! derives `Copy`, so passing one around is a register move with no allocation
//! and no borrow. Unlike a bare `u32` — or a C `typedef uint32_t` — it cannot be
//! confused with a cluster id or a vector count at a call site, and unlike a
//! Java `Integer` wrapper it has no boxing cost. Its constructors are `const fn`
//! so a fixed index can be built at compile time.
//!
//! **Ordered collections chosen for determinism, not for lookup speed.**
//! `BTreeSet` and `BTreeMap` are used here because iteration order is part of
//! the contract. A `HashSet`/`HashMap` would be faster and would still produce a
//! correct set, but its iteration order varies between runs, which would make
//! the resulting manifest bytes — and every checksum over them —
//! non-reproducible.
//!
//! **A consuming builder that enforces a phase change.** `collect` takes
//! `&mut self` while the set is still growing; `finish` takes `self` by value,
//! moving the builder and making it unusable afterwards. The compiler, not a
//! runtime flag, guarantees nobody adds an origin after indices were assigned.
//! Java would express this with a `build()` that flips an internal `built`
//! boolean and throws; Rust's move makes the misuse fail to compile.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::BranchError;
use crate::namespace::{NamespaceId, NamespaceIncarnationId};

/// Physical namespace lifetime that owns one immutable artifact.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArtifactOrigin {
    /// Validated namespace prefix containing the immutable object.
    pub namespace: NamespaceId,
    /// Exact lifetime of that namespace name.
    pub incarnation: NamespaceIncarnationId,
}

/// Compact index into a manifest's canonical physical-origin table.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct ArtifactOriginIndex(u32);

impl ArtifactOriginIndex {
    /// Wrap one already range-checked table index.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Return the persisted numeric index.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// First pass of deterministic origin-table construction.
#[derive(Debug, Default)]
#[allow(dead_code)] // Frozen in phase 02; fork normalization adopts it in phase 05.
pub(crate) struct ArtifactOriginSetBuilder {
    origins: BTreeSet<ArtifactOrigin>,
}

#[allow(dead_code, clippy::result_large_err)]
// The builder reports the complete typed integrity context required by Phase 02.
impl ArtifactOriginSetBuilder {
    /// Collect one ultimate physical owner without assigning an unstable index.
    pub(crate) fn collect(&mut self, origin: ArtifactOrigin) -> Result<(), BranchError> {
        if origin.incarnation.is_nil() {
            return Err(BranchError::ArtifactOriginInvalid {
                manifest_namespace: origin.namespace.as_str().to_string(),
                manifest_incarnation: Some(origin.incarnation.clone()),
                descriptor_kind: "manifest",
                descriptor_id: "artifact_origins".to_string(),
                offending_index: None,
                offending_key: None,
                expected_origin: Some(origin),
                reason: "namespace incarnation is nil".to_string(),
            });
        }
        self.origins.insert(origin);
        Ok(())
    }

    /// Freeze sorted origins and assign their final stable indices.
    pub(crate) fn finish(self) -> Result<CanonicalArtifactOrigins, BranchError> {
        if self.origins.len() > u32::MAX as usize {
            return Err(BranchError::ArtifactOriginInvalid {
                manifest_namespace: "<unbound>".to_string(),
                manifest_incarnation: None,
                descriptor_kind: "manifest",
                descriptor_id: "artifact_origins".to_string(),
                offending_index: None,
                offending_key: None,
                expected_origin: None,
                reason: "origin table exceeds u32 address space".to_string(),
            });
        }

        let table: Vec<_> = self.origins.into_iter().collect();
        let mut indices = BTreeMap::new();
        for (index, origin) in table.iter().cloned().enumerate() {
            let index = u32::try_from(index).map_err(|_| BranchError::ArtifactOriginInvalid {
                manifest_namespace: "<unbound>".to_string(),
                manifest_incarnation: None,
                descriptor_kind: "manifest",
                descriptor_id: "artifact_origins".to_string(),
                offending_index: None,
                offending_key: None,
                expected_origin: Some(origin.clone()),
                reason: "origin table exceeds u32 address space".to_string(),
            })?;
            indices.insert(origin, ArtifactOriginIndex::new(index));
        }
        Ok(CanonicalArtifactOrigins { table, indices })
    }
}

/// Frozen deterministic table plus its origin-to-final-index map.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)] // Frozen in phase 02; fork normalization adopts it in phase 05.
pub(crate) struct CanonicalArtifactOrigins {
    pub(crate) table: Vec<ArtifactOrigin>,
    pub(crate) indices: BTreeMap<ArtifactOrigin, ArtifactOriginIndex>,
}
