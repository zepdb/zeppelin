//! Canonical Merkle roots and inclusion paths for immutable object artifacts.
//!
//! This file owns one cryptographic primitive: a deterministic binary Merkle
//! tree built over a *sorted inventory of object-store keys and their SHA-256
//! content hashes*. It produces the root digest a manifest signs, the inclusion
//! path a retrieval receipt carries for each artifact it touched, and the
//! verification routine that re-checks such a path against an independently
//! obtained root.
//!
//! Its purpose in Zeppelin is verifiable retrieval. Because WAL fragments and
//! segments are write-once, a key's content hash never changes, so a single
//! 32-byte root pins an exact set of immutable objects. A client that trusts the
//! signed manifest root can then verify — without trusting the server that
//! answered the query — that every object cited in a receipt really belonged to
//! the namespace's published state.
//!
//! It deliberately does **not** own:
//!
//! - *what belongs in the inventory* — `RetrievalScope::receipt_artifacts`
//!   decides which reachable keys are in scope;
//! - *computing content hashes* — those come from the storage layer as objects
//!   are written and are recorded in the manifest;
//! - *signing or publishing the root* — `wal/manifest.rs` signs the root together
//!   with the manifest version, fencing token, and binding version, and manifest
//!   publication is what makes it authoritative;
//! - *receipt policy* — issuing, binding, and divergence classification live in
//!   `receipt.rs`; this module has no notion of principals or policy.
//!
//! ## Where this sits
//!
//! ```text
//!   manifest recompute (wal/manifest.rs)
//!        artifact_hashes: BTreeMap<key, sha256>
//!                 |
//!                 v
//!        MerkleTree::build  --> root() --> Manifest::merkle_root, then signed
//!                 |
//!                 | MerkleTree::proof(key)
//!                 v
//!        MerklePath -----> TouchedArtifact in a RetrievalReceipt
//!                 |
//!                 |   client, or POST /v1/verify
//!                 v
//!        MerklePath::verify(key, content_hash, root) -> bool
//! ```
//!
//! ## Reading map
//!
//! 1. `leaf_hash` and `parent_hash` — the two domain-separated hash rules. Every
//!    property of this module follows from them.
//! 2. [`MerkleTree::build`] — level-by-level construction over the sorted keys.
//! 3. [`MerkleTree::proof`] — sibling collection from leaf to root.
//! 4. [`MerklePath::verify`] — the only part of this file an untrusting verifier
//!    needs to run.
//!
//! ## Construction rules
//!
//! ```text
//!   leaf   = SHA256( 0x00 || len(key) as u64 big-endian || key || content_hash )
//!   parent = SHA256( 0x01 || left || right )
//!
//!   keys sorted:  "a/0"   "a/1"   "b/0"        (odd level: last node self-pairs)
//!                   L0      L1      L2
//!                    \      /        |\
//!                     P(L0,L1)     P(L2,L2)
//!                          \        /
//!                           \      /
//!                            root
//! ```
//!
//! The `0x00`/`0x01` tag bytes are domain separation: a leaf digest can never be
//! reinterpreted as an internal node, or vice versa. The big-endian length prefix
//! prevents two different `(key, hash)` pairs from producing the same
//! concatenation — without it, distinct keys could be made to agree by shifting
//! bytes across the boundary.
//!
//! ## Invariants
//!
//! - **Determinism.** The input is a [`BTreeMap`], so leaves are always in sorted
//!   key order regardless of insertion order. This is the same rule that governs
//!   manifest checksums: never derive a digest from a `HashMap`, whose iteration
//!   order is not stable.
//! - **A path binds an exact key *and* an exact body.** [`MerklePath::verify`]
//!   recomputes the leaf from the caller-supplied key and content hash, so a valid
//!   path for one artifact cannot be replayed for another object or for modified
//!   bytes of the same object.
//! - **A path cannot be reordered or padded.** Each step's [`MerkleSide`] must
//!   match the parity of the running index, and the index must have been consumed
//!   down to `0` when the steps run out. A path with an extra step, a missing
//!   step, or a flipped side fails even if the final digest could otherwise be
//!   made to match.
//! - **Odd levels self-pair.** [`MerkleTree::build`] duplicates the last node when
//!   a level has an odd count, and [`MerkleTree::proof`] mirrors that by clamping
//!   the sibling index to the end of the level. The two must stay in step; a
//!   change to one without the other silently produces unverifiable proofs.
//! - **Empty inventories have a defined root.** An empty map yields one sentinel
//!   leaf, `SHA256("zeppelin-empty-merkle-v1")`, rather than a zero digest or a
//!   panic — so "no artifacts" is a stable, distinguishable state.
//!   [`MerkleTree::proof`] then returns `None` for every key, because no key is in
//!   the tree.
//! - **Empty keys are rejected loudly.** A key of length zero fails with
//!   `SecurityError::InvalidReceipt` rather than producing a degenerate leaf.
//! - **Structural violations panic rather than degrade.** [`MerkleTree::root`] and
//!   the level loop in [`MerkleTree::build`] panic if a constructed tree has no
//!   level or no root. Those states are unreachable by construction; substituting
//!   a default digest would let a corrupted tree produce a plausible-looking root,
//!   which is exactly the silent degradation this repository forbids.
//! - **The root is only meaningful once published.** `wal/manifest.rs` clears
//!   `merkle_root` when any reachable artifact still lacks a known content hash,
//!   and receipt issuance refuses to proceed unless the manifest's stored root
//!   equals a freshly rebuilt one. An object existing in storage does not put it
//!   under a root; a published manifest does.
//!
//! ## Rust concepts used here
//!
//! **`[u8; 32]` as the digest type.** A fixed-size array is `Copy` and lives on
//! the stack, so digests are passed by value with no allocation and no ownership
//! question. Unlike a Java `byte[]` or a C `unsigned char *` plus a length, the
//! size is part of the type: `parent_hash(&left, &right)` cannot be handed a
//! 20-byte digest, and `==` compares all 32 bytes rather than object identity or
//! a pointer. `Sha256::finalize().into()` performs the conversion from the
//! digest crate's `GenericArray` with the length checked at compile time.
//!
//! **[`BTreeMap`] as an ordering guarantee, not a convenience.** The nearest Java
//! analogue is `TreeMap`, but here the choice is load-bearing: it is the type
//! system, rather than a comment or a sort call the caller might forget, that
//! makes the digest reproducible across processes and machines.
//!
//! **Borrowed input, owned output.** [`MerkleTree::build`] takes
//! `&BTreeMap<String, [u8; 32]>` — a temporary shared borrow that it cannot
//! outlive — and returns a tree owning cloned keys and freshly computed digests.
//! The caller keeps its inventory and may drop the tree independently. Cloning the
//! key strings is a real allocation per artifact, paid once per manifest
//! recompute so that [`MerkleTree::proof`] can binary-search them afterwards.
//!
//! **`#[must_use]` on `verify`.** [`MerklePath::verify`] returns a plain `bool`.
//! Marking it `#[must_use]` means a caller that computes a verification result and
//! then ignores it will not compile cleanly — a discarded security check is a
//! failure mode the compiler can catch.
//!
//! **`#[serde(deny_unknown_fields)]` on the wire types.** [`MerkleStep`] and
//! [`MerklePath`] reject a proof carrying fields this build does not understand
//! instead of ignoring them, and [`MerkleSide`] serializes as the stable
//! `"left"`/`"right"` spellings. Both are compatibility surfaces: they travel
//! inside persisted receipts.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::SecurityError;

/// Which side of the current digest one sibling occupies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MerkleSide {
    /// The sibling is hashed before the current digest.
    Left,
    /// The sibling is hashed after the current digest.
    Right,
}

/// One sibling step in a canonical binary Merkle inclusion path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MerkleStep {
    /// Sibling digest at this tree level.
    pub hash: [u8; 32],
    /// Sibling position relative to the current digest.
    pub side: MerkleSide,
}

/// Proof that one exact object key and content digest belong to a root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MerklePath {
    /// Canonical leaf position in sorted-key order.
    pub leaf_index: usize,
    /// Sibling steps from leaf to root.
    pub steps: Vec<MerkleStep>,
}

impl MerklePath {
    /// Verify this path against an exact key, content hash, and expected root.
    #[must_use]
    pub fn verify(&self, key: &str, content_hash: &[u8; 32], root: &[u8; 32]) -> bool {
        let mut current = leaf_hash(key, content_hash);
        let mut index = self.leaf_index;
        for step in &self.steps {
            let expected_side = if index % 2 == 0 {
                MerkleSide::Right
            } else {
                MerkleSide::Left
            };
            if step.side != expected_side {
                return false;
            }
            current = match step.side {
                MerkleSide::Left => parent_hash(&step.hash, &current),
                MerkleSide::Right => parent_hash(&current, &step.hash),
            };
            index /= 2;
        }
        index == 0 && &current == root
    }
}

/// Canonical binary Merkle tree over a sorted artifact-key inventory.
#[derive(Debug, Clone)]
pub struct MerkleTree {
    keys: Vec<String>,
    levels: Vec<Vec<[u8; 32]>>,
}

impl MerkleTree {
    /// Build a tree from exact object keys to SHA-256 content hashes.
    pub fn build(artifacts: &BTreeMap<String, [u8; 32]>) -> Result<Self, SecurityError> {
        if artifacts.keys().any(|key| key.is_empty()) {
            return Err(SecurityError::InvalidReceipt(
                "merkle artifact keys must not be empty".to_string(),
            ));
        }

        let keys = artifacts.keys().cloned().collect::<Vec<_>>();
        let mut leaves = artifacts
            .iter()
            .map(|(key, content_hash)| leaf_hash(key, content_hash))
            .collect::<Vec<_>>();
        if leaves.is_empty() {
            leaves.push(Sha256::digest(b"zeppelin-empty-merkle-v1").into());
        }
        let mut levels = vec![leaves];
        while levels.last().map_or(0, Vec::len) > 1 {
            let current = levels
                .last()
                .unwrap_or_else(|| panic!("nonempty Merkle tree lost its current level"));
            let mut next = Vec::with_capacity(current.len().div_ceil(2));
            for pair in current.chunks(2) {
                let left = pair[0];
                let right = pair.get(1).copied().unwrap_or(left);
                next.push(parent_hash(&left, &right));
            }
            levels.push(next);
        }
        Ok(Self { keys, levels })
    }

    /// Return the root digest.
    #[must_use]
    pub fn root(&self) -> [u8; 32] {
        self.levels
            .last()
            .and_then(|level| level.first())
            .copied()
            .unwrap_or_else(|| panic!("constructed Merkle tree must have one root"))
    }

    /// Build the inclusion path for one exact input key.
    #[must_use]
    pub fn proof(&self, key: &str) -> Option<MerklePath> {
        let mut index = self
            .keys
            .binary_search_by(|candidate| candidate.as_str().cmp(key))
            .ok()?;
        let leaf_index = index;
        let mut steps = Vec::with_capacity(self.levels.len().saturating_sub(1));
        for level in self.levels.iter().take(self.levels.len().saturating_sub(1)) {
            let (sibling_index, side) = if index % 2 == 0 {
                ((index + 1).min(level.len() - 1), MerkleSide::Right)
            } else {
                (index - 1, MerkleSide::Left)
            };
            steps.push(MerkleStep {
                hash: level[sibling_index],
                side,
            });
            index /= 2;
        }
        Some(MerklePath { leaf_index, steps })
    }
}

fn leaf_hash(key: &str, content_hash: &[u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update([0_u8]);
    hasher.update((key.len() as u64).to_be_bytes());
    hasher.update(key.as_bytes());
    hasher.update(content_hash);
    hasher.finalize().into()
}

fn parent_hash(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update([1_u8]);
    hasher.update(left);
    hasher.update(right);
    hasher.finalize().into()
}
