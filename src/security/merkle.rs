//! Canonical Merkle roots and inclusion paths for immutable object artifacts.

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
