//! Static registry of persisted object and embedded artifact formats.
//!
//! The registry is deliberately data-only: it performs no object-store I/O,
//! consults no process configuration, and can be exercised by `cargo test
//! --lib` on every backend. Version probes call the production decoder (or the
//! exact header-validation seam used by a decoder whose full validation also
//! needs a manifest reference).
//!
//! A version `0` in an accepted-version list denotes a historical unversioned
//! layout that the production decoder still recognizes. An em dash denotes a
//! format with no version discriminator at all.
//!
//! | Family | Magic | Current / accepted | Encoding | Binding input |
//! | --- | --- | --- | --- | --- |
//! | `namespace_metadata` | — | — | JSON | no |
//! | `manifest` | leading byte | 1 / 1 (+ legacy JSON) | positional MessagePack | yes |
//! | `namespace_lease` | — | — | JSON | no |
//! | `named_snapshot` | leading byte | 1 / 1 (+ legacy JSON) | positional MessagePack | no |
//! | `wal_fragment` | leading byte | 1 / 1 (+ legacy JSON) | positional MessagePack | yes |
//! | `input_wal_fragment` | `ZIW1` | 1 / 1 | positional MessagePack | yes |
//! | `source_payload` | — | — | opaque binary | no |
//! | `ivf_bootstrap` | `ZBS1` | 3 / 1, 2, 3 | binary | yes |
//! | `ivf_centroids` | `ZCT2` | 2 / 0, 2 | binary | no |
//! | `cluster_data_object` | `ZBP5` | 5 / 0, 1, 4, 5 | binary | yes |
//! | `cluster_vector_section` | `ZCL3` | 3 / 0, 2, 3 | binary | no |
//! | `cluster_id_block` | — | — | binary | no |
//! | `cluster_f32_block` | — | — | binary | no |
//! | `ivf_membership` | `ZMB1` | 1 / 1 | binary | no |
//! | `resident_sketch` | `ZSK1` | 4 / 2, 3, 4 | binary | yes |
//! | `filter_cardinality_summary` | `ZFCS` | 1 / 1 | positional MessagePack | no |
//! | `cluster_bitmap_index` | `ZBMP` | 1 / 1 | JSON | no |
//! | `rabitq_cluster` | `ZRQ1` | 1 / 1 | binary | no |
//! | `rabitq_codes_only` | — | — | binary | no |
//! | `cluster_attributes` | — | — | positional MessagePack | no |
//! | `sq8_calibration` | — | — | binary | no |
//! | `sq8_cluster` | — | — | binary | no |
//! | `sq8_codes_only` | — | — | binary | no |
//! | `pq_codebook` | — | — | binary | no |
//! | `pq_cluster` | — | — | binary | no |
//! | `hierarchical_tree_metadata` | — | — | JSON | no |
//! | `hierarchical_tree_node` | — | — | binary | no |
//! | `fts_cluster_index` | `ZFTS` | 1 / 1 | positional MessagePack | no |
//! | `fts_global_index` | `ZGFTS` | 1 / 1 | positional MessagePack | no |
//! | `fts_segment_metadata` | — | — | JSON | no |
//! | `scoped_ann_descriptor` | JSON field | 4 / 4 | JSON | no |
//! | `scoped_fts_index` | `ZSFT1` | 1 / 1 | JSON | no |
//! | `late_state_section` | `ZLS1` | 5 / 1, 2, 3, 4, 5 | positional MessagePack | yes |
//! | `matrix_fragment` | `ZME1` | 1 / 1 | binary | no |
//! | `fde_fragment` | `ZFD1` | 1 / 1 | binary | no |
//! | `fde_transform` | `ZFT1` | 1 / 1 | binary | no |
//! | `centering` | `ZCM1` | 1 / 1 | binary | no |
//! | `quarantine_evidence` | `ZEQ1` | 1 / 1 | named MessagePack | no |
//! | `late_candidate_bootstrap` | `ZLB1` | 1 / 1 | named MessagePack | no |
//! | `late_candidate_cluster` | `ZLC1` | 1 / 1 | named MessagePack | no |
//! | `late_flat_candidate` | `ZFQ1` | 1 / 1 | binary | no |
//! | `late_matrix_block` | `ZMB1` | 1 / 1 | binary | no |
//! | `late_attribute_block` | `ZAB1` | 1 / 1 | binary | no |
//! | `compaction_staging` | — | — | JSON | no |
//! | `gc_candidate_ledger` | JSON field | 1 / 0, 1 | JSON | no |
//! | `branch_visibility_marker` | — | — | JSON | no |
//! | `security_policy_head` | — | — | JSON | no |
//! | `security_policy_snapshot` | — | — | JSON | no |
//! | `security_audit_jsonl` | — | — | JSON Lines | no |
//! | `namespace_destruction_record` | — | — | JSON | no |

/// Release-corpus generation and deep validation seams.
///
/// Kept here so integration tests enumerate and exercise the registry without
/// re-declaring persisted wire shapes.
#[doc(hidden)]
pub mod fixture;

use crate::compaction::gc::{probe_gc_candidate_ledger, GC_CANDIDATE_STORE_VERSION};
use crate::embedding::artifact::{CENTERING_MAGIC, FDE_MAGIC, MATRIX_MAGIC};
use crate::embedding::coordinator::{
    probe_quarantine_evidence_format, QUARANTINE_EVIDENCE_MAGIC, QUARANTINE_EVIDENCE_VERSION,
};
use crate::embedding::{
    ArtifactChecksum, CenteringArtifact, FdeArtifact, FdeGenerationId, MatrixArtifact, MatrixDtype,
    MultiVectorEpochId, CENTERING_ARTIFACT_FORMAT_VERSION, FDE_ARTIFACT_FORMAT_VERSION,
    MATRIX_ARTIFACT_FORMAT_VERSION,
};
use crate::error::{Result, ZeppelinError};
use crate::fts::global_index::{GlobalInvertedIndex, ZGFTS_MAGIC, ZGFTS_VERSION};
use crate::fts::inverted_index::{InvertedIndex, FTS_INDEX_FORMAT_VERSION, ZFTS_MAGIC};
use crate::index::bitmap::{ClusterBitmapIndex, BITMAP_MAGIC, BITMAP_VERSION};
use crate::index::ivf_flat::build::{
    cluster_object_sections, deserialize_bootstrap, deserialize_centroids_data,
    deserialize_cluster, BOOTSTRAP_MAGIC, BOOTSTRAP_VERSION, BOOTSTRAP_VERSION_V1,
    BOOTSTRAP_VERSION_V2, CENTROIDS_V2_MAGIC, CLUSTER_DATA_OBJECT_V4_VERSION,
    CLUSTER_DATA_OBJECT_V5_VERSION, CLUSTER_V3_MAGIC,
};
use crate::index::ivf_flat::filter_summary::{
    FilterCardinalitySummary, FILTER_SUMMARY_MAGIC, FILTER_SUMMARY_VERSION,
};
use crate::index::ivf_flat::membership::{
    deserialize_membership, MEMBERSHIP_MAGIC, MEMBERSHIP_VERSION,
};
use crate::index::ivf_flat::sketch::{
    ResidentSketch, SKETCH_MAGIC, SKETCH_V2_VERSION, SKETCH_V3_VERSION, SKETCH_VERSION,
};
use crate::index::late_interaction::{
    probe_attribute_block_format, probe_candidate_bootstrap_format, probe_candidate_cluster_format,
    probe_flat_candidate_format, probe_matrix_block_format, FdeTransform,
    ATTRIBUTE_BLOCK_FORMAT_VERSION, ATTRIBUTE_BLOCK_MAGIC,
    BOOTSTRAP_MAGIC as LATE_CANDIDATE_BOOTSTRAP_MAGIC,
    CLUSTER_MAGIC as LATE_CANDIDATE_CLUSTER_MAGIC, FDE_TRANSFORM_FORMAT_VERSION, FLAT_MAGIC,
    LATE_CANDIDATE_FORMAT_VERSION, LATE_FLAT_FORMAT_VERSION, MATRIX_BLOCK_FORMAT_VERSION,
    MATRIX_BLOCK_MAGIC, TRANSFORM_MAGIC,
};
use crate::index::quantization::rq::{RqClusterCodes, RQ_MAGIC, RQ_VERSION};
use crate::retrieval_scope::{
    probe_scoped_ann_descriptor, probe_scoped_fts_artifact, SCOPED_ANN_DESCRIPTOR_VERSION,
    SCOPED_FTS_ARTIFACT_VERSION, SCOPED_FTS_MAGIC,
};
use crate::storage::NamespaceObjectFamily;
use crate::wal::fragment::{WalFragment, WAL_FORMAT_MSGPACK};
use crate::wal::input_fragment::{EncoderInputWalFragment, INPUT_WAL_MAGIC, INPUT_WAL_VERSION};
use crate::wal::late_section::{
    LateStateSection, LATE_STATE_FORMAT_VERSION, LATE_STATE_MAGIC, LATE_STATE_VERSION_V1,
    LATE_STATE_VERSION_V2, LATE_STATE_VERSION_V3, LATE_STATE_VERSION_V4, LATE_STATE_VERSION_V5,
};
use crate::wal::manifest::{Manifest, NamedSnapshot, MANIFEST_FORMAT_MSGPACK};

/// Serialization vocabulary used by one persisted family.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encoding {
    /// Serde JSON object or array.
    Json,
    /// One JSON object per newline-delimited record.
    JsonLines,
    /// MessagePack encoded as positional fields.
    MessagePackPositional,
    /// MessagePack encoded with named fields.
    MessagePackNamed,
    /// Hand-written fixed or length-prefixed binary layout.
    Binary,
    /// Caller-defined bytes with no Zeppelin schema.
    Opaque,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VersionHeader {
    Unversioned,
    LeadingU8,
    AfterMagicU8,
    AfterMagicU32Le,
    MagicAsciiSuffix,
    JsonField,
    JsonFieldAfterMagic,
}

/// One persisted artifact family registered against its real decoder.
#[cfg_attr(not(test), allow(dead_code))]
pub struct FormatFamily {
    /// Stable registry name used by tests and the future golden corpus.
    pub name: &'static str,
    /// Namespace key families where this codec can occur; empty means global.
    pub(crate) key_families: &'static [NamespaceObjectFamily],
    /// Fixed wire magic. An empty slice means versioned framing without magic.
    pub magic: Option<&'static [u8]>,
    /// Version written by the current encoder, or zero for unversioned formats.
    pub current_version: u32,
    /// Numeric versions the current decoder accepts; zero denotes a legacy layout.
    pub accepted_versions: &'static [u32],
    /// Serialization vocabulary used after any discriminator.
    pub encoding: Encoding,
    /// Whether this family contributes metadata to a manifest binding projection.
    pub checksum_input: bool,
    /// Production decoder or production-shared version-dispatch probe.
    pub decode_probe: fn(&[u8]) -> Result<()>,
    version_header: VersionHeader,
    /// Why an unversioned family cannot participate in a future-version probe.
    pub probe_note: &'static str,
}

const NONE: &[NamespaceObjectFamily] = &[];
const METADATA: &[NamespaceObjectFamily] = &[NamespaceObjectFamily::Metadata];
const MANIFESTS: &[NamespaceObjectFamily] = &[
    NamespaceObjectFamily::Manifest,
    NamespaceObjectFamily::ManifestHistory,
];
const LEASE: &[NamespaceObjectFamily] = &[NamespaceObjectFamily::Lease];
const SNAPSHOT: &[NamespaceObjectFamily] = &[NamespaceObjectFamily::Snapshot];
const WAL: &[NamespaceObjectFamily] = &[NamespaceObjectFamily::Wal];
const INPUT_WAL: &[NamespaceObjectFamily] = &[NamespaceObjectFamily::InputWal];
const SOURCE: &[NamespaceObjectFamily] = &[NamespaceObjectFamily::Source];
const SEGMENT: &[NamespaceObjectFamily] = &[NamespaceObjectFamily::Segment];
const SEGMENT_AND_LATE: &[NamespaceObjectFamily] = &[
    NamespaceObjectFamily::Segment,
    NamespaceObjectFamily::LateSegment,
];
const LATE_STATE: &[NamespaceObjectFamily] = &[NamespaceObjectFamily::LateSection];
const MATRIX_FRAGMENT: &[NamespaceObjectFamily] = &[NamespaceObjectFamily::MatrixFragment];
const FDE_FRAGMENT: &[NamespaceObjectFamily] = &[NamespaceObjectFamily::FdeFragment];
const FDE_TRANSFORM: &[NamespaceObjectFamily] = &[NamespaceObjectFamily::FdeTransform];
const CENTERING: &[NamespaceObjectFamily] = &[NamespaceObjectFamily::Centering];
const QUARANTINE: &[NamespaceObjectFamily] = &[NamespaceObjectFamily::Quarantine];
const LATE_SEGMENT: &[NamespaceObjectFamily] = &[NamespaceObjectFamily::LateSegment];
const STAGING: &[NamespaceObjectFamily] = &[NamespaceObjectFamily::Staging];
const GC: &[NamespaceObjectFamily] = &[NamespaceObjectFamily::Gc];
const BRANCH_VISIBILITY: &[NamespaceObjectFamily] =
    &[NamespaceObjectFamily::BranchVisibilityRemoved];

const V1: &[u32] = &[1];
const V4: &[u32] = &[4];
const LEGACY_V1: &[u32] = &[0, 1];
const BOOTSTRAP_VERSIONS: &[u32] = &[
    BOOTSTRAP_VERSION_V1,
    BOOTSTRAP_VERSION_V2,
    BOOTSTRAP_VERSION,
];
const CENTROID_VERSIONS: &[u32] = &[0, 2];
const CLUSTER_OBJECT_VERSIONS: &[u32] = &[
    0,
    1,
    CLUSTER_DATA_OBJECT_V4_VERSION as u32,
    CLUSTER_DATA_OBJECT_V5_VERSION as u32,
];
const CLUSTER_SECTION_VERSIONS: &[u32] = &[0, 2, 3];
const SKETCH_VERSIONS: &[u32] = &[SKETCH_V2_VERSION, SKETCH_V3_VERSION, SKETCH_VERSION];
const LATE_STATE_VERSIONS: &[u32] = &[
    LATE_STATE_VERSION_V1 as u32,
    LATE_STATE_VERSION_V2 as u32,
    LATE_STATE_VERSION_V3 as u32,
    LATE_STATE_VERSION_V4 as u32,
    LATE_STATE_VERSION_V5 as u32,
];
const GC_VERSIONS: &[u32] = &[0, GC_CANDIDATE_STORE_VERSION];

macro_rules! family {
    ($name:literal, $keys:expr, $magic:expr, $current:expr, $accepted:expr, $encoding:expr, $checksum:expr, $probe:expr, $header:expr, $note:literal) => {
        FormatFamily {
            name: $name,
            key_families: $keys,
            magic: $magic,
            current_version: $current,
            accepted_versions: $accepted,
            encoding: $encoding,
            checksum_input: $checksum,
            decode_probe: $probe,
            version_header: $header,
            probe_note: $note,
        }
    };
}

/// Complete registry of persisted artifact codecs known to this binary.
#[used]
pub static FORMATS: &[FormatFamily] = &[
    family!(
        "namespace_metadata",
        METADATA,
        None,
        0,
        &[],
        Encoding::Json,
        false,
        unversioned_probe,
        VersionHeader::Unversioned,
        "plain JSON has no format-version discriminator"
    ),
    family!(
        "manifest",
        MANIFESTS,
        Some(b""),
        MANIFEST_FORMAT_MSGPACK as u32,
        LEGACY_V1,
        Encoding::MessagePackPositional,
        true,
        probe_manifest,
        VersionHeader::LeadingU8,
        ""
    ),
    family!(
        "namespace_lease",
        LEASE,
        None,
        0,
        &[],
        Encoding::Json,
        false,
        unversioned_probe,
        VersionHeader::Unversioned,
        "plain JSON has no format-version discriminator"
    ),
    family!(
        "named_snapshot",
        SNAPSHOT,
        Some(b""),
        MANIFEST_FORMAT_MSGPACK as u32,
        LEGACY_V1,
        Encoding::MessagePackPositional,
        false,
        probe_named_snapshot,
        VersionHeader::LeadingU8,
        ""
    ),
    family!(
        "wal_fragment",
        WAL,
        Some(b""),
        WAL_FORMAT_MSGPACK as u32,
        LEGACY_V1,
        Encoding::MessagePackPositional,
        true,
        probe_wal_fragment,
        VersionHeader::LeadingU8,
        ""
    ),
    family!(
        "input_wal_fragment",
        INPUT_WAL,
        Some(INPUT_WAL_MAGIC),
        INPUT_WAL_VERSION as u32,
        V1,
        Encoding::MessagePackPositional,
        true,
        probe_input_wal,
        VersionHeader::AfterMagicU8,
        ""
    ),
    family!(
        "source_payload",
        SOURCE,
        None,
        0,
        &[],
        Encoding::Opaque,
        false,
        unversioned_probe,
        VersionHeader::Unversioned,
        "the source object is caller-defined content, not a Zeppelin envelope"
    ),
    family!(
        "ivf_bootstrap",
        SEGMENT,
        Some(BOOTSTRAP_MAGIC),
        BOOTSTRAP_VERSION,
        BOOTSTRAP_VERSIONS,
        Encoding::Binary,
        true,
        probe_bootstrap,
        VersionHeader::AfterMagicU32Le,
        ""
    ),
    family!(
        "ivf_centroids",
        SEGMENT,
        Some(CENTROIDS_V2_MAGIC),
        2,
        CENTROID_VERSIONS,
        Encoding::Binary,
        false,
        probe_centroids,
        VersionHeader::MagicAsciiSuffix,
        ""
    ),
    family!(
        "cluster_data_object",
        SEGMENT,
        Some(b"ZBP5"),
        CLUSTER_DATA_OBJECT_V5_VERSION as u32,
        CLUSTER_OBJECT_VERSIONS,
        Encoding::Binary,
        true,
        probe_cluster_object,
        VersionHeader::MagicAsciiSuffix,
        ""
    ),
    family!(
        "cluster_vector_section",
        SEGMENT,
        Some(CLUSTER_V3_MAGIC),
        3,
        CLUSTER_SECTION_VERSIONS,
        Encoding::Binary,
        false,
        probe_cluster_section,
        VersionHeader::MagicAsciiSuffix,
        ""
    ),
    family!(
        "cluster_id_block",
        SEGMENT,
        None,
        0,
        &[],
        Encoding::Binary,
        false,
        unversioned_probe,
        VersionHeader::Unversioned,
        "the positional ID block is identified only by its enclosing ZBP5 directory"
    ),
    family!(
        "cluster_f32_block",
        SEGMENT,
        None,
        0,
        &[],
        Encoding::Binary,
        false,
        unversioned_probe,
        VersionHeader::Unversioned,
        "the fixed-stride vector block is identified only by its enclosing ZBP5 directory"
    ),
    family!(
        "ivf_membership",
        SEGMENT,
        Some(MEMBERSHIP_MAGIC),
        MEMBERSHIP_VERSION,
        V1,
        Encoding::Binary,
        false,
        probe_membership,
        VersionHeader::AfterMagicU32Le,
        ""
    ),
    family!(
        "resident_sketch",
        SEGMENT,
        Some(SKETCH_MAGIC),
        SKETCH_VERSION,
        SKETCH_VERSIONS,
        Encoding::Binary,
        true,
        probe_sketch,
        VersionHeader::AfterMagicU32Le,
        ""
    ),
    family!(
        "filter_cardinality_summary",
        SEGMENT,
        Some(FILTER_SUMMARY_MAGIC),
        FILTER_SUMMARY_VERSION as u32,
        V1,
        Encoding::MessagePackPositional,
        false,
        probe_filter_summary,
        VersionHeader::AfterMagicU8,
        ""
    ),
    family!(
        "cluster_bitmap_index",
        SEGMENT,
        Some(BITMAP_MAGIC),
        BITMAP_VERSION as u32,
        V1,
        Encoding::Json,
        false,
        probe_bitmap,
        VersionHeader::AfterMagicU8,
        ""
    ),
    family!(
        "rabitq_cluster",
        SEGMENT,
        Some(RQ_MAGIC),
        RQ_VERSION as u32,
        V1,
        Encoding::Binary,
        false,
        probe_rq,
        VersionHeader::AfterMagicU8,
        ""
    ),
    family!(
        "rabitq_codes_only",
        SEGMENT,
        None,
        0,
        &[],
        Encoding::Binary,
        false,
        unversioned_probe,
        VersionHeader::Unversioned,
        "the codes-only payload is identified by the enclosing ZBP5 row layout"
    ),
    family!(
        "cluster_attributes",
        SEGMENT,
        None,
        0,
        &[],
        Encoding::MessagePackPositional,
        false,
        unversioned_probe,
        VersionHeader::Unversioned,
        "the attrs_<i>.bin positional payload has no header or version"
    ),
    family!(
        "sq8_calibration",
        SEGMENT,
        None,
        0,
        &[],
        Encoding::Binary,
        false,
        unversioned_probe,
        VersionHeader::Unversioned,
        "the SQ8 calibration payload begins directly with dimension"
    ),
    family!(
        "sq8_cluster",
        SEGMENT,
        None,
        0,
        &[],
        Encoding::Binary,
        false,
        unversioned_probe,
        VersionHeader::Unversioned,
        "the SQ8 row payload begins directly with row count and dimension"
    ),
    family!(
        "sq8_codes_only",
        SEGMENT,
        None,
        0,
        &[],
        Encoding::Binary,
        false,
        unversioned_probe,
        VersionHeader::Unversioned,
        "the SQ8 codes-only payload is identified by its enclosing cluster layout"
    ),
    family!(
        "pq_codebook",
        SEGMENT,
        None,
        0,
        &[],
        Encoding::Binary,
        false,
        unversioned_probe,
        VersionHeader::Unversioned,
        "the PQ codebook begins directly with M, K, and sub-dimension"
    ),
    family!(
        "pq_cluster",
        SEGMENT,
        None,
        0,
        &[],
        Encoding::Binary,
        false,
        unversioned_probe,
        VersionHeader::Unversioned,
        "the PQ row payload begins directly with row count and M"
    ),
    family!(
        "hierarchical_tree_metadata",
        SEGMENT,
        None,
        0,
        &[],
        Encoding::Json,
        false,
        unversioned_probe,
        VersionHeader::Unversioned,
        "tree_meta.json has no schema-version field"
    ),
    family!(
        "hierarchical_tree_node",
        SEGMENT,
        None,
        0,
        &[],
        Encoding::Binary,
        false,
        unversioned_probe,
        VersionHeader::Unversioned,
        "tree nodes begin directly with positional counts and dimensions"
    ),
    family!(
        "fts_cluster_index",
        SEGMENT_AND_LATE,
        Some(ZFTS_MAGIC),
        FTS_INDEX_FORMAT_VERSION as u32,
        V1,
        Encoding::MessagePackPositional,
        false,
        probe_fts,
        VersionHeader::AfterMagicU8,
        ""
    ),
    family!(
        "fts_global_index",
        SEGMENT,
        Some(ZGFTS_MAGIC),
        ZGFTS_VERSION as u32,
        V1,
        Encoding::MessagePackPositional,
        false,
        probe_global_fts,
        VersionHeader::AfterMagicU8,
        ""
    ),
    family!(
        "fts_segment_metadata",
        SEGMENT,
        None,
        0,
        &[],
        Encoding::Json,
        false,
        unversioned_probe,
        VersionHeader::Unversioned,
        "fts_meta.json has no schema-version field"
    ),
    family!(
        "scoped_ann_descriptor",
        SEGMENT,
        Some(b""),
        SCOPED_ANN_DESCRIPTOR_VERSION,
        V4,
        Encoding::Json,
        false,
        probe_scoped_ann_descriptor,
        VersionHeader::JsonField,
        ""
    ),
    family!(
        "scoped_fts_index",
        SEGMENT,
        Some(SCOPED_FTS_MAGIC),
        SCOPED_FTS_ARTIFACT_VERSION,
        V1,
        Encoding::Json,
        false,
        probe_scoped_fts_artifact,
        VersionHeader::JsonFieldAfterMagic,
        ""
    ),
    family!(
        "late_state_section",
        LATE_STATE,
        Some(LATE_STATE_MAGIC),
        LATE_STATE_FORMAT_VERSION,
        LATE_STATE_VERSIONS,
        Encoding::MessagePackPositional,
        true,
        probe_late_state,
        VersionHeader::AfterMagicU8,
        ""
    ),
    family!(
        "matrix_fragment",
        MATRIX_FRAGMENT,
        Some(MATRIX_MAGIC),
        MATRIX_ARTIFACT_FORMAT_VERSION as u32,
        V1,
        Encoding::Binary,
        false,
        probe_matrix_fragment,
        VersionHeader::AfterMagicU8,
        ""
    ),
    family!(
        "fde_fragment",
        FDE_FRAGMENT,
        Some(FDE_MAGIC),
        FDE_ARTIFACT_FORMAT_VERSION as u32,
        V1,
        Encoding::Binary,
        false,
        probe_fde_fragment,
        VersionHeader::AfterMagicU8,
        ""
    ),
    family!(
        "fde_transform",
        FDE_TRANSFORM,
        Some(TRANSFORM_MAGIC),
        FDE_TRANSFORM_FORMAT_VERSION as u32,
        V1,
        Encoding::Binary,
        false,
        probe_fde_transform,
        VersionHeader::AfterMagicU8,
        ""
    ),
    family!(
        "centering",
        CENTERING,
        Some(CENTERING_MAGIC),
        CENTERING_ARTIFACT_FORMAT_VERSION as u32,
        V1,
        Encoding::Binary,
        false,
        probe_centering,
        VersionHeader::AfterMagicU8,
        ""
    ),
    family!(
        "quarantine_evidence",
        QUARANTINE,
        Some(QUARANTINE_EVIDENCE_MAGIC),
        QUARANTINE_EVIDENCE_VERSION as u32,
        V1,
        Encoding::MessagePackNamed,
        false,
        probe_quarantine_evidence_format,
        VersionHeader::AfterMagicU8,
        ""
    ),
    family!(
        "late_candidate_bootstrap",
        LATE_SEGMENT,
        Some(LATE_CANDIDATE_BOOTSTRAP_MAGIC),
        LATE_CANDIDATE_FORMAT_VERSION,
        V1,
        Encoding::MessagePackNamed,
        false,
        probe_candidate_bootstrap_format,
        VersionHeader::AfterMagicU8,
        ""
    ),
    family!(
        "late_candidate_cluster",
        LATE_SEGMENT,
        Some(LATE_CANDIDATE_CLUSTER_MAGIC),
        LATE_CANDIDATE_FORMAT_VERSION,
        V1,
        Encoding::MessagePackNamed,
        false,
        probe_candidate_cluster_format,
        VersionHeader::AfterMagicU8,
        ""
    ),
    family!(
        "late_flat_candidate",
        LATE_SEGMENT,
        Some(FLAT_MAGIC),
        LATE_FLAT_FORMAT_VERSION,
        V1,
        Encoding::Binary,
        false,
        probe_flat_candidate_format,
        VersionHeader::AfterMagicU8,
        ""
    ),
    family!(
        "late_matrix_block",
        LATE_SEGMENT,
        Some(MATRIX_BLOCK_MAGIC),
        MATRIX_BLOCK_FORMAT_VERSION,
        V1,
        Encoding::Binary,
        false,
        probe_matrix_block_format,
        VersionHeader::AfterMagicU8,
        ""
    ),
    family!(
        "late_attribute_block",
        LATE_SEGMENT,
        Some(ATTRIBUTE_BLOCK_MAGIC),
        ATTRIBUTE_BLOCK_FORMAT_VERSION,
        V1,
        Encoding::Binary,
        false,
        probe_attribute_block_format,
        VersionHeader::AfterMagicU8,
        ""
    ),
    family!(
        "compaction_staging",
        STAGING,
        None,
        0,
        &[],
        Encoding::Json,
        false,
        unversioned_probe,
        VersionHeader::Unversioned,
        "the staging record is evolvable named-field JSON without a version field"
    ),
    family!(
        "gc_candidate_ledger",
        GC,
        Some(b""),
        GC_CANDIDATE_STORE_VERSION,
        GC_VERSIONS,
        Encoding::Json,
        false,
        probe_gc_candidate_ledger,
        VersionHeader::JsonField,
        ""
    ),
    family!(
        "branch_visibility_marker",
        BRANCH_VISIBILITY,
        None,
        0,
        &[],
        Encoding::Json,
        false,
        unversioned_probe,
        VersionHeader::Unversioned,
        "the marker uses a string domain discriminator, not a numeric format version"
    ),
    family!(
        "security_policy_head",
        NONE,
        None,
        0,
        &[],
        Encoding::Json,
        false,
        unversioned_probe,
        VersionHeader::Unversioned,
        "policy version is semantic policy authority, not a codec version"
    ),
    family!(
        "security_policy_snapshot",
        NONE,
        None,
        0,
        &[],
        Encoding::Json,
        false,
        unversioned_probe,
        VersionHeader::Unversioned,
        "policy version is semantic policy authority, not a codec version"
    ),
    family!(
        "security_audit_jsonl",
        NONE,
        None,
        0,
        &[],
        Encoding::JsonLines,
        false,
        unversioned_probe,
        VersionHeader::Unversioned,
        "audit records are newline-delimited JSON without an envelope version"
    ),
    family!(
        "namespace_destruction_record",
        NONE,
        None,
        0,
        &[],
        Encoding::Json,
        false,
        unversioned_probe,
        VersionHeader::Unversioned,
        "destruction evidence is strict JSON without a format-version field"
    ),
];

fn unversioned_probe(_data: &[u8]) -> Result<()> {
    Err(ZeppelinError::Serialization(
        "unversioned format has no future-version discriminator to probe".to_string(),
    ))
}

fn probe_manifest(data: &[u8]) -> Result<()> {
    Manifest::from_bytes(data).map(drop)
}

fn probe_named_snapshot(data: &[u8]) -> Result<()> {
    NamedSnapshot::from_bytes(data).map(drop)
}

fn probe_wal_fragment(data: &[u8]) -> Result<()> {
    WalFragment::from_bytes(data).map(drop)
}

fn probe_input_wal(data: &[u8]) -> Result<()> {
    EncoderInputWalFragment::from_bytes(data).map(drop)
}

fn probe_bootstrap(data: &[u8]) -> Result<()> {
    deserialize_bootstrap(data).map(drop)
}

fn probe_centroids(data: &[u8]) -> Result<()> {
    deserialize_centroids_data(data).map(drop)
}

fn probe_cluster_object(data: &[u8]) -> Result<()> {
    match cluster_object_sections(data)? {
        Some(_) => Ok(()),
        None => deserialize_cluster(data).map(drop),
    }
}

fn probe_cluster_section(data: &[u8]) -> Result<()> {
    deserialize_cluster(data).map(drop)
}

fn probe_membership(data: &[u8]) -> Result<()> {
    deserialize_membership(data).map(drop)
}

fn probe_sketch(data: &[u8]) -> Result<()> {
    ResidentSketch::from_bytes(data).map(drop)
}

fn probe_filter_summary(data: &[u8]) -> Result<()> {
    FilterCardinalitySummary::from_bytes(data).map(drop)
}

fn probe_bitmap(data: &[u8]) -> Result<()> {
    ClusterBitmapIndex::from_bytes(data).map(drop)
}

fn probe_rq(data: &[u8]) -> Result<()> {
    RqClusterCodes::from_bytes(data)
        .map(drop)
        .map_err(|error| ZeppelinError::Index(error.to_string()))
}

fn probe_fts(data: &[u8]) -> Result<()> {
    InvertedIndex::from_bytes(data).map(drop)
}

fn probe_global_fts(data: &[u8]) -> Result<()> {
    GlobalInvertedIndex::from_bytes(data).map(drop)
}

fn probe_late_state(data: &[u8]) -> Result<()> {
    LateStateSection::from_bytes(data).map(drop)
}

fn probe_matrix_fragment(data: &[u8]) -> Result<()> {
    MatrixArtifact::from_bytes(
        data,
        ArtifactChecksum::digest(data),
        MatrixDtype::F16,
        MultiVectorEpochId::new([0; 32]),
        0,
        1,
        1,
        1,
    )
    .map(drop)
}

fn probe_fde_fragment(data: &[u8]) -> Result<()> {
    FdeArtifact::from_bytes(
        data,
        ArtifactChecksum::digest(data),
        FdeGenerationId::new([0; 32]),
        ArtifactChecksum::new([0; 32]),
        1,
        1,
    )
    .map(drop)
}

fn probe_fde_transform(data: &[u8]) -> Result<()> {
    FdeTransform::from_bytes(data).map(drop)
}

fn probe_centering(data: &[u8]) -> Result<()> {
    CenteringArtifact::from_bytes(data, ArtifactChecksum::digest(data), 1).map(drop)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use bytes::Bytes;
    use chrono::Utc;

    use super::*;
    use crate::embedding::{
        CenteringArtifact, ContentHash, FdeArtifactRow, MatrixArtifactRow, MultiVectorEmbedding,
    };
    use crate::index::ivf_flat::build::{
        serialize_centroids, serialize_cluster, serialize_cluster_data_object,
        serialize_cluster_data_object_v5, serialize_colocated_sq_cluster,
        serialize_fixed_stride_f32_block, serialize_id_block, Zbp5ClusterBlocks,
    };
    use crate::index::ivf_flat::membership::serialize_membership;
    use crate::index::ivf_flat::sketch::build_resident_sketch;
    use crate::index::late_interaction::{
        FdeAlgorithmVersion, FdeParams, FinalProjection, InnerProjection,
    };
    use crate::retrieval_scope::{scoped_ann_descriptor_fixture, scoped_fts_artifact_fixture};

    fn version_probe_bytes(family: &FormatFamily, version: u32) -> Result<Vec<u8>> {
        match family.version_header {
            VersionHeader::Unversioned => Err(ZeppelinError::Serialization(format!(
                "{} is unversioned",
                family.name
            ))),
            VersionHeader::LeadingU8 => Ok(vec![u8::try_from(version).map_err(|_| {
                ZeppelinError::Serialization("probe version exceeds u8".to_string())
            })?]),
            VersionHeader::AfterMagicU8 => {
                let mut bytes = family.magic.unwrap_or_default().to_vec();
                bytes.push(u8::try_from(version).map_err(|_| {
                    ZeppelinError::Serialization("probe version exceeds u8".to_string())
                })?);
                bytes.resize(256, 0);
                Ok(bytes)
            }
            VersionHeader::AfterMagicU32Le => {
                let mut bytes = family.magic.unwrap_or_default().to_vec();
                bytes.extend_from_slice(&version.to_le_bytes());
                bytes.resize(256, 0);
                Ok(bytes)
            }
            VersionHeader::MagicAsciiSuffix => {
                let mut bytes = family.magic.unwrap_or_default().to_vec();
                let suffix = u8::try_from(version)
                    .ok()
                    .and_then(|value| value.checked_add(b'0'))
                    .ok_or_else(|| {
                        ZeppelinError::Serialization(
                            "ASCII-suffix probe version exceeds one digit".to_string(),
                        )
                    })?;
                let last = bytes.last_mut().ok_or_else(|| {
                    ZeppelinError::Serialization("ASCII-suffix magic is empty".to_string())
                })?;
                *last = suffix;
                bytes.resize(256, 0);
                Ok(bytes)
            }
            VersionHeader::JsonField => match family.name {
                "scoped_ann_descriptor" => {
                    scoped_ann_descriptor_fixture(version).map(|bytes| bytes.to_vec())
                }
                "gc_candidate_ledger" if version == 0 => Ok(b"[]".to_vec()),
                "gc_candidate_ledger" => {
                    Ok(format!("{{\"version\":{version},\"candidates\":[]}}").into_bytes())
                }
                _ => Err(ZeppelinError::Serialization(format!(
                    "missing JSON version fixture for {}",
                    family.name
                ))),
            },
            VersionHeader::JsonFieldAfterMagic => {
                scoped_fts_artifact_fixture(version).map(|bytes| bytes.to_vec())
            }
        }
    }

    fn bootstrap_fixture(version: u32) -> Result<Vec<u8>> {
        let (header_len, fields, summary) = match version {
            1 => (40_usize, Vec::new(), Vec::new()),
            2 => (56_usize, 0_u32.to_le_bytes().to_vec(), Vec::new()),
            3 => (72_usize, 0_u32.to_le_bytes().to_vec(), vec![1]),
            _ => {
                return Err(ZeppelinError::Serialization(format!(
                    "unsupported bootstrap fixture version {version}"
                )))
            }
        };
        let centroids = [1_u8];
        let sketch = [2_u8];
        let centroids_offset = header_len;
        let sketch_offset = centroids_offset + centroids.len();
        let fields_offset = sketch_offset + sketch.len();
        let summary_offset = fields_offset + fields.len();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(BOOTSTRAP_MAGIC);
        bytes.extend_from_slice(&version.to_le_bytes());
        bytes.extend_from_slice(&(centroids_offset as u64).to_le_bytes());
        bytes.extend_from_slice(&(centroids.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&(sketch_offset as u64).to_le_bytes());
        bytes.extend_from_slice(&(sketch.len() as u64).to_le_bytes());
        if version >= 2 {
            bytes.extend_from_slice(&(fields_offset as u64).to_le_bytes());
            bytes.extend_from_slice(&(fields.len() as u64).to_le_bytes());
        }
        if version >= 3 {
            bytes.extend_from_slice(&(summary_offset as u64).to_le_bytes());
            bytes.extend_from_slice(&(summary.len() as u64).to_le_bytes());
        }
        bytes.extend_from_slice(&centroids);
        bytes.extend_from_slice(&sketch);
        bytes.extend_from_slice(&fields);
        bytes.extend_from_slice(&summary);
        Ok(bytes)
    }

    fn colocated_fixture(magic: &[u8; 4], coarse: &[u8], full: &[u8]) -> Vec<u8> {
        let coarse_offset = 36_u64;
        let full_offset = coarse_offset + coarse.len() as u64;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(magic);
        bytes.extend_from_slice(&coarse_offset.to_le_bytes());
        bytes.extend_from_slice(&(coarse.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&full_offset.to_le_bytes());
        bytes.extend_from_slice(&(full.len() as u64).to_le_bytes());
        bytes.extend_from_slice(coarse);
        bytes.extend_from_slice(full);
        bytes
    }

    fn cluster_section_fixture(version: u32) -> Result<Vec<u8>> {
        let full = serialize_cluster(&[], &[], 1)?.to_vec();
        match version {
            0 => Ok(full),
            2 => serialize_colocated_sq_cluster(&[], &[], &[], 1).map(|bytes| bytes.to_vec()),
            3 => {
                let mut rq = Vec::new();
                rq.extend_from_slice(RQ_MAGIC);
                rq.push(RQ_VERSION);
                rq.extend_from_slice(&64_u64.to_le_bytes());
                rq.extend_from_slice(&0_u64.to_le_bytes());
                Ok(colocated_fixture(CLUSTER_V3_MAGIC, &rq, &full))
            }
            _ => Err(ZeppelinError::Serialization(format!(
                "unsupported cluster-section fixture version {version}"
            ))),
        }
    }

    fn cluster_object_fixture(version: u32) -> Result<Vec<u8>> {
        match version {
            0 => cluster_section_fixture(0),
            1 => serialize_cluster_data_object(&[(0, Bytes::from(cluster_section_fixture(0)?))])
                .map(|bytes| bytes.to_vec()),
            4 => serialize_cluster_data_object(&[(0, Bytes::from(cluster_section_fixture(2)?))])
                .map(|bytes| bytes.to_vec()),
            5 => {
                let ids = serialize_id_block(&["row".to_string()])?;
                let vectors = serialize_fixed_stride_f32_block(&[vec![0.0]], 1)?;
                serialize_cluster_data_object_v5(&[Zbp5ClusterBlocks {
                    cluster_idx: 0,
                    row_count: 1,
                    dim: 1,
                    coarse: &[],
                    ids: &ids,
                    vectors: &vectors,
                }])
                .map(|object| object.bytes.to_vec())
            }
            _ => Err(ZeppelinError::Serialization(format!(
                "unsupported cluster-object fixture version {version}"
            ))),
        }
    }

    fn sketch_fixture(version: u32) -> Result<Vec<u8>> {
        match version {
            2 | 3 => {
                let (codebook_size, code) = if version == 2 {
                    (16_usize, 0_u8)
                } else {
                    (256_usize, 0_u8)
                };
                let mut bytes = Vec::new();
                bytes.extend_from_slice(SKETCH_MAGIC);
                bytes.extend_from_slice(&version.to_le_bytes());
                bytes.extend_from_slice(&1_u32.to_le_bytes());
                bytes.extend_from_slice(&1_u32.to_le_bytes());
                bytes.extend_from_slice(&1_u32.to_le_bytes());
                bytes.extend_from_slice(&1_u64.to_le_bytes());
                for value in 0..codebook_size {
                    bytes.extend_from_slice(&(value as f32).to_le_bytes());
                }
                bytes.push(0);
                bytes.extend_from_slice(&1_u32.to_le_bytes());
                bytes.push(code);
                Ok(bytes)
            }
            4 => build_resident_sketch(
                "format",
                "probe",
                1,
                &[vec![0.0]],
                &[vec![vec![1.0]]],
                &[vec![None]],
            )
            .map(|(_, bytes, _)| bytes.to_vec()),
            _ => Err(ZeppelinError::Serialization(format!(
                "unsupported sketch fixture version {version}"
            ))),
        }
    }

    fn accepted_fixture(family: &FormatFamily, version: u32) -> Result<Vec<u8>> {
        match family.name {
            "manifest" if version == 0 => serde_json::to_vec(&Manifest::new())
                .map_err(|error| ZeppelinError::Serialization(error.to_string())),
            "manifest" => Manifest::new().to_bytes().map(|bytes| bytes.to_vec()),
            "named_snapshot" => {
                let snapshot = NamedSnapshot {
                    generation: 1,
                    created_at: Utc::now(),
                };
                if version == 0 {
                    serde_json::to_vec(&snapshot)
                        .map_err(|error| ZeppelinError::Serialization(error.to_string()))
                } else {
                    snapshot.to_bytes().map(|bytes| bytes.to_vec())
                }
            }
            "wal_fragment" => {
                let fragment = WalFragment::try_new(Vec::new(), Vec::new())?;
                if version == 0 {
                    serde_json::to_vec(&fragment)
                        .map_err(|error| ZeppelinError::Serialization(error.to_string()))
                } else {
                    fragment.to_bytes().map(|bytes| bytes.to_vec())
                }
            }
            "input_wal_fragment" => EncoderInputWalFragment::try_new(Vec::new(), Vec::new())?
                .to_bytes()
                .map(|bytes| bytes.to_vec()),
            "ivf_bootstrap" => bootstrap_fixture(version),
            "ivf_centroids" if version == 0 => {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(&0_u32.to_le_bytes());
                bytes.extend_from_slice(&1_u32.to_le_bytes());
                Ok(bytes)
            }
            "ivf_centroids" => serialize_centroids(&[], 1).map(|bytes| bytes.to_vec()),
            "cluster_data_object" => cluster_object_fixture(version),
            "cluster_vector_section" => cluster_section_fixture(version),
            "ivf_membership" => Ok(serialize_membership(1, &[]).to_vec()),
            "resident_sketch" => sketch_fixture(version),
            "filter_cardinality_summary" => FilterCardinalitySummary::default()
                .to_bytes()
                .map(|bytes| bytes.to_vec()),
            "cluster_bitmap_index" => ClusterBitmapIndex {
                vector_count: 0,
                fields: HashMap::new(),
            }
            .to_bytes()
            .map(|bytes| bytes.to_vec()),
            "rabitq_cluster" => {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(RQ_MAGIC);
                bytes.push(RQ_VERSION);
                bytes.extend_from_slice(&256_u64.to_le_bytes());
                bytes.extend_from_slice(&0_u64.to_le_bytes());
                Ok(bytes)
            }
            "fts_cluster_index" => InvertedIndex {
                vector_count: 0,
                fields: BTreeMap::new(),
            }
            .to_bytes()
            .map(|bytes| bytes.to_vec()),
            "fts_global_index" => GlobalInvertedIndex {
                total_docs: 0,
                fields: BTreeMap::new(),
            }
            .to_bytes()
            .map(|bytes| bytes.to_vec()),
            "scoped_ann_descriptor" | "gc_candidate_ledger" => version_probe_bytes(family, version),
            "scoped_fts_index" => scoped_fts_artifact_fixture(version).map(|bytes| bytes.to_vec()),
            "late_state_section" => match version {
                1 => Ok(b"ZLS1\x01\x90".to_vec()),
                2 => Ok(b"ZLS1\x02\x92\x90\x90".to_vec()),
                3 => Ok(b"ZLS1\x03\x95\x90\x90\xc0\x90\x90".to_vec()),
                4 => Ok(b"ZLS1\x04\x97\x90\x90\xc0\x90\x90\x90\xc0".to_vec()),
                5 => LateStateSection::new()
                    .to_bytes()
                    .map(|bytes| bytes.to_vec()),
                _ => Err(ZeppelinError::Serialization(format!(
                    "unsupported late-state fixture version {version}"
                ))),
            },
            "matrix_fragment" => {
                let embedding = MultiVectorEmbedding::new(vec![0.0], 1, 1, 1)?;
                MatrixArtifact::new(
                    MatrixDtype::F16,
                    MultiVectorEpochId::new([0; 32]),
                    0,
                    1,
                    vec![MatrixArtifactRow::new(ContentHash::new([1; 32]), embedding)],
                )?
                .to_bytes()
                .map(|artifact| artifact.bytes().to_vec())
            }
            "fde_fragment" => FdeArtifact::new(
                FdeGenerationId::new([0; 32]),
                ArtifactChecksum::new([0; 32]),
                1,
                vec![FdeArtifactRow::new(
                    ContentHash::new([1; 32]),
                    vec![0.0],
                    1,
                )?],
            )?
            .to_bytes()
            .map(|artifact| artifact.bytes().to_vec()),
            "fde_transform" => FdeTransform::generate(
                &FdeParams {
                    algorithm: FdeAlgorithmVersion::ReferenceV1,
                    repetitions: 1,
                    simhash_bits: 1,
                    input_dimension: 1,
                    inner: InnerProjection::Identity,
                    final_projection: FinalProjection::None,
                },
                0,
            )
            .map(|transform| transform.to_bytes().to_vec()),
            "centering" => CenteringArtifact::new(vec![0.0])?
                .to_bytes()
                .map(|artifact| artifact.bytes().to_vec()),
            "quarantine_evidence"
            | "late_candidate_bootstrap"
            | "late_candidate_cluster"
            | "late_flat_candidate"
            | "late_matrix_block"
            | "late_attribute_block" => version_probe_bytes(family, version),
            other => Err(ZeppelinError::Serialization(format!(
                "missing accepted fixture for {other}"
            ))),
        }
    }

    #[test]
    fn every_versioned_family_rejects_current_plus_one() {
        for family in FORMATS.iter().filter(|family| family.magic.is_some()) {
            let future = family.current_version + 1;
            let bytes = version_probe_bytes(family, future).unwrap_or_else(|error| {
                panic!("{} future-version fixture failed: {error}", family.name)
            });
            let error = match (family.decode_probe)(&bytes) {
                Ok(()) => panic!("{} accepted unsupported version {future}", family.name),
                Err(error) => error,
            };
            assert!(
                error.to_string().contains(&future.to_string()),
                "{} error did not name version {future}: {error}",
                family.name
            );
        }
    }

    #[test]
    fn every_registered_version_reaches_its_decoder() {
        for family in FORMATS {
            for &version in family.accepted_versions {
                let bytes = accepted_fixture(family, version).unwrap_or_else(|error| {
                    panic!(
                        "{} accepted-version {version} fixture failed: {error}",
                        family.name
                    )
                });
                (family.decode_probe)(&bytes).unwrap_or_else(|error| {
                    panic!(
                        "{} rejected registered version {version}: {error}",
                        family.name
                    )
                });
            }
        }
    }

    #[test]
    fn magic_values_are_unique_except_for_the_frozen_zmb1_collision() {
        let mut by_magic = BTreeMap::<Vec<u8>, Vec<&str>>::new();
        for family in FORMATS {
            if let Some(magic) = family.magic.filter(|magic| !magic.is_empty()) {
                by_magic
                    .entry(magic.to_vec())
                    .or_default()
                    .push(family.name);
            }
        }
        let collisions = by_magic
            .into_iter()
            .filter(|(_, names)| names.len() > 1)
            .collect::<Vec<_>>();

        // Renaming either ZMB1 codec is itself a format change. Keep this exact
        // pair explicit until a later flag-day migration can retire one magic.
        assert_eq!(
            collisions,
            vec![(
                b"ZMB1".to_vec(),
                vec!["ivf_membership", "late_matrix_block"]
            )]
        );
    }

    #[test]
    fn every_namespace_object_family_has_a_format_row() {
        for key_family in NamespaceObjectFamily::ALL {
            assert!(
                FORMATS
                    .iter()
                    .any(|format| format.key_families.contains(&key_family)),
                "namespace object family {key_family:?} has no format row"
            );
        }
    }

    #[test]
    fn unversioned_rows_explain_why_they_cannot_be_probed() {
        for family in FORMATS.iter().filter(|family| family.magic.is_none()) {
            assert_eq!(family.current_version, 0, "{}", family.name);
            assert!(family.accepted_versions.is_empty(), "{}", family.name);
            assert!(!family.probe_note.is_empty(), "{}", family.name);
            let error = (family.decode_probe)(&[]).unwrap_err();
            assert!(error.to_string().contains("unversioned"), "{}", family.name);
        }
    }

    #[test]
    fn documentation_table_covers_every_registry_row() {
        let module_docs = include_str!("format.rs");
        for family in FORMATS {
            assert!(
                module_docs.contains(&format!("`{}`", family.name)),
                "format docs omit {}",
                family.name
            );
            let _ = (family.encoding, family.checksum_input);
        }
    }
}
