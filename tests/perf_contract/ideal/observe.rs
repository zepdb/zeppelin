use serde::Serialize;

use crate::common::counting::ArtifactClass;
use crate::perf_contract::depth::{OpSpan, PhysicalRequest, SpanKind, SpanOutcome};
use crate::perf_contract::report::stable_depth_key;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SerialGetChain {
    pub depth: u32,
    pub links: Vec<SerialGetLink>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SerialGetLink {
    pub ordinal: u32,
    pub kind: SpanKind,
    pub request: PhysicalRequest,
    pub class: ArtifactClass,
    pub key: String,
    pub bytes: u64,
    pub elapsed_us: u64,
    pub outcome: SpanOutcome,
    pub ok: bool,
}

pub(crate) fn serial_get_chain(spans: &[OpSpan]) -> SerialGetChain {
    let path = deterministic_interval_get_path(spans);
    let links = path
        .iter()
        .enumerate()
        .map(|(index, span)| SerialGetLink {
            ordinal: u32::try_from(index + 1).expect("serial GET chain ordinal overflowed u32"),
            kind: span.kind,
            request: span.request,
            class: span.class,
            key: stable_ideal_key(&span.key),
            bytes: span.bytes,
            elapsed_us: span
                .wall_end_us
                .checked_sub(span.wall_start_us)
                .expect("object-store span ended before it started"),
            outcome: span.outcome,
            ok: span.ok,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        path.len(),
        links.len(),
        "serial GET depth must equal reconstructed chain length"
    );
    SerialGetChain {
        depth: u32::try_from(path.len()).expect("serial GET depth overflowed u32"),
        links,
    }
}

fn deterministic_interval_get_path(spans: &[OpSpan]) -> Vec<OpSpan> {
    let mut ordered = spans
        .iter()
        .filter(|span| span.kind == SpanKind::Get)
        .collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        left.end_seq
            .cmp(&right.end_seq)
            .then_with(|| left.start_seq.cmp(&right.start_seq))
            .then_with(|| span_signature(left).cmp(&span_signature(right)))
    });

    let mut paths = Vec::<Vec<OpSpan>>::with_capacity(ordered.len());
    for span in &ordered {
        let predecessor = ordered
            .iter()
            .take(paths.len())
            .enumerate()
            .filter(|(_, candidate)| candidate.end_seq <= span.start_seq)
            .map(|(index, _)| &paths[index])
            .max_by(|left, right| compare_paths(left, right));
        let mut path = predecessor.cloned().unwrap_or_default();
        path.push((*span).clone());
        paths.push(path);
    }
    paths
        .into_iter()
        .max_by(|left, right| compare_paths(left, right))
        .unwrap_or_default()
}

fn compare_paths(left: &[OpSpan], right: &[OpSpan]) -> std::cmp::Ordering {
    left.len().cmp(&right.len()).then_with(|| {
        let left = left.iter().map(span_signature).collect::<Vec<_>>();
        let right = right.iter().map(span_signature).collect::<Vec<_>>();
        // Reverse the lexical comparison so `max_by` chooses the stable
        // smallest signature for equal-depth alternatives.
        right.cmp(&left)
    })
}

fn span_signature(span: &OpSpan) -> String {
    format!(
        "{}|{:?}|{}|{:020}|{:?}|{}",
        stable_ideal_key(&span.key),
        span.request,
        span.class.name(),
        span.bytes,
        span.outcome,
        span.key
    )
}

pub(crate) fn stable_ideal_key(key: &str) -> String {
    if let Some((from, to)) = key.split_once("->") {
        return format!("{}->{}", stable_ideal_key(from), stable_ideal_key(to));
    }
    let filename = key.rsplit('/').next().unwrap_or(key);
    if filename.strip_prefix("seg_").is_some_and(|suffix| {
        suffix.len() == 26 && suffix.bytes().all(|byte| byte.is_ascii_alphanumeric())
    }) {
        return "seg_<ulid>".to_string();
    }
    if filename
        .strip_prefix("__clone_")
        .and_then(|suffix| suffix.strip_suffix(".msgpack"))
        .is_some_and(|suffix| {
            suffix.len() == 32 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    {
        return "__clone_<uuid>.msgpack".to_string();
    }
    if let Some(stem) = filename.strip_suffix(".bin") {
        if let Some((prefix, suffix)) = stem.rsplit_once('_') {
            if prefix.starts_with("node_")
                && suffix.len() == 26
                && suffix.bytes().all(|byte| byte.is_ascii_alphanumeric())
            {
                return format!("{prefix}_<ulid>.bin");
            }
        }
    }
    if let Some(normalized) = normalize_test_namespace(filename) {
        return normalized;
    }
    stable_depth_key(key)
}

fn normalize_test_namespace(value: &str) -> Option<String> {
    let suffix = value.strip_prefix("test-")?;
    if suffix.len() < 36 {
        return None;
    }
    let uuid = &suffix[..36];
    let valid = uuid.bytes().enumerate().all(|(index, byte)| match index {
        8 | 13 | 18 | 23 => byte == b'-',
        _ => byte.is_ascii_hexdigit(),
    });
    valid.then(|| format!("test-<uuid>{}", &suffix[36..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn span(
        kind: SpanKind,
        request: PhysicalRequest,
        key: &str,
        start_seq: u64,
        end_seq: u64,
    ) -> OpSpan {
        OpSpan {
            kind,
            request,
            class: ArtifactClass::Other,
            key: key.to_string(),
            start_seq,
            end_seq,
            bytes: 11,
            ok: true,
            wall_start_us: start_seq * 10,
            wall_end_us: end_seq * 10,
            outcome: SpanOutcome::Success,
        }
    }

    #[test]
    fn serial_get_chain_excludes_heads_and_parallel_siblings() {
        let spans = vec![
            span(SpanKind::Head, PhysicalRequest::Head, "ns/meta.json", 0, 1),
            span(
                SpanKind::Get,
                PhysicalRequest::GetFull,
                "ns/segments/01/bootstrap.bin",
                0,
                2,
            ),
            span(
                SpanKind::Get,
                PhysicalRequest::GetRange {
                    start: 10,
                    end: Some(20),
                },
                "ns/segments/01/cluster_group_0.bin",
                2,
                3,
            ),
            span(
                SpanKind::Get,
                PhysicalRequest::GetRange {
                    start: 20,
                    end: Some(30),
                },
                "ns/segments/01/cluster_group_1.bin",
                2,
                4,
            ),
        ];

        let chain = serial_get_chain(&spans);

        assert_eq!(chain.depth, 2);
        assert_eq!(chain.links.len(), 2);
        assert_eq!(chain.links[0].key, "bootstrap.bin");
        assert_eq!(chain.links[1].key, "cluster_group_<index>.bin");
        assert!(chain.links.iter().all(|link| link.kind == SpanKind::Get));
    }

    #[test]
    fn equal_depth_parallel_sibling_selection_is_deterministic() {
        let mut small = span(
            SpanKind::Get,
            PhysicalRequest::GetFull,
            "ns/segments/01/attrs_0.bin",
            0,
            1,
        );
        small.bytes = 442;
        let mut large = span(
            SpanKind::Get,
            PhysicalRequest::GetFull,
            "ns/segments/01/attrs_1.bin",
            0,
            2,
        );
        large.bytes = 574;
        let next = span(
            SpanKind::Get,
            PhysicalRequest::GetFull,
            "ns/segments/01/cluster_0.bin",
            2,
            3,
        );
        let first = serial_get_chain(&[small.clone(), large.clone(), next.clone()]);

        small.end_seq = 2;
        large.end_seq = 1;
        let second = serial_get_chain(&[next, large, small]);

        assert_eq!(first.depth, 2);
        assert_eq!(first.links[0].bytes, 442);
        assert_eq!(first, second);
    }

    #[test]
    fn ideal_keys_normalize_run_specific_namespaces_and_segments() {
        assert_eq!(
            stable_ideal_key("test-c2aecf3a-ea1b-4782-8ca6-85b93949c71c-background-tick/segments"),
            "segments"
        );
        assert_eq!(
            stable_ideal_key("prefix/seg_01KXCAYS15MX7W8EQXHJJGK7SN"),
            "seg_<ulid>"
        );
        assert_eq!(
            stable_ideal_key("prefix/node_n_2_01KXCC7MVG2061MC2293NMCCEK.bin"),
            "node_n_2_<ulid>.bin"
        );
        assert_eq!(
            stable_ideal_key("prefix/__clone_439e36e43c3e49d9bee780265956b287.msgpack"),
            "__clone_<uuid>.msgpack"
        );
        assert_eq!(
            stable_ideal_key("test-c2aecf3a-ea1b-4782-8ca6-85b93949c71c-background-tick"),
            "test-<uuid>-background-tick"
        );
        assert_eq!(
            stable_ideal_key("source/segments/01/cluster_0.bin->target/segments/02/cluster_4.bin"),
            "cluster_<index>.bin->cluster_<index>.bin"
        );
    }
}
