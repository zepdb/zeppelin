//! Deterministic cross-object planning for bounded ranged reads.
//!
//! The planner groups logical ranges by immutable object, coalesces useful
//! spans, caps each physical request, and preserves a back-map to caller order.
//! It owns no cache or retry policy and has no production caller until the
//! late-interaction two-wave query adopts it.

use std::collections::BTreeMap;
use std::ops::Range;

use bytes::Bytes;
use futures::{stream, Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

use crate::error::ZeppelinError;
use crate::storage::ZeppelinStore;

/// Default maximum accumulated gap bytes read speculatively per request.
pub const DEFAULT_GAP_BUDGET_BYTES: usize = 64 * 1024;
/// Default maximum bytes fetched by one planned ranged request.
pub const DEFAULT_MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;
/// Default maximum number of physical range requests in flight.
pub const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 8;

/// One caller-ordered logical range read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadRequest {
    /// Immutable object key.
    pub object_key: String,
    /// Non-empty half-open byte range.
    pub range: Range<usize>,
}

/// Validated bounds for one immutable read plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ReadPlanConfig {
    /// Maximum accumulated gap waste within one coalesced request.
    pub gap_budget_bytes: usize,
    /// Hard maximum bytes in one physical ranged request.
    pub max_request_bytes: usize,
    /// Hard maximum physical ranged requests executing concurrently.
    pub max_concurrent_requests: usize,
}

#[derive(Deserialize)]
struct ReadPlanConfigWire {
    gap_budget_bytes: usize,
    max_request_bytes: usize,
    max_concurrent_requests: usize,
}

impl<'de> Deserialize<'de> for ReadPlanConfig {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ReadPlanConfigWire::deserialize(deserializer)?;
        Self::new(
            wire.gap_budget_bytes,
            wire.max_request_bytes,
            wire.max_concurrent_requests,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl Default for ReadPlanConfig {
    fn default() -> Self {
        Self {
            gap_budget_bytes: DEFAULT_GAP_BUDGET_BYTES,
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            max_concurrent_requests: DEFAULT_MAX_CONCURRENT_REQUESTS,
        }
    }
}

impl ReadPlanConfig {
    /// Construct bounds after enforcing every planner invariant.
    pub fn new(
        gap_budget_bytes: usize,
        max_request_bytes: usize,
        max_concurrent_requests: usize,
    ) -> Result<Self, ReadPlanError> {
        let config = Self {
            gap_budget_bytes,
            max_request_bytes,
            max_concurrent_requests,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ReadPlanError> {
        if self.gap_budget_bytes == 0 {
            return Err(ReadPlanError::InvalidConfig {
                reason: "gap_budget_bytes must be greater than zero",
            });
        }
        if self.max_request_bytes == 0 {
            return Err(ReadPlanError::InvalidConfig {
                reason: "max_request_bytes must be greater than zero",
            });
        }
        if self.max_concurrent_requests == 0 {
            return Err(ReadPlanError::InvalidConfig {
                reason: "max_concurrent_requests must be greater than zero",
            });
        }
        if self.gap_budget_bytes >= self.max_request_bytes {
            return Err(ReadPlanError::InvalidConfig {
                reason: "gap_budget_bytes must be less than max_request_bytes",
            });
        }
        Ok(())
    }
}

/// Typed planning and execution failures.
#[derive(Debug, Error)]
pub enum ReadPlanError {
    /// A configuration bound was zero or internally inconsistent.
    #[error("invalid read-plan config: {reason}")]
    InvalidConfig {
        /// Stable non-sensitive validation reason.
        reason: &'static str,
    },
    /// A logical input selected no bytes.
    #[error("read request {request_index} has an empty range")]
    EmptyRange {
        /// Caller-order request position.
        request_index: usize,
    },
    /// A logical input's start followed its end.
    #[error("read request {request_index} has a reversed range")]
    ReversedRange {
        /// Caller-order request position.
        request_index: usize,
    },
    /// A logical request cannot fit one no-copy physical buffer.
    #[error(
        "read request {request_index} is {requested_bytes} bytes, maximum is {max_request_bytes}"
    )]
    RequestTooLarge {
        /// Caller-order request position.
        request_index: usize,
        /// Logical bytes requested.
        requested_bytes: usize,
        /// Configured physical-request maximum.
        max_request_bytes: usize,
    },
    /// Checked offset or byte-count arithmetic overflowed.
    #[error("read-plan arithmetic overflow while {context}")]
    ArithmeticOverflow {
        /// Static operation label.
        context: &'static str,
    },
    /// The storage boundary failed a planned request.
    #[error("read-plan storage failure: {0}")]
    Storage(#[from] ZeppelinError),
    /// The backend did not return the one requested range.
    #[error("read-plan backend returned {actual} buffers, expected {expected}")]
    UnexpectedBufferCount {
        /// Expected buffer count.
        expected: usize,
        /// Actual buffer count.
        actual: usize,
    },
    /// A backend returned fewer bytes than the planned range requires.
    #[error("short read for {object_key}: expected {expected_bytes} bytes, got {actual_bytes}")]
    ShortRead {
        /// Non-sensitive immutable key.
        object_key: String,
        /// Planned byte count.
        expected_bytes: usize,
        /// Returned byte count.
        actual_bytes: usize,
    },
    /// Internal plan state could not resolve a caller output.
    #[error("read-plan back-map is inconsistent")]
    InvalidBackMap,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PlannedRead {
    object_key: String,
    range: Range<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BackMapEntry {
    planned_index: usize,
    slice: Range<usize>,
}

/// Immutable physical requests plus caller-order no-copy slice mappings.
#[derive(Clone, Debug)]
pub struct ReadPlan {
    planned: Vec<PlannedRead>,
    back_map: Vec<BackMapEntry>,
    #[cfg(test)]
    logical_by_planned: Vec<Vec<(usize, Range<usize>)>>,
    planned_bytes: u64,
    max_concurrent_requests: usize,
}

impl ReadPlan {
    /// Build a deterministic plan from caller-ordered logical requests.
    pub fn build(requests: &[ReadRequest], config: &ReadPlanConfig) -> Result<Self, ReadPlanError> {
        config.validate()?;

        #[derive(Clone)]
        struct IndexedRange {
            input_index: usize,
            range: Range<usize>,
        }

        struct PendingRun {
            range: Range<usize>,
            gap_waste: usize,
            members: Vec<IndexedRange>,
        }

        fn finish_run(
            object_key: &str,
            run: PendingRun,
            planned: &mut Vec<PlannedRead>,
            back_map: &mut [Option<BackMapEntry>],
            planned_bytes: &mut u64,
        ) -> Result<(), ReadPlanError> {
            let planned_index = planned.len();
            let run_bytes_usize = run.range.end.checked_sub(run.range.start).ok_or(
                ReadPlanError::ArithmeticOverflow {
                    context: "sizing a planned range",
                },
            )?;
            let run_bytes =
                u64::try_from(run_bytes_usize).map_err(|_| ReadPlanError::ArithmeticOverflow {
                    context: "converting planned bytes to u64",
                })?;
            *planned_bytes =
                planned_bytes
                    .checked_add(run_bytes)
                    .ok_or(ReadPlanError::ArithmeticOverflow {
                        context: "summing planned bytes",
                    })?;

            for member in run.members {
                let slice_start = member.range.start.checked_sub(run.range.start).ok_or(
                    ReadPlanError::ArithmeticOverflow {
                        context: "computing a back-map start",
                    },
                )?;
                let slice_end = member.range.end.checked_sub(run.range.start).ok_or(
                    ReadPlanError::ArithmeticOverflow {
                        context: "computing a back-map end",
                    },
                )?;
                if slice_start >= slice_end || slice_end > run_bytes_usize {
                    return Err(ReadPlanError::InvalidBackMap);
                }
                let slot = back_map
                    .get_mut(member.input_index)
                    .ok_or(ReadPlanError::InvalidBackMap)?;
                if slot
                    .replace(BackMapEntry {
                        planned_index,
                        slice: slice_start..slice_end,
                    })
                    .is_some()
                {
                    return Err(ReadPlanError::InvalidBackMap);
                }
            }
            planned.push(PlannedRead {
                object_key: object_key.to_string(),
                range: run.range,
            });
            Ok(())
        }

        let mut by_object = BTreeMap::<String, Vec<IndexedRange>>::new();
        for (input_index, request) in requests.iter().enumerate() {
            if request.range.start == request.range.end {
                return Err(ReadPlanError::EmptyRange {
                    request_index: input_index,
                });
            }
            if request.range.start > request.range.end {
                return Err(ReadPlanError::ReversedRange {
                    request_index: input_index,
                });
            }
            let requested_bytes = request.range.end - request.range.start;
            if requested_bytes > config.max_request_bytes {
                return Err(ReadPlanError::RequestTooLarge {
                    request_index: input_index,
                    requested_bytes,
                    max_request_bytes: config.max_request_bytes,
                });
            }
            by_object
                .entry(request.object_key.clone())
                .or_default()
                .push(IndexedRange {
                    input_index,
                    range: request.range.clone(),
                });
        }

        let mut planned = Vec::new();
        let mut back_map = vec![None; requests.len()];
        let mut planned_bytes = 0_u64;
        for (object_key, mut ranges) in by_object {
            ranges.sort_unstable_by(|left, right| {
                left.range.start.cmp(&right.range.start).then_with(|| {
                    left.range
                        .end
                        .cmp(&right.range.end)
                        .then_with(|| left.input_index.cmp(&right.input_index))
                })
            });
            let mut ranges = ranges.into_iter();
            let Some(first) = ranges.next() else {
                continue;
            };
            let mut run = PendingRun {
                range: first.range.clone(),
                gap_waste: 0,
                members: vec![first],
            };

            for indexed in ranges {
                let gap = indexed.range.start.saturating_sub(run.range.end);
                let candidate_gap =
                    run.gap_waste
                        .checked_add(gap)
                        .ok_or(ReadPlanError::ArithmeticOverflow {
                            context: "accumulating coalesced gap waste",
                        })?;
                let candidate_end = run.range.end.max(indexed.range.end);
                let candidate_bytes = candidate_end.checked_sub(run.range.start).ok_or(
                    ReadPlanError::ArithmeticOverflow {
                        context: "sizing a candidate coalesced range",
                    },
                )?;
                if candidate_gap <= config.gap_budget_bytes
                    && candidate_bytes <= config.max_request_bytes
                {
                    run.range.end = candidate_end;
                    run.gap_waste = candidate_gap;
                    run.members.push(indexed);
                } else {
                    finish_run(
                        &object_key,
                        run,
                        &mut planned,
                        &mut back_map,
                        &mut planned_bytes,
                    )?;
                    run = PendingRun {
                        range: indexed.range.clone(),
                        gap_waste: 0,
                        members: vec![indexed],
                    };
                }
            }
            finish_run(
                &object_key,
                run,
                &mut planned,
                &mut back_map,
                &mut planned_bytes,
            )?;
        }

        let back_map = back_map
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or(ReadPlanError::InvalidBackMap)?;
        #[cfg(test)]
        let mut logical_by_planned = vec![Vec::new(); planned.len()];
        #[cfg(test)]
        for (logical_index, mapping) in back_map.iter().enumerate() {
            logical_by_planned
                .get_mut(mapping.planned_index)
                .ok_or(ReadPlanError::InvalidBackMap)?
                .push((logical_index, mapping.slice.clone()));
        }
        #[cfg(test)]
        if logical_by_planned.iter().any(Vec::is_empty) {
            return Err(ReadPlanError::InvalidBackMap);
        }
        Ok(Self {
            planned,
            back_map,
            #[cfg(test)]
            logical_by_planned,
            planned_bytes,
            max_concurrent_requests: config.max_concurrent_requests,
        })
    }

    /// Return the number of physical ranged requests.
    #[must_use]
    pub fn planned_request_count(&self) -> usize {
        self.planned.len()
    }

    /// Return the number of caller-ordered logical ranges.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn logical_request_count(&self) -> usize {
        self.back_map.len()
    }

    /// Return total physical bytes, including coalesced gaps and overlap.
    #[must_use]
    pub const fn planned_bytes(&self) -> u64 {
        self.planned_bytes
    }

    /// Restore the caller-ordered logical slices carried by one physical read.
    #[cfg(test)]
    pub(crate) fn logical_slices_for(
        &self,
        planned_index: usize,
        bytes: &Bytes,
    ) -> Result<Vec<(usize, Bytes)>, ReadPlanError> {
        let mappings = self
            .logical_by_planned
            .get(planned_index)
            .ok_or(ReadPlanError::InvalidBackMap)?;
        let mut logical = Vec::with_capacity(mappings.len());
        for (logical_index, slice) in mappings {
            if slice.start >= slice.end || slice.end > bytes.len() {
                return Err(ReadPlanError::InvalidBackMap);
            }
            logical.push((*logical_index, bytes.slice(slice.clone())));
        }
        Ok(logical)
    }
}

/// Execute physical requests as a completion-ordered stream.
pub fn execute_read_plan_streamed<'a>(
    store: &'a ZeppelinStore,
    plan: &'a ReadPlan,
) -> impl Stream<Item = Result<(usize, Bytes), ReadPlanError>> + 'a {
    stream::iter(plan.planned.iter().cloned().enumerate())
        .map(move |(planned_index, planned)| async move {
            let expected_bytes = planned.range.end.checked_sub(planned.range.start).ok_or(
                ReadPlanError::ArithmeticOverflow {
                    context: "sizing an executed range",
                },
            )?;
            // object_store 0.11.2 re-coalesces a multi-range call with its own
            // one-MiB policy. A singleton keeps this plan's gap, size, count,
            // and outer concurrency bounds authoritative.
            let mut buffers = store
                .get_ranges(&planned.object_key, std::slice::from_ref(&planned.range))
                .await?;
            if buffers.len() != 1 {
                return Err(ReadPlanError::UnexpectedBufferCount {
                    expected: 1,
                    actual: buffers.len(),
                });
            }
            let bytes = buffers.pop().ok_or(ReadPlanError::UnexpectedBufferCount {
                expected: 1,
                actual: 0,
            })?;
            if bytes.len() != expected_bytes {
                return Err(ReadPlanError::ShortRead {
                    object_key: planned.object_key,
                    expected_bytes,
                    actual_bytes: bytes.len(),
                });
            }
            Ok((planned_index, bytes))
        })
        .buffer_unordered(plan.max_concurrent_requests)
}

/// Execute every physical request and restore caller-order no-copy slices.
pub async fn execute_read_plan(
    store: &ZeppelinStore,
    plan: &ReadPlan,
) -> Result<Vec<Bytes>, ReadPlanError> {
    let fetched = execute_read_plan_streamed(store, plan)
        .try_collect::<Vec<_>>()
        .await?;

    let mut buffers = vec![None; plan.planned.len()];
    for (planned_index, bytes) in fetched {
        let slot = buffers
            .get_mut(planned_index)
            .ok_or(ReadPlanError::InvalidBackMap)?;
        if slot.replace(bytes).is_some() {
            return Err(ReadPlanError::InvalidBackMap);
        }
    }

    let mut output = Vec::with_capacity(plan.back_map.len());
    for mapping in &plan.back_map {
        let bytes = buffers
            .get(mapping.planned_index)
            .and_then(Option::as_ref)
            .ok_or(ReadPlanError::InvalidBackMap)?;
        if mapping.slice.start >= mapping.slice.end || mapping.slice.end > bytes.len() {
            return Err(ReadPlanError::InvalidBackMap);
        }
        output.push(bytes.slice(mapping.slice.clone()));
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::{
        BackMapEntry, PlannedRead, ReadPlan, ReadPlanConfig, ReadPlanError, ReadRequest,
        DEFAULT_GAP_BUDGET_BYTES, DEFAULT_MAX_CONCURRENT_REQUESTS, DEFAULT_MAX_REQUEST_BYTES,
    };

    #[test]
    fn planner_groups_unsorted_ranges_by_object_and_honors_gap_budget() {
        let config = ReadPlanConfig {
            gap_budget_bytes: 4,
            max_request_bytes: 16,
            max_concurrent_requests: 2,
        };
        let requests = vec![
            ReadRequest {
                object_key: "b".to_string(),
                range: 20..24,
            },
            ReadRequest {
                object_key: "a".to_string(),
                range: 8..12,
            },
            ReadRequest {
                object_key: "a".to_string(),
                range: 0..4,
            },
            ReadRequest {
                object_key: "a".to_string(),
                range: 5..8,
            },
            ReadRequest {
                object_key: "b".to_string(),
                range: 0..4,
            },
            ReadRequest {
                object_key: "b".to_string(),
                range: 10..14,
            },
        ];

        let plan = ReadPlan::build(&requests, &config).expect("valid plan must build");
        assert_eq!(plan.planned_request_count(), 4);
        assert_eq!(plan.planned_bytes(), 24);
        assert_eq!(
            plan.planned,
            vec![
                PlannedRead {
                    object_key: "a".to_string(),
                    range: 0..12,
                },
                PlannedRead {
                    object_key: "b".to_string(),
                    range: 0..4,
                },
                PlannedRead {
                    object_key: "b".to_string(),
                    range: 10..14,
                },
                PlannedRead {
                    object_key: "b".to_string(),
                    range: 20..24,
                },
            ]
        );
    }

    #[test]
    fn planner_merges_exact_gap_budget_but_splits_one_byte_past_it() {
        let config = ReadPlanConfig::new(5, 100, 2).expect("config must validate");
        let exact = ReadPlan::build(
            &[
                ReadRequest {
                    object_key: "object".to_string(),
                    range: 0..10,
                },
                ReadRequest {
                    object_key: "object".to_string(),
                    range: 15..20,
                },
            ],
            &config,
        )
        .expect("exact-boundary plan must build");
        assert_eq!(exact.planned_request_count(), 1);
        assert_eq!(exact.planned_bytes(), 20);
        assert_eq!(exact.planned[0].range, 0..20);

        let past = ReadPlan::build(
            &[
                ReadRequest {
                    object_key: "object".to_string(),
                    range: 0..10,
                },
                ReadRequest {
                    object_key: "object".to_string(),
                    range: 16..20,
                },
            ],
            &config,
        )
        .expect("past-boundary plan must build");
        assert_eq!(past.planned_request_count(), 2);
        assert_eq!(past.planned_bytes(), 14);
        assert_eq!(
            past.planned
                .iter()
                .map(|request| request.range.clone())
                .collect::<Vec<_>>(),
            vec![0..10, 16..20]
        );
    }

    #[test]
    fn planner_caps_runs_and_maps_duplicate_overlapping_inputs() {
        let config = ReadPlanConfig::new(4, 10, 2).expect("config must validate");
        let plan = ReadPlan::build(
            &[
                ReadRequest {
                    object_key: "object".to_string(),
                    range: 8..12,
                },
                ReadRequest {
                    object_key: "object".to_string(),
                    range: 0..6,
                },
                ReadRequest {
                    object_key: "object".to_string(),
                    range: 0..6,
                },
                ReadRequest {
                    object_key: "object".to_string(),
                    range: 4..9,
                },
            ],
            &config,
        )
        .expect("overlapping plan must build");

        assert_eq!(
            plan.planned
                .iter()
                .map(|request| request.range.clone())
                .collect::<Vec<_>>(),
            vec![0..9, 8..12]
        );
        assert_eq!(plan.planned_request_count(), 2);
        assert_eq!(plan.planned_bytes(), 13);
        assert_eq!(
            plan.back_map,
            vec![
                BackMapEntry {
                    planned_index: 1,
                    slice: 0..4,
                },
                BackMapEntry {
                    planned_index: 0,
                    slice: 0..6,
                },
                BackMapEntry {
                    planned_index: 0,
                    slice: 0..6,
                },
                BackMapEntry {
                    planned_index: 0,
                    slice: 4..9,
                },
            ]
        );
    }

    #[test]
    fn planner_rejects_invalid_ranges_and_config_bounds() {
        for config in [
            ReadPlanConfig {
                gap_budget_bytes: 0,
                max_request_bytes: 8,
                max_concurrent_requests: 1,
            },
            ReadPlanConfig {
                gap_budget_bytes: 1,
                max_request_bytes: 0,
                max_concurrent_requests: 1,
            },
            ReadPlanConfig {
                gap_budget_bytes: 1,
                max_request_bytes: 8,
                max_concurrent_requests: 0,
            },
            ReadPlanConfig {
                gap_budget_bytes: 8,
                max_request_bytes: 8,
                max_concurrent_requests: 1,
            },
        ] {
            assert!(matches!(
                ReadPlan::build(&[], &config),
                Err(ReadPlanError::InvalidConfig { .. })
            ));
        }

        let config = ReadPlanConfig::new(1, 8, 1).expect("config must validate");
        assert!(matches!(
            ReadPlan::build(
                &[ReadRequest {
                    object_key: "object".to_string(),
                    range: 3..3,
                }],
                &config
            ),
            Err(ReadPlanError::EmptyRange { request_index: 0 })
        ));
        assert!(matches!(
            ReadPlan::build(
                &[ReadRequest {
                    object_key: "object".to_string(),
                    range: std::ops::Range { start: 4, end: 3 },
                }],
                &config
            ),
            Err(ReadPlanError::ReversedRange { request_index: 0 })
        ));
        assert!(matches!(
            ReadPlan::build(
                &[ReadRequest {
                    object_key: "object".to_string(),
                    range: 0..9,
                }],
                &config
            ),
            Err(ReadPlanError::RequestTooLarge {
                request_index: 0,
                requested_bytes: 9,
                max_request_bytes: 8,
            })
        ));
    }

    #[test]
    fn config_defaults_round_trip_and_deserialization_revalidates() {
        let defaults = ReadPlanConfig::default();
        assert_eq!(defaults.gap_budget_bytes, DEFAULT_GAP_BUDGET_BYTES);
        assert_eq!(defaults.max_request_bytes, DEFAULT_MAX_REQUEST_BYTES);
        assert_eq!(
            defaults.max_concurrent_requests,
            DEFAULT_MAX_CONCURRENT_REQUESTS
        );
        let json = serde_json::to_string(&defaults).expect("config must serialize");
        assert_eq!(
            serde_json::from_str::<ReadPlanConfig>(&json).expect("config must deserialize"),
            defaults
        );
        assert!(
            serde_json::from_str::<ReadPlanConfig>(
                r#"{"gap_budget_bytes":8,"max_request_bytes":8,"max_concurrent_requests":1}"#
            )
            .is_err(),
            "deserialization must reject an invalid relation"
        );

        let empty = ReadPlan::build(&[], &defaults).expect("empty plan must be valid");
        assert_eq!(empty.planned_request_count(), 0);
        assert_eq!(empty.planned_bytes(), 0);
    }
}
