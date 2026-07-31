mod common;

use std::ops::Range;

use bytes::Bytes;
use common::counting::counting_store;
use common::harness::TestHarness;
use zeppelin::storage::read_plan::{execute_read_plan, ReadPlan, ReadPlanConfig, ReadRequest};

const OBJECT_BYTES: usize = 256 * 1024;
const OBJECTS: usize = 4;
const READ_BYTES: usize = 1024;
const LOGICAL_READS_PER_OBJECT: usize = 16;
const EXPECTED_PHYSICAL_REQUESTS: usize = 8;
const EXPECTED_PLANNED_BYTES: u64 = 95_520;

fn logical_ranges() -> Vec<Range<usize>> {
    vec![
        4_096..5_120,
        5_120..6_144,
        7_000..8_024,
        9_000..10_024,
        11_000..12_024,
        13_000..14_024,
        15_000..16_024,
        7_000..8_024,
        131_072..132_096,
        132_096..133_120,
        134_000..135_024,
        136_000..137_024,
        138_000..139_024,
        140_000..141_024,
        142_000..143_024,
        134_000..135_024,
    ]
}

fn object_bytes(object_index: usize) -> Bytes {
    Bytes::from(
        (0..OBJECT_BYTES)
            .map(|offset| ((offset + object_index * 17) % 251) as u8)
            .collect::<Vec<_>>(),
    )
}

#[tokio::test]
async fn planned_reads_match_naive_bytes_and_observed_ranges() {
    let harness = TestHarness::new().await;
    let mut keys = Vec::with_capacity(OBJECTS);
    for object_index in 0..OBJECTS {
        let key = harness.key(&format!("read-plan/object-{object_index}.bin"));
        harness
            .store
            .put(&key, object_bytes(object_index))
            .await
            .expect("fixture object write must succeed");
        keys.push(key);
    }

    let ranges = logical_ranges();
    assert_eq!(ranges.len(), LOGICAL_READS_PER_OBJECT);
    assert!(ranges.iter().all(|range| range.len() == READ_BYTES));

    let mut requests = Vec::with_capacity(OBJECTS * LOGICAL_READS_PER_OBJECT);
    for range_index in (0..LOGICAL_READS_PER_OBJECT).rev() {
        for key in &keys {
            requests.push(ReadRequest {
                object_key: key.clone(),
                range: ranges[range_index].clone(),
            });
        }
    }

    let mut naive = Vec::with_capacity(requests.len());
    for request in &requests {
        naive.push(
            harness
                .store
                .get_range(&request.object_key, request.range.clone())
                .await
                .expect("naive range read must succeed"),
        );
    }

    let config =
        ReadPlanConfig::new(8 * 1024, 32 * 1024, 4).expect("integration bounds must be valid");
    let plan = ReadPlan::build(&requests, &config).expect("read plan must build");
    assert_eq!(plan.planned_request_count(), EXPECTED_PHYSICAL_REQUESTS);
    assert_eq!(plan.planned_bytes(), EXPECTED_PLANNED_BYTES);

    let (counted_store, counter) = counting_store(&harness.store);
    let planned = execute_read_plan(&counted_store, &plan)
        .await
        .expect("planned reads must succeed");

    assert_eq!(planned, naive);
    assert_eq!(planned.len(), OBJECTS * LOGICAL_READS_PER_OBJECT);
    assert_eq!(
        counter.total_observed_gets(),
        plan.planned_request_count() as u64
    );
    for key in &keys {
        let mut observed = counter.ranges_for(key);
        observed.sort_unstable_by_key(|range| (range.start, range.end));
        assert_eq!(observed, vec![4_096..16_024, 131_072..143_024]);
    }

    println!(
        "read_plan_collapse logical_reads={} physical_requests={} planned_bytes={}",
        requests.len(),
        plan.planned_request_count(),
        plan.planned_bytes()
    );
    harness.cleanup().await;
}
