mod common;

use std::fmt::Write as _;

use common::server::{cleanup_ns, create_ns_api, start_test_server};
use serde::{Serialize, Serializer};
use serde_json::Value;
use zeppelin::types::VectorEntry;

#[derive(Serialize)]
struct RowUpsertBody {
    vectors: Vec<VectorEntry>,
}

#[derive(Serialize)]
struct ColumnarUpsertBody {
    columnar: ColumnarVectorsBody,
}

#[derive(Serialize)]
struct ColumnarVectorsBody {
    ids: Vec<String>,
    dimensions: usize,
    #[serde(serialize_with = "serialize_f32_le")]
    values_f32_le: Vec<f32>,
}

#[allow(clippy::ptr_arg)]
fn serialize_f32_le<S>(values: &Vec<f32>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut bytes = Vec::with_capacity(values.len() * std::mem::size_of::<f32>());
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    serializer.serialize_bytes(&bytes)
}

fn row_msgpack(vectors: Vec<VectorEntry>) -> Vec<u8> {
    rmp_serde::to_vec_named(&RowUpsertBody { vectors }).unwrap()
}

fn columnar_msgpack(ids: Vec<String>, dimensions: usize, values_f32_le: Vec<f32>) -> Vec<u8> {
    rmp_serde::to_vec_named(&ColumnarUpsertBody {
        columnar: ColumnarVectorsBody {
            ids,
            dimensions,
            values_f32_le,
        },
    })
    .unwrap()
}

async fn post_body(
    client: &reqwest::Client,
    base_url: &str,
    ns: &str,
    content_type: &str,
    body: Vec<u8>,
) -> (u16, Value) {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .header("content-type", content_type)
        .body(body)
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let body = resp.json().await.unwrap();
    (status, body)
}

fn vector(id: &str, values: &[f32]) -> VectorEntry {
    VectorEntry {
        id: id.to_string(),
        values: values.to_vec(),
        attributes: None,
    }
}

#[tokio::test]
async fn test_msgpack_row_upsert_same_logical_schema_is_queryable() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 4).await;

    let body = row_msgpack(vec![
        vector("mp-a", &[1.0, 0.0, 0.0, 0.0]),
        vector("mp-b", &[0.0, 1.0, 0.0, 0.0]),
    ]);
    let (status, body) = post_body(&client, &base_url, &ns, "application/msgpack", body).await;
    assert_eq!(status, 200, "body: {body}");
    assert_eq!(body["upserted"], 2);

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 1
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let query: Value = resp.json().await.unwrap();
    assert_eq!(query["results"][0]["id"], "mp-a");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_msgpack_errors_match_json_for_wrong_dimensions() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 4).await;

    let vectors = vec![vector("short-2", &[1.0, 0.0])];
    let json_body = serde_json::to_vec(&RowUpsertBody {
        vectors: vectors.clone(),
    })
    .unwrap();
    let msgpack_body = row_msgpack(vectors);

    let json = post_body(&client, &base_url, &ns, "application/json", json_body).await;
    let msgpack = post_body(&client, &base_url, &ns, "application/msgpack", msgpack_body).await;

    assert_eq!(msgpack.0, json.0);
    assert_eq!(msgpack.1["code"], json.1["code"]);
    assert_eq!(msgpack.1["error"], json.1["error"]);
    assert_eq!(msgpack.1["status"], json.1["status"]);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_msgpack_errors_match_json_for_non_finite_values() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 4).await;

    let json_body = br#"{"vectors":[
        {"id":"good-1","values":[0.1,0.2,0.3,0.4]},
        {"id":"bad-42","values":[0.1,0.2,1e39,0.4]}
    ]}"#
    .to_vec();
    let msgpack_body = row_msgpack(vec![
        vector("good-1", &[0.1, 0.2, 0.3, 0.4]),
        vector("bad-42", &[0.1, 0.2, f32::INFINITY, 0.4]),
    ]);

    let json = post_body(&client, &base_url, &ns, "application/json", json_body).await;
    let msgpack = post_body(&client, &base_url, &ns, "application/msgpack", msgpack_body).await;

    assert_eq!(msgpack.0, json.0);
    assert_eq!(msgpack.1["code"], json.1["code"]);
    assert_eq!(msgpack.1["error"], json.1["error"]);
    assert_eq!(msgpack.1["status"], json.1["status"]);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_msgpack_columnar_upsert_is_queryable() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api(&client, &base_url, 4).await;

    let body = columnar_msgpack(
        vec!["col-a".to_string(), "col-b".to_string()],
        4,
        vec![1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0],
    );
    let (status, body) = post_body(&client, &base_url, &ns, "application/msgpack", body).await;
    assert_eq!(status, 200, "body: {body}");
    assert_eq!(body["upserted"], 2);

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "vector": [1.0, 0.0, 0.0, 0.0],
            "top_k": 1
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let query: Value = resp.json().await.unwrap();
    assert_eq!(query["results"][0]["id"], "col-a");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[test]
fn test_columnar_msgpack_is_at_least_5x_smaller_than_json_for_1536d_batch() {
    let dimensions = 1536;
    let vector_count = 8;
    let ids: Vec<String> = (0..vector_count).map(|idx| format!("vec-{idx}")).collect();
    let values: Vec<f32> = (0..vector_count * dimensions)
        .map(|idx| ((idx % 997) as f32 / 997.0) - 0.5)
        .collect();

    let msgpack = columnar_msgpack(ids.clone(), dimensions, values.clone());
    let mut json = String::from("{\"vectors\":[");
    for (row_idx, id) in ids.iter().enumerate() {
        if row_idx > 0 {
            json.push(',');
        }
        write!(&mut json, "{{\"id\":\"{id}\",\"values\":[").unwrap();
        let start = row_idx * dimensions;
        for (dim_idx, value) in values[start..start + dimensions].iter().enumerate() {
            if dim_idx > 0 {
                json.push(',');
            }
            write!(&mut json, "{:.20}", f64::from(*value)).unwrap();
        }
        json.push_str("]}");
    }
    json.push_str("]}");

    assert!(
        json.len() >= msgpack.len() * 5,
        "expected >=5x reduction, json={} msgpack={} ratio={:.2}",
        json.len(),
        msgpack.len(),
        json.len() as f64 / msgpack.len() as f64
    );
}
