//! HTTP client abstraction for Zeppelin API.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Vector entry for upsert.
#[derive(Debug, Clone, Serialize)]
pub struct Vector {
    pub id: String,
    pub values: Vec<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<HashMap<String, serde_json::Value>>,
}

/// Query request.
#[derive(Debug, Clone, Serialize)]
pub struct QueryRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f32>>,
    pub top_k: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nprobe: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rank_by: Option<serde_json::Value>,
}

/// Search result entry.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct SearchResult {
    pub id: String,
    pub score: f32,
}

/// Query response.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct QueryResponse {
    pub results: Vec<SearchResult>,
}

/// Trait for benchmark clients (Zeppelin and turbopuffer share this interface).
#[async_trait::async_trait]
pub trait BenchClient: Send + Sync {
    async fn create_namespace(
        &self,
        name: &str,
        dimensions: usize,
        extra: Option<serde_json::Value>,
    ) -> Result<(), String>;

    async fn upsert(&self, namespace: &str, vectors: &[Vector]) -> Result<(), String>;

    async fn query(
        &self,
        namespace: &str,
        request: &QueryRequest,
    ) -> Result<QueryResponse, String>;

    async fn delete_namespace(&self, name: &str) -> Result<(), String>;

    #[allow(dead_code)]
    fn name(&self) -> &str;
}

/// Zeppelin HTTP client.
pub struct ZeppelinClient {
    base_url: String,
    http: reqwest::Client,
}

impl ZeppelinClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http: reqwest::Client::builder()
                .pool_max_idle_per_host(64)
                .build()
                .expect("failed to build HTTP client"),
        }
    }
}

#[async_trait::async_trait]
impl BenchClient for ZeppelinClient {
    async fn create_namespace(
        &self,
        name: &str,
        dimensions: usize,
        extra: Option<serde_json::Value>,
    ) -> Result<(), String> {
        let mut body = serde_json::json!({
            "name": name,
            "dimensions": dimensions,
            "distance_metric": "cosine",
        });

        // Merge extra fields (e.g. full_text_search config)
        if let Some(extra) = extra {
            if let (Some(base_map), Some(extra_map)) =
                (body.as_object_mut(), extra.as_object())
            {
                for (k, v) in extra_map {
                    base_map.insert(k.clone(), v.clone());
                }
            }
        }

        let resp = self
            .http
            .post(format!("{}/v1/namespaces", self.base_url))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("create namespace request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(format!("create namespace failed ({status}): {text}"));
        }
        Ok(())
    }

    async fn upsert(&self, namespace: &str, vectors: &[Vector]) -> Result<(), String> {
        let body = serde_json::json!({ "vectors": vectors });
        let resp = self
            .http
            .post(format!("{}/v1/namespaces/{}/vectors", self.base_url, namespace))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("upsert request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(format!("upsert failed ({status}): {text}"));
        }
        Ok(())
    }

    async fn query(
        &self,
        namespace: &str,
        request: &QueryRequest,
    ) -> Result<QueryResponse, String> {
        let resp = self
            .http
            .post(format!("{}/v1/namespaces/{}/query", self.base_url, namespace))
            .json(request)
            .send()
            .await
            .map_err(|e| format!("query request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(format!("query failed ({status}): {text}"));
        }

        resp.json::<QueryResponse>()
            .await
            .map_err(|e| format!("query response parse failed: {e}"))
    }

    async fn delete_namespace(&self, name: &str) -> Result<(), String> {
        let resp = self
            .http
            .delete(format!("{}/v1/namespaces/{}", self.base_url, name))
            .send()
            .await
            .map_err(|e| format!("delete namespace request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(format!("delete namespace failed ({status}): {text}"));
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "zeppelin"
    }
}
