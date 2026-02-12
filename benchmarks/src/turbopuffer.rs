//! turbopuffer API adapter for competitive benchmarking.
//!
//! Maps the BenchClient trait to turbopuffer's REST API so the same
//! scenarios can run against both targets.

use crate::client::{BenchClient, QueryRequest, QueryResponse, SearchResult, Vector};

pub struct TurbopufferClient {
    base_url: String,
    api_key: String,
    http: reqwest::Client,
}

impl TurbopufferClient {
    pub fn new(base_url: &str, api_key: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
            http: reqwest::Client::builder()
                .pool_max_idle_per_host(64)
                .build()
                .expect("failed to build HTTP client"),
        }
    }

    fn auth_header(&self) -> String {
        format!("Bearer {}", self.api_key)
    }
}

#[async_trait::async_trait]
impl BenchClient for TurbopufferClient {
    async fn create_namespace(
        &self,
        _name: &str,
        _dimensions: usize,
        _extra: Option<serde_json::Value>,
    ) -> Result<(), String> {
        // turbopuffer auto-creates namespaces on first upsert
        Ok(())
    }

    async fn upsert(&self, namespace: &str, vectors: &[Vector]) -> Result<(), String> {
        // turbopuffer format: { ids: [...], vectors: [[...]], attributes: {...} }
        let ids: Vec<&str> = vectors.iter().map(|v| v.id.as_str()).collect();
        let vecs: Vec<&Vec<f32>> = vectors.iter().map(|v| &v.values).collect();

        // Build column-oriented attributes
        let mut attr_columns: std::collections::HashMap<String, Vec<serde_json::Value>> =
            std::collections::HashMap::new();
        for v in vectors {
            if let Some(ref attrs) = v.attributes {
                for (k, val) in attrs {
                    attr_columns
                        .entry(k.clone())
                        .or_insert_with(|| Vec::with_capacity(vectors.len()))
                        .push(val.clone());
                }
            }
        }

        let body = serde_json::json!({
            "ids": ids,
            "vectors": vecs,
            "attributes": attr_columns,
        });

        let resp = self
            .http
            .post(format!(
                "{}/v1/vectors/{}",
                self.base_url, namespace
            ))
            .header("Authorization", self.auth_header())
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("turbopuffer upsert failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(format!("turbopuffer upsert failed ({status}): {text}"));
        }
        Ok(())
    }

    async fn query(
        &self,
        namespace: &str,
        request: &QueryRequest,
    ) -> Result<QueryResponse, String> {
        let mut body = serde_json::json!({
            "top_k": request.top_k,
        });

        if let Some(ref vector) = request.vector {
            body["vector"] = serde_json::json!(vector);
        }
        if let Some(ref filter) = request.filter {
            body["filters"] = filter.clone();
        }
        if let Some(ref rank_by) = request.rank_by {
            body["rank_by"] = rank_by.clone();
        }

        let resp = self
            .http
            .post(format!(
                "{}/v1/vectors/{}/query",
                self.base_url, namespace
            ))
            .header("Authorization", self.auth_header())
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("turbopuffer query failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(format!("turbopuffer query failed ({status}): {text}"));
        }

        // turbopuffer returns { ids: [...], dist: [...] }
        let data: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("turbopuffer response parse failed: {e}"))?;

        let empty = vec![];
        let ids = data["ids"].as_array().unwrap_or(&empty);
        let dists = data["dist"].as_array().unwrap_or(&empty);

        let results: Vec<SearchResult> = ids
            .iter()
            .zip(dists.iter())
            .map(|(id, dist)| SearchResult {
                id: id.as_str().unwrap_or("").to_string(),
                score: dist.as_f64().unwrap_or(0.0) as f32,
            })
            .collect();

        Ok(QueryResponse { results })
    }

    async fn delete_namespace(&self, namespace: &str) -> Result<(), String> {
        let resp = self
            .http
            .delete(format!(
                "{}/v1/vectors/{}",
                self.base_url, namespace
            ))
            .header("Authorization", self.auth_header())
            .send()
            .await
            .map_err(|e| format!("turbopuffer delete failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(format!("turbopuffer delete failed ({status}): {text}"));
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "turbopuffer"
    }
}
