use chrono::{DateTime, Utc};

use crate::error::ZeppelinError;
use crate::storage::ZeppelinStore;
use crate::wal::manifest::{Manifest, NamedSnapshot};

/// Resolve a retained point-in-time target into a historical manifest.
pub async fn resolve_manifest(
    store: &ZeppelinStore,
    namespace: &str,
    as_of: &str,
) -> Result<Manifest, ZeppelinError> {
    if as_of.is_empty() {
        return Err(ZeppelinError::Validation(
            "as_of must be a generation, RFC3339 timestamp, or snapshot:name".into(),
        ));
    }

    if let Some(snapshot_name) = as_of.strip_prefix("snapshot:") {
        if snapshot_name.is_empty() {
            return Err(ZeppelinError::Validation(
                "as_of snapshot target must be snapshot:<name>".into(),
            ));
        }
        let snapshot = NamedSnapshot::read(store, namespace, snapshot_name)
            .await?
            .ok_or_else(|| ZeppelinError::SnapshotNotFound {
                namespace: namespace.to_string(),
                name: snapshot_name.to_string(),
            })?;
        let target = format!("snapshot:{snapshot_name}");
        return read_retained_history_generation(store, namespace, snapshot.generation, &target)
            .await;
    }

    if let Ok(generation) = as_of.parse::<u64>() {
        return read_retained_history_generation(store, namespace, generation, as_of).await;
    }

    let timestamp = DateTime::parse_from_rfc3339(as_of)
        .map_err(|e| {
            ZeppelinError::Validation(format!(
                "as_of must be a generation, RFC3339 timestamp, or snapshot:name: {e}"
            ))
        })?
        .with_timezone(&Utc);
    resolve_history_at_or_before_timestamp(store, namespace, timestamp, as_of).await
}

async fn read_retained_history_generation(
    store: &ZeppelinStore,
    namespace: &str,
    generation: u64,
    target: &str,
) -> Result<Manifest, ZeppelinError> {
    let live_version = live_manifest_version(store, namespace).await?;
    if generation == 0 || generation > live_version {
        return Err(point_in_time_not_retained(namespace, target));
    }

    Manifest::read_history(store, namespace, generation)
        .await?
        .ok_or_else(|| point_in_time_not_retained(namespace, target))
}

async fn resolve_history_at_or_before_timestamp(
    store: &ZeppelinStore,
    namespace: &str,
    timestamp: DateTime<Utc>,
    target: &str,
) -> Result<Manifest, ZeppelinError> {
    let live_version = live_manifest_version(store, namespace).await?;
    let mut selected = None;
    for entry in Manifest::list_history(store, namespace).await? {
        if entry.version > live_version {
            break;
        }
        let manifest = Manifest::read_history(store, namespace, entry.version)
            .await?
            .ok_or_else(|| ZeppelinError::NotFound { key: entry.key })?;
        if manifest.updated_at <= timestamp {
            selected = Some(manifest);
        }
    }

    selected.ok_or_else(|| point_in_time_not_retained(namespace, target))
}

async fn live_manifest_version(
    store: &ZeppelinStore,
    namespace: &str,
) -> Result<u64, ZeppelinError> {
    Ok(Manifest::read(store, namespace)
        .await?
        .map_or(0, |manifest| manifest.version()))
}

fn point_in_time_not_retained(namespace: &str, target: &str) -> ZeppelinError {
    ZeppelinError::PointInTimeNotRetained {
        namespace: namespace.to_string(),
        target: target.to_string(),
    }
}
