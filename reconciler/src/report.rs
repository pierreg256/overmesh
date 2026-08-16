use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HealthState {
    Absent,
    Healthy,
    Drifted,
    Missing,
    Tampered,
    Quarantined,
    Tombstoned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReconciliationAction {
    None,
    Repaired,
    Quarantined,
    Recovered,
    GarbageCollected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlobReport {
    pub blob: Option<String>,
    pub head_object: String,
    pub health_before: HealthState,
    pub health_after: HealthState,
    pub action: ReconciliationAction,
    pub source_replica: Option<String>,
    pub target_replica: Option<String>,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CycleReport {
    pub api_version: &'static str,
    pub project_version: &'static str,
    pub ring_version: u64,
    pub started_at_unix_ms: u64,
    pub completed_at_unix_ms: u64,
    pub blobs: Vec<BlobReport>,
}
