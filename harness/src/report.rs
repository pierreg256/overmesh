use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use serde::Serialize;

use crate::{
    model::{
        ActualOutcomeClass, CollectionRunObservation, ConsistencyDecision, HealthState,
        PublicObservation, RepairAttempt, ResurrectionAttempt,
    },
    validator::CheckResult,
};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScenarioReport {
    pub api_version: &'static str,
    pub project_version: &'static str,
    pub harness_version: &'static str,
    pub scenario_id: String,
    pub suite: String,
    pub seed: u64,
    pub generated_at_unix_ms: u128,
    pub outcome: ActualOutcomeClass,
    pub health_after_operations: HealthState,
    pub health_after_reconciliation: HealthState,
    pub responses: Vec<PublicObservation>,
    pub consistency_decisions: Vec<ConsistencyDecision>,
    pub repair_attempts: Vec<RepairAttempt>,
    pub resurrection_attempts: Vec<ResurrectionAttempt>,
    pub current_logical_version: Option<u64>,
    pub physical_content_versions: Vec<u64>,
    pub collected_versions: Vec<u64>,
    pub collection_runs: Vec<CollectionRunObservation>,
    pub passed: bool,
    pub checks: Vec<CheckResult>,
}

impl ScenarioReport {
    pub fn write(&self, directory: &Path) -> Result<PathBuf> {
        fs::create_dir_all(directory).with_context(|| {
            format!("failed to create report directory {}", directory.display())
        })?;
        let path = directory.join(format!(
            "{}-{}.json",
            self.scenario_id.to_ascii_lowercase(),
            self.generated_at_unix_ms
        ));
        let content = serde_json::to_vec_pretty(self)?;
        fs::write(&path, content)
            .with_context(|| format!("failed to write report {}", path.display()))?;
        Ok(path)
    }
}

pub fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after Unix epoch")
        .as_millis()
}
