use std::{fs, path::Path, str::FromStr};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::{
    fault::FaultId,
    model::{CollectionRunObservation, HealthState},
    validator::InvariantId,
};

pub const SCENARIO_API_VERSION: &str = "harness.overmesh.io/v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Scenario {
    pub api_version: String,
    pub id: String,
    pub suite: String,
    pub environment: ScenarioEnvironment,
    pub seed: u64,
    pub initial_state: InitialState,
    pub operations: Vec<Operation>,
    #[serde(default)]
    pub faults: Vec<FaultSpec>,
    pub expected: Expected,
    pub invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResponseExpectation {
    pub operation: crate::model::ClientOperation,
    pub status: u16,
    #[serde(default)]
    pub body_hex: Option<String>,
    #[serde(default)]
    pub content_range: Option<String>,
}

impl Scenario {
    pub fn load(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read scenario {}", path.display()))?;
        let scenario: Self = serde_yaml::from_str(&content)
            .with_context(|| format!("failed to parse scenario {}", path.display()))?;
        scenario.validate()?;
        Ok(scenario)
    }

    pub fn validate(&self) -> Result<()> {
        if self.api_version != SCENARIO_API_VERSION {
            bail!(
                "scenario {} uses unsupported apiVersion {}; expected {}",
                self.id,
                self.api_version,
                SCENARIO_API_VERSION
            );
        }
        if self.id.trim().is_empty() {
            bail!("scenario id must not be empty");
        }
        if self.suite.trim().is_empty() {
            bail!("scenario {} suite must not be empty", self.id);
        }
        if self.environment.providers.is_empty() {
            bail!("scenario {} must declare at least one provider", self.id);
        }
        if self.operations.is_empty() {
            bail!("scenario {} must declare at least one operation", self.id);
        }
        for fault in &self.faults {
            FaultId::from_str(&fault.id)
                .with_context(|| format!("scenario {} has invalid fault {}", self.id, fault.id))?;
        }
        for invariant in &self.invariants {
            InvariantId::from_str(invariant).with_context(|| {
                format!("scenario {} has invalid invariant {}", self.id, invariant)
            })?;
        }
        for operation in &self.operations {
            match operation {
                Operation::GetBlob {
                    range: Some(range), ..
                } if range.start > range.end_inclusive => {
                    bail!(
                        "scenario {} has a byte range whose start exceeds its end",
                        self.id
                    );
                }
                Operation::AttemptRepair { source, target, .. } if source == target => {
                    bail!("scenario {} repair source and target must differ", self.id);
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ScenarioEnvironment {
    pub providers: Vec<EnvironmentProvider>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EnvironmentProvider {
    Model,
    Azurite,
    Azure,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InitialState {
    pub blob: InitialBlobState,
    #[serde(default = "default_retention_ms")]
    pub retention_ms: u64,
}

const fn default_retention_ms() -> u64 {
    1_000
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InitialBlobState {
    Absent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum Operation {
    PutBlob {
        blob: String,
        dataset: String,
        write_id: String,
    },
    DeleteBlob {
        blob: String,
        write_id: String,
    },
    Head {
        blob: String,
        #[serde(default)]
        if_match: Option<Condition>,
        #[serde(default)]
        if_none_match: Option<Condition>,
    },
    GetBlob {
        blob: String,
        #[serde(default)]
        if_match: Option<Condition>,
        #[serde(default)]
        if_none_match: Option<Condition>,
        #[serde(default)]
        range: Option<ByteRange>,
    },
    TamperContent {
        blob: String,
        replica: ReplicaName,
    },
    InvalidateSignature {
        blob: String,
        replica: ReplicaName,
    },
    UseUntrustedSigningKey {
        blob: String,
        replica: ReplicaName,
    },
    RemoveReplica {
        blob: String,
        replica: ReplicaName,
    },
    ObservePreparedPublication {
        blob: String,
    },
    ObserveReplicaConsistency {
        blob: String,
    },
    AttemptRepair {
        blob: String,
        source: ReplicaName,
        target: ReplicaName,
    },
    AdvanceTime {
        blob: String,
        milliseconds: u64,
    },
    Collect {
        blob: String,
    },
    AttemptResurrection {
        blob: String,
        logical_version: u64,
    },
    Reconcile {
        blob: String,
    },
}

impl Operation {
    pub fn blob(&self) -> &str {
        match self {
            Self::PutBlob { blob, .. }
            | Self::DeleteBlob { blob, .. }
            | Self::Head { blob, .. }
            | Self::GetBlob { blob, .. }
            | Self::TamperContent { blob, .. }
            | Self::InvalidateSignature { blob, .. }
            | Self::UseUntrustedSigningKey { blob, .. }
            | Self::RemoveReplica { blob, .. }
            | Self::ObservePreparedPublication { blob }
            | Self::ObserveReplicaConsistency { blob }
            | Self::AttemptRepair { blob, .. }
            | Self::AdvanceTime { blob, .. }
            | Self::Collect { blob }
            | Self::AttemptResurrection { blob, .. }
            | Self::Reconcile { blob } => blob,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Condition {
    Current,
    Stale,
    Any,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ByteRange {
    pub start: u64,
    pub end_inclusive: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaName {
    A,
    B,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultSpec {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Expected {
    pub client_outcome: ClientOutcomeExpectation,
    pub visible_blob: VisibleBlobExpectation,
    pub health_after_operations: HealthState,
    pub health_after_reconciliation: HealthState,
    #[serde(default)]
    pub responses: Vec<ResponseExpectation>,
    #[serde(default)]
    pub current_logical_version: Option<u64>,
    #[serde(default)]
    pub physical_content_versions: Option<Vec<u64>>,
    #[serde(default)]
    pub collected_versions: Option<Vec<u64>>,
    #[serde(default)]
    pub collection_runs: Vec<CollectionRunObservation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientOutcomeExpectation {
    pub class: ExpectedOutcomeClass,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExpectedOutcomeClass {
    Success,
    Failure,
    Ambiguous,
    FailureOrAmbiguous,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VisibleBlobExpectation {
    pub present: bool,
    pub must_never_be_prepared: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unknown_api_version() {
        let scenario = Scenario {
            api_version: "unsupported/v1".to_owned(),
            id: "TEST-001".to_owned(),
            suite: "test".to_owned(),
            environment: ScenarioEnvironment {
                providers: vec![EnvironmentProvider::Model],
            },
            seed: 1,
            initial_state: InitialState {
                blob: InitialBlobState::Absent,
                retention_ms: default_retention_ms(),
            },
            operations: vec![Operation::Head {
                blob: "/container/blob".to_owned(),
                if_match: None,
                if_none_match: None,
            }],
            faults: Vec::new(),
            expected: Expected {
                client_outcome: ClientOutcomeExpectation {
                    class: ExpectedOutcomeClass::Failure,
                },
                visible_blob: VisibleBlobExpectation {
                    present: false,
                    must_never_be_prepared: true,
                },
                health_after_operations: HealthState::Absent,
                health_after_reconciliation: HealthState::Absent,
                responses: Vec::new(),
                current_logical_version: None,
                physical_content_versions: None,
                collected_versions: None,
                collection_runs: Vec::new(),
            },
            invariants: vec!["INVARIANT-009".to_owned()],
        };

        assert!(scenario.validate().is_err());
    }

    #[test]
    fn rejects_unknown_operation_fields() {
        let yaml = r#"
apiVersion: harness.overmesh.io/v1
id: STRICT-001
suite: strict-schema
environment:
  providers: [model]
seed: 1
initialState:
  blob: absent
operations:
  - action: head
    blob: /container/blob
    ignored: true
faults: []
expected:
  clientOutcome:
    class: failure
  visibleBlob:
    present: false
    mustNeverBePrepared: true
  healthAfterOperations: ABSENT
  healthAfterReconciliation: ABSENT
invariants:
  - INVARIANT-009
"#;

        assert!(serde_yaml::from_str::<Scenario>(yaml).is_err());
    }
}
