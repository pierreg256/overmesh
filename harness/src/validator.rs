use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    model::{ActualOutcomeClass, BlobState, CommitState, ConsistencyIdentifier, HealthState},
    scenario::{Expected, ExpectedOutcomeClass},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum InvariantId {
    #[serde(rename = "INVARIANT-001")]
    Invariant001,
    #[serde(rename = "INVARIANT-002")]
    Invariant002,
    #[serde(rename = "INVARIANT-003")]
    Invariant003,
    #[serde(rename = "INVARIANT-004")]
    Invariant004,
    #[serde(rename = "INVARIANT-005")]
    Invariant005,
    #[serde(rename = "INVARIANT-006")]
    Invariant006,
    #[serde(rename = "INVARIANT-007")]
    Invariant007,
    #[serde(rename = "INVARIANT-008")]
    Invariant008,
    #[serde(rename = "INVARIANT-009")]
    Invariant009,
    #[serde(rename = "INVARIANT-010")]
    Invariant010,
    #[serde(rename = "INVARIANT-011")]
    Invariant011,
    #[serde(rename = "INVARIANT-012")]
    Invariant012,
    #[serde(rename = "INVARIANT-013")]
    Invariant013,
    #[serde(rename = "INVARIANT-014")]
    Invariant014,
}

impl fmt::Display for InvariantId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let number = match self {
            Self::Invariant001 => 1,
            Self::Invariant002 => 2,
            Self::Invariant003 => 3,
            Self::Invariant004 => 4,
            Self::Invariant005 => 5,
            Self::Invariant006 => 6,
            Self::Invariant007 => 7,
            Self::Invariant008 => 8,
            Self::Invariant009 => 9,
            Self::Invariant010 => 10,
            Self::Invariant011 => 11,
            Self::Invariant012 => 12,
            Self::Invariant013 => 13,
            Self::Invariant014 => 14,
        };
        write!(formatter, "INVARIANT-{number:03}")
    }
}

impl FromStr for InvariantId {
    type Err = InvariantParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "INVARIANT-001" => Ok(Self::Invariant001),
            "INVARIANT-002" => Ok(Self::Invariant002),
            "INVARIANT-003" => Ok(Self::Invariant003),
            "INVARIANT-004" => Ok(Self::Invariant004),
            "INVARIANT-005" => Ok(Self::Invariant005),
            "INVARIANT-006" => Ok(Self::Invariant006),
            "INVARIANT-007" => Ok(Self::Invariant007),
            "INVARIANT-008" => Ok(Self::Invariant008),
            "INVARIANT-009" => Ok(Self::Invariant009),
            "INVARIANT-010" => Ok(Self::Invariant010),
            "INVARIANT-011" => Ok(Self::Invariant011),
            "INVARIANT-012" => Ok(Self::Invariant012),
            "INVARIANT-013" => Ok(Self::Invariant013),
            "INVARIANT-014" => Ok(Self::Invariant014),
            _ => Err(InvariantParseError(value.to_owned())),
        }
    }
}

#[derive(Debug, Error)]
#[error("unsupported invariant id: {0}")]
pub struct InvariantParseError(String);

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckResult {
    pub id: String,
    pub passed: bool,
    pub detail: String,
}

pub fn validate_invariant(id: InvariantId, blob: &BlobState) -> CheckResult {
    let (passed, detail) = match id {
        InvariantId::Invariant001 => {
            let valid = !blob.acknowledged_success
                || (blob.replica_a.commit_state == CommitState::Committed
                    && blob.replica_b.commit_state == CommitState::Committed
                    && same_committed_head(blob));
            (valid, "acknowledged writes are committed on both replicas")
        }
        InvariantId::Invariant002 => {
            let valid = blob.health() != HealthState::Healthy
                || blob.replica_a.content_sha256 == blob.replica_b.content_sha256;
            (valid, "healthy replica content hashes match")
        }
        InvariantId::Invariant003 => {
            let valid = blob.health() != HealthState::Healthy
                || blob.replica_a.commit_manifest_sha256 == blob.replica_b.commit_manifest_sha256;
            (valid, "healthy signed committed manifests match")
        }
        InvariantId::Invariant004 => {
            let valid = blob.health() != HealthState::Healthy
                || blob.replica_a.ring_version == blob.replica_b.ring_version;
            (valid, "healthy Ring versions match")
        }
        InvariantId::Invariant005 => {
            let valid = blob.health() != HealthState::Healthy
                || blob.replica_a.logical_version == blob.replica_b.logical_version;
            (valid, "healthy logical versions match")
        }
        InvariantId::Invariant006 => {
            let valid = blob
                .consistency_decisions
                .iter()
                .all(|decision| !decision.accepted || decision.signatures_valid);
            (
                valid,
                "every accepted consistency decision used validly signed metadata",
            )
        }
        InvariantId::Invariant007 => {
            let valid = blob
                .consistency_decisions
                .iter()
                .all(|decision| !decision.accepted || decision.signing_keys_trusted);
            (
                valid,
                "every accepted consistency decision used trusted signing keys",
            )
        }
        InvariantId::Invariant008 => {
            let corruption_exists =
                blob.replica_a.content_tampered || blob.replica_b.content_tampered;
            (
                !corruption_exists || blob.tamper_detected,
                "triggered validation detects content or metadata corruption",
            )
        }
        InvariantId::Invariant009 => (
            !blob.prepared_is_visible(),
            "prepared and unsigned objects are not publicly visible",
        ),
        InvariantId::Invariant010 => {
            let tombstoned = blob.replica_a.commit_state == CommitState::Tombstoned
                && blob.replica_b.commit_state == CommitState::Tombstoned;
            let older_generation_applied = blob.resurrection_attempts.iter().any(|attempt| {
                attempt.attempted_logical_version < attempt.high_water_logical_version
                    && attempt.applied
            });
            (
                (!tombstoned || !blob.is_publicly_visible()) && !older_generation_applied,
                "valid high-water state and tombstones prevent resurrection",
            )
        }
        InvariantId::Invariant011 => {
            let valid = blob
                .consistency_decisions
                .iter()
                .all(|decision| decision.identifier == ConsistencyIdentifier::LogicalEtag);
            (
                valid,
                "consistency decisions use logical ETags rather than backend ETags",
            )
        }
        InvariantId::Invariant012 => {
            let valid = !blob.repaired_from_tampered_source
                && blob.repair_attempts.iter().all(|attempt| {
                    let unsafe_source = attempt.source_tampered
                        || attempt.source_quarantined
                        || !attempt.source_signature_valid
                        || !attempt.source_signing_key_trusted;
                    !unsafe_source || !attempt.applied
                });
            (
                valid,
                "tampered, quarantined, invalid, or untrusted replicas are refused as repair sources",
            )
        }
        InvariantId::Invariant013 => (
            blob.public_observations
                .iter()
                .filter(|observation| {
                    observation.operation == crate::model::ClientOperation::PutBlock
                })
                .all(|observation| !observation.exposed),
            "staged blocks are never publicly visible",
        ),
        InvariantId::Invariant014 => (
            !blob.committed_from_tampered_stage,
            "tampered staged blocks are never used as commit or repair sources",
        ),
    };
    CheckResult {
        id: id.to_string(),
        passed,
        detail: detail.to_owned(),
    }
}

fn same_committed_head(blob: &BlobState) -> bool {
    blob.replica_a.content_sha256 == blob.replica_b.content_sha256
        && blob.replica_a.commit_manifest_sha256 == blob.replica_b.commit_manifest_sha256
        && blob.replica_a.logical_version == blob.replica_b.logical_version
        && blob.replica_a.logical_etag == blob.replica_b.logical_etag
        && blob.replica_a.ring_version == blob.replica_b.ring_version
        && blob.replica_a.write_id == blob.replica_b.write_id
}

pub fn validate_expected(
    expected: &Expected,
    outcome: ActualOutcomeClass,
    blob: &BlobState,
    health_after_reconciliation: HealthState,
) -> Vec<CheckResult> {
    vec![
        CheckResult {
            id: "EXPECTED-CLIENT-OUTCOME".to_owned(),
            passed: outcome_matches(expected.client_outcome.class, outcome),
            detail: format!(
                "expected {:?}, observed {:?}",
                expected.client_outcome.class, outcome
            ),
        },
        CheckResult {
            id: "EXPECTED-VISIBILITY".to_owned(),
            passed: expected.visible_blob.present == blob.is_publicly_visible(),
            detail: format!(
                "expected visible {}, observed {}",
                expected.visible_blob.present,
                blob.is_publicly_visible()
            ),
        },
        CheckResult {
            id: "EXPECTED-PREPARED-HIDDEN".to_owned(),
            passed: !expected.visible_blob.must_never_be_prepared || !blob.prepared_is_visible(),
            detail: "prepared objects remain hidden".to_owned(),
        },
        CheckResult {
            id: "EXPECTED-HEALTH-AFTER-OPERATIONS".to_owned(),
            passed: expected.health_after_operations == blob.health(),
            detail: format!(
                "expected {:?}, observed {:?}",
                expected.health_after_operations,
                blob.health()
            ),
        },
        CheckResult {
            id: "EXPECTED-HEALTH-AFTER-RECONCILIATION".to_owned(),
            passed: expected.health_after_reconciliation == health_after_reconciliation,
            detail: format!(
                "expected {:?}, observed {:?}",
                expected.health_after_reconciliation, health_after_reconciliation
            ),
        },
        CheckResult {
            id: "EXPECTED-RESPONSES".to_owned(),
            passed: expected.responses.is_empty()
                || (expected.responses.len() == blob.public_observations.len()
                    && expected
                        .responses
                        .iter()
                        .zip(&blob.public_observations)
                        .all(|(expected, actual)| {
                            expected.operation == actual.operation
                                && expected.status == actual.status
                                && expected.body_hex == actual.body_hex
                                && expected.content_range == actual.content_range
                        })),
            detail: format!(
                "expected {} response observations, observed {}",
                expected.responses.len(),
                blob.public_observations.len()
            ),
        },
        CheckResult {
            id: "EXPECTED-CURRENT-LOGICAL-VERSION".to_owned(),
            passed: expected
                .current_logical_version
                .is_none_or(|version| blob.current_logical_version() == Some(version)),
            detail: format!(
                "expected {:?}, observed {:?}",
                expected.current_logical_version,
                blob.current_logical_version()
            ),
        },
        CheckResult {
            id: "EXPECTED-PHYSICAL-CONTENT-VERSIONS".to_owned(),
            passed: expected
                .physical_content_versions
                .as_ref()
                .is_none_or(|versions| versions == &blob.physical_content_versions()),
            detail: format!(
                "expected {:?}, observed {:?}",
                expected.physical_content_versions,
                blob.physical_content_versions()
            ),
        },
        CheckResult {
            id: "EXPECTED-COLLECTED-VERSIONS".to_owned(),
            passed: expected
                .collected_versions
                .as_ref()
                .is_none_or(|versions| versions == &blob.collected_versions()),
            detail: format!(
                "expected {:?}, observed {:?}",
                expected.collected_versions,
                blob.collected_versions()
            ),
        },
        CheckResult {
            id: "EXPECTED-COLLECTION-RUNS".to_owned(),
            passed: expected.collection_runs.is_empty()
                || expected.collection_runs == blob.collection_runs,
            detail: format!(
                "expected {:?}, observed {:?}",
                expected.collection_runs, blob.collection_runs
            ),
        },
    ]
}

fn outcome_matches(expected: ExpectedOutcomeClass, actual: ActualOutcomeClass) -> bool {
    match expected {
        ExpectedOutcomeClass::Success => actual == ActualOutcomeClass::Success,
        ExpectedOutcomeClass::Failure => actual == ActualOutcomeClass::Failure,
        ExpectedOutcomeClass::Ambiguous => actual == ActualOutcomeClass::Ambiguous,
        ExpectedOutcomeClass::FailureOrAmbiguous => {
            matches!(
                actual,
                ActualOutcomeClass::Failure | ActualOutcomeClass::Ambiguous
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        model::{
            ClientOperation, ConsistencyDecision, DecisionOperation, PublicObservation,
            RepairAttempt,
        },
        scenario::ReplicaName,
    };

    #[test]
    fn invariant_006_rejects_an_accepted_invalid_signature() {
        let mut blob = BlobState::default();
        blob.consistency_decisions.push(ConsistencyDecision {
            operation: DecisionOperation::Head,
            accepted: true,
            signatures_valid: false,
            signing_keys_trusted: true,
            identifier: ConsistencyIdentifier::LogicalEtag,
            backend_etags_distinct: true,
        });
        assert!(!validate_invariant(InvariantId::Invariant006, &blob).passed);
    }

    #[test]
    fn invariant_007_rejects_an_accepted_untrusted_key() {
        let mut blob = BlobState::default();
        blob.consistency_decisions.push(ConsistencyDecision {
            operation: DecisionOperation::Head,
            accepted: true,
            signatures_valid: true,
            signing_keys_trusted: false,
            identifier: ConsistencyIdentifier::LogicalEtag,
            backend_etags_distinct: true,
        });
        assert!(!validate_invariant(InvariantId::Invariant007, &blob).passed);
    }

    #[test]
    fn invariant_009_rejects_observed_prepared_publication() {
        let mut blob = BlobState::default();
        blob.public_observations.push(PublicObservation {
            operation: ClientOperation::ObservePreparedPublication,
            status: 200,
            exposed: true,
            replica_a_state: CommitState::Prepared,
            replica_b_state: CommitState::Prepared,
            signatures_valid: true,
            signing_keys_trusted: true,
            body_hex: None,
            content_range: None,
        });
        assert!(!validate_invariant(InvariantId::Invariant009, &blob).passed);
    }

    #[test]
    fn invariant_011_rejects_backend_etag_consistency_decision() {
        let mut blob = BlobState::default();
        blob.consistency_decisions.push(ConsistencyDecision {
            operation: DecisionOperation::ObserveReplicaConsistency,
            accepted: true,
            signatures_valid: true,
            signing_keys_trusted: true,
            identifier: ConsistencyIdentifier::BackendEtag,
            backend_etags_distinct: true,
        });
        assert!(!validate_invariant(InvariantId::Invariant011, &blob).passed);
    }

    #[test]
    fn invariant_012_rejects_repair_from_tampered_source() {
        let mut blob = BlobState::default();
        blob.repair_attempts.push(RepairAttempt {
            source: ReplicaName::A,
            target: ReplicaName::B,
            source_tampered: true,
            source_quarantined: false,
            source_signature_valid: true,
            source_signing_key_trusted: true,
            applied: true,
        });
        assert!(!validate_invariant(InvariantId::Invariant012, &blob).passed);
    }
}
