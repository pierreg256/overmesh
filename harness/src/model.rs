use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::scenario::ReplicaName;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HealthState {
    Absent,
    Healthy,
    Drifted,
    Missing,
    Tampered,
    Quarantined,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CommitState {
    Absent,
    ContentOnly,
    Prepared,
    Committed,
    Tombstoned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ActualOutcomeClass {
    Success,
    Failure,
    Ambiguous,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConsistencyIdentifier {
    LogicalEtag,
    BackendEtag,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DecisionOperation {
    Put,
    Delete,
    Head,
    Get,
    Reconcile,
    ObserveReplicaConsistency,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ClientOperation {
    PutBlob,
    DeleteBlob,
    Head,
    GetBlob,
    ObservePreparedPublication,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsistencyDecision {
    pub operation: DecisionOperation,
    pub accepted: bool,
    pub signatures_valid: bool,
    pub signing_keys_trusted: bool,
    pub identifier: ConsistencyIdentifier,
    pub backend_etags_distinct: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicObservation {
    pub operation: ClientOperation,
    pub status: u16,
    pub exposed: bool,
    pub replica_a_state: CommitState,
    pub replica_b_state: CommitState,
    pub signatures_valid: bool,
    pub signing_keys_trusted: bool,
    pub body_hex: Option<String>,
    pub content_range: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepairAttempt {
    pub source: ReplicaName,
    pub target: ReplicaName,
    pub source_tampered: bool,
    pub source_quarantined: bool,
    pub source_signature_valid: bool,
    pub source_signing_key_trusted: bool,
    pub applied: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResurrectionAttempt {
    pub attempted_logical_version: u64,
    pub high_water_logical_version: u64,
    pub applied: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GenerationState {
    pub logical_version: u64,
    pub commit_state: CommitState,
    pub committed_at_ms: u64,
    pub superseded_at_ms: Option<u64>,
    pub physical_content_a: bool,
    pub physical_content_b: bool,
    pub collected: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CollectionRunObservation {
    pub at_ms: u64,
    pub collected_versions: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaState {
    pub commit_state: CommitState,
    pub content_sha256: Option<String>,
    pub content: Option<Vec<u8>>,
    pub block_manifest_sha256: Option<String>,
    pub commit_manifest_sha256: Option<String>,
    pub logical_version: Option<u64>,
    pub logical_etag: Option<String>,
    pub backend_etag: Option<String>,
    pub ring_version: Option<u64>,
    pub write_id: Option<String>,
    pub signature_valid: bool,
    pub signing_key_trusted: bool,
    pub content_tampered: bool,
}

impl Default for ReplicaState {
    fn default() -> Self {
        Self {
            commit_state: CommitState::Absent,
            content_sha256: None,
            content: None,
            block_manifest_sha256: None,
            commit_manifest_sha256: None,
            logical_version: None,
            logical_etag: None,
            backend_etag: None,
            ring_version: None,
            write_id: None,
            signature_valid: true,
            signing_key_trusted: true,
            content_tampered: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlobState {
    pub replica_a: ReplicaState,
    pub replica_b: ReplicaState,
    pub acknowledged_success: bool,
    pub tamper_detected: bool,
    pub quarantined: bool,
    pub tombstone_seen: bool,
    pub repaired_from_tampered_source: bool,
    pub consistency_decisions: Vec<ConsistencyDecision>,
    pub public_observations: Vec<PublicObservation>,
    pub repair_attempts: Vec<RepairAttempt>,
    pub resurrection_attempts: Vec<ResurrectionAttempt>,
    pub generations: BTreeMap<u64, GenerationState>,
    pub collection_runs: Vec<CollectionRunObservation>,
    pub high_water_logical_version: u64,
    pub now_ms: u64,
    pub retention_ms: u64,
}

impl Default for BlobState {
    fn default() -> Self {
        Self {
            replica_a: ReplicaState::default(),
            replica_b: ReplicaState::default(),
            acknowledged_success: false,
            tamper_detected: false,
            quarantined: false,
            tombstone_seen: false,
            repaired_from_tampered_source: false,
            consistency_decisions: Vec::new(),
            public_observations: Vec::new(),
            repair_attempts: Vec::new(),
            resurrection_attempts: Vec::new(),
            generations: BTreeMap::new(),
            collection_runs: Vec::new(),
            high_water_logical_version: 0,
            now_ms: 0,
            retention_ms: 1_000,
        }
    }
}

impl BlobState {
    pub fn replica(&self, replica: ReplicaName) -> &ReplicaState {
        match replica {
            ReplicaName::A => &self.replica_a,
            ReplicaName::B => &self.replica_b,
        }
    }

    pub fn replica_mut(&mut self, replica: ReplicaName) -> &mut ReplicaState {
        match replica {
            ReplicaName::A => &mut self.replica_a,
            ReplicaName::B => &mut self.replica_b,
        }
    }

    pub fn is_publicly_visible(&self) -> bool {
        if self.quarantined {
            return false;
        }
        self.replica_a.commit_state == CommitState::Committed
            && self.replica_b.commit_state == CommitState::Committed
            && committed_heads_match(&self.replica_a, &self.replica_b)
    }

    pub fn prepared_is_visible(&self) -> bool {
        self.public_observations.iter().any(|observation| {
            observation.exposed
                && (matches!(
                    observation.replica_a_state,
                    CommitState::ContentOnly | CommitState::Prepared
                ) || matches!(
                    observation.replica_b_state,
                    CommitState::ContentOnly | CommitState::Prepared
                ) || !observation.signatures_valid
                    || !observation.signing_keys_trusted)
        })
    }

    pub fn current_logical_version(&self) -> Option<u64> {
        self.replica_a
            .logical_version
            .into_iter()
            .chain(self.replica_b.logical_version)
            .max()
    }

    pub fn physical_content_versions(&self) -> Vec<u64> {
        self.generations
            .values()
            .filter(|generation| generation.physical_content_a || generation.physical_content_b)
            .map(|generation| generation.logical_version)
            .collect()
    }

    pub fn collected_versions(&self) -> Vec<u64> {
        self.generations
            .values()
            .filter(|generation| generation.collected)
            .map(|generation| generation.logical_version)
            .collect()
    }

    pub fn health(&self) -> HealthState {
        if self.quarantined {
            return HealthState::Quarantined;
        }
        if self.tamper_detected
            || self.replica_a.content_tampered
            || self.replica_b.content_tampered
            || !self.replica_a.signature_valid
            || !self.replica_b.signature_valid
            || !self.replica_a.signing_key_trusted
            || !self.replica_b.signing_key_trusted
        {
            return HealthState::Tampered;
        }
        if self.replica_a.commit_state == CommitState::Absent
            && self.replica_b.commit_state == CommitState::Absent
        {
            return HealthState::Absent;
        }
        if self.replica_a.commit_state == CommitState::Tombstoned
            && self.replica_b.commit_state == CommitState::Tombstoned
            && committed_heads_match(&self.replica_a, &self.replica_b)
        {
            return HealthState::Healthy;
        }
        if self.replica_a.commit_state == CommitState::Committed
            && self.replica_b.commit_state == CommitState::Committed
        {
            return if committed_heads_match(&self.replica_a, &self.replica_b) {
                HealthState::Healthy
            } else {
                HealthState::Drifted
            };
        }
        if self.replica_a.commit_state == CommitState::Absent
            || self.replica_b.commit_state == CommitState::Absent
        {
            return HealthState::Missing;
        }
        HealthState::Drifted
    }
}

pub(crate) fn committed_heads_match(a: &ReplicaState, b: &ReplicaState) -> bool {
    a.commit_state == b.commit_state
        && a.content_sha256 == b.content_sha256
        && a.block_manifest_sha256 == b.block_manifest_sha256
        && a.commit_manifest_sha256 == b.commit_manifest_sha256
        && a.logical_version == b.logical_version
        && a.logical_etag == b.logical_etag
        && a.ring_version == b.ring_version
        && a.write_id == b.write_id
        && a.signature_valid
        && b.signature_valid
        && a.signing_key_trusted
        && b.signing_key_trusted
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelState {
    pub blobs: BTreeMap<String, BlobState>,
    pub last_outcome: Option<ActualOutcomeClass>,
}

impl ModelState {
    pub fn blob(&self, path: &str) -> Option<&BlobState> {
        self.blobs.get(path)
    }

    pub fn blob_mut(&mut self, path: &str) -> &mut BlobState {
        self.blobs.entry(path.to_owned()).or_default()
    }
}

pub fn logical_etag(blob: &str, version: u64, write_id: &str, content_hash: &str) -> String {
    let digest =
        Sha256::digest(format!("{blob}\0{version}\0{write_id}\0{content_hash}").as_bytes());
    format!("\"om-v{version}-{}\"", hex::encode(&digest[..8]))
}

pub fn manifest_hash(
    blob: &str,
    version: u64,
    write_id: &str,
    content_hash: &str,
    logical_etag: &str,
    state: CommitState,
) -> String {
    let digest = Sha256::digest(
        format!("{blob}\0{version}\0{write_id}\0{content_hash}\0{logical_etag}\0{state:?}")
            .as_bytes(),
    );
    format!("sha256:{}", hex::encode(digest))
}

pub fn backend_etag(replica: ReplicaName, version: u64, write_id: &str) -> String {
    let replica = match replica {
        ReplicaName::A => "a",
        ReplicaName::B => "b",
    };
    let digest = Sha256::digest(format!("{replica}\0{version}\0{write_id}").as_bytes());
    format!("\"azure-{replica}-{}\"", hex::encode(&digest[..8]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_etag_is_deterministic() {
        let first = logical_etag("/c/b", 1, "write-1", "sha256:abc");
        let second = logical_etag("/c/b", 1, "write-1", "sha256:abc");
        assert_eq!(first, second);
        assert!(first.starts_with("\"om-v1-"));
    }

    #[test]
    fn prepared_content_is_not_visible() {
        let mut blob = BlobState::default();
        blob.replica_a.commit_state = CommitState::Prepared;
        blob.replica_b.commit_state = CommitState::Prepared;
        assert!(!blob.is_publicly_visible());
        assert_eq!(blob.health(), HealthState::Drifted);
    }

    #[test]
    fn prepared_visibility_comes_from_observations() {
        let mut blob = BlobState::default();
        blob.public_observations.push(PublicObservation {
            operation: ClientOperation::Head,
            status: 200,
            exposed: true,
            replica_a_state: CommitState::Prepared,
            replica_b_state: CommitState::Prepared,
            signatures_valid: true,
            signing_keys_trusted: true,
            body_hex: None,
            content_range: None,
        });
        assert!(blob.prepared_is_visible());
    }

    #[test]
    fn backend_etags_are_replica_local_and_not_logical_etags() {
        let logical = logical_etag("/c/b", 1, "write-1", "sha256:abc");
        let backend_a = backend_etag(ReplicaName::A, 1, "write-1");
        let backend_b = backend_etag(ReplicaName::B, 1, "write-1");
        assert_ne!(backend_a, backend_b);
        assert_ne!(logical, backend_a);
        assert_ne!(logical, backend_b);
    }
}
