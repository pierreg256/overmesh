use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs,
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use overmesh_gateway::{
    backend::{
        BackendError, BackendLease, DataObjectValidation, ObjectValue, PutCondition,
        ReplicaBackend, SharedBackend,
    },
    commit::logical_path_hash,
    identity::ControlToken,
    manifest::{
        BlockDescriptor, BlockManifest, BlockManifestPage, CommitManifest, GarbageCollectionMarker,
        HistoryCompactionCheckpoint, ManifestSigner, ManifestState, ReconciliationClassification,
        ReconciliationRecord, ReconciliationRecordAction, SignatureDomain, SignedDocument,
        commit_manifest_object_prefix, logical_etag, sha256_bytes, validate_block_manifest_link,
        validate_block_manifest_page,
    },
    resource::stable_component,
    ring::RingDocument,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    identity::SharedTokenProvider,
    posture::{RbacPostureReport, SharedRbacPostureAuditor},
    report::{BlobReport, CycleReport, HealthState, ReconciliationAction},
};

const HEAD_PREFIX: &str = "heads/";
const QUARANTINE_PREFIX: &str = "quarantine/";
const AUDIT_PREFIX: &str = "audit/";
const GARBAGE_COLLECTION_PREFIX: &str = "garbage-collection/";
const HISTORY_COMPACTION_API_VERSION: &str = "overmesh.io/history-compaction-checkpoint/v1";

#[derive(Clone)]
pub struct ReconcilerEngine {
    ring: Arc<RingDocument>,
    backends: HashMap<String, SharedBackend>,
    signer: Arc<dyn ManifestSigner>,
    token_provider: SharedTokenProvider,
    posture_auditor: SharedRbacPostureAuditor,
    physical_collection_delay: Duration,
    history_compaction_max_versions_per_cycle: usize,
    head_discovery_batch_size: usize,
    head_discovery_cursor_path: PathBuf,
    staged_block_gc_max_records_per_cycle: usize,
    staged_block_metadata_cursor_path: PathBuf,
    staged_block_marker_cursor_path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeadDiscoveryMode {
    Incremental,
    FullAudit,
}

#[derive(Debug, Clone)]
pub struct ReconcilerOptions {
    pub physical_collection_delay: Duration,
    pub history_compaction_max_versions_per_cycle: usize,
    pub head_discovery_batch_size: usize,
    pub head_discovery_cursor_path: PathBuf,
    pub staged_block_gc_max_records_per_cycle: usize,
    pub staged_block_metadata_cursor_path: PathBuf,
    pub staged_block_marker_cursor_path: PathBuf,
}

mod audit;
mod catalog;
mod discovery;
mod gc;
mod history;
mod orchestration;
mod repair;
mod staging;
mod storage;
mod validation;

pub use audit::verify_reconciliation_record;

use catalog::CatalogReconciliation;
use history::{
    expected_version_prefix, garbage_collection_evidence, garbage_collection_marker_key,
    history_compaction_checkpoint_key, validate_compaction_checkpoint,
};
use storage::*;

#[cfg(test)]
use history::high_water_history_key;
#[cfg(test)]
use orchestration::authoritative_over;

struct ValidatedHead {
    signed: SignedDocument<CommitManifest>,
    bytes: Vec<u8>,
    backend_etag: Option<String>,
}

struct ValidatedReplica {
    head: ValidatedHead,
    block_manifest: Option<Vec<u8>>,
    block_pages: Vec<(String, Vec<u8>)>,
    committed_manifest: Vec<u8>,
    high_water_checkpoint: Vec<u8>,
}

#[derive(Debug, Clone)]
struct HeadCandidate {
    object_key: String,
    discovered_on: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HeadDiscoveryCursor {
    api_version: String,
    ring_version: u64,
    node_index: usize,
    backend_cursor: Option<String>,
}

struct HeadDiscoveryBatch {
    candidates: Vec<HeadCandidate>,
    next_cursor: Option<HeadDiscoveryCursor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StagedDiscoveryCursor {
    api_version: String,
    ring_version: u64,
    node_index: usize,
    backend_cursor: Option<String>,
}

#[derive(Debug)]
struct GarbageCollectionPlan {
    blob: String,
    health: HealthState,
    marker_repairs: Vec<MarkerRepair>,
    data_deletes: Vec<DataDelete>,
    metadata_deletes: Vec<String>,
    new_marker: Option<(String, Vec<u8>)>,
    compaction_marker_verification: Option<(String, Vec<u8>)>,
    new_compaction_checkpoint: Option<CheckpointPublication>,
    history_deletes: Vec<ControlDelete>,
    obsolete_marker_deletes: Vec<ControlDelete>,
    compaction_checkpoint_bytes: Option<Vec<u8>>,
    collected_versions: Vec<u64>,
    eligible_through: Option<u64>,
}

#[derive(Debug)]
struct MarkerRepair {
    backend_id: String,
    object_key: String,
    bytes: Vec<u8>,
}

#[derive(Debug)]
struct CheckpointPublication {
    expected_previous_bytes: Option<Vec<u8>>,
    bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
struct ControlDelete {
    object_key: String,
    first_etag: Option<String>,
    second_etag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DataDelete {
    container: String,
    object_key: String,
}

struct ValidatedHistory {
    entries: Vec<ValidatedHistoryEntry>,
    covered_deletes: Vec<ControlDelete>,
}

struct ValidatedHistoryEntry {
    signed: SignedDocument<CommitManifest>,
    bytes: Vec<u8>,
    object_key: String,
    first_etag: Option<String>,
    second_etag: Option<String>,
}

struct ValidatedMarkers {
    latest_through: Option<u64>,
    latest_evidence: Option<GarbageCollectionEvidence>,
    repairs: Vec<MarkerRepair>,
    objects: Vec<(u64, String, ControlDelete)>,
}

#[derive(Clone)]
struct GarbageCollectionEvidence {
    object_key: String,
    sha256: String,
    bytes: Option<Vec<u8>>,
    history_head_logical_version: u64,
    collected_through_logical_version: u64,
    collected_committed_versions: Vec<u64>,
    physical_collection_delay_ms: u64,
    collected_at_unix_ms: u64,
}

struct LoadedCompactionCheckpoint {
    signed: SignedDocument<HistoryCompactionCheckpoint>,
    bytes: Vec<u8>,
}

struct ReplicaCompactionCheckpoint {
    signed: SignedDocument<HistoryCompactionCheckpoint>,
    bytes: Vec<u8>,
    etag: Option<String>,
}

enum ReplicaValidation {
    MissingHead,
    Incomplete {
        head: ValidatedHead,
        reason: String,
    },
    RecoverableTombstone {
        replica: ValidatedReplica,
        reason: String,
    },
    Valid(ValidatedReplica),
    Tampered {
        blob: Option<String>,
        reason: String,
    },
    Unavailable {
        reason: String,
    },
}

impl ReplicaValidation {
    fn blob(&self) -> Option<&str> {
        match self {
            Self::Incomplete { head, .. } => Some(&head.signed.payload.blob),
            Self::RecoverableTombstone { replica, .. } => Some(&replica.head.signed.payload.blob),
            Self::Valid(replica) => Some(&replica.head.signed.payload.blob),
            Self::Tampered { blob, .. } => blob.as_deref(),
            Self::MissingHead | Self::Unavailable { .. } => None,
        }
    }

    fn fully_validated_head(&self) -> Option<&ValidatedHead> {
        match self {
            Self::RecoverableTombstone { replica, .. } | Self::Valid(replica) => {
                Some(&replica.head)
            }
            Self::MissingHead
            | Self::Incomplete { .. }
            | Self::Tampered { .. }
            | Self::Unavailable { .. } => None,
        }
    }
}

fn committed_manifest_object(manifest: &CommitManifest) -> Result<String> {
    let prefix = commit_manifest_object_prefix(manifest)
        .context("commit manifest does not use the expected version layout")?;
    Ok(format!("{prefix}/committed.json"))
}

fn head_object_key(blob: &str) -> String {
    format!("{HEAD_PREFIX}{}.json", logical_path_hash(blob))
}

fn head_hash(head_object: &str) -> Result<&str> {
    head_object
        .strip_prefix(HEAD_PREFIX)
        .and_then(|value| value.strip_suffix(".json"))
        .context("invalid committed head object path")
}

async fn maintain_lease(
    backend: &dyn ReplicaBackend,
    lease: &BackendLease,
    token: &ControlToken,
    renewal_interval: Duration,
) -> BackendError {
    loop {
        tokio::time::sleep(renewal_interval).await;
        if let Err(error) = backend.control_renew_lock(lease, token).await {
            return error;
        }
    }
}

fn now_unix_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after Unix epoch")
            .as_millis(),
    )
    .expect("Unix timestamp milliseconds fit in u64")
}

#[cfg(test)]
mod tests;
