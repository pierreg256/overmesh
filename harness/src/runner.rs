use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

use crate::{
    fault::{FaultId, FaultSchedule},
    model::{
        ActualOutcomeClass, BlobState, ClientOperation, CollectionRunObservation, CommitState,
        ConsistencyDecision, ConsistencyIdentifier, DecisionOperation, GenerationState, ModelState,
        PublicObservation, RepairAttempt, ReplicaState, ResurrectionAttempt, backend_etag,
        committed_heads_match, logical_etag, manifest_hash,
    },
    report::{ScenarioReport, now_unix_ms},
    scenario::{ByteRange, Condition, Operation, ReplicaName, Scenario},
    validator::{CheckResult, InvariantId, validate_expected, validate_invariant},
};

#[derive(Debug, Clone)]
pub struct RunOptions {
    pub repository_root: PathBuf,
    pub report_directory: PathBuf,
    pub write_report: bool,
}

impl RunOptions {
    pub fn for_repository(repository_root: PathBuf) -> Self {
        let report_directory = repository_root.join("harness/reports");
        Self {
            repository_root,
            report_directory,
            write_report: true,
        }
    }
}

#[derive(Debug)]
pub struct ScenarioRun {
    pub report: ScenarioReport,
    pub report_path: Option<PathBuf>,
}

pub fn run_scenario(path: &Path, options: &RunOptions) -> Result<ScenarioRun> {
    let scenario = Scenario::load(path)?;
    let faults = scenario
        .faults
        .iter()
        .map(|fault| FaultId::from_str(&fault.id))
        .collect::<Result<Vec<_>, _>>()?;
    let fault_schedule = FaultSchedule::new(faults);
    let mut state = ModelState::default();

    for operation in &scenario.operations {
        state.blob_mut(operation.blob()).retention_ms = scenario.initial_state.retention_ms;
        execute_operation(
            operation,
            &fault_schedule,
            &options.repository_root,
            &mut state,
        )?;
    }

    let primary_blob_path = scenario
        .operations
        .first()
        .map(Operation::blob)
        .context("scenario has no operations")?;
    let blob_after_operations = state.blob(primary_blob_path).cloned().unwrap_or_default();
    let outcome = state.last_outcome.unwrap_or(ActualOutcomeClass::Failure);

    let mut reconciled_blob = blob_after_operations.clone();
    reconcile_blob(&mut reconciled_blob);
    let health_after_reconciliation = reconciled_blob.health();

    let mut checks = validate_expected(
        &scenario.expected,
        outcome,
        &blob_after_operations,
        health_after_reconciliation,
    );
    for invariant in &scenario.invariants {
        let id = InvariantId::from_str(invariant)?;
        checks.push(validate_invariant(id, &reconciled_blob));
    }

    let passed = checks.iter().all(|check| check.passed);
    let report = ScenarioReport {
        api_version: "harness.overmesh.io/report/v1",
        project_version: env!("CARGO_PKG_VERSION"),
        harness_version: env!("CARGO_PKG_VERSION"),
        scenario_id: scenario.id,
        suite: scenario.suite,
        seed: scenario.seed,
        generated_at_unix_ms: now_unix_ms(),
        outcome,
        health_after_operations: blob_after_operations.health(),
        health_after_reconciliation,
        responses: blob_after_operations.public_observations.clone(),
        consistency_decisions: blob_after_operations.consistency_decisions.clone(),
        repair_attempts: blob_after_operations.repair_attempts.clone(),
        resurrection_attempts: blob_after_operations.resurrection_attempts.clone(),
        current_logical_version: blob_after_operations.current_logical_version(),
        physical_content_versions: blob_after_operations.physical_content_versions(),
        collected_versions: blob_after_operations.collected_versions(),
        collection_runs: blob_after_operations.collection_runs.clone(),
        passed,
        checks,
    };
    let report_path = if options.write_report {
        Some(report.write(&options.report_directory)?)
    } else {
        None
    };

    Ok(ScenarioRun {
        report,
        report_path,
    })
}

fn execute_operation(
    operation: &Operation,
    faults: &FaultSchedule,
    repository_root: &Path,
    state: &mut ModelState,
) -> Result<()> {
    match operation {
        Operation::PutBlob {
            blob,
            dataset,
            write_id,
        } => {
            let content = fs::read(repository_root.join("harness/datasets").join(dataset))
                .with_context(|| format!("failed to read dataset {dataset}"))?;
            put_blob(state, blob, write_id, &content, faults);
        }
        Operation::DeleteBlob { blob, write_id } => delete_blob(state, blob, write_id),
        Operation::Head {
            blob,
            if_match,
            if_none_match,
        } => head_blob(
            state,
            blob,
            *if_match,
            *if_none_match,
            ClientOperation::Head,
        ),
        Operation::GetBlob {
            blob,
            if_match,
            if_none_match,
            range,
        } => get_blob(state, blob, *if_match, *if_none_match, *range),
        Operation::TamperContent { blob, replica } => {
            let blob_state = state.blob_mut(blob);
            blob_state.replica_mut(*replica).content_tampered = true;
            state.last_outcome = Some(ActualOutcomeClass::Success);
        }
        Operation::InvalidateSignature { blob, replica } => {
            state.blob_mut(blob).replica_mut(*replica).signature_valid = false;
            state.last_outcome = Some(ActualOutcomeClass::Success);
        }
        Operation::UseUntrustedSigningKey { blob, replica } => {
            state
                .blob_mut(blob)
                .replica_mut(*replica)
                .signing_key_trusted = false;
            state.last_outcome = Some(ActualOutcomeClass::Success);
        }
        Operation::RemoveReplica { blob, replica } => {
            *state.blob_mut(blob).replica_mut(*replica) = ReplicaState::default();
            state.last_outcome = Some(ActualOutcomeClass::Success);
        }
        Operation::ObservePreparedPublication { blob } => head_blob(
            state,
            blob,
            None,
            None,
            ClientOperation::ObservePreparedPublication,
        ),
        Operation::ObserveReplicaConsistency { blob } => {
            observe_replica_consistency(state, blob);
        }
        Operation::AttemptRepair {
            blob,
            source,
            target,
        } => {
            let applied = attempt_repair(state.blob_mut(blob), *source, *target);
            state.last_outcome = Some(if applied {
                ActualOutcomeClass::Success
            } else {
                ActualOutcomeClass::Failure
            });
        }
        Operation::AdvanceTime { blob, milliseconds } => {
            let blob = state.blob_mut(blob);
            blob.now_ms = blob.now_ms.saturating_add(*milliseconds);
            state.last_outcome = Some(ActualOutcomeClass::Success);
        }
        Operation::Collect { blob } => {
            collect_superseded_generations(state.blob_mut(blob));
            state.last_outcome = Some(ActualOutcomeClass::Success);
        }
        Operation::AttemptResurrection {
            blob,
            logical_version,
        } => {
            let applied = attempt_resurrection(state.blob_mut(blob), *logical_version);
            state.last_outcome = Some(if applied {
                ActualOutcomeClass::Success
            } else {
                ActualOutcomeClass::Failure
            });
        }
        Operation::Reconcile { blob } => {
            reconcile_blob(state.blob_mut(blob));
            state.last_outcome = Some(ActualOutcomeClass::Success);
        }
    }
    Ok(())
}

fn put_blob(
    state: &mut ModelState,
    blob_path: &str,
    write_id: &str,
    content: &[u8],
    faults: &FaultSchedule,
) {
    let blob = state.blob_mut(blob_path);
    let current_version = blob
        .current_logical_version()
        .unwrap_or(blob.high_water_logical_version);
    let version = current_version + 1;
    let content_hash = format!("sha256:{}", hex::encode(Sha256::digest(content)));
    let block_manifest_hash = format!(
        "sha256:{}",
        hex::encode(Sha256::digest(format!("block\0{content_hash}").as_bytes()))
    );
    let etag = logical_etag(blob_path, version, write_id, &content_hash);
    let committed_manifest_hash = manifest_hash(
        blob_path,
        version,
        write_id,
        &content_hash,
        &etag,
        CommitState::Committed,
    );
    let prepared_manifest_hash = manifest_hash(
        blob_path,
        version,
        write_id,
        &content_hash,
        &etag,
        CommitState::Prepared,
    );

    let replica_state = |replica| ReplicaState {
        commit_state: CommitState::ContentOnly,
        content_sha256: Some(content_hash.clone()),
        content: Some(content.to_vec()),
        block_manifest_sha256: Some(block_manifest_hash.clone()),
        commit_manifest_sha256: None,
        logical_version: Some(version),
        logical_etag: Some(etag.clone()),
        backend_etag: Some(backend_etag(replica, version, write_id)),
        ring_version: Some(1),
        write_id: Some(write_id.to_owned()),
        signature_valid: true,
        signing_key_trusted: true,
        content_tampered: false,
    };

    let rank = faults.first_commit_rank();
    blob.acknowledged_success = false;
    blob.replica_a = ReplicaState::default();
    blob.replica_b = ReplicaState::default();

    if rank.is_some_and(|value| value <= 2) {
        record_response(blob, ClientOperation::PutBlob, 503, false, None, None);
        state.last_outcome = Some(ActualOutcomeClass::Failure);
        return;
    }

    blob.replica_a = replica_state(ReplicaName::A);
    if rank == Some(3) {
        record_response(blob, ClientOperation::PutBlob, 503, false, None, None);
        state.last_outcome = Some(ActualOutcomeClass::Failure);
        return;
    }

    blob.replica_b = replica_state(ReplicaName::B);
    if rank == Some(4) {
        record_response(blob, ClientOperation::PutBlob, 503, false, None, None);
        state.last_outcome = Some(ActualOutcomeClass::Failure);
        return;
    }

    prepare_replica(&mut blob.replica_a, &prepared_manifest_hash);
    if rank == Some(5) {
        record_response(blob, ClientOperation::PutBlob, 503, false, None, None);
        state.last_outcome = Some(ActualOutcomeClass::Failure);
        return;
    }

    prepare_replica(&mut blob.replica_b, &prepared_manifest_hash);
    if matches!(rank, Some(6 | 7)) {
        record_response(blob, ClientOperation::PutBlob, 503, false, None, None);
        state.last_outcome = Some(ActualOutcomeClass::Failure);
        return;
    }

    commit_replica(&mut blob.replica_a, &committed_manifest_hash);
    if rank == Some(8) {
        record_response(blob, ClientOperation::PutBlob, 503, false, None, None);
        state.last_outcome = Some(ActualOutcomeClass::Ambiguous);
        return;
    }

    commit_replica(&mut blob.replica_b, &committed_manifest_hash);
    record_consistency_decision(blob, DecisionOperation::Put, true);
    record_generation(blob, version, CommitState::Committed, current_version);
    if matches!(rank, Some(9..=12)) {
        record_response(blob, ClientOperation::PutBlob, 503, true, None, None);
        state.last_outcome = Some(ActualOutcomeClass::Ambiguous);
        return;
    }

    blob.acknowledged_success = true;
    record_response(blob, ClientOperation::PutBlob, 201, true, None, None);
    state.last_outcome = Some(ActualOutcomeClass::Success);
}

fn prepare_replica(replica: &mut ReplicaState, manifest_hash: &str) {
    replica.commit_state = CommitState::Prepared;
    replica.commit_manifest_sha256 = Some(manifest_hash.to_owned());
}

fn commit_replica(replica: &mut ReplicaState, manifest_hash: &str) {
    replica.commit_state = CommitState::Committed;
    replica.commit_manifest_sha256 = Some(manifest_hash.to_owned());
}

fn delete_blob(state: &mut ModelState, blob_path: &str, write_id: &str) {
    let blob = state.blob_mut(blob_path);
    if !blob.is_publicly_visible() {
        record_response(blob, ClientOperation::DeleteBlob, 404, false, None, None);
        state.last_outcome = Some(ActualOutcomeClass::Failure);
        return;
    }

    let previous_version = blob.current_logical_version().unwrap_or(0);
    let version = previous_version + 1;
    let content_hash = "sha256:tombstone";
    let etag = logical_etag(blob_path, version, write_id, content_hash);
    let manifest = manifest_hash(
        blob_path,
        version,
        write_id,
        content_hash,
        &etag,
        CommitState::Tombstoned,
    );
    for (name, replica) in [
        (ReplicaName::A, &mut blob.replica_a),
        (ReplicaName::B, &mut blob.replica_b),
    ] {
        replica.commit_state = CommitState::Tombstoned;
        replica.content_sha256 = None;
        replica.content = None;
        replica.block_manifest_sha256 = None;
        replica.commit_manifest_sha256 = Some(manifest.clone());
        replica.logical_version = Some(version);
        replica.logical_etag = Some(etag.clone());
        replica.backend_etag = Some(backend_etag(name, version, write_id));
        replica.write_id = Some(write_id.to_owned());
    }
    blob.tombstone_seen = true;
    blob.acknowledged_success = true;
    record_consistency_decision(blob, DecisionOperation::Delete, true);
    record_generation(blob, version, CommitState::Tombstoned, previous_version);
    record_response(blob, ClientOperation::DeleteBlob, 202, false, None, None);
    state.last_outcome = Some(ActualOutcomeClass::Success);
}

fn head_blob(
    state: &mut ModelState,
    blob_path: &str,
    if_match: Option<Condition>,
    if_none_match: Option<Condition>,
    operation: ClientOperation,
) {
    let blob = state.blob_mut(blob_path);
    let metadata_accepted = metadata_is_accepted(blob);
    blob.consistency_decisions.push(ConsistencyDecision {
        operation: DecisionOperation::Head,
        accepted: metadata_accepted,
        signatures_valid: signatures_valid(blob),
        signing_keys_trusted: signing_keys_trusted(blob),
        identifier: ConsistencyIdentifier::LogicalEtag,
        backend_etags_distinct: backend_etags_distinct(blob),
    });
    let visible = blob.is_publicly_visible();
    let status = if !signatures_valid(blob) || !signing_keys_trusted(blob) {
        blob.tamper_detected = true;
        blob.quarantined = true;
        409
    } else if !visible {
        404
    } else {
        conditional_status(if_match, if_none_match).unwrap_or(200)
    };
    record_response(
        blob,
        operation,
        status,
        visible && matches!(status, 200 | 304),
        None,
        None,
    );
    state.last_outcome = Some(outcome_for_status(status));
}

fn get_blob(
    state: &mut ModelState,
    blob_path: &str,
    if_match: Option<Condition>,
    if_none_match: Option<Condition>,
    range: Option<ByteRange>,
) {
    let blob = state.blob_mut(blob_path);
    let metadata_accepted = metadata_is_accepted(blob);
    blob.consistency_decisions.push(ConsistencyDecision {
        operation: DecisionOperation::Get,
        accepted: metadata_accepted,
        signatures_valid: signatures_valid(blob),
        signing_keys_trusted: signing_keys_trusted(blob),
        identifier: ConsistencyIdentifier::LogicalEtag,
        backend_etags_distinct: backend_etags_distinct(blob),
    });

    let visible = blob.is_publicly_visible();
    let (status, body_hex, content_range) = if !signatures_valid(blob)
        || !signing_keys_trusted(blob)
        || blob.replica_a.content_tampered
        || blob.replica_b.content_tampered
    {
        blob.tamper_detected = true;
        blob.quarantined = true;
        (409, None, None)
    } else if !visible {
        (404, None, None)
    } else if let Some(status) = conditional_status(if_match, if_none_match) {
        (status, None, None)
    } else {
        read_content(blob, range)
    };

    record_response(
        blob,
        ClientOperation::GetBlob,
        status,
        visible && matches!(status, 200 | 206 | 304),
        body_hex,
        content_range,
    );
    state.last_outcome = Some(outcome_for_status(status));
}

fn reconcile_blob(blob: &mut BlobState) {
    if blob.quarantined
        || blob.replica_a.content_tampered
        || blob.replica_b.content_tampered
        || !blob.replica_a.signature_valid
        || !blob.replica_b.signature_valid
        || !blob.replica_a.signing_key_trusted
        || !blob.replica_b.signing_key_trusted
    {
        blob.tamper_detected = true;
        blob.quarantined = true;
        return;
    }

    match (blob.replica_a.commit_state, blob.replica_b.commit_state) {
        (CommitState::Committed | CommitState::Tombstoned, CommitState::Absent) => {
            attempt_repair(blob, ReplicaName::A, ReplicaName::B);
        }
        (CommitState::Absent, CommitState::Committed | CommitState::Tombstoned) => {
            attempt_repair(blob, ReplicaName::B, ReplicaName::A);
        }
        (CommitState::Committed, CommitState::Prepared | CommitState::ContentOnly)
            if same_payload(&blob.replica_a, &blob.replica_b) =>
        {
            attempt_repair(blob, ReplicaName::A, ReplicaName::B);
        }
        (CommitState::Prepared | CommitState::ContentOnly, CommitState::Committed)
            if same_payload(&blob.replica_a, &blob.replica_b) =>
        {
            attempt_repair(blob, ReplicaName::B, ReplicaName::A);
        }
        _ => {}
    }
}

fn same_payload(a: &ReplicaState, b: &ReplicaState) -> bool {
    a.content_sha256 == b.content_sha256
        && a.block_manifest_sha256 == b.block_manifest_sha256
        && a.logical_version == b.logical_version
        && a.logical_etag == b.logical_etag
        && a.ring_version == b.ring_version
        && a.write_id == b.write_id
}

fn signatures_valid(blob: &BlobState) -> bool {
    [&blob.replica_a, &blob.replica_b]
        .into_iter()
        .filter(|replica| replica.commit_state != CommitState::Absent)
        .all(|replica| replica.signature_valid)
}

fn signing_keys_trusted(blob: &BlobState) -> bool {
    [&blob.replica_a, &blob.replica_b]
        .into_iter()
        .filter(|replica| replica.commit_state != CommitState::Absent)
        .all(|replica| replica.signing_key_trusted)
}

fn metadata_is_accepted(blob: &BlobState) -> bool {
    signatures_valid(blob)
        && signing_keys_trusted(blob)
        && committed_heads_match(&blob.replica_a, &blob.replica_b)
}

fn backend_etags_distinct(blob: &BlobState) -> bool {
    blob.replica_a.backend_etag.is_some()
        && blob.replica_b.backend_etag.is_some()
        && blob.replica_a.backend_etag != blob.replica_b.backend_etag
}

fn record_consistency_decision(blob: &mut BlobState, operation: DecisionOperation, accepted: bool) {
    blob.consistency_decisions.push(ConsistencyDecision {
        operation,
        accepted,
        signatures_valid: signatures_valid(blob),
        signing_keys_trusted: signing_keys_trusted(blob),
        identifier: ConsistencyIdentifier::LogicalEtag,
        backend_etags_distinct: backend_etags_distinct(blob),
    });
}

fn record_response(
    blob: &mut BlobState,
    operation: ClientOperation,
    status: u16,
    exposed: bool,
    body_hex: Option<String>,
    content_range: Option<String>,
) {
    blob.public_observations.push(PublicObservation {
        operation,
        status,
        exposed,
        replica_a_state: blob.replica_a.commit_state,
        replica_b_state: blob.replica_b.commit_state,
        signatures_valid: signatures_valid(blob),
        signing_keys_trusted: signing_keys_trusted(blob),
        body_hex,
        content_range,
    });
}

fn conditional_status(
    if_match: Option<Condition>,
    if_none_match: Option<Condition>,
) -> Option<u16> {
    if matches!(if_match, Some(Condition::Stale)) {
        return Some(412);
    }
    if matches!(if_none_match, Some(Condition::Current | Condition::Any)) {
        return Some(304);
    }
    None
}

fn read_content(
    blob: &BlobState,
    range: Option<ByteRange>,
) -> (u16, Option<String>, Option<String>) {
    let Some(content) = blob.replica_a.content.as_deref() else {
        return (404, None, None);
    };
    let Some(range) = range else {
        return (200, Some(hex::encode(content)), None);
    };
    let Ok(start) = usize::try_from(range.start) else {
        return (416, None, None);
    };
    if start >= content.len() {
        return (416, None, None);
    }
    let requested_end = usize::try_from(range.end_inclusive).unwrap_or(usize::MAX);
    let end = requested_end.min(content.len() - 1);
    (
        206,
        Some(hex::encode(&content[start..=end])),
        Some(format!("bytes {start}-{end}/{}", content.len())),
    )
}

fn outcome_for_status(status: u16) -> ActualOutcomeClass {
    if (200..300).contains(&status) || status == 304 {
        ActualOutcomeClass::Success
    } else {
        ActualOutcomeClass::Failure
    }
}

fn record_generation(
    blob: &mut BlobState,
    version: u64,
    commit_state: CommitState,
    previous_version: u64,
) {
    if previous_version > 0
        && let Some(previous) = blob.generations.get_mut(&previous_version)
    {
        previous.superseded_at_ms = Some(blob.now_ms);
    }
    blob.generations.insert(
        version,
        GenerationState {
            logical_version: version,
            commit_state,
            committed_at_ms: blob.now_ms,
            superseded_at_ms: None,
            physical_content_a: commit_state == CommitState::Committed,
            physical_content_b: commit_state == CommitState::Committed,
            collected: false,
        },
    );
    blob.high_water_logical_version = version;
}

fn observe_replica_consistency(state: &mut ModelState, blob_path: &str) {
    let blob = state.blob_mut(blob_path);
    let accepted = metadata_is_accepted(blob);
    let backend_etags_are_distinct = backend_etags_distinct(blob);
    record_consistency_decision(blob, DecisionOperation::ObserveReplicaConsistency, accepted);
    state.last_outcome = Some(if accepted && backend_etags_are_distinct {
        ActualOutcomeClass::Success
    } else {
        ActualOutcomeClass::Failure
    });
}

fn attempt_repair(blob: &mut BlobState, source: ReplicaName, target: ReplicaName) -> bool {
    let source_state = blob.replica(source).clone();
    let source_quarantined = blob.quarantined;
    let source_eligible = !source_quarantined
        && !source_state.content_tampered
        && source_state.signature_valid
        && source_state.signing_key_trusted
        && matches!(
            source_state.commit_state,
            CommitState::Committed | CommitState::Tombstoned
        );
    let applied = if source_eligible {
        let mut repaired = source_state.clone();
        let version = repaired.logical_version.unwrap_or(0);
        let write_id = repaired.write_id.as_deref().unwrap_or("repair");
        repaired.backend_etag = Some(backend_etag(target, version, write_id));
        *blob.replica_mut(target) = repaired;
        true
    } else {
        blob.tamper_detected |= source_state.content_tampered
            || !source_state.signature_valid
            || !source_state.signing_key_trusted;
        blob.quarantined |= blob.tamper_detected;
        false
    };
    blob.repaired_from_tampered_source |= applied
        && (source_state.content_tampered
            || source_quarantined
            || !source_state.signature_valid
            || !source_state.signing_key_trusted);
    blob.repair_attempts.push(RepairAttempt {
        source,
        target,
        source_tampered: source_state.content_tampered,
        source_quarantined,
        source_signature_valid: source_state.signature_valid,
        source_signing_key_trusted: source_state.signing_key_trusted,
        applied,
    });
    record_consistency_decision(blob, DecisionOperation::Reconcile, applied);
    applied
}

fn collect_superseded_generations(blob: &mut BlobState) {
    let current_version = blob.high_water_logical_version;
    let mut collected_versions = Vec::new();
    for generation in blob.generations.values_mut() {
        let eligible = generation.logical_version < current_version
            && generation.commit_state != CommitState::Tombstoned
            && generation.superseded_at_ms.is_some_and(|superseded_at| {
                blob.now_ms >= superseded_at.saturating_add(blob.retention_ms)
            });
        if eligible {
            generation.physical_content_a = false;
            generation.physical_content_b = false;
            if !generation.collected {
                generation.collected = true;
                collected_versions.push(generation.logical_version);
            }
        }
    }
    blob.collection_runs.push(CollectionRunObservation {
        at_ms: blob.now_ms,
        collected_versions,
    });
}

fn attempt_resurrection(blob: &mut BlobState, logical_version: u64) -> bool {
    let applied = logical_version >= blob.high_water_logical_version;
    blob.resurrection_attempts.push(ResurrectionAttempt {
        attempted_logical_version: logical_version,
        high_water_logical_version: blob.high_water_logical_version,
        applied,
    });
    applied
}

pub fn failed_checks(checks: &[CheckResult]) -> Vec<&CheckResult> {
    checks.iter().filter(|check| !check.passed).collect()
}

pub fn ensure_passed(run: &ScenarioRun) -> Result<()> {
    if run.report.passed {
        return Ok(());
    }
    let failures = failed_checks(&run.report.checks)
        .into_iter()
        .map(|check| format!("{}: {}", check.id, check.detail))
        .collect::<Vec<_>>()
        .join("\n");
    bail!("scenario {} failed:\n{}", run.report.scenario_id, failures)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::HealthState;

    #[test]
    fn commit_fault_eight_is_repairable() {
        let mut state = ModelState::default();
        let faults =
            FaultSchedule::new(vec![FaultId::from_str("FAULT-COMMIT-008").expect("fault")]);
        put_blob(
            &mut state,
            "/container/blob",
            "write-1",
            b"content",
            &faults,
        );
        let blob = state.blob("/container/blob").expect("blob");
        assert_eq!(blob.health(), HealthState::Drifted);
        assert_eq!(state.last_outcome, Some(ActualOutcomeClass::Ambiguous));

        let mut repaired = blob.clone();
        reconcile_blob(&mut repaired);
        assert_eq!(repaired.health(), HealthState::Healthy);
    }
}
