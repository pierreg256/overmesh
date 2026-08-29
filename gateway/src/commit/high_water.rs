use super::*;

impl CommitCoordinator {
    /// Validates the Reconciler-owned safety state once under the canonical
    /// commit lease (ADR-0012) and proves the merged commit-state document sits
    /// above the durable compaction floor.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn validate_commit_context(
        primary: &dyn ReplicaBackend,
        secondary: &dyn ReplicaBackend,
        path_hash: &str,
        expected_blob: &str,
        ring_version: u64,
        current: Option<&LoadedState>,
        control_token: &ControlToken,
        signer: &dyn ManifestSigner,
    ) -> Result<ValidatedCommitContext, CommitError> {
        let compaction = Self::validate_or_repair_compaction_checkpoint(
            primary,
            secondary,
            path_hash,
            expected_blob,
            ring_version,
            control_token,
            signer,
        )
        .await?;
        if let Some(state) = current
            && (state.signed.payload.blob != expected_blob
                || state.signed.payload.ring_version != ring_version)
        {
            return Err(CommitError::VerificationFailed);
        }
        if let Some(checkpoint) = &compaction {
            let Some(head) = current.and_then(LoadedState::current) else {
                return Err(CommitError::VerificationFailed);
            };
            if head.logical_version <= checkpoint.signed.payload.compacted_through_logical_version
                || head.logical_version
                    < checkpoint
                        .signed
                        .payload
                        .garbage_collection_history_head_logical_version
            {
                return Err(CommitError::VerificationFailed);
            }
        }
        let current_terminal = match current {
            Some(state) => {
                Self::validate_or_repair_current_history(
                    primary,
                    secondary,
                    path_hash,
                    state,
                    control_token,
                    signer,
                )
                .await?
            }
            None => None,
        };
        Ok(ValidatedCommitContext {
            compaction,
            current_terminal,
        })
    }

    /// ADR-0012 folds the high-water current object into the merged document,
    /// so the durable per-version history entry becomes the independent witness
    /// that the published generation was retained. A commit proves that witness
    /// exists on both replicas before it publishes the next generation.
    async fn validate_or_repair_current_history(
        primary: &dyn ReplicaBackend,
        secondary: &dyn ReplicaBackend,
        path_hash: &str,
        state: &LoadedState,
        control_token: &ControlToken,
        signer: &dyn ManifestSigner,
    ) -> Result<Option<TerminalCommitState>, CommitError> {
        let Some(current) = state.current() else {
            return Ok(None);
        };
        let history_key = Self::high_water_history_key(path_hash, current);
        // A durable history entry above the published generation proves the
        // merged document was replayed backwards. ADR-0012 removes the
        // duplicated high-water object, so this is the rollback witness.
        let successor_prefix = format!(
            "high-water/{path_hash}/history/{:020}",
            current.logical_version.saturating_add(1)
        );
        let (primary_value, secondary_value, primary_successors, secondary_successors) = tokio::try_join!(
            primary.control_get_object(&history_key, control_token),
            secondary.control_get_object(&history_key, control_token),
            primary.control_list_objects(&successor_prefix, control_token),
            secondary.control_list_objects(&successor_prefix, control_token)
        )?;
        if !primary_successors.is_empty() || !secondary_successors.is_empty() {
            return Err(CommitError::VerificationFailed);
        }
        // The validated history entry is the terminal form of the generation the
        // document publishes. Idempotent replays reuse it rather than
        // republishing a document that still carries an interrupted preparation.
        match (primary_value, secondary_value) {
            (Some(primary_value), Some(secondary_value)) => {
                if primary_value.bytes != secondary_value.bytes {
                    return Err(CommitError::VerificationFailed);
                }
                let signed = validate_state_history_entry(&primary_value.bytes, current, signer)?;
                Ok(Some(TerminalCommitState {
                    signed,
                    bytes: primary_value.bytes,
                }))
            }
            (Some(value), None) => {
                let signed = validate_state_history_entry(&value.bytes, current, signer)?;
                control_put_bytes_idempotent(
                    secondary,
                    &history_key,
                    value.bytes.clone(),
                    control_token,
                )
                .await?;
                Ok(Some(TerminalCommitState {
                    signed,
                    bytes: value.bytes,
                }))
            }
            (None, Some(value)) => {
                let signed = validate_state_history_entry(&value.bytes, current, signer)?;
                control_put_bytes_idempotent(
                    primary,
                    &history_key,
                    value.bytes.clone(),
                    control_token,
                )
                .await?;
                Ok(Some(TerminalCommitState {
                    signed,
                    bytes: value.bytes,
                }))
            }
            (None, None) => {
                // Only a terminal document is its own history entry; an
                // interrupted preparation cannot be republished as one.
                if state.prepared().is_some() {
                    return Err(CommitError::VerificationFailed);
                }
                Self::publish_state_history(
                    primary,
                    secondary,
                    path_hash,
                    current,
                    &state.bytes,
                    control_token,
                )
                .await?;
                Ok(Some(TerminalCommitState {
                    signed: state.signed.clone(),
                    bytes: state.bytes.clone(),
                }))
            }
        }
    }

    /// Retains the immutable per-version copy of a terminal commit-state
    /// document. ADR-0012 keeps this history outside the merge because it is
    /// the Reconciler's compaction and garbage-collection evidence.
    pub(crate) async fn publish_state_history(
        primary: &dyn ReplicaBackend,
        secondary: &dyn ReplicaBackend,
        path_hash: &str,
        committed: &CommitManifest,
        committed_bytes: &[u8],
        control_token: &ControlToken,
    ) -> Result<(), CommitError> {
        let history_key = Self::high_water_history_key(path_hash, committed);
        tokio::try_join!(
            control_put_bytes_idempotent(
                primary,
                &history_key,
                committed_bytes.to_vec(),
                control_token
            ),
            control_put_bytes_idempotent(
                secondary,
                &history_key,
                committed_bytes.to_vec(),
                control_token
            )
        )?;
        Ok(())
    }

    /// Validates a commit-state document discovered on a single replica during
    /// partial-publication recovery against the durable compaction floor and the
    /// generation the lagging replica still publishes.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn validate_recovery_candidate(
        primary: &dyn ReplicaBackend,
        secondary: &dyn ReplicaBackend,
        path_hash: &str,
        expected_blob: &str,
        ring_version: u64,
        candidate: &CommitManifest,
        lagging: Option<&CommitManifest>,
        control_token: &ControlToken,
        signer: &dyn ManifestSigner,
    ) -> Result<(), CommitError> {
        if candidate.blob != expected_blob || candidate.ring_version != ring_version {
            return Err(CommitError::VerificationFailed);
        }
        Self::reject_replayed_generation(primary, secondary, path_hash, candidate, control_token)
            .await?;
        let compaction = Self::strict_compaction_checkpoint(
            primary,
            secondary,
            path_hash,
            expected_blob,
            ring_version,
            control_token,
            signer,
        )
        .await?;
        validate_publication_floor(candidate, lagging, compaction.as_ref())
    }

    /// Fails closed when a durable history entry exists above the generation a
    /// document publishes. ADR-0012 folds the high-water current object into the
    /// merged document, so the retained per-version history is the witness that
    /// a published generation has not been replayed backwards.
    async fn reject_replayed_generation(
        primary: &dyn ReplicaBackend,
        secondary: &dyn ReplicaBackend,
        path_hash: &str,
        candidate: &CommitManifest,
        control_token: &ControlToken,
    ) -> Result<(), CommitError> {
        let successor_prefix = format!(
            "high-water/{path_hash}/history/{:020}",
            candidate.logical_version.saturating_add(1)
        );
        let (primary_successors, secondary_successors) = tokio::try_join!(
            primary.control_list_objects(&successor_prefix, control_token),
            secondary.control_list_objects(&successor_prefix, control_token)
        )?;
        if primary_successors.is_empty() && secondary_successors.is_empty() {
            Ok(())
        } else {
            Err(CommitError::VerificationFailed)
        }
    }

    pub(crate) fn history_compaction_checkpoint_key(path_hash: &str) -> String {
        format!("high-water/{path_hash}/compaction/current.json")
    }

    pub(in crate::commit) fn high_water_history_key(
        path_hash: &str,
        record: &CommitManifest,
    ) -> String {
        format!(
            "high-water/{path_hash}/history/{:020}-{}.json",
            record.logical_version,
            stable_component(&record.write_id)
        )
    }

    pub(crate) async fn strict_compaction_checkpoint(
        primary: &dyn ReplicaBackend,
        secondary: &dyn ReplicaBackend,
        path_hash: &str,
        expected_blob: &str,
        ring_version: u64,
        control_token: &ControlToken,
        signer: &dyn ManifestSigner,
    ) -> Result<Option<LoadedCompactionCheckpoint>, CommitError> {
        let (primary_value, secondary_value) = tokio::try_join!(
            Self::load_compaction_checkpoint(
                primary,
                path_hash,
                expected_blob,
                ring_version,
                control_token,
                signer
            ),
            Self::load_compaction_checkpoint(
                secondary,
                path_hash,
                expected_blob,
                ring_version,
                control_token,
                signer
            )
        )?;
        match (primary_value, secondary_value) {
            (None, None) => Ok(None),
            (Some(primary), Some(secondary)) if primary.bytes == secondary.bytes => {
                Ok(Some(primary))
            }
            _ => Err(CommitError::VerificationFailed),
        }
    }

    async fn validate_or_repair_compaction_checkpoint(
        primary: &dyn ReplicaBackend,
        secondary: &dyn ReplicaBackend,
        path_hash: &str,
        expected_blob: &str,
        ring_version: u64,
        control_token: &ControlToken,
        signer: &dyn ManifestSigner,
    ) -> Result<Option<LoadedCompactionCheckpoint>, CommitError> {
        let (primary_value, secondary_value) = tokio::try_join!(
            Self::load_compaction_checkpoint(
                primary,
                path_hash,
                expected_blob,
                ring_version,
                control_token,
                signer
            ),
            Self::load_compaction_checkpoint(
                secondary,
                path_hash,
                expected_blob,
                ring_version,
                control_token,
                signer
            )
        )?;
        let authoritative = match (primary_value, secondary_value) {
            (None, None) => return Ok(None),
            (Some(value), None) => {
                Self::copy_compaction_checkpoint(
                    secondary,
                    path_hash,
                    &value,
                    PutCondition::IfAbsent,
                    control_token,
                )
                .await?;
                value
            }
            (None, Some(value)) => {
                Self::copy_compaction_checkpoint(
                    primary,
                    path_hash,
                    &value,
                    PutCondition::IfAbsent,
                    control_token,
                )
                .await?;
                value
            }
            (Some(primary_value), Some(secondary_value))
                if primary_value.bytes == secondary_value.bytes =>
            {
                primary_value
            }
            (Some(primary_value), Some(secondary_value))
                if compaction_checkpoint_descends(&primary_value, &secondary_value) =>
            {
                Self::copy_compaction_checkpoint(
                    secondary,
                    path_hash,
                    &primary_value,
                    head_condition_from_etag(secondary_value.backend_etag.as_deref()),
                    control_token,
                )
                .await?;
                primary_value
            }
            (Some(primary_value), Some(secondary_value))
                if compaction_checkpoint_descends(&secondary_value, &primary_value) =>
            {
                Self::copy_compaction_checkpoint(
                    primary,
                    path_hash,
                    &secondary_value,
                    head_condition_from_etag(primary_value.backend_etag.as_deref()),
                    control_token,
                )
                .await?;
                secondary_value
            }
            (Some(_), Some(_)) => return Err(CommitError::VerificationFailed),
        };
        verify_identical_objects(
            primary,
            secondary,
            &Self::history_compaction_checkpoint_key(path_hash),
            &authoritative.bytes,
            control_token,
        )
        .await?;
        Ok(Some(authoritative))
    }

    async fn load_compaction_checkpoint(
        backend: &dyn ReplicaBackend,
        path_hash: &str,
        expected_blob: &str,
        ring_version: u64,
        control_token: &ControlToken,
        signer: &dyn ManifestSigner,
    ) -> Result<Option<LoadedCompactionCheckpoint>, CommitError> {
        let Some(value) = backend
            .control_get_object(
                &Self::history_compaction_checkpoint_key(path_hash),
                control_token,
            )
            .await?
        else {
            return Ok(None);
        };
        let signed = SignedDocument::<HistoryCompactionCheckpoint>::from_bytes(&value.bytes)?;
        if signed.canonical_bytes()? != value.bytes {
            return Err(CommitError::VerificationFailed);
        }
        signed.verify(
            SignatureDomain::HistoryCompactionCheckpoint,
            &signed.payload.signing_key_id,
            signer,
        )?;
        if signed.signed_at_unix_ms < signed.payload.compacted_at_unix_ms {
            return Err(CommitError::VerificationFailed);
        }
        validate_compaction_checkpoint(&signed.payload, path_hash, expected_blob, ring_version)?;
        Ok(Some(LoadedCompactionCheckpoint {
            signed,
            bytes: value.bytes,
            backend_etag: value.etag,
        }))
    }

    async fn copy_compaction_checkpoint(
        backend: &dyn ReplicaBackend,
        path_hash: &str,
        value: &LoadedCompactionCheckpoint,
        condition: PutCondition,
        control_token: &ControlToken,
    ) -> Result<(), CommitError> {
        backend
            .control_put_bytes(
                &Self::history_compaction_checkpoint_key(path_hash),
                value.bytes.clone(),
                "application/json",
                condition,
                control_token,
            )
            .await?;
        Ok(())
    }
}

/// The rollback floor for a generation that is about to become, or claims to
/// be, the published commit state. ADR-0012 folds the high-water assertion into
/// the merged document, so the independent floor is the Reconciler-owned
/// compaction checkpoint plus the generation the replicas already publish.
pub(crate) fn validate_publication_floor(
    candidate: &CommitManifest,
    previous: Option<&CommitManifest>,
    compaction: Option<&LoadedCompactionCheckpoint>,
) -> Result<(), CommitError> {
    if let Some(checkpoint) = compaction {
        let floor = &checkpoint.signed.payload;
        if candidate.logical_version <= floor.compacted_through_logical_version
            || candidate.logical_version < floor.garbage_collection_history_head_logical_version
        {
            return Err(CommitError::VerificationFailed);
        }
        if candidate.logical_version == floor.compacted_through_logical_version.saturating_add(1)
            && (candidate.previous_logical_etag.as_deref()
                != Some(floor.compacted_through_logical_etag.as_str())
                || !valid_state_transition(floor.compacted_through_state, candidate.state))
        {
            return Err(CommitError::VerificationFailed);
        }
    }
    match previous {
        Some(current) if current == candidate => Ok(()),
        Some(current) => validate_manifest_successor(candidate, current),
        None if compaction.is_some() => {
            let floor = &compaction.expect("checked compaction").signed.payload;
            if candidate.logical_version
                == floor.compacted_through_logical_version.saturating_add(1)
                && candidate.previous_logical_etag.as_deref()
                    == Some(floor.compacted_through_logical_etag.as_str())
                && valid_state_transition(floor.compacted_through_state, candidate.state)
            {
                Ok(())
            } else {
                Err(CommitError::VerificationFailed)
            }
        }
        None if candidate.logical_version == 1 && candidate.previous_logical_etag.is_none() => {
            Ok(())
        }
        None => Err(CommitError::VerificationFailed),
    }
}

pub(crate) fn validate_manifest_successor(
    candidate: &CommitManifest,
    previous: &CommitManifest,
) -> Result<(), CommitError> {
    if candidate.blob == previous.blob
        && candidate.ring_version == previous.ring_version
        && candidate.logical_version == previous.logical_version.saturating_add(1)
        && candidate.previous_logical_etag.as_deref() == Some(previous.logical_etag.as_str())
        && candidate.committed_at_unix_ms >= previous.committed_at_unix_ms
        && valid_state_transition(previous.state, candidate.state)
    {
        Ok(())
    } else {
        Err(CommitError::VerificationFailed)
    }
}

fn valid_state_transition(previous: ManifestState, candidate: ManifestState) -> bool {
    matches!(
        (previous, candidate),
        (ManifestState::Committed, ManifestState::Committed)
            | (ManifestState::Committed, ManifestState::Tombstoned)
            | (ManifestState::Tombstoned, ManifestState::Committed)
    )
}

fn compaction_checkpoint_descends(
    newer: &LoadedCompactionCheckpoint,
    older: &LoadedCompactionCheckpoint,
) -> bool {
    newer.signed.payload.checkpoint_version
        == older.signed.payload.checkpoint_version.saturating_add(1)
        && newer.signed.payload.compacted_through_logical_version
            > older.signed.payload.compacted_through_logical_version
        && newer.signed.payload.previous_checkpoint_version
            == Some(older.signed.payload.checkpoint_version)
        && newer.signed.payload.previous_checkpoint_sha256 == Some(sha256_bytes(&older.bytes))
}

fn validate_compaction_checkpoint(
    checkpoint: &HistoryCompactionCheckpoint,
    path_hash: &str,
    expected_blob: &str,
    ring_version: u64,
) -> Result<(), CommitError> {
    let valid_sha256 = |value: &str| {
        value.strip_prefix("sha256:").is_some_and(|digest| {
            digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    };
    if checkpoint.api_version != "overmesh.io/history-compaction-checkpoint/v1"
        || checkpoint.blob != expected_blob
        || checkpoint.path_hash != path_hash
        || checkpoint.head_object != format!("heads/{path_hash}.json")
        || checkpoint.ring_version != ring_version
        || checkpoint.checkpoint_version == 0
        || checkpoint.compacted_through_logical_version == 0
        || checkpoint.compacted_through_logical_etag.is_empty()
        || checkpoint.compacted_through_committed_at_unix_ms == 0
        || checkpoint.compacted_through_state == ManifestState::Prepared
        || !valid_sha256(&checkpoint.covered_terminal_manifest_sha256)
        || !valid_sha256(&checkpoint.garbage_collection_marker_sha256)
        || checkpoint.garbage_collection_through_logical_version
            < checkpoint.compacted_through_logical_version
        || checkpoint.garbage_collection_history_head_logical_version
            <= checkpoint.garbage_collection_through_logical_version
        || checkpoint.garbage_collection_marker_object
            != format!(
                "garbage-collection/{path_hash}/{:020}.json",
                checkpoint.garbage_collection_through_logical_version
            )
        || checkpoint.compacted_at_unix_ms < checkpoint.garbage_collected_at_unix_ms
    {
        return Err(CommitError::VerificationFailed);
    }
    let previous_is_valid = match (
        checkpoint.previous_checkpoint_sha256.as_deref(),
        checkpoint.previous_checkpoint_version,
    ) {
        (None, None) => checkpoint.checkpoint_version == 1,
        (Some(hash), Some(version)) => {
            checkpoint.checkpoint_version > 1
                && version.saturating_add(1) == checkpoint.checkpoint_version
                && valid_sha256(hash)
        }
        _ => false,
    };
    if !previous_is_valid {
        return Err(CommitError::VerificationFailed);
    }
    Ok(())
}

/// A high-water history entry is the terminal form of the merged commit-state
/// document for exactly one generation.
pub(crate) fn validate_state_history_entry(
    bytes: &[u8],
    expected: &CommitManifest,
    signer: &dyn ManifestSigner,
) -> Result<SignedDocument<BlobCommitState>, CommitError> {
    let signed = SignedDocument::<BlobCommitState>::from_bytes(bytes)?;
    if signed.canonical_bytes()? != bytes {
        return Err(CommitError::VerificationFailed);
    }
    signed.verify(
        SignatureDomain::BlobCommitState,
        &signed.payload.signing_key_id,
        signer,
    )?;
    validate_blob_commit_state(&signed.payload)?;
    if signed.payload.prepared().is_some() || signed.payload.current() != Some(expected) {
        return Err(CommitError::VerificationFailed);
    }
    Ok(signed)
}
