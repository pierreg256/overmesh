use super::*;

impl CommitCoordinator {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::commit) async fn validate_or_repair_high_water(
        primary: &dyn ReplicaBackend,
        secondary: &dyn ReplicaBackend,
        path_hash: &str,
        expected_blob: &str,
        ring_version: u64,
        current: Option<&LoadedHead>,
        control_token: &ControlToken,
        signer: &dyn ManifestSigner,
    ) -> Result<(), CommitError> {
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
        if let Some(checkpoint) = &compaction {
            let Some(head) = current else {
                return Err(CommitError::VerificationFailed);
            };
            if head.signed.payload.logical_version
                <= checkpoint.signed.payload.compacted_through_logical_version
                || head.signed.payload.logical_version
                    < checkpoint
                        .signed
                        .payload
                        .garbage_collection_history_head_logical_version
            {
                return Err(CommitError::VerificationFailed);
            }
        }
        let (primary_high, secondary_high) = tokio::try_join!(
            Self::load_current_high_water(primary, path_hash, control_token, signer),
            Self::load_current_high_water(secondary, path_hash, control_token, signer)
        )?;
        let highest = match (primary_high, secondary_high) {
            (None, None) => None,
            (Some(value), None) => {
                Self::copy_high_water(
                    secondary,
                    path_hash,
                    &value,
                    PutCondition::IfAbsent,
                    control_token,
                )
                .await?;
                Some(value)
            }
            (None, Some(value)) => {
                Self::copy_high_water(
                    primary,
                    path_hash,
                    &value,
                    PutCondition::IfAbsent,
                    control_token,
                )
                .await?;
                Some(value)
            }
            (Some(primary_value), Some(secondary_value)) => {
                if primary_value.signed.payload.logical_version
                    == secondary_value.signed.payload.logical_version
                {
                    if primary_value.bytes != secondary_value.bytes {
                        return Err(CommitError::VerificationFailed);
                    }
                    Some(primary_value)
                } else if primary_value.signed.payload.logical_version
                    > secondary_value.signed.payload.logical_version
                {
                    Self::copy_high_water(
                        secondary,
                        path_hash,
                        &primary_value,
                        head_condition_from_etag(secondary_value.backend_etag.as_deref()),
                        control_token,
                    )
                    .await?;
                    Some(primary_value)
                } else {
                    Self::copy_high_water(
                        primary,
                        path_hash,
                        &secondary_value,
                        head_condition_from_etag(primary_value.backend_etag.as_deref()),
                        control_token,
                    )
                    .await?;
                    Some(secondary_value)
                }
            }
        };
        match (current, highest) {
            (None, None) => Ok(()),
            (None, Some(_)) => Err(CommitError::VerificationFailed),
            (Some(head), Some(high))
                if high.signed.payload.logical_version > head.signed.payload.logical_version =>
            {
                Err(CommitError::VerificationFailed)
            }
            (Some(head), Some(high))
                if high.signed.payload.logical_version == head.signed.payload.logical_version =>
            {
                if high.signed.payload.logical_etag == head.signed.payload.logical_etag
                    && high.signed.payload.write_id == head.signed.payload.write_id
                    && high.signed.payload.blob == head.signed.payload.blob
                    && high.bytes == head.bytes
                {
                    Ok(())
                } else {
                    Err(CommitError::VerificationFailed)
                }
            }
            (Some(head), _) => {
                Self::publish_high_water(
                    primary,
                    secondary,
                    path_hash,
                    &head.signed,
                    &head.bytes,
                    control_token,
                    signer,
                )
                .await
            }
        }
    }

    pub(in crate::commit) async fn publish_high_water(
        primary: &dyn ReplicaBackend,
        secondary: &dyn ReplicaBackend,
        path_hash: &str,
        committed: &SignedDocument<CommitManifest>,
        committed_bytes: &[u8],
        control_token: &ControlToken,
        signer: &dyn ManifestSigner,
    ) -> Result<(), CommitError> {
        committed.verify(
            SignatureDomain::CommitManifest,
            &committed.payload.signing_key_id,
            signer,
        )?;
        let bytes = committed_bytes.to_vec();
        let history_key = Self::high_water_history_key(path_hash, &committed.payload);
        tokio::try_join!(
            put_bytes_idempotent(primary, &history_key, bytes.clone(), control_token),
            put_bytes_idempotent(secondary, &history_key, bytes.clone(), control_token)
        )?;
        let current_key = Self::high_water_current_key(path_hash);
        let (primary_current, secondary_current) = tokio::try_join!(
            primary.control_get_object(&current_key, control_token),
            secondary.control_get_object(&current_key, control_token)
        )?;
        let (primary_publish, secondary_publish) = tokio::join!(
            primary.control_put_bytes(
                &current_key,
                bytes.clone(),
                "application/json",
                head_condition_from_object(primary_current.as_ref()),
                control_token,
            ),
            secondary.control_put_bytes(
                &current_key,
                bytes.clone(),
                "application/json",
                head_condition_from_object(secondary_current.as_ref()),
                control_token,
            )
        );
        match (primary_publish, secondary_publish) {
            (Ok(_), Ok(_)) => {}
            (Err(first), Err(second))
                if is_condition_error(&first) && is_condition_error(&second) =>
            {
                return Err(CommitError::ConditionFailed);
            }
            (Err(error), Ok(_)) | (Ok(_), Err(error)) => {
                warn!(error = %error, "only one replica published the high-water checkpoint");
                return Err(CommitError::Ambiguous);
            }
            (Err(first), Err(_second)) => return Err(CommitError::Backend(first)),
        }
        verify_identical_objects(primary, secondary, &current_key, &bytes, control_token).await
    }

    pub(in crate::commit) async fn load_current_high_water(
        backend: &dyn ReplicaBackend,
        path_hash: &str,
        control_token: &ControlToken,
        signer: &dyn ManifestSigner,
    ) -> Result<Option<LoadedHighWater>, CommitError> {
        let object_key = Self::high_water_current_key(path_hash);
        let Some(value) = backend
            .control_get_object(&object_key, control_token)
            .await?
        else {
            return Ok(None);
        };
        let signed = SignedDocument::<CommitManifest>::from_bytes(&value.bytes)?;
        if !matches!(
            signed.payload.state,
            ManifestState::Committed | ManifestState::Tombstoned
        ) {
            return Err(CommitError::VerificationFailed);
        }
        if signed.payload.state == ManifestState::Tombstoned {
            validate_tombstone_manifest(&signed.payload)?;
        }
        signed.verify(
            SignatureDomain::CommitManifest,
            &signed.payload.signing_key_id,
            signer,
        )?;
        Ok(Some(LoadedHighWater {
            signed,
            bytes: value.bytes,
            backend_etag: value.etag,
        }))
    }

    pub(in crate::commit) async fn copy_high_water(
        backend: &dyn ReplicaBackend,
        path_hash: &str,
        value: &LoadedHighWater,
        condition: PutCondition,
        control_token: &ControlToken,
    ) -> Result<(), CommitError> {
        let history_key = Self::high_water_history_key(path_hash, &value.signed.payload);
        put_bytes_idempotent(backend, &history_key, value.bytes.clone(), control_token).await?;
        backend
            .control_put_bytes(
                &Self::high_water_current_key(path_hash),
                value.bytes.clone(),
                "application/json",
                condition,
                control_token,
            )
            .await?;
        Ok(())
    }

    pub(in crate::commit) fn high_water_current_key(path_hash: &str) -> String {
        format!("high-water/{path_hash}/current.json")
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
