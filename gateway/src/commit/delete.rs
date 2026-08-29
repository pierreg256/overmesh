use super::*;

impl CommitCoordinator {
    pub(in crate::commit) async fn delete_blob_locked(
        &self,
        logical_blob: &LogicalBlobId,
        principal: &AuthenticatedPrincipal,
        write_id: &str,
        logical_condition: LogicalCondition,
        control_token: &ControlToken,
    ) -> Result<DeleteResult, CommitError> {
        let path_hash = logical_blob.path_hash();
        let state_key = blob_state_key(&path_hash);
        let ((primary_state, secondary_state), _) = tokio::try_join!(
            async {
                tokio::try_join!(
                    load_state(
                        self.primary.as_ref(),
                        &state_key,
                        control_token,
                        self.signer.as_ref()
                    ),
                    load_state(
                        self.secondary.as_ref(),
                        &state_key,
                        control_token,
                        self.signer.as_ref()
                    )
                )
            },
            async {
                tokio::try_join!(
                    self.primary
                        .authorize_blob_delete(logical_blob, &principal.access_token),
                    self.secondary
                        .authorize_blob_delete(logical_blob, &principal.access_token)
                )
                .map_err(CommitError::Backend)?;
                Ok::<(), CommitError>(())
            }
        )?;
        if let Some(result) = self
            .recover_partial_tombstone_publication(
                primary_state.as_ref(),
                secondary_state.as_ref(),
                &state_key,
                logical_blob,
                write_id,
                control_token,
            )
            .await?
        {
            return Ok(result);
        }
        let current_state = resolve_write_state(primary_state.as_ref(), secondary_state.as_ref())?;
        let context = Self::validate_commit_context(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            &path_hash,
            logical_blob.canonical(),
            self.ring_version,
            current_state,
            control_token,
            self.signer.as_ref(),
        )
        .await?;
        let Some(current_state) = current_state else {
            return Err(CommitError::NotFound);
        };
        let Some(current) = current_state.current() else {
            return Err(CommitError::NotFound);
        };
        if current.state == ManifestState::Tombstoned {
            if current.write_id == write_id {
                // A replay republishes the terminal form of the published
                // tombstone; the loaded document may still carry an unrelated
                // interrupted preparation.
                let terminal = context
                    .current_terminal
                    .as_ref()
                    .ok_or(CommitError::VerificationFailed)?;
                // Listing exposes this tombstone only after both replicas hold
                // these exact merged bytes.
                publish_catalog_current(
                    self.primary.as_ref(),
                    self.secondary.as_ref(),
                    logical_blob,
                    &terminal.signed,
                    &terminal.bytes,
                    control_token,
                    self.signer.as_ref(),
                )
                .await?;
                Self::publish_state_history(
                    self.primary.as_ref(),
                    self.secondary.as_ref(),
                    &path_hash,
                    current,
                    &terminal.bytes,
                    control_token,
                )
                .await?;
                return delete_result(current, true);
            }
            return Err(CommitError::NotFound);
        }
        match logical_condition {
            LogicalCondition::None => {}
            LogicalCondition::IfMatch(expected)
                if expected == "*" || expected == current.logical_etag => {}
            LogicalCondition::IfMatch(_) | LogicalCondition::IfAbsent => {
                return Err(CommitError::ConditionFailed);
            }
        }

        let logical_version = current
            .logical_version
            .checked_add(1)
            .ok_or(CommitError::VerificationFailed)?;
        let deleted_at_unix_ms = now_unix_ms();
        let tombstone_sha256 = sha256_bytes(b"overmesh:tombstone:v1");
        let logical_etag = logical_etag(
            logical_blob.canonical(),
            logical_version,
            write_id,
            &tombstone_sha256,
        );
        let version_prefix = format!(
            "objects/{path_hash}/tombstones/{}",
            stable_component(write_id)
        );
        let mut prepared_payload = CommitManifest {
            blob: logical_blob.canonical().to_owned(),
            caller: principal.identity(),
            write_id: write_id.to_owned(),
            logical_version,
            logical_etag: logical_etag.clone(),
            previous_logical_etag: Some(current.logical_etag.clone()),
            ring_version: self.ring_version,
            content_length: 0,
            content_sha256: tombstone_sha256,
            content_container: String::new(),
            content_object: String::new(),
            block_manifest_object: String::new(),
            block_manifest_sha256: String::new(),
            version_object_prefix: Some(version_prefix),
            committed_at_unix_ms: deleted_at_unix_ms,
            deleted_at_unix_ms: Some(deleted_at_unix_ms),
            state: ManifestState::Prepared,
            prepared_replicas: Vec::new(),
            signing_key_id: self.signer.key_id().to_owned(),
        };
        adopt_interrupted_preparation(
            primary_state.as_ref(),
            secondary_state.as_ref(),
            &mut prepared_payload,
        )?;
        let deleted_at_unix_ms = prepared_payload
            .deleted_at_unix_ms
            .ok_or(CommitError::VerificationFailed)?;
        let tombstone_payload = CommitManifest {
            state: ManifestState::Tombstoned,
            committed_at_unix_ms: deleted_at_unix_ms,
            prepared_replicas: vec![self.primary.id().to_owned(), self.secondary.id().to_owned()],
            ..prepared_payload.clone()
        };
        validate_tombstone_transition(&tombstone_payload, current)?;
        validate_publication_floor(
            &tombstone_payload,
            Some(current),
            context.compaction.as_ref(),
        )?;

        let prepared_etags = self
            .publish_prepared_state(
                logical_blob,
                &state_key,
                Some(current_state),
                primary_state.as_ref(),
                secondary_state.as_ref(),
                prepared_payload,
                control_token,
            )
            .await?;

        let (signed_tombstone, tombstone_bytes) = self
            .sign_commit_state(logical_blob, Some(tombstone_payload), None)
            .await?;

        publish_catalog_current(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            logical_blob,
            &signed_tombstone,
            &tombstone_bytes,
            control_token,
            self.signer.as_ref(),
        )
        .await?;

        publish_blob_state(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            &state_key,
            &tombstone_bytes,
            PutCondition::IfMatch(prepared_etags.0.ok_or(CommitError::VerificationFailed)?),
            PutCondition::IfMatch(prepared_etags.1.ok_or(CommitError::VerificationFailed)?),
            control_token,
        )
        .await?;

        let tombstone = signed_tombstone
            .payload
            .current()
            .ok_or(CommitError::VerificationFailed)?;
        Self::publish_state_history(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            &path_hash,
            tombstone,
            &tombstone_bytes,
            control_token,
        )
        .await?;
        delete_result(tombstone, false)
    }
}
