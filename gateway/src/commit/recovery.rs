use super::*;

impl CommitCoordinator {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::commit) async fn recover_partial_publication(
        &self,
        primary_head: Option<&LoadedHead>,
        secondary_head: Option<&LoadedHead>,
        head_key: &str,
        logical_blob: &LogicalBlobId,
        principal: &AuthenticatedPrincipal,
        write_id: &str,
        content: &SpoolContent,
        control_token: &ControlToken,
    ) -> Result<Option<CommitResult>, CommitError> {
        let (committed, lagging, missing_backend) = match (primary_head, secondary_head) {
            (Some(committed), None) => (committed, None, self.secondary.as_ref()),
            (None, Some(committed)) => (committed, None, self.primary.as_ref()),
            (Some(committed), Some(lagging))
                if committed.signed.payload.write_id == write_id
                    && committed.signed.payload.previous_logical_etag.as_deref()
                        == Some(&lagging.signed.payload.logical_etag)
                    && committed.signed.payload.logical_version
                        == lagging.signed.payload.logical_version.saturating_add(1) =>
            {
                (committed, Some(lagging), self.secondary.as_ref())
            }
            (Some(lagging), Some(committed))
                if committed.signed.payload.write_id == write_id
                    && committed.signed.payload.previous_logical_etag.as_deref()
                        == Some(&lagging.signed.payload.logical_etag)
                    && committed.signed.payload.logical_version
                        == lagging.signed.payload.logical_version.saturating_add(1) =>
            {
                (committed, Some(lagging), self.primary.as_ref())
            }
            _ => return Ok(None),
        };
        if committed.signed.payload.write_id != write_id {
            return Err(CommitError::ReplicaDrift);
        }
        if committed.signed.payload.content_sha256 != content.content_sha256 {
            return Err(CommitError::IdempotencyConflict);
        }
        Self::validate_recovery_candidate(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            &logical_blob.path_hash(),
            logical_blob.canonical(),
            self.ring_version,
            committed,
            control_token,
            self.signer.as_ref(),
        )
        .await?;
        self.authorize_replay(principal, &committed.signed.payload)
            .await?;
        let committed_key = format!(
            "{}/committed.json",
            commit_manifest_object_prefix(&committed.signed.payload)?
        );
        verify_identical_objects(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            &committed_key,
            &committed.bytes,
            control_token,
        )
        .await?;
        tokio::try_join!(
            put_file_idempotent(
                self.primary.as_ref(),
                &committed.signed.payload.content_container,
                &committed.signed.payload.content_object,
                content,
                &principal.access_token
            ),
            put_file_idempotent(
                self.secondary.as_ref(),
                &committed.signed.payload.content_container,
                &committed.signed.payload.content_object,
                content,
                &principal.access_token
            )
        )?;
        match missing_backend
            .control_put_bytes(
                head_key,
                committed.bytes.clone(),
                "application/json",
                head_condition(lagging),
                control_token,
            )
            .await
        {
            Ok(_) => {}
            Err(BackendError::PreconditionFailed | BackendError::AlreadyExists) => {
                return Err(CommitError::ReplicaDrift);
            }
            Err(error) => return Err(CommitError::Backend(error)),
        }
        verify_identical_objects(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            head_key,
            &committed.bytes,
            control_token,
        )
        .await?;
        let path_hash = logical_blob.path_hash();
        Self::publish_high_water(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            &path_hash,
            &committed.signed,
            &committed.bytes,
            control_token,
            self.signer.as_ref(),
        )
        .await?;
        publish_catalog_current(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            logical_blob,
            &committed.signed,
            &committed.bytes,
            control_token,
            self.signer.as_ref(),
        )
        .await?;
        Ok(Some(CommitResult {
            logical_version: committed.signed.payload.logical_version,
            logical_etag: committed.signed.payload.logical_etag.clone(),
            write_id: write_id.to_owned(),
            idempotent_replay: true,
        }))
    }

    pub(in crate::commit) async fn recover_partial_tombstone_publication(
        &self,
        primary_head: Option<&LoadedHead>,
        secondary_head: Option<&LoadedHead>,
        head_key: &str,
        logical_blob: &LogicalBlobId,
        write_id: &str,
        control_token: &ControlToken,
    ) -> Result<Option<DeleteResult>, CommitError> {
        let (tombstone, lagging, lagging_backend) = match (primary_head, secondary_head) {
            (Some(primary), Some(secondary))
                if primary.bytes != secondary.bytes
                    && primary.signed.payload.state == ManifestState::Tombstoned
                    && primary.signed.payload.write_id == write_id =>
            {
                (primary, secondary, self.secondary.as_ref())
            }
            (Some(primary), Some(secondary))
                if primary.bytes != secondary.bytes
                    && secondary.signed.payload.state == ManifestState::Tombstoned
                    && secondary.signed.payload.write_id == write_id =>
            {
                (secondary, primary, self.primary.as_ref())
            }
            _ => return Ok(None),
        };
        validate_tombstone_transition(&tombstone.signed.payload, &lagging.signed.payload)?;
        let committed_key = format!(
            "{}/committed.json",
            commit_manifest_object_prefix(&tombstone.signed.payload)?
        );
        let (primary_sidecar, secondary_sidecar) = tokio::try_join!(
            self.primary
                .control_get_object(&committed_key, control_token),
            self.secondary
                .control_get_object(&committed_key, control_token)
        )?;
        if primary_sidecar.as_ref().map(|value| value.bytes.as_slice())
            != Some(tombstone.bytes.as_slice())
            || secondary_sidecar
                .as_ref()
                .map(|value| value.bytes.as_slice())
                != Some(tombstone.bytes.as_slice())
        {
            return Err(CommitError::VerificationFailed);
        }
        lagging_backend
            .control_put_bytes(
                head_key,
                tombstone.bytes.clone(),
                "application/json",
                head_condition(Some(lagging)),
                control_token,
            )
            .await?;
        verify_identical_objects(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            head_key,
            &tombstone.bytes,
            control_token,
        )
        .await?;
        Self::publish_high_water(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            &logical_path_hash(&tombstone.signed.payload.blob),
            &tombstone.signed,
            &tombstone.bytes,
            control_token,
            self.signer.as_ref(),
        )
        .await?;
        publish_catalog_current(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            logical_blob,
            &tombstone.signed,
            &tombstone.bytes,
            control_token,
            self.signer.as_ref(),
        )
        .await?;
        Ok(Some(delete_result(&tombstone.signed.payload, true)?))
    }
}
