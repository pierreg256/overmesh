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
        let head_key = format!("heads/{path_hash}.json");
        let ((primary_head, secondary_head), _) = tokio::try_join!(
            async {
                tokio::try_join!(
                    load_head(
                        self.primary.as_ref(),
                        &head_key,
                        control_token,
                        self.signer.as_ref()
                    ),
                    load_head(
                        self.secondary.as_ref(),
                        &head_key,
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
                primary_head.as_ref(),
                secondary_head.as_ref(),
                &head_key,
                write_id,
                control_token,
            )
            .await?
        {
            return Ok(result);
        }
        let current = strict_current_head(primary_head.as_ref(), secondary_head.as_ref())?;
        Self::validate_or_repair_high_water(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            &path_hash,
            logical_blob.canonical(),
            self.ring_version,
            current,
            control_token,
            self.signer.as_ref(),
        )
        .await?;
        let Some(current) = current else {
            return Err(CommitError::NotFound);
        };
        if current.signed.payload.state == ManifestState::Tombstoned {
            if current.signed.payload.write_id == write_id {
                return delete_result(&current.signed.payload, true);
            }
            return Err(CommitError::NotFound);
        }
        match logical_condition {
            LogicalCondition::None => {}
            LogicalCondition::IfMatch(expected)
                if expected == "*" || expected == current.signed.payload.logical_etag => {}
            LogicalCondition::IfMatch(_) | LogicalCondition::IfAbsent => {
                return Err(CommitError::ConditionFailed);
            }
        }

        let logical_version = current
            .signed
            .payload
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
        let prepared_manifest_key = format!("{version_prefix}/prepared.json");
        let committed_manifest_key = format!("{version_prefix}/committed.json");
        let prepared_payload = CommitManifest {
            blob: logical_blob.canonical().to_owned(),
            caller: principal.identity(),
            write_id: write_id.to_owned(),
            logical_version,
            logical_etag: logical_etag.clone(),
            previous_logical_etag: Some(current.signed.payload.logical_etag.clone()),
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
        let signed_prepared = SignedDocument::create(
            prepared_payload,
            SignatureDomain::CommitManifest,
            self.signer.as_ref(),
        )
        .await?;
        let prepared_bytes = signed_prepared.canonical_bytes()?;
        tokio::try_join!(
            put_bytes_idempotent(
                self.primary.as_ref(),
                &prepared_manifest_key,
                prepared_bytes.clone(),
                control_token
            ),
            put_bytes_idempotent(
                self.secondary.as_ref(),
                &prepared_manifest_key,
                prepared_bytes.clone(),
                control_token
            )
        )?;
        verify_identical_objects(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            &prepared_manifest_key,
            &prepared_bytes,
            control_token,
        )
        .await?;

        let tombstone_payload = CommitManifest {
            state: ManifestState::Tombstoned,
            prepared_replicas: vec![self.primary.id().to_owned(), self.secondary.id().to_owned()],
            committed_at_unix_ms: deleted_at_unix_ms,
            ..signed_prepared.payload
        };
        let signed_tombstone = SignedDocument::create(
            tombstone_payload,
            SignatureDomain::CommitManifest,
            self.signer.as_ref(),
        )
        .await?;
        let tombstone_bytes = signed_tombstone.canonical_bytes()?;
        tokio::try_join!(
            put_bytes_idempotent(
                self.primary.as_ref(),
                &committed_manifest_key,
                tombstone_bytes.clone(),
                control_token
            ),
            put_bytes_idempotent(
                self.secondary.as_ref(),
                &committed_manifest_key,
                tombstone_bytes.clone(),
                control_token
            )
        )?;

        let (primary_publish, secondary_publish) = tokio::join!(
            self.primary.control_put_bytes(
                &head_key,
                tombstone_bytes.clone(),
                "application/json",
                head_condition(primary_head.as_ref()),
                control_token
            ),
            self.secondary.control_put_bytes(
                &head_key,
                tombstone_bytes.clone(),
                "application/json",
                head_condition(secondary_head.as_ref()),
                control_token
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
                warn!(error = %error, "only one replica published the tombstoned head");
                return Err(CommitError::Ambiguous);
            }
            (Err(first), Err(second)) => {
                warn!(primary_error = %first, secondary_error = %second, "both tombstone head publications failed");
                return Err(CommitError::Backend(first));
            }
        }
        verify_identical_objects(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            &head_key,
            &tombstone_bytes,
            control_token,
        )
        .await?;
        Self::publish_high_water(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            &path_hash,
            &signed_tombstone,
            &tombstone_bytes,
            control_token,
            self.signer.as_ref(),
        )
        .await?;
        delete_result(&signed_tombstone.payload, false)
    }
}
