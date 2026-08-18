use super::*;

impl ReconcilerEngine {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn repair(
        &self,
        head_object: &str,
        health_before: HealthState,
        source: &ValidatedReplica,
        source_backend: &dyn ReplicaBackend,
        target_backend: &dyn ReplicaBackend,
        target_head: Option<&ValidatedHead>,
        token: &ControlToken,
    ) -> Result<BlobReport> {
        self.copy_immutable_objects(source, source_backend, target_backend, token)
            .await?;
        target_backend
            .control_put_bytes(
                head_object,
                source.head.bytes.clone(),
                "application/json",
                head_condition(target_head),
                token,
            )
            .await
            .map_err(|error| match error {
                BackendError::PreconditionFailed | BackendError::AlreadyExists => {
                    anyhow::anyhow!("conditional repair refused a newer target head")
                }
                other => anyhow::Error::new(other),
            })?;
        let target_validation = self
            .validate_replica(target_backend, head_object, token)
            .await;
        ensure!(
            matches!(
                target_validation,
                ReplicaValidation::Valid(ref target)
                    if target.head.bytes == source.head.bytes
            ),
            "repaired target did not validate as the authoritative committed version"
        );
        let logical_blob = &source.head.logical_blob;
        self.write_audit(
            Some(logical_blob),
            head_object,
            match health_before {
                HealthState::Drifted => ReconciliationClassification::Drifted,
                _ => ReconciliationClassification::Missing,
            },
            ReconciliationRecordAction::Repaired,
            "conditional repair from the unique fully validated source",
            Some(source_backend.id()),
            Some(target_backend.id()),
            token,
        )
        .await?;
        info!(
            blob = logical_blob.canonical(),
            source = source_backend.id(),
            target = target_backend.id(),
            "replica repaired"
        );
        Ok(BlobReport {
            blob: Some(logical_blob.canonical().to_owned()),
            head_object: head_object.to_owned(),
            health_before,
            health_after: HealthState::Healthy,
            action: ReconciliationAction::Repaired,
            source_replica: Some(source_backend.id().to_owned()),
            target_replica: Some(target_backend.id().to_owned()),
            detail: "conditional repair completed from a fully validated source".to_owned(),
        })
    }

    pub(super) async fn quarantine(
        &self,
        blob: Option<&LogicalBlobId>,
        head_object: &str,
        reason: String,
        token: &ControlToken,
    ) -> Result<BlobReport> {
        let path_hash = head_hash(head_object)?;
        let blob_string = blob.map(|logical_blob| logical_blob.canonical().to_owned());
        let record = self
            .signed_record(
                blob,
                head_object,
                ReconciliationClassification::Tampered,
                ReconciliationRecordAction::Quarantined,
                &reason,
                None,
                None,
            )
            .await?;
        let bytes = record.canonical_bytes()?;
        let quarantine_object = format!("{QUARANTINE_PREFIX}{path_hash}.json");
        for backend in self.target_backends(blob)? {
            put_current_quarantine(
                backend.as_ref(),
                &quarantine_object,
                bytes.clone(),
                token,
                self.signer.as_ref(),
            )
            .await?;
        }
        self.write_audit(
            blob,
            head_object,
            ReconciliationClassification::Tampered,
            ReconciliationRecordAction::Quarantined,
            &reason,
            None,
            None,
            token,
        )
        .await?;
        error!(blob = ?blob_string, reason, "blob quarantined");
        Ok(BlobReport {
            blob: blob_string,
            head_object: head_object.to_owned(),
            health_before: HealthState::Tampered,
            health_after: HealthState::Quarantined,
            action: ReconciliationAction::Quarantined,
            source_replica: None,
            target_replica: None,
            detail: reason,
        })
    }

    async fn copy_immutable_objects(
        &self,
        source: &ValidatedReplica,
        source_backend: &dyn ReplicaBackend,
        target: &dyn ReplicaBackend,
        token: &ControlToken,
    ) -> Result<()> {
        if source.head.signed.payload.state == ManifestState::Committed {
            let content = source_backend
                .service_get_data_object(
                    &source.head.signed.payload.content_container,
                    &source.head.signed.payload.content_object,
                    token,
                )
                .await?
                .context("validated repair source content disappeared")?;
            put_immutable_data(
                target,
                &source.head.signed.payload.content_container,
                &source.head.signed.payload.content_object,
                content.bytes,
                token,
            )
            .await?;
        }
        if let Some(block_manifest) = &source.block_manifest {
            put_immutable(
                target,
                &source.head.signed.payload.block_manifest_object,
                block_manifest.clone(),
                "application/json",
                token,
            )
            .await?;
        }
        for (object, bytes) in &source.block_pages {
            put_immutable(target, object, bytes.clone(), "application/json", token).await?;
        }
        put_immutable(
            target,
            &committed_manifest_object(&source.head.signed.payload)?,
            source.committed_manifest.clone(),
            "application/json",
            token,
        )
        .await?;
        self.repair_high_water_checkpoint(source, target, token)
            .await
    }

    pub(super) async fn repair_high_water_checkpoint(
        &self,
        source: &ValidatedReplica,
        target: &dyn ReplicaBackend,
        token: &ControlToken,
    ) -> Result<()> {
        let path_hash = source.head.logical_blob.path_hash();
        let history_key = format!(
            "high-water/{path_hash}/history/{:020}-{}.json",
            source.head.signed.payload.logical_version,
            stable_component(&source.head.signed.payload.write_id)
        );
        put_immutable(
            target,
            &history_key,
            source.high_water_checkpoint.clone(),
            "application/json",
            token,
        )
        .await?;
        let current_key = format!("high-water/{path_hash}/current.json");
        let current = target.control_get_object(&current_key, token).await?;
        if current
            .as_ref()
            .is_some_and(|value| value.bytes == source.high_water_checkpoint)
        {
            return Ok(());
        }
        if let Some(value) = current.as_ref() {
            let signed = SignedDocument::<CommitManifest>::from_bytes(&value.bytes)
                .context("target high-water checkpoint is invalid")?;
            signed
                .verify(
                    SignatureDomain::CommitManifest,
                    &signed.payload.signing_key_id,
                    self.signer.as_ref(),
                )
                .context("target high-water checkpoint signature validation failed")?;
            ensure!(
                signed.payload.logical_version <= source.head.signed.payload.logical_version,
                "repair refused to lower the target high-water checkpoint"
            );
        }
        target
            .control_put_bytes(
                &current_key,
                source.high_water_checkpoint.clone(),
                "application/json",
                match current.and_then(|value| value.etag) {
                    Some(etag) => PutCondition::IfMatch(etag),
                    None => PutCondition::IfAbsent,
                },
                token,
            )
            .await?;
        Ok(())
    }

    pub(super) async fn copy_for_recovery(
        &self,
        source: &ValidatedReplica,
        source_backend: &dyn ReplicaBackend,
        target: &dyn ReplicaBackend,
        target_head: Option<&ObjectValue>,
        head_object: &str,
        token: &ControlToken,
    ) -> Result<()> {
        if source.head.signed.payload.state == ManifestState::Committed {
            let content = source_backend
                .service_get_data_object(
                    &source.head.signed.payload.content_container,
                    &source.head.signed.payload.content_object,
                    token,
                )
                .await?
                .context("validated recovery source content disappeared")?;
            overwrite_data_for_recovery(
                target,
                &source.head.signed.payload.content_container,
                &source.head.signed.payload.content_object,
                content.bytes,
                token,
            )
            .await?;
        }
        if let Some(block_manifest) = &source.block_manifest {
            overwrite_for_recovery(
                target,
                &source.head.signed.payload.block_manifest_object,
                block_manifest.clone(),
                "application/json",
                token,
            )
            .await?;
        }
        for (object, bytes) in &source.block_pages {
            overwrite_for_recovery(target, object, bytes.clone(), "application/json", token)
                .await?;
        }
        overwrite_for_recovery(
            target,
            &committed_manifest_object(&source.head.signed.payload)?,
            source.committed_manifest.clone(),
            "application/json",
            token,
        )
        .await?;
        self.repair_high_water_checkpoint(source, target, token)
            .await?;
        target
            .control_put_bytes(
                head_object,
                source.head.bytes.clone(),
                "application/json",
                match target_head.and_then(|value| value.etag.clone()) {
                    Some(etag) => PutCondition::IfMatch(etag),
                    None => PutCondition::IfAbsent,
                },
                token,
            )
            .await?;
        Ok(())
    }

    pub(super) async fn load_quarantine(
        &self,
        path_hash: &str,
        blob: Option<&LogicalBlobId>,
        discovered_on: &str,
        token: &ControlToken,
    ) -> Result<Option<SignedDocument<ReconciliationRecord>>> {
        let object_key = format!("{QUARANTINE_PREFIX}{path_hash}.json");
        let mut found: Option<(SignedDocument<ReconciliationRecord>, Vec<u8>)> = None;
        let mut missing = Vec::new();
        let backends = match blob {
            Some(blob) => self.target_backends(Some(blob))?,
            None => vec![self.backend(discovered_on)?],
        };
        for backend in backends {
            if let Some(value) = backend.control_get_object(&object_key, token).await? {
                let record = SignedDocument::<ReconciliationRecord>::from_bytes(&value.bytes)
                    .context("quarantine record is not valid JSON")?;
                record
                    .verify(
                        SignatureDomain::ReconciliationRecord,
                        &record.payload.signing_key_id,
                        self.signer.as_ref(),
                    )
                    .context("quarantine record signature validation failed")?;
                ensure!(
                    record.payload.action == ReconciliationRecordAction::Quarantined,
                    "quarantine object does not contain a quarantine action"
                );
                ensure!(
                    record.payload.ring_version == self.ring.ring_version,
                    "quarantine record Ring version mismatch"
                );
                if let Some((_, expected)) = &found {
                    ensure!(
                        expected == &value.bytes,
                        "replica quarantine records are different"
                    );
                } else {
                    found = Some((record, value.bytes));
                }
            } else {
                missing.push(backend.clone());
            }
        }
        if let Some((record, bytes)) = found {
            for backend in missing {
                put_immutable(
                    backend.as_ref(),
                    &object_key,
                    bytes.clone(),
                    "application/json",
                    token,
                )
                .await?;
            }
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    pub(super) async fn clear_quarantine(
        &self,
        blob: &LogicalBlobId,
        token: &ControlToken,
    ) -> Result<()> {
        let object_key = format!("{QUARANTINE_PREFIX}{}.json", blob.path_hash());
        for backend in self.target_backends(Some(blob))? {
            if let Some(value) = backend.control_get_object(&object_key, token).await? {
                backend
                    .control_delete_object(&object_key, value.etag.as_deref(), token)
                    .await?;
            }
        }
        Ok(())
    }
}

fn head_condition(head: Option<&ValidatedHead>) -> PutCondition {
    match head.and_then(|value| value.backend_etag.clone()) {
        Some(etag) => PutCondition::IfMatch(etag),
        None => PutCondition::IfAbsent,
    }
}
