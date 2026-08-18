use super::*;

impl ReconcilerEngine {
    pub fn new(
        ring: Arc<SignedRing>,
        backends: HashMap<String, SharedBackend>,
        signer: Arc<dyn ManifestSigner>,
        token_provider: SharedTokenProvider,
        posture_auditor: SharedRbacPostureAuditor,
        options: ReconcilerOptions,
    ) -> Self {
        Self {
            ring,
            backends,
            signer,
            token_provider,
            posture_auditor,
            physical_collection_delay: options.physical_collection_delay,
            history_compaction_max_versions_per_cycle: options
                .history_compaction_max_versions_per_cycle,
            head_discovery_batch_size: options.head_discovery_batch_size,
            staged_block_gc_max_records_per_cycle: options.staged_block_gc_max_records_per_cycle,
        }
    }

    pub async fn audit_rbac_posture(&self) -> Result<RbacPostureReport> {
        self.posture_auditor
            .audit()
            .await
            .context("RBAC posture audit failed closed")
    }

    pub async fn validate_signing_provider(&self) -> Result<String> {
        let payload = b"overmesh-v050-live-signing-canary";
        let signed_at_unix_ms = now_unix_ms();
        let signature = self
            .signer
            .sign(
                SignatureDomain::ReconciliationRecord,
                signed_at_unix_ms,
                payload,
            )
            .await
            .context("manifest signing provider failed")?;
        self.signer
            .verify(
                self.signer.key_id(),
                SignatureDomain::ReconciliationRecord,
                signed_at_unix_ms,
                payload,
                &signature,
            )
            .context("manifest signing provider returned an unverifiable signature")?;
        Ok(self.signer.key_id().to_owned())
    }

    pub async fn run_cycle(&self) -> Result<CycleReport> {
        self.run_cycle_with_mode(HeadDiscoveryMode::Incremental)
            .await
    }

    pub async fn run_full_audit_cycle(&self) -> Result<CycleReport> {
        self.run_cycle_with_mode(HeadDiscoveryMode::FullAudit).await
    }

    async fn run_cycle_with_mode(&self, mode: HeadDiscoveryMode) -> Result<CycleReport> {
        self.audit_rbac_posture().await?;
        let started_at_unix_ms = now_unix_ms();
        let token = self.token_provider.token().await?;
        self.reconcile_staged_blocks(&token).await?;
        let discovery = self.discover_heads(&token, mode).await?;
        let mut blobs = Vec::with_capacity(discovery.candidates.len());
        for candidate in discovery.candidates {
            blobs.push(self.reconcile_head(&candidate, &token).await?);
        }
        if let Some(cursor) = discovery.next_cursor {
            self.persist_cursor(cursor, &token).await?;
        }
        Ok(CycleReport {
            api_version: "reconciler.overmesh.io/report/v1",
            project_version: env!("CARGO_PKG_VERSION"),
            ring_version: self.ring.ring_version,
            started_at_unix_ms,
            completed_at_unix_ms: now_unix_ms(),
            blobs,
        })
    }

    pub async fn recover(
        &self,
        logical_blob: &LogicalBlobId,
        source_replica: &str,
    ) -> Result<BlobReport> {
        self.audit_rbac_posture().await?;
        let token = self.token_provider.token().await?;
        let replicas = self.ring.replicas_for(logical_blob)?;
        let primary = self.backend(&replicas[0].id)?;
        let lock_key = format!("locks/{}", logical_blob.path_hash());
        self.with_lease(
            primary,
            &lock_key,
            &token,
            self.recover_locked(logical_blob, source_replica, &token),
        )
        .await
    }

    async fn recover_locked(
        &self,
        logical_blob: &LogicalBlobId,
        source_replica: &str,
        token: &ControlToken,
    ) -> Result<BlobReport> {
        let replicas = self.ring.replicas_for(logical_blob)?;
        let source_index = replicas
            .iter()
            .position(|replica| replica.id == source_replica)
            .with_context(|| {
                format!(
                    "replica {source_replica} is not assigned to {}",
                    logical_blob.canonical()
                )
            })?;
        let target_index = 1_usize.saturating_sub(source_index);
        let source = self.backend(&replicas[source_index].id)?;
        let target = self.backend(&replicas[target_index].id)?;
        let head_object = head_object_key(logical_blob);
        let source_validation = self
            .validate_replica(source.as_ref(), &head_object, token)
            .await;
        let ReplicaValidation::Valid(source_value) = source_validation else {
            bail!("administrator-selected source replica is not fully valid");
        };
        let target_current = target.control_get_object(&head_object, token).await?;
        if let Some(target_value) = &target_current
            && let Ok(target_head) =
                SignedDocument::<CommitManifest>::from_bytes(&target_value.bytes)
            && target_head
                .verify(
                    SignatureDomain::CommitManifest,
                    &target_head.payload.signing_key_id,
                    self.signer.as_ref(),
                )
                .is_ok()
        {
            ensure!(
                target_head.payload.logical_version
                    <= source_value.head.signed.payload.logical_version,
                "administrator recovery cannot roll back a newer signed logical version"
            );
        }
        self.copy_for_recovery(
            &source_value,
            source.as_ref(),
            target.as_ref(),
            target_current.as_ref(),
            &head_object,
            token,
        )
        .await?;
        let verified = self
            .validate_replica(target.as_ref(), &head_object, token)
            .await;
        ensure!(
            matches!(
                verified,
                ReplicaValidation::Valid(ref target_value)
                    if target_value.head.bytes == source_value.head.bytes
            ),
            "administrator-authorized recovery did not produce an identical valid replica"
        );
        match self
            .reconcile_catalog_current(
                logical_blob,
                &head_object,
                source.as_ref(),
                target.as_ref(),
                token,
            )
            .await?
        {
            CatalogReconciliation::Current | CatalogReconciliation::Repaired => {}
            CatalogReconciliation::Conflict(reason) => {
                return self
                    .quarantine(Some(logical_blob), &head_object, reason, token)
                    .await;
            }
        }
        self.write_audit(
            Some(logical_blob),
            &head_object,
            ReconciliationClassification::Quarantined,
            ReconciliationRecordAction::Recovered,
            "administrator-authorized recovery from a fully validated source",
            Some(source.id()),
            Some(target.id()),
            token,
        )
        .await?;
        self.clear_quarantine(logical_blob, token).await?;
        info!(
            blob = logical_blob.canonical(),
            source = source.id(),
            target = target.id(),
            "blob recovered"
        );
        Ok(BlobReport {
            blob: Some(logical_blob.canonical().to_owned()),
            head_object,
            health_before: HealthState::Quarantined,
            health_after: HealthState::Healthy,
            action: ReconciliationAction::Recovered,
            source_replica: Some(source.id().to_owned()),
            target_replica: Some(target.id().to_owned()),
            detail: "administrator-authorized recovery completed".to_owned(),
        })
    }

    async fn reconcile_head(
        &self,
        candidate: &HeadCandidate,
        token: &ControlToken,
    ) -> Result<BlobReport> {
        let head_object = &candidate.object_key;
        let path_hash = head_hash(head_object)?;
        let blob = self.discover_blob_path(head_object, token).await?;
        let lock_backend = if let Some(blob) = blob.as_ref() {
            let replicas = self.ring.replicas_for(blob)?;
            ensure!(
                replicas
                    .iter()
                    .any(|replica| replica.id == candidate.discovered_on),
                "head was discovered on a backend outside its active Ring placement"
            );
            self.backend(&replicas[0].id)?
        } else {
            self.backend(&candidate.discovered_on)?
        };
        let lock_key = format!("locks/{path_hash}");
        self.with_lease(
            lock_backend,
            &lock_key,
            token,
            self.reconcile_head_locked(head_object, blob.as_ref(), &candidate.discovered_on, token),
        )
        .await
    }

    pub(super) async fn reconcile_head_locked(
        &self,
        head_object: &str,
        blob: Option<&LogicalBlobId>,
        discovered_on: &str,
        token: &ControlToken,
    ) -> Result<BlobReport> {
        let path_hash = head_hash(head_object)?;
        if let Some(record) = self
            .load_quarantine(path_hash, blob, discovered_on, token)
            .await?
        {
            return Ok(BlobReport {
                blob: record.payload.blob,
                head_object: head_object.to_owned(),
                health_before: HealthState::Quarantined,
                health_after: HealthState::Quarantined,
                action: ReconciliationAction::None,
                source_replica: None,
                target_replica: None,
                detail: record.payload.reason,
            });
        }

        let Some(blob) = blob else {
            let backend = self.backend(discovered_on)?;
            return match self
                .validate_replica(backend.as_ref(), head_object, token)
                .await
            {
                ReplicaValidation::Tampered { blob, reason } => {
                    self.quarantine(blob.as_ref(), head_object, reason, token)
                        .await
                }
                ReplicaValidation::Unavailable { reason } => {
                    bail!("replica {} is unavailable: {reason}", backend.id())
                }
                ReplicaValidation::MissingHead => Ok(BlobReport {
                    blob: None,
                    head_object: head_object.to_owned(),
                    health_before: HealthState::Absent,
                    health_after: HealthState::Absent,
                    action: ReconciliationAction::None,
                    source_replica: None,
                    target_replica: None,
                    detail: "discovered head disappeared before validation".to_owned(),
                }),
                _ => bail!("signed head could not be routed to its active Ring replicas"),
            };
        };
        let replicas = self.ring.replicas_for(blob)?;
        ensure!(
            replicas.len() == 2,
            "V1 reconciliation requires replicationFactor 2"
        );
        let first = self.backend(&replicas[0].id)?;
        let second = self.backend(&replicas[1].id)?;
        let first_validation = self
            .validate_replica(first.as_ref(), head_object, token)
            .await;
        let second_validation = self
            .validate_replica(second.as_ref(), head_object, token)
            .await;
        if matches!(
            (
                first_validation.fully_validated_head(),
                second_validation.fully_validated_head(),
            ),
            (Some(first_head), Some(second_head)) if first_head.bytes == second_head.bytes
        ) {
            match self
                .reconcile_catalog_current(
                    blob,
                    head_object,
                    first.as_ref(),
                    second.as_ref(),
                    token,
                )
                .await?
            {
                CatalogReconciliation::Current | CatalogReconciliation::Repaired => {}
                CatalogReconciliation::Conflict(reason) => {
                    return self
                        .quarantine(Some(blob), head_object, reason, token)
                        .await;
                }
            }
        }
        let report = self
            .reconcile_pair(
                head_object,
                first.as_ref(),
                first_validation,
                second.as_ref(),
                second_validation,
                token,
            )
            .await?;
        if matches!(
            report.health_after,
            HealthState::Healthy | HealthState::Tombstoned
        ) {
            match self
                .reconcile_catalog_current(
                    blob,
                    head_object,
                    first.as_ref(),
                    second.as_ref(),
                    token,
                )
                .await?
            {
                CatalogReconciliation::Current | CatalogReconciliation::Repaired => {}
                CatalogReconciliation::Conflict(reason) => {
                    return self
                        .quarantine(Some(blob), head_object, reason, token)
                        .await;
                }
            }
        }
        Ok(report)
    }

    async fn with_lease<T, F>(
        &self,
        backend: SharedBackend,
        lock_key: &str,
        token: &ControlToken,
        operation: F,
    ) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        let lease = backend.control_acquire_lock(lock_key, token).await?;
        let maintenance = maintain_lease(backend.as_ref(), &lease, token, Duration::from_secs(30));
        tokio::pin!(operation);
        tokio::pin!(maintenance);
        let result = tokio::select! {
            result = &mut operation => result,
            error = &mut maintenance => Err(error.into()),
        };
        if let Err(error) = backend.control_release_lock(&lease, token).await {
            if result.is_ok() {
                return Err(error.into());
            }
            warn!(error = %error, "failed to release reconciliation lease");
        }
        result
    }

    async fn reconcile_pair(
        &self,
        head_object: &str,
        first_backend: &dyn ReplicaBackend,
        first: ReplicaValidation,
        second_backend: &dyn ReplicaBackend,
        second: ReplicaValidation,
        token: &ControlToken,
    ) -> Result<BlobReport> {
        if let ReplicaValidation::Tampered { blob, reason } = &first {
            return self
                .quarantine(
                    blob.as_ref().or_else(|| second.blob()),
                    head_object,
                    format!("{}: {reason}", first_backend.id()),
                    token,
                )
                .await;
        }
        if let ReplicaValidation::Tampered { blob, reason } = &second {
            return self
                .quarantine(
                    blob.as_ref().or_else(|| first.blob()),
                    head_object,
                    format!("{}: {reason}", second_backend.id()),
                    token,
                )
                .await;
        }
        if let ReplicaValidation::Unavailable { reason } = &first {
            bail!("replica {} is unavailable: {reason}", first_backend.id());
        }
        if let ReplicaValidation::Unavailable { reason } = &second {
            bail!("replica {} is unavailable: {reason}", second_backend.id());
        }

        match (&first, &second) {
            (ReplicaValidation::MissingHead, ReplicaValidation::MissingHead) => Ok(BlobReport {
                blob: None,
                head_object: head_object.to_owned(),
                health_before: HealthState::Absent,
                health_after: HealthState::Absent,
                action: ReconciliationAction::None,
                source_replica: None,
                target_replica: None,
                detail: "no committed head exists on either replica".to_owned(),
            }),
            (
                ReplicaValidation::RecoverableTombstone {
                    replica: first,
                    reason: first_reason,
                },
                ReplicaValidation::RecoverableTombstone {
                    replica: second,
                    reason: second_reason,
                },
            ) if first.head.bytes == second.head.bytes => {
                warn!(
                    blob = first.head.logical_blob.canonical(),
                    first_reason, second_reason, "finalizing tombstone high-water checkpoints"
                );
                self.repair_high_water_checkpoint(first, first_backend, token)
                    .await?;
                self.repair_high_water_checkpoint(second, second_backend, token)
                    .await?;
                self.reconcile_garbage_collection(
                    head_object,
                    first_backend,
                    first,
                    second_backend,
                    second,
                    token,
                )
                .await
            }
            (
                ReplicaValidation::Valid(first),
                ReplicaValidation::RecoverableTombstone {
                    replica: second, ..
                },
            ) if first.head.bytes == second.head.bytes => {
                self.repair_high_water_checkpoint(second, second_backend, token)
                    .await?;
                self.reconcile_garbage_collection(
                    head_object,
                    first_backend,
                    first,
                    second_backend,
                    second,
                    token,
                )
                .await
            }
            (
                ReplicaValidation::RecoverableTombstone { replica: first, .. },
                ReplicaValidation::Valid(second),
            ) if first.head.bytes == second.head.bytes => {
                self.repair_high_water_checkpoint(first, first_backend, token)
                    .await?;
                self.reconcile_garbage_collection(
                    head_object,
                    first_backend,
                    first,
                    second_backend,
                    second,
                    token,
                )
                .await
            }
            (
                ReplicaValidation::RecoverableTombstone {
                    replica: source, ..
                },
                ReplicaValidation::Valid(target),
            ) if authoritative_over(&source.head, &target.head) => {
                self.repair_high_water_checkpoint(source, first_backend, token)
                    .await?;
                self.repair(
                    head_object,
                    HealthState::Drifted,
                    source,
                    first_backend,
                    second_backend,
                    Some(&target.head),
                    token,
                )
                .await
            }
            (
                ReplicaValidation::Valid(target),
                ReplicaValidation::RecoverableTombstone {
                    replica: source, ..
                },
            ) if authoritative_over(&source.head, &target.head) => {
                self.repair_high_water_checkpoint(source, second_backend, token)
                    .await?;
                self.repair(
                    head_object,
                    HealthState::Drifted,
                    source,
                    second_backend,
                    first_backend,
                    Some(&target.head),
                    token,
                )
                .await
            }
            (ReplicaValidation::Valid(first), ReplicaValidation::Valid(second))
                if first.head.bytes == second.head.bytes =>
            {
                if first.head.signed.payload.state == ManifestState::Tombstoned {
                    return self
                        .reconcile_garbage_collection(
                            head_object,
                            first_backend,
                            first,
                            second_backend,
                            second,
                            token,
                        )
                        .await;
                }
                self.reconcile_garbage_collection(
                    head_object,
                    first_backend,
                    first,
                    second_backend,
                    second,
                    token,
                )
                .await
            }
            (ReplicaValidation::Valid(source), ReplicaValidation::MissingHead) => {
                self.repair(
                    head_object,
                    HealthState::Missing,
                    source,
                    first_backend,
                    second_backend,
                    None,
                    token,
                )
                .await
            }
            (
                ReplicaValidation::RecoverableTombstone {
                    replica: source, ..
                },
                ReplicaValidation::MissingHead,
            ) => {
                self.repair_high_water_checkpoint(source, first_backend, token)
                    .await?;
                self.repair(
                    head_object,
                    HealthState::Missing,
                    source,
                    first_backend,
                    second_backend,
                    None,
                    token,
                )
                .await
            }
            (ReplicaValidation::MissingHead, ReplicaValidation::Valid(source)) => {
                self.repair(
                    head_object,
                    HealthState::Missing,
                    source,
                    second_backend,
                    first_backend,
                    None,
                    token,
                )
                .await
            }
            (
                ReplicaValidation::MissingHead,
                ReplicaValidation::RecoverableTombstone {
                    replica: source, ..
                },
            ) => {
                self.repair_high_water_checkpoint(source, second_backend, token)
                    .await?;
                self.repair(
                    head_object,
                    HealthState::Missing,
                    source,
                    second_backend,
                    first_backend,
                    None,
                    token,
                )
                .await
            }
            (ReplicaValidation::Valid(first_value), ReplicaValidation::Valid(second_value)) => {
                if authoritative_over(&first_value.head, &second_value.head) {
                    self.repair(
                        head_object,
                        HealthState::Drifted,
                        first_value,
                        first_backend,
                        second_backend,
                        Some(&second_value.head),
                        token,
                    )
                    .await
                } else if authoritative_over(&second_value.head, &first_value.head) {
                    self.repair(
                        head_object,
                        HealthState::Drifted,
                        second_value,
                        second_backend,
                        first_backend,
                        Some(&first_value.head),
                        token,
                    )
                    .await
                } else {
                    self.quarantine(
                        Some(&first_value.head.logical_blob),
                        head_object,
                        "different valid heads have no provable unique authority".to_owned(),
                        token,
                    )
                    .await
                }
            }
            (
                ReplicaValidation::Valid(source),
                ReplicaValidation::Incomplete {
                    head: target_head,
                    reason,
                },
            ) if source.head.bytes == target_head.bytes
                || authoritative_over(&source.head, target_head) =>
            {
                warn!(
                    blob = source.head.logical_blob.canonical(),
                    target = second_backend.id(),
                    reason,
                    "repairing incomplete replica"
                );
                self.repair(
                    head_object,
                    if source.head.bytes == target_head.bytes {
                        HealthState::Missing
                    } else {
                        HealthState::Drifted
                    },
                    source,
                    first_backend,
                    second_backend,
                    Some(target_head),
                    token,
                )
                .await
            }
            (
                ReplicaValidation::Incomplete {
                    head: target_head,
                    reason,
                },
                ReplicaValidation::Valid(source),
            ) if source.head.bytes == target_head.bytes
                || authoritative_over(&source.head, target_head) =>
            {
                warn!(
                    blob = source.head.logical_blob.canonical(),
                    target = first_backend.id(),
                    reason,
                    "repairing incomplete replica"
                );
                self.repair(
                    head_object,
                    if source.head.bytes == target_head.bytes {
                        HealthState::Missing
                    } else {
                        HealthState::Drifted
                    },
                    source,
                    second_backend,
                    first_backend,
                    Some(target_head),
                    token,
                )
                .await
            }
            (ReplicaValidation::Incomplete { head, reason }, ReplicaValidation::MissingHead)
            | (ReplicaValidation::MissingHead, ReplicaValidation::Incomplete { head, reason }) => {
                Ok(BlobReport {
                    blob: Some(head.logical_blob.canonical().to_owned()),
                    head_object: head_object.to_owned(),
                    health_before: HealthState::Missing,
                    health_after: HealthState::Missing,
                    action: ReconciliationAction::None,
                    source_replica: None,
                    target_replica: None,
                    detail: format!("no fully validated repair source is available: {reason}"),
                })
            }
            (ReplicaValidation::Incomplete { head, .. }, ReplicaValidation::Incomplete { .. }) => {
                Ok(BlobReport {
                    blob: Some(head.logical_blob.canonical().to_owned()),
                    head_object: head_object.to_owned(),
                    health_before: HealthState::Missing,
                    health_after: HealthState::Missing,
                    action: ReconciliationAction::None,
                    source_replica: None,
                    target_replica: None,
                    detail: "neither incomplete replica is a valid repair source".to_owned(),
                })
            }
            _ => {
                self.quarantine(
                    first.blob().or_else(|| second.blob()),
                    head_object,
                    "replica state is inconsistent and no safe repair transition exists".to_owned(),
                    token,
                )
                .await
            }
        }
    }

    pub(super) fn backend(&self, id: &str) -> Result<SharedBackend> {
        self.backends
            .get(id)
            .cloned()
            .with_context(|| format!("Ring node {id} has no configured backend"))
    }

    pub(super) fn target_backends(
        &self,
        blob: Option<&LogicalBlobId>,
    ) -> Result<Vec<SharedBackend>> {
        match blob {
            Some(blob) => self
                .ring
                .replicas_for(blob)?
                .into_iter()
                .map(|node| self.backend(&node.id))
                .collect(),
            None => self
                .ring
                .nodes
                .iter()
                .map(|node| self.backend(&node.id))
                .collect(),
        }
    }
}

pub(super) fn authoritative_over(newer: &ValidatedHead, older: &ValidatedHead) -> bool {
    newer.signed.payload.logical_version == older.signed.payload.logical_version + 1
        && newer.signed.payload.previous_logical_etag.as_deref()
            == Some(&older.signed.payload.logical_etag)
}
