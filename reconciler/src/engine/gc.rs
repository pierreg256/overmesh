use super::*;

impl ReconcilerEngine {
    pub(super) async fn reconcile_garbage_collection(
        &self,
        head_object: &str,
        first_backend: &dyn ReplicaBackend,
        first: &ValidatedReplica,
        second_backend: &dyn ReplicaBackend,
        second: &ValidatedReplica,
        token: &ControlToken,
    ) -> Result<BlobReport> {
        let plan = self
            .plan_garbage_collection(
                head_object,
                first_backend,
                first,
                second_backend,
                second,
                token,
            )
            .await?;
        self.execute_garbage_collection(head_object, first_backend, second_backend, plan, token)
            .await
    }

    async fn plan_garbage_collection(
        &self,
        head_object: &str,
        first_backend: &dyn ReplicaBackend,
        first: &ValidatedReplica,
        second_backend: &dyn ReplicaBackend,
        second: &ValidatedReplica,
        token: &ControlToken,
    ) -> Result<GarbageCollectionPlan> {
        ensure!(
            first.head.bytes == second.head.bytes,
            "replica heads differ before garbage-collection planning"
        );
        ensure!(
            first.high_water_checkpoint == first.head.bytes
                && second.high_water_checkpoint == second.head.bytes
                && first.high_water_checkpoint == second.high_water_checkpoint,
            "current head and durable high-water checkpoints do not correspond exactly"
        );
        let active = &first.head.signed.payload;
        let health = if active.state == ManifestState::Tombstoned {
            HealthState::Tombstoned
        } else {
            HealthState::Healthy
        };
        let path_hash = head_hash(head_object)?;
        let checkpoint = self
            .validate_or_repair_compaction_checkpoint(
                path_hash,
                head_object,
                active,
                first_backend,
                second_backend,
                token,
            )
            .await?;
        let history = self
            .validate_history(
                path_hash,
                head_object,
                active,
                &first.head.bytes,
                checkpoint.as_ref(),
                first_backend,
                second_backend,
                token,
            )
            .await?;
        let markers = self
            .validate_garbage_collection_markers(
                path_hash,
                head_object,
                active,
                &history,
                checkpoint.as_ref(),
                first_backend,
                second_backend,
                token,
            )
            .await?;
        let delay_ms = u64::try_from(self.physical_collection_delay.as_millis())
            .context("physical collection delay exceeds u64 milliseconds")?;
        let now = now_unix_ms();
        let mut eligible_through = None;
        for pair in history.entries.windows(2) {
            let predecessor = &pair[0].signed.payload;
            let successor = &pair[1].signed.payload;
            let eligible_at = successor
                .committed_at_unix_ms
                .checked_add(delay_ms)
                .context("physical collection retention deadline overflow")?;
            if now < eligible_at {
                break;
            }
            eligible_through = Some(predecessor.logical_version);
        }

        let latest_through = markers.latest_through.unwrap_or(0);
        let new_through = eligible_through.filter(|through| *through > latest_through);
        let mut data_deletes = BTreeSet::new();
        let mut metadata_deletes = BTreeSet::new();
        let mut collected_versions = Vec::new();
        if let Some(through) = new_through {
            for entry in &history.entries {
                let manifest = &entry.signed.payload;
                if manifest.logical_version <= latest_through || manifest.logical_version > through
                {
                    continue;
                }
                match manifest.state {
                    ManifestState::Committed => {
                        let (data, metadata) = self
                            .validate_candidate_namespace(
                                path_hash,
                                entry,
                                first_backend,
                                second_backend,
                                token,
                            )
                            .await?;
                        data_deletes.insert(data);
                        metadata_deletes.extend(metadata);
                        collected_versions.push(manifest.logical_version);
                    }
                    ManifestState::Tombstoned => {
                        metadata_deletes.extend(
                            self.validate_tombstone_candidate_namespace(
                                path_hash,
                                entry,
                                first_backend,
                                second_backend,
                                token,
                            )
                            .await?,
                        );
                    }
                    ManifestState::Prepared => {
                        bail!("prepared history cannot be garbage-collected")
                    }
                }
            }
        }

        let new_marker = if let Some(through) = new_through {
            let marker = SignedDocument::create(
                GarbageCollectionMarker {
                    api_version: "overmesh.io/garbage-collection-marker/v1".to_owned(),
                    blob: active.blob.clone(),
                    head_object: head_object.to_owned(),
                    ring_version: self.ring.ring_version,
                    history_head_logical_version: active.logical_version,
                    collected_through_logical_version: through,
                    collected_committed_versions: collected_versions.clone(),
                    previous_marker_sha256: markers
                        .latest_evidence
                        .as_ref()
                        .map(|evidence| evidence.sha256.clone()),
                    physical_collection_delay_ms: delay_ms,
                    collected_at_unix_ms: now,
                    signing_key_id: self.signer.key_id().to_owned(),
                },
                SignatureDomain::GarbageCollectionMarker,
                self.signer.as_ref(),
            )
            .await?;
            Some((
                garbage_collection_marker_key(path_hash, through),
                marker.canonical_bytes()?,
            ))
        } else {
            None
        };

        let latest_evidence = if let Some((marker_key, marker_bytes)) = &new_marker {
            let marker = SignedDocument::<GarbageCollectionMarker>::from_bytes(marker_bytes)
                .context("new garbage-collection marker is not valid JSON")?;
            Some(garbage_collection_evidence(
                marker_key.clone(),
                marker_bytes,
                &marker.payload,
            ))
        } else {
            markers.latest_evidence.clone()
        };
        let current_floor = checkpoint.as_ref().map_or(0, |value| {
            value.signed.payload.compacted_through_logical_version
        });
        let compaction_target = latest_evidence.as_ref().and_then(|evidence| {
            let maximum = current_floor.saturating_add(
                u64::try_from(self.history_compaction_max_versions_per_cycle).unwrap_or(u64::MAX),
            );
            let target = evidence.collected_through_logical_version.min(maximum);
            (target > current_floor).then_some(target)
        });
        let new_compaction_checkpoint = if let Some(target) = compaction_target {
            let terminal = history
                .entries
                .iter()
                .find(|entry| entry.signed.payload.logical_version == target)
                .context("compaction target is not present in retained history")?;
            let evidence = latest_evidence
                .as_ref()
                .context("compaction target has no garbage-collection evidence")?;
            let checkpoint_version = checkpoint.as_ref().map_or(1, |value| {
                value.signed.payload.checkpoint_version.saturating_add(1)
            });
            let signed = SignedDocument::create(
                HistoryCompactionCheckpoint {
                    api_version: HISTORY_COMPACTION_API_VERSION.to_owned(),
                    blob: active.blob.clone(),
                    path_hash: path_hash.to_owned(),
                    head_object: head_object.to_owned(),
                    ring_version: self.ring.ring_version,
                    checkpoint_version,
                    compacted_through_logical_version: target,
                    compacted_through_state: terminal.signed.payload.state,
                    compacted_through_logical_etag: terminal.signed.payload.logical_etag.clone(),
                    compacted_through_committed_at_unix_ms: terminal
                        .signed
                        .payload
                        .committed_at_unix_ms,
                    covered_terminal_manifest_sha256: sha256_bytes(&terminal.bytes),
                    previous_checkpoint_sha256: checkpoint
                        .as_ref()
                        .map(|value| sha256_bytes(&value.bytes)),
                    previous_checkpoint_version: checkpoint
                        .as_ref()
                        .map(|value| value.signed.payload.checkpoint_version),
                    garbage_collection_marker_object: evidence.object_key.clone(),
                    garbage_collection_marker_sha256: evidence.sha256.clone(),
                    garbage_collection_through_logical_version: evidence
                        .collected_through_logical_version,
                    garbage_collection_history_head_logical_version: evidence
                        .history_head_logical_version,
                    garbage_collected_committed_versions: evidence
                        .collected_committed_versions
                        .clone(),
                    garbage_collection_delay_ms: evidence.physical_collection_delay_ms,
                    garbage_collected_at_unix_ms: evidence.collected_at_unix_ms,
                    compacted_at_unix_ms: now,
                    signing_key_id: self.signer.key_id().to_owned(),
                },
                SignatureDomain::HistoryCompactionCheckpoint,
                self.signer.as_ref(),
            )
            .await?;
            validate_compaction_checkpoint(
                &signed.payload,
                path_hash,
                head_object,
                active,
                self.ring.ring_version,
            )?;
            Some(CheckpointPublication {
                expected_previous_bytes: checkpoint.as_ref().map(|value| value.bytes.clone()),
                bytes: signed.canonical_bytes()?,
            })
        } else {
            None
        };
        let effective_floor = compaction_target.unwrap_or(current_floor);
        ensure!(
            effective_floor < active.logical_version,
            "active current generation may not be compacted"
        );
        let mut history_deletes = history.covered_deletes.clone();
        history_deletes.extend(
            history
                .entries
                .iter()
                .filter(|entry| entry.signed.payload.logical_version <= effective_floor)
                .map(history_entry_delete),
        );
        history_deletes.sort_by(|left, right| left.object_key.cmp(&right.object_key));
        history_deletes.dedup_by(|left, right| left.object_key == right.object_key);
        let anchor_marker_key = latest_evidence
            .as_ref()
            .filter(|_| effective_floor > 0)
            .map(|evidence| evidence.object_key.as_str());
        let obsolete_marker_deletes = markers
            .objects
            .iter()
            .filter(|(through, key, _)| {
                latest_evidence.as_ref().is_some_and(|evidence| {
                    *through <= evidence.collected_through_logical_version
                        && Some(key.as_str()) != anchor_marker_key
                })
            })
            .map(|(_, _, deletion)| deletion.clone())
            .collect();
        let compaction_checkpoint_bytes = new_compaction_checkpoint
            .as_ref()
            .map(|publication| publication.bytes.clone())
            .or_else(|| checkpoint.as_ref().map(|value| value.bytes.clone()));
        let compaction_marker_verification = latest_evidence.as_ref().and_then(|evidence| {
            evidence
                .bytes
                .as_ref()
                .map(|bytes| (evidence.object_key.clone(), bytes.clone()))
        });

        Ok(GarbageCollectionPlan {
            blob: active.blob.clone(),
            health,
            marker_repairs: markers.repairs,
            data_deletes: data_deletes.into_iter().collect(),
            metadata_deletes: metadata_deletes.into_iter().collect(),
            new_marker,
            compaction_marker_verification,
            new_compaction_checkpoint,
            history_deletes,
            obsolete_marker_deletes,
            compaction_checkpoint_bytes,
            collected_versions,
            eligible_through,
        })
    }

    async fn execute_garbage_collection(
        &self,
        head_object: &str,
        first_backend: &dyn ReplicaBackend,
        second_backend: &dyn ReplicaBackend,
        plan: GarbageCollectionPlan,
        token: &ControlToken,
    ) -> Result<BlobReport> {
        for repair in &plan.marker_repairs {
            let backend = self.backend(&repair.backend_id)?;
            put_immutable(
                backend.as_ref(),
                &repair.object_key,
                repair.bytes.clone(),
                "application/json",
                token,
            )
            .await?;
        }
        for data in &plan.data_deletes {
            tokio::try_join!(
                first_backend.service_delete_data_object(
                    &data.container,
                    &data.object_key,
                    None,
                    token
                ),
                second_backend.service_delete_data_object(
                    &data.container,
                    &data.object_key,
                    None,
                    token
                )
            )?;
        }
        for object in &plan.metadata_deletes {
            tokio::try_join!(
                first_backend.control_delete_object(object, None, token),
                second_backend.control_delete_object(object, None, token)
            )?;
        }
        if let Some((marker_key, marker_bytes)) = &plan.new_marker {
            tokio::try_join!(
                put_immutable(
                    first_backend,
                    marker_key,
                    marker_bytes.clone(),
                    "application/json",
                    token
                ),
                put_immutable(
                    second_backend,
                    marker_key,
                    marker_bytes.clone(),
                    "application/json",
                    token
                )
            )?;
        }
        if let Some((marker_key, marker_bytes)) = &plan.compaction_marker_verification {
            verify_identical_control_objects(
                first_backend,
                second_backend,
                marker_key,
                marker_bytes,
                token,
            )
            .await?;
        }
        if let Some(publication) = &plan.new_compaction_checkpoint {
            self.publish_compaction_checkpoint(
                first_backend,
                second_backend,
                head_hash(head_object)?,
                publication,
                token,
            )
            .await?;
        }
        if let Some(checkpoint_bytes) = &plan.compaction_checkpoint_bytes {
            verify_identical_control_objects(
                first_backend,
                second_backend,
                &history_compaction_checkpoint_key(head_hash(head_object)?),
                checkpoint_bytes,
                token,
            )
            .await?;
            for deletion in &plan.history_deletes {
                delete_validated_control_object(first_backend, second_backend, deletion, token)
                    .await?;
            }
            for deletion in &plan.obsolete_marker_deletes {
                delete_validated_control_object(first_backend, second_backend, deletion, token)
                    .await?;
            }
        } else {
            ensure!(
                plan.history_deletes.is_empty() && plan.obsolete_marker_deletes.is_empty(),
                "history deletion was planned without a durable compaction checkpoint"
            );
        }
        let changed = plan.new_marker.is_some()
            || plan.new_compaction_checkpoint.is_some()
            || !plan.history_deletes.is_empty()
            || !plan.obsolete_marker_deletes.is_empty();
        let detail = if changed {
            format!(
                "validated checkpoint-anchored history, collected committed generations {:?} through watermark {}, and compacted safely",
                plan.collected_versions,
                plan.eligible_through.unwrap_or(0)
            )
        } else if !plan.marker_repairs.is_empty() {
            "validated complete history and repaired one-sided garbage-collection marker publication"
                .to_owned()
        } else {
            "validated complete history; no additional superseded generation has aged past retention"
                .to_owned()
        };
        Ok(BlobReport {
            blob: Some(plan.blob),
            head_object: head_object.to_owned(),
            health_before: plan.health,
            health_after: plan.health,
            action: if changed {
                ReconciliationAction::GarbageCollected
            } else {
                ReconciliationAction::None
            },
            source_replica: None,
            target_replica: None,
            detail,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn validate_candidate_namespace(
        &self,
        path_hash: &str,
        history: &ValidatedHistoryEntry,
        first_backend: &dyn ReplicaBackend,
        second_backend: &dyn ReplicaBackend,
        token: &ControlToken,
    ) -> Result<(DataDelete, BTreeSet<String>)> {
        let manifest = &history.signed.payload;
        ensure!(
            manifest.state == ManifestState::Committed,
            "only committed generations may be physically collected"
        );
        let version_prefix = expected_version_prefix(path_hash, manifest)?;
        let version_namespace = format!("{version_prefix}/");
        let (first_objects, second_objects) = tokio::try_join!(
            first_backend.control_list_objects(&version_namespace, token),
            second_backend.control_list_objects(&version_namespace, token)
        )?;
        let objects = first_objects
            .into_iter()
            .chain(second_objects)
            .collect::<BTreeSet<_>>();
        let mut values = BTreeMap::new();
        for object_key in &objects {
            validate_metadata_object_name(&version_prefix, object_key)?;
            let (first_value, second_value) = tokio::try_join!(
                first_backend.control_get_object(object_key, token),
                second_backend.control_get_object(object_key, token)
            )?;
            let bytes = match (first_value, second_value) {
                (Some(first), Some(second)) => {
                    ensure!(
                        first.bytes == second.bytes,
                        "candidate metadata bytes differ between replicas"
                    );
                    first.bytes
                }
                (Some(value), None) | (None, Some(value)) => value.bytes,
                (None, None) => bail!("listed candidate metadata object is missing"),
            };
            values.insert(object_key.clone(), bytes);
        }

        let committed_key = format!("{version_prefix}/committed.json");
        if let Some(bytes) = values.get(&committed_key) {
            ensure!(
                bytes == &history.bytes,
                "candidate committed sidecar differs from signed high-water history"
            );
        }
        let prepared_key = format!("{version_prefix}/prepared.json");
        if let Some(bytes) = values.get(&prepared_key) {
            validate_prepared_candidate(bytes, manifest, self.signer.as_ref())?;
        }
        let block_key = format!("{version_prefix}/block-manifest.json");
        let block_manifest = if let Some(bytes) = values.get(&block_key) {
            ensure!(
                sha256_bytes(bytes) == manifest.block_manifest_sha256,
                "candidate block manifest hash differs from signed history"
            );
            let signed = SignedDocument::<BlockManifest>::from_bytes(bytes)
                .context("candidate block manifest is not valid JSON")?;
            ensure!(
                signed.canonical_bytes()? == *bytes,
                "candidate block manifest is not canonically encoded"
            );
            signed
                .verify(
                    SignatureDomain::BlockManifest,
                    &signed.payload.signing_key_id,
                    self.signer.as_ref(),
                )
                .context("candidate block manifest signature validation failed")?;
            validate_block_manifest_link(manifest, &signed.payload)
                .context("candidate block manifest is not bound to signed history")?;
            Some(signed)
        } else {
            None
        };
        for (object_key, bytes) in &values {
            let Some(page_index) = metadata_page_index(&version_prefix, object_key)? else {
                continue;
            };
            let page: BlockManifestPage = serde_json::from_slice(bytes)
                .context("candidate block manifest page is not valid JSON")?;
            ensure!(
                page.blob == manifest.blob
                    && page.write_id == manifest.write_id
                    && page.logical_version == manifest.logical_version
                    && page.page_index == page_index,
                "candidate block manifest page is not bound to signed history"
            );
            if let Some(block_manifest) = &block_manifest {
                let reference = block_manifest
                    .payload
                    .pages
                    .iter()
                    .find(|reference| reference.object == *object_key)
                    .context("candidate block manifest page is not referenced")?;
                ensure!(
                    sha256_bytes(bytes) == reference.sha256,
                    "candidate block manifest page hash validation failed"
                );
                validate_block_manifest_page(&block_manifest.payload, reference, &page)
                    .context("candidate block manifest page structure validation failed")?;
            }
        }
        Ok((
            DataDelete {
                container: manifest.content_container.clone(),
                object_key: manifest.content_object.clone(),
            },
            objects,
        ))
    }

    async fn validate_tombstone_candidate_namespace(
        &self,
        path_hash: &str,
        history: &ValidatedHistoryEntry,
        first_backend: &dyn ReplicaBackend,
        second_backend: &dyn ReplicaBackend,
        token: &ControlToken,
    ) -> Result<BTreeSet<String>> {
        let manifest = &history.signed.payload;
        ensure!(
            manifest.state == ManifestState::Tombstoned,
            "only tombstones may use the tombstone collection path"
        );
        let version_prefix = format!(
            "objects/{path_hash}/tombstones/{}",
            stable_component(&manifest.write_id)
        );
        ensure!(
            manifest.version_object_prefix.as_deref() == Some(version_prefix.as_str()),
            "tombstone collection namespace does not match signed history"
        );
        let prefix = format!("{version_prefix}/");
        let (first_objects, second_objects) = tokio::try_join!(
            first_backend.control_list_objects(&prefix, token),
            second_backend.control_list_objects(&prefix, token)
        )?;
        let objects = first_objects
            .into_iter()
            .chain(second_objects)
            .collect::<BTreeSet<_>>();
        for object_key in &objects {
            let relative = object_key
                .strip_prefix(&prefix)
                .context("tombstone metadata is outside its signed namespace")?;
            ensure!(
                matches!(relative, "prepared.json" | "committed.json"),
                "tombstone namespace contains an unknown metadata object"
            );
            let (first_value, second_value) = tokio::try_join!(
                first_backend.control_get_object(object_key, token),
                second_backend.control_get_object(object_key, token)
            )?;
            let bytes = match (first_value, second_value) {
                (Some(first), Some(second)) => {
                    ensure!(
                        first.bytes == second.bytes,
                        "tombstone metadata bytes differ between replicas"
                    );
                    first.bytes
                }
                (Some(value), None) | (None, Some(value)) => value.bytes,
                (None, None) => bail!("listed tombstone metadata object is missing"),
            };
            if relative == "committed.json" {
                ensure!(
                    bytes == history.bytes,
                    "tombstone committed sidecar differs from signed high-water history"
                );
            } else {
                validate_prepared_candidate(&bytes, manifest, self.signer.as_ref())?;
            }
        }
        Ok(objects)
    }
}

fn validate_metadata_object_name(version_prefix: &str, object_key: &str) -> Result<()> {
    metadata_page_index(version_prefix, object_key)?;
    Ok(())
}

fn metadata_page_index(version_prefix: &str, object_key: &str) -> Result<Option<u32>> {
    let relative = object_key
        .strip_prefix(&format!("{version_prefix}/"))
        .context("candidate metadata object is outside the signed version namespace")?;
    if matches!(
        relative,
        "prepared.json" | "committed.json" | "block-manifest.json"
    ) {
        return Ok(None);
    }
    let page = relative
        .strip_prefix("block-pages/")
        .and_then(|value| value.strip_suffix(".json"))
        .filter(|value| value.len() == 8 && value.bytes().all(|byte| byte.is_ascii_digit()))
        .context("candidate version namespace contains an unknown metadata object")?;
    Ok(Some(
        page.parse::<u32>()
            .context("candidate block page index is invalid")?,
    ))
}

fn validate_prepared_candidate(
    bytes: &[u8],
    committed: &CommitManifest,
    signer: &dyn ManifestSigner,
) -> Result<()> {
    let signed = SignedDocument::<CommitManifest>::from_bytes(bytes)
        .context("candidate prepared manifest is not valid JSON")?;
    ensure!(
        signed.canonical_bytes()? == bytes,
        "candidate prepared manifest is not canonically encoded"
    );
    signed
        .verify(
            SignatureDomain::CommitManifest,
            &signed.payload.signing_key_id,
            signer,
        )
        .context("candidate prepared manifest signature validation failed")?;
    let prepared = &signed.payload;
    ensure!(
        prepared.state == ManifestState::Prepared
            && prepared.blob == committed.blob
            && prepared.write_id == committed.write_id
            && prepared.logical_version == committed.logical_version
            && prepared.logical_etag == committed.logical_etag
            && prepared.previous_logical_etag == committed.previous_logical_etag
            && prepared.ring_version == committed.ring_version
            && prepared.content_length == committed.content_length
            && prepared.content_sha256 == committed.content_sha256
            && prepared.content_container == committed.content_container
            && prepared.content_object == committed.content_object
            && prepared.block_manifest_object == committed.block_manifest_object
            && prepared.block_manifest_sha256 == committed.block_manifest_sha256
            && prepared.version_object_prefix == committed.version_object_prefix
            && prepared.committed_at_unix_ms == committed.committed_at_unix_ms
            && prepared.deleted_at_unix_ms == committed.deleted_at_unix_ms
            && prepared.prepared_replicas.is_empty(),
        "candidate prepared manifest is not the precursor of signed committed history"
    );
    Ok(())
}

fn history_entry_delete(entry: &ValidatedHistoryEntry) -> ControlDelete {
    ControlDelete {
        object_key: entry.object_key.clone(),
        first_etag: entry.first_etag.clone(),
        second_etag: entry.second_etag.clone(),
    }
}
