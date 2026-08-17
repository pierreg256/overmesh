use super::*;

impl ReconcilerEngine {
    pub(super) async fn validate_or_repair_compaction_checkpoint(
        &self,
        head_object: &str,
        active: &CommitManifest,
        active_logical_blob: &LogicalBlobId,
        first_backend: &dyn ReplicaBackend,
        second_backend: &dyn ReplicaBackend,
        token: &ControlToken,
    ) -> Result<Option<LoadedCompactionCheckpoint>> {
        let path_hash = active_logical_blob.path_hash();
        let (first, second) = tokio::try_join!(
            self.load_compaction_checkpoint(
                first_backend,
                &path_hash,
                head_object,
                active,
                active_logical_blob,
                token
            ),
            self.load_compaction_checkpoint(
                second_backend,
                &path_hash,
                head_object,
                active,
                active_logical_blob,
                token
            )
        )?;
        let authoritative = match (first, second) {
            (None, None) => return Ok(None),
            (Some(value), None) => {
                put_immutable(
                    second_backend,
                    &history_compaction_checkpoint_key(&path_hash),
                    value.bytes.clone(),
                    "application/json",
                    token,
                )
                .await?;
                value
            }
            (None, Some(value)) => {
                put_immutable(
                    first_backend,
                    &history_compaction_checkpoint_key(&path_hash),
                    value.bytes.clone(),
                    "application/json",
                    token,
                )
                .await?;
                value
            }
            (Some(first), Some(second)) if first.bytes == second.bytes => first,
            (Some(first), Some(second)) if checkpoint_descends(&first, &second) => {
                replace_control_object(
                    second_backend,
                    &history_compaction_checkpoint_key(&path_hash),
                    first.bytes.clone(),
                    second.etag.as_deref(),
                    token,
                )
                .await?;
                first
            }
            (Some(first), Some(second)) if checkpoint_descends(&second, &first) => {
                replace_control_object(
                    first_backend,
                    &history_compaction_checkpoint_key(&path_hash),
                    second.bytes.clone(),
                    first.etag.as_deref(),
                    token,
                )
                .await?;
                second
            }
            (Some(_), Some(_)) => bail!("history compaction checkpoints conflict"),
        };
        verify_identical_control_objects(
            first_backend,
            second_backend,
            &history_compaction_checkpoint_key(&path_hash),
            &authoritative.bytes,
            token,
        )
        .await?;
        Ok(Some(LoadedCompactionCheckpoint {
            signed: authoritative.signed,
            bytes: authoritative.bytes,
        }))
    }

    async fn load_compaction_checkpoint(
        &self,
        backend: &dyn ReplicaBackend,
        path_hash: &str,
        head_object: &str,
        active: &CommitManifest,
        active_logical_blob: &LogicalBlobId,
        token: &ControlToken,
    ) -> Result<Option<ReplicaCompactionCheckpoint>> {
        let Some(value) = backend
            .control_get_object(&history_compaction_checkpoint_key(path_hash), token)
            .await?
        else {
            return Ok(None);
        };
        let signed = SignedDocument::<HistoryCompactionCheckpoint>::from_bytes(&value.bytes)
            .context("history compaction checkpoint is not valid JSON")?;
        ensure!(
            signed.canonical_bytes()? == value.bytes,
            "history compaction checkpoint is not canonically encoded"
        );
        signed
            .verify(
                SignatureDomain::HistoryCompactionCheckpoint,
                &signed.payload.signing_key_id,
                self.signer.as_ref(),
            )
            .context("history compaction checkpoint signature validation failed")?;
        ensure!(
            signed.signed_at_unix_ms >= signed.payload.compacted_at_unix_ms,
            "history compaction checkpoint predates its payload timestamp"
        );
        validate_compaction_checkpoint(
            &signed.payload,
            active_logical_blob,
            path_hash,
            head_object,
            active,
            self.ring.ring_version,
        )?;
        Ok(Some(ReplicaCompactionCheckpoint {
            signed,
            bytes: value.bytes,
            etag: value.etag,
        }))
    }

    pub(super) async fn publish_compaction_checkpoint(
        &self,
        first_backend: &dyn ReplicaBackend,
        second_backend: &dyn ReplicaBackend,
        path_hash: &str,
        publication: &CheckpointPublication,
        token: &ControlToken,
    ) -> Result<()> {
        let object_key = history_compaction_checkpoint_key(path_hash);
        let (first, second) = tokio::try_join!(
            first_backend.control_get_object(&object_key, token),
            second_backend.control_get_object(&object_key, token)
        )?;
        let (first_result, second_result) = tokio::join!(
            publish_checkpoint_to_backend(first_backend, &object_key, first, publication, token),
            publish_checkpoint_to_backend(second_backend, &object_key, second, publication, token)
        );
        match (first_result, second_result) {
            (Ok(()), Ok(())) => {}
            (Err(first), Ok(())) | (Ok(()), Err(first)) => return Err(first),
            (Err(first), Err(_)) => return Err(first),
        }
        verify_identical_control_objects(
            first_backend,
            second_backend,
            &object_key,
            &publication.bytes,
            token,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn validate_history(
        &self,
        path_hash: &str,
        head_object: &str,
        active: &CommitManifest,
        active_logical_blob: &LogicalBlobId,
        active_bytes: &[u8],
        checkpoint: Option<&LoadedCompactionCheckpoint>,
        first_backend: &dyn ReplicaBackend,
        second_backend: &dyn ReplicaBackend,
        token: &ControlToken,
    ) -> Result<ValidatedHistory> {
        let history_prefix = format!("high-water/{path_hash}/history/");
        let (first_objects, second_objects) = tokio::try_join!(
            first_backend.control_list_objects(&history_prefix, token),
            second_backend.control_list_objects(&history_prefix, token)
        )?;
        let first_objects = first_objects.into_iter().collect::<BTreeSet<_>>();
        let second_objects = second_objects.into_iter().collect::<BTreeSet<_>>();
        let objects = first_objects
            .union(&second_objects)
            .cloned()
            .collect::<BTreeSet<_>>();
        ensure!(!objects.is_empty(), "high-water history is empty");

        let expected_replicas = self
            .ring
            .replicas_for(active_logical_blob)?
            .into_iter()
            .map(|node| node.id.as_str())
            .collect::<HashSet<_>>();
        let compacted_through = checkpoint.map_or(0, |value| {
            value.signed.payload.compacted_through_logical_version
        });
        let mut by_version = BTreeMap::new();
        let mut covered_deletes = Vec::new();
        for object_key in objects {
            let (first_value, second_value) = tokio::try_join!(
                first_backend.control_get_object(&object_key, token),
                second_backend.control_get_object(&object_key, token)
            )?;
            let bytes = match (&first_value, &second_value) {
                (Some(first), Some(second)) => {
                    ensure!(
                        first.bytes == second.bytes,
                        "high-water history bytes differ between replicas"
                    );
                    first.bytes.clone()
                }
                (Some(first), None) => first.bytes.clone(),
                (None, Some(second)) => second.bytes.clone(),
                (None, None) => bail!("listed high-water history object is missing"),
            };
            let signed = SignedDocument::<CommitManifest>::from_bytes(&bytes)
                .context("high-water history is not a signed commit manifest")?;
            ensure!(
                signed.canonical_bytes()? == bytes,
                "high-water history is not canonically encoded"
            );
            signed
                .verify(
                    SignatureDomain::CommitManifest,
                    &signed.payload.signing_key_id,
                    self.signer.as_ref(),
                )
                .context("high-water history signature validation failed")?;
            let logical_blob = validate_history_manifest(
                &signed.payload,
                active_logical_blob,
                self.ring.ring_version,
                path_hash,
                &expected_replicas,
            )?;
            ensure!(
                object_key == high_water_history_key(path_hash, &signed.payload),
                "high-water history object name does not match its signed version"
            );
            if signed.payload.logical_version > compacted_through {
                ensure!(
                    first_value.is_some() && second_value.is_some(),
                    "retained high-water history object sets differ between replicas"
                );
            } else {
                covered_deletes.push(ControlDelete {
                    object_key: object_key.clone(),
                    first_etag: first_value.as_ref().and_then(|value| value.etag.clone()),
                    second_etag: second_value.as_ref().and_then(|value| value.etag.clone()),
                });
            }
            ensure!(
                by_version
                    .insert(
                        signed.payload.logical_version,
                        ValidatedHistoryEntry {
                            signed,
                            logical_blob,
                            bytes,
                            object_key,
                            first_etag: first_value.and_then(|value| value.etag),
                            second_etag: second_value.and_then(|value| value.etag),
                        }
                    )
                    .is_none(),
                "multiple signed histories exist for one logical version"
            );
        }

        if let Some(checkpoint) = checkpoint {
            let covered = by_version
                .values()
                .filter(|entry| entry.signed.payload.logical_version <= compacted_through)
                .collect::<Vec<_>>();
            if !covered.is_empty() {
                ensure!(
                    covered.last().is_some_and(|entry| {
                        entry.signed.payload.logical_version == compacted_through
                    }),
                    "replayed history below the compaction floor is not a pending deletion suffix"
                );
                for pair in covered.windows(2) {
                    let previous = &pair[0].signed.payload;
                    let current = &pair[1].signed.payload;
                    ensure!(
                        current.logical_version == previous.logical_version.saturating_add(1)
                            && current.previous_logical_etag.as_deref()
                                == Some(previous.logical_etag.as_str())
                            && current.committed_at_unix_ms >= previous.committed_at_unix_ms
                            && valid_history_transition(previous.state, current.state),
                        "covered history conflicts with the checkpoint-anchored lineage"
                    );
                }
                let terminal = covered.last().context("covered history is empty")?;
                ensure!(
                    terminal.signed.payload.state
                        == checkpoint.signed.payload.compacted_through_state
                        && terminal.signed.payload.logical_etag
                            == checkpoint.signed.payload.compacted_through_logical_etag
                        && terminal.signed.payload.committed_at_unix_ms
                            == checkpoint
                                .signed
                                .payload
                                .compacted_through_committed_at_unix_ms
                        && sha256_bytes(&terminal.bytes)
                            == checkpoint.signed.payload.covered_terminal_manifest_sha256,
                    "covered terminal history conflicts with the compaction checkpoint"
                );
            }
        }
        let entries = by_version
            .into_values()
            .filter(|entry| entry.signed.payload.logical_version > compacted_through)
            .collect::<Vec<_>>();
        let expected_first = compacted_through.saturating_add(1);
        for (index, entry) in entries.iter().enumerate() {
            ensure!(
                entry.signed.payload.logical_version == u64::try_from(index)? + expected_first,
                "high-water history versions are not contiguous"
            );
            if index == 0 {
                if let Some(checkpoint) = checkpoint {
                    ensure!(
                        entry.signed.payload.previous_logical_etag.as_deref()
                            == Some(
                                checkpoint
                                    .signed
                                    .payload
                                    .compacted_through_logical_etag
                                    .as_str()
                            )
                            && entry.signed.payload.committed_at_unix_ms
                                >= checkpoint
                                    .signed
                                    .payload
                                    .compacted_through_committed_at_unix_ms
                            && valid_history_transition(
                                checkpoint.signed.payload.compacted_through_state,
                                entry.signed.payload.state
                            ),
                        "first retained history successor is not anchored by the compaction checkpoint"
                    );
                } else {
                    ensure!(
                        entry.signed.payload.logical_version == 1
                            && entry.signed.payload.state == ManifestState::Committed
                            && entry.signed.payload.previous_logical_etag.is_none(),
                        "logical version 1 must be an initial committed generation"
                    );
                }
                continue;
            }
            let previous = &entries[index - 1].signed.payload;
            let current = &entry.signed.payload;
            ensure!(
                current.previous_logical_etag.as_deref() == Some(previous.logical_etag.as_str()),
                "high-water history previousLogicalEtag lineage is invalid"
            );
            ensure!(
                current.committed_at_unix_ms >= previous.committed_at_unix_ms,
                "high-water history timestamps are not monotonic"
            );
            ensure!(
                valid_history_transition(previous.state, current.state),
                "high-water history contains an invalid state transition"
            );
        }
        let current = entries.last().context("high-water history is empty")?;
        ensure!(
            current.signed.payload == *active
                && current.bytes == active_bytes
                && current.signed.payload.logical_version == active.logical_version
                && head_object == head_object_key(&current.logical_blob),
            "current head does not correspond to the authoritative history high-water entry"
        );
        Ok(ValidatedHistory {
            entries,
            covered_deletes,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn validate_garbage_collection_markers(
        &self,
        path_hash: &str,
        head_object: &str,
        active: &CommitManifest,
        active_logical_blob: &LogicalBlobId,
        history: &ValidatedHistory,
        checkpoint: Option<&LoadedCompactionCheckpoint>,
        first_backend: &dyn ReplicaBackend,
        second_backend: &dyn ReplicaBackend,
        token: &ControlToken,
    ) -> Result<ValidatedMarkers> {
        let prefix = format!("{GARBAGE_COLLECTION_PREFIX}{path_hash}/");
        let (first_objects, second_objects) = tokio::try_join!(
            first_backend.control_list_objects(&prefix, token),
            second_backend.control_list_objects(&prefix, token)
        )?;
        let objects = first_objects
            .iter()
            .chain(&second_objects)
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut markers = BTreeMap::new();
        let mut repairs = Vec::new();
        let mut marker_objects = Vec::new();
        for object_key in objects {
            let (first_value, second_value) = tokio::try_join!(
                first_backend.control_get_object(&object_key, token),
                second_backend.control_get_object(&object_key, token)
            )?;
            let (bytes, missing_backend) = match (&first_value, &second_value) {
                (Some(first), Some(second)) => {
                    ensure!(
                        first.bytes == second.bytes,
                        "garbage-collection markers conflict between replicas"
                    );
                    (first.bytes.clone(), None)
                }
                (Some(first), None) => (first.bytes.clone(), Some(second_backend.id().to_owned())),
                (None, Some(second)) => (second.bytes.clone(), Some(first_backend.id().to_owned())),
                (None, None) => bail!("listed garbage-collection marker is missing"),
            };
            let signed = SignedDocument::<GarbageCollectionMarker>::from_bytes(&bytes)
                .context("garbage-collection marker is not valid JSON")?;
            ensure!(
                signed.canonical_bytes()? == bytes,
                "garbage-collection marker is not canonically encoded"
            );
            signed
                .verify(
                    SignatureDomain::GarbageCollectionMarker,
                    &signed.payload.signing_key_id,
                    self.signer.as_ref(),
                )
                .context("garbage-collection marker signature validation failed")?;
            let marker = &signed.payload;
            let marker_logical_blob =
                parse_signed_logical_blob(&marker.blob, "garbage-collection marker")?;
            ensure!(
                marker.api_version == "overmesh.io/garbage-collection-marker/v1"
                    && marker_logical_blob == *active_logical_blob
                    && marker.head_object == head_object
                    && marker.ring_version == self.ring.ring_version
                    && marker.history_head_logical_version
                        > marker.collected_through_logical_version
                    && marker.history_head_logical_version <= active.logical_version
                    && marker.collected_through_logical_version < active.logical_version,
                "garbage-collection marker is not bound to the active history"
            );
            ensure!(
                object_key
                    == garbage_collection_marker_key(
                        &marker_logical_blob.path_hash(),
                        marker.collected_through_logical_version
                    ),
                "garbage-collection marker object name does not match its watermark"
            );
            let marker_through = marker.collected_through_logical_version;
            ensure!(
                markers
                    .insert(marker_through, (object_key.clone(), signed, bytes.clone()))
                    .is_none(),
                "duplicate garbage-collection marker watermark"
            );
            marker_objects.push((
                marker_through,
                object_key.clone(),
                ControlDelete {
                    object_key: object_key.clone(),
                    first_etag: first_value.as_ref().and_then(|value| value.etag.clone()),
                    second_etag: second_value.as_ref().and_then(|value| value.etag.clone()),
                },
            ));
            if let Some(backend_id) = missing_backend {
                repairs.push(MarkerRepair {
                    backend_id,
                    object_key: object_key.clone(),
                    bytes,
                });
            }
        }

        let mut latest_evidence = checkpoint.map(checkpoint_garbage_collection_evidence);
        let mut previous_through = latest_evidence
            .as_ref()
            .map_or(0, |value| value.collected_through_logical_version);
        let mut previous_sha256 = latest_evidence.as_ref().map(|value| value.sha256.clone());
        let mut previous_collected_at = latest_evidence
            .as_ref()
            .map_or(0, |value| value.collected_at_unix_ms);
        for (through, (object_key, signed, bytes)) in &markers {
            let marker = &signed.payload;
            if *through < previous_through {
                continue;
            }
            if *through == previous_through {
                let evidence = latest_evidence
                    .as_ref()
                    .context("zero garbage-collection marker watermark is invalid")?;
                ensure!(
                    object_key == &evidence.object_key
                        && sha256_bytes(bytes) == evidence.sha256
                        && marker.history_head_logical_version
                            == evidence.history_head_logical_version
                        && marker.collected_committed_versions
                            == evidence.collected_committed_versions
                        && marker.physical_collection_delay_ms
                            == evidence.physical_collection_delay_ms
                        && marker.collected_at_unix_ms == evidence.collected_at_unix_ms,
                    "garbage-collection marker conflicts with compaction checkpoint evidence"
                );
                continue;
            }
            ensure!(
                *through > previous_through,
                "garbage-collection marker watermarks are not increasing"
            );
            ensure!(
                marker.previous_marker_sha256 == previous_sha256,
                "garbage-collection marker lineage is invalid"
            );
            ensure!(
                marker.collected_at_unix_ms >= previous_collected_at,
                "garbage-collection marker timestamps are not monotonic"
            );
            ensure!(
                marker.collected_at_unix_ms <= now_unix_ms(),
                "garbage-collection marker timestamp is in the future"
            );
            let expected_collected = history
                .entries
                .iter()
                .filter(|entry| {
                    entry.signed.payload.logical_version > previous_through
                        && entry.signed.payload.logical_version <= *through
                        && entry.signed.payload.state == ManifestState::Committed
                })
                .map(|entry| entry.signed.payload.logical_version)
                .collect::<Vec<_>>();
            ensure!(
                marker.collected_committed_versions == expected_collected,
                "garbage-collection marker committed-version set is invalid"
            );
            for version in (previous_through + 1)..=*through {
                let successor = history_entry_by_version(history, version.saturating_add(1))
                    .context("garbage-collection marker exceeds available successor history")?;
                let eligible_at = successor
                    .signed
                    .payload
                    .committed_at_unix_ms
                    .checked_add(marker.physical_collection_delay_ms)
                    .context("garbage-collection marker retention deadline overflow")?;
                ensure!(
                    marker.collected_at_unix_ms >= eligible_at,
                    "garbage-collection marker predates its signed retention deadline"
                );
            }
            previous_through = *through;
            previous_sha256 = Some(sha256_bytes(bytes));
            previous_collected_at = marker.collected_at_unix_ms;
            latest_evidence = Some(garbage_collection_evidence(
                object_key.clone(),
                bytes,
                marker,
            ));
        }
        Ok(ValidatedMarkers {
            latest_through: (previous_through != 0).then_some(previous_through),
            latest_evidence,
            repairs,
            objects: marker_objects,
        })
    }
}

fn validate_history_manifest(
    manifest: &CommitManifest,
    expected_logical_blob: &LogicalBlobId,
    expected_ring_version: u64,
    path_hash: &str,
    expected_replicas: &HashSet<&str>,
) -> Result<LogicalBlobId> {
    let logical_blob = parse_signed_logical_blob(&manifest.blob, "high-water history")?;
    ensure!(
        logical_blob == *expected_logical_blob
            && manifest.ring_version == expected_ring_version
            && !manifest.write_id.is_empty()
            && manifest.logical_version > 0
            && manifest.committed_at_unix_ms > 0
            && logical_blob.path_hash() == path_hash,
        "high-water history is not bound to the active blob and Ring"
    );
    let replicas = manifest
        .prepared_replicas
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    ensure!(
        manifest.prepared_replicas.len() == expected_replicas.len()
            && replicas == *expected_replicas,
        "high-water history prepared replicas do not match active Ring placement"
    );
    ensure!(
        manifest.logical_etag
            == logical_etag(
                logical_blob.canonical(),
                manifest.logical_version,
                &manifest.write_id,
                &manifest.content_sha256
            ),
        "high-water history logical ETag is invalid"
    );
    match manifest.state {
        ManifestState::Committed => {
            ensure!(
                manifest.deleted_at_unix_ms.is_none()
                    && valid_sha256(&manifest.content_sha256)
                    && valid_sha256(&manifest.block_manifest_sha256)
                    && manifest.content_container == logical_blob.container()
                    && valid_content_object(path_hash, &manifest.content_object),
                "committed history content namespace or timestamp metadata is invalid"
            );
            let expected_prefix = expected_version_prefix(path_hash, manifest)?;
            ensure!(
                manifest.version_object_prefix.as_deref() == Some(expected_prefix.as_str())
                    && manifest.block_manifest_object
                        == format!("{expected_prefix}/block-manifest.json"),
                "committed history version metadata namespace is invalid"
            );
        }
        ManifestState::Tombstoned => {
            let expected_prefix = format!(
                "objects/{path_hash}/tombstones/{}",
                stable_component(&manifest.write_id)
            );
            ensure!(
                manifest.logical_version > 1
                    && manifest.previous_logical_etag.is_some()
                    && manifest.deleted_at_unix_ms == Some(manifest.committed_at_unix_ms)
                    && manifest.content_length == 0
                    && manifest.content_sha256 == sha256_bytes(b"overmesh:tombstone:v1")
                    && manifest.content_container.is_empty()
                    && manifest.content_object.is_empty()
                    && manifest.block_manifest_object.is_empty()
                    && manifest.block_manifest_sha256.is_empty()
                    && manifest.version_object_prefix.as_deref() == Some(expected_prefix.as_str()),
                "tombstone history structure or namespace is invalid"
            );
        }
        ManifestState::Prepared => bail!("high-water history contains a prepared manifest"),
    }
    Ok(logical_blob)
}

fn valid_content_object(path_hash: &str, object_key: &str) -> bool {
    object_key
        .strip_prefix(&format!(".overmesh/objects/{path_hash}/"))
        .is_some_and(|content_id| {
            content_id.len() == 32
                && content_id
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
}

pub(super) fn expected_version_prefix(
    path_hash: &str,
    manifest: &CommitManifest,
) -> Result<String> {
    ensure!(
        manifest.state == ManifestState::Committed,
        "only committed manifests have a collectible version namespace"
    );
    let digest = manifest
        .content_sha256
        .strip_prefix("sha256:")
        .filter(|value| {
            value.len() == 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        .context("committed content SHA-256 is invalid")?;
    Ok(format!(
        "objects/{path_hash}/versions/{}/{}",
        stable_component(&manifest.write_id),
        digest
    ))
}

fn valid_sha256(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

pub(super) fn high_water_history_key(path_hash: &str, manifest: &CommitManifest) -> String {
    format!(
        "high-water/{path_hash}/history/{:020}-{}.json",
        manifest.logical_version,
        stable_component(&manifest.write_id)
    )
}

pub(super) fn history_compaction_checkpoint_key(path_hash: &str) -> String {
    format!("high-water/{path_hash}/compaction/current.json")
}

pub(super) fn garbage_collection_marker_key(path_hash: &str, through: u64) -> String {
    format!("{GARBAGE_COLLECTION_PREFIX}{path_hash}/{through:020}.json")
}

fn valid_history_transition(previous: ManifestState, current: ManifestState) -> bool {
    matches!(
        (previous, current),
        (ManifestState::Committed, ManifestState::Committed)
            | (ManifestState::Committed, ManifestState::Tombstoned)
            | (ManifestState::Tombstoned, ManifestState::Committed)
    )
}

fn history_entry_by_version(
    history: &ValidatedHistory,
    version: u64,
) -> Option<&ValidatedHistoryEntry> {
    history
        .entries
        .iter()
        .find(|entry| entry.signed.payload.logical_version == version)
}

pub(super) fn garbage_collection_evidence(
    object_key: String,
    bytes: &[u8],
    marker: &GarbageCollectionMarker,
) -> GarbageCollectionEvidence {
    GarbageCollectionEvidence {
        object_key,
        sha256: sha256_bytes(bytes),
        bytes: Some(bytes.to_vec()),
        history_head_logical_version: marker.history_head_logical_version,
        collected_through_logical_version: marker.collected_through_logical_version,
        collected_committed_versions: marker.collected_committed_versions.clone(),
        physical_collection_delay_ms: marker.physical_collection_delay_ms,
        collected_at_unix_ms: marker.collected_at_unix_ms,
    }
}

fn checkpoint_garbage_collection_evidence(
    checkpoint: &LoadedCompactionCheckpoint,
) -> GarbageCollectionEvidence {
    let payload = &checkpoint.signed.payload;
    GarbageCollectionEvidence {
        object_key: payload.garbage_collection_marker_object.clone(),
        sha256: payload.garbage_collection_marker_sha256.clone(),
        bytes: None,
        history_head_logical_version: payload.garbage_collection_history_head_logical_version,
        collected_through_logical_version: payload.garbage_collection_through_logical_version,
        collected_committed_versions: payload.garbage_collected_committed_versions.clone(),
        physical_collection_delay_ms: payload.garbage_collection_delay_ms,
        collected_at_unix_ms: payload.garbage_collected_at_unix_ms,
    }
}

fn checkpoint_descends(
    newer: &ReplicaCompactionCheckpoint,
    older: &ReplicaCompactionCheckpoint,
) -> bool {
    newer.signed.payload.checkpoint_version
        == older.signed.payload.checkpoint_version.saturating_add(1)
        && newer.signed.payload.compacted_through_logical_version
            > older.signed.payload.compacted_through_logical_version
        && newer.signed.payload.previous_checkpoint_version
            == Some(older.signed.payload.checkpoint_version)
        && newer.signed.payload.previous_checkpoint_sha256 == Some(sha256_bytes(&older.bytes))
}

pub(super) fn validate_compaction_checkpoint(
    checkpoint: &HistoryCompactionCheckpoint,
    active_logical_blob: &LogicalBlobId,
    path_hash: &str,
    head_object: &str,
    active: &CommitManifest,
    ring_version: u64,
) -> Result<()> {
    let logical_blob =
        parse_signed_logical_blob(&checkpoint.blob, "history compaction checkpoint")?;
    ensure!(
        checkpoint.api_version == HISTORY_COMPACTION_API_VERSION
            && logical_blob == *active_logical_blob
            && checkpoint.path_hash == path_hash
            && checkpoint.path_hash == active_logical_blob.path_hash()
            && checkpoint.head_object == head_object
            && checkpoint.head_object == head_object_key(active_logical_blob)
            && checkpoint.ring_version == ring_version
            && checkpoint.checkpoint_version > 0
            && checkpoint.compacted_through_logical_version > 0
            && checkpoint.compacted_through_logical_version < active.logical_version
            && checkpoint.compacted_through_state != ManifestState::Prepared
            && !checkpoint.compacted_through_logical_etag.is_empty()
            && checkpoint.compacted_through_committed_at_unix_ms > 0
            && valid_sha256(&checkpoint.covered_terminal_manifest_sha256)
            && valid_sha256(&checkpoint.garbage_collection_marker_sha256)
            && checkpoint.garbage_collection_through_logical_version
                >= checkpoint.compacted_through_logical_version
            && checkpoint.garbage_collection_through_logical_version < active.logical_version
            && checkpoint.garbage_collection_history_head_logical_version
                > checkpoint.garbage_collection_through_logical_version
            && checkpoint.garbage_collection_history_head_logical_version <= active.logical_version
            && checkpoint.garbage_collection_marker_object
                == garbage_collection_marker_key(
                    path_hash,
                    checkpoint.garbage_collection_through_logical_version
                )
            && checkpoint.compacted_at_unix_ms >= checkpoint.garbage_collected_at_unix_ms,
        "history compaction checkpoint is not bound to the active blob, Ring, and GC evidence"
    );
    let previous_valid = match (
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
    ensure!(
        previous_valid,
        "history compaction checkpoint predecessor binding is invalid"
    );
    ensure!(
        checkpoint
            .garbage_collected_committed_versions
            .windows(2)
            .all(|pair| pair[0] < pair[1])
            && checkpoint
                .garbage_collected_committed_versions
                .iter()
                .all(|version| *version <= checkpoint.garbage_collection_through_logical_version),
        "history compaction checkpoint GC plan evidence is invalid"
    );
    Ok(())
}
