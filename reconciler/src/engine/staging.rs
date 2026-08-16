use super::*;
use overmesh_gateway::manifest::{StagedBlock, StagedBlockGcMarker};

pub(super) const STAGED_BLOCK_PREFIX: &str = "staged-blocks/";
pub(super) const STAGED_BLOCK_GC_PREFIX: &str = "staged-block-gc/";
const STAGED_BLOCK_API_VERSION: &str = "overmesh.io/staged-block/v1";
const STAGED_BLOCK_GC_API_VERSION: &str = "overmesh.io/staged-block-gc-marker/v1";
pub(super) const STAGED_METADATA_CURSOR_API_VERSION: &str =
    "reconciler.overmesh.io/staged-metadata-cursor/v1";
pub(super) const STAGED_MARKER_CURSOR_API_VERSION: &str =
    "reconciler.overmesh.io/staged-marker-cursor/v1";

impl ReconcilerEngine {
    pub(super) async fn reconcile_staged_blocks(&self, token: &ControlToken) -> Result<()> {
        let (keys, metadata_cursor) = self
            .discover_staged_page(
                STAGED_BLOCK_PREFIX,
                &self.staged_block_metadata_cursor_path,
                STAGED_METADATA_CURSOR_API_VERSION,
                token,
            )
            .await?;
        for key in keys {
            if let Err(error) = self.reconcile_staged_block(&key, token).await {
                warn!(object = key, error = %error, "staged block failed closed during reconciliation");
            }
        }
        self.persist_staged_cursor(&self.staged_block_metadata_cursor_path, &metadata_cursor)?;

        let (markers, marker_cursor) = self
            .discover_staged_page(
                STAGED_BLOCK_GC_PREFIX,
                &self.staged_block_marker_cursor_path,
                STAGED_MARKER_CURSOR_API_VERSION,
                token,
            )
            .await?;
        for marker in markers {
            if let Err(error) = self.reconcile_staged_gc_marker(&marker, token).await {
                warn!(object = marker, error = %error, "staged-block GC marker failed closed");
            }
        }
        self.persist_staged_cursor(&self.staged_block_marker_cursor_path, &marker_cursor)?;
        Ok(())
    }

    pub(super) async fn discover_staged_page(
        &self,
        prefix: &str,
        cursor_path: &Path,
        api_version: &str,
        token: &ControlToken,
    ) -> Result<(Vec<String>, StagedDiscoveryCursor)> {
        ensure!(!self.ring.nodes.is_empty(), "Ring contains no nodes");
        let cursor = self.load_staged_cursor(cursor_path, api_version)?;
        let node = self
            .ring
            .nodes
            .get(cursor.node_index)
            .context("staged discovery cursor node index is out of range")?;
        let page = self
            .backend(&node.id)?
            .control_list_objects_page(
                prefix,
                cursor.backend_cursor.as_deref(),
                self.staged_block_gc_max_records_per_cycle,
                token,
            )
            .await?;
        ensure!(
            page.next_cursor.is_none()
                || page.next_cursor.as_ref() != cursor.backend_cursor.as_ref(),
            "staged discovery cursor did not advance"
        );
        let next = match page.next_cursor {
            Some(backend_cursor) => StagedDiscoveryCursor {
                backend_cursor: Some(backend_cursor),
                ..cursor
            },
            None => StagedDiscoveryCursor {
                api_version: cursor.api_version,
                ring_version: cursor.ring_version,
                node_index: (cursor.node_index + 1) % self.ring.nodes.len(),
                backend_cursor: None,
            },
        };
        Ok((page.objects, next))
    }

    fn load_staged_cursor(&self, path: &Path, api_version: &str) -> Result<StagedDiscoveryCursor> {
        let cursor = if path.exists() {
            serde_json::from_slice::<StagedDiscoveryCursor>(
                &fs::read(path)
                    .with_context(|| format!("failed to read staged cursor {}", path.display()))?,
            )
            .with_context(|| format!("failed to parse staged cursor {}", path.display()))?
        } else {
            StagedDiscoveryCursor {
                api_version: api_version.to_owned(),
                ring_version: self.ring.ring_version,
                node_index: 0,
                backend_cursor: None,
            }
        };
        ensure!(
            cursor.api_version == api_version,
            "unsupported staged discovery cursor apiVersion"
        );
        ensure!(
            cursor.ring_version == self.ring.ring_version,
            "staged discovery cursor Ring version mismatch"
        );
        ensure!(
            cursor.node_index < self.ring.nodes.len(),
            "staged discovery cursor node index is out of range"
        );
        Ok(cursor)
    }

    pub(super) fn persist_staged_cursor(
        &self,
        path: &Path,
        cursor: &StagedDiscoveryCursor,
    ) -> Result<()> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create staged cursor directory {}",
                    parent.display()
                )
            })?;
        }
        let temporary = staged_cursor_temporary_path(path);
        fs::write(&temporary, serde_json::to_vec(cursor)?)
            .with_context(|| format!("failed to write staged cursor {}", temporary.display()))?;
        if let Err(error) = fs::rename(&temporary, path) {
            let _ = fs::remove_file(&temporary);
            return Err(error)
                .with_context(|| format!("failed to publish staged cursor {}", path.display()));
        }
        Ok(())
    }

    async fn reconcile_staged_block(&self, key: &str, token: &ControlToken) -> Result<()> {
        let mut discovered = Vec::new();
        for backend in self.backends.values() {
            if let Some(value) = backend.control_get_object(key, token).await? {
                discovered.push((backend.id().to_owned(), value));
            }
        }
        let first = discovered
            .first()
            .context("staged block metadata disappeared during discovery")?;
        let signed = SignedDocument::<StagedBlock>::from_bytes(&first.1.bytes)
            .context("staged block metadata is not valid JSON")?;
        signed
            .verify(
                SignatureDomain::StagedBlock,
                &signed.payload.signing_key_id,
                self.signer.as_ref(),
            )
            .context("staged block signature validation failed")?;
        ensure!(
            signed.canonical_bytes()? == first.1.bytes,
            "staged block metadata is not canonically encoded"
        );
        self.validate_staged_block(key, &signed.payload)?;
        let gc_marker_key = staged_gc_marker_key(key)?;
        if self
            .load_staged_gc_marker(&gc_marker_key, token)
            .await?
            .is_some()
        {
            self.reconcile_staged_gc_marker(&gc_marker_key, token)
                .await?;
            return Ok(());
        }
        let replicas = self.ring.replicas_for(&signed.payload.blob)?;
        let first_backend = self.backend(&replicas[0].id)?;
        let second_backend = self.backend(&replicas[1].id)?;
        let (first_metadata, second_metadata) = tokio::try_join!(
            first_backend.control_get_object(key, token),
            second_backend.control_get_object(key, token)
        )?;

        let authoritative = match (first_metadata.as_ref(), second_metadata.as_ref()) {
            (Some(first), Some(second)) => {
                if first.bytes != second.bytes || first.bytes != signed.canonical_bytes()? {
                    self.quarantine(
                        Some(signed.payload.blob.clone()),
                        &head_object_key(&signed.payload.blob),
                        "signed staged-block metadata diverges across active replicas".to_owned(),
                        token,
                    )
                    .await?;
                    return Ok(());
                }
                signed.canonical_bytes()?
            }
            (Some(value), None) if value.bytes == signed.canonical_bytes()? => {
                let source = self
                    .validated_staged_data(first_backend.as_ref(), &signed.payload, token)
                    .await?;
                put_immutable_data(
                    second_backend.as_ref(),
                    &signed.payload.content_container,
                    &signed.payload.content_object,
                    source,
                    token,
                )
                .await?;
                put_immutable(
                    second_backend.as_ref(),
                    key,
                    value.bytes.clone(),
                    "application/json",
                    token,
                )
                .await?;
                return Ok(());
            }
            (None, Some(value)) if value.bytes == signed.canonical_bytes()? => {
                let source = self
                    .validated_staged_data(second_backend.as_ref(), &signed.payload, token)
                    .await?;
                put_immutable_data(
                    first_backend.as_ref(),
                    &signed.payload.content_container,
                    &signed.payload.content_object,
                    source,
                    token,
                )
                .await?;
                put_immutable(
                    first_backend.as_ref(),
                    key,
                    value.bytes.clone(),
                    "application/json",
                    token,
                )
                .await?;
                return Ok(());
            }
            _ => bail!("staged block metadata is missing from both active replicas"),
        };
        let (first_value, second_value) = tokio::try_join!(
            first_backend.service_get_data_object(
                &signed.payload.content_container,
                &signed.payload.content_object,
                token
            ),
            second_backend.service_get_data_object(
                &signed.payload.content_container,
                &signed.payload.content_object,
                token
            )
        )?;
        let first_data = validate_optional_staged_data(first_value.as_ref(), &signed.payload);
        let second_data = validate_optional_staged_data(second_value.as_ref(), &signed.payload);
        let both_missing = matches!((&first_data, &second_data), (Ok(None), Ok(None)));
        match (&first_data, &second_data) {
            (Ok(Some(first_bytes)), Ok(Some(second_bytes))) if first_bytes == second_bytes => {}
            (Ok(Some(source)), Ok(None)) => {
                put_immutable_data(
                    second_backend.as_ref(),
                    &signed.payload.content_container,
                    &signed.payload.content_object,
                    source.clone(),
                    token,
                )
                .await?;
                return Ok(());
            }
            (Ok(None), Ok(Some(source))) => {
                put_immutable_data(
                    first_backend.as_ref(),
                    &signed.payload.content_container,
                    &signed.payload.content_object,
                    source.clone(),
                    token,
                )
                .await?;
                return Ok(());
            }
            (Ok(None), Ok(None)) if now_unix_ms() <= signed.payload.expires_at_unix_ms => {
                return Ok(());
            }
            (Ok(None), Ok(None)) => {}
            _ => {
                self.quarantine(
                    Some(signed.payload.blob.clone()),
                    &head_object_key(&signed.payload.blob),
                    "signed staged-block physical content is missing, tampered, or divergent"
                        .to_owned(),
                    token,
                )
                .await?;
                return Ok(());
            }
        }
        if now_unix_ms() <= signed.payload.expires_at_unix_ms {
            return Ok(());
        }

        let first_metadata = first_metadata.context("validated first metadata disappeared")?;
        let second_metadata = second_metadata.context("validated second metadata disappeared")?;
        ensure!(
            first_metadata.bytes == authoritative && second_metadata.bytes == authoritative,
            "staged block metadata changed before garbage collection"
        );
        let (first_current_data, second_current_data) = tokio::try_join!(
            first_backend.service_get_data_object(
                &signed.payload.content_container,
                &signed.payload.content_object,
                token
            ),
            second_backend.service_get_data_object(
                &signed.payload.content_container,
                &signed.payload.content_object,
                token
            )
        )?;
        match (first_current_data.as_ref(), second_current_data.as_ref()) {
            (Some(first), Some(second)) => ensure!(
                !both_missing
                    && first.bytes == second.bytes
                    && sha256_bytes(&first.bytes) == signed.payload.content_sha256,
                "staged data changed before garbage collection"
            ),
            (None, None) => ensure!(
                both_missing,
                "staged data disappeared before garbage collection"
            ),
            _ => bail!("staged data became partial before garbage collection"),
        }

        let validated_at_unix_ms = now_unix_ms();
        let marker = SignedDocument::create(
            StagedBlockGcMarker {
                api_version: STAGED_BLOCK_GC_API_VERSION.to_owned(),
                blob: signed.payload.blob.clone(),
                metadata_object: key.to_owned(),
                metadata_sha256: sha256_bytes(&authoritative),
                content_container: signed.payload.content_container.clone(),
                content_object: signed.payload.content_object.clone(),
                content_length: signed.payload.content_length,
                content_sha256: signed.payload.content_sha256.clone(),
                ring_version: self.ring.ring_version,
                replicas: vec![
                    first_backend.id().to_owned(),
                    second_backend.id().to_owned(),
                ],
                expired_at_unix_ms: signed.payload.expires_at_unix_ms,
                validated_at_unix_ms,
                signing_key_id: self.signer.key_id().to_owned(),
            },
            SignatureDomain::StagedBlockGcMarker,
            self.signer.as_ref(),
        )
        .await?;
        let marker_bytes = marker.canonical_bytes()?;
        tokio::try_join!(
            put_immutable(
                first_backend.as_ref(),
                &gc_marker_key,
                marker_bytes.clone(),
                "application/json",
                token
            ),
            put_immutable(
                second_backend.as_ref(),
                &gc_marker_key,
                marker_bytes.clone(),
                "application/json",
                token
            )
        )?;
        verify_identical_control_objects(
            first_backend.as_ref(),
            second_backend.as_ref(),
            &gc_marker_key,
            &marker_bytes,
            token,
        )
        .await?;
        self.reconcile_staged_gc_marker(&gc_marker_key, token).await
    }

    async fn load_staged_gc_marker(
        &self,
        marker_key: &str,
        token: &ControlToken,
    ) -> Result<Option<SignedDocument<StagedBlockGcMarker>>> {
        for backend in self.backends.values() {
            let Some(value) = backend.control_get_object(marker_key, token).await? else {
                continue;
            };
            let signed = SignedDocument::<StagedBlockGcMarker>::from_bytes(&value.bytes)
                .context("staged-block GC marker is not valid JSON")?;
            ensure!(
                signed.canonical_bytes()? == value.bytes,
                "staged-block GC marker is not canonical"
            );
            signed
                .verify(
                    SignatureDomain::StagedBlockGcMarker,
                    &signed.payload.signing_key_id,
                    self.signer.as_ref(),
                )
                .context("staged-block GC marker signature validation failed")?;
            self.validate_staged_gc_marker(marker_key, &signed.payload)?;
            return Ok(Some(signed));
        }
        Ok(None)
    }

    async fn reconcile_staged_gc_marker(
        &self,
        marker_key: &str,
        token: &ControlToken,
    ) -> Result<()> {
        let marker = self
            .load_staged_gc_marker(marker_key, token)
            .await?
            .context("staged-block GC marker disappeared during reconciliation")?;
        let marker_bytes = marker.canonical_bytes()?;
        let replicas = self.ring.replicas_for(&marker.payload.blob)?;
        let first = self.backend(&replicas[0].id)?;
        let second = self.backend(&replicas[1].id)?;
        let (first_marker, second_marker) = tokio::try_join!(
            first.control_get_object(marker_key, token),
            second.control_get_object(marker_key, token)
        )?;
        match (first_marker.as_ref(), second_marker.as_ref()) {
            (Some(first_value), Some(second_value)) => ensure!(
                first_value.bytes == marker_bytes && second_value.bytes == marker_bytes,
                "staged-block GC markers diverge across active replicas"
            ),
            (Some(value), None) if value.bytes == marker_bytes => {
                put_immutable(
                    second.as_ref(),
                    marker_key,
                    marker_bytes.clone(),
                    "application/json",
                    token,
                )
                .await?;
            }
            (None, Some(value)) if value.bytes == marker_bytes => {
                put_immutable(
                    first.as_ref(),
                    marker_key,
                    marker_bytes.clone(),
                    "application/json",
                    token,
                )
                .await?;
            }
            _ => bail!("staged-block GC marker is missing or conflicting"),
        }
        verify_identical_control_objects(
            first.as_ref(),
            second.as_ref(),
            marker_key,
            &marker_bytes,
            token,
        )
        .await?;

        let (first_metadata, second_metadata, first_data, second_data) = tokio::try_join!(
            first.control_get_object(&marker.payload.metadata_object, token),
            second.control_get_object(&marker.payload.metadata_object, token),
            first.service_get_data_object(
                &marker.payload.content_container,
                &marker.payload.content_object,
                token
            ),
            second.service_get_data_object(
                &marker.payload.content_container,
                &marker.payload.content_object,
                token
            )
        )?;
        for metadata in [&first_metadata, &second_metadata].into_iter().flatten() {
            ensure!(
                sha256_bytes(&metadata.bytes) == marker.payload.metadata_sha256,
                "staged metadata changed after GC marker publication"
            );
        }
        for data in [&first_data, &second_data].into_iter().flatten() {
            ensure!(
                u64::try_from(data.bytes.len())? == marker.payload.content_length
                    && sha256_bytes(&data.bytes) == marker.payload.content_sha256,
                "staged data changed after GC marker publication"
            );
        }

        if let Some(value) = first_data {
            first
                .service_delete_data_object(
                    &marker.payload.content_container,
                    &marker.payload.content_object,
                    value.etag.as_deref(),
                    token,
                )
                .await?;
        }
        if let Some(value) = second_data {
            second
                .service_delete_data_object(
                    &marker.payload.content_container,
                    &marker.payload.content_object,
                    value.etag.as_deref(),
                    token,
                )
                .await?;
        }
        if let Some(value) = first_metadata {
            first
                .control_delete_object(
                    &marker.payload.metadata_object,
                    value.etag.as_deref(),
                    token,
                )
                .await?;
        }
        if let Some(value) = second_metadata {
            second
                .control_delete_object(
                    &marker.payload.metadata_object,
                    value.etag.as_deref(),
                    token,
                )
                .await?;
        }
        let marker_sha256 = sha256_bytes(&marker_bytes);
        let audit_reason = format!(
            "completed staged-block GC marker {marker_key} with signed marker hash {marker_sha256}"
        );
        self.write_audit(
            Some(&marker.payload.blob),
            &head_object_key(&marker.payload.blob),
            ReconciliationClassification::Drifted,
            ReconciliationRecordAction::GarbageCollected,
            &audit_reason,
            Some(first.id()),
            Some(second.id()),
            token,
        )
        .await?;
        let (first_marker, second_marker) = tokio::try_join!(
            first.control_get_object(marker_key, token),
            second.control_get_object(marker_key, token)
        )?;
        let (Some(first_marker), Some(second_marker)) = (first_marker, second_marker) else {
            bail!("staged-block GC marker disappeared before evidence cleanup");
        };
        ensure!(
            first_marker.bytes == marker_bytes && second_marker.bytes == marker_bytes,
            "staged-block GC marker changed before evidence cleanup"
        );
        tokio::try_join!(
            first.control_delete_object(marker_key, first_marker.etag.as_deref(), token),
            second.control_delete_object(marker_key, second_marker.etag.as_deref(), token)
        )?;
        Ok(())
    }

    async fn validated_staged_data(
        &self,
        backend: &dyn ReplicaBackend,
        staged: &StagedBlock,
        token: &ControlToken,
    ) -> Result<Vec<u8>> {
        let value = backend
            .service_get_data_object(&staged.content_container, &staged.content_object, token)
            .await?
            .context("staged block physical content is missing")?;
        ensure!(
            u64::try_from(value.bytes.len())? == staged.content_length
                && sha256_bytes(&value.bytes) == staged.content_sha256,
            "staged block physical content hash validation failed"
        );
        Ok(value.bytes)
    }

    fn validate_staged_block(&self, key: &str, staged: &StagedBlock) -> Result<()> {
        let path_hash = logical_path_hash(&staged.blob);
        let expected_key = format!(
            "staged-blocks/{}/{}/{}.json",
            path_hash,
            stable_component(&staged.upload_id),
            stable_component(&staged.block_id)
        );
        let expected_data_prefix = format!(
            ".overmesh/staged/{}/{}/",
            path_hash,
            stable_component(&staged.upload_id)
        );
        ensure!(
            staged.api_version == STAGED_BLOCK_API_VERSION
                && staged.ring_version == self.ring.ring_version
                && staged.prepared_replicas.len() == 2
                && key == expected_key
                && staged.content_object.starts_with(&expected_data_prefix)
                && !staged.content_container.is_empty()
                && staged.content_length <= overmesh_gateway::block::MAX_BLOCK_SIZE
                && staged.created_at_unix_ms <= staged.expires_at_unix_ms
                && (staged.base_logical_version > 0) == staged.base_logical_etag.is_some(),
            "staged block signed structure is invalid"
        );
        let replicas = self.ring.replicas_for(&staged.blob)?;
        ensure!(
            staged.prepared_replicas == [replicas[0].id.as_str(), replicas[1].id.as_str()],
            "staged block replica binding does not match active Ring placement"
        );
        Ok(())
    }

    fn validate_staged_gc_marker(
        &self,
        marker_key: &str,
        marker: &StagedBlockGcMarker,
    ) -> Result<()> {
        let path_hash = logical_path_hash(&marker.blob);
        let valid_sha256 = |value: &str| {
            value.strip_prefix("sha256:").is_some_and(|digest| {
                digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        };
        ensure!(
            marker.api_version == STAGED_BLOCK_GC_API_VERSION
                && marker.ring_version == self.ring.ring_version
                && marker_key == staged_gc_marker_key(&marker.metadata_object)?
                && marker
                    .metadata_object
                    .starts_with(&format!("{STAGED_BLOCK_PREFIX}{path_hash}/"))
                && marker
                    .content_object
                    .starts_with(&format!(".overmesh/staged/{path_hash}/"))
                && valid_sha256(&marker.metadata_sha256)
                && valid_sha256(&marker.content_sha256)
                && marker.content_length <= overmesh_gateway::block::MAX_BLOCK_SIZE
                && marker.expired_at_unix_ms <= marker.validated_at_unix_ms
                && marker.replicas.len() == 2,
            "staged-block GC marker structure is invalid"
        );
        let replicas = self.ring.replicas_for(&marker.blob)?;
        ensure!(
            marker.replicas == [replicas[0].id.as_str(), replicas[1].id.as_str()],
            "staged-block GC marker replica binding is invalid"
        );
        Ok(())
    }
}

fn validate_optional_staged_data(
    value: Option<&ObjectValue>,
    staged: &StagedBlock,
) -> Result<Option<Vec<u8>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    ensure!(
        u64::try_from(value.bytes.len())? == staged.content_length
            && sha256_bytes(&value.bytes) == staged.content_sha256,
        "staged block physical content hash validation failed"
    );
    Ok(Some(value.bytes.clone()))
}

fn staged_gc_marker_key(metadata_key: &str) -> Result<String> {
    let suffix = metadata_key
        .strip_prefix(STAGED_BLOCK_PREFIX)
        .context("invalid staged-block metadata namespace")?;
    Ok(format!("{STAGED_BLOCK_GC_PREFIX}{suffix}"))
}

fn staged_cursor_temporary_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("staged-cursor");
    path.with_file_name(format!(".{file_name}.next"))
}
