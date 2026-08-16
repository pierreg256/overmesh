use super::*;

impl CommitCoordinator {
    pub(crate) async fn put_blob_locked(
        &self,
        logical_blob: &LogicalBlobId,
        principal: &AuthenticatedPrincipal,
        write_id: &str,
        content: &SpoolContent,
        logical_condition: LogicalCondition,
        control_token: &ControlToken,
    ) -> Result<CommitResult, CommitError> {
        let path_hash = logical_blob.path_hash();
        let head_key = format!("heads/{path_hash}.json");
        let (primary_head, secondary_head) = tokio::try_join!(
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
        )?;
        if let Some(result) = self
            .recover_partial_publication(
                primary_head.as_ref(),
                secondary_head.as_ref(),
                &head_key,
                logical_blob,
                principal,
                write_id,
                content,
                control_token,
            )
            .await?
        {
            return Ok(result);
        }
        let current = strict_current_head(primary_head.as_ref(), secondary_head.as_ref())?;
        if let Some(head) = current
            && head.signed.payload.write_id == write_id
        {
            if head.signed.payload.content_sha256 == content.content_sha256
                && head.signed.payload.state == ManifestState::Committed
            {
                self.authorize_replay(principal, &head.signed.payload)
                    .await?;
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
                publish_catalog_current(
                    self.primary.as_ref(),
                    self.secondary.as_ref(),
                    logical_blob,
                    &head.signed,
                    &head.bytes,
                    control_token,
                    self.signer.as_ref(),
                )
                .await?;
                return Ok(CommitResult {
                    logical_version: head.signed.payload.logical_version,
                    logical_etag: head.signed.payload.logical_etag.clone(),
                    write_id: write_id.to_owned(),
                    idempotent_replay: true,
                });
            }
            return Err(CommitError::IdempotencyConflict);
        }
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
        match (&logical_condition, current) {
            (LogicalCondition::None, _) | (LogicalCondition::IfAbsent, None) => {}
            (LogicalCondition::IfAbsent, Some(head))
                if head.signed.payload.state == ManifestState::Tombstoned => {}
            (LogicalCondition::IfAbsent, Some(_)) => return Err(CommitError::ConditionFailed),
            (LogicalCondition::IfMatch(expected), Some(head))
                if head.signed.payload.state == ManifestState::Committed
                    && &head.signed.payload.logical_etag == expected => {}
            (LogicalCondition::IfMatch(_), _) => return Err(CommitError::ConditionFailed),
        }

        let logical_version = current
            .map(|head| head.signed.payload.logical_version + 1)
            .unwrap_or(1);
        let previous_logical_etag = current.map(|head| head.signed.payload.logical_etag.clone());
        let logical_etag = logical_etag(
            logical_blob.canonical(),
            logical_version,
            write_id,
            &content.content_sha256,
        );
        let version_prefix = format!(
            "objects/{path_hash}/versions/{}/{}",
            stable_component(write_id),
            content
                .content_sha256
                .strip_prefix("sha256:")
                .unwrap_or(&content.content_sha256)
        );
        let block_manifest_key = format!("{version_prefix}/block-manifest.json");
        let prepared_manifest_key = format!("{version_prefix}/prepared.json");
        let committed_manifest_key = format!("{version_prefix}/committed.json");

        let (signed_block, block_bytes) = Self::load_or_create_block_manifest(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            &block_manifest_key,
            logical_blob,
            write_id,
            logical_version,
            content,
            control_token,
            self.signer.as_ref(),
            self.ring_version,
        )
        .await?;
        let content_key = signed_block.payload.content_object.clone();
        tokio::try_join!(
            put_file_idempotent(
                self.primary.as_ref(),
                logical_blob.container(),
                &content_key,
                content,
                &principal.access_token
            ),
            put_file_idempotent(
                self.secondary.as_ref(),
                logical_blob.container(),
                &content_key,
                content,
                &principal.access_token
            )
        )?;

        let block_manifest_sha256 = sha256_bytes(&block_bytes);

        let prepared_payload = CommitManifest {
            blob: logical_blob.canonical().to_owned(),
            caller: principal.identity(),
            write_id: write_id.to_owned(),
            logical_version,
            logical_etag: logical_etag.clone(),
            previous_logical_etag: previous_logical_etag.clone(),
            ring_version: self.ring_version,
            content_length: content.length,
            content_sha256: content.content_sha256.clone(),
            content_container: logical_blob.container().to_owned(),
            content_object: content_key.clone(),
            block_manifest_object: block_manifest_key.clone(),
            block_manifest_sha256: block_manifest_sha256.clone(),
            version_object_prefix: Some(version_prefix.clone()),
            committed_at_unix_ms: now_unix_ms(),
            deleted_at_unix_ms: None,
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

        let committed_payload = CommitManifest {
            state: ManifestState::Committed,
            prepared_replicas: vec![self.primary.id().to_owned(), self.secondary.id().to_owned()],
            ..signed_prepared.payload
        };
        let signed_committed = SignedDocument::create(
            committed_payload,
            SignatureDomain::CommitManifest,
            self.signer.as_ref(),
        )
        .await?;
        let committed_bytes = signed_committed.canonical_bytes()?;
        tokio::try_join!(
            put_bytes_idempotent(
                self.primary.as_ref(),
                &committed_manifest_key,
                committed_bytes.clone(),
                control_token
            ),
            put_bytes_idempotent(
                self.secondary.as_ref(),
                &committed_manifest_key,
                committed_bytes.clone(),
                control_token
            )
        )?;

        let primary_condition = head_condition(primary_head.as_ref());
        let secondary_condition = head_condition(secondary_head.as_ref());
        let (primary_publish, secondary_publish) = tokio::join!(
            self.primary.control_put_bytes(
                &head_key,
                committed_bytes.clone(),
                "application/json",
                primary_condition,
                control_token
            ),
            self.secondary.control_put_bytes(
                &head_key,
                committed_bytes.clone(),
                "application/json",
                secondary_condition,
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
                warn!(error = %error, "only one replica published the committed head");
                return Err(CommitError::Ambiguous);
            }
            (Err(first), Err(second)) => {
                warn!(primary_error = %first, secondary_error = %second, "both head publications failed");
                return Err(CommitError::Backend(first));
            }
        }

        verify_identical_objects(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            &head_key,
            &committed_bytes,
            control_token,
        )
        .await?;
        Self::publish_high_water(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            &path_hash,
            &signed_committed,
            &committed_bytes,
            control_token,
            self.signer.as_ref(),
        )
        .await?;
        publish_catalog_current(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            logical_blob,
            &signed_committed,
            &committed_bytes,
            control_token,
            self.signer.as_ref(),
        )
        .await?;

        Ok(CommitResult {
            logical_version,
            logical_etag,
            write_id: write_id.to_owned(),
            idempotent_replay: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn load_or_create_block_manifest(
        primary: &dyn ReplicaBackend,
        secondary: &dyn ReplicaBackend,
        object_key: &str,
        logical_blob: &LogicalBlobId,
        write_id: &str,
        logical_version: u64,
        content: &SpoolContent,
        control_token: &ControlToken,
        signer: &dyn ManifestSigner,
        ring_version: u64,
    ) -> Result<(SignedDocument<BlockManifest>, Vec<u8>), CommitError> {
        let (primary_value, secondary_value) = tokio::try_join!(
            primary.control_get_object(object_key, control_token),
            secondary.control_get_object(object_key, control_token)
        )?;
        let existing = match (primary_value, secondary_value) {
            (None, None) => None,
            (Some(value), None) => {
                put_bytes_idempotent(secondary, object_key, value.bytes.clone(), control_token)
                    .await?;
                Some(value.bytes)
            }
            (None, Some(value)) => {
                put_bytes_idempotent(primary, object_key, value.bytes.clone(), control_token)
                    .await?;
                Some(value.bytes)
            }
            (Some(primary_value), Some(secondary_value)) => {
                if primary_value.bytes != secondary_value.bytes {
                    return Err(CommitError::VerificationFailed);
                }
                Some(primary_value.bytes)
            }
        };
        let (signed, bytes, blocks) = if let Some(bytes) = existing {
            let signed = SignedDocument::<BlockManifest>::from_bytes(&bytes)?;
            signed.verify(
                SignatureDomain::BlockManifest,
                &signed.payload.signing_key_id,
                signer,
            )?;
            validate_block_manifest_layout(object_key, &signed.payload)?;
            let blocks =
                Self::load_block_manifest_pages(primary, secondary, &signed.payload, control_token)
                    .await?;
            (signed, bytes, blocks)
        } else {
            let content_id = Uuid::new_v4().simple().to_string();
            let pages = Self::encode_block_manifest_pages(
                object_key,
                logical_blob,
                write_id,
                logical_version,
                &content.blocks,
            )?;
            for page in &pages {
                tokio::try_join!(
                    put_bytes_idempotent(
                        primary,
                        &page.reference.object,
                        page.bytes.clone(),
                        control_token
                    ),
                    put_bytes_idempotent(
                        secondary,
                        &page.reference.object,
                        page.bytes.clone(),
                        control_token
                    )
                )?;
            }
            let signed = SignedDocument::create(
                BlockManifest {
                    blob: logical_blob.canonical().to_owned(),
                    write_id: write_id.to_owned(),
                    logical_version,
                    content_container: logical_blob.container().to_owned(),
                    content_object: logical_blob.immutable_content_key(&content_id),
                    content_length: content.length,
                    block_count: u32::try_from(content.blocks.len())
                        .map_err(|_| CommitError::VerificationFailed)?,
                    page_size: BLOCK_MANIFEST_PAGE_SIZE,
                    pages: pages.iter().map(|page| page.reference.clone()).collect(),
                    content_sha256: content.content_sha256.clone(),
                    ring_version,
                    signing_key_id: signer.key_id().to_owned(),
                },
                SignatureDomain::BlockManifest,
                signer,
            )
            .await?;
            let bytes = signed.canonical_bytes()?;
            tokio::try_join!(
                put_bytes_idempotent(primary, object_key, bytes.clone(), control_token),
                put_bytes_idempotent(secondary, object_key, bytes.clone(), control_token)
            )?;
            (signed, bytes, content.blocks.clone())
        };
        let expected_prefix = format!(".overmesh/objects/{}/", logical_blob.path_hash());
        if signed.payload.blob != logical_blob.canonical()
            || signed.payload.write_id != write_id
            || signed.payload.logical_version != logical_version
            || signed.payload.content_container != logical_blob.container()
            || !signed.payload.content_object.starts_with(&expected_prefix)
            || signed.payload.content_length != content.length
            || signed.payload.block_count
                != u32::try_from(content.blocks.len())
                    .map_err(|_| CommitError::VerificationFailed)?
            || signed.payload.page_size != BLOCK_MANIFEST_PAGE_SIZE
            || blocks != content.blocks
            || signed.payload.content_sha256 != content.content_sha256
            || signed.payload.ring_version != ring_version
        {
            return Err(CommitError::IdempotencyConflict);
        }
        Ok((signed, bytes))
    }

    fn encode_block_manifest_pages(
        block_manifest_object: &str,
        logical_blob: &LogicalBlobId,
        write_id: &str,
        logical_version: u64,
        blocks: &[BlockDescriptor],
    ) -> Result<Vec<EncodedBlockPage>, CommitError> {
        let prefix = block_manifest_object
            .strip_suffix("/block-manifest.json")
            .ok_or(CommitError::VerificationFailed)?;
        blocks
            .chunks(
                usize::try_from(BLOCK_MANIFEST_PAGE_SIZE)
                    .map_err(|_| CommitError::VerificationFailed)?,
            )
            .enumerate()
            .map(|(page_index, page_blocks)| {
                let page_index =
                    u32::try_from(page_index).map_err(|_| CommitError::VerificationFailed)?;
                let first = page_blocks.first().ok_or(CommitError::VerificationFailed)?;
                let content_length = page_blocks.iter().try_fold(0_u64, |total, block| {
                    total
                        .checked_add(block.length)
                        .ok_or(CommitError::VerificationFailed)
                })?;
                let page = BlockManifestPage {
                    blob: logical_blob.canonical().to_owned(),
                    write_id: write_id.to_owned(),
                    logical_version,
                    page_index,
                    first_block_index: first.index,
                    blocks: page_blocks.to_vec(),
                };
                let bytes = serde_jcs::to_vec(&page)
                    .map_err(ManifestError::from)
                    .map_err(CommitError::from)?;
                Ok(EncodedBlockPage {
                    reference: BlockManifestPageReference {
                        index: page_index,
                        first_block_index: first.index,
                        block_count: u32::try_from(page_blocks.len())
                            .map_err(|_| CommitError::VerificationFailed)?,
                        first_offset: first.offset,
                        content_length,
                        object: format!("{prefix}/block-pages/{page_index:08}.json"),
                        sha256: sha256_bytes(&bytes),
                    },
                    bytes,
                })
            })
            .collect()
    }

    async fn load_block_manifest_pages(
        primary: &dyn ReplicaBackend,
        secondary: &dyn ReplicaBackend,
        manifest: &BlockManifest,
        control_token: &ControlToken,
    ) -> Result<Vec<BlockDescriptor>, CommitError> {
        let mut blocks = Vec::with_capacity(
            usize::try_from(manifest.block_count).map_err(|_| CommitError::VerificationFailed)?,
        );
        for reference in &manifest.pages {
            let (primary_value, secondary_value) = tokio::try_join!(
                primary.control_get_object(&reference.object, control_token),
                secondary.control_get_object(&reference.object, control_token)
            )?;
            let bytes = match (primary_value, secondary_value) {
                (Some(primary_value), Some(secondary_value)) => {
                    if primary_value.bytes != secondary_value.bytes {
                        return Err(CommitError::VerificationFailed);
                    }
                    primary_value.bytes
                }
                (Some(value), None) => {
                    put_bytes_idempotent(
                        secondary,
                        &reference.object,
                        value.bytes.clone(),
                        control_token,
                    )
                    .await?;
                    value.bytes
                }
                (None, Some(value)) => {
                    put_bytes_idempotent(
                        primary,
                        &reference.object,
                        value.bytes.clone(),
                        control_token,
                    )
                    .await?;
                    value.bytes
                }
                (None, None) => return Err(CommitError::VerificationFailed),
            };
            if sha256_bytes(&bytes) != reference.sha256 {
                return Err(CommitError::VerificationFailed);
            }
            let page: BlockManifestPage = serde_json::from_slice(&bytes)?;
            validate_block_manifest_page(manifest, reference, &page)?;
            blocks.extend(page.blocks);
        }
        Ok(blocks)
    }
}
