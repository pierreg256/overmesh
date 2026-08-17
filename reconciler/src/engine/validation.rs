use super::*;

impl ReconcilerEngine {
    pub(super) async fn validate_replica(
        &self,
        backend: &dyn ReplicaBackend,
        head_object: &str,
        token: &ControlToken,
    ) -> ReplicaValidation {
        match self
            .validate_replica_inner(backend, head_object, token)
            .await
        {
            Ok(validation) => validation,
            Err(error)
                if error
                    .downcast_ref::<BackendError>()
                    .is_some_and(BackendError::is_unavailable) =>
            {
                ReplicaValidation::Unavailable {
                    reason: error.to_string(),
                }
            }
            Err(error) => ReplicaValidation::Tampered {
                blob: None,
                reason: error.to_string(),
            },
        }
    }

    async fn validate_replica_inner(
        &self,
        backend: &dyn ReplicaBackend,
        head_object: &str,
        token: &ControlToken,
    ) -> Result<ReplicaValidation> {
        let Some(head_object_value) = backend.control_get_object(head_object, token).await? else {
            return Ok(ReplicaValidation::MissingHead);
        };
        let signed = SignedDocument::<CommitManifest>::from_bytes(&head_object_value.bytes)
            .context("committed head is not valid JSON")?;
        signed
            .verify(
                SignatureDomain::CommitManifest,
                &signed.payload.signing_key_id,
                self.signer.as_ref(),
            )
            .context("committed head signature validation failed")?;
        ensure!(
            matches!(
                signed.payload.state,
                ManifestState::Committed | ManifestState::Tombstoned
            ),
            "head references a non-committed state"
        );
        ensure!(
            signed.payload.ring_version == self.ring.ring_version,
            "head Ring version does not match the active Ring"
        );
        let logical_blob = parse_signed_logical_blob(&signed.payload.blob, "committed head")?;
        ensure!(
            head_object == head_object_key(&logical_blob),
            "head object path does not match the signed blob path"
        );
        if signed.payload.state == ManifestState::Tombstoned {
            ensure!(
                signed.payload.deleted_at_unix_ms.is_some()
                    && signed.payload.previous_logical_etag.is_some()
                    && signed.payload.version_object_prefix.is_some()
                    && signed.payload.content_length == 0
                    && signed.payload.content_container.is_empty()
                    && signed.payload.content_object.is_empty()
                    && signed.payload.block_manifest_object.is_empty()
                    && signed.payload.block_manifest_sha256.is_empty()
                    && signed.payload.prepared_replicas.len() == 2,
                "tombstone structure is invalid"
            );
        }
        let high_water_checkpoint = validate_high_water(
            backend,
            head_object,
            &signed,
            &logical_blob,
            token,
            self.signer.as_ref(),
        )
        .await?;
        let head = ValidatedHead {
            logical_blob,
            signed,
            bytes: head_object_value.bytes,
            backend_etag: head_object_value.etag,
        };
        let committed_object = committed_manifest_object(&head.signed.payload)?;
        let Some(committed_value) = backend.control_get_object(&committed_object, token).await?
        else {
            return Ok(ReplicaValidation::Incomplete {
                head,
                reason: "the committed manifest sidecar is missing".to_owned(),
            });
        };
        ensure!(
            committed_value.bytes == head.bytes,
            "committed manifest sidecar differs from the published head"
        );
        if head.signed.payload.state == ManifestState::Tombstoned {
            let Some(high_water_checkpoint) = high_water_checkpoint else {
                return Ok(ReplicaValidation::Incomplete {
                    head,
                    reason: "the durable tombstone high-water checkpoint is missing".to_owned(),
                });
            };
            if high_water_checkpoint == head.bytes {
                return Ok(ReplicaValidation::Valid(ValidatedReplica {
                    head,
                    block_manifest: None,
                    block_pages: Vec::new(),
                    committed_manifest: committed_value.bytes,
                    high_water_checkpoint,
                }));
            }
            let previous = SignedDocument::<CommitManifest>::from_bytes(&high_water_checkpoint)
                .context("previous high-water checkpoint is not a signed commit manifest")?;
            let previous_logical_blob = parse_signed_logical_blob(
                &previous.payload.blob,
                "previous high-water checkpoint",
            )?;
            ensure!(
                previous.payload.state == ManifestState::Committed
                    && previous_logical_blob == head.logical_blob
                    && previous.payload.logical_version.saturating_add(1)
                        == head.signed.payload.logical_version
                    && head.signed.payload.previous_logical_etag.as_deref()
                        == Some(previous.payload.logical_etag.as_str()),
                "tombstone does not directly extend the durable high-water checkpoint"
            );
            let tombstone_checkpoint = head.bytes.clone();
            return Ok(ReplicaValidation::RecoverableTombstone {
                replica: ValidatedReplica {
                    head,
                    block_manifest: None,
                    block_pages: Vec::new(),
                    committed_manifest: committed_value.bytes,
                    high_water_checkpoint: tombstone_checkpoint,
                },
                reason: "the signed tombstone head was published before its high-water checkpoint"
                    .to_owned(),
            });
        }
        let Some(high_water_checkpoint) = high_water_checkpoint else {
            return Ok(ReplicaValidation::Incomplete {
                head,
                reason: "the durable high-water checkpoint is missing".to_owned(),
            });
        };

        async fn validate_high_water(
            backend: &dyn ReplicaBackend,
            head_object: &str,
            head: &SignedDocument<CommitManifest>,
            logical_blob: &LogicalBlobId,
            token: &ControlToken,
            signer: &dyn ManifestSigner,
        ) -> Result<Option<Vec<u8>>> {
            let path_hash = head_hash(head_object)?;
            let object_key = format!("high-water/{path_hash}/current.json");
            if let Some(value) = backend.control_get_object(&object_key, token).await? {
                let highest = SignedDocument::<CommitManifest>::from_bytes(&value.bytes)
                    .context("high-water checkpoint is not a signed commit manifest")?;
                highest
                    .verify(
                        SignatureDomain::CommitManifest,
                        &highest.payload.signing_key_id,
                        signer,
                    )
                    .context("high-water checkpoint signature validation failed")?;
                let highest_logical_blob =
                    parse_signed_logical_blob(&highest.payload.blob, "high-water checkpoint")?;
                ensure!(
                    highest_logical_blob == *logical_blob
                        && highest.payload.logical_version <= head.payload.logical_version,
                    "committed head was replayed below the durable high-water version"
                );
                if highest.payload.logical_version == head.payload.logical_version {
                    ensure!(
                        value.bytes == head.canonical_bytes()?,
                        "committed head does not match the durable high-water checkpoint"
                    );
                }
                return Ok(Some(value.bytes));
            }
            Ok(None)
        }

        let Some(block_value) = backend
            .control_get_object(&head.signed.payload.block_manifest_object, token)
            .await?
        else {
            return Ok(ReplicaValidation::Incomplete {
                head,
                reason: "the signed block manifest is missing".to_owned(),
            });
        };
        ensure!(
            sha256_bytes(&block_value.bytes) == head.signed.payload.block_manifest_sha256,
            "block manifest hash does not match the committed head"
        );
        let signed_block = SignedDocument::<BlockManifest>::from_bytes(&block_value.bytes)
            .context("block manifest is not valid JSON")?;
        signed_block
            .verify(
                SignatureDomain::BlockManifest,
                &signed_block.payload.signing_key_id,
                self.signer.as_ref(),
            )
            .context("block manifest signature validation failed")?;
        validate_block_manifest_link(&head.signed.payload, &signed_block.payload)
            .context("block manifest structure validation failed")?;
        let mut block_pages = Vec::with_capacity(signed_block.payload.pages.len());
        let mut blocks = Vec::with_capacity(usize::try_from(signed_block.payload.block_count)?);
        for reference in &signed_block.payload.pages {
            let Some(page_value) = backend.control_get_object(&reference.object, token).await?
            else {
                return Ok(ReplicaValidation::Incomplete {
                    head,
                    reason: format!("block manifest page {} is missing", reference.index),
                });
            };
            ensure!(
                sha256_bytes(&page_value.bytes) == reference.sha256,
                "block manifest page hash validation failed"
            );
            let page: BlockManifestPage = serde_json::from_slice(&page_value.bytes)
                .context("block manifest page is not valid JSON")?;
            validate_block_manifest_page(&signed_block.payload, reference, &page)
                .context("block manifest page structure validation failed")?;
            blocks.extend(page.blocks);
            block_pages.push((reference.object.clone(), page_value.bytes));
        }

        validate_block_layout(&head.signed.payload, &blocks)?;
        let block_lengths = blocks.iter().map(|block| block.length).collect::<Vec<_>>();
        let Some(content_validation) = backend
            .service_validate_data_object(
                &head.signed.payload.content_container,
                &head.signed.payload.content_object,
                &block_lengths,
                token,
            )
            .await?
        else {
            return Ok(ReplicaValidation::Incomplete {
                head,
                reason: "the immutable content object is missing".to_owned(),
            });
        };
        validate_content_digests(&head.signed.payload, &blocks, &content_validation)?;
        Ok(ReplicaValidation::Valid(ValidatedReplica {
            head,
            block_manifest: Some(block_value.bytes),
            block_pages,
            committed_manifest: committed_value.bytes,
            high_water_checkpoint,
        }))
    }
}

fn validate_block_layout(commit: &CommitManifest, blocks: &[BlockDescriptor]) -> Result<()> {
    let mut expected_offset = 0_u64;
    for (expected_index, block) in blocks.iter().enumerate() {
        ensure!(
            block.index == u32::try_from(expected_index)?,
            "block indices are not contiguous"
        );
        ensure!(
            block.offset == expected_offset,
            "block offsets are not contiguous"
        );
        expected_offset = expected_offset
            .checked_add(block.length)
            .context("block layout length overflow")?;
    }
    ensure!(
        expected_offset == commit.content_length,
        "block manifest does not cover the complete content"
    );
    Ok(())
}

fn validate_content_digests(
    commit: &CommitManifest,
    blocks: &[BlockDescriptor],
    content: &DataObjectValidation,
) -> Result<()> {
    ensure!(
        content.digest.length == commit.content_length,
        "content length does not match the committed manifest"
    );
    ensure!(
        content.digest.sha256 == commit.content_sha256,
        "complete content hash validation failed"
    );
    ensure!(
        content.block_sha256.len() == blocks.len(),
        "streaming validation returned the wrong block count"
    );
    for (block, actual) in blocks.iter().zip(&content.block_sha256) {
        ensure!(
            actual == &block.sha256,
            "block content hash validation failed"
        );
    }
    Ok(())
}
