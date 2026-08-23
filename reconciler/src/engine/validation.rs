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
        // ADR-0012: one signed document carries the committed generation, its
        // high-water assertion and any interrupted preparation.
        let signed = parse_blob_commit_state(
            &head_object_value.bytes,
            self.signer.as_ref(),
            "committed head",
        )?;
        let Some(manifest) = signed.payload.current().cloned() else {
            return Ok(ReplicaValidation::MissingHead);
        };
        ensure!(
            manifest.ring_version == self.ring.ring_version,
            "head Ring version does not match the active Ring"
        );
        let logical_blob = parse_signed_logical_blob(&manifest.blob, "committed head")?;
        ensure!(
            head_object == head_object_key(&logical_blob),
            "head object path does not match the signed blob path"
        );
        if manifest.state == ManifestState::Tombstoned {
            ensure!(
                manifest.deleted_at_unix_ms.is_some()
                    && manifest.previous_logical_etag.is_some()
                    && manifest.version_object_prefix.is_some()
                    && manifest.content_length == 0
                    && manifest.content_container.is_empty()
                    && manifest.content_object.is_empty()
                    && manifest.block_manifest_object.is_empty()
                    && manifest.block_manifest_sha256.is_empty()
                    && manifest.prepared_replicas.len() == 2,
                "tombstone structure is invalid"
            );
        }
        let terminal = signed.payload.prepared().is_none();
        drop(signed);
        let high_water_checkpoint =
            validate_high_water(backend, head_object, &manifest, token, self.signer.as_ref())
                .await?;
        let head = ValidatedHead {
            logical_blob,
            manifest,
            bytes: head_object_value.bytes,
            backend_etag: head_object_value.etag,
        };
        if head.manifest.state == ManifestState::Tombstoned {
            if let Some(high_water_checkpoint) = high_water_checkpoint {
                return Ok(ReplicaValidation::Valid(ValidatedReplica {
                    head,
                    block_manifest: None,
                    block_pages: Vec::new(),
                    high_water_checkpoint,
                }));
            }
            if !terminal {
                return Ok(ReplicaValidation::Incomplete {
                    head,
                    reason: "the durable tombstone high-water checkpoint is missing".to_owned(),
                });
            }
            // The signed tombstone was published before its durable history
            // entry. The head is already terminal, so its own bytes are the
            // checkpoint and no Gateway-owned state has to be minted.
            let tombstone_checkpoint = head.bytes.clone();
            return Ok(ReplicaValidation::RecoverableTombstone {
                replica: ValidatedReplica {
                    head,
                    block_manifest: None,
                    block_pages: Vec::new(),
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

        /// The durable per-version history entry is the independent witness
        /// that the published generation was retained (ADR-0012).
        async fn validate_high_water(
            backend: &dyn ReplicaBackend,
            head_object: &str,
            manifest: &CommitManifest,
            token: &ControlToken,
            signer: &dyn ManifestSigner,
        ) -> Result<Option<Vec<u8>>> {
            let path_hash = head_hash(head_object)?;
            let object_key = high_water_history_key(path_hash, manifest);
            // ADR-0012 folds the high-water current object into the merged
            // document, so the retained per-version history is the witness that
            // the published generation has not been replayed backwards.
            let successor_prefix = format!(
                "high-water/{path_hash}/history/{:020}",
                manifest.logical_version.saturating_add(1)
            );
            let (value, successors) = tokio::try_join!(
                backend.control_get_object(&object_key, token),
                backend.control_list_objects(&successor_prefix, token)
            )?;
            ensure!(
                successors.is_empty(),
                "committed head was replayed below the durable high-water version"
            );
            let Some(value) = value else {
                return Ok(None);
            };
            let history = parse_blob_commit_state(&value.bytes, signer, "high-water checkpoint")?;
            ensure!(
                history.payload.prepared().is_none() && history.payload.current() == Some(manifest),
                "committed head does not match the durable high-water checkpoint"
            );
            Ok(Some(value.bytes))
        }

        let Some(block_value) = backend
            .control_get_object(&head.manifest.block_manifest_object, token)
            .await?
        else {
            return Ok(ReplicaValidation::Incomplete {
                head,
                reason: "the signed block manifest is missing".to_owned(),
            });
        };
        ensure!(
            sha256_bytes(&block_value.bytes) == head.manifest.block_manifest_sha256,
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
        validate_block_manifest_link(&head.manifest, &signed_block.payload)
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

        validate_block_layout(&head.manifest, &blocks)?;
        let block_lengths = blocks.iter().map(|block| block.length).collect::<Vec<_>>();
        let Some(content_validation) = backend
            .service_validate_data_object(
                &head.manifest.content_container,
                &head.manifest.content_object,
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
        validate_content_digests(&head.manifest, &blocks, &content_validation)?;
        Ok(ReplicaValidation::Valid(ValidatedReplica {
            head,
            block_manifest: Some(block_value.bytes),
            block_pages,
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
