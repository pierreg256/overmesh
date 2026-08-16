use super::*;

impl ReconcilerEngine {
    pub(super) async fn discover_heads(
        &self,
        token: &ControlToken,
        mode: HeadDiscoveryMode,
    ) -> Result<HeadDiscoveryBatch> {
        if mode == HeadDiscoveryMode::FullAudit {
            let mut heads = BTreeMap::new();
            for node in &self.ring.nodes {
                let backend = self.backend(&node.id)?;
                for object_key in backend.control_list_objects(HEAD_PREFIX, token).await? {
                    heads.entry(object_key).or_insert_with(|| node.id.clone());
                }
            }
            return Ok(HeadDiscoveryBatch {
                candidates: heads
                    .into_iter()
                    .map(|(object_key, discovered_on)| HeadCandidate {
                        object_key,
                        discovered_on,
                    })
                    .collect(),
                next_cursor: None,
            });
        }

        ensure!(!self.ring.nodes.is_empty(), "Ring contains no nodes");
        let cursor = self.load_discovery_cursor()?;
        let node = self
            .ring
            .nodes
            .get(cursor.node_index)
            .context("head discovery cursor node index is out of range")?;
        let backend = self.backend(&node.id)?;
        let page = backend
            .control_list_objects_page(
                HEAD_PREFIX,
                cursor.backend_cursor.as_deref(),
                self.head_discovery_batch_size,
                token,
            )
            .await?;
        let next_cursor = match page.next_cursor {
            Some(backend_cursor) => HeadDiscoveryCursor {
                backend_cursor: Some(backend_cursor),
                ..cursor
            },
            None => HeadDiscoveryCursor {
                api_version: cursor.api_version,
                ring_version: cursor.ring_version,
                node_index: (cursor.node_index + 1) % self.ring.nodes.len(),
                backend_cursor: None,
            },
        };
        Ok(HeadDiscoveryBatch {
            candidates: page
                .objects
                .into_iter()
                .map(|object_key| HeadCandidate {
                    object_key,
                    discovered_on: node.id.clone(),
                })
                .collect(),
            next_cursor: Some(next_cursor),
        })
    }

    pub(super) fn load_discovery_cursor(&self) -> Result<HeadDiscoveryCursor> {
        let cursor = if self.head_discovery_cursor_path.exists() {
            serde_json::from_slice::<HeadDiscoveryCursor>(
                &fs::read(&self.head_discovery_cursor_path).with_context(|| {
                    format!(
                        "failed to read head discovery cursor {}",
                        self.head_discovery_cursor_path.display()
                    )
                })?,
            )
            .with_context(|| {
                format!(
                    "failed to parse head discovery cursor {}",
                    self.head_discovery_cursor_path.display()
                )
            })?
        } else {
            HeadDiscoveryCursor {
                api_version: "reconciler.overmesh.io/head-discovery-cursor/v1".to_owned(),
                ring_version: self.ring.ring_version,
                node_index: 0,
                backend_cursor: None,
            }
        };
        ensure!(
            cursor.api_version == "reconciler.overmesh.io/head-discovery-cursor/v1",
            "unsupported head discovery cursor apiVersion"
        );
        ensure!(
            cursor.ring_version == self.ring.ring_version,
            "head discovery cursor Ring version mismatch"
        );
        ensure!(
            cursor.node_index < self.ring.nodes.len(),
            "head discovery cursor node index is out of range"
        );
        Ok(cursor)
    }

    pub(super) fn persist_discovery_cursor(&self, cursor: &HeadDiscoveryCursor) -> Result<()> {
        if let Some(parent) = self.head_discovery_cursor_path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create head discovery cursor directory {}",
                    parent.display()
                )
            })?;
        }
        let temporary = cursor_temporary_path(&self.head_discovery_cursor_path);
        fs::write(&temporary, serde_json::to_vec(cursor)?).with_context(|| {
            format!(
                "failed to write head discovery cursor {}",
                temporary.display()
            )
        })?;
        if let Err(error) = fs::rename(&temporary, &self.head_discovery_cursor_path) {
            let _ = fs::remove_file(&temporary);
            return Err(error).with_context(|| {
                format!(
                    "failed to publish head discovery cursor {}",
                    self.head_discovery_cursor_path.display()
                )
            });
        }
        Ok(())
    }

    pub(super) async fn discover_blob_path(
        &self,
        head_object: &str,
        token: &ControlToken,
    ) -> Result<Option<String>> {
        let expected_hash = head_hash(head_object)?;
        let mut discovered = None;
        for backend in self.backends.values() {
            let Some(value) = backend.control_get_object(head_object, token).await? else {
                continue;
            };
            let Ok(head) = SignedDocument::<CommitManifest>::from_bytes(&value.bytes) else {
                continue;
            };
            if head
                .verify(
                    SignatureDomain::CommitManifest,
                    &head.payload.signing_key_id,
                    self.signer.as_ref(),
                )
                .is_err()
                || head.payload.ring_version != self.ring.ring_version
                || logical_path_hash(&head.payload.blob) != expected_hash
            {
                continue;
            }
            if let Some(existing) = &discovered {
                ensure!(
                    existing == &head.payload.blob,
                    "head hash resolves to conflicting signed blob paths"
                );
            } else {
                discovered = Some(head.payload.blob);
            }
        }
        Ok(discovered)
    }
}

fn cursor_temporary_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("head-discovery-cursor");
    path.with_file_name(format!("{file_name}.{}.tmp", Uuid::new_v4()))
}
