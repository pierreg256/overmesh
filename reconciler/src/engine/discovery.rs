use super::*;

pub(super) const HEAD_CURSOR_KEY: &str = "reconciler-cursors/head-discovery.json";
pub(super) const HEAD_CURSOR_API_VERSION: &str = "reconciler.overmesh.io/head-discovery-cursor/v1";

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
        let loaded = self
            .load_cursor(HEAD_CURSOR_KEY, HEAD_CURSOR_API_VERSION, token)
            .await?;
        let cursor = loaded.cursor.clone();
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
            Some(backend_cursor) => DiscoveryCursor {
                backend_cursor: Some(backend_cursor),
                ..cursor
            },
            None => DiscoveryCursor {
                api_version: cursor.api_version,
                ring_version: cursor.ring_version,
                sequence: cursor.sequence,
                node_index: (cursor.node_index + 1) % self.ring.nodes.len(),
                backend_cursor: None,
                signing_key_id: cursor.signing_key_id,
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
            next_cursor: Some(CursorPublication {
                cursor: next_cursor,
                ..loaded
            }),
        })
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
