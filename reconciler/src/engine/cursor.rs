use super::*;

const CURSOR_CONTENT_TYPE: &str = "application/json";

impl ReconcilerEngine {
    pub(super) async fn load_cursor(
        &self,
        object_key: &'static str,
        api_version: &str,
        token: &ControlToken,
    ) -> Result<CursorPublication> {
        let mut replicas = Vec::with_capacity(self.ring.nodes.len());
        let mut candidates = Vec::new();
        for node in &self.ring.nodes {
            let value = self
                .backend(&node.id)?
                .control_get_object(object_key, token)
                .await?;
            replicas.push(CursorReplicaState {
                backend_id: node.id.clone(),
                etag: value.as_ref().and_then(|value| value.etag.clone()),
                exists: value.is_some(),
            });
            let Some(value) = value else {
                continue;
            };
            let signed = SignedDocument::<DiscoveryCursor>::from_bytes(&value.bytes)
                .with_context(|| format!("cursor {object_key} is not valid signed JSON"))?;
            signed
                .verify(
                    SignatureDomain::ReconcilerCursor,
                    &signed.payload.signing_key_id,
                    self.signer.as_ref(),
                )
                .with_context(|| format!("cursor {object_key} signature validation failed"))?;
            ensure!(
                signed.canonical_bytes()? == value.bytes,
                "cursor {object_key} is not canonically encoded"
            );
            self.validate_cursor(&signed.payload, api_version)?;
            candidates.push((signed.payload, value.bytes));
        }

        let cursor = if candidates.is_empty() {
            DiscoveryCursor {
                api_version: api_version.to_owned(),
                ring_version: self.ring.ring_version,
                sequence: 0,
                node_index: 0,
                backend_cursor: None,
                signing_key_id: self.signer.key_id().to_owned(),
            }
        } else {
            candidates.sort_by(|left, right| {
                left.0
                    .sequence
                    .cmp(&right.0.sequence)
                    .then_with(|| left.1.cmp(&right.1))
            });
            let selected = candidates.pop().expect("non-empty cursor candidates");
            if candidates.last().is_some_and(|candidate| {
                candidate.0.sequence == selected.0.sequence && candidate.1 != selected.1
            }) {
                warn!(
                    cursor = object_key,
                    sequence = selected.0.sequence,
                    "concurrent signed cursor publications detected; selected deterministically"
                );
            }
            selected.0
        };

        Ok(CursorPublication {
            object_key,
            cursor,
            replicas,
        })
    }

    pub(super) async fn persist_cursor(
        &self,
        mut publication: CursorPublication,
        token: &ControlToken,
    ) -> Result<()> {
        publication.cursor.sequence = publication
            .cursor
            .sequence
            .checked_add(1)
            .context("cursor sequence overflow")?;
        publication.cursor.signing_key_id = self.signer.key_id().to_owned();
        let signed = SignedDocument::create(
            publication.cursor,
            SignatureDomain::ReconcilerCursor,
            self.signer.as_ref(),
        )
        .await?;
        let bytes = signed.canonical_bytes()?;
        let mut errors = Vec::new();
        for replica in publication.replicas {
            let condition = if replica.exists {
                match replica.etag {
                    Some(etag) => PutCondition::IfMatch(etag),
                    None => {
                        errors.push(format!(
                            "{}: existing cursor has no backend ETag",
                            replica.backend_id
                        ));
                        continue;
                    }
                }
            } else {
                PutCondition::IfAbsent
            };
            if let Err(error) = self
                .backend(&replica.backend_id)?
                .control_put_bytes(
                    publication.object_key,
                    bytes.clone(),
                    CURSOR_CONTENT_TYPE,
                    condition,
                    token,
                )
                .await
            {
                errors.push(format!("{}: {error}", replica.backend_id));
            }
        }
        ensure!(
            errors.is_empty(),
            "cursor {} publication was incomplete: {}",
            publication.object_key,
            errors.join("; ")
        );
        Ok(())
    }

    fn validate_cursor(&self, cursor: &DiscoveryCursor, api_version: &str) -> Result<()> {
        ensure!(
            cursor.api_version == api_version,
            "unsupported discovery cursor apiVersion"
        );
        ensure!(
            cursor.ring_version == self.ring.ring_version,
            "discovery cursor Ring version mismatch"
        );
        ensure!(
            cursor.node_index < self.ring.nodes.len(),
            "discovery cursor node index is out of range"
        );
        ensure!(
            !cursor.signing_key_id.is_empty(),
            "discovery cursor signing key ID is empty"
        );
        Ok(())
    }
}
