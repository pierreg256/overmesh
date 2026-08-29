use super::*;
use overmesh_gateway::catalog::{catalog_key, validate_catalog_entry_for_logical_blob};

pub(super) enum CatalogReconciliation {
    Current,
    Repaired,
    Conflict(String),
}

impl ReconcilerEngine {
    /// The terminal commit-state bytes for the generation a head publishes.
    /// Returns `None` when an interrupted preparation makes the head non-terminal
    /// and no durable history entry is available to stand in for it.
    async fn terminal_state_bytes(
        &self,
        logical_blob: &LogicalBlobId,
        head_bytes: &[u8],
        first: &dyn ReplicaBackend,
        second: &dyn ReplicaBackend,
        token: &ControlToken,
    ) -> Result<Option<Vec<u8>>> {
        let signed = parse_blob_commit_state(head_bytes, self.signer.as_ref(), "committed head")?;
        if signed.payload.prepared().is_none() {
            return Ok(Some(head_bytes.to_vec()));
        }
        let Some(current) = signed.payload.current() else {
            return Ok(None);
        };
        let history_key = high_water_history_key(&logical_blob.path_hash(), current);
        let (first_value, second_value) = tokio::try_join!(
            first.control_get_object(&history_key, token),
            second.control_get_object(&history_key, token)
        )?;
        let Some(value) = first_value.or(second_value) else {
            warn!(
                blob = logical_blob.canonical(),
                "catalogue reconciliation deferred while a preparation is interrupted"
            );
            return Ok(None);
        };
        let history =
            parse_blob_commit_state(&value.bytes, self.signer.as_ref(), "high-water history")?;
        ensure!(
            history.payload.prepared().is_none() && history.payload.current() == Some(current),
            "high-water history does not publish the head generation"
        );
        Ok(Some(value.bytes))
    }

    pub(super) async fn reconcile_catalog_current(
        &self,
        logical_blob: &LogicalBlobId,
        head_object: &str,
        first: &dyn ReplicaBackend,
        second: &dyn ReplicaBackend,
        token: &ControlToken,
    ) -> Result<CatalogReconciliation> {
        let (first_head, second_head) = tokio::try_join!(
            first.control_get_object(head_object, token),
            second.control_get_object(head_object, token)
        )?;
        let (Some(first_head), Some(second_head)) = (first_head, second_head) else {
            bail!("catalog reconciliation requires W=2 current heads");
        };
        ensure!(
            first_head.bytes == second_head.bytes,
            "catalog reconciliation requires identical W=2 current heads"
        );
        let replicas = self.ring.replicas_for(logical_blob)?;
        ensure!(replicas.len() == 2, "catalog reconciliation requires W=2");
        let replica_ids = [replicas[0].id.as_str(), replicas[1].id.as_str()];
        let object_key = catalog_key(logical_blob);
        // A catalogue entry is the terminal form of the merged commit-state
        // document. While a preparation is in flight the head is not terminal,
        // so the Reconciler reads the durable history entry rather than minting
        // Gateway-owned state it does not own (ADR-0003, ADR-0012).
        let Some(terminal_bytes) = self
            .terminal_state_bytes(logical_blob, &first_head.bytes, first, second, token)
            .await?
        else {
            return Ok(CatalogReconciliation::Current);
        };
        let first_head = ObjectValue {
            bytes: terminal_bytes,
            ..first_head
        };
        let expected = validate_catalog_entry_for_logical_blob(
            logical_blob,
            &object_key,
            &first_head.bytes,
            self.ring.ring_version,
            replica_ids,
            self.signer.as_ref(),
        )
        .context("current head is not valid catalog truth")?;
        ensure!(
            expected.signed_state.payload.blob == logical_blob.canonical(),
            "catalog head blob mismatch"
        );

        let (first_catalog, second_catalog) = tokio::try_join!(
            first.control_get_object(&object_key, token),
            second.control_get_object(&object_key, token)
        )?;
        let mut predecessor: Option<&[u8]> = None;
        for (replica, value) in [
            (first.id(), first_catalog.as_ref()),
            (second.id(), second_catalog.as_ref()),
        ] {
            let Some(value) = value else {
                continue;
            };
            if value.bytes == first_head.bytes {
                continue;
            }
            let existing = match validate_catalog_entry_for_logical_blob(
                logical_blob,
                &object_key,
                &value.bytes,
                self.ring.ring_version,
                replica_ids,
                self.signer.as_ref(),
            ) {
                Ok(value) => value,
                Err(error) => {
                    return Ok(CatalogReconciliation::Conflict(format!(
                        "{replica} catalog entry is tampered or mis-keyed: {error}"
                    )));
                }
            };
            if existing.head().map(|head| head.logical_version)
                >= expected.head().map(|head| head.logical_version)
            {
                return Ok(CatalogReconciliation::Conflict(format!(
                    "{replica} catalog entry conflicts with or is newer than the W=2 current head"
                )));
            }
            if predecessor.is_some_and(|bytes| bytes != value.bytes) {
                return Ok(CatalogReconciliation::Conflict(
                    "replica catalog entries contain different older signed states".to_owned(),
                ));
            }
            predecessor = Some(&value.bytes);
        }

        if first_catalog
            .as_ref()
            .is_some_and(|value| value.bytes == first_head.bytes)
            && second_catalog
                .as_ref()
                .is_some_and(|value| value.bytes == first_head.bytes)
        {
            return Ok(CatalogReconciliation::Current);
        }

        let first_write = publish_catalog_repair(
            first,
            &object_key,
            &first_head.bytes,
            first_catalog.as_ref(),
            token,
        );
        let second_write = publish_catalog_repair(
            second,
            &object_key,
            &first_head.bytes,
            second_catalog.as_ref(),
            token,
        );
        tokio::try_join!(first_write, second_write)?;
        verify_identical_control_objects(first, second, &object_key, &first_head.bytes, token)
            .await?;
        Ok(CatalogReconciliation::Repaired)
    }
}

async fn publish_catalog_repair(
    backend: &dyn ReplicaBackend,
    object_key: &str,
    expected: &[u8],
    current: Option<&ObjectValue>,
    token: &ControlToken,
) -> Result<()> {
    if current.is_some_and(|value| value.bytes == expected) {
        return Ok(());
    }
    backend
        .control_put_bytes(
            object_key,
            expected.to_vec(),
            "application/json",
            match current.and_then(|value| value.etag.clone()) {
                Some(etag) => PutCondition::IfMatch(etag),
                None => PutCondition::IfAbsent,
            },
            token,
        )
        .await?;
    Ok(())
}
