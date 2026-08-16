use super::*;
use overmesh_gateway::catalog::{catalog_key_from_canonical, validate_catalog_entry_for_blob};

pub(super) enum CatalogReconciliation {
    Current,
    Repaired,
    Conflict(String),
}

impl ReconcilerEngine {
    pub(super) async fn reconcile_catalog_current(
        &self,
        blob: &str,
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
        let replicas = self.ring.replicas_for(blob)?;
        ensure!(replicas.len() == 2, "catalog reconciliation requires W=2");
        let replica_ids = [replicas[0].id.as_str(), replicas[1].id.as_str()];
        let object_key = catalog_key_from_canonical(blob)?;
        let expected = validate_catalog_entry_for_blob(
            blob,
            &object_key,
            &first_head.bytes,
            self.ring.ring_version,
            replica_ids,
            self.signer.as_ref(),
        )
        .context("current head is not valid catalog truth")?;
        ensure!(
            expected.signed_head.payload.blob == blob,
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
            let existing = match validate_catalog_entry_for_blob(
                blob,
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
            if existing.signed_head.payload.logical_version
                >= expected.signed_head.payload.logical_version
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
