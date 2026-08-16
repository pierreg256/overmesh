use super::*;

pub(super) async fn put_immutable(
    backend: &dyn ReplicaBackend,
    object_key: &str,
    bytes: Vec<u8>,
    content_type: &'static str,
    token: &ControlToken,
) -> Result<()> {
    match backend
        .control_put_bytes(
            object_key,
            bytes.clone(),
            content_type,
            PutCondition::IfAbsent,
            token,
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(BackendError::PreconditionFailed | BackendError::AlreadyExists) => {
            let existing = backend
                .control_get_object(object_key, token)
                .await?
                .context("conditional immutable write found no existing object")?;
            ensure!(
                existing.bytes == bytes,
                "existing immutable object differs from the authoritative source"
            );
            Ok(())
        }

        Err(error) => Err(error.into()),
    }
}

pub(super) async fn replace_control_object(
    backend: &dyn ReplicaBackend,
    object_key: &str,
    bytes: Vec<u8>,
    expected_etag: Option<&str>,
    token: &ControlToken,
) -> Result<()> {
    let expected_etag = expected_etag.context("mutable checkpoint object has no backend ETag")?;
    backend
        .control_put_bytes(
            object_key,
            bytes,
            "application/json",
            PutCondition::IfMatch(expected_etag.to_owned()),
            token,
        )
        .await?;
    Ok(())
}

pub(super) async fn publish_checkpoint_to_backend(
    backend: &dyn ReplicaBackend,
    object_key: &str,
    current: Option<ObjectValue>,
    publication: &CheckpointPublication,
    token: &ControlToken,
) -> Result<()> {
    if current
        .as_ref()
        .is_some_and(|value| value.bytes == publication.bytes)
    {
        return Ok(());
    }
    ensure!(
        current.as_ref().map(|value| value.bytes.as_slice())
            == publication.expected_previous_bytes.as_deref(),
        "history compaction checkpoint changed during publication"
    );
    let condition = match current.as_ref().and_then(|value| value.etag.clone()) {
        Some(etag) => PutCondition::IfMatch(etag),
        None => PutCondition::IfAbsent,
    };
    backend
        .control_put_bytes(
            object_key,
            publication.bytes.clone(),
            "application/json",
            condition,
            token,
        )
        .await?;
    Ok(())
}

pub(super) async fn verify_identical_control_objects(
    first_backend: &dyn ReplicaBackend,
    second_backend: &dyn ReplicaBackend,
    object_key: &str,
    expected_bytes: &[u8],
    token: &ControlToken,
) -> Result<()> {
    let (first, second) = tokio::try_join!(
        first_backend.control_get_object(object_key, token),
        second_backend.control_get_object(object_key, token)
    )?;
    ensure!(
        first.as_ref().map(|value| value.bytes.as_slice()) == Some(expected_bytes)
            && second.as_ref().map(|value| value.bytes.as_slice()) == Some(expected_bytes),
        "W=2 control object verification failed"
    );
    Ok(())
}

pub(super) async fn delete_validated_control_object(
    first_backend: &dyn ReplicaBackend,
    second_backend: &dyn ReplicaBackend,
    deletion: &ControlDelete,
    token: &ControlToken,
) -> Result<()> {
    match (
        deletion.first_etag.as_deref(),
        deletion.second_etag.as_deref(),
    ) {
        (Some(first_etag), Some(second_etag)) => {
            tokio::try_join!(
                first_backend.control_delete_object(&deletion.object_key, Some(first_etag), token),
                second_backend.control_delete_object(
                    &deletion.object_key,
                    Some(second_etag),
                    token
                )
            )?;
        }
        (Some(first_etag), None) => {
            first_backend
                .control_delete_object(&deletion.object_key, Some(first_etag), token)
                .await?;
        }
        (None, Some(second_etag)) => {
            second_backend
                .control_delete_object(&deletion.object_key, Some(second_etag), token)
                .await?;
        }
        (None, None) => {}
    }
    Ok(())
}

pub(super) async fn put_immutable_data(
    backend: &dyn ReplicaBackend,
    container: &str,
    object_key: &str,
    bytes: Vec<u8>,
    token: &ControlToken,
) -> Result<()> {
    match backend
        .service_put_data_bytes(
            container,
            object_key,
            bytes.clone(),
            PutCondition::IfAbsent,
            token,
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(BackendError::PreconditionFailed | BackendError::AlreadyExists) => {
            let existing = backend
                .service_get_data_object(container, object_key, token)
                .await?
                .context("conditional immutable data write found no existing object")?;
            ensure!(
                existing.bytes == bytes,
                "existing immutable data differs from the authoritative source"
            );
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

pub(super) async fn overwrite_for_recovery(
    backend: &dyn ReplicaBackend,
    object_key: &str,
    bytes: Vec<u8>,
    content_type: &'static str,
    token: &ControlToken,
) -> Result<()> {
    let existing = backend.control_get_object(object_key, token).await?;
    if existing.as_ref().is_some_and(|value| value.bytes == bytes) {
        return Ok(());
    }

    backend
        .control_put_bytes(
            object_key,
            bytes,
            content_type,
            match existing.and_then(|value| value.etag) {
                Some(etag) => PutCondition::IfMatch(etag),
                None => PutCondition::IfAbsent,
            },
            token,
        )
        .await?;
    Ok(())
}

pub(super) async fn overwrite_data_for_recovery(
    backend: &dyn ReplicaBackend,
    container: &str,
    object_key: &str,
    bytes: Vec<u8>,
    token: &ControlToken,
) -> Result<()> {
    let existing = backend
        .service_get_data_object(container, object_key, token)
        .await?;
    if existing.as_ref().is_some_and(|value| value.bytes == bytes) {
        return Ok(());
    }
    backend
        .service_put_data_bytes(
            container,
            object_key,
            bytes,
            match existing.and_then(|value| value.etag) {
                Some(etag) => PutCondition::IfMatch(etag),
                None => PutCondition::IfAbsent,
            },
            token,
        )
        .await?;
    Ok(())
}

pub(super) async fn put_current_quarantine(
    backend: &dyn ReplicaBackend,
    object_key: &str,
    bytes: Vec<u8>,
    token: &ControlToken,
    signer: &dyn ManifestSigner,
) -> Result<()> {
    match backend
        .control_put_bytes(
            object_key,
            bytes,
            "application/json",
            PutCondition::IfAbsent,
            token,
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(BackendError::PreconditionFailed | BackendError::AlreadyExists) => {
            let existing = backend
                .control_get_object(object_key, token)
                .await?
                .context("quarantine record disappeared after a conditional conflict")?;
            let record = SignedDocument::<ReconciliationRecord>::from_bytes(&existing.bytes)
                .context("existing quarantine record is invalid JSON")?;
            record
                .verify(
                    SignatureDomain::ReconciliationRecord,
                    &record.payload.signing_key_id,
                    signer,
                )
                .context("existing quarantine record signature is invalid")?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}
