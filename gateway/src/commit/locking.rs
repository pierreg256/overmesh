use super::*;

impl CommitCoordinator {
    pub async fn put_blob(
        &self,
        logical_blob: &LogicalBlobId,
        principal: &AuthenticatedPrincipal,
        write_id: &str,
        content: &SpoolContent,
        logical_condition: LogicalCondition,
    ) -> Result<CommitResult, CommitError> {
        let control_token = self
            .control_tokens
            .token()
            .await
            .map_err(|error| BackendError::InvalidResponse(error.to_string()))?;
        let path_hash = logical_blob.path_hash();
        ensure_not_quarantined(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            &path_hash,
            &control_token,
            self.signer.as_ref(),
        )
        .await?;
        let lock_key = format!("locks/{path_hash}");
        let lease = self
            .primary
            .control_acquire_lock(&lock_key, &control_token)
            .await
            .map_err(|error| match error {
                BackendError::LeaseConflict => CommitError::LockConflict,
                other => CommitError::Backend(other),
            })?;

        let commit = self.put_blob_locked(
            logical_blob,
            principal,
            write_id,
            content,
            logical_condition,
            &control_token,
        );
        let lease_maintenance = maintain_lease(
            self.primary.as_ref(),
            &lease,
            &control_token,
            Duration::from_secs(30),
        );
        tokio::pin!(commit);
        tokio::pin!(lease_maintenance);
        let result = tokio::select! {
            result = &mut commit => result,
            error = &mut lease_maintenance => Err(CommitError::Backend(error)),
        };
        if let Err(error) = self
            .primary
            .control_release_lock(&lease, &control_token)
            .await
        {
            if result.is_ok() {
                return Err(CommitError::Backend(error));
            }
            warn!(error = %error, "failed to release blob lock after write failure");
        }
        result
    }

    pub async fn delete_blob(
        &self,
        logical_blob: &LogicalBlobId,
        principal: &AuthenticatedPrincipal,
        write_id: &str,
        logical_condition: LogicalCondition,
    ) -> Result<DeleteResult, CommitError> {
        let control_token = self
            .control_tokens
            .token()
            .await
            .map_err(|error| BackendError::InvalidResponse(error.to_string()))?;
        let path_hash = logical_blob.path_hash();
        ensure_not_quarantined(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            &path_hash,
            &control_token,
            self.signer.as_ref(),
        )
        .await?;
        let lock_key = format!("locks/{path_hash}");
        let lease = self
            .primary
            .control_acquire_lock(&lock_key, &control_token)
            .await
            .map_err(|error| match error {
                BackendError::LeaseConflict => CommitError::LockConflict,
                other => CommitError::Backend(other),
            })?;

        let commit = self.delete_blob_locked(
            logical_blob,
            principal,
            write_id,
            logical_condition,
            &control_token,
        );
        let lease_maintenance = maintain_lease(
            self.primary.as_ref(),
            &lease,
            &control_token,
            Duration::from_secs(30),
        );
        tokio::pin!(commit);
        tokio::pin!(lease_maintenance);
        let result = tokio::select! {
            result = &mut commit => result,
            error = &mut lease_maintenance => Err(CommitError::Backend(error)),
        };
        if let Err(error) = self
            .primary
            .control_release_lock(&lease, &control_token)
            .await
        {
            if result.is_ok() {
                return Err(CommitError::Backend(error));
            }
            warn!(error = %error, "failed to release blob lock after delete failure");
        }
        result
    }
}
