use super::*;

/// A merged commit-state document that one replica published and the other did
/// not. ADR-0012 keeps the crash window of the two-phase commit, but the window
/// now spans one object instead of four.
struct PartialPublication<'a> {
    committed: &'a LoadedState,
    lagging: Option<&'a LoadedState>,
    missing_backend: &'a dyn ReplicaBackend,
}

impl CommitCoordinator {
    fn detect_partial_publication<'a>(
        &'a self,
        primary_state: Option<&'a LoadedState>,
        secondary_state: Option<&'a LoadedState>,
        write_id: &str,
    ) -> Option<PartialPublication<'a>> {
        let extends = |committed: &LoadedState, lagging: &LoadedState| {
            let Some(committed) = committed.current() else {
                return false;
            };
            if committed.write_id != write_id {
                return false;
            }
            match lagging.current() {
                // A replica that publishes no generation lags a first commit.
                None => committed.logical_version == 1 && committed.previous_logical_etag.is_none(),
                Some(lagging) => {
                    committed.previous_logical_etag.as_deref() == Some(&lagging.logical_etag)
                        && committed.logical_version == lagging.logical_version.saturating_add(1)
                }
            }
        };
        match (primary_state, secondary_state) {
            (Some(committed), None) if committed.current().is_some() => Some(PartialPublication {
                committed,
                lagging: None,
                missing_backend: self.secondary.as_ref(),
            }),
            (None, Some(committed)) if committed.current().is_some() => Some(PartialPublication {
                committed,
                lagging: None,
                missing_backend: self.primary.as_ref(),
            }),
            (Some(committed), Some(lagging)) if extends(committed, lagging) => {
                Some(PartialPublication {
                    committed,
                    lagging: Some(lagging),
                    missing_backend: self.secondary.as_ref(),
                })
            }
            (Some(lagging), Some(committed)) if extends(committed, lagging) => {
                Some(PartialPublication {
                    committed,
                    lagging: Some(lagging),
                    missing_backend: self.primary.as_ref(),
                })
            }
            _ => None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::commit) async fn recover_partial_publication(
        &self,
        primary_state: Option<&LoadedState>,
        secondary_state: Option<&LoadedState>,
        state_key: &str,
        logical_blob: &LogicalBlobId,
        principal: &AuthenticatedPrincipal,
        write_id: &str,
        content: &SpoolContent,
        control_token: &ControlToken,
    ) -> Result<Option<CommitResult>, CommitError> {
        let Some(partial) =
            self.detect_partial_publication(primary_state, secondary_state, write_id)
        else {
            return Ok(None);
        };
        let committed = partial
            .committed
            .current()
            .ok_or(CommitError::VerificationFailed)?;
        if committed.write_id != write_id {
            return Err(CommitError::ReplicaDrift);
        }
        if committed.state != ManifestState::Committed {
            return Err(CommitError::VerificationFailed);
        }
        if committed.content_sha256 != content.content_sha256 {
            return Err(CommitError::IdempotencyConflict);
        }
        // A recovered publication must never be a rollback, and the recovered
        // document is now its own high-water assertion.
        Self::validate_recovery_candidate(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            &logical_blob.path_hash(),
            logical_blob.canonical(),
            self.ring_version,
            committed,
            partial.lagging.and_then(LoadedState::current),
            control_token,
            self.signer.as_ref(),
        )
        .await?;
        self.authorize_replay(principal, committed).await?;
        publish_catalog_current(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            logical_blob,
            &partial.committed.signed,
            &partial.committed.bytes,
            control_token,
            self.signer.as_ref(),
        )
        .await?;
        tokio::try_join!(
            caller_put_file_idempotent(
                self.primary.as_ref(),
                &committed.content_container,
                &committed.content_object,
                content,
                &principal.access_token
            ),
            caller_put_file_idempotent(
                self.secondary.as_ref(),
                &committed.content_container,
                &committed.content_object,
                content,
                &principal.access_token
            )
        )?;
        match partial
            .missing_backend
            .control_put_bytes(
                state_key,
                partial.committed.bytes.clone(),
                "application/json",
                state_condition(partial.lagging),
                control_token,
            )
            .await
        {
            Ok(_) => {}
            Err(BackendError::PreconditionFailed | BackendError::AlreadyExists) => {
                return Err(CommitError::ReplicaDrift);
            }
            Err(error) => return Err(CommitError::Backend(error)),
        }
        verify_identical_objects(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            state_key,
            &partial.committed.bytes,
            control_token,
        )
        .await?;
        Self::publish_state_history(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            &logical_blob.path_hash(),
            committed,
            &partial.committed.bytes,
            control_token,
        )
        .await?;
        Ok(Some(CommitResult {
            logical_version: committed.logical_version,
            logical_etag: committed.logical_etag.clone(),
            write_id: write_id.to_owned(),
            idempotent_replay: true,
        }))
    }

    pub(in crate::commit) async fn recover_partial_tombstone_publication(
        &self,
        primary_state: Option<&LoadedState>,
        secondary_state: Option<&LoadedState>,
        state_key: &str,
        logical_blob: &LogicalBlobId,
        write_id: &str,
        control_token: &ControlToken,
    ) -> Result<Option<DeleteResult>, CommitError> {
        let published_tombstone = |state: &LoadedState| {
            state.current().is_some_and(|current| {
                current.state == ManifestState::Tombstoned && current.write_id == write_id
            })
        };
        // A partial tombstone publication is a divergence of the *published
        // generation*. ADR-0012 makes the prepared manifest a state of the same
        // document, so two replicas can hold different bytes while publishing
        // the same tombstone. That is an asymmetric preparation, not a partial
        // publication: it falls through to the idempotent replay path and is
        // converged by reconciliation.
        let diverged =
            |first: &LoadedState, second: &LoadedState| first.current() != second.current();
        let (tombstone_state, lagging_state, lagging_backend) =
            match (primary_state, secondary_state) {
                (Some(primary), Some(secondary))
                    if diverged(primary, secondary) && published_tombstone(primary) =>
                {
                    (primary, secondary, self.secondary.as_ref())
                }
                (Some(primary), Some(secondary))
                    if diverged(primary, secondary) && published_tombstone(secondary) =>
                {
                    (secondary, primary, self.primary.as_ref())
                }
                _ => return Ok(None),
            };
        let tombstone = tombstone_state
            .current()
            .ok_or(CommitError::VerificationFailed)?;
        let lagging = lagging_state
            .current()
            .ok_or(CommitError::VerificationFailed)?;
        validate_tombstone_transition(tombstone, lagging)?;
        Self::validate_recovery_candidate(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            &logical_blob.path_hash(),
            logical_blob.canonical(),
            self.ring_version,
            tombstone,
            Some(lagging),
            control_token,
            self.signer.as_ref(),
        )
        .await?;
        publish_catalog_current(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            logical_blob,
            &tombstone_state.signed,
            &tombstone_state.bytes,
            control_token,
            self.signer.as_ref(),
        )
        .await?;
        lagging_backend
            .control_put_bytes(
                state_key,
                tombstone_state.bytes.clone(),
                "application/json",
                state_condition(Some(lagging_state)),
                control_token,
            )
            .await?;
        verify_identical_objects(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            state_key,
            &tombstone_state.bytes,
            control_token,
        )
        .await?;
        Self::publish_state_history(
            self.primary.as_ref(),
            self.secondary.as_ref(),
            &logical_blob.path_hash(),
            tombstone,
            &tombstone_state.bytes,
            control_token,
        )
        .await?;
        Ok(Some(delete_result(tombstone, true)?))
    }
}
