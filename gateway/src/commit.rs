use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::warn;
use uuid::Uuid;

use crate::{
    SignedRing,
    auth::AuthenticatedPrincipal,
    backend::{
        BackendError, BackendLease, ObjectValue, PutCondition, ReplicaBackend, SharedBackend,
    },
    catalog::{CatalogError, catalog_key, validate_catalog_entry},
    identity::{ControlToken, SharedControlTokenProvider},
    manifest::{
        BLOCK_MANIFEST_PAGE_SIZE, BlobCommitState, BlockDescriptor, BlockManifest,
        BlockManifestPage, BlockManifestPageReference, CommitManifest, HistoryCompactionCheckpoint,
        ManifestError, ManifestSigner, ManifestState, ReconciliationRecord,
        ReconciliationRecordAction, SignatureDomain, SignedDocument, logical_etag, sha256_bytes,
        validate_blob_commit_state, validate_block_manifest_layout, validate_block_manifest_page,
    },
    read::ReadService,
    resource::{LogicalBlobId, stable_component},
    upload::SpoolContent,
};

#[derive(Debug, Clone)]
pub struct CommitResult {
    pub logical_version: u64,
    pub logical_etag: String,
    pub write_id: String,
    pub idempotent_replay: bool,
}

#[derive(Debug, Clone)]
pub struct DeleteResult {
    pub logical_version: u64,
    pub logical_etag: String,
    pub write_id: String,
    pub deleted_at_unix_ms: u64,
    pub idempotent_replay: bool,
}

#[derive(Debug, Clone)]
pub enum LogicalCondition {
    None,
    IfAbsent,
    IfMatch(String),
}

#[derive(Debug, Error)]
pub enum CommitError {
    #[error("replica backend failed: {0}")]
    Backend(#[from] BackendError),
    #[error("manifest operation failed: {0}")]
    Manifest(#[from] ManifestError),
    #[error("manifest serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("catalog validation failed: {0}")]
    Catalog(#[from] CatalogError),
    #[error("replica heads do not have one strict committed value")]
    ReplicaDrift,
    #[error("write id already exists with a different payload")]
    IdempotencyConflict,
    #[error("a conditional head update failed")]
    ConditionFailed,
    #[error("the write outcome is ambiguous because only part of head publication completed")]
    Ambiguous,
    #[error("the primary blob lock is already held")]
    LockConflict,
    #[error("committed head verification failed")]
    VerificationFailed,
    #[error("the logical blob is quarantined")]
    Quarantined,
    #[error("the logical blob does not exist")]
    NotFound,
}

#[derive(Clone)]
pub struct CommitCoordinator {
    pub(crate) primary: SharedBackend,
    pub(crate) secondary: SharedBackend,
    pub(crate) signer: Arc<dyn ManifestSigner>,
    pub(crate) control_tokens: SharedControlTokenProvider,
    pub(crate) ring_version: u64,
}

#[derive(Clone)]
pub struct CommitService {
    pub(crate) ring: Arc<SignedRing>,
    pub(crate) backends: HashMap<String, SharedBackend>,
    pub(crate) signer: Arc<dyn ManifestSigner>,
    pub(crate) control_tokens: SharedControlTokenProvider,
    listing_token_lifetime: Duration,
    listing_validation_concurrency: usize,
    listing_validation_limiter: Arc<Semaphore>,
    staging_lifetime: Duration,
}

#[derive(Debug, Clone, Copy)]
pub struct CommitServiceOptions {
    pub listing_token_lifetime: Duration,
    pub listing_validation_concurrency: usize,
    pub staging_lifetime: Duration,
}

impl Default for CommitServiceOptions {
    fn default() -> Self {
        Self {
            listing_token_lifetime: Duration::from_secs(15 * 60),
            listing_validation_concurrency: 32,
            staging_lifetime: Duration::from_secs(7 * 24 * 60 * 60),
        }
    }
}

pub(crate) struct LoadedState {
    pub(crate) signed: SignedDocument<BlobCommitState>,
    pub(crate) bytes: Vec<u8>,
    pub(crate) backend_etag: Option<String>,
}

impl LoadedState {
    pub(crate) fn current(&self) -> Option<&CommitManifest> {
        self.signed.payload.current()
    }

    pub(crate) fn prepared(&self) -> Option<&CommitManifest> {
        self.signed.payload.prepared()
    }
}

struct EncodedBlockPage {
    reference: BlockManifestPageReference,
    bytes: Vec<u8>,
}

#[derive(Clone)]
pub(crate) struct LoadedCompactionCheckpoint {
    pub(crate) signed: SignedDocument<HistoryCompactionCheckpoint>,
    pub(crate) bytes: Vec<u8>,
    pub(crate) backend_etag: Option<String>,
}

/// The terminal form of the generation a merged commit-state document
/// publishes. It is the durable high-water history entry, which never carries a
/// preparation, so an idempotent replay can republish it safely.
pub(crate) struct TerminalCommitState {
    pub(crate) signed: SignedDocument<BlobCommitState>,
    pub(crate) bytes: Vec<u8>,
}

/// The Reconciler-owned safety state validated once under the canonical commit
/// lease and reused for the whole request. ADR-0012 makes this reuse sound by
/// making the lease canonical; ADR-0010 keeps the state itself replicated.
pub(crate) struct ValidatedCommitContext {
    pub(crate) compaction: Option<LoadedCompactionCheckpoint>,
    pub(crate) current_terminal: Option<TerminalCommitState>,
}

mod delete;
mod high_water;
mod locking;
mod quarantine;
mod recovery;
mod write;

pub(crate) use high_water::validate_publication_floor;
pub(crate) use quarantine::ensure_not_quarantined;

impl CommitCoordinator {
    pub fn new(
        primary: SharedBackend,
        secondary: SharedBackend,
        signer: Arc<dyn ManifestSigner>,
        control_tokens: SharedControlTokenProvider,
        ring_version: u64,
    ) -> Self {
        Self {
            primary,
            secondary,
            signer,
            control_tokens,
            ring_version,
        }
    }

    pub(crate) async fn authorize_replay(
        &self,
        principal: &AuthenticatedPrincipal,
        committed: &CommitManifest,
    ) -> Result<(), CommitError> {
        if committed.caller != principal.identity() {
            return Err(CommitError::IdempotencyConflict);
        }
        let (primary_content, secondary_content) = tokio::try_join!(
            self.primary.caller_head_data_object(
                &committed.content_container,
                &committed.content_object,
                &principal.access_token
            ),
            self.secondary.caller_head_data_object(
                &committed.content_container,
                &committed.content_object,
                &principal.access_token
            )
        )?;
        if [primary_content, secondary_content]
            .into_iter()
            .any(|content| content.is_none_or(|value| value.length != committed.content_length))
        {
            return Err(CommitError::VerificationFailed);
        }
        tokio::try_join!(
            self.primary.authorize_existing_blob_write(
                &committed.content_container,
                &committed.content_object,
                &principal.access_token
            ),
            self.secondary.authorize_existing_blob_write(
                &committed.content_container,
                &committed.content_object,
                &principal.access_token
            )
        )?;
        Ok(())
    }
}

impl CommitService {
    pub fn new(
        ring: Arc<SignedRing>,
        backends: HashMap<String, SharedBackend>,
        signer: Arc<dyn ManifestSigner>,
        control_tokens: SharedControlTokenProvider,
    ) -> Self {
        Self::new_with_options(
            ring,
            backends,
            signer,
            control_tokens,
            CommitServiceOptions::default(),
        )
    }

    pub fn new_with_options(
        ring: Arc<SignedRing>,
        backends: HashMap<String, SharedBackend>,
        signer: Arc<dyn ManifestSigner>,
        control_tokens: SharedControlTokenProvider,
        options: CommitServiceOptions,
    ) -> Self {
        Self {
            ring,
            backends,
            signer,
            control_tokens,
            listing_token_lifetime: options.listing_token_lifetime,
            listing_validation_concurrency: options.listing_validation_concurrency,
            listing_validation_limiter: Arc::new(Semaphore::new(
                options.listing_validation_concurrency,
            )),
            staging_lifetime: options.staging_lifetime,
        }
    }

    pub async fn validate_control_plane(&self) -> Result<(), CommitError> {
        let control_token = self
            .control_tokens
            .token()
            .await
            .map_err(|error| BackendError::InvalidResponse(error.to_string()))?;
        for backend in self.backends.values() {
            backend.validate_control_container(&control_token).await?;
        }
        Ok(())
    }

    pub fn read_service(&self) -> ReadService {
        ReadService::new(
            self.ring.clone(),
            self.backends.clone(),
            self.signer.clone(),
            self.control_tokens.clone(),
        )
    }

    pub fn listing_service(
        &self,
        logical_account: impl Into<String>,
    ) -> crate::listing::ListingService {
        crate::listing::ListingService::new(
            logical_account,
            self.ring.clone(),
            self.backends.clone(),
            self.signer.clone(),
            self.control_tokens.clone(),
            self.listing_token_lifetime,
            self.listing_validation_concurrency,
            self.listing_validation_limiter.clone(),
        )
    }

    pub fn block_service(self: &Arc<Self>) -> crate::block::BlockService {
        crate::block::BlockService::new(self.clone(), self.staging_lifetime)
    }

    pub(crate) fn coordinator(
        &self,
        logical_blob: &LogicalBlobId,
    ) -> Result<CommitCoordinator, CommitError> {
        let replicas = self
            .ring
            .replicas_for(logical_blob)
            .map_err(|_| CommitError::ReplicaDrift)?;
        let primary = self
            .backends
            .get(&replicas[0].id)
            .cloned()
            .ok_or(CommitError::ReplicaDrift)?;
        let secondary = self
            .backends
            .get(&replicas[1].id)
            .cloned()
            .ok_or(CommitError::ReplicaDrift)?;
        Ok(CommitCoordinator::new(
            primary,
            secondary,
            self.signer.clone(),
            self.control_tokens.clone(),
            self.ring.ring_version,
        ))
    }

    pub async fn put_blob(
        &self,
        logical_blob: &LogicalBlobId,
        principal: &AuthenticatedPrincipal,
        write_id: &str,
        content: &SpoolContent,
        logical_condition: LogicalCondition,
    ) -> Result<CommitResult, CommitError> {
        self.coordinator(logical_blob)?
            .put_blob(
                logical_blob,
                principal,
                write_id,
                content,
                logical_condition,
            )
            .await
    }

    pub async fn delete_blob(
        &self,
        logical_blob: &LogicalBlobId,
        principal: &AuthenticatedPrincipal,
        write_id: &str,
        logical_condition: LogicalCondition,
    ) -> Result<DeleteResult, CommitError> {
        self.coordinator(logical_blob)?
            .delete_blob(logical_blob, principal, write_id, logical_condition)
            .await
    }
}

pub(crate) fn blob_state_key(path_hash: &str) -> String {
    format!("heads/{path_hash}.json")
}

/// The write path resolves the generation both replicas publish. ADR-0002, as
/// amended by ADR-0012, requires byte-identical merged documents on read; under
/// the canonical commit lease a divergence confined to an interrupted
/// preparation is recoverable and is re-driven rather than failed closed.
pub(crate) fn resolve_write_state<'a>(
    primary: Option<&'a LoadedState>,
    secondary: Option<&'a LoadedState>,
) -> Result<Option<&'a LoadedState>, CommitError> {
    match (primary, secondary) {
        (None, None) => Ok(None),
        (Some(state), None) | (None, Some(state)) if state.current().is_none() => Ok(None),
        (Some(primary), Some(secondary)) if primary.current() == secondary.current() => {
            Ok(primary.current().is_some().then_some(primary))
        }
        _ => Err(CommitError::ReplicaDrift),
    }
}

pub(crate) fn strict_current_state<'a>(
    primary: Option<&'a LoadedState>,
    secondary: Option<&'a LoadedState>,
) -> Result<Option<&'a LoadedState>, CommitError> {
    match (primary, secondary) {
        (None, None) => Ok(None),
        (Some(primary), Some(secondary)) if primary.bytes == secondary.bytes => Ok(Some(primary)),
        _ => Err(CommitError::ReplicaDrift),
    }
}

/// Verifies a merged commit-state document. Every element ADR-0012 merged is
/// verified here once: canonical encoding, the signature that covers the
/// current generation, the high-water assertion it carries, any interrupted
/// preparation, and the binding to its own object key.
pub(crate) fn verify_state_bytes(
    bytes: &[u8],
    state_key: &str,
    signer: &dyn ManifestSigner,
) -> Result<SignedDocument<BlobCommitState>, CommitError> {
    let signed = SignedDocument::<BlobCommitState>::from_bytes(bytes)?;
    if signed.canonical_bytes()? != bytes {
        return Err(CommitError::VerificationFailed);
    }
    signed.verify(
        SignatureDomain::BlobCommitState,
        &signed.payload.signing_key_id,
        signer,
    )?;
    validate_blob_commit_state(&signed.payload)?;
    if blob_state_key(&signed.payload.path_hash) != state_key {
        return Err(CommitError::VerificationFailed);
    }
    if signed
        .payload
        .current()
        .is_some_and(|current| current.state == ManifestState::Tombstoned)
    {
        validate_tombstone_manifest(signed.payload.current().expect("checked current"))?;
    }
    Ok(signed)
}

/// Loads and fully verifies the merged commit-state document.
pub(crate) async fn load_state(
    backend: &dyn ReplicaBackend,
    state_key: &str,
    control_token: &ControlToken,
    signer: &dyn ManifestSigner,
) -> Result<Option<LoadedState>, CommitError> {
    let Some(object) = backend.control_get_object(state_key, control_token).await? else {
        return Ok(None);
    };
    let signed = verify_state_bytes(&object.bytes, state_key, signer)?;
    Ok(Some(LoadedState {
        signed,
        bytes: object.bytes,
        backend_etag: object.etag,
    }))
}

/// Publishes a merged commit-state document to both replicas under the
/// conditional transition ADR-0012 requires, then proves under ADR-0013 that
/// both replicas hold byte-identical bytes.
pub(crate) async fn publish_blob_state(
    primary: &dyn ReplicaBackend,
    secondary: &dyn ReplicaBackend,
    state_key: &str,
    bytes: &[u8],
    primary_condition: PutCondition,
    secondary_condition: PutCondition,
    control_token: &ControlToken,
) -> Result<(Option<String>, Option<String>), CommitError> {
    let (primary_publish, secondary_publish) = tokio::join!(
        primary.control_put_bytes(
            state_key,
            bytes.to_vec(),
            "application/json",
            primary_condition,
            control_token
        ),
        secondary.control_put_bytes(
            state_key,
            bytes.to_vec(),
            "application/json",
            secondary_condition,
            control_token
        )
    );
    let (primary_result, secondary_result) = match (primary_publish, secondary_publish) {
        (Ok(primary_result), Ok(secondary_result)) => (primary_result, secondary_result),
        (Err(first), Err(second)) if is_condition_error(&first) && is_condition_error(&second) => {
            return Err(CommitError::ConditionFailed);
        }
        (Err(error), Ok(_)) | (Ok(_), Err(error)) => {
            warn!(error = %error, "only one replica published the merged commit state");
            return Err(CommitError::Ambiguous);
        }
        (Err(first), Err(second)) => {
            warn!(primary_error = %first, secondary_error = %second, "both commit state publications failed");
            return Err(CommitError::Backend(first));
        }
    };
    verify_identical_objects(primary, secondary, state_key, bytes, control_token).await?;
    Ok((primary_result.etag, secondary_result.etag))
}

fn validate_tombstone_manifest(manifest: &CommitManifest) -> Result<(), CommitError> {
    if manifest.state != ManifestState::Tombstoned
        || manifest.deleted_at_unix_ms.is_none()
        || manifest.previous_logical_etag.is_none()
        || manifest.version_object_prefix.is_none()
        || manifest.content_length != 0
        || !manifest.content_container.is_empty()
        || !manifest.content_object.is_empty()
        || !manifest.block_manifest_object.is_empty()
        || !manifest.block_manifest_sha256.is_empty()
        || manifest.prepared_replicas.len() != 2
    {
        return Err(CommitError::VerificationFailed);
    }
    Ok(())
}

fn validate_tombstone_transition(
    tombstone: &CommitManifest,
    previous: &CommitManifest,
) -> Result<(), CommitError> {
    validate_tombstone_manifest(tombstone)?;
    if previous.state != ManifestState::Committed
        || tombstone.blob != previous.blob
        || tombstone.ring_version != previous.ring_version
        || tombstone.logical_version != previous.logical_version.saturating_add(1)
        || tombstone.previous_logical_etag.as_deref() != Some(&previous.logical_etag)
    {
        return Err(CommitError::VerificationFailed);
    }
    Ok(())
}

fn delete_result(
    tombstone: &CommitManifest,
    idempotent_replay: bool,
) -> Result<DeleteResult, CommitError> {
    validate_tombstone_manifest(tombstone)?;
    Ok(DeleteResult {
        logical_version: tombstone.logical_version,
        logical_etag: tombstone.logical_etag.clone(),
        write_id: tombstone.write_id.clone(),
        deleted_at_unix_ms: tombstone
            .deleted_at_unix_ms
            .ok_or(CommitError::VerificationFailed)?,
        idempotent_replay,
    })
}

/// Adopts an interrupted preparation for the same write so a retry re-publishes
/// the identical generation. ADR-0012 makes the prepared manifest a state of the
/// merged document, and the absence of an overwrite is the interruption signal.
pub(crate) fn adopt_interrupted_preparation(
    primary_state: Option<&LoadedState>,
    secondary_state: Option<&LoadedState>,
    prepared: &mut CommitManifest,
) -> Result<(), CommitError> {
    let existing = primary_state
        .and_then(LoadedState::prepared)
        .or_else(|| secondary_state.and_then(LoadedState::prepared));
    let Some(existing) = existing else {
        return Ok(());
    };
    if existing.write_id != prepared.write_id {
        return Ok(());
    }
    prepared.committed_at_unix_ms = existing.committed_at_unix_ms;
    prepared.deleted_at_unix_ms = existing.deleted_at_unix_ms;
    if existing == prepared {
        Ok(())
    } else {
        Err(CommitError::IdempotencyConflict)
    }
}

pub(crate) fn state_condition(state: Option<&LoadedState>) -> PutCondition {
    head_condition_from_etag(state.and_then(|value| value.backend_etag.as_deref()))
}

fn head_condition_from_object(object: Option<&ObjectValue>) -> PutCondition {
    head_condition_from_etag(object.and_then(|value| value.etag.as_deref()))
}

fn head_condition_from_etag(etag: Option<&str>) -> PutCondition {
    match etag {
        Some(etag) => PutCondition::IfMatch(etag.to_owned()),
        None => PutCondition::IfAbsent,
    }
}

pub(crate) async fn caller_put_file_idempotent(
    backend: &dyn ReplicaBackend,
    container: &str,
    object_key: &str,
    content: &SpoolContent,
    caller_token: &crate::identity::CallerToken,
) -> Result<(), CommitError> {
    match backend
        .caller_put_data_file(
            container,
            object_key,
            &content.path,
            content.length,
            PutCondition::IfAbsent,
            caller_token,
        )
        .await
    {
        Ok(_) => return Ok(()),
        Err(BackendError::PreconditionFailed | BackendError::AlreadyExists) => {}
        Err(error) => return Err(error.into()),
    }
    let stored = backend
        .caller_digest_data_object(container, object_key, caller_token)
        .await?
        .ok_or(CommitError::VerificationFailed)?;
    if stored.length == content.length && stored.sha256 == content.content_sha256 {
        Ok(())
    } else {
        Err(CommitError::VerificationFailed)
    }
}

pub(crate) async fn control_put_bytes_idempotent(
    backend: &dyn ReplicaBackend,
    object_key: &str,
    bytes: Vec<u8>,
    control_token: &ControlToken,
) -> Result<(), CommitError> {
    match backend
        .control_put_bytes(
            object_key,
            bytes.clone(),
            "application/json",
            PutCondition::IfAbsent,
            control_token,
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(BackendError::PreconditionFailed | BackendError::AlreadyExists) => {
            let existing = backend
                .control_get_object(object_key, control_token)
                .await?
                .ok_or(CommitError::VerificationFailed)?;
            if existing.bytes == bytes {
                Ok(())
            } else {
                Err(CommitError::IdempotencyConflict)
            }
        }
        Err(error) => Err(error.into()),
    }
}

pub(crate) async fn verify_identical_objects(
    primary: &dyn ReplicaBackend,
    secondary: &dyn ReplicaBackend,
    object_key: &str,
    expected: &[u8],
    control_token: &ControlToken,
) -> Result<(), CommitError> {
    let (primary_value, secondary_value) = tokio::try_join!(
        primary.control_get_object(object_key, control_token),
        secondary.control_get_object(object_key, control_token)
    )?;
    if values_match(primary_value.as_ref(), secondary_value.as_ref(), expected) {
        Ok(())
    } else {
        Err(CommitError::VerificationFailed)
    }
}

pub(crate) async fn publish_catalog_current(
    primary: &dyn ReplicaBackend,
    secondary: &dyn ReplicaBackend,
    logical_blob: &LogicalBlobId,
    committed: &SignedDocument<BlobCommitState>,
    committed_bytes: &[u8],
    control_token: &ControlToken,
    signer: &dyn ManifestSigner,
) -> Result<(), CommitError> {
    let object_key = catalog_key(logical_blob);
    let replica_ids = [primary.id(), secondary.id()];
    let expected = validate_catalog_entry(
        logical_blob.account(),
        &object_key,
        committed_bytes,
        committed.payload.ring_version,
        replica_ids,
        signer,
    )?;
    if expected.signed_state.payload != committed.payload {
        return Err(CommitError::VerificationFailed);
    }
    let (primary_current, secondary_current) = tokio::try_join!(
        primary.control_get_object(&object_key, control_token),
        secondary.control_get_object(&object_key, control_token)
    )?;
    validate_catalog_predecessors(
        logical_blob,
        &object_key,
        primary_current.as_ref(),
        secondary_current.as_ref(),
        committed,
        committed_bytes,
        replica_ids,
        signer,
    )?;

    let (primary_publish, secondary_publish) = tokio::join!(
        publish_catalog_to_backend(
            primary,
            &object_key,
            committed_bytes,
            primary_current.as_ref(),
            control_token
        ),
        publish_catalog_to_backend(
            secondary,
            &object_key,
            committed_bytes,
            secondary_current.as_ref(),
            control_token
        )
    );
    match (primary_publish, secondary_publish) {
        (Ok(()), Ok(())) => {}
        (Err(first), Err(second)) if is_condition_error(&first) && is_condition_error(&second) => {
            return Err(CommitError::ConditionFailed);
        }
        (Err(error), Ok(())) | (Ok(()), Err(error)) => {
            warn!(error = %error, "only one replica published the current catalog entry");
            return Err(CommitError::Ambiguous);
        }
        (Err(first), Err(_)) => return Err(CommitError::Backend(first)),
    }
    verify_identical_objects(
        primary,
        secondary,
        &object_key,
        committed_bytes,
        control_token,
    )
    .await
}

async fn publish_catalog_to_backend(
    backend: &dyn ReplicaBackend,
    object_key: &str,
    bytes: &[u8],
    current: Option<&ObjectValue>,
    control_token: &ControlToken,
) -> Result<(), BackendError> {
    if current.is_some_and(|value| value.bytes == bytes) {
        return Ok(());
    }
    backend
        .control_put_bytes(
            object_key,
            bytes.to_vec(),
            "application/json",
            head_condition_from_object(current),
            control_token,
        )
        .await
        .map(|_| ())
}

#[allow(clippy::too_many_arguments)]
fn validate_catalog_predecessors(
    logical_blob: &LogicalBlobId,
    object_key: &str,
    primary: Option<&ObjectValue>,
    secondary: Option<&ObjectValue>,
    expected: &SignedDocument<BlobCommitState>,
    expected_bytes: &[u8],
    replica_ids: [&str; 2],
    signer: &dyn ManifestSigner,
) -> Result<(), CommitError> {
    let expected_head = expected
        .payload
        .current()
        .ok_or(CommitError::VerificationFailed)?;
    let mut predecessor: Option<&[u8]> = None;
    for current in [primary, secondary].into_iter().flatten() {
        if current.bytes == expected_bytes {
            continue;
        }
        let validated = validate_catalog_entry(
            logical_blob.account(),
            object_key,
            &current.bytes,
            expected.payload.ring_version,
            replica_ids,
            signer,
        )?;
        let old = validated.head().ok_or(CommitError::VerificationFailed)?;
        // ADR-0012 removes the immutable terminal manifest, so a retried write
        // re-signs the same generation. Two signatures of one generation are the
        // same catalogue truth, not a conflicting predecessor.
        if old == expected_head {
            continue;
        }
        if old.logical_version.saturating_add(1) != expected_head.logical_version
            || expected_head.previous_logical_etag.as_deref() != Some(old.logical_etag.as_str())
        {
            return Err(CommitError::VerificationFailed);
        }
        if predecessor.is_some_and(|bytes| bytes != current.bytes) {
            return Err(CommitError::VerificationFailed);
        }
        predecessor = Some(&current.bytes);
    }
    Ok(())
}

fn values_match(
    primary: Option<&ObjectValue>,
    secondary: Option<&ObjectValue>,
    expected: &[u8],
) -> bool {
    matches!(
        (primary, secondary),
        (Some(primary), Some(secondary))
            if primary.bytes == expected && secondary.bytes == expected
    )
}

fn is_condition_error(error: &BackendError) -> bool {
    matches!(
        error,
        BackendError::PreconditionFailed | BackendError::AlreadyExists
    )
}

pub fn logical_path_hash(logical_blob: &str) -> String {
    hex::encode(Sha256::digest(logical_blob.as_bytes()))
}

fn now_unix_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after Unix epoch")
            .as_millis(),
    )
    .expect("Unix timestamp milliseconds fit in u64")
}

pub(crate) async fn maintain_lease(
    backend: &dyn ReplicaBackend,
    lease: &BackendLease,
    control_token: &ControlToken,
    renewal_interval: Duration,
) -> BackendError {
    loop {
        tokio::time::sleep(renewal_interval).await;
        if let Err(error) = backend.control_renew_lock(lease, control_token).await {
            return error;
        }
    }
}

#[cfg(test)]
mod tests;
