use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use sha2::{Digest, Sha256};
use thiserror::Error;
use tracing::warn;
use uuid::Uuid;

use crate::{
    RingDocument,
    auth::AuthenticatedPrincipal,
    backend::{
        BackendError, BackendLease, ObjectValue, PutCondition, ReplicaBackend, SharedBackend,
    },
    catalog::{CatalogError, catalog_key, validate_catalog_entry},
    identity::{ControlToken, SharedControlTokenProvider},
    manifest::{
        BLOCK_MANIFEST_PAGE_SIZE, BlockDescriptor, BlockManifest, BlockManifestPage,
        BlockManifestPageReference, CommitManifest, HistoryCompactionCheckpoint, ManifestError,
        ManifestSigner, ManifestState, ReconciliationRecord, ReconciliationRecordAction,
        SignatureDomain, SignedDocument, commit_manifest_object_prefix, logical_etag, sha256_bytes,
        validate_block_manifest_layout, validate_block_manifest_page,
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
    pub(crate) ring: Arc<RingDocument>,
    pub(crate) backends: HashMap<String, SharedBackend>,
    pub(crate) signer: Arc<dyn ManifestSigner>,
    pub(crate) control_tokens: SharedControlTokenProvider,
    listing_token_lifetime: Duration,
    staging_lifetime: Duration,
}

#[derive(Debug, Clone, Copy)]
pub struct CommitServiceOptions {
    pub listing_token_lifetime: Duration,
    pub staging_lifetime: Duration,
}

impl Default for CommitServiceOptions {
    fn default() -> Self {
        Self {
            listing_token_lifetime: Duration::from_secs(15 * 60),
            staging_lifetime: Duration::from_secs(7 * 24 * 60 * 60),
        }
    }
}

pub(crate) struct LoadedHead {
    pub(crate) signed: SignedDocument<CommitManifest>,
    pub(crate) bytes: Vec<u8>,
    pub(crate) backend_etag: Option<String>,
}

struct EncodedBlockPage {
    reference: BlockManifestPageReference,
    bytes: Vec<u8>,
}

struct LoadedHighWater {
    signed: SignedDocument<CommitManifest>,
    bytes: Vec<u8>,
    backend_etag: Option<String>,
}

pub(crate) struct LoadedCompactionCheckpoint {
    pub(crate) signed: SignedDocument<HistoryCompactionCheckpoint>,
    pub(crate) bytes: Vec<u8>,
    pub(crate) backend_etag: Option<String>,
}

mod delete;
mod high_water;
mod locking;
mod quarantine;
mod recovery;
mod write;

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
        ring: Arc<RingDocument>,
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
        ring: Arc<RingDocument>,
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
            .replicas_for(logical_blob.canonical())
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

pub(crate) fn strict_current_head<'a>(
    primary: Option<&'a LoadedHead>,
    secondary: Option<&'a LoadedHead>,
) -> Result<Option<&'a LoadedHead>, CommitError> {
    match (primary, secondary) {
        (None, None) => Ok(None),
        (Some(primary), Some(secondary)) if primary.bytes == secondary.bytes => Ok(Some(primary)),
        _ => Err(CommitError::ReplicaDrift),
    }
}

pub(crate) async fn load_head(
    backend: &dyn ReplicaBackend,
    head_key: &str,
    control_token: &ControlToken,
    signer: &dyn ManifestSigner,
) -> Result<Option<LoadedHead>, CommitError> {
    let Some(object) = backend.control_get_object(head_key, control_token).await? else {
        return Ok(None);
    };
    let signed = SignedDocument::<CommitManifest>::from_bytes(&object.bytes)?;
    signed.verify(
        SignatureDomain::CommitManifest,
        &signed.payload.signing_key_id,
        signer,
    )?;
    if !matches!(
        signed.payload.state,
        ManifestState::Committed | ManifestState::Tombstoned
    ) {
        return Err(CommitError::VerificationFailed);
    }
    if signed.payload.state == ManifestState::Tombstoned {
        validate_tombstone_manifest(&signed.payload)?;
    }

    Ok(Some(LoadedHead {
        signed,
        bytes: object.bytes,
        backend_etag: object.etag,
    }))
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

pub(crate) fn head_condition(head: Option<&LoadedHead>) -> PutCondition {
    match head.and_then(|value| value.backend_etag.clone()) {
        Some(etag) => PutCondition::IfMatch(etag),
        None => PutCondition::IfAbsent,
    }
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

pub(crate) async fn put_file_idempotent(
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

pub(crate) async fn put_bytes_idempotent(
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
    committed: &SignedDocument<CommitManifest>,
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
    if expected.signed_head.payload != committed.payload {
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
    expected: &SignedDocument<CommitManifest>,
    expected_bytes: &[u8],
    replica_ids: [&str; 2],
    signer: &dyn ManifestSigner,
) -> Result<(), CommitError> {
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
        let old = &validated.signed_head.payload;
        if old.logical_version.saturating_add(1) != expected.payload.logical_version
            || expected.payload.previous_logical_etag.as_deref() != Some(old.logical_etag.as_str())
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
