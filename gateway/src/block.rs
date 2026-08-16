use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use quick_xml::de::from_str;
use serde::Deserialize;
use thiserror::Error;
use tracing::warn;
use uuid::Uuid;

use crate::{
    auth::AuthenticatedPrincipal,
    backend::BackendError,
    commit::{
        CommitCoordinator, CommitError, CommitResult, CommitService, LogicalCondition,
        ensure_not_quarantined, head_condition, load_head, maintain_lease, publish_catalog_current,
        put_bytes_idempotent, put_file_idempotent, strict_current_head, verify_identical_objects,
    },
    identity::ControlToken,
    manifest::{
        BlockDescriptor, BlockManifest, BlockManifestPage, CommitManifest, ManifestError,
        ManifestSigner, ManifestState, SignatureDomain, SignedDocument, StagedBlock,
        UploadGeneration, commit_manifest_object_prefix, sha256_bytes,
        validate_block_manifest_link, validate_block_manifest_page,
    },
    read::validate_committed_head,
    resource::{LogicalBlobId, stable_component},
    upload::{SpoolBuilder, SpoolContent},
};

pub const MAX_BLOCK_COUNT: usize = 50_000;
pub const MAX_BLOCK_ID_LENGTH: usize = 64;
pub const MAX_BLOCK_SIZE: u64 = 100 * 1024 * 1024;
const STAGED_BLOCK_API_VERSION: &str = "overmesh.io/staged-block/v1";
const UPLOAD_GENERATION_API_VERSION: &str = "overmesh.io/upload-generation/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockListType {
    Committed,
    Uncommitted,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockSelectionKind {
    Latest,
    Committed,
    Uncommitted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockSelection {
    pub kind: BlockSelectionKind,
    pub block_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockItem {
    pub block_id: String,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockListResult {
    pub list_type: BlockListType,
    pub committed: Vec<BlockItem>,
    pub uncommitted: Vec<BlockItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutBlockResult {
    pub write_id: String,
    pub idempotent_replay: bool,
}

#[derive(Debug, Error)]
pub enum BlockError {
    #[error("block ID is invalid")]
    InvalidBlockId,
    #[error("block list is invalid: {0}")]
    InvalidBlockList(String),
    #[error("block count exceeds the supported limit")]
    BlockCountExceedsLimit,
    #[error("block content exceeds the supported limit")]
    BlockTooLarge,
    #[error("staged blocks in one upload must have equal decoded ID lengths")]
    UnequalBlockIdLength,
    #[error("the staged block conflicts with an existing block")]
    Conflict,
    #[error("a selected block does not exist")]
    MissingBlock,
    #[error("blob and staged block list do not exist")]
    NotFound,
    #[error("staged block metadata or content validation failed")]
    VerificationFailed,
    #[error("staged block has expired")]
    Expired,
    #[error("backend operation failed: {0}")]
    Backend(#[from] BackendError),
    #[error("manifest operation failed: {0}")]
    Manifest(#[from] ManifestError),
    #[error("commit operation failed: {0}")]
    Commit(#[from] CommitError),
    #[error("block spool operation failed: {0}")]
    Spool(#[from] anyhow::Error),
}

#[derive(Clone)]
pub struct BlockService {
    commit_service: Arc<CommitService>,
    staging_lifetime: Duration,
}

struct CurrentBlocks {
    head: CommitManifest,
    blocks: Vec<BlockDescriptor>,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "BlockList", deny_unknown_fields)]
struct BlockListXml {
    #[serde(rename = "$value", default)]
    entries: Vec<BlockListXmlEntry>,
}

#[derive(Debug, Deserialize)]
enum BlockListXmlEntry {
    Latest(String),
    Committed(String),
    Uncommitted(String),
}

impl BlockListType {
    pub fn parse(value: Option<&str>) -> Result<Self, BlockError> {
        match value.unwrap_or("committed") {
            "committed" => Ok(Self::Committed),
            "uncommitted" => Ok(Self::Uncommitted),
            "all" => Ok(Self::All),
            _ => Err(BlockError::InvalidBlockList(
                "blocklisttype must be committed, uncommitted, or all".to_owned(),
            )),
        }
    }
}

impl BlockService {
    pub fn new(commit_service: Arc<CommitService>, staging_lifetime: Duration) -> Self {
        Self {
            commit_service,
            staging_lifetime,
        }
    }

    pub async fn put_block(
        &self,
        logical_blob: &LogicalBlobId,
        principal: &AuthenticatedPrincipal,
        upload_id: &str,
        write_id: &str,
        block_id: &str,
        content: &SpoolContent,
    ) -> Result<PutBlockResult, BlockError> {
        let decoded = decode_block_id(block_id)?;
        let decoded_block_id_length =
            u32::try_from(decoded.len()).map_err(|_| BlockError::InvalidBlockId)?;
        if content.length > MAX_BLOCK_SIZE {
            return Err(BlockError::BlockTooLarge);
        }
        let coordinator = self.commit_service.coordinator(logical_blob)?;
        let control_token = coordinator
            .control_tokens
            .token()
            .await
            .map_err(|error| BackendError::InvalidResponse(error.to_string()))?;
        let path_hash = logical_blob.path_hash();
        ensure_not_quarantined(
            coordinator.primary.as_ref(),
            coordinator.secondary.as_ref(),
            &path_hash,
            &control_token,
            coordinator.signer.as_ref(),
        )
        .await?;
        let current = load_current_head(&coordinator, logical_blob, &control_token).await?;
        let base_logical_version = current
            .as_ref()
            .map_or(0, |head| head.payload.logical_version);
        let base_logical_etag = current
            .as_ref()
            .map(|head| head.payload.logical_etag.clone());
        let upload_id = effective_upload_id(
            upload_id,
            logical_blob,
            principal,
            base_logical_version,
            base_logical_etag.as_deref(),
        );
        let lock_key = format!(
            "locks/staged/{}/{}",
            path_hash,
            stable_component(&upload_id)
        );
        let lease = coordinator
            .primary
            .control_acquire_lock(&lock_key, &control_token)
            .await
            .map_err(|error| match error {
                BackendError::LeaseConflict => CommitError::LockConflict,
                other => CommitError::Backend(other),
            })?;
        let operation = self.put_block_locked(
            &coordinator,
            logical_blob,
            principal,
            &upload_id,
            write_id,
            block_id,
            decoded_block_id_length,
            content,
            base_logical_version,
            base_logical_etag,
            &control_token,
        );
        let maintenance = maintain_lease(
            coordinator.primary.as_ref(),
            &lease,
            &control_token,
            Duration::from_secs(30),
        );
        tokio::pin!(operation);
        tokio::pin!(maintenance);
        let result = tokio::select! {
            result = &mut operation => result,
            error = &mut maintenance => Err(BlockError::Backend(error)),
        };
        if let Err(error) = coordinator
            .primary
            .control_release_lock(&lease, &control_token)
            .await
        {
            if result.is_ok() {
                return Err(BlockError::Backend(error));
            }
            warn!(error = %error, "failed to release upload-generation lock");
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn put_block_locked(
        &self,
        coordinator: &CommitCoordinator,
        logical_blob: &LogicalBlobId,
        principal: &AuthenticatedPrincipal,
        upload_id: &str,
        write_id: &str,
        block_id: &str,
        decoded_block_id_length: u32,
        content: &SpoolContent,
        base_logical_version: u64,
        base_logical_etag: Option<String>,
        control_token: &ControlToken,
    ) -> Result<PutBlockResult, BlockError> {
        let generation = self
            .load_or_create_upload_generation(
                coordinator,
                logical_blob,
                principal,
                upload_id,
                decoded_block_id_length,
                base_logical_version,
                base_logical_etag.as_deref(),
                control_token,
            )
            .await?;
        let metadata_key = staged_metadata_key(logical_blob, upload_id, block_id);
        let (primary_existing, secondary_existing) = tokio::try_join!(
            coordinator
                .primary
                .control_get_object(&metadata_key, control_token),
            coordinator
                .secondary
                .control_get_object(&metadata_key, control_token)
        )?;
        if primary_existing.is_some() || secondary_existing.is_some() {
            let bytes = match (primary_existing.as_ref(), secondary_existing.as_ref()) {
                (Some(primary), Some(secondary)) if primary.bytes == secondary.bytes => {
                    primary.bytes.clone()
                }
                (Some(value), None) => value.bytes.clone(),
                (None, Some(value)) => value.bytes.clone(),
                _ => return Err(BlockError::VerificationFailed),
            };
            let staged = validate_staged_document(
                &bytes,
                logical_blob,
                upload_id,
                coordinator.ring_version,
                coordinator.primary.id(),
                coordinator.secondary.id(),
                coordinator.signer.as_ref(),
            )?;
            if now_unix_ms() > staged.payload.expires_at_unix_ms {
                return Err(BlockError::Expired);
            }
            if staged.payload.write_id != write_id
                || staged.payload.block_id != block_id
                || staged.payload.decoded_block_id_length != decoded_block_id_length
                || staged.payload.content_length != content.length
                || staged.payload.content_sha256 != content.content_sha256
                || staged.payload.caller != principal.identity()
                || staged.payload.base_logical_version != base_logical_version
                || staged.payload.base_logical_etag != base_logical_etag
                || staged.payload.expires_at_unix_ms != generation.payload.expires_at_unix_ms
            {
                return Err(BlockError::Conflict);
            }
            match (primary_existing, secondary_existing) {
                (Some(_), None) => {
                    put_bytes_idempotent(
                        coordinator.secondary.as_ref(),
                        &metadata_key,
                        bytes.clone(),
                        control_token,
                    )
                    .await?;
                }
                (None, Some(_)) => {
                    put_bytes_idempotent(
                        coordinator.primary.as_ref(),
                        &metadata_key,
                        bytes.clone(),
                        control_token,
                    )
                    .await?;
                }
                (Some(_), Some(_)) => {}
                (None, None) => unreachable!("existing stage was checked"),
            }
            verify_identical_objects(
                coordinator.primary.as_ref(),
                coordinator.secondary.as_ref(),
                &metadata_key,
                &bytes,
                control_token,
            )
            .await?;
            tokio::try_join!(
                put_file_idempotent(
                    coordinator.primary.as_ref(),
                    logical_blob.container(),
                    &staged.payload.content_object,
                    content,
                    &principal.access_token
                ),
                put_file_idempotent(
                    coordinator.secondary.as_ref(),
                    logical_blob.container(),
                    &staged.payload.content_object,
                    content,
                    &principal.access_token
                )
            )?;
            self.verify_staged_content(coordinator, &staged.payload, principal)
                .await?;
            return Ok(PutBlockResult {
                write_id: write_id.to_owned(),
                idempotent_replay: true,
            });
        }

        let content_object = format!(
            ".overmesh/staged/{}/{}/{}",
            logical_blob.path_hash(),
            stable_component(upload_id),
            Uuid::new_v4().simple()
        );
        let created_at_unix_ms = now_unix_ms();
        let signed = SignedDocument::create(
            StagedBlock {
                api_version: STAGED_BLOCK_API_VERSION.to_owned(),
                blob: logical_blob.canonical().to_owned(),
                upload_id: upload_id.to_owned(),
                write_id: write_id.to_owned(),
                block_id: block_id.to_owned(),
                decoded_block_id_length,
                block_id_sha256: sha256_bytes(&decode_block_id(block_id)?),
                content_container: logical_blob.container().to_owned(),
                content_object,
                content_length: content.length,
                content_sha256: content.content_sha256.clone(),
                base_logical_version,
                base_logical_etag,
                ring_version: coordinator.ring_version,
                prepared_replicas: vec![
                    coordinator.primary.id().to_owned(),
                    coordinator.secondary.id().to_owned(),
                ],
                created_at_unix_ms,
                expires_at_unix_ms: generation.payload.expires_at_unix_ms,
                caller: principal.identity(),
                signing_key_id: coordinator.signer.key_id().to_owned(),
            },
            SignatureDomain::StagedBlock,
            coordinator.signer.as_ref(),
        )
        .await?;
        let bytes = signed.canonical_bytes()?;
        tokio::try_join!(
            put_bytes_idempotent(
                coordinator.primary.as_ref(),
                &metadata_key,
                bytes.clone(),
                control_token
            ),
            put_bytes_idempotent(
                coordinator.secondary.as_ref(),
                &metadata_key,
                bytes.clone(),
                control_token
            )
        )?;
        verify_identical_objects(
            coordinator.primary.as_ref(),
            coordinator.secondary.as_ref(),
            &metadata_key,
            &bytes,
            control_token,
        )
        .await?;
        tokio::try_join!(
            put_file_idempotent(
                coordinator.primary.as_ref(),
                logical_blob.container(),
                &signed.payload.content_object,
                content,
                &principal.access_token
            ),
            put_file_idempotent(
                coordinator.secondary.as_ref(),
                logical_blob.container(),
                &signed.payload.content_object,
                content,
                &principal.access_token
            )
        )?;
        self.verify_staged_content(coordinator, &signed.payload, principal)
            .await?;
        Ok(PutBlockResult {
            write_id: write_id.to_owned(),
            idempotent_replay: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn load_or_create_upload_generation(
        &self,
        coordinator: &CommitCoordinator,
        logical_blob: &LogicalBlobId,
        principal: &AuthenticatedPrincipal,
        upload_id: &str,
        decoded_block_id_length: u32,
        base_logical_version: u64,
        base_logical_etag: Option<&str>,
        control_token: &ControlToken,
    ) -> Result<SignedDocument<UploadGeneration>, BlockError> {
        let key = upload_generation_key(logical_blob, upload_id);
        let (primary, secondary) = tokio::try_join!(
            coordinator.primary.control_get_object(&key, control_token),
            coordinator
                .secondary
                .control_get_object(&key, control_token)
        )?;
        if primary.is_some() || secondary.is_some() {
            let bytes = match (primary.as_ref(), secondary.as_ref()) {
                (Some(primary), Some(secondary)) if primary.bytes == secondary.bytes => {
                    primary.bytes.clone()
                }
                (Some(value), None) | (None, Some(value)) => value.bytes.clone(),
                _ => return Err(BlockError::VerificationFailed),
            };
            let generation = validate_upload_generation(
                &bytes,
                logical_blob,
                upload_id,
                principal,
                decoded_block_id_length,
                base_logical_version,
                base_logical_etag,
                coordinator,
            )?;
            if now_unix_ms() > generation.payload.expires_at_unix_ms {
                return Err(BlockError::Expired);
            }
            match (primary, secondary) {
                (Some(_), None) => {
                    put_bytes_idempotent(
                        coordinator.secondary.as_ref(),
                        &key,
                        bytes.clone(),
                        control_token,
                    )
                    .await?;
                }
                (None, Some(_)) => {
                    put_bytes_idempotent(
                        coordinator.primary.as_ref(),
                        &key,
                        bytes.clone(),
                        control_token,
                    )
                    .await?;
                }
                (Some(_), Some(_)) => {}
                (None, None) => unreachable!("existing generation was checked"),
            }
            verify_identical_objects(
                coordinator.primary.as_ref(),
                coordinator.secondary.as_ref(),
                &key,
                &bytes,
                control_token,
            )
            .await?;
            return Ok(generation);
        }
        let created_at_unix_ms = now_unix_ms();
        let expires_at_unix_ms = created_at_unix_ms
            .checked_add(
                u64::try_from(self.staging_lifetime.as_millis())
                    .map_err(|_| BlockError::VerificationFailed)?,
            )
            .ok_or(BlockError::VerificationFailed)?;
        let generation = SignedDocument::create(
            UploadGeneration {
                api_version: UPLOAD_GENERATION_API_VERSION.to_owned(),
                blob: logical_blob.canonical().to_owned(),
                upload_id: upload_id.to_owned(),
                decoded_block_id_length,
                base_logical_version,
                base_logical_etag: base_logical_etag.map(ToOwned::to_owned),
                ring_version: coordinator.ring_version,
                prepared_replicas: vec![
                    coordinator.primary.id().to_owned(),
                    coordinator.secondary.id().to_owned(),
                ],
                created_at_unix_ms,
                expires_at_unix_ms,
                caller: principal.identity(),
                signing_key_id: coordinator.signer.key_id().to_owned(),
            },
            SignatureDomain::UploadGeneration,
            coordinator.signer.as_ref(),
        )
        .await?;
        let bytes = generation.canonical_bytes()?;
        tokio::try_join!(
            put_bytes_idempotent(
                coordinator.primary.as_ref(),
                &key,
                bytes.clone(),
                control_token
            ),
            put_bytes_idempotent(
                coordinator.secondary.as_ref(),
                &key,
                bytes.clone(),
                control_token
            )
        )?;
        verify_identical_objects(
            coordinator.primary.as_ref(),
            coordinator.secondary.as_ref(),
            &key,
            &bytes,
            control_token,
        )
        .await?;
        Ok(generation)
    }

    pub async fn put_block_list(
        &self,
        logical_blob: &LogicalBlobId,
        principal: &AuthenticatedPrincipal,
        upload_id: &str,
        write_id: &str,
        selections: &[BlockSelection],
        condition: LogicalCondition,
    ) -> Result<CommitResult, BlockError> {
        if selections.is_empty() || selections.len() > MAX_BLOCK_COUNT {
            return Err(BlockError::BlockCountExceedsLimit);
        }
        for selection in selections {
            decode_block_id(&selection.block_id)?;
        }
        let coordinator = self.commit_service.coordinator(logical_blob)?;
        let control_token = coordinator
            .control_tokens
            .token()
            .await
            .map_err(|error| BackendError::InvalidResponse(error.to_string()))?;
        let path_hash = logical_blob.path_hash();
        ensure_not_quarantined(
            coordinator.primary.as_ref(),
            coordinator.secondary.as_ref(),
            &path_hash,
            &control_token,
            coordinator.signer.as_ref(),
        )
        .await?;
        let lock_key = format!("locks/{path_hash}");
        let lease = coordinator
            .primary
            .control_acquire_lock(&lock_key, &control_token)
            .await
            .map_err(|error| match error {
                BackendError::LeaseConflict => CommitError::LockConflict,
                other => CommitError::Backend(other),
            })?;
        let operation = self.put_block_list_locked(
            &coordinator,
            logical_blob,
            principal,
            upload_id,
            write_id,
            selections,
            condition,
            &control_token,
        );
        let maintenance = maintain_lease(
            coordinator.primary.as_ref(),
            &lease,
            &control_token,
            Duration::from_secs(30),
        );
        tokio::pin!(operation);
        tokio::pin!(maintenance);
        let result = tokio::select! {
            result = &mut operation => result,
            error = &mut maintenance => Err(BlockError::Backend(error)),
        };
        if let Err(error) = coordinator
            .primary
            .control_release_lock(&lease, &control_token)
            .await
        {
            if result.is_ok() {
                return Err(BlockError::Backend(error));
            }
            warn!(error = %error, "failed to release blob lock after block-list commit failure");
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn put_block_list_locked(
        &self,
        coordinator: &CommitCoordinator,
        logical_blob: &LogicalBlobId,
        principal: &AuthenticatedPrincipal,
        upload_id: &str,
        write_id: &str,
        selections: &[BlockSelection],
        condition: LogicalCondition,
        control_token: &ControlToken,
    ) -> Result<CommitResult, BlockError> {
        if let Some(result) = self
            .recover_partial_block_list_publication(
                coordinator,
                logical_blob,
                principal,
                write_id,
                selections,
                control_token,
            )
            .await?
        {
            return Ok(result);
        }
        let current_head = load_head_pair(coordinator, logical_blob, control_token).await?;
        let upload_id = effective_upload_id(
            upload_id,
            logical_blob,
            principal,
            current_head
                .as_ref()
                .map_or(0, |head| head.payload.logical_version),
            current_head
                .as_ref()
                .map(|head| head.payload.logical_etag.as_str()),
        );
        if let Some(head) = current_head.as_ref()
            && head.payload.state == ManifestState::Committed
            && head.payload.write_id == write_id
        {
            coordinator
                .authorize_replay(principal, &head.payload)
                .await?;
            let current = self
                .load_blocks_for_head(coordinator, &head.payload, control_token)
                .await?;
            let committed_ids = current
                .blocks
                .iter()
                .map(|block| block.client_block_id.as_deref())
                .collect::<Vec<_>>();
            let requested_ids = selections
                .iter()
                .map(|selection| Some(selection.block_id.as_str()))
                .collect::<Vec<_>>();
            if committed_ids != requested_ids {
                return Err(BlockError::Conflict);
            }
            CommitCoordinator::validate_or_repair_high_water(
                coordinator.primary.as_ref(),
                coordinator.secondary.as_ref(),
                &logical_blob.path_hash(),
                logical_blob.canonical(),
                coordinator.ring_version,
                current_head.as_ref().map(|head| &head.loaded),
                control_token,
                coordinator.signer.as_ref(),
            )
            .await?;
            publish_catalog_current(
                coordinator.primary.as_ref(),
                coordinator.secondary.as_ref(),
                logical_blob,
                &head.loaded.signed,
                &head.loaded.bytes,
                control_token,
                coordinator.signer.as_ref(),
            )
            .await?;
            return Ok(CommitResult {
                logical_version: head.payload.logical_version,
                logical_etag: head.payload.logical_etag.clone(),
                write_id: write_id.to_owned(),
                idempotent_replay: true,
            });
        }
        let current = self
            .load_current_blocks(coordinator, logical_blob, control_token)
            .await?;
        CommitCoordinator::validate_or_repair_high_water(
            coordinator.primary.as_ref(),
            coordinator.secondary.as_ref(),
            &logical_blob.path_hash(),
            logical_blob.canonical(),
            coordinator.ring_version,
            current_head.as_ref().map(|head| &head.loaded),
            control_token,
            coordinator.signer.as_ref(),
        )
        .await?;
        let staged = self
            .load_staged_blocks(
                coordinator,
                logical_blob,
                Some(&upload_id),
                principal,
                control_token,
            )
            .await?;
        let mut staged_by_id = BTreeMap::new();
        for value in staged {
            if value.payload.base_logical_version
                != current_head
                    .as_ref()
                    .map_or(0, |head| head.payload.logical_version)
                || value.payload.base_logical_etag
                    != current_head
                        .as_ref()
                        .map(|head| head.payload.logical_etag.clone())
            {
                continue;
            }
            staged_by_id.insert(value.payload.block_id.clone(), value);
        }
        let mut committed_by_id = BTreeMap::new();
        if let Some(current) = &current {
            for descriptor in &current.blocks {
                if let Some(block_id) = &descriptor.client_block_id {
                    committed_by_id.insert(block_id.clone(), descriptor.clone());
                }
            }
        }
        let mut spool = SpoolBuilder::new().await?;
        for selection in selections {
            let use_staged = match selection.kind {
                BlockSelectionKind::Uncommitted => true,
                BlockSelectionKind::Committed => false,
                BlockSelectionKind::Latest => staged_by_id.contains_key(&selection.block_id),
            };
            if use_staged {
                let staged = staged_by_id
                    .get(&selection.block_id)
                    .ok_or(BlockError::MissingBlock)?;
                let bytes = self
                    .read_staged_bytes(coordinator, &staged.payload, principal)
                    .await?;
                spool
                    .append_block(&bytes, Some(selection.block_id.clone()))
                    .await?;
            } else {
                let descriptor = committed_by_id
                    .get(&selection.block_id)
                    .ok_or(BlockError::MissingBlock)?;
                let current = current.as_ref().ok_or(BlockError::MissingBlock)?;
                let bytes =
                    read_committed_block(coordinator, &current.head, descriptor, principal).await?;
                spool
                    .append_block(&bytes, Some(selection.block_id.clone()))
                    .await?;
            }
        }
        let content = spool.finish().await?;
        Ok(coordinator
            .put_blob_locked(
                logical_blob,
                principal,
                write_id,
                &content,
                condition,
                control_token,
            )
            .await?)
    }

    pub async fn get_block_list(
        &self,
        logical_blob: &LogicalBlobId,
        principal: &AuthenticatedPrincipal,
        upload_id: Option<&str>,
        list_type: BlockListType,
    ) -> Result<BlockListResult, BlockError> {
        let coordinator = self.commit_service.coordinator(logical_blob)?;
        let control_token = coordinator
            .control_tokens
            .token()
            .await
            .map_err(|error| BackendError::InvalidResponse(error.to_string()))?;
        ensure_not_quarantined(
            coordinator.primary.as_ref(),
            coordinator.secondary.as_ref(),
            &logical_blob.path_hash(),
            &control_token,
            coordinator.signer.as_ref(),
        )
        .await?;
        tokio::try_join!(
            coordinator
                .primary
                .authorize_blob_read(logical_blob, &principal.access_token),
            coordinator
                .secondary
                .authorize_blob_read(logical_blob, &principal.access_token)
        )?;
        let current_head = load_current_head(&coordinator, logical_blob, &control_token).await?;
        let base_version = current_head
            .as_ref()
            .map_or(0, |head| head.payload.logical_version);
        let base_etag = current_head
            .as_ref()
            .map(|head| head.payload.logical_etag.clone());
        let effective_upload_id = upload_id.map_or_else(
            || {
                effective_upload_id(
                    "",
                    logical_blob,
                    principal,
                    base_version,
                    base_etag.as_deref(),
                )
            },
            ToOwned::to_owned,
        );
        let has_committed_blob = current_head
            .as_ref()
            .is_some_and(|head| head.payload.state == ManifestState::Committed);
        let committed = if matches!(list_type, BlockListType::Committed | BlockListType::All) {
            self.load_current_blocks(&coordinator, logical_blob, &control_token)
                .await?
                .map(|current| {
                    current
                        .blocks
                        .into_iter()
                        .filter_map(|block| {
                            block.client_block_id.map(|block_id| BlockItem {
                                block_id,
                                size: block.length,
                            })
                        })
                        .collect()
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        let uncommitted = if matches!(list_type, BlockListType::Uncommitted | BlockListType::All) {
            self.load_staged_blocks(
                &coordinator,
                logical_blob,
                Some(&effective_upload_id),
                principal,
                &control_token,
            )
            .await?
            .into_iter()
            .filter(|block| {
                block.payload.base_logical_version == base_version
                    && block.payload.base_logical_etag == base_etag
            })
            .map(|block| BlockItem {
                block_id: block.payload.block_id,
                size: block.payload.content_length,
            })
            .collect()
        } else {
            Vec::new()
        };
        if (uncommitted.is_empty() || list_type == BlockListType::Committed) && !has_committed_blob
        {
            return Err(BlockError::NotFound);
        }
        Ok(BlockListResult {
            list_type,
            committed,
            uncommitted,
        })
    }

    async fn load_current_blocks(
        &self,
        coordinator: &CommitCoordinator,
        logical_blob: &LogicalBlobId,
        control_token: &ControlToken,
    ) -> Result<Option<CurrentBlocks>, BlockError> {
        let Some(current) = load_current_head(coordinator, logical_blob, control_token).await?
        else {
            return Ok(None);
        };
        if current.payload.state == ManifestState::Tombstoned {
            return Ok(None);
        }
        Ok(Some(
            self.load_blocks_for_head(coordinator, &current.payload, control_token)
                .await?,
        ))
    }

    async fn load_blocks_for_head(
        &self,
        coordinator: &CommitCoordinator,
        head: &CommitManifest,
        control_token: &ControlToken,
    ) -> Result<CurrentBlocks, BlockError> {
        let (primary_value, secondary_value) = tokio::try_join!(
            coordinator
                .primary
                .control_get_object(&head.block_manifest_object, control_token),
            coordinator
                .secondary
                .control_get_object(&head.block_manifest_object, control_token)
        )?;
        let (Some(primary_value), Some(secondary_value)) = (primary_value, secondary_value) else {
            return Err(BlockError::VerificationFailed);
        };
        if primary_value.bytes != secondary_value.bytes
            || sha256_bytes(&primary_value.bytes) != head.block_manifest_sha256
        {
            return Err(BlockError::VerificationFailed);
        }
        let signed = SignedDocument::<BlockManifest>::from_bytes(&primary_value.bytes)
            .map_err(|_| BlockError::VerificationFailed)?;
        signed.verify(
            SignatureDomain::BlockManifest,
            &signed.payload.signing_key_id,
            coordinator.signer.as_ref(),
        )?;
        validate_block_manifest_link(head, &signed.payload)?;
        let mut blocks = Vec::with_capacity(
            usize::try_from(signed.payload.block_count)
                .map_err(|_| BlockError::VerificationFailed)?,
        );
        for reference in &signed.payload.pages {
            let (primary_page, secondary_page) = tokio::try_join!(
                coordinator
                    .primary
                    .control_get_object(&reference.object, control_token),
                coordinator
                    .secondary
                    .control_get_object(&reference.object, control_token)
            )?;
            let (Some(primary_page), Some(secondary_page)) = (primary_page, secondary_page) else {
                return Err(BlockError::VerificationFailed);
            };
            if primary_page.bytes != secondary_page.bytes
                || sha256_bytes(&primary_page.bytes) != reference.sha256
            {
                return Err(BlockError::VerificationFailed);
            }
            let page: BlockManifestPage = serde_json::from_slice(&primary_page.bytes)
                .map_err(|_| BlockError::VerificationFailed)?;
            validate_block_manifest_page(&signed.payload, reference, &page)?;
            blocks.extend(page.blocks);
        }
        Ok(CurrentBlocks {
            head: head.clone(),
            blocks,
        })
    }

    async fn recover_partial_block_list_publication(
        &self,
        coordinator: &CommitCoordinator,
        logical_blob: &LogicalBlobId,
        principal: &AuthenticatedPrincipal,
        write_id: &str,
        selections: &[BlockSelection],
        control_token: &ControlToken,
    ) -> Result<Option<CommitResult>, BlockError> {
        let head_key = format!("heads/{}.json", logical_blob.path_hash());
        let (primary, secondary) = tokio::try_join!(
            load_head(
                coordinator.primary.as_ref(),
                &head_key,
                control_token,
                coordinator.signer.as_ref()
            ),
            load_head(
                coordinator.secondary.as_ref(),
                &head_key,
                control_token,
                coordinator.signer.as_ref()
            )
        )?;
        let (committed, lagging, lagging_backend) = match (primary.as_ref(), secondary.as_ref()) {
            (Some(committed), None) if committed.signed.payload.write_id == write_id => {
                (committed, None, coordinator.secondary.as_ref())
            }
            (None, Some(committed)) if committed.signed.payload.write_id == write_id => {
                (committed, None, coordinator.primary.as_ref())
            }
            (Some(committed), Some(lagging))
                if committed.signed.payload.write_id == write_id
                    && committed.signed.payload.previous_logical_etag.as_deref()
                        == Some(&lagging.signed.payload.logical_etag)
                    && committed.signed.payload.logical_version
                        == lagging.signed.payload.logical_version.saturating_add(1) =>
            {
                (committed, Some(lagging), coordinator.secondary.as_ref())
            }
            (Some(lagging), Some(committed))
                if committed.signed.payload.write_id == write_id
                    && committed.signed.payload.previous_logical_etag.as_deref()
                        == Some(&lagging.signed.payload.logical_etag)
                    && committed.signed.payload.logical_version
                        == lagging.signed.payload.logical_version.saturating_add(1) =>
            {
                (committed, Some(lagging), coordinator.primary.as_ref())
            }
            _ => return Ok(None),
        };
        if committed.signed.payload.state != ManifestState::Committed {
            return Err(BlockError::VerificationFailed);
        }
        CommitCoordinator::validate_recovery_candidate(
            coordinator.primary.as_ref(),
            coordinator.secondary.as_ref(),
            &logical_blob.path_hash(),
            logical_blob.canonical(),
            coordinator.ring_version,
            committed,
            control_token,
            coordinator.signer.as_ref(),
        )
        .await?;
        coordinator
            .authorize_replay(principal, &committed.signed.payload)
            .await?;
        validate_committed_head(
            &committed.signed.payload,
            logical_blob,
            coordinator.ring_version,
            coordinator.primary.id(),
            coordinator.secondary.id(),
        )
        .map_err(|_| BlockError::VerificationFailed)?;
        let current = self
            .load_blocks_for_head(coordinator, &committed.signed.payload, control_token)
            .await?;
        let committed_ids = current
            .blocks
            .iter()
            .map(|block| block.client_block_id.as_deref())
            .collect::<Vec<_>>();
        let requested_ids = selections
            .iter()
            .map(|selection| Some(selection.block_id.as_str()))
            .collect::<Vec<_>>();
        if committed_ids != requested_ids {
            return Err(BlockError::Conflict);
        }
        let sidecar_key = format!(
            "{}/committed.json",
            commit_manifest_object_prefix(&committed.signed.payload)?
        );
        let (primary_sidecar, secondary_sidecar, primary_digest, secondary_digest) = tokio::try_join!(
            coordinator
                .primary
                .control_get_object(&sidecar_key, control_token),
            coordinator
                .secondary
                .control_get_object(&sidecar_key, control_token),
            coordinator.primary.caller_digest_data_object(
                &committed.signed.payload.content_container,
                &committed.signed.payload.content_object,
                &principal.access_token
            ),
            coordinator.secondary.caller_digest_data_object(
                &committed.signed.payload.content_container,
                &committed.signed.payload.content_object,
                &principal.access_token
            )
        )?;
        if primary_sidecar.as_ref().map(|value| value.bytes.as_slice())
            != Some(committed.bytes.as_slice())
            || secondary_sidecar
                .as_ref()
                .map(|value| value.bytes.as_slice())
                != Some(committed.bytes.as_slice())
            || primary_digest.as_ref() != secondary_digest.as_ref()
            || primary_digest.as_ref().is_none_or(|digest| {
                digest.length != committed.signed.payload.content_length
                    || digest.sha256 != committed.signed.payload.content_sha256
            })
        {
            return Err(BlockError::VerificationFailed);
        }
        lagging_backend
            .control_put_bytes(
                &head_key,
                committed.bytes.clone(),
                "application/json",
                head_condition(lagging),
                control_token,
            )
            .await?;
        verify_identical_objects(
            coordinator.primary.as_ref(),
            coordinator.secondary.as_ref(),
            &head_key,
            &committed.bytes,
            control_token,
        )
        .await?;
        CommitCoordinator::publish_high_water(
            coordinator.primary.as_ref(),
            coordinator.secondary.as_ref(),
            &logical_blob.path_hash(),
            &committed.signed,
            &committed.bytes,
            control_token,
            coordinator.signer.as_ref(),
        )
        .await?;
        publish_catalog_current(
            coordinator.primary.as_ref(),
            coordinator.secondary.as_ref(),
            logical_blob,
            &committed.signed,
            &committed.bytes,
            control_token,
            coordinator.signer.as_ref(),
        )
        .await?;
        Ok(Some(CommitResult {
            logical_version: committed.signed.payload.logical_version,
            logical_etag: committed.signed.payload.logical_etag.clone(),
            write_id: write_id.to_owned(),
            idempotent_replay: true,
        }))
    }

    async fn load_staged_blocks(
        &self,
        coordinator: &CommitCoordinator,
        logical_blob: &LogicalBlobId,
        upload_id: Option<&str>,
        principal: &AuthenticatedPrincipal,
        control_token: &ControlToken,
    ) -> Result<Vec<SignedDocument<StagedBlock>>, BlockError> {
        self.load_staged_blocks_internal(
            coordinator,
            logical_blob,
            upload_id,
            Some(principal),
            control_token,
            true,
        )
        .await
    }

    async fn load_staged_blocks_internal(
        &self,
        coordinator: &CommitCoordinator,
        logical_blob: &LogicalBlobId,
        upload_id: Option<&str>,
        principal: Option<&AuthenticatedPrincipal>,
        control_token: &ControlToken,
        require_content: bool,
    ) -> Result<Vec<SignedDocument<StagedBlock>>, BlockError> {
        let prefix = staged_metadata_prefix(logical_blob, upload_id);
        let (primary_keys, secondary_keys) = tokio::try_join!(
            list_staged_keys_bounded(coordinator.primary.as_ref(), &prefix, control_token),
            list_staged_keys_bounded(coordinator.secondary.as_ref(), &prefix, control_token)
        )?;
        let keys = primary_keys
            .into_iter()
            .chain(secondary_keys)
            .collect::<BTreeSet<_>>();
        if keys.len() > MAX_BLOCK_COUNT {
            return Err(BlockError::BlockCountExceedsLimit);
        }
        let mut blocks = Vec::with_capacity(keys.len());
        for key in keys {
            let (primary, secondary) = tokio::try_join!(
                coordinator.primary.control_get_object(&key, control_token),
                coordinator
                    .secondary
                    .control_get_object(&key, control_token)
            )?;
            let (Some(primary), Some(secondary)) = (primary, secondary) else {
                return Err(BlockError::VerificationFailed);
            };
            if primary.bytes != secondary.bytes {
                return Err(BlockError::VerificationFailed);
            }
            let block = validate_staged_document(
                &primary.bytes,
                logical_blob,
                upload_id.unwrap_or_default(),
                coordinator.ring_version,
                coordinator.primary.id(),
                coordinator.secondary.id(),
                coordinator.signer.as_ref(),
            )?;
            if upload_id.is_some_and(|expected| block.payload.upload_id != expected) {
                return Err(BlockError::VerificationFailed);
            }
            if key
                != staged_metadata_key(
                    logical_blob,
                    &block.payload.upload_id,
                    &block.payload.block_id,
                )
            {
                return Err(BlockError::VerificationFailed);
            }
            if now_unix_ms() > block.payload.expires_at_unix_ms {
                continue;
            }
            if let Some(principal) = principal
                && block.payload.caller != principal.identity()
            {
                return Err(BlockError::VerificationFailed);
            }
            if require_content {
                let principal = principal.ok_or(BlockError::VerificationFailed)?;
                self.verify_staged_content(coordinator, &block.payload, principal)
                    .await?;
            }
            blocks.push(block);
        }
        blocks.sort_by(|left, right| left.payload.block_id.cmp(&right.payload.block_id));
        Ok(blocks)
    }

    async fn verify_staged_content(
        &self,
        coordinator: &CommitCoordinator,
        staged: &StagedBlock,
        principal: &AuthenticatedPrincipal,
    ) -> Result<(), BlockError> {
        self.read_staged_bytes(coordinator, staged, principal)
            .await
            .map(|_| ())
    }

    async fn read_staged_bytes(
        &self,
        coordinator: &CommitCoordinator,
        staged: &StagedBlock,
        principal: &AuthenticatedPrincipal,
    ) -> Result<Vec<u8>, BlockError> {
        let (primary, secondary) = tokio::try_join!(
            coordinator.primary.caller_get_data_range(
                &staged.content_container,
                &staged.content_object,
                None,
                &principal.access_token
            ),
            coordinator.secondary.caller_get_data_range(
                &staged.content_container,
                &staged.content_object,
                None,
                &principal.access_token
            )
        )?;
        let (Some(primary), Some(secondary)) = (primary, secondary) else {
            return Err(BlockError::VerificationFailed);
        };
        if primary != secondary
            || u64::try_from(primary.len()).map_err(|_| BlockError::VerificationFailed)?
                != staged.content_length
            || sha256_bytes(&primary) != staged.content_sha256
        {
            return Err(BlockError::VerificationFailed);
        }
        Ok(primary)
    }
}

impl BlockListResult {
    pub fn to_xml(&self) -> String {
        let mut xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList>".to_owned();
        if matches!(
            self.list_type,
            BlockListType::Committed | BlockListType::All
        ) {
            xml.push_str("<CommittedBlocks>");
            append_block_items(&mut xml, &self.committed);
            xml.push_str("</CommittedBlocks>");
        }
        if matches!(
            self.list_type,
            BlockListType::Uncommitted | BlockListType::All
        ) {
            xml.push_str("<UncommittedBlocks>");
            append_block_items(&mut xml, &self.uncommitted);
            xml.push_str("</UncommittedBlocks>");
        }
        xml.push_str("</BlockList>");
        xml
    }
}

async fn list_staged_keys_bounded(
    backend: &dyn crate::backend::ReplicaBackend,
    prefix: &str,
    control_token: &ControlToken,
) -> Result<Vec<String>, BlockError> {
    let mut cursor = None;
    let mut keys = Vec::new();
    loop {
        let remaining = MAX_BLOCK_COUNT.saturating_add(1).saturating_sub(keys.len());
        if remaining == 0 {
            return Err(BlockError::BlockCountExceedsLimit);
        }
        let page = backend
            .control_list_objects_page(
                prefix,
                cursor.as_deref(),
                remaining.min(5_000),
                control_token,
            )
            .await?;
        if page.next_cursor.is_some() && page.next_cursor.as_ref() == cursor.as_ref() {
            return Err(BlockError::VerificationFailed);
        }
        keys.extend(page.objects);
        if keys.len() > MAX_BLOCK_COUNT {
            return Err(BlockError::BlockCountExceedsLimit);
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            return Ok(keys);
        }
    }
}

pub fn parse_block_list_xml(bytes: &[u8]) -> Result<Vec<BlockSelection>, BlockError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| BlockError::InvalidBlockList("XML must be UTF-8".to_owned()))?;
    let parsed: BlockListXml = from_str(text)
        .map_err(|error| BlockError::InvalidBlockList(format!("XML parsing failed: {error}")))?;
    if parsed.entries.is_empty() || parsed.entries.len() > MAX_BLOCK_COUNT {
        return Err(BlockError::BlockCountExceedsLimit);
    }
    parsed
        .entries
        .into_iter()
        .map(|entry| {
            let (kind, block_id) = match entry {
                BlockListXmlEntry::Latest(value) => (BlockSelectionKind::Latest, value),
                BlockListXmlEntry::Committed(value) => (BlockSelectionKind::Committed, value),
                BlockListXmlEntry::Uncommitted(value) => (BlockSelectionKind::Uncommitted, value),
            };
            decode_block_id(&block_id)?;
            Ok(BlockSelection { kind, block_id })
        })
        .collect()
}

struct CurrentHead {
    loaded: crate::commit::LoadedHead,
    payload: CommitManifest,
}

async fn load_current_head(
    coordinator: &CommitCoordinator,
    logical_blob: &LogicalBlobId,
    control_token: &ControlToken,
) -> Result<Option<CurrentHead>, BlockError> {
    let path_hash = logical_blob.path_hash();
    let head_key = format!("heads/{path_hash}.json");
    let high_water_key = format!("high-water/{path_hash}/current.json");
    let (primary_head, secondary_head, primary_high, secondary_high) = tokio::try_join!(
        load_head(
            coordinator.primary.as_ref(),
            &head_key,
            control_token,
            coordinator.signer.as_ref()
        ),
        load_head(
            coordinator.secondary.as_ref(),
            &head_key,
            control_token,
            coordinator.signer.as_ref()
        ),
        load_head(
            coordinator.primary.as_ref(),
            &high_water_key,
            control_token,
            coordinator.signer.as_ref()
        ),
        load_head(
            coordinator.secondary.as_ref(),
            &high_water_key,
            control_token,
            coordinator.signer.as_ref()
        )
    )?;
    let current = strict_current_head(primary_head.as_ref(), secondary_head.as_ref())?;
    let high = strict_current_head(primary_high.as_ref(), secondary_high.as_ref())?;
    match (current, high) {
        (None, None) => Ok(None),
        (Some(current), Some(high)) if current.bytes == high.bytes => {
            if current.signed.payload.state == ManifestState::Committed {
                validate_committed_head(
                    &current.signed.payload,
                    logical_blob,
                    coordinator.ring_version,
                    coordinator.primary.id(),
                    coordinator.secondary.id(),
                )
                .map_err(|_| BlockError::VerificationFailed)?;
            }
            Ok(Some(CurrentHead {
                loaded: crate::commit::LoadedHead {
                    signed: current.signed.clone(),
                    bytes: current.bytes.clone(),
                    backend_etag: current.backend_etag.clone(),
                },
                payload: current.signed.payload.clone(),
            }))
        }
        _ => Err(BlockError::VerificationFailed),
    }
}

async fn load_head_pair(
    coordinator: &CommitCoordinator,
    logical_blob: &LogicalBlobId,
    control_token: &ControlToken,
) -> Result<Option<CurrentHead>, BlockError> {
    let path_hash = logical_blob.path_hash();
    let head_key = format!("heads/{path_hash}.json");
    let (primary_head, secondary_head) = tokio::try_join!(
        load_head(
            coordinator.primary.as_ref(),
            &head_key,
            control_token,
            coordinator.signer.as_ref()
        ),
        load_head(
            coordinator.secondary.as_ref(),
            &head_key,
            control_token,
            coordinator.signer.as_ref()
        )
    )?;
    let Some(current) = strict_current_head(primary_head.as_ref(), secondary_head.as_ref())? else {
        return Ok(None);
    };
    if current.signed.payload.state == ManifestState::Committed {
        validate_committed_head(
            &current.signed.payload,
            logical_blob,
            coordinator.ring_version,
            coordinator.primary.id(),
            coordinator.secondary.id(),
        )
        .map_err(|_| BlockError::VerificationFailed)?;
    }
    Ok(Some(CurrentHead {
        loaded: crate::commit::LoadedHead {
            signed: current.signed.clone(),
            bytes: current.bytes.clone(),
            backend_etag: current.backend_etag.clone(),
        },
        payload: current.signed.payload.clone(),
    }))
}

async fn read_committed_block(
    coordinator: &CommitCoordinator,
    head: &CommitManifest,
    block: &BlockDescriptor,
    principal: &AuthenticatedPrincipal,
) -> Result<Vec<u8>, BlockError> {
    let range = if block.length == 0 {
        None
    } else {
        Some((
            block.offset,
            block
                .offset
                .checked_add(block.length - 1)
                .ok_or(BlockError::VerificationFailed)?,
        ))
    };
    let (primary, secondary) = tokio::try_join!(
        coordinator.primary.caller_get_data_range(
            &head.content_container,
            &head.content_object,
            range,
            &principal.access_token
        ),
        coordinator.secondary.caller_get_data_range(
            &head.content_container,
            &head.content_object,
            range,
            &principal.access_token
        )
    )?;
    let (Some(primary), Some(secondary)) = (primary, secondary) else {
        return Err(BlockError::VerificationFailed);
    };
    if primary != secondary
        || u64::try_from(primary.len()).map_err(|_| BlockError::VerificationFailed)? != block.length
        || sha256_bytes(&primary) != block.sha256
    {
        return Err(BlockError::VerificationFailed);
    }
    Ok(primary)
}

fn validate_staged_document(
    bytes: &[u8],
    logical_blob: &LogicalBlobId,
    upload_id: &str,
    ring_version: u64,
    primary_id: &str,
    secondary_id: &str,
    signer: &dyn ManifestSigner,
) -> Result<SignedDocument<StagedBlock>, BlockError> {
    let signed = SignedDocument::<StagedBlock>::from_bytes(bytes)
        .map_err(|_| BlockError::VerificationFailed)?;
    if signed.canonical_bytes()? != bytes {
        return Err(BlockError::VerificationFailed);
    }
    signed.verify(
        SignatureDomain::StagedBlock,
        &signed.payload.signing_key_id,
        signer,
    )?;
    let decoded = decode_block_id(&signed.payload.block_id)?;
    let expected_prefix = format!(
        ".overmesh/staged/{}/{}/",
        logical_blob.path_hash(),
        stable_component(&signed.payload.upload_id)
    );
    if signed.payload.api_version != STAGED_BLOCK_API_VERSION
        || signed.payload.blob != logical_blob.canonical()
        || (!upload_id.is_empty() && signed.payload.upload_id != upload_id)
        || signed.payload.write_id.is_empty()
        || signed.payload.decoded_block_id_length
            != u32::try_from(decoded.len()).map_err(|_| BlockError::InvalidBlockId)?
        || signed.payload.block_id_sha256 != sha256_bytes(&decoded)
        || signed.payload.content_container != logical_blob.container()
        || !signed.payload.content_object.starts_with(&expected_prefix)
        || signed.payload.content_length > MAX_BLOCK_SIZE
        || signed.payload.prepared_replicas != [primary_id, secondary_id]
        || signed.payload.ring_version != ring_version
        || signed.payload.created_at_unix_ms > signed.payload.expires_at_unix_ms
        || signed.signed_at_unix_ms < signed.payload.created_at_unix_ms
    {
        return Err(BlockError::VerificationFailed);
    }
    Ok(signed)
}

#[allow(clippy::too_many_arguments)]
fn validate_upload_generation(
    bytes: &[u8],
    logical_blob: &LogicalBlobId,
    upload_id: &str,
    principal: &AuthenticatedPrincipal,
    decoded_block_id_length: u32,
    base_logical_version: u64,
    base_logical_etag: Option<&str>,
    coordinator: &CommitCoordinator,
) -> Result<SignedDocument<UploadGeneration>, BlockError> {
    let signed = SignedDocument::<UploadGeneration>::from_bytes(bytes)
        .map_err(|_| BlockError::VerificationFailed)?;
    if signed.canonical_bytes()? != bytes {
        return Err(BlockError::VerificationFailed);
    }
    signed.verify(
        SignatureDomain::UploadGeneration,
        &signed.payload.signing_key_id,
        coordinator.signer.as_ref(),
    )?;
    if signed.payload.decoded_block_id_length != decoded_block_id_length {
        return Err(BlockError::UnequalBlockIdLength);
    }
    if signed.payload.api_version != UPLOAD_GENERATION_API_VERSION
        || signed.payload.blob != logical_blob.canonical()
        || signed.payload.upload_id != upload_id
        || signed.payload.ring_version != coordinator.ring_version
        || signed.payload.prepared_replicas
            != [coordinator.primary.id(), coordinator.secondary.id()]
        || signed.payload.created_at_unix_ms > signed.payload.expires_at_unix_ms
        || signed.signed_at_unix_ms < signed.payload.created_at_unix_ms
        || (signed.payload.base_logical_version > 0) != signed.payload.base_logical_etag.is_some()
    {
        return Err(BlockError::VerificationFailed);
    }
    if signed.payload.caller != principal.identity()
        || signed.payload.base_logical_version != base_logical_version
        || signed.payload.base_logical_etag.as_deref() != base_logical_etag
    {
        return Err(BlockError::Conflict);
    }
    Ok(signed)
}

fn decode_block_id(value: &str) -> Result<Vec<u8>, BlockError> {
    if value.is_empty() || value.len() > 4 * MAX_BLOCK_ID_LENGTH.div_ceil(3) + 4 {
        return Err(BlockError::InvalidBlockId);
    }
    let decoded = STANDARD
        .decode(value)
        .map_err(|_| BlockError::InvalidBlockId)?;
    if decoded.is_empty()
        || decoded.len() > MAX_BLOCK_ID_LENGTH
        || STANDARD.encode(&decoded) != value
    {
        return Err(BlockError::InvalidBlockId);
    }
    Ok(decoded)
}

fn effective_upload_id(
    requested: &str,
    logical_blob: &LogicalBlobId,
    principal: &AuthenticatedPrincipal,
    base_logical_version: u64,
    base_logical_etag: Option<&str>,
) -> String {
    if !requested.is_empty() {
        return requested.to_owned();
    }
    let identity = principal.identity();
    format!(
        "implicit-{}",
        stable_component(&format!(
            "{}\0{}\0{}\0{}\0{}\0{}",
            logical_blob.canonical(),
            identity.tenant_id,
            identity.object_id,
            identity.authorized_party.as_deref().unwrap_or_default(),
            base_logical_version,
            base_logical_etag.unwrap_or_default()
        ))
    )
}

fn staged_metadata_prefix(logical_blob: &LogicalBlobId, upload_id: Option<&str>) -> String {
    match upload_id {
        Some(upload_id) => format!(
            "staged-blocks/{}/{}/",
            logical_blob.path_hash(),
            stable_component(upload_id)
        ),
        None => format!("staged-blocks/{}/", logical_blob.path_hash()),
    }
}

fn upload_generation_key(logical_blob: &LogicalBlobId, upload_id: &str) -> String {
    format!(
        "staged-uploads/{}/{}/generation.json",
        logical_blob.path_hash(),
        stable_component(upload_id)
    )
}

fn staged_metadata_key(logical_blob: &LogicalBlobId, upload_id: &str, block_id: &str) -> String {
    format!(
        "{}{}.json",
        staged_metadata_prefix(logical_blob, Some(upload_id)),
        stable_component(block_id)
    )
}

fn append_block_items(xml: &mut String, blocks: &[BlockItem]) {
    for block in blocks {
        xml.push_str("<Block><Name>");
        xml.push_str(&escape_xml(&block.block_id));
        xml.push_str("</Name><Size>");
        xml.push_str(&block.size.to_string());
        xml.push_str("</Size></Block>");
    }
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ordered_azure_block_list() {
        let xml = br#"<?xml version="1.0" encoding="utf-8"?><BlockList><Latest>YQ==</Latest><Committed>YmI=</Committed><Uncommitted>Y2Nj</Uncommitted></BlockList>"#;
        let blocks = parse_block_list_xml(xml).expect("block list");
        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks[0].kind, BlockSelectionKind::Latest);
        assert_eq!(blocks[1].kind, BlockSelectionKind::Committed);
        assert_eq!(blocks[2].kind, BlockSelectionKind::Uncommitted);
    }

    #[test]
    fn rejects_noncanonical_or_oversized_block_ids() {
        assert!(decode_block_id("not base64").is_err());
        assert!(decode_block_id("").is_err());
        let oversized = STANDARD.encode(vec![0_u8; MAX_BLOCK_ID_LENGTH + 1]);
        assert!(decode_block_id(&oversized).is_err());
    }

    #[test]
    fn serializes_the_requested_empty_block_list_sections() {
        let committed = BlockListResult {
            list_type: BlockListType::Committed,
            committed: Vec::new(),
            uncommitted: Vec::new(),
        }
        .to_xml();
        assert!(committed.contains("<CommittedBlocks></CommittedBlocks>"));
        assert!(!committed.contains("<UncommittedBlocks>"));

        let uncommitted = BlockListResult {
            list_type: BlockListType::Uncommitted,
            committed: Vec::new(),
            uncommitted: Vec::new(),
        }
        .to_xml();
        assert!(uncommitted.contains("<UncommittedBlocks></UncommittedBlocks>"));
        assert!(!uncommitted.contains("<CommittedBlocks>"));

        let all = BlockListResult {
            list_type: BlockListType::All,
            committed: Vec::new(),
            uncommitted: Vec::new(),
        }
        .to_xml();
        assert!(all.contains("<CommittedBlocks></CommittedBlocks>"));
        assert!(all.contains("<UncommittedBlocks></UncommittedBlocks>"));
    }
}
