use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use axum::body::Body;
use bytes::Bytes;
use futures_util::stream;
use thiserror::Error;

use crate::{
    SignedRing,
    auth::AuthenticatedPrincipal,
    backend::{BackendError, SharedBackend},
    commit::{
        CommitCoordinator, CommitError, ensure_not_quarantined, load_head, strict_current_head,
    },
    identity::{CallerToken, ControlToken, SharedControlTokenProvider},
    manifest::{
        BlockDescriptor, BlockManifest, BlockManifestPage, BlockManifestPageReference,
        CommitManifest, ManifestError, ManifestSigner, ManifestState, SignatureDomain,
        SignedDocument, logical_etag, sha256_bytes, validate_block_manifest_link,
        validate_block_manifest_page,
    },
    resource::{LogicalBlobId, stable_component},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobMetadata {
    pub logical_etag: String,
    pub logical_version: u64,
    pub write_id: String,
    pub ring_version: u64,
    pub content_length: u64,
    pub content_sha256: String,
    pub committed_at_unix_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedRange {
    pub start: u64,
    pub end: u64,
    pub total_length: u64,
}

impl ResolvedRange {
    pub fn length(self) -> u64 {
        self.end - self.start + 1
    }
}

pub struct BlobRead {
    pub metadata: BlobMetadata,
    pub range: Option<ResolvedRange>,
    pub body: Body,
}

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("blob not found")]
    NotFound,
    #[error("blob is quarantined")]
    Quarantined,
    #[error("replica metadata does not have one strict committed value")]
    ReplicaDrift,
    #[error("read metadata or content validation failed")]
    VerificationFailed,
    #[error("requested byte range is invalid")]
    InvalidRange { content_length: u64 },
    #[error("backend read failed: {0}")]
    Backend(#[from] BackendError),
    #[error("manifest validation failed: {0}")]
    Manifest(#[from] ManifestError),
    #[error("manifest serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Clone)]
pub struct ReadService {
    ring: Arc<SignedRing>,
    backends: HashMap<String, SharedBackend>,
    signer: Arc<dyn ManifestSigner>,
    control_tokens: SharedControlTokenProvider,
}

struct PreparedCommon {
    primary: SharedBackend,
    secondary: SharedBackend,
    control_token: ControlToken,
    caller_token: CallerToken,
    metadata: BlobMetadata,
    head: CommitManifest,
}

struct PreparedRead {
    common: PreparedCommon,
    block_manifest: BlockManifest,
    pages: VecDeque<BlockManifestPageReference>,
    requested_start: u64,
    requested_end: u64,
}

struct PlannedBlock {
    descriptor: BlockDescriptor,
    slice_start: u64,
    slice_end: u64,
}

struct ReadStreamState {
    primary: SharedBackend,
    secondary: SharedBackend,
    control_token: ControlToken,
    caller_token: CallerToken,
    content_container: String,
    content_object: String,
    block_manifest: BlockManifest,
    pages: VecDeque<BlockManifestPageReference>,
    blocks: VecDeque<PlannedBlock>,
    requested_start: u64,
    requested_end: u64,
    content_length: u64,
}

impl ReadService {
    pub fn new(
        ring: Arc<SignedRing>,
        backends: HashMap<String, SharedBackend>,
        signer: Arc<dyn ManifestSigner>,
        control_tokens: SharedControlTokenProvider,
    ) -> Self {
        Self {
            ring,
            backends,
            signer,
            control_tokens,
        }
    }

    pub async fn head_blob(
        &self,
        logical_blob: &LogicalBlobId,
        principal: &AuthenticatedPrincipal,
    ) -> Result<BlobMetadata, ReadError> {
        let prepared = self.prepare_common(logical_blob, principal).await?;
        self.validate_content_properties(&prepared).await?;
        Ok(prepared.metadata)
    }

    pub async fn get_blob(
        &self,
        logical_blob: &LogicalBlobId,
        principal: &AuthenticatedPrincipal,
        range_header: Option<&str>,
    ) -> Result<BlobRead, ReadError> {
        let common = self.prepare_common(logical_blob, principal).await?;
        let range = range_header
            .map(|value| resolve_range(value, common.metadata.content_length))
            .transpose()?;
        let requested_start = range.map_or(0, |value| value.start);
        let requested_end = range.map_or_else(
            || common.metadata.content_length.saturating_sub(1),
            |value| value.end,
        );
        let ((), block_manifest) = tokio::try_join!(
            self.validate_content_properties(&common),
            self.load_block_manifest(&common)
        )?;
        let pages = block_manifest
            .pages
            .iter()
            .filter(|reference| {
                if reference.content_length == 0 {
                    return common.metadata.content_length == 0;
                }
                let page_end = reference.first_offset + reference.content_length - 1;
                page_end >= requested_start && reference.first_offset <= requested_end
            })
            .cloned()
            .collect::<VecDeque<_>>();
        let prepared = PreparedRead {
            common,
            block_manifest,
            pages,
            requested_start,
            requested_end,
        };
        let state = ReadStreamState {
            primary: prepared.common.primary,
            secondary: prepared.common.secondary,
            control_token: prepared.common.control_token,
            caller_token: prepared.common.caller_token,
            content_container: prepared.common.head.content_container,
            content_object: prepared.common.head.content_object,
            block_manifest: prepared.block_manifest,
            pages: prepared.pages,
            blocks: VecDeque::new(),
            requested_start: prepared.requested_start,
            requested_end: prepared.requested_end,
            content_length: prepared.common.metadata.content_length,
        };
        let body = Body::from_stream(stream::try_unfold(state, |mut state| async move {
            loop {
                if let Some(block) = state.blocks.pop_front() {
                    let bytes = read_validated_block(&state, &block.descriptor).await?;
                    let start = usize::try_from(block.slice_start)
                        .map_err(|_| ReadError::VerificationFailed)?;
                    let end = usize::try_from(block.slice_end)
                        .map_err(|_| ReadError::VerificationFailed)?;
                    let output = Bytes::copy_from_slice(
                        bytes.get(start..end).ok_or(ReadError::VerificationFailed)?,
                    );
                    return Ok::<_, ReadError>(Some((output, state)));
                }
                let Some(reference) = state.pages.pop_front() else {
                    return Ok(None);
                };
                let page = load_validated_block_page(&state, &reference).await?;
                state
                    .blocks
                    .extend(page.blocks.into_iter().filter_map(|descriptor| {
                        if descriptor.length == 0 {
                            return (state.content_length == 0).then_some(PlannedBlock {
                                descriptor,
                                slice_start: 0,
                                slice_end: 0,
                            });
                        }
                        let block_end = descriptor.offset + descriptor.length - 1;
                        if block_end < state.requested_start
                            || descriptor.offset > state.requested_end
                        {
                            return None;
                        }
                        Some(PlannedBlock {
                            slice_start: state.requested_start.saturating_sub(descriptor.offset),
                            slice_end: (state.requested_end.min(block_end) - descriptor.offset) + 1,
                            descriptor,
                        })
                    }));
            }
        }));
        Ok(BlobRead {
            metadata: prepared.common.metadata,
            range,
            body,
        })
    }

    async fn prepare_common(
        &self,
        logical_blob: &LogicalBlobId,
        principal: &AuthenticatedPrincipal,
    ) -> Result<PreparedCommon, ReadError> {
        let control_token = self
            .control_tokens
            .token()
            .await
            .map_err(|error| BackendError::InvalidResponse(error.to_string()))?;
        let replicas = self
            .ring
            .replicas_for(logical_blob)
            .map_err(|_| ReadError::ReplicaDrift)?;
        let primary = self
            .backends
            .get(&replicas[0].id)
            .cloned()
            .ok_or(ReadError::ReplicaDrift)?;
        let secondary = self
            .backends
            .get(&replicas[1].id)
            .cloned()
            .ok_or(ReadError::ReplicaDrift)?;
        let path_hash = logical_blob.path_hash();
        let head_key = format!("heads/{path_hash}.json");
        let high_water_key = format!("high-water/{path_hash}/current.json");
        let (_, (primary_head, secondary_head), compaction, (primary_high, secondary_high)) = tokio::try_join!(
            async {
                map_quarantine(
                    ensure_not_quarantined(
                        primary.as_ref(),
                        secondary.as_ref(),
                        &path_hash,
                        &control_token,
                        self.signer.as_ref(),
                    )
                    .await,
                )
            },
            async {
                tokio::try_join!(
                    load_head(
                        primary.as_ref(),
                        &head_key,
                        &control_token,
                        self.signer.as_ref()
                    ),
                    load_head(
                        secondary.as_ref(),
                        &head_key,
                        &control_token,
                        self.signer.as_ref()
                    )
                )
                .map_err(map_commit_error)
            },
            async {
                CommitCoordinator::strict_compaction_checkpoint(
                    primary.as_ref(),
                    secondary.as_ref(),
                    &path_hash,
                    logical_blob.canonical(),
                    self.ring.ring_version,
                    &control_token,
                    self.signer.as_ref(),
                )
                .await
                .map_err(map_commit_error)
            },
            async {
                tokio::try_join!(
                    load_head(
                        primary.as_ref(),
                        &high_water_key,
                        &control_token,
                        self.signer.as_ref()
                    ),
                    load_head(
                        secondary.as_ref(),
                        &high_water_key,
                        &control_token,
                        self.signer.as_ref()
                    )
                )
                .map_err(map_commit_error)
            }
        )?;
        let head = strict_current_head(primary_head.as_ref(), secondary_head.as_ref())
            .map_err(map_commit_error)?
            .ok_or(ReadError::NotFound)?;
        if head.signed.payload.state == ManifestState::Tombstoned {
            return Err(ReadError::NotFound);
        }
        validate_committed_head(
            &head.signed.payload,
            logical_blob,
            self.ring.ring_version,
            primary.id(),
            secondary.id(),
        )?;
        let high_water = strict_current_head(primary_high.as_ref(), secondary_high.as_ref())
            .map_err(map_commit_error)?
            .ok_or(ReadError::VerificationFailed)?;
        if high_water.bytes != head.bytes {
            return Err(ReadError::VerificationFailed);
        }
        if compaction.as_ref().is_some_and(|checkpoint| {
            head.signed.payload.logical_version
                <= checkpoint.signed.payload.compacted_through_logical_version
                || head.signed.payload.logical_version
                    < checkpoint
                        .signed
                        .payload
                        .garbage_collection_history_head_logical_version
        }) {
            return Err(ReadError::VerificationFailed);
        }
        let head = head.signed.payload.clone();
        Ok(PreparedCommon {
            primary,
            secondary,
            control_token,
            caller_token: principal.access_token.clone(),
            metadata: BlobMetadata {
                logical_etag: head.logical_etag.clone(),
                logical_version: head.logical_version,
                write_id: head.write_id.clone(),
                ring_version: head.ring_version,
                content_length: head.content_length,
                content_sha256: head.content_sha256.clone(),
                committed_at_unix_ms: head.committed_at_unix_ms,
            },
            head,
        })
    }

    async fn validate_content_properties(
        &self,
        prepared: &PreparedCommon,
    ) -> Result<(), ReadError> {
        let (primary_properties, secondary_properties) = tokio::try_join!(
            prepared.primary.caller_head_data_object(
                &prepared.head.content_container,
                &prepared.head.content_object,
                &prepared.caller_token
            ),
            prepared.secondary.caller_head_data_object(
                &prepared.head.content_container,
                &prepared.head.content_object,
                &prepared.caller_token
            )
        )?;
        if !matches!(
            (primary_properties, secondary_properties),
            (Some(primary), Some(secondary))
                if primary.length == prepared.head.content_length
                    && secondary.length == prepared.head.content_length
        ) {
            return Err(ReadError::VerificationFailed);
        }
        Ok(())
    }

    async fn load_block_manifest(
        &self,
        prepared: &PreparedCommon,
    ) -> Result<BlockManifest, ReadError> {
        let (primary_block, secondary_block) = tokio::try_join!(
            prepared.primary.control_get_object(
                &prepared.head.block_manifest_object,
                &prepared.control_token
            ),
            prepared.secondary.control_get_object(
                &prepared.head.block_manifest_object,
                &prepared.control_token
            )
        )?;
        let (Some(primary_block), Some(secondary_block)) = (primary_block, secondary_block) else {
            return Err(ReadError::VerificationFailed);
        };
        if primary_block.bytes != secondary_block.bytes
            || sha256_bytes(&primary_block.bytes) != prepared.head.block_manifest_sha256
        {
            return Err(ReadError::VerificationFailed);
        }
        let signed = SignedDocument::<BlockManifest>::from_bytes(&primary_block.bytes)?;
        signed.verify(
            SignatureDomain::BlockManifest,
            &signed.payload.signing_key_id,
            self.signer.as_ref(),
        )?;
        validate_block_manifest_link(&prepared.head, &signed.payload)?;
        Ok(signed.payload)
    }
}

pub(crate) fn validate_committed_head(
    head: &CommitManifest,
    logical_blob: &LogicalBlobId,
    ring_version: u64,
    primary_id: &str,
    secondary_id: &str,
) -> Result<(), ReadError> {
    let digest = head
        .content_sha256
        .strip_prefix("sha256:")
        .ok_or(ReadError::VerificationFailed)?;
    let expected_block_object = format!(
        "objects/{}/versions/{}/{digest}/block-manifest.json",
        logical_blob.path_hash(),
        stable_component(&head.write_id)
    );
    let expected_content_prefix = format!(".overmesh/objects/{}/", logical_blob.path_hash());
    if head.state != ManifestState::Committed
        || head.blob != logical_blob.canonical()
        || head.ring_version != ring_version
        || head.content_container != logical_blob.container()
        || !head.content_object.starts_with(&expected_content_prefix)
        || head.block_manifest_object != expected_block_object
        || head.prepared_replicas != [primary_id, secondary_id]
        || head.logical_etag
            != logical_etag(
                logical_blob.canonical(),
                head.logical_version,
                &head.write_id,
                &head.content_sha256,
            )
        || !valid_sha256(&head.content_sha256)
        || !valid_sha256(&head.block_manifest_sha256)
    {
        return Err(ReadError::VerificationFailed);
    }
    Ok(())
}

async fn read_validated_block(
    state: &ReadStreamState,
    descriptor: &BlockDescriptor,
) -> Result<Vec<u8>, ReadError> {
    let range = if descriptor.length == 0 {
        None
    } else {
        Some((
            descriptor.offset,
            descriptor
                .offset
                .checked_add(descriptor.length - 1)
                .ok_or(ReadError::VerificationFailed)?,
        ))
    };
    match state
        .primary
        .caller_get_data_range(
            &state.content_container,
            &state.content_object,
            range,
            &state.caller_token,
        )
        .await
    {
        Ok(Some(bytes)) => validate_block_bytes(descriptor, bytes),
        Ok(None) => Err(ReadError::VerificationFailed),
        Err(error) if error.is_unavailable() => {
            let bytes = state
                .secondary
                .caller_get_data_range(
                    &state.content_container,
                    &state.content_object,
                    range,
                    &state.caller_token,
                )
                .await?
                .ok_or(ReadError::VerificationFailed)?;
            validate_block_bytes(descriptor, bytes)
        }
        Err(error) => Err(ReadError::Backend(error)),
    }
}

async fn load_validated_block_page(
    state: &ReadStreamState,
    reference: &BlockManifestPageReference,
) -> Result<BlockManifestPage, ReadError> {
    let (primary, secondary) = tokio::try_join!(
        state
            .primary
            .control_get_object(&reference.object, &state.control_token),
        state
            .secondary
            .control_get_object(&reference.object, &state.control_token)
    )?;
    let (Some(primary), Some(secondary)) = (primary, secondary) else {
        return Err(ReadError::VerificationFailed);
    };
    if primary.bytes != secondary.bytes || sha256_bytes(&primary.bytes) != reference.sha256 {
        return Err(ReadError::VerificationFailed);
    }
    let page: BlockManifestPage = serde_json::from_slice(&primary.bytes)?;
    validate_block_manifest_page(&state.block_manifest, reference, &page)?;
    Ok(page)
}

fn validate_block_bytes(
    descriptor: &BlockDescriptor,
    bytes: Vec<u8>,
) -> Result<Vec<u8>, ReadError> {
    if u64::try_from(bytes.len()).map_err(|_| ReadError::VerificationFailed)? != descriptor.length
        || sha256_bytes(&bytes) != descriptor.sha256
    {
        return Err(ReadError::VerificationFailed);
    }
    Ok(bytes)
}

pub fn resolve_range(value: &str, content_length: u64) -> Result<ResolvedRange, ReadError> {
    let value = value
        .strip_prefix("bytes=")
        .ok_or(ReadError::InvalidRange { content_length })?;
    if value.contains(',') || value.is_empty() || content_length == 0 {
        return Err(ReadError::InvalidRange { content_length });
    }
    let (start, end) = value
        .split_once('-')
        .ok_or(ReadError::InvalidRange { content_length })?;
    let (start, end) = if start.is_empty() {
        let suffix = end
            .parse::<u64>()
            .map_err(|_| ReadError::InvalidRange { content_length })?;
        if suffix == 0 {
            return Err(ReadError::InvalidRange { content_length });
        }
        let length = suffix.min(content_length);
        (content_length - length, content_length - 1)
    } else {
        let start = start
            .parse::<u64>()
            .map_err(|_| ReadError::InvalidRange { content_length })?;
        if start >= content_length {
            return Err(ReadError::InvalidRange { content_length });
        }
        let end = if end.is_empty() {
            content_length - 1
        } else {
            end.parse::<u64>()
                .map_err(|_| ReadError::InvalidRange { content_length })?
                .min(content_length - 1)
        };
        if end < start {
            return Err(ReadError::InvalidRange { content_length });
        }
        (start, end)
    };
    Ok(ResolvedRange {
        start,
        end,
        total_length: content_length,
    })
}

fn valid_sha256(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

fn map_quarantine(result: Result<(), CommitError>) -> Result<(), ReadError> {
    result.map_err(map_commit_error)
}

fn map_commit_error(error: CommitError) -> ReadError {
    match error {
        CommitError::Backend(error) => ReadError::Backend(error),
        CommitError::Manifest(error) => ReadError::Manifest(error),
        CommitError::Serialization(error) => ReadError::Serialization(error),
        CommitError::Quarantined => ReadError::Quarantined,
        CommitError::ReplicaDrift => ReadError::ReplicaDrift,
        _ => ReadError::VerificationFailed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_closed_open_and_suffix_ranges() {
        assert_eq!(
            resolve_range("bytes=2-5", 10).expect("closed"),
            ResolvedRange {
                start: 2,
                end: 5,
                total_length: 10
            }
        );
        assert_eq!(
            resolve_range("bytes=7-", 10).expect("open"),
            ResolvedRange {
                start: 7,
                end: 9,
                total_length: 10
            }
        );
        assert_eq!(
            resolve_range("bytes=-3", 10).expect("suffix"),
            ResolvedRange {
                start: 7,
                end: 9,
                total_length: 10
            }
        );
    }

    #[test]
    fn rejects_unsatisfiable_or_multiple_ranges() {
        assert!(matches!(
            resolve_range("bytes=10-", 10),
            Err(ReadError::InvalidRange { content_length: 10 })
        ));
        assert!(matches!(
            resolve_range("bytes=0-1,3-4", 10),
            Err(ReadError::InvalidRange { content_length: 10 })
        ));
        assert!(matches!(
            resolve_range("bytes=0-0", 0),
            Err(ReadError::InvalidRange { content_length: 0 })
        ));
    }
}
