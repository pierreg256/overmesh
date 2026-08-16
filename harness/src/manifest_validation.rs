use std::{fs, path::Path};

use anyhow::{Context, Result, ensure};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use overmesh_gateway::manifest::{
    BlockManifest, BlockManifestPage, CommitManifest, GarbageCollectionMarker,
    HistoryCompactionCheckpoint, SignedDocument, canonical_signed_payload, sha256_bytes,
};
use p256::ecdsa::{Signature, SigningKey, signature::Verifier};
use serde::{Serialize, de::DeserializeOwned};

const LOCAL_MANIFEST_KEY: [u8; 32] = [11; 32];
const BLOCK_MANIFEST_DOMAIN: &[u8] = b"overmesh:block-manifest:v1\0";
const COMMIT_MANIFEST_DOMAIN: &[u8] = b"overmesh:commit-manifest:v1\0";
const GARBAGE_COLLECTION_MARKER_DOMAIN: &[u8] = b"overmesh:garbage-collection-marker:v1\0";
const HISTORY_COMPACTION_CHECKPOINT_DOMAIN: &[u8] = b"overmesh:history-compaction-checkpoint:v1\0";
const LOCAL_KEY_ID: &str = "test-blob-key-01";
const LOCAL_KEY_NOT_BEFORE_UNIX_MS: u64 = 1_700_000_000_000;
const LOCAL_KEY_NOT_AFTER_UNIX_MS: u64 = 2_000_000_000_000;

pub fn verify_local_commit_manifest(
    path: &Path,
    block_manifest_path: Option<&Path>,
) -> Result<CommitManifest> {
    let bytes = fs::read(path)?;
    let commit = verify_local_commit_manifest_bytes(&bytes)?;
    if let Some(block_manifest_path) = block_manifest_path {
        let block_bytes = fs::read(block_manifest_path)?;
        let signed_block = verify_signed::<BlockManifest>(&block_bytes, BLOCK_MANIFEST_DOMAIN)?;
        ensure!(
            signed_block.payload.signing_key_id == LOCAL_KEY_ID,
            "block manifest signing key id is not trusted"
        );
        ensure!(
            sha256_bytes(&block_bytes) == commit.block_manifest_sha256,
            "block manifest hash does not match the committed manifest"
        );
        ensure!(
            signed_block.payload.blob == commit.blob
                && signed_block.payload.write_id == commit.write_id
                && signed_block.payload.logical_version == commit.logical_version
                && signed_block.payload.content_length == commit.content_length
                && signed_block.payload.content_sha256 == commit.content_sha256
                && signed_block.payload.ring_version == commit.ring_version,
            "block and commit manifests do not describe the same logical version"
        );
        validate_block_manifest_layout(&commit, &signed_block.payload)?;
    }
    Ok(commit)
}

pub fn verify_local_commit_manifest_bytes(bytes: &[u8]) -> Result<CommitManifest> {
    let signed = verify_signed::<CommitManifest>(bytes, COMMIT_MANIFEST_DOMAIN)?;
    ensure!(
        signed.payload.signing_key_id == LOCAL_KEY_ID,
        "commit manifest signing key id is not trusted"
    );
    Ok(signed.payload)
}

pub fn verify_local_garbage_collection_marker(path: &Path) -> Result<GarbageCollectionMarker> {
    let bytes = fs::read(path)?;
    let signed =
        verify_signed::<GarbageCollectionMarker>(&bytes, GARBAGE_COLLECTION_MARKER_DOMAIN)?;
    ensure!(
        signed.payload.signing_key_id == LOCAL_KEY_ID,
        "garbage-collection marker signing key id is not trusted"
    );
    Ok(signed.payload)
}

pub fn verify_local_history_compaction_checkpoint(
    path: &Path,
) -> Result<HistoryCompactionCheckpoint> {
    let bytes = fs::read(path)?;
    let signed =
        verify_signed::<HistoryCompactionCheckpoint>(&bytes, HISTORY_COMPACTION_CHECKPOINT_DOMAIN)?;
    ensure!(
        signed.payload.signing_key_id == LOCAL_KEY_ID,
        "history compaction checkpoint signing key id is not trusted"
    );
    Ok(signed.payload)
}

pub fn verify_block_manifest_page(
    manifest: &BlockManifest,
    reference_index: usize,
    bytes: &[u8],
) -> Result<BlockManifestPage> {
    let reference = manifest
        .pages
        .get(reference_index)
        .context("block manifest page reference is missing")?;
    ensure!(
        sha256_bytes(bytes) == reference.sha256,
        "block manifest page hash is invalid"
    );
    let page: BlockManifestPage =
        serde_json::from_slice(bytes).context("block manifest page is not valid JSON")?;
    ensure!(
        page.blob == manifest.blob
            && page.write_id == manifest.write_id
            && page.logical_version == manifest.logical_version
            && page.page_index == reference.index
            && page.first_block_index == reference.first_block_index
            && u32::try_from(page.blocks.len())? == reference.block_count,
        "block manifest page identity is invalid"
    );
    let mut next_offset = reference.first_offset;
    for (local_index, block) in page.blocks.iter().enumerate() {
        ensure!(
            block.index == reference.first_block_index + u32::try_from(local_index)?
                && block.offset == next_offset
                && valid_sha256(&block.sha256),
            "block descriptor is invalid"
        );
        next_offset = next_offset
            .checked_add(block.length)
            .context("block offset overflow")?;
    }
    ensure!(
        next_offset
            == reference
                .first_offset
                .checked_add(reference.content_length)
                .context("page content length overflow")?,
        "block manifest page does not cover its declared range"
    );
    Ok(page)
}

pub fn verify_local_block_manifest(commit: &CommitManifest, bytes: &[u8]) -> Result<BlockManifest> {
    let signed = verify_signed::<BlockManifest>(bytes, BLOCK_MANIFEST_DOMAIN)?;
    ensure!(
        signed.payload.signing_key_id == LOCAL_KEY_ID,
        "block manifest signing key id is not trusted"
    );
    ensure!(
        sha256_bytes(bytes) == commit.block_manifest_sha256,
        "block manifest hash does not match the committed manifest"
    );
    ensure!(
        signed.payload.blob == commit.blob
            && signed.payload.write_id == commit.write_id
            && signed.payload.logical_version == commit.logical_version
            && signed.payload.content_container == commit.content_container
            && signed.payload.content_object == commit.content_object
            && signed.payload.content_length == commit.content_length
            && signed.payload.content_sha256 == commit.content_sha256
            && signed.payload.ring_version == commit.ring_version,
        "block and commit manifests do not describe the same logical version"
    );
    validate_block_manifest_layout(commit, &signed.payload)?;
    Ok(signed.payload)
}

pub fn verify_signed<T>(bytes: &[u8], domain: &[u8]) -> Result<SignedDocument<T>>
where
    T: DeserializeOwned + Serialize,
{
    let signed = SignedDocument::<T>::from_bytes(bytes)?;
    ensure!(
        signed.signature_algorithm == "ES256",
        "unexpected signature algorithm"
    );
    ensure!(
        (LOCAL_KEY_NOT_BEFORE_UNIX_MS..=LOCAL_KEY_NOT_AFTER_UNIX_MS)
            .contains(&signed.signed_at_unix_ms),
        "signature timestamp is outside the local manifest key validity period"
    );
    let signature_bytes = URL_SAFE_NO_PAD
        .decode(&signed.signature)
        .context("signature is not valid base64url")?;
    let signature =
        Signature::from_slice(&signature_bytes).context("signature is not valid ES256")?;
    let canonical = canonical_signed_payload(&signed.payload, signed.signed_at_unix_ms)?;
    let mut input = Vec::with_capacity(domain.len() + canonical.len());
    input.extend_from_slice(domain);
    input.extend_from_slice(&canonical);
    let signing_key = SigningKey::from_bytes((&LOCAL_MANIFEST_KEY).into())
        .context("local manifest key is invalid")?;
    signing_key
        .verifying_key()
        .verify(&input, &signature)
        .context("manifest signature verification failed")?;
    Ok(signed)
}

fn validate_block_manifest_layout(commit: &CommitManifest, manifest: &BlockManifest) -> Result<()> {
    ensure!(manifest.block_count > 0, "block manifest has no blocks");
    ensure!(manifest.page_size > 0, "block manifest page size is zero");
    ensure!(!manifest.pages.is_empty(), "block manifest has no pages");
    let prefix = commit
        .block_manifest_object
        .strip_suffix("/block-manifest.json")
        .context("block manifest object uses an invalid layout")?;
    let expected_pages = manifest.block_count.div_ceil(manifest.page_size);
    ensure!(
        usize::try_from(expected_pages)? == manifest.pages.len(),
        "block manifest page count is inconsistent"
    );
    let mut next_block = 0_u32;
    let mut next_offset = 0_u64;
    for (index, page) in manifest.pages.iter().enumerate() {
        let index = u32::try_from(index)?;
        ensure!(
            page.index == index
                && page.first_block_index == next_block
                && page.block_count > 0
                && page.block_count <= manifest.page_size
                && page.first_offset == next_offset
                && page.object == format!("{prefix}/block-pages/{index:08}.json")
                && valid_sha256(&page.sha256),
            "block manifest page reference is invalid"
        );
        next_block = next_block
            .checked_add(page.block_count)
            .context("block count overflow")?;
        next_offset = next_offset
            .checked_add(page.content_length)
            .context("content length overflow")?;
    }
    ensure!(
        next_block == manifest.block_count && next_offset == manifest.content_length,
        "block manifest pages do not cover the complete content"
    );
    Ok(())
}

fn valid_sha256(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}
