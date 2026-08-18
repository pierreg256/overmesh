use thiserror::Error;

use crate::{
    manifest::{
        CommitManifest, ManifestError, ManifestSigner, ManifestState, SignatureDomain,
        SignedDocument, logical_etag, sha256_bytes,
    },
    resource::{LogicalBlobId, LogicalResourceError},
};

pub const CATALOG_PREFIX: &str = "catalog/v1/";

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("catalog entry path is invalid")]
    InvalidPath,
    #[error("catalog entry logical blob is invalid: {0}")]
    InvalidBlob(#[from] LogicalResourceError),
    #[error("catalog entry signed head is invalid: {0}")]
    Manifest(#[from] ManifestError),
    #[error("catalog entry serialization is invalid: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("catalog entry does not match the signed current head")]
    VerificationFailed,
}

pub struct ValidatedCatalogEntry {
    pub logical_blob: LogicalBlobId,
    pub signed_head: SignedDocument<CommitManifest>,
}

pub fn catalog_key(logical_blob: &LogicalBlobId) -> String {
    format!(
        "{CATALOG_PREFIX}{}/{}.json",
        ordered_component(logical_blob.container()),
        ordered_component(logical_blob.blob())
    )
}

pub fn catalog_key_from_canonical(canonical: &str) -> Result<String, CatalogError> {
    Ok(catalog_key(&LogicalBlobId::parse_canonical(canonical)?))
}

pub fn catalog_container_prefix(container: &str) -> String {
    format!("{CATALOG_PREFIX}{}/", ordered_component(container))
}

pub fn catalog_containers_prefix(prefix: &str) -> String {
    format!("{CATALOG_PREFIX}{}", ordered_component(prefix))
}

pub fn catalog_listing_prefix(container: &str, blob_prefix: &str) -> String {
    format!(
        "{}{}",
        catalog_container_prefix(container),
        ordered_component(blob_prefix)
    )
}

pub fn validate_catalog_entry(
    logical_account: &str,
    object_key: &str,
    bytes: &[u8],
    ring_version: u64,
    replica_ids: [&str; 2],
    signer: &dyn ManifestSigner,
) -> Result<ValidatedCatalogEntry, CatalogError> {
    let logical_blob = logical_blob_from_catalog_key(logical_account, object_key)?;
    validate_catalog_entry_for_logical_blob(
        &logical_blob,
        object_key,
        bytes,
        ring_version,
        replica_ids,
        signer,
    )
}

pub fn validate_catalog_entry_for_logical_blob(
    logical_blob: &LogicalBlobId,
    object_key: &str,
    bytes: &[u8],
    ring_version: u64,
    replica_ids: [&str; 2],
    signer: &dyn ManifestSigner,
) -> Result<ValidatedCatalogEntry, CatalogError> {
    if object_key != catalog_key(logical_blob) {
        return Err(CatalogError::InvalidPath);
    }
    let signed_head = SignedDocument::<CommitManifest>::from_bytes(bytes)?;
    if signed_head.canonical_bytes()? != bytes {
        return Err(CatalogError::VerificationFailed);
    }
    signed_head.verify(
        SignatureDomain::CommitManifest,
        &signed_head.payload.signing_key_id,
        signer,
    )?;
    let head = &signed_head.payload;
    if head.blob != logical_blob.canonical()
        || head.ring_version != ring_version
        || head.logical_version == 0
        || head.prepared_replicas != replica_ids
        || head.logical_etag
            != logical_etag(
                logical_blob.canonical(),
                head.logical_version,
                &head.write_id,
                &head.content_sha256,
            )
    {
        return Err(CatalogError::VerificationFailed);
    }
    match head.state {
        ManifestState::Committed => validate_committed(head, logical_blob)?,
        ManifestState::Tombstoned => validate_tombstone(head)?,
        ManifestState::Prepared => return Err(CatalogError::VerificationFailed),
    }
    Ok(ValidatedCatalogEntry {
        logical_blob: logical_blob.clone(),
        signed_head,
    })
}

pub fn validate_catalog_entry_for_blob(
    canonical_blob: &str,
    object_key: &str,
    bytes: &[u8],
    ring_version: u64,
    replica_ids: [&str; 2],
    signer: &dyn ManifestSigner,
) -> Result<ValidatedCatalogEntry, CatalogError> {
    let logical_blob = LogicalBlobId::parse_canonical(canonical_blob)?;
    validate_catalog_entry_for_logical_blob(
        &logical_blob,
        object_key,
        bytes,
        ring_version,
        replica_ids,
        signer,
    )
}

pub fn logical_blob_from_catalog_key(
    logical_account: &str,
    object_key: &str,
) -> Result<LogicalBlobId, CatalogError> {
    let encoded = object_key
        .strip_prefix(CATALOG_PREFIX)
        .and_then(|value| value.strip_suffix(".json"))
        .ok_or(CatalogError::InvalidPath)?;
    let (container, blob) = encoded.split_once('/').ok_or(CatalogError::InvalidPath)?;
    if container.is_empty() || blob.is_empty() || blob.contains('/') {
        return Err(CatalogError::InvalidPath);
    }
    let container = decode_ordered_component(container)?;
    let blob = decode_ordered_component(blob)?;
    let logical_blob = LogicalBlobId::parse(
        logical_account,
        &format!(
            "/{}/{}",
            percent_encode(&container),
            percent_encode_blob(&blob)
        ),
    )?;
    if catalog_key(&logical_blob) != object_key {
        return Err(CatalogError::InvalidPath);
    }
    Ok(logical_blob)
}

fn validate_committed(
    head: &CommitManifest,
    logical_blob: &LogicalBlobId,
) -> Result<(), CatalogError> {
    let digest = head
        .content_sha256
        .strip_prefix("sha256:")
        .filter(|value| valid_hex_digest(value))
        .ok_or(CatalogError::VerificationFailed)?;
    let expected_block_object = format!(
        "objects/{}/versions/{}/{digest}/block-manifest.json",
        logical_blob.path_hash(),
        crate::resource::stable_component(&head.write_id)
    );
    let expected_content_prefix = format!(".overmesh/objects/{}/", logical_blob.path_hash());
    if head.deleted_at_unix_ms.is_some()
        || head.content_container != logical_blob.container()
        || !head.content_object.starts_with(&expected_content_prefix)
        || head.block_manifest_object != expected_block_object
        || !head
            .block_manifest_sha256
            .strip_prefix("sha256:")
            .is_some_and(valid_hex_digest)
        || head.version_object_prefix.as_deref()
            != head
                .block_manifest_object
                .strip_suffix("/block-manifest.json")
    {
        return Err(CatalogError::VerificationFailed);
    }
    Ok(())
}

fn validate_tombstone(head: &CommitManifest) -> Result<(), CatalogError> {
    if head.deleted_at_unix_ms.is_none()
        || head.previous_logical_etag.is_none()
        || head.version_object_prefix.is_none()
        || head.content_length != 0
        || head.content_sha256 != sha256_bytes(b"overmesh:tombstone:v1")
        || !head.content_container.is_empty()
        || !head.content_object.is_empty()
        || !head.block_manifest_object.is_empty()
        || !head.block_manifest_sha256.is_empty()
    {
        return Err(CatalogError::VerificationFailed);
    }
    Ok(())
}

fn ordered_component(value: &str) -> String {
    hex::encode(value.as_bytes())
}

fn decode_ordered_component(value: &str) -> Result<String, CatalogError> {
    let bytes = hex::decode(value).map_err(|_| CatalogError::InvalidPath)?;
    String::from_utf8(bytes).map_err(|_| CatalogError::InvalidPath)
}

fn percent_encode(value: &str) -> String {
    value
        .as_bytes()
        .iter()
        .map(|byte| format!("%{byte:02X}"))
        .collect()
}

fn percent_encode_blob(value: &str) -> String {
    value
        .split('/')
        .map(percent_encode)
        .collect::<Vec<_>>()
        .join("/")
}

fn valid_hex_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_keys_preserve_container_and_blob_utf8_order() {
        let mut blobs = ["é", "a/", "a", "z"]
            .map(|name| LogicalBlobId::parse("account", &format!("/photos/{name}")).expect("blob"));
        blobs.sort_by(|left, right| left.blob().as_bytes().cmp(right.blob().as_bytes()));
        let mut keys = blobs.iter().map(catalog_key).collect::<Vec<_>>();
        keys.sort();
        let decoded = keys
            .iter()
            .map(|key| {
                logical_blob_from_catalog_key("account", key)
                    .expect("catalog key")
                    .blob()
                    .to_owned()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            decoded,
            blobs
                .iter()
                .map(|blob| blob.blob().to_owned())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn listing_prefix_is_a_physical_key_prefix() {
        let blob = LogicalBlobId::parse("account", "/photos/dir/leaf").expect("blob");
        assert!(catalog_key(&blob).starts_with(&catalog_listing_prefix("photos", "dir/")));
    }
}
