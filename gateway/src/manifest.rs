use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use azure_core::credentials::TokenCredential;
use azure_security_keyvault_keys::{
    KeyClient,
    models::{KeyClientSignOptions, SignParameters, SignatureAlgorithm},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use p256::ecdsa::{
    Signature, SigningKey, VerifyingKey,
    signature::{Signer, Verifier},
};
use p256::pkcs8::DecodePublicKey;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::identity::CallerIdentity;

const BLOCK_MANIFEST_DOMAIN: &[u8] = b"overmesh:block-manifest:v1\0";
const COMMIT_MANIFEST_DOMAIN: &[u8] = b"overmesh:commit-manifest:v1\0";
const RECONCILIATION_RECORD_DOMAIN: &[u8] = b"overmesh:reconciliation-record:v1\0";
const GARBAGE_COLLECTION_MARKER_DOMAIN: &[u8] = b"overmesh:garbage-collection-marker:v1\0";
const HISTORY_COMPACTION_CHECKPOINT_DOMAIN: &[u8] = b"overmesh:history-compaction-checkpoint:v1\0";
const CONTINUATION_TOKEN_DOMAIN: &[u8] = b"overmesh:continuation-token:v1\0";
const UPLOAD_GENERATION_DOMAIN: &[u8] = b"overmesh:upload-generation:v1\0";
const STAGED_BLOCK_DOMAIN: &[u8] = b"overmesh:staged-block:v1\0";
const STAGED_BLOCK_GC_MARKER_DOMAIN: &[u8] = b"overmesh:staged-block-gc-marker:v1\0";
const LOCAL_TEST_MANIFEST_KEY: [u8; 32] = [11; 32];
pub const BLOCK_MANIFEST_PAGE_SIZE: u32 = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyValidity {
    pub not_before_unix_ms: u64,
    pub not_after_unix_ms: u64,
}

impl KeyValidity {
    pub fn new(not_before_unix_ms: u64, not_after_unix_ms: u64) -> Result<Self, ManifestError> {
        if not_before_unix_ms > not_after_unix_ms {
            return Err(ManifestError::InvalidKeyValidity);
        }
        Ok(Self {
            not_before_unix_ms,
            not_after_unix_ms,
        })
    }

    fn permits(self, signed_at_unix_ms: u64) -> bool {
        (self.not_before_unix_ms..=self.not_after_unix_ms).contains(&signed_at_unix_ms)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ManifestState {
    Prepared,
    Committed,
    Tombstoned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReconciliationClassification {
    Drifted,
    Missing,
    Tampered,
    Quarantined,
    Tombstoned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReconciliationRecordAction {
    Repaired,
    Quarantined,
    Recovered,
    GarbageCollected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReconciliationRecord {
    pub api_version: String,
    pub blob: Option<String>,
    pub head_object: String,
    pub ring_version: u64,
    pub observed_at_unix_ms: u64,
    pub classification: ReconciliationClassification,
    pub action: ReconciliationRecordAction,
    pub reason: String,
    pub source_replica: Option<String>,
    pub target_replica: Option<String>,
    pub signing_key_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GarbageCollectionMarker {
    pub api_version: String,
    pub blob: String,
    pub head_object: String,
    pub ring_version: u64,
    pub history_head_logical_version: u64,
    pub collected_through_logical_version: u64,
    pub collected_committed_versions: Vec<u64>,
    pub previous_marker_sha256: Option<String>,
    pub physical_collection_delay_ms: u64,
    pub collected_at_unix_ms: u64,
    pub signing_key_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HistoryCompactionCheckpoint {
    pub api_version: String,
    pub blob: String,
    pub path_hash: String,
    pub head_object: String,
    pub ring_version: u64,
    pub checkpoint_version: u64,
    pub compacted_through_logical_version: u64,
    pub compacted_through_state: ManifestState,
    pub compacted_through_logical_etag: String,
    pub compacted_through_committed_at_unix_ms: u64,
    pub covered_terminal_manifest_sha256: String,
    pub previous_checkpoint_sha256: Option<String>,
    pub previous_checkpoint_version: Option<u64>,
    pub garbage_collection_marker_object: String,
    pub garbage_collection_marker_sha256: String,
    pub garbage_collection_through_logical_version: u64,
    pub garbage_collection_history_head_logical_version: u64,
    pub garbage_collected_committed_versions: Vec<u64>,
    pub garbage_collection_delay_ms: u64,
    pub garbage_collected_at_unix_ms: u64,
    pub compacted_at_unix_ms: u64,
    pub signing_key_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BlockDescriptor {
    pub index: u32,
    pub offset: u64,
    pub length: u64,
    pub sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_block_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StagedBlock {
    pub api_version: String,
    pub blob: String,
    pub upload_id: String,
    pub write_id: String,
    pub block_id: String,
    pub decoded_block_id_length: u32,
    pub block_id_sha256: String,
    pub content_container: String,
    pub content_object: String,
    pub content_length: u64,
    pub content_sha256: String,
    pub base_logical_version: u64,
    pub base_logical_etag: Option<String>,
    pub ring_version: u64,
    pub prepared_replicas: Vec<String>,
    pub created_at_unix_ms: u64,
    pub expires_at_unix_ms: u64,
    pub caller: CallerIdentity,
    pub signing_key_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UploadGeneration {
    pub api_version: String,
    pub blob: String,
    pub upload_id: String,
    pub decoded_block_id_length: u32,
    pub base_logical_version: u64,
    pub base_logical_etag: Option<String>,
    pub ring_version: u64,
    pub prepared_replicas: Vec<String>,
    pub created_at_unix_ms: u64,
    pub expires_at_unix_ms: u64,
    pub caller: CallerIdentity,
    pub signing_key_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StagedBlockGcMarker {
    pub api_version: String,
    pub blob: String,
    pub metadata_object: String,
    pub metadata_sha256: String,
    pub content_container: String,
    pub content_object: String,
    pub content_length: u64,
    pub content_sha256: String,
    pub ring_version: u64,
    pub replicas: Vec<String>,
    pub expired_at_unix_ms: u64,
    pub validated_at_unix_ms: u64,
    pub signing_key_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BlockManifestPageReference {
    pub index: u32,
    pub first_block_index: u32,
    pub block_count: u32,
    pub first_offset: u64,
    pub content_length: u64,
    pub object: String,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BlockManifestPage {
    pub blob: String,
    pub write_id: String,
    pub logical_version: u64,
    pub page_index: u32,
    pub first_block_index: u32,
    pub blocks: Vec<BlockDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BlockManifest {
    pub blob: String,
    pub write_id: String,
    pub logical_version: u64,
    pub content_container: String,
    pub content_object: String,
    pub content_length: u64,
    pub block_count: u32,
    pub page_size: u32,
    pub pages: Vec<BlockManifestPageReference>,
    pub content_sha256: String,
    pub ring_version: u64,
    pub signing_key_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CommitManifest {
    pub blob: String,
    pub caller: CallerIdentity,
    pub write_id: String,
    pub logical_version: u64,
    pub logical_etag: String,
    pub previous_logical_etag: Option<String>,
    pub ring_version: u64,
    pub content_length: u64,
    pub content_sha256: String,
    pub content_container: String,
    pub content_object: String,
    pub block_manifest_object: String,
    pub block_manifest_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_object_prefix: Option<String>,
    pub committed_at_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_at_unix_ms: Option<u64>,
    pub state: ManifestState,
    pub prepared_replicas: Vec<String>,
    pub signing_key_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SignedDocument<T> {
    pub payload: T,
    pub signed_at_unix_ms: u64,
    pub signature_algorithm: String,
    pub signature: String,
}

#[derive(Debug, Clone, Copy)]
pub enum SignatureDomain {
    BlockManifest,
    CommitManifest,
    ReconciliationRecord,
    GarbageCollectionMarker,
    HistoryCompactionCheckpoint,
    ContinuationToken,
    UploadGeneration,
    StagedBlock,
    StagedBlockGcMarker,
}

impl SignatureDomain {
    fn prefix(self) -> &'static [u8] {
        match self {
            Self::BlockManifest => BLOCK_MANIFEST_DOMAIN,
            Self::CommitManifest => COMMIT_MANIFEST_DOMAIN,
            Self::ReconciliationRecord => RECONCILIATION_RECORD_DOMAIN,
            Self::GarbageCollectionMarker => GARBAGE_COLLECTION_MARKER_DOMAIN,
            Self::HistoryCompactionCheckpoint => HISTORY_COMPACTION_CHECKPOINT_DOMAIN,
            Self::ContinuationToken => CONTINUATION_TOKEN_DOMAIN,
            Self::UploadGeneration => UPLOAD_GENERATION_DOMAIN,
            Self::StagedBlock => STAGED_BLOCK_DOMAIN,
            Self::StagedBlockGcMarker => STAGED_BLOCK_GC_MARKER_DOMAIN,
        }
    }
}

#[derive(Debug, Error)]
pub enum ManifestError {
    #[error("manifest canonicalization failed: {0}")]
    Canonicalization(#[from] serde_json::Error),
    #[error("manifest signature is not valid base64url")]
    InvalidSignatureEncoding,
    #[error("manifest signature verification failed")]
    InvalidSignature,
    #[error("manifest signer is not permitted outside a test configuration")]
    TestSignerNotPermitted,
    #[error("manifest signing failed: {0}")]
    Signing(String),
    #[error("manifest public key is invalid")]
    InvalidPublicKey,
    #[error("manifest key validity period is invalid")]
    InvalidKeyValidity,
    #[error("manifest signing key is not trusted: {0}")]
    UnknownSigningKey(String),
    #[error(
        "manifest signature timestamp {signed_at_unix_ms} is outside key validity period \
         {not_before_unix_ms}..={not_after_unix_ms}"
    )]
    SignatureOutsideValidity {
        signed_at_unix_ms: u64,
        not_before_unix_ms: u64,
        not_after_unix_ms: u64,
    },
    #[error("manifest structure is invalid: {0}")]
    InvalidStructure(String),
}

#[async_trait]
pub trait ManifestSigner: Send + Sync {
    fn key_id(&self) -> &str;
    async fn sign(
        &self,
        domain: SignatureDomain,
        signed_at_unix_ms: u64,
        canonical_payload: &[u8],
    ) -> Result<String, ManifestError>;
    fn verify(
        &self,
        key_id: &str,
        domain: SignatureDomain,
        signed_at_unix_ms: u64,
        canonical_payload: &[u8],
        signature: &str,
    ) -> Result<(), ManifestError>;
}

#[derive(Clone)]
pub struct LocalTestManifestSigner {
    key_id: String,
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
    validity: KeyValidity,
}

impl LocalTestManifestSigner {
    pub fn new(
        key_id: impl Into<String>,
        explicitly_allowed: bool,
        validity: KeyValidity,
    ) -> Result<Self, ManifestError> {
        if !explicitly_allowed {
            return Err(ManifestError::TestSignerNotPermitted);
        }
        validate_active_key(validity)?;
        let signing_key =
            SigningKey::from_bytes((&LOCAL_TEST_MANIFEST_KEY).into()).expect("valid test key");
        let verifying_key = *signing_key.verifying_key();
        Ok(Self {
            key_id: key_id.into(),
            signing_key,
            verifying_key,
            validity,
        })
    }
}

#[async_trait]
impl ManifestSigner for LocalTestManifestSigner {
    fn key_id(&self) -> &str {
        &self.key_id
    }

    async fn sign(
        &self,
        domain: SignatureDomain,
        signed_at_unix_ms: u64,
        canonical_payload: &[u8],
    ) -> Result<String, ManifestError> {
        validate_signature_time(self.validity, signed_at_unix_ms)?;
        let signature: Signature = self
            .signing_key
            .sign(&signature_input(domain, canonical_payload));
        Ok(URL_SAFE_NO_PAD.encode(signature.to_bytes()))
    }

    fn verify(
        &self,
        key_id: &str,
        domain: SignatureDomain,
        signed_at_unix_ms: u64,
        canonical_payload: &[u8],
        signature: &str,
    ) -> Result<(), ManifestError> {
        if key_id != self.key_id {
            return Err(ManifestError::UnknownSigningKey(key_id.to_owned()));
        }
        validate_signature_time(self.validity, signed_at_unix_ms)?;
        let bytes = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| ManifestError::InvalidSignatureEncoding)?;
        let signature =
            Signature::from_slice(&bytes).map_err(|_| ManifestError::InvalidSignatureEncoding)?;
        self.verifying_key
            .verify(&signature_input(domain, canonical_payload), &signature)
            .map_err(|_| ManifestError::InvalidSignature)
    }
}

pub struct KeyVaultManifestSigner {
    key_id: String,
    key_name: String,
    key_version: String,
    client: KeyClient,
    trusted_keys: HashMap<String, TrustedManifestKey>,
}

struct TrustedManifestKey {
    verifying_key: VerifyingKey,
    validity: KeyValidity,
}

impl KeyVaultManifestSigner {
    pub fn new(
        vault_url: &str,
        key_name: impl Into<String>,
        key_version: impl Into<String>,
        public_key_pem: &str,
        active_validity: KeyValidity,
        additional_trusted_keys: Vec<(String, String, KeyValidity)>,
        credential: Arc<dyn TokenCredential>,
    ) -> Result<Self, ManifestError> {
        let key_name = key_name.into();
        let key_version = key_version.into();
        if key_name.is_empty() || key_version.is_empty() {
            return Err(ManifestError::Signing(
                "Key Vault key name and version are required".to_owned(),
            ));
        }
        let vault_url = vault_url.trim_end_matches('/');
        let client = KeyClient::new(vault_url, credential, None)
            .map_err(|error| ManifestError::Signing(error.to_string()))?;
        let active_key_id = format!("{vault_url}/keys/{key_name}/{key_version}");
        let verifying_key = VerifyingKey::from_public_key_pem(public_key_pem)
            .map_err(|_| ManifestError::InvalidPublicKey)?;
        validate_active_key(active_validity)?;
        let mut trusted_keys = HashMap::from([(
            active_key_id.clone(),
            TrustedManifestKey {
                verifying_key,
                validity: active_validity,
            },
        )]);
        for (key_id, public_key_pem, validity) in additional_trusted_keys {
            if trusted_keys.contains_key(&key_id) {
                return Err(ManifestError::Signing(format!(
                    "duplicate trusted manifest key {key_id}"
                )));
            }
            let verifying_key = VerifyingKey::from_public_key_pem(&public_key_pem)
                .map_err(|_| ManifestError::InvalidPublicKey)?;
            if validity.not_before_unix_ms > validity.not_after_unix_ms {
                return Err(ManifestError::InvalidKeyValidity);
            }
            trusted_keys.insert(
                key_id,
                TrustedManifestKey {
                    verifying_key,
                    validity,
                },
            );
        }
        Ok(Self {
            key_id: active_key_id,
            key_name,
            key_version,
            client,
            trusted_keys,
        })
    }
}

#[async_trait]
impl ManifestSigner for KeyVaultManifestSigner {
    fn key_id(&self) -> &str {
        &self.key_id
    }

    async fn sign(
        &self,
        domain: SignatureDomain,
        signed_at_unix_ms: u64,
        canonical_payload: &[u8],
    ) -> Result<String, ManifestError> {
        let active = self
            .trusted_keys
            .get(&self.key_id)
            .ok_or_else(|| ManifestError::UnknownSigningKey(self.key_id.clone()))?;
        validate_signature_time(active.validity, signed_at_unix_ms)?;
        let digest = Sha256::digest(signature_input(domain, canonical_payload)).to_vec();
        let parameters = SignParameters {
            algorithm: Some(SignatureAlgorithm::Es256),
            value: Some(digest),
        };
        let options = KeyClientSignOptions {
            key_version: Some(self.key_version.clone()),
            ..Default::default()
        };
        let result = self
            .client
            .sign(
                &self.key_name,
                parameters.try_into().map_err(|error: azure_core::Error| {
                    ManifestError::Signing(error.to_string())
                })?,
                Some(options),
            )
            .await
            .map_err(|error| ManifestError::Signing(error.to_string()))?
            .into_model()
            .map_err(|error| ManifestError::Signing(error.to_string()))?;
        let signature = result
            .result
            .ok_or_else(|| ManifestError::Signing("Key Vault returned no signature".to_owned()))?;
        let encoded = URL_SAFE_NO_PAD.encode(signature);
        self.verify(
            &self.key_id,
            domain,
            signed_at_unix_ms,
            canonical_payload,
            &encoded,
        )?;
        Ok(encoded)
    }

    fn verify(
        &self,
        key_id: &str,
        domain: SignatureDomain,
        signed_at_unix_ms: u64,
        canonical_payload: &[u8],
        signature: &str,
    ) -> Result<(), ManifestError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| ManifestError::InvalidSignatureEncoding)?;
        let signature =
            Signature::from_slice(&bytes).map_err(|_| ManifestError::InvalidSignatureEncoding)?;
        let trusted = self
            .trusted_keys
            .get(key_id)
            .ok_or_else(|| ManifestError::UnknownSigningKey(key_id.to_owned()))?;
        validate_signature_time(trusted.validity, signed_at_unix_ms)?;
        trusted
            .verifying_key
            .verify(&signature_input(domain, canonical_payload), &signature)
            .map_err(|_| ManifestError::InvalidSignature)
    }
}

impl<T> SignedDocument<T>
where
    T: Clone + Serialize,
{
    pub async fn create(
        payload: T,
        domain: SignatureDomain,
        signer: &dyn ManifestSigner,
    ) -> Result<Self, ManifestError> {
        Self::create_at(payload, domain, signer, now_unix_ms()).await
    }

    pub async fn create_at(
        payload: T,
        domain: SignatureDomain,
        signer: &dyn ManifestSigner,
        signed_at_unix_ms: u64,
    ) -> Result<Self, ManifestError> {
        let canonical = canonical_signed_payload(&payload, signed_at_unix_ms)?;
        Ok(Self {
            payload,
            signed_at_unix_ms,
            signature_algorithm: "ES256".to_owned(),
            signature: signer.sign(domain, signed_at_unix_ms, &canonical).await?,
        })
    }

    pub fn verify(
        &self,
        domain: SignatureDomain,
        key_id: &str,
        signer: &dyn ManifestSigner,
    ) -> Result<(), ManifestError> {
        if self.signature_algorithm != "ES256" {
            return Err(ManifestError::InvalidSignature);
        }
        signer.verify(
            key_id,
            domain,
            self.signed_at_unix_ms,
            &canonical_signed_payload(&self.payload, self.signed_at_unix_ms)?,
            &self.signature,
        )
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ManifestError> {
        serde_jcs::to_vec(self).map_err(ManifestError::Canonicalization)
    }
}

pub fn validate_block_manifest_link(
    commit: &CommitManifest,
    manifest: &BlockManifest,
) -> Result<(), ManifestError> {
    if manifest.blob != commit.blob
        || manifest.write_id != commit.write_id
        || manifest.logical_version != commit.logical_version
        || manifest.content_container != commit.content_container
        || manifest.content_object != commit.content_object
        || manifest.content_length != commit.content_length
        || manifest.content_sha256 != commit.content_sha256
        || manifest.ring_version != commit.ring_version
        || manifest.block_count == 0
        || manifest.page_size == 0
        || manifest.pages.is_empty()
    {
        return Err(ManifestError::InvalidStructure(
            "block manifest does not match the committed version".to_owned(),
        ));
    }
    validate_block_manifest_layout(&commit.block_manifest_object, manifest)
}

pub fn validate_block_manifest_layout(
    block_manifest_object: &str,
    manifest: &BlockManifest,
) -> Result<(), ManifestError> {
    let prefix = block_manifest_object
        .strip_suffix("/block-manifest.json")
        .ok_or_else(|| {
            ManifestError::InvalidStructure(
                "block manifest object does not use the expected layout".to_owned(),
            )
        })?;
    let expected_page_count = manifest.block_count.div_ceil(manifest.page_size);
    if usize::try_from(expected_page_count).ok() != Some(manifest.pages.len()) {
        return Err(ManifestError::InvalidStructure(
            "block manifest page count is inconsistent".to_owned(),
        ));
    }
    let mut next_block = 0_u32;
    let mut next_offset = 0_u64;
    for (page_index, page) in manifest.pages.iter().enumerate() {
        let page_index = u32::try_from(page_index).map_err(|_| {
            ManifestError::InvalidStructure("too many block manifest pages".to_owned())
        })?;
        let expected_object = format!("{prefix}/block-pages/{page_index:08}.json");
        if page.index != page_index
            || page.first_block_index != next_block
            || page.block_count == 0
            || page.block_count > manifest.page_size
            || (page_index + 1 < expected_page_count && page.block_count != manifest.page_size)
            || page.first_offset != next_offset
            || page.object != expected_object
            || !valid_sha256(&page.sha256)
        {
            return Err(ManifestError::InvalidStructure(
                "block manifest page reference is invalid".to_owned(),
            ));
        }
        next_block = next_block
            .checked_add(page.block_count)
            .ok_or_else(|| ManifestError::InvalidStructure("block count overflow".to_owned()))?;
        next_offset = next_offset
            .checked_add(page.content_length)
            .ok_or_else(|| {
                ManifestError::InvalidStructure("block content length overflow".to_owned())
            })?;
    }
    if next_block != manifest.block_count || next_offset != manifest.content_length {
        return Err(ManifestError::InvalidStructure(
            "block manifest pages do not cover the complete content".to_owned(),
        ));
    }
    Ok(())
}

pub fn validate_block_manifest_page(
    manifest: &BlockManifest,
    reference: &BlockManifestPageReference,
    page: &BlockManifestPage,
) -> Result<(), ManifestError> {
    if page.blob != manifest.blob
        || page.write_id != manifest.write_id
        || page.logical_version != manifest.logical_version
        || page.page_index != reference.index
        || page.first_block_index != reference.first_block_index
        || u32::try_from(page.blocks.len()).ok() != Some(reference.block_count)
    {
        return Err(ManifestError::InvalidStructure(
            "block manifest page identity is invalid".to_owned(),
        ));
    }
    let mut next_offset = reference.first_offset;
    for (local_index, block) in page.blocks.iter().enumerate() {
        let local_index = u32::try_from(local_index)
            .map_err(|_| ManifestError::InvalidStructure("too many blocks in a page".to_owned()))?;
        if block.index != reference.first_block_index + local_index
            || block.offset != next_offset
            || !valid_sha256(&block.sha256)
            || (block.length == 0 && !(manifest.content_length == 0 && manifest.block_count == 1))
        {
            return Err(ManifestError::InvalidStructure(
                "block descriptor is invalid".to_owned(),
            ));
        }
        next_offset = next_offset
            .checked_add(block.length)
            .ok_or_else(|| ManifestError::InvalidStructure("block offset overflow".to_owned()))?;
    }
    let expected_end = reference
        .first_offset
        .checked_add(reference.content_length)
        .ok_or_else(|| {
            ManifestError::InvalidStructure("block manifest page length overflow".to_owned())
        })?;
    if next_offset != expected_end {
        return Err(ManifestError::InvalidStructure(
            "block manifest page length is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn valid_sha256(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

impl<T> SignedDocument<T>
where
    T: DeserializeOwned,
{
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

pub fn logical_etag(blob: &str, version: u64, write_id: &str, content_sha256: &str) -> String {
    let digest =
        Sha256::digest(format!("{blob}\0{version}\0{write_id}\0{content_sha256}").as_bytes());
    format!("\"om-v{version}-{}\"", hex::encode(&digest[..8]))
}

pub fn commit_manifest_object_prefix(manifest: &CommitManifest) -> Result<&str, ManifestError> {
    if let Some(prefix) = manifest.version_object_prefix.as_deref() {
        if prefix.is_empty() {
            return Err(ManifestError::InvalidStructure(
                "commit manifest version object prefix is empty".to_owned(),
            ));
        }
        return Ok(prefix);
    }
    manifest
        .block_manifest_object
        .strip_suffix("/block-manifest.json")
        .ok_or_else(|| {
            ManifestError::InvalidStructure(
                "block manifest object does not use the expected version layout".to_owned(),
            )
        })
}

pub fn sha256_bytes(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

fn signature_input(domain: SignatureDomain, canonical_payload: &[u8]) -> Vec<u8> {
    let mut input = Vec::with_capacity(domain.prefix().len() + canonical_payload.len());
    input.extend_from_slice(domain.prefix());
    input.extend_from_slice(canonical_payload);
    input
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SignedPayload<'a, T> {
    payload: &'a T,
    signed_at_unix_ms: u64,
}

pub fn canonical_signed_payload<T: Serialize>(
    payload: &T,
    signed_at_unix_ms: u64,
) -> Result<Vec<u8>, ManifestError> {
    Ok(serde_jcs::to_vec(&SignedPayload {
        payload,
        signed_at_unix_ms,
    })?)
}

fn validate_active_key(validity: KeyValidity) -> Result<(), ManifestError> {
    validate_signature_time(validity, now_unix_ms())
}

fn validate_signature_time(
    validity: KeyValidity,
    signed_at_unix_ms: u64,
) -> Result<(), ManifestError> {
    if validity.permits(signed_at_unix_ms) {
        return Ok(());
    }
    Err(ManifestError::SignatureOutsideValidity {
        signed_at_unix_ms,
        not_before_unix_ms: validity.not_before_unix_ms,
        not_after_unix_ms: validity.not_after_unix_ms,
    })
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use azure_core::{
        credentials::{AccessToken, TokenRequestOptions},
        time::{Duration, OffsetDateTime},
    };
    use p256::pkcs8::{EncodePublicKey, LineEnding};

    use super::*;

    #[derive(Debug)]
    struct TestCredential;

    #[async_trait]
    impl TokenCredential for TestCredential {
        async fn get_token(
            &self,
            _scopes: &[&str],
            _options: Option<TokenRequestOptions<'_>>,
        ) -> azure_core::Result<AccessToken> {
            Ok(AccessToken::new(
                "test-token",
                OffsetDateTime::now_utc() + Duration::hours(1),
            ))
        }
    }

    fn signer() -> LocalTestManifestSigner {
        LocalTestManifestSigner::new(
            "test-blob-key-01",
            true,
            KeyValidity::new(0, u64::MAX).expect("validity"),
        )
        .expect("test signer")
    }

    fn page_reference(content_length: u64) -> BlockManifestPageReference {
        BlockManifestPageReference {
            index: 0,
            first_block_index: 0,
            block_count: 1,
            first_offset: 0,
            content_length,
            object: "objects/path/block-pages/00000000.json".to_owned(),
            sha256: sha256_bytes(b"page"),
        }
    }

    #[tokio::test]
    async fn signs_and_verifies_block_manifest() {
        let signer = signer();
        let payload = BlockManifest {
            blob: "/container/blob".to_owned(),
            write_id: "write-1".to_owned(),
            logical_version: 1,
            content_container: "container".to_owned(),
            content_object: ".overmesh/objects/path/content".to_owned(),
            content_length: 3,
            block_count: 1,
            page_size: BLOCK_MANIFEST_PAGE_SIZE,
            pages: vec![page_reference(3)],
            content_sha256: sha256_bytes(b"abc"),
            ring_version: 1,
            signing_key_id: signer.key_id().to_owned(),
        };
        let signed = SignedDocument::create(payload, SignatureDomain::BlockManifest, &signer)
            .await
            .expect("sign");
        signed
            .verify(
                SignatureDomain::BlockManifest,
                &signed.payload.signing_key_id,
                &signer,
            )
            .expect("verify");
    }

    #[tokio::test]
    async fn domain_separation_rejects_cross_type_signature() {
        let signer = signer();
        let payload = BlockManifest {
            blob: "/container/blob".to_owned(),
            write_id: "write-1".to_owned(),
            logical_version: 1,
            content_container: "container".to_owned(),
            content_object: ".overmesh/objects/path/content".to_owned(),
            content_length: 0,
            block_count: 1,
            page_size: BLOCK_MANIFEST_PAGE_SIZE,
            pages: vec![page_reference(0)],
            content_sha256: sha256_bytes(b""),
            ring_version: 1,
            signing_key_id: signer.key_id().to_owned(),
        };
        let signed = SignedDocument::create(payload, SignatureDomain::BlockManifest, &signer)
            .await
            .expect("sign");
        assert!(
            signed
                .verify(
                    SignatureDomain::CommitManifest,
                    &signed.payload.signing_key_id,
                    &signer,
                )
                .is_err()
        );
    }

    #[tokio::test]
    async fn rejects_a_tampered_signed_payload() {
        let signer = signer();
        let payload = BlockManifest {
            blob: "/container/blob".to_owned(),
            write_id: "write-1".to_owned(),
            logical_version: 1,
            content_container: "container".to_owned(),
            content_object: ".overmesh/objects/path/content".to_owned(),
            content_length: 3,
            block_count: 1,
            page_size: BLOCK_MANIFEST_PAGE_SIZE,
            pages: vec![page_reference(3)],
            content_sha256: sha256_bytes(b"abc"),
            ring_version: 1,
            signing_key_id: signer.key_id().to_owned(),
        };
        let mut signed = SignedDocument::create(payload, SignatureDomain::BlockManifest, &signer)
            .await
            .expect("sign");
        signed.payload.logical_version = 2;

        assert!(
            signed
                .verify(
                    SignatureDomain::BlockManifest,
                    &signed.payload.signing_key_id,
                    &signer,
                )
                .is_err()
        );
    }

    #[tokio::test]
    async fn rejects_a_tampered_signed_timestamp() {
        let signer = signer();
        let payload = BlockManifest {
            blob: "/container/blob".to_owned(),
            write_id: "write-1".to_owned(),
            logical_version: 1,
            content_container: "container".to_owned(),
            content_object: ".overmesh/objects/path/content".to_owned(),
            content_length: 0,
            block_count: 1,
            page_size: BLOCK_MANIFEST_PAGE_SIZE,
            pages: vec![page_reference(0)],
            content_sha256: sha256_bytes(b""),
            ring_version: 1,
            signing_key_id: signer.key_id().to_owned(),
        };
        let mut signed = SignedDocument::create(payload, SignatureDomain::BlockManifest, &signer)
            .await
            .expect("sign");
        signed.signed_at_unix_ms += 1;

        assert!(
            signed
                .verify(
                    SignatureDomain::BlockManifest,
                    &signed.payload.signing_key_id,
                    &signer,
                )
                .is_err()
        );
    }

    #[test]
    fn key_vault_signer_pins_a_versioned_key_id() {
        let signing_key = SigningKey::from_bytes((&[7_u8; 32]).into()).expect("test key");
        let public_key = signing_key
            .verifying_key()
            .to_public_key_pem(LineEnding::LF)
            .expect("public key");
        let signer = KeyVaultManifestSigner::new(
            "https://example.vault.azure.net/",
            "overmesh-manifests",
            "version-01",
            &public_key,
            KeyValidity::new(0, u64::MAX).expect("validity"),
            Vec::new(),
            Arc::new(TestCredential),
        )
        .expect("Key Vault signer");

        assert_eq!(
            signer.key_id(),
            "https://example.vault.azure.net/keys/overmesh-manifests/version-01"
        );
    }

    #[test]
    fn key_vault_signer_accepts_an_overlapping_trusted_key() {
        let active_key = SigningKey::from_bytes((&[7_u8; 32]).into()).expect("active key");
        let active_public = active_key
            .verifying_key()
            .to_public_key_pem(LineEnding::LF)
            .expect("active public key");
        let retired_key = SigningKey::from_bytes((&[8_u8; 32]).into()).expect("retired key");
        let retired_public = retired_key
            .verifying_key()
            .to_public_key_pem(LineEnding::LF)
            .expect("retired public key");
        let retired_key_id = "https://example.vault.azure.net/keys/overmesh-manifests/version-00";
        let signer = KeyVaultManifestSigner::new(
            "https://example.vault.azure.net/",
            "overmesh-manifests",
            "version-01",
            &active_public,
            KeyValidity::new(0, u64::MAX).expect("validity"),
            vec![(
                retired_key_id.to_owned(),
                retired_public,
                KeyValidity::new(100, 200).expect("retired validity"),
            )],
            Arc::new(TestCredential),
        )
        .expect("Key Vault signer");
        let payload = b"trusted retired payload";
        let signature: Signature =
            retired_key.sign(&signature_input(SignatureDomain::CommitManifest, payload));

        for signed_at_unix_ms in [100, 200] {
            signer
                .verify(
                    retired_key_id,
                    SignatureDomain::CommitManifest,
                    signed_at_unix_ms,
                    payload,
                    &URL_SAFE_NO_PAD.encode(signature.to_bytes()),
                )
                .expect("retired overlap key remains trusted at inclusive boundary");
        }
    }

    #[tokio::test]
    async fn signed_time_boundaries_remain_readable_after_rotation() {
        let signer = signer();
        let payload = BlockManifest {
            blob: "/container/blob".to_owned(),
            write_id: "write-1".to_owned(),
            logical_version: 1,
            content_container: "container".to_owned(),
            content_object: ".overmesh/objects/path/content".to_owned(),
            content_length: 0,
            block_count: 1,
            page_size: BLOCK_MANIFEST_PAGE_SIZE,
            pages: vec![page_reference(0)],
            content_sha256: sha256_bytes(b""),
            ring_version: 1,
            signing_key_id: signer.key_id().to_owned(),
        };
        let signed =
            SignedDocument::create_at(payload, SignatureDomain::BlockManifest, &signer, 100)
                .await
                .expect("sign at boundary");
        signed
            .verify(
                SignatureDomain::BlockManifest,
                &signed.payload.signing_key_id,
                &signer,
            )
            .expect("old signed object remains readable");
    }

    #[test]
    fn rejects_retired_key_signature_outside_its_window() {
        let active_key = SigningKey::from_bytes((&[7_u8; 32]).into()).expect("active key");
        let active_public = active_key
            .verifying_key()
            .to_public_key_pem(LineEnding::LF)
            .expect("active public key");
        let retired_key = SigningKey::from_bytes((&[8_u8; 32]).into()).expect("retired key");
        let retired_public = retired_key
            .verifying_key()
            .to_public_key_pem(LineEnding::LF)
            .expect("retired public key");
        let retired_key_id = "https://example.vault.azure.net/keys/overmesh-manifests/version-00";
        let signer = KeyVaultManifestSigner::new(
            "https://example.vault.azure.net/",
            "overmesh-manifests",
            "version-01",
            &active_public,
            KeyValidity::new(0, u64::MAX).expect("validity"),
            vec![(
                retired_key_id.to_owned(),
                retired_public,
                KeyValidity::new(100, 200).expect("retired validity"),
            )],
            Arc::new(TestCredential),
        )
        .expect("Key Vault signer");
        let payload = b"late retired payload";
        let signature: Signature =
            retired_key.sign(&signature_input(SignatureDomain::CommitManifest, payload));

        assert!(matches!(
            signer.verify(
                retired_key_id,
                SignatureDomain::CommitManifest,
                201,
                payload,
                &URL_SAFE_NO_PAD.encode(signature.to_bytes()),
            ),
            Err(ManifestError::SignatureOutsideValidity { .. })
        ));
        assert!(matches!(
            signer.verify(
                "unknown-key",
                SignatureDomain::CommitManifest,
                150,
                payload,
                &URL_SAFE_NO_PAD.encode(signature.to_bytes()),
            ),
            Err(ManifestError::UnknownSigningKey(_))
        ));
    }
}
