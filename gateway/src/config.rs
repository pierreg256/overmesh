use std::{
    collections::HashMap,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use azure_core::credentials::TokenCredential;
use azure_identity::{
    AzureCliCredential, ManagedIdentityCredential, ManagedIdentityCredentialOptions,
    UserAssignedId, WorkloadIdentityCredential,
};
use serde::Deserialize;

use crate::{
    auth::Authenticator,
    backend::{HttpBlobBackend, SharedBackend},
    commit::{CommitService, CommitServiceOptions},
    identity::{AzureControlTokenProvider, LocalControlTokenProvider, SharedControlTokenProvider},
    manifest::{KeyValidity, KeyVaultManifestSigner, LocalTestManifestSigner, ManifestSigner},
    ring::{SignedRing, TrustedRingKey, TrustedRingPredecessor},
    topology::{
        AzureArmStorageTopologyValidator, DisabledStorageTopologyValidator,
        SharedStorageTopologyValidator, StorageAccountBinding,
    },
};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GatewayConfig {
    pub listen_address: SocketAddr,
    pub logical_account: String,
    pub authentication: AuthenticationConfig,
    pub control_identity: ControlIdentityConfig,
    pub ring: RingConfig,
    pub backends: Vec<BackendConfig>,
    pub signing: SigningConfig,
    #[serde(default)]
    pub topology_validation: TopologyValidationConfig,
    #[serde(default)]
    pub listing: ListingConfig,
    #[serde(default)]
    pub staged_blocks: StagedBlocksConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TopologyValidationConfig {
    #[serde(default)]
    pub provider: TopologyValidationProvider,
    #[serde(default = "default_management_endpoint")]
    pub management_endpoint: String,
    #[serde(default)]
    pub allow_disabled: bool,
}

impl Default for TopologyValidationConfig {
    fn default() -> Self {
        Self {
            provider: TopologyValidationProvider::AzureArm,
            management_endpoint: default_management_endpoint(),
            allow_disabled: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TopologyValidationProvider {
    #[default]
    AzureArm,
    Disabled,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ListingConfig {
    #[serde(default = "default_continuation_token_lifetime_seconds")]
    pub continuation_token_lifetime_seconds: u64,
}

impl Default for ListingConfig {
    fn default() -> Self {
        Self {
            continuation_token_lifetime_seconds: default_continuation_token_lifetime_seconds(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StagedBlocksConfig {
    #[serde(default = "default_staged_block_retention_seconds")]
    pub retention_seconds: u64,
}

impl Default for StagedBlocksConfig {
    fn default() -> Self {
        Self {
            retention_seconds: default_staged_block_retention_seconds(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AuthenticationConfig {
    pub issuer: String,
    pub audience: String,
    pub tenant_id: String,
    pub jwks_path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ControlIdentityConfig {
    pub provider: ControlIdentityProvider,
    pub managed_identity_client_id: Option<String>,
    pub test_token_path: Option<PathBuf>,
    #[serde(default)]
    pub allow_test_token: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ControlIdentityProvider {
    ManagedIdentity,
    WorkloadIdentity,
    LocalTest,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RingConfig {
    pub document_path: PathBuf,
    pub signature_path: PathBuf,
    pub key_id: String,
    pub public_key_path: PathBuf,
    pub not_before_unix_ms: u64,
    pub not_after_unix_ms: u64,
    #[serde(default)]
    pub trusted_public_keys: Vec<TrustedPublicKeyConfig>,
    pub trusted_predecessor: Option<TrustedPredecessorConfig>,
    pub minimum_version: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BackendConfig {
    pub id: String,
    pub endpoint: String,
    pub resource_id: Option<String>,
    #[serde(default)]
    pub danger_accept_invalid_certificates: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SigningConfig {
    pub provider: SigningProvider,
    pub key_id: String,
    pub not_before_unix_ms: u64,
    pub not_after_unix_ms: u64,
    #[serde(default)]
    pub allow_test_signer: bool,
    pub vault_url: Option<String>,
    pub key_name: Option<String>,
    pub key_version: Option<String>,
    pub public_key_path: Option<PathBuf>,
    #[serde(default)]
    pub trusted_public_keys: Vec<TrustedPublicKeyConfig>,
    pub credential: Option<AzureCredentialProvider>,
    pub managed_identity_client_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TrustedPublicKeyConfig {
    pub key_id: String,
    pub public_key_path: PathBuf,
    pub not_before_unix_ms: u64,
    pub not_after_unix_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TrustedPredecessorConfig {
    pub ring_version: u64,
    pub ring_hash: String,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SigningProvider {
    LocalTest,
    AzureKeyVault,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AzureCredentialProvider {
    ManagedIdentity,
    WorkloadIdentity,
    AzureCli,
}

impl GatewayConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read gateway configuration {}", path.display()))?;
        let mut config: Self = serde_yaml::from_str(&content)
            .with_context(|| format!("failed to parse gateway configuration {}", path.display()))?;
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        config.authentication.jwks_path = resolve(base, &config.authentication.jwks_path);
        if let Some(path) = config.control_identity.test_token_path.as_mut() {
            *path = resolve(base, path);
        }
        config.ring.document_path = resolve(base, &config.ring.document_path);
        config.ring.signature_path = resolve(base, &config.ring.signature_path);
        config.ring.public_key_path = resolve(base, &config.ring.public_key_path);
        for trusted_key in &mut config.ring.trusted_public_keys {
            trusted_key.public_key_path = resolve(base, &trusted_key.public_key_path);
        }
        if let Some(path) = config.signing.public_key_path.as_mut() {
            *path = resolve(base, path);
        }
        for trusted_key in &mut config.signing.trusted_public_keys {
            trusted_key.public_key_path = resolve(base, &trusted_key.public_key_path);
        }
        Ok(config)
    }

    pub fn load_authenticator(&self) -> Result<Authenticator> {
        let jwks = fs::read_to_string(&self.authentication.jwks_path).with_context(|| {
            format!(
                "failed to read JWKS {}",
                self.authentication.jwks_path.display()
            )
        })?;
        Authenticator::from_jwks_json(
            &self.authentication.issuer,
            &self.authentication.audience,
            &self.authentication.tenant_id,
            &jwks,
        )
    }

    pub fn load_ring(&self) -> Result<SignedRing> {
        let mut trusted_keys = Vec::with_capacity(self.ring.trusted_public_keys.len() + 1);
        trusted_keys.push(TrustedRingKey {
            key_id: self.ring.key_id.clone(),
            public_key_path: self.ring.public_key_path.clone(),
            not_before_unix_ms: self.ring.not_before_unix_ms,
            not_after_unix_ms: self.ring.not_after_unix_ms,
        });
        trusted_keys.extend(
            self.ring
                .trusted_public_keys
                .iter()
                .map(|trusted| TrustedRingKey {
                    key_id: trusted.key_id.clone(),
                    public_key_path: trusted.public_key_path.clone(),
                    not_before_unix_ms: trusted.not_before_unix_ms,
                    not_after_unix_ms: trusted.not_after_unix_ms,
                }),
        );
        let trusted_predecessor =
            self.ring
                .trusted_predecessor
                .as_ref()
                .map(|predecessor| TrustedRingPredecessor {
                    ring_version: predecessor.ring_version,
                    ring_hash: predecessor.ring_hash.clone(),
                });
        SignedRing::load(
            &self.ring.document_path,
            &self.ring.signature_path,
            &trusted_keys,
            trusted_predecessor.as_ref(),
            self.ring.minimum_version,
        )
    }

    pub fn load_commit_service(&self, signed_ring: Arc<SignedRing>) -> Result<CommitService> {
        anyhow::ensure!(
            (60..=24 * 60 * 60).contains(&self.listing.continuation_token_lifetime_seconds),
            "listing.continuationTokenLifetimeSeconds must be between 60 and 86400"
        );
        anyhow::ensure!(
            (60..=30 * 24 * 60 * 60).contains(&self.staged_blocks.retention_seconds),
            "stagedBlocks.retentionSeconds must be between 60 and 2592000"
        );
        let mut backends = HashMap::new();
        for backend in &self.backends {
            let value: SharedBackend = Arc::new(HttpBlobBackend::new(
                &backend.id,
                &backend.endpoint,
                backend.danger_accept_invalid_certificates,
            )?);
            if backends.insert(backend.id.clone(), value).is_some() {
                anyhow::bail!("duplicate backend configuration {}", backend.id);
            }
        }
        for node in &signed_ring.document.nodes {
            if !backends.contains_key(&node.id) {
                anyhow::bail!("Ring node {} has no backend configuration", node.id);
            }
        }
        let signer = build_manifest_signer(&self.signing)?;
        let control_tokens = build_control_token_provider(&self.control_identity)?;
        Ok(CommitService::new_with_options(
            signed_ring,
            backends,
            signer,
            control_tokens,
            CommitServiceOptions {
                listing_token_lifetime: std::time::Duration::from_secs(
                    self.listing.continuation_token_lifetime_seconds,
                ),
                staging_lifetime: std::time::Duration::from_secs(
                    self.staged_blocks.retention_seconds,
                ),
            },
        ))
    }

    pub fn load_topology_validator(
        &self,
        signed_ring: &SignedRing,
    ) -> Result<SharedStorageTopologyValidator> {
        match self.topology_validation.provider {
            TopologyValidationProvider::AzureArm => {
                let accounts = signed_ring
                    .document
                    .nodes
                    .iter()
                    .map(|node| {
                        let backend = self
                            .backends
                            .iter()
                            .find(|backend| backend.id == node.id)
                            .with_context(|| {
                                format!("Ring node {} has no backend configuration", node.id)
                            })?;
                        Ok(StorageAccountBinding {
                            backend_id: node.id.clone(),
                            resource_id: required(
                                backend.resource_id.clone(),
                                &format!("backends[{}].resourceId", backend.id),
                            )?,
                            expected_region: node.region.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let credential = build_control_credential(&self.control_identity)?.context(
                    "Azure ARM topology validation requires managed or workload identity",
                )?;
                Ok(Arc::new(AzureArmStorageTopologyValidator::new(
                    credential,
                    &self.topology_validation.management_endpoint,
                    accounts,
                )?))
            }
            TopologyValidationProvider::Disabled => {
                anyhow::ensure!(
                    self.topology_validation.allow_disabled
                        && self.control_identity.provider == ControlIdentityProvider::LocalTest,
                    "topology validation may be disabled only for an explicitly allowed local test identity"
                );
                Ok(Arc::new(DisabledStorageTopologyValidator))
            }
        }
    }
}

fn build_control_token_provider(
    identity: &ControlIdentityConfig,
) -> Result<SharedControlTokenProvider> {
    let provider: SharedControlTokenProvider = match build_control_credential(identity)? {
        Some(credential) => Arc::new(AzureControlTokenProvider::new(credential)),
        None => {
            anyhow::ensure!(
                identity.provider == ControlIdentityProvider::LocalTest,
                "Azure control identity credential is unavailable"
            );
            Arc::new(LocalControlTokenProvider::new(
                required(
                    identity.test_token_path.clone(),
                    "controlIdentity.testTokenPath",
                )?,
                identity.allow_test_token,
            )?)
        }
    };
    Ok(provider)
}

fn build_control_credential(
    identity: &ControlIdentityConfig,
) -> Result<Option<Arc<dyn TokenCredential>>> {
    let credential: Option<Arc<dyn TokenCredential>> = match identity.provider {
        ControlIdentityProvider::ManagedIdentity => Some(ManagedIdentityCredential::new(Some(
            ManagedIdentityCredentialOptions {
                user_assigned_id: identity
                    .managed_identity_client_id
                    .clone()
                    .map(UserAssignedId::ClientId),
                ..Default::default()
            },
        ))?),
        ControlIdentityProvider::WorkloadIdentity => Some(WorkloadIdentityCredential::new(None)?),
        ControlIdentityProvider::LocalTest => None,
    };
    Ok(credential)
}

pub fn build_manifest_signer(signing: &SigningConfig) -> Result<Arc<dyn ManifestSigner>> {
    let active_validity = KeyValidity::new(signing.not_before_unix_ms, signing.not_after_unix_ms)?;
    let signer: Arc<dyn ManifestSigner> = match signing.provider {
        SigningProvider::LocalTest => Arc::new(LocalTestManifestSigner::new(
            &signing.key_id,
            signing.allow_test_signer,
            active_validity,
        )?),
        SigningProvider::AzureKeyVault => {
            let credential: Arc<dyn TokenCredential> =
                match required(signing.credential, "signing.credential")? {
                    AzureCredentialProvider::ManagedIdentity => {
                        let options = ManagedIdentityCredentialOptions {
                            user_assigned_id: signing
                                .managed_identity_client_id
                                .clone()
                                .map(UserAssignedId::ClientId),
                            ..Default::default()
                        };
                        ManagedIdentityCredential::new(Some(options))?
                    }
                    AzureCredentialProvider::WorkloadIdentity => {
                        WorkloadIdentityCredential::new(None)?
                    }
                    AzureCredentialProvider::AzureCli => AzureCliCredential::new(None)?,
                };
            let public_key_path =
                required(signing.public_key_path.as_ref(), "signing.publicKeyPath")?;
            let public_key_pem = fs::read_to_string(public_key_path).with_context(|| {
                format!(
                    "failed to read manifest signing public key {}",
                    public_key_path.display()
                )
            })?;
            let additional_trusted_keys = signing
                .trusted_public_keys
                .iter()
                .map(|trusted| {
                    let pem = fs::read_to_string(&trusted.public_key_path).with_context(|| {
                        format!(
                            "failed to read trusted manifest public key {}",
                            trusted.public_key_path.display()
                        )
                    })?;
                    Ok((
                        trusted.key_id.clone(),
                        pem,
                        KeyValidity::new(trusted.not_before_unix_ms, trusted.not_after_unix_ms)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let signer = KeyVaultManifestSigner::new(
                required(signing.vault_url.as_deref(), "signing.vaultUrl")?,
                required(signing.key_name.as_deref(), "signing.keyName")?,
                required(signing.key_version.as_deref(), "signing.keyVersion")?,
                &public_key_pem,
                active_validity,
                additional_trusted_keys,
                credential,
            )?;
            if signer.key_id() != signing.key_id {
                anyhow::bail!(
                    "signing.keyId must equal the pinned Azure Key Vault key version URI"
                );
            }
            Arc::new(signer)
        }
    };
    Ok(signer)
}

fn required<T>(value: Option<T>, field: &str) -> Result<T> {
    value.with_context(|| format!("{field} is required"))
}

fn resolve(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

const fn default_continuation_token_lifetime_seconds() -> u64 {
    15 * 60
}

const fn default_staged_block_retention_seconds() -> u64 {
    7 * 24 * 60 * 60
}

fn default_management_endpoint() -> String {
    "https://management.azure.com".to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_config_loads_the_trusted_root_ring() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config/local.yaml");
        let config = GatewayConfig::load(&path).expect("local config");
        let ring = config.load_ring().expect("trusted local Ring");
        assert!(ring.document.root);
        assert_eq!(ring.document.ring_version, 1);
    }
}
