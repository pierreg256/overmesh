use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use azure_core::credentials::TokenCredential;
use azure_identity::{
    AzureCliCredential, ManagedIdentityCredential, ManagedIdentityCredentialOptions,
    UserAssignedId, WorkloadIdentityCredential,
};
use overmesh_gateway::{
    backend::{HttpBlobBackend, SharedBackend},
    config::{
        BackendConfig, RingConfig, SigningConfig, TopologyValidationConfig,
        TopologyValidationProvider, build_manifest_signer,
    },
    manifest::ManifestSigner,
    ring::{SignedRing, TrustedRingKey, TrustedRingPredecessor},
    topology::{
        AzureArmStorageTopologyValidator, DisabledStorageTopologyValidator,
        SharedStorageTopologyValidator, StorageAccountBinding,
    },
};
use serde::Deserialize;

use crate::identity::{AzureIdentityTokenProvider, LocalTestTokenProvider, SharedTokenProvider};
use crate::posture::{
    AzureArmRbacPostureAuditor, DisabledRbacPostureAuditor, SharedRbacPostureAuditor,
    StorageAccountScope,
};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReconcilerConfig {
    pub ring: RingConfig,
    pub backends: Vec<BackendConfig>,
    pub signing: SigningConfig,
    pub identity: IdentityConfig,
    pub rbac_posture: RbacPostureConfig,
    #[serde(default)]
    pub topology_validation: TopologyValidationConfig,
    #[serde(default = "default_interval_seconds")]
    pub interval_seconds: u64,
    #[serde(default = "default_physical_collection_delay_seconds")]
    pub physical_collection_delay_seconds: u64,
    #[serde(default)]
    pub history_compaction: HistoryCompactionConfig,
    #[serde(default)]
    pub head_discovery: HeadDiscoveryConfig,
    #[serde(default)]
    pub staged_block_gc: StagedBlockGcConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StagedBlockGcConfig {
    #[serde(default = "default_staged_block_gc_max_records_per_cycle")]
    pub max_records_per_cycle: usize,
}

impl Default for StagedBlockGcConfig {
    fn default() -> Self {
        Self {
            max_records_per_cycle: default_staged_block_gc_max_records_per_cycle(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HistoryCompactionConfig {
    #[serde(default = "default_history_compaction_max_versions_per_cycle")]
    pub max_versions_per_cycle: usize,
}

impl Default for HistoryCompactionConfig {
    fn default() -> Self {
        Self {
            max_versions_per_cycle: default_history_compaction_max_versions_per_cycle(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HeadDiscoveryConfig {
    #[serde(default = "default_head_discovery_batch_size")]
    pub batch_size: usize,
}

impl Default for HeadDiscoveryConfig {
    fn default() -> Self {
        Self {
            batch_size: default_head_discovery_batch_size(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IdentityConfig {
    pub provider: IdentityProvider,
    pub managed_identity_client_id: Option<String>,
    pub test_token_path: Option<PathBuf>,
    #[serde(default)]
    pub allow_test_token: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum IdentityProvider {
    ManagedIdentity,
    WorkloadIdentity,
    AzureCli,
    LocalTest,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RbacPostureConfig {
    pub provider: RbacPostureProvider,
    #[serde(default = "default_management_endpoint")]
    pub management_endpoint: String,
    #[serde(default)]
    pub approved_system_principal_ids: Vec<String>,
    #[serde(default)]
    pub allow_disabled: bool,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RbacPostureProvider {
    AzureArm,
    Disabled,
}

pub struct ReconcilerRuntime {
    pub ring: SignedRing,
    pub backends: HashMap<String, SharedBackend>,
    pub signer: Arc<dyn ManifestSigner>,
    pub token_provider: SharedTokenProvider,
    pub posture_auditor: SharedRbacPostureAuditor,
    pub topology_validator: SharedStorageTopologyValidator,
    pub interval: Duration,
    pub physical_collection_delay: Duration,
    pub history_compaction_max_versions_per_cycle: usize,
    pub head_discovery_batch_size: usize,
    pub staged_block_gc_max_records_per_cycle: usize,
}

impl ReconcilerConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path).with_context(|| {
            format!("failed to read reconciler configuration {}", path.display())
        })?;
        let mut config: Self = serde_yaml::from_str(&content).with_context(|| {
            format!(
                "failed to parse reconciler configuration {}",
                path.display()
            )
        })?;
        let base = path.parent().unwrap_or_else(|| Path::new("."));
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
        if let Some(path) = config.identity.test_token_path.as_mut() {
            *path = resolve(base, path);
        }
        Ok(config)
    }

    pub fn build(self) -> Result<ReconcilerRuntime> {
        anyhow::ensure!(
            (1..=5_000).contains(&self.head_discovery.batch_size),
            "headDiscovery.batchSize must be between 1 and 5000"
        );
        anyhow::ensure!(
            (1..=5_000).contains(&self.staged_block_gc.max_records_per_cycle),
            "stagedBlockGc.maxRecordsPerCycle must be between 1 and 5000"
        );
        anyhow::ensure!(
            (1..=5_000).contains(&self.history_compaction.max_versions_per_cycle),
            "historyCompaction.maxVersionsPerCycle must be between 1 and 5000"
        );
        let mut trusted_ring_keys = Vec::with_capacity(self.ring.trusted_public_keys.len() + 1);
        trusted_ring_keys.push(TrustedRingKey {
            key_id: self.ring.key_id.clone(),
            public_key_path: self.ring.public_key_path.clone(),
            not_before_unix_ms: self.ring.not_before_unix_ms,
            not_after_unix_ms: self.ring.not_after_unix_ms,
        });
        trusted_ring_keys.extend(self.ring.trusted_public_keys.iter().map(|trusted| {
            TrustedRingKey {
                key_id: trusted.key_id.clone(),
                public_key_path: trusted.public_key_path.clone(),
                not_before_unix_ms: trusted.not_before_unix_ms,
                not_after_unix_ms: trusted.not_after_unix_ms,
            }
        }));
        let trusted_predecessor =
            self.ring
                .trusted_predecessor
                .as_ref()
                .map(|predecessor| TrustedRingPredecessor {
                    ring_version: predecessor.ring_version,
                    ring_hash: predecessor.ring_hash.clone(),
                });
        let ring = SignedRing::load(
            &self.ring.document_path,
            &self.ring.signature_path,
            &trusted_ring_keys,
            trusted_predecessor.as_ref(),
            self.ring.minimum_version,
        )?;
        let account_scopes = self
            .backends
            .iter()
            .map(|backend| {
                Ok(StorageAccountScope {
                    backend_id: backend.id.clone(),
                    resource_id: required(
                        backend.resource_id.clone(),
                        &format!("backends[{}].resourceId", backend.id),
                    )?,
                })
            })
            .collect::<Result<Vec<_>>>();
        let topology_bindings =
            if self.topology_validation.provider == TopologyValidationProvider::AzureArm {
                Some(
                    ring.document
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
                        .collect::<Result<Vec<_>>>()?,
                )
            } else {
                None
            };
        let mut backends = HashMap::new();
        for backend in self.backends {
            let value: SharedBackend = Arc::new(HttpBlobBackend::new(
                &backend.id,
                &backend.endpoint,
                backend.danger_accept_invalid_certificates,
            )?);
            anyhow::ensure!(
                backends.insert(backend.id.clone(), value).is_none(),
                "duplicate backend configuration {}",
                backend.id
            );
        }
        for node in &ring.document.nodes {
            anyhow::ensure!(
                backends.contains_key(&node.id),
                "Ring node {} has no backend configuration",
                node.id
            );
        }
        let signer = build_manifest_signer(&self.signing)?;
        let (token_provider, azure_credential): (
            SharedTokenProvider,
            Option<Arc<dyn TokenCredential>>,
        ) = match self.identity.provider {
            IdentityProvider::ManagedIdentity => {
                let credential: Arc<dyn TokenCredential> =
                    ManagedIdentityCredential::new(Some(ManagedIdentityCredentialOptions {
                        user_assigned_id: self
                            .identity
                            .managed_identity_client_id
                            .map(UserAssignedId::ClientId),
                        ..Default::default()
                    }))?;
                (
                    Arc::new(AzureIdentityTokenProvider::new(credential.clone())),
                    Some(credential),
                )
            }
            IdentityProvider::WorkloadIdentity => {
                let credential: Arc<dyn TokenCredential> = WorkloadIdentityCredential::new(None)?;
                (
                    Arc::new(AzureIdentityTokenProvider::new(credential.clone())),
                    Some(credential),
                )
            }
            IdentityProvider::AzureCli => {
                let credential: Arc<dyn TokenCredential> = AzureCliCredential::new(None)?;
                (
                    Arc::new(AzureIdentityTokenProvider::new(credential.clone())),
                    Some(credential),
                )
            }
            IdentityProvider::LocalTest => (
                Arc::new(LocalTestTokenProvider::new(
                    required(self.identity.test_token_path, "identity.testTokenPath")?,
                    self.identity.allow_test_token,
                )?),
                None,
            ),
        };
        let posture_auditor: SharedRbacPostureAuditor = match self.rbac_posture.provider {
            RbacPostureProvider::AzureArm => Arc::new(AzureArmRbacPostureAuditor::new(
                azure_credential.clone().context(
                    "Azure ARM RBAC posture auditing requires managed or workload identity",
                )?,
                &self.rbac_posture.management_endpoint,
                account_scopes?,
                self.rbac_posture.approved_system_principal_ids,
            )?),
            RbacPostureProvider::Disabled => {
                anyhow::ensure!(
                    self.rbac_posture.allow_disabled
                        && self.identity.provider == IdentityProvider::LocalTest,
                    "RBAC posture auditing may be disabled only for an explicitly allowed local test identity"
                );
                Arc::new(DisabledRbacPostureAuditor)
            }
        };
        let topology_validator: SharedStorageTopologyValidator = match self
            .topology_validation
            .provider
        {
            TopologyValidationProvider::AzureArm => {
                Arc::new(AzureArmStorageTopologyValidator::new(
                    azure_credential.context(
                        "Azure ARM topology validation requires managed or workload identity",
                    )?,
                    &self.topology_validation.management_endpoint,
                    topology_bindings.context("Azure ARM topology bindings are missing")?,
                )?)
            }
            TopologyValidationProvider::Disabled => {
                anyhow::ensure!(
                    self.topology_validation.allow_disabled
                        && self.identity.provider == IdentityProvider::LocalTest,
                    "topology validation may be disabled only for an explicitly allowed local test identity"
                );
                Arc::new(DisabledStorageTopologyValidator)
            }
        };
        Ok(ReconcilerRuntime {
            ring,
            backends,
            signer,
            token_provider,
            posture_auditor,
            topology_validator,
            interval: Duration::from_secs(self.interval_seconds.max(1)),
            physical_collection_delay: Duration::from_secs(self.physical_collection_delay_seconds),
            history_compaction_max_versions_per_cycle: self
                .history_compaction
                .max_versions_per_cycle,
            head_discovery_batch_size: self.head_discovery.batch_size,
            staged_block_gc_max_records_per_cycle: self.staged_block_gc.max_records_per_cycle,
        })
    }
}

fn required<T>(value: Option<T>, field: &str) -> Result<T> {
    value.with_context(|| format!("{field} is required"))
}

fn default_interval_seconds() -> u64 {
    300
}

fn default_physical_collection_delay_seconds() -> u64 {
    7 * 24 * 60 * 60
}

fn default_history_compaction_max_versions_per_cycle() -> usize {
    256
}

fn default_head_discovery_batch_size() -> usize {
    256
}

const fn default_staged_block_gc_max_records_per_cycle() -> usize {
    256
}

fn default_management_endpoint() -> String {
    "https://management.azure.com".to_owned()
}

fn resolve(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}
