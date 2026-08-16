use std::{fmt, fs, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use async_trait::async_trait;
use azure_core::credentials::TokenCredential;
use serde::{Deserialize, Serialize};

const STORAGE_SCOPE: &str = "https://storage.azure.com/.default";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CallerIdentity {
    pub tenant_id: String,
    pub object_id: String,
    pub subject: String,
    pub authorized_party: Option<String>,
}

#[derive(Clone)]
pub struct CallerToken(String);

impl CallerToken {
    pub(crate) fn new(value: String) -> Self {
        Self(value)
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for CallerToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CallerToken([REDACTED])")
    }
}

#[derive(Clone)]
pub struct ControlToken(String);

impl ControlToken {
    pub(crate) fn new(value: String) -> Self {
        Self(value)
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ControlToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ControlToken([REDACTED])")
    }
}

#[async_trait]
pub trait ControlTokenProvider: Send + Sync {
    async fn token(&self) -> Result<ControlToken>;
}

pub type SharedControlTokenProvider = Arc<dyn ControlTokenProvider>;

pub struct AzureControlTokenProvider {
    credential: Arc<dyn TokenCredential>,
}

impl AzureControlTokenProvider {
    pub fn new(credential: Arc<dyn TokenCredential>) -> Self {
        Self { credential }
    }
}

#[async_trait]
impl ControlTokenProvider for AzureControlTokenProvider {
    async fn token(&self) -> Result<ControlToken> {
        Ok(ControlToken::new(
            self.credential
                .get_token(&[STORAGE_SCOPE], None)
                .await?
                .token
                .secret()
                .to_owned(),
        ))
    }
}

pub struct LocalControlTokenProvider {
    path: PathBuf,
}

impl LocalControlTokenProvider {
    pub fn new(path: PathBuf, explicitly_allowed: bool) -> Result<Self> {
        anyhow::ensure!(
            explicitly_allowed,
            "the local control token provider requires allowTestToken: true"
        );
        Ok(Self { path })
    }
}

#[async_trait]
impl ControlTokenProvider for LocalControlTokenProvider {
    async fn token(&self) -> Result<ControlToken> {
        let token = fs::read_to_string(&self.path)
            .with_context(|| format!("failed to read control token {}", self.path.display()))?;
        let token = token.trim();
        anyhow::ensure!(!token.is_empty(), "the local control token is empty");
        Ok(ControlToken::new(token.to_owned()))
    }
}
