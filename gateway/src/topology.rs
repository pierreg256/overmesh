use std::{collections::HashSet, sync::Arc};

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use azure_core::credentials::TokenCredential;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};

const ARM_SCOPE: &str = "https://management.azure.com/.default";
const STORAGE_API_VERSION: &str = "2023-05-01";

#[derive(Debug, Clone)]
pub struct StorageAccountBinding {
    pub backend_id: String,
    pub resource_id: String,
    pub expected_region: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageTopologyReport {
    pub api_version: &'static str,
    pub accounts: Vec<StorageTopologyAccount>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageTopologyAccount {
    pub backend_id: String,
    pub resource_id: String,
    pub region: String,
}

#[async_trait]
pub trait StorageTopologyValidator: Send + Sync {
    async fn validate(&self) -> Result<StorageTopologyReport>;
}

pub type SharedStorageTopologyValidator = Arc<dyn StorageTopologyValidator>;

pub struct DisabledStorageTopologyValidator;

#[async_trait]
impl StorageTopologyValidator for DisabledStorageTopologyValidator {
    async fn validate(&self) -> Result<StorageTopologyReport> {
        Ok(StorageTopologyReport {
            api_version: "overmesh.io/storage-topology/v1",
            accounts: Vec::new(),
        })
    }
}

pub struct AzureArmStorageTopologyValidator {
    credential: Arc<dyn TokenCredential>,
    client: Client,
    management_endpoint: String,
    accounts: Vec<StorageAccountBinding>,
}

impl AzureArmStorageTopologyValidator {
    pub fn new(
        credential: Arc<dyn TokenCredential>,
        management_endpoint: &str,
        accounts: Vec<StorageAccountBinding>,
    ) -> Result<Self> {
        ensure!(
            accounts.len() >= 2,
            "storage topology validation requires at least two Ring accounts"
        );
        let mut backend_ids = HashSet::new();
        let mut expected_regions = HashSet::new();
        for account in &accounts {
            ensure!(
                backend_ids.insert(account.backend_id.to_ascii_lowercase()),
                "storage topology contains duplicate backend {}",
                account.backend_id
            );
            ensure!(
                account.resource_id.starts_with("/subscriptions/"),
                "backend {} has an invalid Azure resource ID",
                account.backend_id
            );
            let expected = normalize_region(&account.expected_region);
            ensure!(
                !expected.is_empty(),
                "backend {} has an empty signed Ring region",
                account.backend_id
            );
            ensure!(
                expected_regions.insert(expected),
                "signed Ring contains duplicate Azure region {}",
                account.expected_region
            );
        }
        Ok(Self {
            credential,
            client: Client::new(),
            management_endpoint: management_endpoint.trim_end_matches('/').to_owned(),
            accounts,
        })
    }
}

#[async_trait]
impl StorageTopologyValidator for AzureArmStorageTopologyValidator {
    async fn validate(&self) -> Result<StorageTopologyReport> {
        let token = self
            .credential
            .get_token(&[ARM_SCOPE], None)
            .await
            .context("failed to acquire Azure ARM token for storage topology validation")?
            .token
            .secret()
            .to_owned();
        let mut regions = HashSet::new();
        let mut accounts = Vec::with_capacity(self.accounts.len());
        for binding in &self.accounts {
            let url = format!(
                "{}{}?api-version={STORAGE_API_VERSION}",
                self.management_endpoint, binding.resource_id
            );
            let response = self
                .client
                .get(&url)
                .bearer_auth(&token)
                .send()
                .await
                .with_context(|| {
                    format!(
                        "Azure ARM request failed for backend {}",
                        binding.backend_id
                    )
                })?;
            let status = response.status();
            if status != StatusCode::OK {
                let body = response.text().await.unwrap_or_default();
                bail!(
                    "Azure ARM returned {status} for backend {}: {body}",
                    binding.backend_id
                );
            }
            let account: ArmStorageAccount = response.json().await.with_context(|| {
                format!(
                    "Azure ARM returned invalid storage account JSON for backend {}",
                    binding.backend_id
                )
            })?;
            let actual = normalize_region(&account.location);
            let expected = normalize_region(&binding.expected_region);
            ensure!(
                actual == expected,
                "backend {} signed Ring region {} does not match Azure ARM location {}",
                binding.backend_id,
                binding.expected_region,
                account.location
            );
            ensure!(
                regions.insert(actual.clone()),
                "Azure ARM reports duplicate storage region {}",
                account.location
            );
            accounts.push(StorageTopologyAccount {
                backend_id: binding.backend_id.clone(),
                resource_id: binding.resource_id.clone(),
                region: actual,
            });
        }
        Ok(StorageTopologyReport {
            api_version: "overmesh.io/storage-topology/v1",
            accounts,
        })
    }
}

#[derive(Debug, Deserialize)]
struct ArmStorageAccount {
    location: String,
}

fn normalize_region(value: &str) -> String {
    value
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .flat_map(char::to_lowercase)
        .collect()
}

#[cfg(test)]
mod tests {
    use axum::{Json, Router, extract::Path, http::StatusCode as AxumStatusCode, routing::get};
    use azure_core::{
        credentials::{AccessToken, TokenRequestOptions},
        time::{Duration, OffsetDateTime},
    };
    use serde_json::{Value, json};
    use tokio::net::TcpListener;

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

    async fn arm_account(Path(path): Path<String>) -> (AxumStatusCode, Json<Value>) {
        let name = path.rsplit('/').next().unwrap_or_default();
        if name == "forbidden" {
            return (
                AxumStatusCode::FORBIDDEN,
                Json(json!({"error": "AuthorizationFailed"})),
            );
        }
        let location = match name {
            "storage-a" => "francecentral",
            "storage-b" => "swedencentral",
            "storage-c" => "norwayeast",
            "mismatch" => "westus",
            _ => return (AxumStatusCode::NOT_FOUND, Json(json!({"error": "missing"}))),
        };
        (AxumStatusCode::OK, Json(json!({"location": location})))
    }

    async fn endpoint() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        tokio::spawn(async move {
            axum::serve(listener, Router::new().route("/{*path}", get(arm_account)))
                .await
                .expect("mock ARM server");
        });
        format!("http://{address}")
    }

    fn binding(backend_id: &str, account_name: &str, region: &str) -> StorageAccountBinding {
        StorageAccountBinding {
            backend_id: backend_id.to_owned(),
            resource_id: format!(
                "/subscriptions/test/resourceGroups/test/providers/Microsoft.Storage/storageAccounts/{account_name}"
            ),
            expected_region: region.to_owned(),
        }
    }

    #[tokio::test]
    async fn validates_three_distinct_arm_regions() {
        let validator = AzureArmStorageTopologyValidator::new(
            Arc::new(TestCredential),
            &endpoint().await,
            vec![
                binding("a", "storage-a", "France Central"),
                binding("b", "storage-b", "swedencentral"),
                binding("c", "storage-c", "norwayeast"),
            ],
        )
        .expect("validator");
        let report = validator.validate().await.expect("valid topology");
        assert_eq!(report.accounts.len(), 3);
    }

    #[tokio::test]
    async fn rejects_signed_region_mismatch() {
        let validator = AzureArmStorageTopologyValidator::new(
            Arc::new(TestCredential),
            &endpoint().await,
            vec![
                binding("a", "storage-a", "francecentral"),
                binding("b", "mismatch", "swedencentral"),
            ],
        )
        .expect("validator");
        assert!(validator.validate().await.is_err());
    }

    #[tokio::test]
    async fn rejects_arm_authorization_failure() {
        let validator = AzureArmStorageTopologyValidator::new(
            Arc::new(TestCredential),
            &endpoint().await,
            vec![
                binding("a", "storage-a", "francecentral"),
                binding("b", "forbidden", "swedencentral"),
            ],
        )
        .expect("validator");
        let error = validator
            .validate()
            .await
            .expect_err("authorization failure");
        assert!(error.to_string().contains("403"));
    }

    #[tokio::test]
    async fn rejects_unavailable_arm_endpoint() {
        let validator = AzureArmStorageTopologyValidator::new(
            Arc::new(TestCredential),
            "http://127.0.0.1:1",
            vec![
                binding("a", "storage-a", "francecentral"),
                binding("b", "storage-b", "swedencentral"),
            ],
        )
        .expect("validator");
        assert!(validator.validate().await.is_err());
    }

    #[test]
    fn rejects_duplicate_signed_regions_before_arm() {
        assert!(
            AzureArmStorageTopologyValidator::new(
                Arc::new(TestCredential),
                "https://management.azure.com",
                vec![
                    binding("a", "storage-a", "France Central"),
                    binding("b", "storage-b", "francecentral"),
                ],
            )
            .is_err()
        );
    }
}
