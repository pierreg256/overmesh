use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use azure_core::credentials::TokenCredential;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};

const ARM_SCOPE: &str = "https://management.azure.com/.default";
const AUTHORIZATION_API_VERSION: &str = "2022-04-01";
const STORAGE_API_VERSION: &str = "2023-05-01";
const SYSTEM_CONTAINER: &str = "overmesh-system";
const BLOB_DATA_OPERATIONS: &[&str] = &[
    "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/read",
    "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/write",
    "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/delete",
    "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/add/action",
    "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/move/action",
    "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/permanentDelete/action",
    "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/tags/action",
];

#[derive(Debug, Clone)]
pub struct StorageAccountScope {
    pub backend_id: String,
    pub resource_id: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RbacPostureReport {
    pub api_version: &'static str,
    pub accounts: Vec<AccountPostureReport>,
    pub approved_system_principals: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountPostureReport {
    pub backend_id: String,
    pub customer_containers: usize,
    pub effective_data_assignments: usize,
}

#[async_trait]
pub trait RbacPostureAuditor: Send + Sync {
    async fn audit(&self) -> Result<RbacPostureReport>;
}

pub type SharedRbacPostureAuditor = Arc<dyn RbacPostureAuditor>;

pub struct DisabledRbacPostureAuditor;

#[async_trait]
impl RbacPostureAuditor for DisabledRbacPostureAuditor {
    async fn audit(&self) -> Result<RbacPostureReport> {
        Ok(RbacPostureReport {
            api_version: "reconciler.overmesh.io/rbac-posture/v1",
            accounts: Vec::new(),
            approved_system_principals: 0,
        })
    }
}

pub struct AzureArmRbacPostureAuditor {
    credential: Arc<dyn TokenCredential>,
    client: Client,
    management_endpoint: String,
    accounts: Vec<StorageAccountScope>,
    approved_system_principals: HashSet<String>,
}

impl AzureArmRbacPostureAuditor {
    pub fn new(
        credential: Arc<dyn TokenCredential>,
        management_endpoint: &str,
        accounts: Vec<StorageAccountScope>,
        approved_system_principals: Vec<String>,
    ) -> Result<Self> {
        ensure!(
            accounts.len() >= 2,
            "RBAC posture auditing requires at least two backend account scopes"
        );
        ensure!(
            !approved_system_principals.is_empty(),
            "at least one approved system principal is required"
        );
        for account in &accounts {
            ensure!(
                account.resource_id.starts_with("/subscriptions/"),
                "backend {} has an invalid Azure resource ID",
                account.backend_id
            );
        }
        Ok(Self {
            credential,
            client: Client::new(),
            management_endpoint: management_endpoint.trim_end_matches('/').to_owned(),
            accounts,
            approved_system_principals: approved_system_principals
                .into_iter()
                .map(|value| value.to_ascii_lowercase())
                .collect(),
        })
    }

    async fn arm_get<T: for<'de> Deserialize<'de>>(&self, url: &str, token: &str) -> Result<T> {
        let response = self
            .client
            .get(url)
            .bearer_auth(token)
            .send()
            .await
            .with_context(|| format!("Azure ARM request failed for {url}"))?;
        let status = response.status();
        if status != StatusCode::OK {
            let body = response.text().await.unwrap_or_default();
            bail!("Azure ARM returned {status} for {url}: {body}");
        }
        response
            .json()
            .await
            .with_context(|| format!("Azure ARM returned invalid JSON for {url}"))
    }

    async fn list_containers(
        &self,
        account: &StorageAccountScope,
        token: &str,
    ) -> Result<Vec<String>> {
        let mut url = format!(
            "{}{}/blobServices/default/containers?api-version={STORAGE_API_VERSION}",
            self.management_endpoint, account.resource_id
        );
        let mut containers = Vec::new();
        loop {
            let page: ArmList<ArmContainer> = self.arm_get(&url, token).await?;
            containers.extend(page.value.into_iter().map(|container| container.name));
            match page.next_link {
                Some(next) if !next.is_empty() => url = next,
                _ => break,
            }
        }
        Ok(containers)
    }

    async fn list_assignments(&self, scope: &str, token: &str) -> Result<Vec<ArmRoleAssignment>> {
        let mut url = reqwest::Url::parse(&format!(
            "{}{scope}/providers/Microsoft.Authorization/roleAssignments",
            self.management_endpoint
        ))?;
        url.query_pairs_mut()
            .append_pair("api-version", AUTHORIZATION_API_VERSION)
            .append_pair("$filter", "atScope()");
        let mut next = url.to_string();
        let mut assignments = Vec::new();
        loop {
            let page: ArmList<ArmRoleAssignment> = self.arm_get(&next, token).await?;
            assignments.extend(page.value);
            match page.next_link {
                Some(value) if !value.is_empty() => next = value,
                _ => break,
            }
        }
        Ok(assignments)
    }

    async fn load_role_definition(
        &self,
        role_definition_id: &str,
        token: &str,
    ) -> Result<ArmRoleDefinition> {
        let url = format!(
            "{}{}?api-version={AUTHORIZATION_API_VERSION}",
            self.management_endpoint, role_definition_id
        );
        self.arm_get(&url, token).await
    }

    async fn snapshot_account(
        &self,
        account: &StorageAccountScope,
        token: &str,
        role_cache: &mut HashMap<String, BTreeSet<String>>,
    ) -> Result<AccountSnapshot> {
        let containers = self.list_containers(account, token).await?;
        ensure!(
            containers.iter().any(|name| name == SYSTEM_CONTAINER),
            "backend {} does not expose the reserved system container through Azure ARM",
            account.backend_id
        );
        let mut result = BTreeMap::new();
        for container in containers {
            let scope = format!(
                "{}/blobServices/default/containers/{container}",
                account.resource_id
            );
            let assignments = self.list_assignments(&scope, token).await?;
            let mut effective = BTreeSet::new();
            for assignment in assignments {
                let role_id = assignment
                    .properties
                    .role_definition_id
                    .to_ascii_lowercase();
                let operations = if let Some(cached) = role_cache.get(&role_id) {
                    cached.clone()
                } else {
                    let definition = self
                        .load_role_definition(&assignment.properties.role_definition_id, token)
                        .await?;
                    let value = effective_blob_operations(&definition);
                    role_cache.insert(role_id.clone(), value.clone());
                    value
                };
                if operations.is_empty() {
                    continue;
                }
                effective.insert(AssignmentFingerprint {
                    principal_id: assignment.properties.principal_id.to_ascii_lowercase(),
                    role_definition_id: role_definition_guid(&role_id)?,
                    condition: normalize_condition(assignment.properties.condition.as_deref()),
                    condition_version: assignment.properties.condition_version.unwrap_or_default(),
                    scope_kind: scope_kind(
                        assignment.properties.scope.as_deref(),
                        &account.resource_id,
                        &scope,
                    ),
                    operations,
                });
            }
            result.insert(container, effective);
        }
        Ok(AccountSnapshot {
            backend_id: account.backend_id.clone(),
            containers: result,
        })
    }
}

#[async_trait]
impl RbacPostureAuditor for AzureArmRbacPostureAuditor {
    async fn audit(&self) -> Result<RbacPostureReport> {
        let token = self
            .credential
            .get_token(&[ARM_SCOPE], None)
            .await?
            .token
            .secret()
            .to_owned();
        let mut role_cache = HashMap::new();
        let mut snapshots = Vec::with_capacity(self.accounts.len());
        for account in &self.accounts {
            snapshots.push(
                self.snapshot_account(account, &token, &mut role_cache)
                    .await?,
            );
        }
        evaluate_snapshots(&snapshots, &self.approved_system_principals)?;
        Ok(RbacPostureReport {
            api_version: "reconciler.overmesh.io/rbac-posture/v1",
            accounts: snapshots
                .iter()
                .map(|snapshot| AccountPostureReport {
                    backend_id: snapshot.backend_id.clone(),
                    customer_containers: snapshot
                        .containers
                        .keys()
                        .filter(|name| name.as_str() != SYSTEM_CONTAINER)
                        .count(),
                    effective_data_assignments: snapshot
                        .containers
                        .values()
                        .map(BTreeSet::len)
                        .sum(),
                })
                .collect(),
            approved_system_principals: self.approved_system_principals.len(),
        })
    }
}

#[derive(Debug)]
struct AccountSnapshot {
    backend_id: String,
    containers: BTreeMap<String, BTreeSet<AssignmentFingerprint>>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct AssignmentFingerprint {
    principal_id: String,
    role_definition_id: String,
    condition: String,
    condition_version: String,
    scope_kind: String,
    operations: BTreeSet<String>,
}

fn evaluate_snapshots(
    snapshots: &[AccountSnapshot],
    approved_system_principals: &HashSet<String>,
) -> Result<()> {
    ensure!(
        snapshots.len() >= 2,
        "RBAC posture comparison requires at least two account snapshots"
    );
    for snapshot in snapshots {
        let system_assignments = snapshot.containers.get(SYSTEM_CONTAINER).with_context(|| {
            format!(
                "backend {} is missing the reserved system container",
                snapshot.backend_id
            )
        })?;
        for assignment in system_assignments {
            ensure!(
                approved_system_principals.contains(&assignment.principal_id),
                "unapproved principal {} has effective blob data access to {} on backend {}",
                assignment.principal_id,
                SYSTEM_CONTAINER,
                snapshot.backend_id
            );
        }
    }
    let first = &snapshots[0];
    for other in &snapshots[1..] {
        ensure!(
            first.containers.keys().eq(other.containers.keys()),
            "customer-container sets differ between backends {} and {}",
            first.backend_id,
            other.backend_id
        );
        for (container, first_assignments) in &first.containers {
            let other_assignments = other
                .containers
                .get(container)
                .context("container disappeared during RBAC posture comparison")?;
            ensure!(
                first_assignments == other_assignments,
                "effective RBAC assignments or conditions differ for container {container} between backends {} and {}",
                first.backend_id,
                other.backend_id
            );
        }
    }
    Ok(())
}

fn effective_blob_operations(definition: &ArmRoleDefinition) -> BTreeSet<String> {
    BLOB_DATA_OPERATIONS
        .iter()
        .filter(|operation| {
            definition.properties.permissions.iter().any(|permission| {
                permission
                    .data_actions
                    .iter()
                    .any(|pattern| wildcard_matches(pattern, operation))
                    && !permission
                        .not_data_actions
                        .iter()
                        .any(|pattern| wildcard_matches(pattern, operation))
            })
        })
        .map(|operation| (*operation).to_owned())
        .collect()
}

fn wildcard_matches(pattern: &str, value: &str) -> bool {
    let pattern = pattern.to_ascii_lowercase();
    let value = value.to_ascii_lowercase();
    let parts = pattern.split('*').collect::<Vec<_>>();
    if parts.len() == 1 {
        return pattern == value;
    }
    let mut remainder = value.as_str();
    for (index, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if index == 0 && !pattern.starts_with('*') {
            let Some(next) = remainder.strip_prefix(part) else {
                return false;
            };
            remainder = next;
            continue;
        }
        let Some(position) = remainder.find(part) else {
            return false;
        };
        remainder = &remainder[position + part.len()..];
    }
    pattern.ends_with('*') || remainder.is_empty()
}

fn role_definition_guid(role_definition_id: &str) -> Result<String> {
    role_definition_id
        .rsplit('/')
        .next()
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .context("role definition ID has no terminal identifier")
}

fn normalize_condition(condition: Option<&str>) -> String {
    condition
        .unwrap_or_default()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn scope_kind(scope: Option<&str>, account_scope: &str, container_scope: &str) -> String {
    let Some(scope) = scope else {
        return "unknown".to_owned();
    };
    if scope.eq_ignore_ascii_case(container_scope) {
        "container".to_owned()
    } else if scope.eq_ignore_ascii_case(account_scope) {
        "account".to_owned()
    } else if scope.to_ascii_lowercase().contains("/resourcegroups/")
        && !scope.to_ascii_lowercase().contains("/providers/")
    {
        "resourceGroup".to_owned()
    } else if scope.matches('/').count() == 2 {
        "subscription".to_owned()
    } else {
        "inherited".to_owned()
    }
}

#[derive(Debug, Deserialize)]
struct ArmList<T> {
    value: Vec<T>,
    #[serde(rename = "nextLink")]
    next_link: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ArmContainer {
    name: String,
}

#[derive(Debug, Deserialize)]
struct ArmRoleAssignment {
    properties: ArmRoleAssignmentProperties,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArmRoleAssignmentProperties {
    role_definition_id: String,
    principal_id: String,
    scope: Option<String>,
    condition: Option<String>,
    condition_version: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ArmRoleDefinition {
    properties: ArmRoleDefinitionProperties,
}

#[derive(Debug, Deserialize)]
struct ArmRoleDefinitionProperties {
    #[serde(default)]
    permissions: Vec<ArmPermission>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArmPermission {
    #[serde(default)]
    data_actions: Vec<String>,
    #[serde(default)]
    not_data_actions: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assignment(principal: &str) -> AssignmentFingerprint {
        AssignmentFingerprint {
            principal_id: principal.to_owned(),
            role_definition_id: "role".to_owned(),
            condition: String::new(),
            condition_version: String::new(),
            scope_kind: "container".to_owned(),
            operations: BTreeSet::from([BLOB_DATA_OPERATIONS[0].to_owned()]),
        }
    }

    fn snapshot(
        backend_id: &str,
        system_principal: &str,
        customer_principal: &str,
    ) -> AccountSnapshot {
        AccountSnapshot {
            backend_id: backend_id.to_owned(),
            containers: BTreeMap::from([
                (
                    SYSTEM_CONTAINER.to_owned(),
                    BTreeSet::from([assignment(system_principal)]),
                ),
                (
                    "photos".to_owned(),
                    BTreeSet::from([assignment(system_principal), assignment(customer_principal)]),
                ),
            ]),
        }
    }

    #[test]
    fn accepts_only_symmetric_approved_posture() {
        let snapshots = [
            snapshot("storage-a", "system", "caller"),
            snapshot("storage-b", "system", "caller"),
            snapshot("storage-c", "system", "caller"),
        ];
        evaluate_snapshots(&snapshots, &HashSet::from(["system".to_owned()]))
            .expect("safe posture");
    }

    #[test]
    fn rejects_unapproved_system_container_access() {
        let snapshots = [
            snapshot("storage-a", "unapproved", "caller"),
            snapshot("storage-b", "unapproved", "caller"),
        ];
        assert!(evaluate_snapshots(&snapshots, &HashSet::from(["system".to_owned()])).is_err());
    }

    #[test]
    fn rejects_replica_role_asymmetry() {
        let snapshots = [
            snapshot("storage-a", "system", "caller-a"),
            snapshot("storage-b", "system", "caller-b"),
        ];
        assert!(evaluate_snapshots(&snapshots, &HashSet::from(["system".to_owned()])).is_err());
    }

    #[test]
    fn honors_data_action_exclusions() {
        let definition = ArmRoleDefinition {
            properties: ArmRoleDefinitionProperties {
                permissions: vec![ArmPermission {
                    data_actions: vec![
                        "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/*"
                            .to_owned(),
                    ],
                    not_data_actions: vec![
                        "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/delete"
                            .to_owned(),
                    ],
                }],
            },
        };
        let operations = effective_blob_operations(&definition);
        assert!(operations.contains(BLOB_DATA_OPERATIONS[0]));
        assert!(!operations.contains(BLOB_DATA_OPERATIONS[2]));
    }
}
