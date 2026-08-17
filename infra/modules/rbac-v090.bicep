targetScope = 'resourceGroup'

@description('Storage Account A name.')
param storageAccountAName string

@description('Storage Account B name.')
param storageAccountBName string

@description('Storage Account C name.')
param storageAccountCName string

@description('Key Vault name.')
param keyVaultName string

@description('Container Registry name.')
param containerRegistryName string

@description('Gateway managed identity principal ID.')
param gatewayPrincipalId string

@description('Reconciler managed identity principal ID.')
param reconcilerPrincipalId string

@description('Positive caller canary managed identity principal ID.')
param allowedCallerPrincipalId string

@description('GitHub Actions OIDC publisher principal ID.')
param githubPublisherPrincipalId string

@description('System control container name.')
param systemContainerName string

@description('Customer validation container name.')
param customerContainerName string

var readerRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'acdd72a7-3385-48ef-bd42-f606fba81ae7'
)
var blobContributorRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
)
var keyVaultCryptoUserRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '12338af0-0e69-4776-bea7-57ae8d297424'
)
var acrPullRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '7f951dda-4ed3-4680-a7ca-43fe172d538d'
)
var acrPushRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '8311e382-0749-4cb8-b61a-304f252e45ec'
)
var acrTasksContributorRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'fb382eab-e894-4461-af04-94435c366c3f'
)

resource storageA 'Microsoft.Storage/storageAccounts@2025-01-01' existing = {
  name: storageAccountAName
}

resource storageB 'Microsoft.Storage/storageAccounts@2025-01-01' existing = {
  name: storageAccountBName
}

resource storageC 'Microsoft.Storage/storageAccounts@2025-01-01' existing = {
  name: storageAccountCName
}

resource blobServiceA 'Microsoft.Storage/storageAccounts/blobServices@2025-01-01' existing = {
  parent: storageA
  name: 'default'
}

resource blobServiceB 'Microsoft.Storage/storageAccounts/blobServices@2025-01-01' existing = {
  parent: storageB
  name: 'default'
}

resource blobServiceC 'Microsoft.Storage/storageAccounts/blobServices@2025-01-01' existing = {
  parent: storageC
  name: 'default'
}

resource systemContainerA 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' existing = {
  parent: blobServiceA
  name: systemContainerName
}

resource systemContainerB 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' existing = {
  parent: blobServiceB
  name: systemContainerName
}

resource systemContainerC 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' existing = {
  parent: blobServiceC
  name: systemContainerName
}

resource customerContainerA 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' existing = {
  parent: blobServiceA
  name: customerContainerName
}

resource customerContainerB 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' existing = {
  parent: blobServiceB
  name: customerContainerName
}

resource customerContainerC 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' existing = {
  parent: blobServiceC
  name: customerContainerName
}

resource keyVault 'Microsoft.KeyVault/vaults@2024-11-01' existing = {
  name: keyVaultName
}

resource registry 'Microsoft.ContainerRegistry/registries@2025-04-01' existing = {
  name: containerRegistryName
}

resource gatewayReaderA 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageA.id, gatewayPrincipalId, readerRoleId)
  scope: storageA
  properties: {
    principalId: gatewayPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: readerRoleId
  }
}

resource gatewayReaderB 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageB.id, gatewayPrincipalId, readerRoleId)
  scope: storageB
  properties: {
    principalId: gatewayPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: readerRoleId
  }
}

resource gatewayReaderC 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageC.id, gatewayPrincipalId, readerRoleId)
  scope: storageC
  properties: {
    principalId: gatewayPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: readerRoleId
  }
}

resource reconcilerReaderA 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageA.id, reconcilerPrincipalId, readerRoleId)
  scope: storageA
  properties: {
    principalId: reconcilerPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: readerRoleId
  }
}

resource reconcilerReaderB 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageB.id, reconcilerPrincipalId, readerRoleId)
  scope: storageB
  properties: {
    principalId: reconcilerPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: readerRoleId
  }
}

resource reconcilerReaderC 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageC.id, reconcilerPrincipalId, readerRoleId)
  scope: storageC
  properties: {
    principalId: reconcilerPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: readerRoleId
  }
}

resource gatewaySystemA 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(systemContainerA.id, gatewayPrincipalId, blobContributorRoleId)
  scope: systemContainerA
  properties: {
    principalId: gatewayPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobContributorRoleId
  }
}

resource gatewaySystemB 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(systemContainerB.id, gatewayPrincipalId, blobContributorRoleId)
  scope: systemContainerB
  properties: {
    principalId: gatewayPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobContributorRoleId
  }
}

resource gatewaySystemC 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(systemContainerC.id, gatewayPrincipalId, blobContributorRoleId)
  scope: systemContainerC
  properties: {
    principalId: gatewayPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobContributorRoleId
  }
}

resource reconcilerSystemA 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(systemContainerA.id, reconcilerPrincipalId, blobContributorRoleId)
  scope: systemContainerA
  properties: {
    principalId: reconcilerPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobContributorRoleId
  }
}

resource reconcilerSystemB 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(systemContainerB.id, reconcilerPrincipalId, blobContributorRoleId)
  scope: systemContainerB
  properties: {
    principalId: reconcilerPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobContributorRoleId
  }
}

resource reconcilerSystemC 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(systemContainerC.id, reconcilerPrincipalId, blobContributorRoleId)
  scope: systemContainerC
  properties: {
    principalId: reconcilerPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobContributorRoleId
  }
}

resource reconcilerCustomerA 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(customerContainerA.id, reconcilerPrincipalId, blobContributorRoleId)
  scope: customerContainerA
  properties: {
    principalId: reconcilerPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobContributorRoleId
  }
}

resource reconcilerCustomerB 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(customerContainerB.id, reconcilerPrincipalId, blobContributorRoleId)
  scope: customerContainerB
  properties: {
    principalId: reconcilerPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobContributorRoleId
  }
}

resource reconcilerCustomerC 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(customerContainerC.id, reconcilerPrincipalId, blobContributorRoleId)
  scope: customerContainerC
  properties: {
    principalId: reconcilerPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobContributorRoleId
  }
}

resource callerCustomerA 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(customerContainerA.id, allowedCallerPrincipalId, blobContributorRoleId)
  scope: customerContainerA
  properties: {
    principalId: allowedCallerPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobContributorRoleId
  }
}

resource callerCustomerB 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(customerContainerB.id, allowedCallerPrincipalId, blobContributorRoleId)
  scope: customerContainerB
  properties: {
    principalId: allowedCallerPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobContributorRoleId
  }
}

resource callerCustomerC 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(customerContainerC.id, allowedCallerPrincipalId, blobContributorRoleId)
  scope: customerContainerC
  properties: {
    principalId: allowedCallerPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobContributorRoleId
  }
}

resource gatewayKeyVault 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(keyVault.id, gatewayPrincipalId, keyVaultCryptoUserRoleId)
  scope: keyVault
  properties: {
    principalId: gatewayPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: keyVaultCryptoUserRoleId
  }
}

resource reconcilerKeyVault 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(keyVault.id, reconcilerPrincipalId, keyVaultCryptoUserRoleId)
  scope: keyVault
  properties: {
    principalId: reconcilerPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: keyVaultCryptoUserRoleId
  }
}

resource gatewayAcrPull 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, gatewayPrincipalId, acrPullRoleId)
  scope: registry
  properties: {
    principalId: gatewayPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: acrPullRoleId
  }
}

resource reconcilerAcrPull 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, reconcilerPrincipalId, acrPullRoleId)
  scope: registry
  properties: {
    principalId: reconcilerPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: acrPullRoleId
  }
}

resource githubAcrPush 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, githubPublisherPrincipalId, acrPushRoleId)
  scope: registry
  properties: {
    principalId: githubPublisherPrincipalId
    roleDefinitionId: acrPushRoleId
  }
}

resource githubAcrTasksContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, githubPublisherPrincipalId, acrTasksContributorRoleId)
  scope: registry
  properties: {
    principalId: githubPublisherPrincipalId
    roleDefinitionId: acrTasksContributorRoleId
  }
}
