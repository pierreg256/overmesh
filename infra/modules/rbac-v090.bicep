targetScope = 'resourceGroup'

@description('Storage Account A name.')
param storageAccountAName string

@description('Storage Account B name.')
param storageAccountBName string

@description('Storage Account C name.')
param storageAccountCName string

@description('Container Registry name.')
param containerRegistryName string

@description('Gateway managed identity principal ID.')
param gatewayPrincipalId string

@description('Reconciler managed identity principal ID.')
param reconcilerPrincipalId string

@description('Positive caller canary managed identity principal ID.')
param allowedCallerPrincipalId string

@description('System control container name.')
param systemContainerName string

@description('Customer validation container name.')
param customerContainerName string

@description('Retained customer containers requiring symmetric access on Storage C.')
param retainedCustomerContainerNames array

@description('Persistent performance fixture containers requiring symmetric access.')
param performanceFixtureContainerNames array

var readerRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'acdd72a7-3385-48ef-bd42-f606fba81ae7'
)
var blobContributorRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
)
var acrPullRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '7f951dda-4ed3-4680-a7ca-43fe172d538d'
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

resource retainedCustomerContainersC 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' existing = [
  for containerName in retainedCustomerContainerNames: {
    parent: blobServiceC
    name: containerName
  }
]

resource performanceFixtureContainersA 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' existing = [
  for containerName in performanceFixtureContainerNames: {
    parent: blobServiceA
    name: containerName
  }
]

resource performanceFixtureContainersB 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' existing = [
  for containerName in performanceFixtureContainerNames: {
    parent: blobServiceB
    name: containerName
  }
]

resource performanceFixtureContainersC 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' existing = [
  for containerName in performanceFixtureContainerNames: {
    parent: blobServiceC
    name: containerName
  }
]

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

resource gatewaySystemC 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(systemContainerC.id, gatewayPrincipalId, blobContributorRoleId)
  scope: systemContainerC
  properties: {
    principalId: gatewayPrincipalId
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

resource reconcilerRetainedCustomersC 'Microsoft.Authorization/roleAssignments@2022-04-01' = [
  for (containerName, index) in retainedCustomerContainerNames: {
    name: guid(retainedCustomerContainersC[index].id, reconcilerPrincipalId, blobContributorRoleId)
    scope: retainedCustomerContainersC[index]
    properties: {
      principalId: reconcilerPrincipalId
      principalType: 'ServicePrincipal'
      roleDefinitionId: blobContributorRoleId
    }
  }
]

resource callerRetainedCustomersC 'Microsoft.Authorization/roleAssignments@2022-04-01' = [
  for (containerName, index) in retainedCustomerContainerNames: {
    name: guid(retainedCustomerContainersC[index].id, allowedCallerPrincipalId, blobContributorRoleId)
    scope: retainedCustomerContainersC[index]
    properties: {
      principalId: allowedCallerPrincipalId
      principalType: 'ServicePrincipal'
      roleDefinitionId: blobContributorRoleId
    }
  }
]

resource reconcilerPerformanceFixturesA 'Microsoft.Authorization/roleAssignments@2022-04-01' = [
  for (containerName, index) in performanceFixtureContainerNames: {
    name: guid(performanceFixtureContainersA[index].id, reconcilerPrincipalId, blobContributorRoleId)
    scope: performanceFixtureContainersA[index]
    properties: {
      principalId: reconcilerPrincipalId
      principalType: 'ServicePrincipal'
      roleDefinitionId: blobContributorRoleId
    }
  }
]

resource reconcilerPerformanceFixturesB 'Microsoft.Authorization/roleAssignments@2022-04-01' = [
  for (containerName, index) in performanceFixtureContainerNames: {
    name: guid(performanceFixtureContainersB[index].id, reconcilerPrincipalId, blobContributorRoleId)
    scope: performanceFixtureContainersB[index]
    properties: {
      principalId: reconcilerPrincipalId
      principalType: 'ServicePrincipal'
      roleDefinitionId: blobContributorRoleId
    }
  }
]

resource reconcilerPerformanceFixturesC 'Microsoft.Authorization/roleAssignments@2022-04-01' = [
  for (containerName, index) in performanceFixtureContainerNames: {
    name: guid(performanceFixtureContainersC[index].id, reconcilerPrincipalId, blobContributorRoleId)
    scope: performanceFixtureContainersC[index]
    properties: {
      principalId: reconcilerPrincipalId
      principalType: 'ServicePrincipal'
      roleDefinitionId: blobContributorRoleId
    }
  }
]

resource callerPerformanceFixturesA 'Microsoft.Authorization/roleAssignments@2022-04-01' = [
  for (containerName, index) in performanceFixtureContainerNames: {
    name: guid(performanceFixtureContainersA[index].id, allowedCallerPrincipalId, blobContributorRoleId)
    scope: performanceFixtureContainersA[index]
    properties: {
      principalId: allowedCallerPrincipalId
      principalType: 'ServicePrincipal'
      roleDefinitionId: blobContributorRoleId
    }
  }
]

resource callerPerformanceFixturesB 'Microsoft.Authorization/roleAssignments@2022-04-01' = [
  for (containerName, index) in performanceFixtureContainerNames: {
    name: guid(performanceFixtureContainersB[index].id, allowedCallerPrincipalId, blobContributorRoleId)
    scope: performanceFixtureContainersB[index]
    properties: {
      principalId: allowedCallerPrincipalId
      principalType: 'ServicePrincipal'
      roleDefinitionId: blobContributorRoleId
    }
  }
]

resource callerPerformanceFixturesC 'Microsoft.Authorization/roleAssignments@2022-04-01' = [
  for (containerName, index) in performanceFixtureContainerNames: {
    name: guid(performanceFixtureContainersC[index].id, allowedCallerPrincipalId, blobContributorRoleId)
    scope: performanceFixtureContainersC[index]
    properties: {
      principalId: allowedCallerPrincipalId
      principalType: 'ServicePrincipal'
      roleDefinitionId: blobContributorRoleId
    }
  }
]

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
