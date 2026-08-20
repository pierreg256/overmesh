targetScope = 'resourceGroup'

@description('Primary registry region.')
param primaryLocation string

@description('Secondary registry replication region.')
param secondaryLocation string

@description('Third Storage Account region.')
param thirdStorageLocation string

@description('Globally unique suffix.')
param uniqueSuffix string

@description('Existing Storage Account A name.')
param storageAccountAName string

@description('Existing Storage Account B name.')
param storageAccountBName string

@description('Customer container created on every replica.')
param customerContainerName string

@description('Retained customer containers copied to the new replica before reconciliation.')
param retainedCustomerContainerNames array

@description('Persistent performance fixture containers created on every replica.')
param performanceFixtureContainerNames array

@description('Common resource tags.')
param tags object

var storageAccountCName = 'stomv090c${uniqueSuffix}'
var containerRegistryName = 'crovermesh090${uniqueSuffix}'
var systemContainerName = 'overmesh-system'

resource storageA 'Microsoft.Storage/storageAccounts@2025-01-01' existing = {
  name: storageAccountAName
}

resource storageB 'Microsoft.Storage/storageAccounts@2025-01-01' existing = {
  name: storageAccountBName
}

resource blobServiceA 'Microsoft.Storage/storageAccounts/blobServices@2025-01-01' existing = {
  parent: storageA
  name: 'default'
}

resource blobServiceB 'Microsoft.Storage/storageAccounts/blobServices@2025-01-01' existing = {
  parent: storageB
  name: 'default'
}

resource customerContainerA 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' = {
  parent: blobServiceA
  name: customerContainerName
  properties: {
    publicAccess: 'None'
  }
}

resource customerContainerB 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' = {
  parent: blobServiceB
  name: customerContainerName
  properties: {
    publicAccess: 'None'
  }
}

resource performanceFixtureContainersA 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' = [
  for containerName in performanceFixtureContainerNames: {
    parent: blobServiceA
    name: containerName
    properties: {
      publicAccess: 'None'
    }
  }
]

resource performanceFixtureContainersB 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' = [
  for containerName in performanceFixtureContainerNames: {
    parent: blobServiceB
    name: containerName
    properties: {
      publicAccess: 'None'
    }
  }
]

resource storageC 'Microsoft.Storage/storageAccounts@2025-01-01' = {
  name: storageAccountCName
  location: thirdStorageLocation
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    defaultToOAuthAuthentication: true
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
    publicNetworkAccess: 'Disabled'
    networkAcls: {
      bypass: 'None'
      defaultAction: 'Deny'
    }
  }
  tags: union(tags, {
    replica: 'c'
    region: thirdStorageLocation
  })
}

resource blobServiceC 'Microsoft.Storage/storageAccounts/blobServices@2025-01-01' = {
  parent: storageC
  name: 'default'
  properties: {
    isVersioningEnabled: true
    deleteRetentionPolicy: {
      enabled: true
      days: 7
    }
    containerDeleteRetentionPolicy: {
      enabled: true
      days: 7
    }
  }
}

resource systemContainerC 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' = {
  parent: blobServiceC
  name: systemContainerName
  properties: {
    publicAccess: 'None'
  }
}

resource customerContainerC 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' = {
  parent: blobServiceC
  name: customerContainerName
  properties: {
    publicAccess: 'None'
  }
}

resource performanceFixtureContainersC 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' = [
  for containerName in performanceFixtureContainerNames: {
    parent: blobServiceC
    name: containerName
    properties: {
      publicAccess: 'None'
    }
  }
]

resource retainedCustomerContainersC 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' = [
  for containerName in retainedCustomerContainerNames: {
    parent: blobServiceC
    name: containerName
    properties: {
      publicAccess: 'None'
    }
  }
]

resource registry 'Microsoft.ContainerRegistry/registries@2025-04-01' = {
  name: containerRegistryName
  location: primaryLocation
  sku: {
    name: 'Premium'
  }
  properties: {
    adminUserEnabled: false
    anonymousPullEnabled: false
    publicNetworkAccess: 'Disabled'
    networkRuleBypassOptions: 'AzureServices'
    policies: {
      exportPolicy: {
        status: 'disabled'
      }
      quarantinePolicy: {
        status: 'disabled'
      }
      retentionPolicy: {
        days: 7
        status: 'enabled'
      }
      trustPolicy: {
        status: 'disabled'
        type: 'Notary'
      }
    }
  }
  tags: tags
}

resource registryReplication 'Microsoft.ContainerRegistry/registries/replications@2025-04-01' = {
  parent: registry
  name: secondaryLocation
  location: secondaryLocation
  properties: {
    regionEndpointEnabled: true
    zoneRedundancy: 'Enabled'
  }
  tags: tags
}

output storageAccountAId string = storageA.id
output storageAccountBId string = storageB.id
output storageAccountCId string = storageC.id
output storageAccountCName string = storageC.name
output containerRegistryId string = registry.id
output containerRegistryName string = registry.name
output containerRegistryLoginServer string = registry.properties.loginServer
