targetScope = 'resourceGroup'

@description('Primary deployment region.')
param location string

@description('Globally unique suffix for Storage and Key Vault names.')
param uniqueSuffix string

@description('Operator object ID granted temporary Key Vault key administration.')
param operatorPrincipalId string

@description('SSH public key for the validation VM. The VM has no public IP.')
param sshPublicKey string

@description('Administrative username for the validation VM.')
param adminUsername string

var storageAccountAName = 'stomv050a${uniqueSuffix}'
var storageAccountBName = 'stomv050b${uniqueSuffix}'
var keyVaultName = 'kv-overmesh-v050-${uniqueSuffix}'
var systemContainerName = 'overmesh-system'
var customerContainerName = 'live-v050'
var blobContributorRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
)
var readerRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'acdd72a7-3385-48ef-bd42-f606fba81ae7'
)
var rbacReaderRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'f58310d9-a9f6-439a-9e8d-f62e7b41a168'
)
var keyVaultCryptoUserRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '12338af0-0e69-4776-bea7-57ae8d297424'
)
var keyVaultCryptoOfficerRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '14b46e9e-c2b7-41b4-b07b-48a6ebf60603'
)

resource gatewayIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2024-11-30' = {
  name: 'id-overmesh-gateway-v050'
  location: location
  tags: {
    purpose: 'overmesh-gateway-control'
    lifecycle: 'retained'
    autoDelete: 'false'
  }
}

resource reconcilerIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2024-11-30' = {
  name: 'id-overmesh-reconciler-v050'
  location: location
  tags: {
    purpose: 'overmesh-reconciler'
    lifecycle: 'retained'
    autoDelete: 'false'
  }
}

resource allowedIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2024-11-30' = {
  name: 'id-overmesh-caller-allowed-v050'
  location: location
  tags: {
    purpose: 'overmesh-allowed-canary'
    lifecycle: 'retained'
    autoDelete: 'false'
  }
}

resource deniedIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2024-11-30' = {
  name: 'id-overmesh-caller-denied-v050'
  location: location
  tags: {
    purpose: 'overmesh-denied-canary'
    lifecycle: 'retained'
    autoDelete: 'false'
  }
}

resource natPublicIp 'Microsoft.Network/publicIPAddresses@2024-07-01' = {
  name: 'pip-overmesh-v050-nat'
  location: location
  sku: {
    name: 'Standard'
  }
  properties: {
    publicIPAllocationMethod: 'Static'
  }
  tags: {
    lifecycle: 'retained'
    autoDelete: 'false'
  }
}

resource natGateway 'Microsoft.Network/natGateways@2024-07-01' = {
  name: 'ng-overmesh-v050-live'
  location: location
  sku: {
    name: 'Standard'
  }
  properties: {
    idleTimeoutInMinutes: 4
    publicIpAddresses: [
      {
        id: natPublicIp.id
      }
    ]
  }
  tags: {
    lifecycle: 'retained'
    autoDelete: 'false'
  }
}

resource validationNsg 'Microsoft.Network/networkSecurityGroups@2024-07-01' = {
  name: 'nsg-overmesh-v050-validation'
  location: location
  properties: {
    securityRules: []
  }
  tags: {
    lifecycle: 'retained'
    autoDelete: 'false'
  }
}

resource validationVnet 'Microsoft.Network/virtualNetworks@2024-07-01' = {
  name: 'vnet-overmesh-v050-live'
  location: location
  properties: {
    addressSpace: {
      addressPrefixes: [
        '10.50.0.0/16'
      ]
    }
    subnets: [
      {
        name: 'snet-validation'
        properties: {
          addressPrefix: '10.50.1.0/24'
          networkSecurityGroup: {
            id: validationNsg.id
          }
          natGateway: {
            id: natGateway.id
          }
        }
      }
      {
        name: 'snet-private-endpoints'
        properties: {
          addressPrefix: '10.50.2.0/27'
          privateEndpointNetworkPolicies: 'Disabled'
        }
      }
    ]
  }
  tags: {
    lifecycle: 'retained'
    autoDelete: 'false'
  }
}

resource storageA 'Microsoft.Storage/storageAccounts@2025-01-01' = {
  name: storageAccountAName
  location: 'francecentral'
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
    publicNetworkAccess: 'Enabled'
    networkAcls: {
      bypass: 'None'
      defaultAction: 'Allow'
    }
  }
  tags: {
    replica: 'a'
    lifecycle: 'retained'
    autoDelete: 'false'
  }
}

resource storageB 'Microsoft.Storage/storageAccounts@2025-01-01' = {
  name: storageAccountBName
  location: 'swedencentral'
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
    publicNetworkAccess: 'Enabled'
    networkAcls: {
      bypass: 'None'
      defaultAction: 'Allow'
    }
  }
  tags: {
    replica: 'b'
    lifecycle: 'retained'
    autoDelete: 'false'
  }
}

resource blobServiceA 'Microsoft.Storage/storageAccounts/blobServices@2025-01-01' = {
  parent: storageA
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

resource blobServiceB 'Microsoft.Storage/storageAccounts/blobServices@2025-01-01' = {
  parent: storageB
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

resource systemContainerA 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' = {
  parent: blobServiceA
  name: systemContainerName
  properties: {
    publicAccess: 'None'
  }
}

resource customerContainerA 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' = {
  parent: blobServiceA
  name: customerContainerName
  properties: {
    publicAccess: 'None'
  }
}

resource systemContainerB 'Microsoft.Storage/storageAccounts/blobServices/containers@2025-01-01' = {
  parent: blobServiceB
  name: systemContainerName
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

resource blobPrivateDns 'Microsoft.Network/privateDnsZones@2024-06-01' = {
  name: 'privatelink.blob.${environment().suffixes.storage}'
  location: 'global'
}

resource blobPrivateDnsVnetLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = {
  parent: blobPrivateDns
  name: 'link-overmesh-v050'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: validationVnet.id
    }
  }
}

resource keyVaultPrivateDns 'Microsoft.Network/privateDnsZones@2024-06-01' = {
  name: 'privatelink.vaultcore.azure.net'
  location: 'global'
}

resource keyVaultPrivateDnsVnetLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = {
  parent: keyVaultPrivateDns
  name: 'link-overmesh-v050'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: validationVnet.id
    }
  }
}

resource privateEndpointA 'Microsoft.Network/privateEndpoints@2024-07-01' = {
  name: 'pep-overmesh-storage-a-blob'
  location: location
  properties: {
    subnet: {
      id: validationVnet.properties.subnets[1].id
    }
    privateLinkServiceConnections: [
      {
        name: 'storage-a-blob'
        properties: {
          privateLinkServiceId: storageA.id
          groupIds: [
            'blob'
          ]
        }
      }
    ]
  }
  tags: {
    lifecycle: 'retained'
    autoDelete: 'false'
  }
}

resource privateEndpointB 'Microsoft.Network/privateEndpoints@2024-07-01' = {
  name: 'pep-overmesh-storage-b-blob'
  location: location
  properties: {
    subnet: {
      id: validationVnet.properties.subnets[1].id
    }
    privateLinkServiceConnections: [
      {
        name: 'storage-b-blob'
        properties: {
          privateLinkServiceId: storageB.id
          groupIds: [
            'blob'
          ]
        }
      }
    ]
  }
  tags: {
    lifecycle: 'retained'
    autoDelete: 'false'
  }
}

resource privateDnsZoneGroupA 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2024-07-01' = {
  parent: privateEndpointA
  name: 'default'
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'blob'
        properties: {
          privateDnsZoneId: blobPrivateDns.id
        }
      }
    ]
  }
}

resource privateDnsZoneGroupB 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2024-07-01' = {
  parent: privateEndpointB
  name: 'default'
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'blob'
        properties: {
          privateDnsZoneId: blobPrivateDns.id
        }
      }
    ]
  }
}

resource keyVault 'Microsoft.KeyVault/vaults@2024-11-01' = {
  name: keyVaultName
  location: location
  properties: {
    tenantId: subscription().tenantId
    enableRbacAuthorization: true
    enablePurgeProtection: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 7
    publicNetworkAccess: 'Disabled'
    sku: {
      family: 'A'
      name: 'standard'
    }
  }
  tags: {
    lifecycle: 'retained'
    autoDelete: 'false'
  }
}

resource keyVaultPrivateEndpoint 'Microsoft.Network/privateEndpoints@2024-07-01' = {
  name: 'pep-overmesh-key-vault'
  location: location
  properties: {
    subnet: {
      id: validationVnet.properties.subnets[1].id
    }
    privateLinkServiceConnections: [
      {
        name: 'key-vault'
        properties: {
          privateLinkServiceId: keyVault.id
          groupIds: [
            'vault'
          ]
        }
      }
    ]
  }
  tags: {
    lifecycle: 'retained'
    autoDelete: 'false'
  }
}

resource keyVaultPrivateDnsZoneGroup 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2024-07-01' = {
  parent: keyVaultPrivateEndpoint
  name: 'default'
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'vault'
        properties: {
          privateDnsZoneId: keyVaultPrivateDns.id
        }
      }
    ]
  }
}

resource operatorCryptoOfficer 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(keyVault.id, operatorPrincipalId, keyVaultCryptoOfficerRoleId)
  scope: keyVault
  properties: {
    principalId: operatorPrincipalId
    principalType: 'User'
    roleDefinitionId: keyVaultCryptoOfficerRoleId
  }
}

resource gatewayCryptoUser 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(keyVault.id, gatewayIdentity.name, keyVaultCryptoUserRoleId)
  scope: keyVault
  properties: {
    principalId: gatewayIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: keyVaultCryptoUserRoleId
  }
}

resource reconcilerCryptoUser 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(keyVault.id, reconcilerIdentity.name, keyVaultCryptoUserRoleId)
  scope: keyVault
  properties: {
    principalId: reconcilerIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: keyVaultCryptoUserRoleId
  }
}

resource signingKey 'Microsoft.KeyVault/vaults/keys@2024-11-01' = {
  parent: keyVault
  name: 'overmesh-manifests'
  properties: {
    attributes: {
      enabled: true
      exportable: false
    }
    curveName: 'P-256'
    keyOps: [
      'sign'
      'verify'
    ]
    kty: 'EC'
  }
  dependsOn: [
    operatorCryptoOfficer
  ]
}

resource gatewaySystemA 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(systemContainerA.id, gatewayIdentity.name, blobContributorRoleId)
  scope: systemContainerA
  properties: {
    principalId: gatewayIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobContributorRoleId
  }
}

resource gatewaySystemB 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(systemContainerB.id, gatewayIdentity.name, blobContributorRoleId)
  scope: systemContainerB
  properties: {
    principalId: gatewayIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobContributorRoleId
  }
}

resource reconcilerSystemA 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(systemContainerA.id, reconcilerIdentity.name, blobContributorRoleId)
  scope: systemContainerA
  properties: {
    principalId: reconcilerIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobContributorRoleId
  }
}

resource reconcilerSystemB 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(systemContainerB.id, reconcilerIdentity.name, blobContributorRoleId)
  scope: systemContainerB
  properties: {
    principalId: reconcilerIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobContributorRoleId
  }
}

resource reconcilerCustomerA 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(customerContainerA.id, reconcilerIdentity.name, blobContributorRoleId)
  scope: customerContainerA
  properties: {
    principalId: reconcilerIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobContributorRoleId
  }
}

resource reconcilerCustomerB 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(customerContainerB.id, reconcilerIdentity.name, blobContributorRoleId)
  scope: customerContainerB
  properties: {
    principalId: reconcilerIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobContributorRoleId
  }
}

resource allowedCustomerA 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(customerContainerA.id, allowedIdentity.name, blobContributorRoleId)
  scope: customerContainerA
  properties: {
    principalId: allowedIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobContributorRoleId
  }
}

resource allowedCustomerB 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(customerContainerB.id, allowedIdentity.name, blobContributorRoleId)
  scope: customerContainerB
  properties: {
    principalId: allowedIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobContributorRoleId
  }
}

resource reconcilerReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(resourceGroup().id, reconcilerIdentity.name, readerRoleId)
  scope: resourceGroup()
  properties: {
    principalId: reconcilerIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: readerRoleId
  }
}

resource reconcilerRbacReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(resourceGroup().id, reconcilerIdentity.name, rbacReaderRoleId)
  scope: resourceGroup()
  properties: {
    principalId: reconcilerIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: rbacReaderRoleId
  }
}

resource validationNic 'Microsoft.Network/networkInterfaces@2024-07-01' = {
  name: 'nic-overmesh-v050-validation'
  location: location
  properties: {
    ipConfigurations: [
      {
        name: 'ipconfig'
        properties: {
          privateIPAllocationMethod: 'Dynamic'
          subnet: {
            id: validationVnet.properties.subnets[0].id
          }
        }
      }
    ]
  }
  tags: {
    lifecycle: 'retained'
    autoDelete: 'false'
  }
}

resource validationVm 'Microsoft.Compute/virtualMachines@2024-11-01' = {
  name: 'vm-overmesh-v050'
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${allowedIdentity.id}': {}
      '${deniedIdentity.id}': {}
      '${reconcilerIdentity.id}': {}
    }
  }
  properties: {
    hardwareProfile: {
      vmSize: 'Standard_B1s'
    }
    osProfile: {
      computerName: 'omv050'
      adminUsername: adminUsername
      linuxConfiguration: {
        disablePasswordAuthentication: true
        ssh: {
          publicKeys: [
            {
              keyData: sshPublicKey
              path: '/home/${adminUsername}/.ssh/authorized_keys'
            }
          ]
        }
      }
    }
    storageProfile: {
      imageReference: {
        publisher: 'Canonical'
        offer: 'ubuntu-24_04-lts'
        sku: 'server'
        version: 'latest'
      }
      osDisk: {
        createOption: 'FromImage'
        managedDisk: {
          storageAccountType: 'Standard_LRS'
        }
      }
    }
    networkProfile: {
      networkInterfaces: [
        {
          id: validationNic.id
          properties: {
            primary: true
          }
        }
      ]
    }
    diagnosticsProfile: {
      bootDiagnostics: {
        enabled: true
      }
    }
  }
  tags: {
    lifecycle: 'retained'
    autoDelete: 'false'
  }
}

output storageAccountAName string = storageA.name
output storageAccountBName string = storageB.name
output storageAccountAId string = storageA.id
output storageAccountBId string = storageB.id
output gatewayIdentityClientId string = gatewayIdentity.properties.clientId
output gatewayIdentityPrincipalId string = gatewayIdentity.properties.principalId
output reconcilerIdentityClientId string = reconcilerIdentity.properties.clientId
output reconcilerIdentityPrincipalId string = reconcilerIdentity.properties.principalId
output allowedIdentityClientId string = allowedIdentity.properties.clientId
output deniedIdentityClientId string = deniedIdentity.properties.clientId
output keyVaultName string = keyVault.name
output signingKeyId string = signingKey.properties.keyUriWithVersion
output validationVmName string = validationVm.name
