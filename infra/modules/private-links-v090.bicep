targetScope = 'resourceGroup'

@description('Private Endpoint region.')
param location string

@description('Short regional suffix used in resource names.')
param regionalSuffix string

@description('Private Endpoint subnet resource ID.')
param privateEndpointSubnetId string

@description('Blob Private DNS zone resource ID.')
param blobPrivateDnsZoneId string

@description('Key Vault Private DNS zone resource ID.')
param keyVaultPrivateDnsZoneId string

@description('ACR Private DNS zone resource ID.')
param acrPrivateDnsZoneId string

@description('Storage Account resource IDs.')
param storageAccountIds array

@description('Key Vault resource ID.')
param keyVaultId string

@description('Container Registry resource ID.')
param containerRegistryId string

@description('Common resource tags.')
param tags object

resource storagePrivateEndpoints 'Microsoft.Network/privateEndpoints@2024-07-01' = [
  for (storageAccountId, index) in storageAccountIds: {
    name: 'pep-overmesh-v090-${regionalSuffix}-st${index + 1}-blob'
    location: location
    properties: {
      subnet: {
        id: privateEndpointSubnetId
      }
      privateLinkServiceConnections: [
        {
          name: 'blob'
          properties: {
            privateLinkServiceId: storageAccountId
            groupIds: [
              'blob'
            ]
          }
        }
      ]
    }
    tags: tags
  }
]

resource storagePrivateDnsZoneGroups 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2024-07-01' = [
  for (storageAccountId, index) in storageAccountIds: {
    parent: storagePrivateEndpoints[index]
    name: 'blob'
    properties: {
      privateDnsZoneConfigs: [
        {
          name: 'blob'
          properties: {
            privateDnsZoneId: blobPrivateDnsZoneId
          }
        }
      ]
    }
  }
]

resource keyVaultPrivateEndpoint 'Microsoft.Network/privateEndpoints@2024-07-01' = {
  name: 'pep-overmesh-v090-${regionalSuffix}-key-vault'
  location: location
  properties: {
    subnet: {
      id: privateEndpointSubnetId
    }
    privateLinkServiceConnections: [
      {
        name: 'vault'
        properties: {
          privateLinkServiceId: keyVaultId
          groupIds: [
            'vault'
          ]
        }
      }
    ]
  }
  tags: tags
}

resource keyVaultPrivateDnsZoneGroup 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2024-07-01' = {
  parent: keyVaultPrivateEndpoint
  name: 'vault'
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'vault'
        properties: {
          privateDnsZoneId: keyVaultPrivateDnsZoneId
        }
      }
    ]
  }
}

resource acrPrivateEndpoint 'Microsoft.Network/privateEndpoints@2024-07-01' = {
  name: 'pep-overmesh-v090-${regionalSuffix}-acr'
  location: location
  properties: {
    subnet: {
      id: privateEndpointSubnetId
    }
    privateLinkServiceConnections: [
      {
        name: 'registry'
        properties: {
          privateLinkServiceId: containerRegistryId
          groupIds: [
            'registry'
          ]
        }
      }
    ]
  }
  tags: tags
}

resource acrPrivateDnsZoneGroup 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2024-07-01' = {
  parent: acrPrivateEndpoint
  name: 'registry'
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'registry'
        properties: {
          privateDnsZoneId: acrPrivateDnsZoneId
        }
      }
    ]
  }
}
