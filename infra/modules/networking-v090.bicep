targetScope = 'resourceGroup'

@description('Primary ACA region.')
param primaryLocation string

@description('Secondary ACA region.')
param secondaryLocation string

@description('Retained validation VNet connected to the private DNS zones.')
param retainedValidationVnetName string

@description('Common resource tags.')
param tags object

var primaryVnetName = 'vnet-overmesh-v090-frc'
var secondaryVnetName = 'vnet-overmesh-v090-swe'
var acaSubnetName = 'snet-aca'
var privateEndpointSubnetName = 'snet-private-endpoints'

resource primaryPublicIp 'Microsoft.Network/publicIPAddresses@2024-07-01' = {
  name: 'pip-overmesh-v090-frc'
  location: primaryLocation
  sku: {
    name: 'Standard'
  }
  properties: {
    publicIPAllocationMethod: 'Static'
  }
  tags: tags
}

resource secondaryPublicIp 'Microsoft.Network/publicIPAddresses@2024-07-01' = {
  name: 'pip-overmesh-v090-swe'
  location: secondaryLocation
  sku: {
    name: 'Standard'
  }
  properties: {
    publicIPAllocationMethod: 'Static'
  }
  tags: tags
}

resource primaryNatGateway 'Microsoft.Network/natGateways@2024-07-01' = {
  name: 'ng-overmesh-v090-frc'
  location: primaryLocation
  sku: {
    name: 'Standard'
  }
  properties: {
    idleTimeoutInMinutes: 4
    publicIpAddresses: [
      {
        id: primaryPublicIp.id
      }
    ]
  }
  tags: tags
}

resource secondaryNatGateway 'Microsoft.Network/natGateways@2024-07-01' = {
  name: 'ng-overmesh-v090-swe'
  location: secondaryLocation
  sku: {
    name: 'Standard'
  }
  properties: {
    idleTimeoutInMinutes: 4
    publicIpAddresses: [
      {
        id: secondaryPublicIp.id
      }
    ]
  }
  tags: tags
}

resource primaryVnet 'Microsoft.Network/virtualNetworks@2024-07-01' = {
  name: primaryVnetName
  location: primaryLocation
  properties: {
    addressSpace: {
      addressPrefixes: [
        '10.90.0.0/16'
      ]
    }
    subnets: [
      {
        name: acaSubnetName
        properties: {
          addressPrefix: '10.90.0.0/23'
          natGateway: {
            id: primaryNatGateway.id
          }
          delegations: [
            {
              name: 'Microsoft.App.environments'
              properties: {
                serviceName: 'Microsoft.App/environments'
              }
            }
          ]
        }
      }
      {
        name: privateEndpointSubnetName
        properties: {
          addressPrefix: '10.90.4.0/24'
          privateEndpointNetworkPolicies: 'Disabled'
        }
      }
    ]
  }
  tags: tags
}

resource secondaryVnet 'Microsoft.Network/virtualNetworks@2024-07-01' = {
  name: secondaryVnetName
  location: secondaryLocation
  properties: {
    addressSpace: {
      addressPrefixes: [
        '10.91.0.0/16'
      ]
    }
    subnets: [
      {
        name: acaSubnetName
        properties: {
          addressPrefix: '10.91.0.0/23'
          natGateway: {
            id: secondaryNatGateway.id
          }
          delegations: [
            {
              name: 'Microsoft.App.environments'
              properties: {
                serviceName: 'Microsoft.App/environments'
              }
            }
          ]
        }
      }
      {
        name: privateEndpointSubnetName
        properties: {
          addressPrefix: '10.91.4.0/24'
          privateEndpointNetworkPolicies: 'Disabled'
        }
      }
    ]
  }
  tags: tags
}

resource retainedValidationVnet 'Microsoft.Network/virtualNetworks@2024-07-01' existing = {
  name: retainedValidationVnetName
}

resource primaryToSecondary 'Microsoft.Network/virtualNetworks/virtualNetworkPeerings@2024-07-01' = {
  parent: primaryVnet
  name: 'peer-to-v090-swe'
  properties: {
    allowForwardedTraffic: false
    allowGatewayTransit: false
    allowVirtualNetworkAccess: true
    remoteVirtualNetwork: {
      id: secondaryVnet.id
    }
    useRemoteGateways: false
  }
}

resource secondaryToPrimary 'Microsoft.Network/virtualNetworks/virtualNetworkPeerings@2024-07-01' = {
  parent: secondaryVnet
  name: 'peer-to-v090-frc'
  properties: {
    allowForwardedTraffic: false
    allowGatewayTransit: false
    allowVirtualNetworkAccess: true
    remoteVirtualNetwork: {
      id: primaryVnet.id
    }
    useRemoteGateways: false
  }
}

resource primaryToRetained 'Microsoft.Network/virtualNetworks/virtualNetworkPeerings@2024-07-01' = {
  parent: primaryVnet
  name: 'peer-to-v050-live'
  properties: {
    allowForwardedTraffic: false
    allowGatewayTransit: false
    allowVirtualNetworkAccess: true
    remoteVirtualNetwork: {
      id: retainedValidationVnet.id
    }
    useRemoteGateways: false
  }
}

resource retainedToPrimary 'Microsoft.Network/virtualNetworks/virtualNetworkPeerings@2024-07-01' = {
  parent: retainedValidationVnet
  name: 'peer-to-v090-frc'
  properties: {
    allowForwardedTraffic: false
    allowGatewayTransit: false
    allowVirtualNetworkAccess: true
    remoteVirtualNetwork: {
      id: primaryVnet.id
    }
    useRemoteGateways: false
  }
}

resource secondaryToRetained 'Microsoft.Network/virtualNetworks/virtualNetworkPeerings@2024-07-01' = {
  parent: secondaryVnet
  name: 'peer-to-v050-live'
  properties: {
    allowForwardedTraffic: false
    allowGatewayTransit: false
    allowVirtualNetworkAccess: true
    remoteVirtualNetwork: {
      id: retainedValidationVnet.id
    }
    useRemoteGateways: false
  }
}

resource retainedToSecondary 'Microsoft.Network/virtualNetworks/virtualNetworkPeerings@2024-07-01' = {
  parent: retainedValidationVnet
  name: 'peer-to-v090-swe'
  properties: {
    allowForwardedTraffic: false
    allowGatewayTransit: false
    allowVirtualNetworkAccess: true
    remoteVirtualNetwork: {
      id: secondaryVnet.id
    }
    useRemoteGateways: false
  }
}

resource blobPrivateDns 'Microsoft.Network/privateDnsZones@2024-06-01' existing = {
  name: 'privatelink.blob.${environment().suffixes.storage}'
}

resource keyVaultPrivateDns 'Microsoft.Network/privateDnsZones@2024-06-01' existing = {
  name: 'privatelink.vaultcore.azure.net'
}

resource acrPrivateDns 'Microsoft.Network/privateDnsZones@2024-06-01' = {
  name: 'privatelink.azurecr.io'
  location: 'global'
  tags: tags
}

resource blobPrimaryLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = {
  parent: blobPrivateDns
  name: 'link-overmesh-v090-frc'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: primaryVnet.id
    }
  }
}

resource blobSecondaryLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = {
  parent: blobPrivateDns
  name: 'link-overmesh-v090-swe'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: secondaryVnet.id
    }
  }
}

resource keyVaultPrimaryLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = {
  parent: keyVaultPrivateDns
  name: 'link-overmesh-v090-frc'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: primaryVnet.id
    }
  }
}

resource keyVaultSecondaryLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = {
  parent: keyVaultPrivateDns
  name: 'link-overmesh-v090-swe'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: secondaryVnet.id
    }
  }
}

resource acrPrimaryLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = {
  parent: acrPrivateDns
  name: 'link-overmesh-v090-frc'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: primaryVnet.id
    }
  }
}

resource acrSecondaryLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = {
  parent: acrPrivateDns
  name: 'link-overmesh-v090-swe'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: secondaryVnet.id
    }
  }
}

output primaryAcaSubnetId string = resourceId('Microsoft.Network/virtualNetworks/subnets', primaryVnet.name, acaSubnetName)
output primaryPrivateEndpointSubnetId string = resourceId('Microsoft.Network/virtualNetworks/subnets', primaryVnet.name, privateEndpointSubnetName)
output secondaryAcaSubnetId string = resourceId('Microsoft.Network/virtualNetworks/subnets', secondaryVnet.name, acaSubnetName)
output secondaryPrivateEndpointSubnetId string = resourceId('Microsoft.Network/virtualNetworks/subnets', secondaryVnet.name, privateEndpointSubnetName)
output blobPrivateDnsZoneId string = blobPrivateDns.id
output keyVaultPrivateDnsZoneId string = keyVaultPrivateDns.id
output acrPrivateDnsZoneId string = acrPrivateDns.id
