targetScope = 'resourceGroup'

param keyVaultName string
param ringKeyName string

resource keyVault 'Microsoft.KeyVault/vaults@2024-11-01' existing = {
  name: keyVaultName
}

resource ringKey 'Microsoft.KeyVault/vaults/keys@2024-11-01' = {
  parent: keyVault
  name: ringKeyName
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
}

output keyUriWithVersion string = ringKey.properties.keyUriWithVersion
