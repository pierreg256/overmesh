targetScope = 'resourceGroup'

@description('Primary active Gateway and scheduled Reconciler region.')
param primaryLocation string = 'francecentral'

@description('Secondary active Gateway and manual standby Reconciler region.')
param secondaryLocation string = 'swedencentral'

@description('Third Storage Account region used for RF=2 placement validation.')
param thirdStorageLocation string = 'norwayeast'

@description('Globally unique numeric suffix for Storage and ACR names.')
param uniqueSuffix string = '8152352'

@description('Existing France Storage Account name.')
param storageAccountAName string = 'stomv050a8152352'

@description('Existing Sweden Storage Account name.')
param storageAccountBName string = 'stomv050b8152352'

@description('Existing Key Vault name.')
param keyVaultName string = 'kv-overmesh-v050-8152352'

@description('Dedicated non-exportable Ring signing key.')
param ringKeyName string = 'overmesh-ring-v090'

@description('Existing Gateway user-assigned managed identity name.')
param gatewayIdentityName string = 'id-overmesh-gateway-v050'

@description('Existing Reconciler user-assigned managed identity name.')
param reconcilerIdentityName string = 'id-overmesh-reconciler-v050'

@description('Existing positive caller canary managed identity name.')
param allowedCallerIdentityName string = 'id-overmesh-caller-allowed-v050'

@description('Optional GitHub Actions OIDC publisher principal ID. Empty omits publisher RBAC.')
param githubPublisherPrincipalId string = ''

@description('Container image tag deployed to both regions.')
param imageTag string = '0.9.0'

@description('Deploy Gateway, Reconciler, and Front Door after images exist in ACR.')
param deployRuntime bool = false

@secure()
@description('Base64-encoded Gateway YAML configuration without credentials.')
param gatewayConfigBase64 string

@secure()
@description('Base64-encoded Reconciler YAML configuration without credentials.')
param reconcilerConfigBase64 string

@secure()
@description('Base64-encoded signed three-node Ring document.')
param ringDocumentBase64 string

@secure()
@description('Base64-encoded detached Ring signature.')
param ringSignatureBase64 string

var tags = {
  workload: 'overmesh'
  milestone: '0.9.0'
  lifecycle: 'retained'
  autoDelete: 'false'
}

resource gatewayIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2024-11-30' existing = {
  name: gatewayIdentityName
}

resource reconcilerIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2024-11-30' existing = {
  name: reconcilerIdentityName
}

resource allowedCallerIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2024-11-30' existing = {
  name: allowedCallerIdentityName
}

module data './modules/data-v090.bicep' = {
  name: 'overmesh-v090-data'
  params: {
    primaryLocation: primaryLocation
    secondaryLocation: secondaryLocation
    thirdStorageLocation: thirdStorageLocation
    uniqueSuffix: uniqueSuffix
    storageAccountAName: storageAccountAName
    storageAccountBName: storageAccountBName
    customerContainerName: 'live-v090'
    tags: tags
  }
}

module ringKey './modules/ring-key-v090.bicep' = {
  name: 'overmesh-v090-ring-key'
  params: {
    keyVaultName: keyVaultName
    ringKeyName: ringKeyName
  }
}

module networking './modules/networking-v090.bicep' = {
  name: 'overmesh-v090-networking'
  params: {
    primaryLocation: primaryLocation
    secondaryLocation: secondaryLocation
    tags: tags
  }
}

module monitoring './modules/monitoring-v090.bicep' = {
  name: 'overmesh-v090-monitoring'
  params: {
    primaryLocation: primaryLocation
    secondaryLocation: secondaryLocation
    tags: tags
  }
}

module privateLinksPrimary './modules/private-links-v090.bicep' = {
  name: 'overmesh-v090-private-links-frc'
  params: {
    location: primaryLocation
    regionalSuffix: 'frc'
    privateEndpointSubnetId: networking.outputs.primaryPrivateEndpointSubnetId
    blobPrivateDnsZoneId: networking.outputs.blobPrivateDnsZoneId
    keyVaultPrivateDnsZoneId: networking.outputs.keyVaultPrivateDnsZoneId
    acrPrivateDnsZoneId: networking.outputs.acrPrivateDnsZoneId
    storageAccountIds: [
      data.outputs.storageAccountAId
      data.outputs.storageAccountBId
      data.outputs.storageAccountCId
    ]
    keyVaultId: resourceId('Microsoft.KeyVault/vaults', keyVaultName)
    containerRegistryId: data.outputs.containerRegistryId
    tags: tags
  }
}

module privateLinksSecondary './modules/private-links-v090.bicep' = {
  name: 'overmesh-v090-private-links-swe'
  params: {
    location: secondaryLocation
    regionalSuffix: 'swe'
    privateEndpointSubnetId: networking.outputs.secondaryPrivateEndpointSubnetId
    blobPrivateDnsZoneId: networking.outputs.blobPrivateDnsZoneId
    keyVaultPrivateDnsZoneId: networking.outputs.keyVaultPrivateDnsZoneId
    acrPrivateDnsZoneId: networking.outputs.acrPrivateDnsZoneId
    storageAccountIds: [
      data.outputs.storageAccountAId
      data.outputs.storageAccountBId
      data.outputs.storageAccountCId
    ]
    keyVaultId: resourceId('Microsoft.KeyVault/vaults', keyVaultName)
    containerRegistryId: data.outputs.containerRegistryId
    tags: tags
  }
}

module primaryRuntime './modules/aca-region-v090.bicep' = {
  name: 'overmesh-v090-aca-frc'
  params: {
    location: primaryLocation
    regionalSuffix: 'frc'
    infrastructureSubnetId: networking.outputs.primaryAcaSubnetId
    workspaceId: monitoring.outputs.primaryWorkspaceId
    gatewayIdentityId: gatewayIdentity.id
    reconcilerIdentityId: reconcilerIdentity.id
    registryServer: data.outputs.containerRegistryLoginServer
    gatewayImage: '${data.outputs.containerRegistryLoginServer}/overmesh-gateway:${imageTag}'
    reconcilerImage: '${data.outputs.containerRegistryLoginServer}/overmesh-reconciler:${imageTag}'
    gatewayConfigBase64: gatewayConfigBase64
    reconcilerConfigBase64: reconcilerConfigBase64
    ringDocumentBase64: ringDocumentBase64
    ringSignatureBase64: ringSignatureBase64
    deployRuntime: deployRuntime
    reconcilerTriggerType: 'Schedule'
    reconcilerCronExpression: '0 */5 * * * *'
    tags: tags
  }
  dependsOn: [
    privateLinksPrimary
    rbac
  ]
}

module secondaryRuntime './modules/aca-region-v090.bicep' = {
  name: 'overmesh-v090-aca-swe'
  params: {
    location: secondaryLocation
    regionalSuffix: 'swe'
    infrastructureSubnetId: networking.outputs.secondaryAcaSubnetId
    workspaceId: monitoring.outputs.secondaryWorkspaceId
    gatewayIdentityId: gatewayIdentity.id
    reconcilerIdentityId: reconcilerIdentity.id
    registryServer: data.outputs.containerRegistryLoginServer
    gatewayImage: '${data.outputs.containerRegistryLoginServer}/overmesh-gateway:${imageTag}'
    reconcilerImage: '${data.outputs.containerRegistryLoginServer}/overmesh-reconciler:${imageTag}'
    gatewayConfigBase64: gatewayConfigBase64
    reconcilerConfigBase64: reconcilerConfigBase64
    ringDocumentBase64: ringDocumentBase64
    ringSignatureBase64: ringSignatureBase64
    deployRuntime: deployRuntime
    reconcilerTriggerType: 'Manual'
    reconcilerCronExpression: ''
    tags: tags
  }
  dependsOn: [
    privateLinksSecondary
    rbac
  ]
}

module rbac './modules/rbac-v090.bicep' = {
  name: 'overmesh-v090-rbac'
  params: {
    storageAccountAName: storageAccountAName
    storageAccountBName: storageAccountBName
    storageAccountCName: data.outputs.storageAccountCName
    containerRegistryName: data.outputs.containerRegistryName
    gatewayPrincipalId: gatewayIdentity.properties.principalId
    reconcilerPrincipalId: reconcilerIdentity.properties.principalId
    allowedCallerPrincipalId: allowedCallerIdentity.properties.principalId
    githubPublisherPrincipalId: githubPublisherPrincipalId
    systemContainerName: 'overmesh-system'
    customerContainerName: 'live-v090'
  }
}

module frontDoor './modules/front-door-v090.bicep' = if (deployRuntime) {
  name: 'overmesh-v090-front-door'
  params: {
    uniqueSuffix: uniqueSuffix
    primaryLocation: primaryLocation
    secondaryLocation: secondaryLocation
    primaryEnvironmentId: primaryRuntime.outputs.environmentId
    secondaryEnvironmentId: secondaryRuntime.outputs.environmentId
    primaryGatewayFqdn: primaryRuntime.outputs.gatewayFqdn
    secondaryGatewayFqdn: secondaryRuntime.outputs.gatewayFqdn
    tags: tags
  }
}

output containerRegistryName string = data.outputs.containerRegistryName
output primaryGatewayFqdn string = deployRuntime ? primaryRuntime.outputs.gatewayFqdn : ''
output secondaryGatewayFqdn string = deployRuntime ? secondaryRuntime.outputs.gatewayFqdn : ''
output frontDoorEndpointHostName string = frontDoor.?outputs.endpointHostName ?? ''
output storageAccountCName string = data.outputs.storageAccountCName
output ringKeyId string = ringKey.outputs.keyUriWithVersion
