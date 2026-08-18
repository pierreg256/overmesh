targetScope = 'subscription'

@description('Azure region for the validation network and control resources.')
param location string = 'francecentral'

@description('Resource group containing the retained live validation environment.')
param resourceGroupName string = 'rg-overmesh-v050-live'

@description('Globally unique suffix already checked before deployment.')
param uniqueSuffix string = '8152352'

@description('Object ID of the operator running the validation.')
param operatorPrincipalId string

@description('SSH public key for the private validation VM.')
param sshPublicKey string

@description('Administrative username for the private validation VM.')
param adminUsername string = 'overmesh'

resource validationResourceGroup 'Microsoft.Resources/resourceGroups@2024-03-01' = {
  name: resourceGroupName
  location: location
  tags: {
    workload: 'overmesh'
    milestone: '0.7.0'
    purpose: 'live-validation'
    lifecycle: 'retained'
    autoDelete: 'false'
  }
}

module validation './modules/live-v050.bicep' = {
  name: 'overmesh-v050-live'
  scope: validationResourceGroup
  params: {
    location: location
    uniqueSuffix: uniqueSuffix
    operatorPrincipalId: operatorPrincipalId
    sshPublicKey: sshPublicKey
    adminUsername: adminUsername
  }
}

output resourceGroupName string = validationResourceGroup.name
output storageAccountAName string = validation.outputs.storageAccountAName
output storageAccountBName string = validation.outputs.storageAccountBName
output storageAccountAId string = validation.outputs.storageAccountAId
output storageAccountBId string = validation.outputs.storageAccountBId
output gatewayIdentityClientId string = validation.outputs.gatewayIdentityClientId
output gatewayIdentityPrincipalId string = validation.outputs.gatewayIdentityPrincipalId
output reconcilerIdentityClientId string = validation.outputs.reconcilerIdentityClientId
output reconcilerIdentityPrincipalId string = validation.outputs.reconcilerIdentityPrincipalId
output allowedIdentityClientId string = validation.outputs.allowedIdentityClientId
output deniedIdentityClientId string = validation.outputs.deniedIdentityClientId
output keyVaultName string = validation.outputs.keyVaultName
output signingKeyId string = validation.outputs.signingKeyId
output validationVmName string = validation.outputs.validationVmName
