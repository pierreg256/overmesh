using './main.bicep'

param primaryLocation = 'francecentral'
param secondaryLocation = 'swedencentral'
param thirdStorageLocation = 'norwayeast'
param uniqueSuffix = '8152352'
param storageAccountAName = 'stomv050a8152352'
param storageAccountBName = 'stomv050b8152352'
param keyVaultName = 'kv-overmesh-v050-8152352'
param retainedValidationVnetName = 'vnet-overmesh-v050-live'
param ringKeyName = 'overmesh-ring-v090'
param gatewayIdentityName = 'id-overmesh-gateway-v050'
param reconcilerIdentityName = 'id-overmesh-reconciler-v050'
param allowedCallerIdentityName = 'id-overmesh-caller-allowed-v050'

// Supply signed runtime configuration at validation/deployment time.
param imageTag = '0.9.0'
param deployRuntime = false
param deployRbac = true
param gatewayConfig = ''
param reconcilerConfig = ''
param ringDocument = ''
param ringSignature = ''
param entraJwks = ''
param ringPublicKey = ''
param manifestPublicKey = ''
