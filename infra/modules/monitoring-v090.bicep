targetScope = 'resourceGroup'

@description('Primary monitoring region.')
param primaryLocation string

@description('Secondary monitoring region.')
param secondaryLocation string

@description('Common resource tags.')
param tags object

resource primaryWorkspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: 'log-overmesh-v090-frc'
  location: primaryLocation
  properties: {
    retentionInDays: 30
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
    features: {
      enableLogAccessUsingOnlyResourcePermissions: true
    }
    sku: {
      name: 'PerGB2018'
    }
  }
  tags: tags
}

resource secondaryWorkspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: 'log-overmesh-v090-swe'
  location: secondaryLocation
  properties: {
    retentionInDays: 30
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
    features: {
      enableLogAccessUsingOnlyResourcePermissions: true
    }
    sku: {
      name: 'PerGB2018'
    }
  }
  tags: tags
}

output primaryWorkspaceId string = primaryWorkspace.id
output secondaryWorkspaceId string = secondaryWorkspace.id
