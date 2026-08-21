targetScope = 'resourceGroup'

@description('Globally unique endpoint suffix.')
param uniqueSuffix string

@description('Primary ACA region used by Front Door Private Link.')
param primaryLocation string

@description('Secondary ACA region used by Front Door Private Link.')
param secondaryLocation string

@description('Primary ACA environment resource ID.')
param primaryEnvironmentId string

@description('Secondary ACA environment resource ID.')
param secondaryEnvironmentId string

@description('Primary Gateway ACA hostname.')
param primaryGatewayFqdn string

@description('Secondary Gateway ACA hostname.')
param secondaryGatewayFqdn string

@description('Common resource tags.')
param tags object

resource profile 'Microsoft.Cdn/profiles@2025-06-01' = {
  name: 'afd-overmesh-v090'
  location: 'global'
  sku: {
    name: 'Premium_AzureFrontDoor'
  }
  properties: {
    originResponseTimeoutSeconds: 240
  }
  tags: tags
}

resource endpoint 'Microsoft.Cdn/profiles/afdEndpoints@2025-06-01' = {
  parent: profile
  name: 'overmesh-v090-${uniqueSuffix}'
  location: 'global'
  properties: {
    enabledState: 'Enabled'
  }
  tags: tags
}

resource originGroup 'Microsoft.Cdn/profiles/originGroups@2025-06-01' = {
  parent: profile
  name: 'og-overmesh-v090'
  properties: {
    healthProbeSettings: {
      probeIntervalInSeconds: 30
      probePath: '/healthz'
      probeProtocol: 'Https'
      probeRequestType: 'HEAD'
    }
    loadBalancingSettings: {
      additionalLatencyInMilliseconds: 0
      sampleSize: 4
      successfulSamplesRequired: 3
    }
    sessionAffinityState: 'Disabled'
  }
}

resource primaryOrigin 'Microsoft.Cdn/profiles/originGroups/origins@2025-06-01' = {
  parent: originGroup
  name: 'origin-frc'
  properties: {
    enabledState: 'Enabled'
    enforceCertificateNameCheck: true
    hostName: primaryGatewayFqdn
    httpPort: 80
    httpsPort: 443
    originHostHeader: primaryGatewayFqdn
    priority: 1
    weight: 1000
    sharedPrivateLinkResource: {
      groupId: 'managedEnvironments'
      privateLink: {
        id: primaryEnvironmentId
      }
      privateLinkLocation: primaryLocation
      requestMessage: 'Overmesh Front Door primary origin'
      status: 'Pending'
    }
  }
}

resource secondaryOrigin 'Microsoft.Cdn/profiles/originGroups/origins@2025-06-01' = {
  parent: originGroup
  name: 'origin-swe'
  properties: {
    enabledState: 'Enabled'
    enforceCertificateNameCheck: true
    hostName: secondaryGatewayFqdn
    httpPort: 80
    httpsPort: 443
    originHostHeader: secondaryGatewayFqdn
    priority: 1
    weight: 1000
    sharedPrivateLinkResource: {
      groupId: 'managedEnvironments'
      privateLink: {
        id: secondaryEnvironmentId
      }
      privateLinkLocation: secondaryLocation
      requestMessage: 'Overmesh Front Door secondary origin'
      status: 'Pending'
    }
  }
}

resource route 'Microsoft.Cdn/profiles/afdEndpoints/routes@2025-06-01' = {
  parent: endpoint
  name: 'route-blob-data-plane'
  properties: {
    enabledState: 'Enabled'
    forwardingProtocol: 'HttpsOnly'
    httpsRedirect: 'Enabled'
    linkToDefaultDomain: 'Enabled'
    originGroup: {
      id: originGroup.id
    }
    patternsToMatch: [
      '/*'
    ]
    ruleSets: []
    supportedProtocols: [
      'Http'
      'Https'
    ]
  }
  dependsOn: [
    primaryOrigin
    secondaryOrigin
  ]
}

output endpointHostName string = endpoint.properties.hostName
