targetScope = 'resourceGroup'

@description('ACA deployment region.')
param location string

@description('Short regional suffix.')
param regionalSuffix string

@description('Dedicated ACA infrastructure subnet resource ID.')
param infrastructureSubnetId string

@description('Regional Log Analytics workspace resource ID.')
param workspaceId string

@description('Gateway user-assigned managed identity resource ID.')
param gatewayIdentityId string

@description('Reconciler user-assigned managed identity resource ID.')
param reconcilerIdentityId string

@description('Private ACR login server.')
param registryServer string

@description('Gateway image including immutable tag or digest.')
param gatewayImage string

@description('Reconciler image including immutable tag or digest.')
param reconcilerImage string

@secure()
@description('Gateway configuration.')
param gatewayConfig string

@secure()
@description('Reconciler configuration.')
param reconcilerConfig string

@secure()
@description('Signed Ring document.')
param ringDocument string

@secure()
@description('Ring detached signature.')
param ringSignature string

@secure()
@description('Microsoft Entra JWKS document.')
param entraJwks string

@secure()
@description('Ring verification public key.')
param ringPublicKey string

@secure()
@description('Manifest verification public key.')
param manifestPublicKey string

@allowed([
  'Schedule'
  'Manual'
])
@description('Reconciler job trigger type.')
param reconcilerTriggerType string

@description('Five-field cron expression when trigger type is Schedule.')
param reconcilerCronExpression string

@description('Common resource tags.')
param tags object

@description('Deploy application and job resources after images are available.')
param deployRuntime bool

var environmentName = 'cae-overmesh-v090-${regionalSuffix}'
var gatewayName = 'ca-overmesh-gateway-${regionalSuffix}'
var reconcilerName = 'caj-overmesh-reconciler-${regionalSuffix}'
var reconcilerTriggerConfiguration = reconcilerTriggerType == 'Schedule'
  ? {
      scheduleTriggerConfig: {
        cronExpression: reconcilerCronExpression
        parallelism: 1
        replicaCompletionCount: 1
      }
    }
  : {
      manualTriggerConfig: {
        parallelism: 1
        replicaCompletionCount: 1
      }
    }

resource environment 'Microsoft.App/managedEnvironments@2025-07-01' = {
  name: environmentName
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'azure-monitor'
    }
    publicNetworkAccess: 'Disabled'
    vnetConfiguration: {
      infrastructureSubnetId: infrastructureSubnetId
      internal: true
    }
    workloadProfiles: [
      {
        name: 'Consumption'
        workloadProfileType: 'Consumption'
      }
    ]
    zoneRedundant: true
  }
  tags: tags
}

resource gateway 'Microsoft.App/containerApps@2025-07-01' = if (deployRuntime) {
  name: gatewayName
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${gatewayIdentityId}': {}
    }
  }
  properties: {
    environmentId: environment.id
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: {
        allowInsecure: false
        external: true
        targetPort: 8080
        transport: 'http2'
        traffic: [
          {
            latestRevision: true
            weight: 100
          }
        ]
      }
      registries: [
        {
          server: registryServer
          identity: gatewayIdentityId
        }
      ]
      secrets: [
        {
          name: 'gateway-config'
          value: gatewayConfig
        }
        {
          name: 'ring-document'
          value: ringDocument
        }
        {
          name: 'ring-signature'
          value: ringSignature
        }
        {
          name: 'entra-jwks'
          value: entraJwks
        }
        {
          name: 'ring-public-key'
          value: ringPublicKey
        }
        {
          name: 'manifest-public-key'
          value: manifestPublicKey
        }
      ]
    }
    template: {
      volumes: [
        {
          name: 'runtime-config'
          storageType: 'Secret'
          secrets: [
            {
              secretRef: 'gateway-config'
              path: 'config.yaml'
            }
            {
              secretRef: 'ring-document'
              path: 'ring.yaml'
            }
            {
              secretRef: 'ring-signature'
              path: 'ring.sig'
            }
            {
              secretRef: 'entra-jwks'
              path: 'entra-jwks.json'
            }
            {
              secretRef: 'ring-public-key'
              path: 'ring-public.pem'
            }
            {
              secretRef: 'manifest-public-key'
              path: 'manifest-public.pem'
            }
          ]
        }
      ]
      containers: [
        {
          name: 'gateway'
          image: gatewayImage
          command: [
            '/usr/local/bin/overmesh-gateway'
          ]
          args: [
            '--config'
            '/run/overmesh/config.yaml'
          ]
          env: [
            {
              name: 'RUST_LOG'
              value: 'info'
            }
          ]
          volumeMounts: [
            {
              volumeName: 'runtime-config'
              mountPath: '/run/overmesh'
            }
          ]
          probes: [
            {
              type: 'Liveness'
              httpGet: {
                path: '/healthz'
                port: 8080
                scheme: 'HTTP'
              }
              initialDelaySeconds: 10
              periodSeconds: 10
              timeoutSeconds: 5
              failureThreshold: 3
            }
            {
              type: 'Readiness'
              httpGet: {
                path: '/healthz'
                port: 8080
                scheme: 'HTTP'
              }
              initialDelaySeconds: 5
              periodSeconds: 5
              timeoutSeconds: 3
              failureThreshold: 3
            }
          ]
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 25
      }
    }
  }
  tags: tags
}

resource reconciler 'Microsoft.App/jobs@2025-07-01' = if (deployRuntime) {
  name: reconcilerName
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${reconcilerIdentityId}': {}
    }
  }
  properties: {
    environmentId: environment.id
    configuration: union({
      triggerType: reconcilerTriggerType
      replicaTimeout: 3600
      replicaRetryLimit: 1
      registries: [
        {
          server: registryServer
          identity: reconcilerIdentityId
        }
      ]
      secrets: [
        {
          name: 'reconciler-config'
          value: reconcilerConfig
        }
        {
          name: 'ring-document'
          value: ringDocument
        }
        {
          name: 'ring-signature'
          value: ringSignature
        }
        {
          name: 'ring-public-key'
          value: ringPublicKey
        }
        {
          name: 'manifest-public-key'
          value: manifestPublicKey
        }
      ]
    }, reconcilerTriggerConfiguration)
    template: {
      volumes: [
        {
          name: 'runtime-config'
          storageType: 'Secret'
          secrets: [
            {
              secretRef: 'reconciler-config'
              path: 'config.yaml'
            }
            {
              secretRef: 'ring-document'
              path: 'ring.yaml'
            }
            {
              secretRef: 'ring-signature'
              path: 'ring.sig'
            }
            {
              secretRef: 'ring-public-key'
              path: 'ring-public.pem'
            }
            {
              secretRef: 'manifest-public-key'
              path: 'manifest-public.pem'
            }
          ]
        }
      ]
      containers: [
        {
          name: 'reconciler'
          image: reconcilerImage
          command: [
            '/usr/local/bin/overmesh-reconciler'
          ]
          args: [
            '--config'
            '/run/overmesh/config.yaml'
            'once'
          ]
          env: [
            {
              name: 'RUST_LOG'
              value: 'info'
            }
          ]
          volumeMounts: [
            {
              volumeName: 'runtime-config'
              mountPath: '/run/overmesh'
            }
          ]
          resources: {
            cpu: json('1.0')
            memory: '2Gi'
          }
        }
      ]
    }
  }
  tags: tags
}

resource environmentDiagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: 'diag-${environmentName}'
  scope: environment
  properties: {
    workspaceId: workspaceId
    logs: [
      {
        categoryGroup: 'allLogs'
        enabled: true
      }
    ]
    metrics: [
      {
        category: 'AllMetrics'
        enabled: true
      }
    ]
  }
}

resource gatewayDiagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = if (deployRuntime) {
  name: 'diag-${gatewayName}'
  scope: gateway
  properties: {
    workspaceId: workspaceId
    metrics: [
      {
        category: 'AllMetrics'
        enabled: true
      }
    ]
  }
}

output environmentId string = environment.id
output gatewayFqdn string = gateway.?properties.configuration.ingress.fqdn ?? ''
output reconcilerJobName string = deployRuntime ? reconciler.name : ''
