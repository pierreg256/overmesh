# Azure infrastructure

`main.bicep` is the milestone `0.9.0` orchestrator. The retained pre-ACA
environment remains reproducible through `retained-v050.bicep` and
`retained-v050.bicepparam`.

The `0.9.0` deployment is intentionally phased:

1. Build and validate the template.
2. Deploy the foundation with `deployRuntime=false`.
3. Build the Gateway and Reconciler images inside the private ACR through
   GitHub Actions and ACR Tasks.
4. Supply signed Ring and runtime configuration values outside source control.
5. Deploy with `deployRuntime=true`.
6. Approve the two Front Door Private Link requests on the ACA environments.

The foundation phase creates the third Storage Account, geo-replicated Premium
ACR, regional networks, Private Endpoints, monitoring, ACA environments, and
least-privilege role assignments. It does not create an application revision
before its image exists.

The retained `0.5.0` deployment remains authoritative for the Gateway and
Reconciler permissions on the two original `overmesh-system` containers and
Key Vault. The `0.9.0` RBAC module adds only the new Storage C, `live-v090`,
Storage ARM Reader, and ACR permissions.

## Local validation

```bash
make infra-build

az deployment group validate \
  --resource-group rg-overmesh-v050-live \
  --template-file infra/main.bicep \
  --parameters infra/main.bicepparam

az deployment group what-if \
  --resource-group rg-overmesh-v050-live \
  --template-file infra/main.bicep \
  --parameters infra/main.bicepparam
```

The secure configuration parameters in `main.bicepparam` are empty
placeholders. Never commit rendered runtime configuration or deployment
parameter files containing signed environment material.

The Gateway and Reconciler identities remain distinct. GitHub-hosted runners do
not connect to the ACR data endpoint: GitHub OIDC authorizes an `az acr build`
request, and ACR Tasks performs the private build and push inside Azure.

`githubPublisherPrincipalId` is optional. When it is empty, the foundation
omits the GitHub publisher role assignments. Before running the image workflow,
configure a secretless OIDC principal for the `live-azure` GitHub environment,
redeploy with its object ID, then set `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`,
`AZURE_SUBSCRIPTION_ID`, and the `OVERMESH_ACR_NAME` environment variable.
