# Azure infrastructure

`main.bicep` is the milestone `0.9.0` orchestrator. The retained pre-ACA
environment remains reproducible through `retained-v050.bicep` and
`retained-v050.bicepparam`.

The `0.9.0` deployment is intentionally phased:

1. Build and validate the template.
2. Deploy the foundation with `deployRuntime=false`.
3. Build the Gateway and Reconciler images in public GHCR through GitHub
   Actions.
4. Import the immutable GHCR digests into private ACR through Azure.
5. Supply signed Ring and runtime configuration values outside source control.
6. Deploy with `deployRuntime=true`.
7. Approve the two Front Door Private Link requests on the ACA environments.

The foundation phase creates the third Storage Account, geo-replicated Premium
ACR, regional networks, Private Endpoints, monitoring, ACA environments, and
least-privilege role assignments. It also creates the dedicated non-exportable
P-256 `overmesh-ring-v090` Key Vault signing key. It does not create an
application revision before its image exists.

The retained `10.50.0.0/16` validation VNet and the two ACA VNets
(`10.90.0.0/16`, `10.91.0.0/16`) use a full mesh of direct peerings. Private
DNS may select a Private Endpoint in any of those VNets, so every linked VNet
must have a direct route to every published private address. Gateway transit
and forwarded traffic remain disabled.

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

At deployment time, Bicep projects the supplied configuration, signed Ring,
JWKS, and verification keys into an ACA secret volume mounted read-only at
`/run/overmesh`. Gateway and Reconciler start directly from the generic
published images and read their files from that mount. No environment-specific
configuration or trust material is embedded in either image.

The Gateway and Reconciler identities remain distinct. GitHub Actions uses only
the repository-scoped `GITHUB_TOKEN` to publish `linux/amd64` images to GHCR;
it receives no Azure identity or credential. After the first publication,
change both package visibilities to **Public** in GitHub package settings. The
workflow's anonymous-pull job enforces that state.

An Azure-authenticated operator imports the public images server-side by
immutable digest. No runner or workstation connects to the private ACR data
endpoint:

```bash
./deploy/import-ghcr-images-to-acr.sh \
  --acr-name crovermesh0908152352 \
  --tag 0.9.0 \
  --commit-sha "$(git rev-parse HEAD)"
```

The script resolves each public GHCR digest, invokes synchronous `az acr
import`, and assigns both `0.9.0` and `sha-<12-character-commit>` in ACR.
