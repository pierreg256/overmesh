# Live Azure Environment

The live provider must provision two Storage Accounts in distinct regions with
Shared Key access disabled, Microsoft Entra data-plane role assignments, and
ES256 signing through Azure Key Vault or Managed HSM.

The capability gate validates:

- private endpoints with public network access disabled;
- caller roles on corresponding customer containers in both replicas;
- gateway and reconciler managed identities on `overmesh-system`;
- absence of unapproved inherited data roles covering `overmesh-system`;
- equivalent role assignments and ABAC conditions across replicas;
- Reconciler Audit Engine readiness remains false before its first successful
  ARM posture audit and whenever the audit is unavailable or unsafe;
- conditional immutable-blob writes with an allowed caller;
- the same conditional write with a deliberately unauthorized canary identity;
- `201` for initial creation, the API-version-specific `409` or `412` for the
  authorized idempotent retry, `403` for the denied identity, and `202` for
  explicit canary cleanup;
- Gateway-level denied `Put Blob` and `Put Block` requests;
- Gateway `Put Block List` after the original caller stages blocks and then
  loses write permission while retaining read permission;
- Gateway idempotent replay by the original caller after write-permission
  revocation, asserting `403` rather than `409`/`412`;
- Azure SDK for .NET compatibility through the live gateway with explicit
  `x-ms-client-request-id` injection;
- Azure SDK for Python compatibility through the live gateway with explicit
  `x-ms-client-request-id` injection;
- Azure SDK for JavaScript/Node compatibility through the live gateway with
  explicit `x-ms-client-request-id` injection;
- Azure CLI compatibility through the live gateway with managed-identity login
  only;
- AzCopy compatibility through the live gateway with managed identity only;
- upload/PUT, download/GET, delete, payload-byte verification, and cleanup for
  every client, plus properties/HEAD and listing through each SDK and Azure CLI;
- machine-readable JSON evidence with the endpoint, timestamp, commit, client
  versions, per-client operations, and overall result;
- `404` for an allowed absent-blob `HEAD` and `403` for a denied one;
- `404` for an allowed exact-path nonexistent-snapshot `DELETE` and `403` for
  a denied one;
- blob versioning, soft delete, and configured retention posture.

The repository includes an executable provider:

```bash
make test-live-azure
make test-live-azure-storage
make test-live-azure-gateway
make test-live-azure-client-compat
make test-live-azure-placement
```

It requires:

- `OVERMESH_LIVE_RECONCILER_CONFIG`;
- both `OVERMESH_LIVE_ACCOUNT_*_BLOB_ENDPOINT` values;
- both `OVERMESH_LIVE_ACCOUNT_*_RESOURCE_ID` values;
- `OVERMESH_LIVE_CUSTOMER_CONTAINER`;
- `OVERMESH_LIVE_ALLOWED_TOKEN`;
- `OVERMESH_LIVE_DENIED_TOKEN`;
- `OVERMESH_LIVE_ARM_TOKEN`.

`OVERMESH_LIVE_STORAGE_API_VERSIONS` optionally supplies a comma-separated
version list and defaults to `2025-11-05`.

The direct-storage provider verifies ARM security posture, runs the Reconciler
RBAC audit, then executes allowed and denied write, read, and delete probes on
both accounts for every configured Storage API version.

The Gateway authorization provider additionally requires:

- `OVERMESH_LIVE_GATEWAY_ENDPOINT`;
- `OVERMESH_LIVE_ALLOWED_WRITE_MUTATOR`.

`OVERMESH_LIVE_ALLOWED_WRITE_MUTATOR` must be an executable helper. The gate
invokes it as:

```bash
"$OVERMESH_LIVE_ALLOWED_WRITE_MUTATOR" revoke-write
"$OVERMESH_LIVE_ALLOWED_WRITE_MUTATOR" restore-write
```

The helper must temporarily remove the allowed caller's customer-container
write permission across both replicas while preserving read permission, and
must restore the original write grants afterward. The gate fails closed if the
helper is absent, if replay loses read permission after revocation, or if a
revoked write still succeeds.

`make test-live-azure` runs the storage, gateway-authorization, and client
compatibility providers. Missing configuration or an ambiguous authorization
status fails closed.

The placement provider runs in three explicit phases: `baseline`, `outage`,
and `recovery`. It writes one blob for every RF=2 pair in the signed
three-node Ring, verifies each signed head exists on exactly those two Storage
Accounts, proves that a Storage A outage rejects only the A/B and A/C writes,
then retries those writes after restoration and removes the logical canaries.

`HARNESS_LIVE_AZURE_COMMAND` can override the bundled provider when an
organization needs an equivalent internal runner.

## Client compatibility gate

`make test-live-azure-client-compat` executes the milestone `0.9.0` client
compatibility matrix against the configured live Overmesh gateway.

Required environment:

- `OVERMESH_LIVE_GATEWAY_ENDPOINT`;
- `OVERMESH_LIVE_CUSTOMER_CONTAINER`;
- `OVERMESH_LIVE_ALLOWED_MANAGED_IDENTITY_CLIENT_ID`.

Runtime prerequisites on the validation host:

- Linux `x86_64`;
- the allowed user-assigned managed identity attached to the VM and reachable
  through IMDS;
- `python3`, `curl`, `jq`, `git`, and `tar`;
- outbound HTTPS to `nodejs.org`, `dot.net`, `pypi.org`,
  `files.pythonhosted.org`, and GitHub release assets unless the toolchain has
  already been cached locally;
- write access to `.harness/` and to
  `${OVERMESH_LIVE_CLIENT_COMPAT_ROOT:-/opt/overmesh-live/client-compat}`.

The gate installs or reuses:

- Node.js under `/opt/overmesh-live/client-compat/tools/`;
- .NET SDK under `/opt/overmesh-live/client-compat/tools/`;
- AzCopy under `/opt/overmesh-live/client-compat/tools/`;
- isolated Python virtual environments for the Azure SDK and Azure CLI under
  `/opt/overmesh-live/client-compat/venvs/`.

No Storage account keys, SAS tokens, or client secrets are used. The SDK
clients inject `x-ms-client-request-id` explicitly so Overmesh write-id
requirements are deterministic. Azure CLI and AzCopy rely on their native
generated client request IDs; a successful write is itself proof because the
gateway rejects any write missing `x-overmesh-write-id`/`x-ms-client-request-id`.

The default evidence file is:

```text
.harness/live-client-compat/<run-id>/evidence.json
```

Optional overrides:

- `OVERMESH_LIVE_CLIENT_COMPAT_ROOT` to relocate the cached toolchain;
- `OVERMESH_LIVE_CLIENT_COMPAT_WORK_DIR` to relocate local logs and downloads;
- `OVERMESH_LIVE_CLIENT_COMPAT_EVIDENCE_PATH` to choose an explicit JSON output
  path;
- `OVERMESH_LIVE_CLIENT_COMPAT_NODE_VERSION`,
  `OVERMESH_LIVE_CLIENT_COMPAT_DOTNET_VERSION`,
  `OVERMESH_LIVE_CLIENT_COMPAT_AZURE_CLI_VERSION`, and
  `OVERMESH_LIVE_CLIENT_COMPAT_AZCOPY_VERSION` to pin alternate tool versions.

Example invocation on the retained Linux validation VM:

```bash
OVERMESH_LIVE_GATEWAY_ENDPOINT="https://overmesh.example.internal" \
OVERMESH_LIVE_CUSTOMER_CONTAINER="customer-data" \
OVERMESH_LIVE_ALLOWED_MANAGED_IDENTITY_CLIENT_ID="00000000-0000-0000-0000-000000000000" \
make test-live-azure-client-compat
```
