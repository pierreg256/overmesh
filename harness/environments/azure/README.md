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
make test-pre-pr-live
make test-live-azure-storage
make test-live-azure-posture
make test-live-azure-gateway
make test-live-azure-client-compat
make test-live-azure-placement
make test-live-azure-reconciliation
make test-live-azure-performance
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

`make test-pre-pr-live` is the optional local pre-PR entry point and delegates
to `make test-live-azure`. It runs the Storage, negative-posture,
Gateway-authorization, client-compatibility, and reconciliation providers.
Missing configuration or an ambiguous result fails closed.

The live gate is intentionally not a GitHub Actions workflow. Enterprise policy
prohibits Azure login from GitHub-hosted runners, so no repository workflow may
request `id-token: write` or call `azure/login` for this environment. The
operator runs the gate from an approved local workstation and retained
validation VM, then commits only the redacted signed evidence.

The placement provider runs in three explicit phases: `baseline`, `outage`,
and `recovery`. It writes one blob for every RF=2 pair in the signed
three-node Ring, verifies each signed head exists on exactly those two Storage
Accounts, proves that a Storage A outage rejects only the A/B and A/C writes,
then retries those writes after restoration and removes the logical canaries.

The posture provider requires executable audit and mutation helpers. It proves
the healthy three-account ARM snapshot, rejection of an unapproved inherited
account-level data role, rejection of a path-dependent ABAC condition, removal
of both temporary assignments, and successful nominal revalidation.

The reconciliation provider proves missing-replica repair, quarantine without
automatic use of tampered content, administrator-selected recovery, and
retention-backed collection. A collection configuration shorter than the
production delay is accepted only when
`OVERMESH_LIVE_RECONCILIATION_ISOLATED_ENVIRONMENT=true`; this assertion is
reserved for a validation environment containing no customer workload.

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

## Performance baseline

`make test-live-azure-performance` executes the versioned matrix in
`harness/performance/live-v1.toml`. The runner alternates each case between one
direct Storage Account and the live Overmesh endpoint, using the same managed
identity, Azure SDK versions, validation host, payload bytes, operation count,
and concurrency.

The performance gate is intentionally excluded from `test-pre-pr-live` because
it is long-running and retains signed release evidence. `make test-release`
includes it.

The initial contract covers `Put Blob`, full `Get Blob`, ranged reads, `Head
Blob`, and `Delete Blob` at 1 KiB, 1 MiB, and 16 MiB where applicable, with
concurrency levels 1, 4, and 16. Warm-up samples are excluded. Retained
measurements include min, mean, p50, p90, p95, p99, max, operations per second,
bytes per second, successful and failed operation counts, and
gateway-to-direct ratios.

Additional required environment:

- `OVERMESH_LIVE_PERFORMANCE_RING_VERSION`;
- `OVERMESH_LIVE_PERFORMANCE_RING_HASH`;
- `OVERMESH_LIVE_PERFORMANCE_DEPLOYMENT`;
- `OVERMESH_LIVE_PERFORMANCE_ENVIRONMENT`;
- `OVERMESH_LIVE_PERFORMANCE_ISOLATED_ENVIRONMENT=true`;
- `OVERMESH_LIVE_PERFORMANCE_PUBLIC_KEY`;
- `OVERMESH_LIVE_PERFORMANCE_WORKSPACE_ID` (the workspace customer GUID);
- `OVERMESH_LIVE_PERFORMANCE_GATEWAY_APP_NAME`;
- `OVERMESH_LIVE_PERFORMANCE_GATEWAY_RESOURCE_ID`;
- `OVERMESH_LIVE_EVIDENCE_KEY_ID`;
- `OVERMESH_LIVE_EVIDENCE_SIGNING_CLIENT_ID`.

The default raw result is
`.harness/live-performance/<run-id>/raw-performance.json`. It is local input,
not retained evidence. The gate then deterministically redacts the result,
copies the public verification key, signs the canonical JSON through Key
Vault, verifies the signature through Key Vault, and writes `SHA256SUMS` under
`.harness/live-performance/<run-id>/signed/`. The endpoint itself is not
written to evidence; only a deterministic hostname fingerprint is retained.
The runner records the commit, project version, Ring provenance, immutable
deployment identifier, logical environment identifier, selected Storage API
version, matrix hash, and pinned SDK versions.

The gate installs the pinned Log Analytics Azure CLI extension `1.0.0b1` under
`$OVERMESH_LIVE_PERFORMANCE_ROOT/az-extensions`, not in the operator's global
Azure CLI extension directory.

`OVERMESH_LIVE_PERFORMANCE_BASELINE_EVIDENCE` optionally points to an earlier
canonical performance evidence file. The gate rejects a different contract or
case set, then records changes in Gateway-to-direct latency and throughput
ratios, backend requests per operation, signing p95, peak CPU, and peak memory.
Without a predecessor, the signed result explicitly records
`baseline-established`.

The client-side baseline deliberately does not infer server behavior. Gateway
logs emit one structured `overmesh_backend_request` event per Storage request
and one `overmesh_manifest_sign` event per Key Vault signing request, including
duration and success. Before signing, the live gate queries those events and
Container Apps `UsageNanoCores` and `WorkingSetBytes` metrics for each measured
Gateway case, together with the replica count needed to interpret aggregate
resource use. The gate requires an explicit isolated-environment assertion so
other client traffic cannot contaminate backend request counts. Backend timings
explicitly measure time to response headers, not full response-body transfer.
Azure Monitor exposes those resource metrics at one-minute granularity; shorter
cases retain the exact enclosing one-minute query window, which can overlap an
adjacent case. Raw logs and Azure resource identifiers are not retained.
