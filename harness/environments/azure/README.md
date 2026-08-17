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
- `404` for an allowed absent-blob `HEAD` and `403` for a denied one;
- `404` for an allowed exact-path nonexistent-snapshot `DELETE` and `403` for
  a denied one;
- blob versioning, soft delete, and configured retention posture.

The repository includes an executable provider:

```bash
make test-live-azure
make test-live-azure-storage
make test-live-azure-gateway
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

`make test-live-azure` runs both providers. Missing configuration or an
ambiguous authorization status fails closed.

`HARNESS_LIVE_AZURE_COMMAND` can override the bundled provider when an
organization needs an equivalent internal runner.
