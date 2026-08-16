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
- `404` for an allowed absent-blob `HEAD` and `403` for a denied one;
- `404` for an allowed exact-path nonexistent-snapshot `DELETE` and `403` for
  a denied one;
- blob versioning, soft delete, and configured retention posture.

The repository includes an executable provider:

```bash
make test-live-azure
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

The provider verifies ARM security posture, runs the Reconciler RBAC audit,
then executes allowed and denied write, read, and delete probes on both
accounts for every configured Storage API version. Missing configuration or
an ambiguous authorization status fails closed.

`HARNESS_LIVE_AZURE_COMMAND` can override the bundled provider when an
organization needs an equivalent internal runner.
