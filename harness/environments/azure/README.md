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

## Performance campaigns

`make test-live-azure-performance` executes the versioned matrix in
`harness/performance/live-v5.1.toml`. This is the fast, 38-case diagnostic
contract used while milestone 0.11 optimizations are being implemented. It has
a 3,600-second client-execution budget, zero warm-up iterations, and three
repeats. Reads run 20 measured operations per repeat; writes and deletes 10;
100-entry listing 10; 1,000-entry listing 3; container listing 5; block
sequences 5; and `Get Block List` 10.
Repeats are separated by the rest of the matrix so their p50 spread
measures run-to-run conditions rather than adjacent samples. The retained
0.10.0 baseline remains bound to `live-v1.toml`; v2 is the first
request-attributed contract and v3 is the single-path, 240-read-sample
predecessor. The retained v4 contract remains immutable for its signed campaign.
The v5 contract is likewise retained unchanged as the source of the signed
failed-campaign diagnostic. V5.1 records that diagnostic and its SHA-256 as the
basis for the operator-approved fast diagnostic. It sets
`baseline_eligible = false`, so an unsigned or signed execution can neither
establish nor replace the milestone 0.11 non-regression baseline. A first
execution records `diagnostic-not-baseline`; comparison with an earlier
execution of the same contract records `diagnostic-compared`. The p50 policy is
explicitly `signal-only`; only backend requests per operation and listing
requests per scanned entry remain blocking.

The four 5,000-entry cases are isolated in
`harness/performance/live-v5.1-listing-confirmation.toml`. That contract runs
one measured operation in each of three repeats, with no warm-up, and is
launched once after the listing optimization lands:

```bash
OVERMESH_LIVE_PERFORMANCE_CONTRACT=harness/performance/live-v5.1-listing-confirmation.toml \
  make test-live-azure-performance
```

The fast contract pins the confirmation contract by path and SHA-256. Both
contracts retain every measured latency sample in each run so the later,
high-iteration baseline can size its samples from observed distributions.
Because the fast contract samples each block-sequence case only 15 times across
three repeats, a passing diagnostic does not by itself prove that the rare
442/443 request-budget instability observed by live-v5 has disappeared.

Each read case cycles deterministically over the same 24 logical paths in every
repeat and campaign. Setup creates every path at the case payload size before measurement.
Collection fails unless the paths exercise all three RF=2 placement
pairs, every individual client operation keeps the declared request budget,
and every repeat has zero unattributed requests. Evidence records per-run p50,
the max/min p50 spread, exact request budgets per run, placement coverage, and
campaign-level read and write resolution. V5.1 materializes the median of the
per-run p50 values as the normative comparison statistic while preserving the
pooled p50 as a diagnostic. Schema v5 also records the direct
target's worst spread and the number of cases eligible for latency gating,
including machine-readable reasons for every case degraded to a signal.
Resolution describes variation between repeats inside one campaign; it does
not estimate drift between campaigns run hours or days apart. Pool provisioning
and cleanup remain outside the measured campaign window. V5.1 counterbalances
the direct and Gateway target order deterministically across repeats and cases,
and records the actual order in every run. Both targets use the same managed
identity, Azure SDK versions, validation host, payload bytes, operation count,
and concurrency.

### Certified 0.11.1 current matrix

`harness/performance/live-v7-certified-current-matrix.toml` is the
baseline-eligible, 43-case contract for the 0.11.1 closure. It runs twice from
the same dedicated `Standard_D2as_v5` validation host: first against the
pre-optimization `v0.11.0` base at
`5202eccff4b1e277342cf784dde285e891eb865b`, instrumented by
`9aa9fff33c1a7d75406d6570445da503c2c3cdad`, then against the final `0.11.1`
base at `5596a1701bec0c0132a715b28c92013c4550d150`, instrumented by
`1cce8e6d3120370cec773e19d33c61ddb047a5dd`. Keep the harness tooling and
contract at the closure checkout for both runs; only the deployed Gateway
runtime changes. Checking out the old runtime would change the contract bytes
and make the evidence incomparable.

Both derived runtimes add only the request-batched evidence transport. The
contract pins the byte-identical protocol source SHA-256
`cfcc9bdca85ab9a0b68709c1c3cbacf65e1a23dd5a594fb707fdc2b91e9f67ff`.
Canonical validation rejects a derived commit whose declared base differs,
a different telemetry format or protocol hash, an incomplete batch, or a
comparison between different protocols.

The contract freezes the retained `5202ecc` baseline and the closed ADR-0012
budgets. First `PUT` moves from 49 to 33 requests, established overwrite from
49 to 37, and `DELETE` from 43 to 31. Structural block-sequence budgets move
from 181 to 153 requests at 16 MiB and from 442 to 396 at 100 MiB; permitted
duration-dependent `control_renew_lock` requests remain outside those counts.

The runner records a pseudonymous host fingerprint derived from
`OVERMESH_LIVE_PERFORMANCE_HOST_ID`; it never retains the raw value. Both
evidence documents must carry that same fingerprint and the required SKU. The
first run must use the `pre-optimization` role, fixed 0.11.0 commit, and
0.11.0 project version. Only the `final` role at project version 0.11.1 may
compare against it. A paired comparison proves the two distinct deployment
identities, matching environment string, distinct run IDs, and the complete
baseline/current runtime identities in signed historical evidence.

Affected paths will declare paired **structural** budgets in the one signed
contract, excluding only explicitly allowed `control_renew_lock` events. The
baseline is accepted only at its declared pre-optimization count; the final
run is accepted only at its exact final count. Any higher **or lower**
structural count requires freezing a new contract before run one—lower counts
do not pass an existing final budget. Unchanged non-listing and listing budgets
remain exact.

The 7,200-second client wall-time ceiling derives from retained execution:
the corrected v5.1 fast campaign took 2,846.863107 seconds and its corrected
5,000-entry confirmation took 362.995550 seconds. Doubling their combined
3,209.858657-second execution provides room for the integrated current matrix
and its added comparable hierarchical case without silently reducing samples.

The current matrix includes both 5,000-entry hierarchical cases. The
`max_results=10` `delimiter-page` case verifies continuation pagination over
five pages; `max_results=1000` `comparable-page` measures the comparable
full-result cost.

With the contract bytes frozen, deploy the immutable 0.11.0 runtime and
establish the baseline:

```bash
export OVERMESH_LIVE_PERFORMANCE_CONTRACT=harness/performance/live-v7-certified-current-matrix.toml
export OVERMESH_LIVE_PERFORMANCE_HOST_SKU=Standard_D2as_v5
export OVERMESH_LIVE_PERFORMANCE_HOST_ID="$(cat /sys/class/dmi/id/product_uuid)"
export OVERMESH_LIVE_PERFORMANCE_RUNTIME_ROLE=pre-optimization
export OVERMESH_LIVE_PERFORMANCE_COMMIT=9aa9fff33c1a7d75406d6570445da503c2c3cdad
export OVERMESH_LIVE_PERFORMANCE_PROJECT_VERSION=0.11.0
export OVERMESH_LIVE_PERFORMANCE_RELEASE_TAG=v0.11.0
export OVERMESH_LIVE_PERFORMANCE_BACKEND_TELEMETRY_FORMAT=request-batch-v1
export OVERMESH_LIVE_PERFORMANCE_TELEMETRY_PROTOCOL_SHA256=cfcc9bdca85ab9a0b68709c1c3cbacf65e1a23dd5a594fb707fdc2b91e9f67ff
shasum -a 256 "$OVERMESH_LIVE_PERFORMANCE_CONTRACT"
make test-live-azure-performance
```

Verify the baseline canonical evidence and `SHA256SUMS`, retain its canonical
evidence path, then deploy the final runtime on the unchanged host. The final
commit must have an annotated nearest candidate or release tag accepted by
`validate-live-performance.sh`:

```bash
export OVERMESH_LIVE_PERFORMANCE_RUNTIME_ROLE=final
export OVERMESH_LIVE_PERFORMANCE_COMMIT=1cce8e6d3120370cec773e19d33c61ddb047a5dd
export OVERMESH_LIVE_PERFORMANCE_PROJECT_VERSION=0.11.1
export OVERMESH_LIVE_PERFORMANCE_RELEASE_TAG=v0.11.1-rc.1
export OVERMESH_LIVE_PERFORMANCE_BASELINE_EVIDENCE=/path/to/pre-optimization-evidence.json
shasum -a 256 "$OVERMESH_LIVE_PERFORMANCE_CONTRACT"
make test-live-azure-performance
```

The final invocation validates the canonical baseline with the same contract,
requires the same host, compares the two roles, and signs final evidence only
when the exact final budget and historical gate both pass.

The performance gate is intentionally excluded from `test-pre-pr-live` because
it is long-running and retains signed release evidence. `make test-release`
includes it.

The current contract covers first `Put Blob`, overwrite, full `Get Blob`,
ranged reads, `Head Blob`, and established-blob `Delete Blob` at 1 KiB, 1 MiB,
and 16 MiB where applicable, with concurrency levels 1, 4, and 16. Deliberate
matrix exclusions carry reasons in the contract. Warm-up samples are excluded.
Retained measurements include min, mean, p50, p90, p95, max, operations per
second, bytes per second, successful and failed operation counts, and
gateway-to-direct ratios. Thirty samples do not support a distinct p99, so the
contract does not publish one.

V5 adds flat, hierarchical, paginated, and container listing, complete staged
block upload sequences, and committed block-list reads. Listing fixtures are
persistent and their sorted logical-name manifest, payload size, and content
hashes are checked before measurement. Direct and Gateway fixtures use
disjoint target namespaces under the same canonical manifest so a direct
physical upload cannot bypass or collide with the Gateway catalogue. The
signed fixture evidence records both target namespaces and identifies the
manifest as canonical and target-independent. The 20 container fixtures
must be pre-created on every backend replica; the runner writes their sentinel
through both the direct and Gateway targets so both surfaces are validated.
Fixture setup time and Gateway backend request count are campaign evidence but
remain outside every measured case window. Listing cases use a 600-second
request timeout. The 5,000-blob fixtures are traversed in pages of at most
1,000 entries so each request remains below Azure Front Door's 240-second
origin-response ceiling; the measured operation still validates all 5,000
logical names.

The v5 non-regression policy separates controlled and observed quantities.
Backend requests per operation are deterministic, exact, and blocking. A p50
Gateway-to-direct overhead comparison becomes blocking only when both targets
in both campaigns measure that case below the contract's p50 spread threshold;
otherwise it remains a signal. Absolute latency and p95 remain informational.
The evidence publishes eligible and total case counts so a latency gate with
little or no effective coverage cannot appear equivalent to a fully active
gate.
For `live-v5`, the historical blocking request gate remains
`requestsPerEntryScanned`. Starting with `live-v5.1`, listing evidence separates
`entriesConsidered`, `entriesValidated`, and `entriesReturned`; the blocking
gate is `requestsPerEntryValidated = 4.0`. The evidence also records the
configured validation concurrency. Fixed pagination, authorization, and
quarantine-list requests remain visible in full backend telemetry but do not
dilute the four catalogue/head reads per validated entry.

Additional required environment:

- `OVERMESH_LIVE_PERFORMANCE_RING_VERSION`;
- `OVERMESH_LIVE_PERFORMANCE_RING_HASH`;
- `OVERMESH_LIVE_PERFORMANCE_DEPLOYMENT`;
- `OVERMESH_LIVE_PERFORMANCE_RELEASE_TAG`, naming an annotated tag that is an
  ancestor of the campaign commit;
- `OVERMESH_LIVE_PERFORMANCE_ENVIRONMENT`;
- `OVERMESH_LIVE_PERFORMANCE_ISOLATED_ENVIRONMENT=true`;
- `OVERMESH_LIVE_PERFORMANCE_PUBLIC_KEY`;
- `OVERMESH_LIVE_PERFORMANCE_WORKSPACE_ID` (the workspace customer GUID);
- `OVERMESH_LIVE_PERFORMANCE_GATEWAY_APP_NAME`, as a comma-separated list
  when Front Door can route to multiple Gateway Container Apps;
- `OVERMESH_LIVE_PERFORMANCE_GATEWAY_RESOURCE_ID`, in the same order and as a
  comma-separated list when multiple Gateway resources serve the endpoint;
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
ratios, backend requests per operation, signing p95, campaign peak CPU, and
campaign peak memory.
Without a predecessor, a baseline-eligible contract records
`baseline-established`; the active v5.1 contracts instead record
`diagnostic-not-baseline`.

The client-side baseline deliberately does not infer server behavior. Gateway
logs emit one structured `overmesh_backend_request` event per Storage request
and one `overmesh_manifest_sign` event per Key Vault signing request, including
duration, success, backend operation and object class. Evidence publishes
object-class totals and operation/object-class decompositions so generic
`control_get_object` traffic becomes a checkable budget. Before signing, the
live gate queries those events per case across every Gateway origin and waits
for the Azure Monitor event count to stabilize before accepting the result.
Every backend and signing event carries a SHA-256 fingerprint of the incoming
`x-ms-client-request-id`; setup, warm-up, measured, and cleanup operations use
distinct identifiers. The collector requires all measured request fingerprints
and retains only their count, never the identifiers themselves. It also sums
Container Apps `UsageNanoCores`, `WorkingSetBytes`, and replica metrics across
the configured Gateway resources once for the campaign. The gate requires an
explicit isolated-environment assertion so other client traffic cannot
contaminate backend request counts. Backend timings explicitly measure time to
response headers, not full response-body transfer. Azure Monitor exposes
resource metrics at one-minute granularity, so they are not attributed to
individual sub-minute cases. Raw logs and Azure resource identifiers are not
retained.

## Client-observed campaigns

`make test-live-azure-client-observed` runs
`harness/environments/azure/validate-client-observed.sh`, which executes the
standalone contract `harness/performance/client-observed-v1.toml` through
`harness/environments/azure/performance/client_observed_campaign.py`. It
measures what the Azure CLI and AzCopy observe from a real operator machine,
against the direct Storage endpoint and against the Gateway, and publishes
`performance.overmesh.io/client-observed/v1` bundles under
`harness/artifacts/client-observed/`.

That schema is deliberately separate from the isolated performance contract.
`validate_performance_evidence.py` and `compare_live_performance.py` both
reject it, so a client-observed bundle can never become a baseline, a gate, or
a comparison input. The campaign requires a non-isolated client context
supplied through the environment, runs five repetitions, and publishes only
observation lists with their minimum, median and maximum. It computes no p50
spread ratio and no stability or gating metric.

Each measurement publishes the exact remote paths the tools touched, its own
measurement window, and the campaign setup window. A download reads a source
under its published attribution prefix; the seed write that creates that
source is excluded by window, and the bundle declares it. The runner tracks
every prefix it writes on both targets and removes them with the same Entra
login after the last measured operation, including after a partial failure.
Cleanup gets its own window: the runner waits for the second to turn over so
`cleanupWindow.startedAt` is strictly after `measurementWindow.finishedAt`,
and the bundle publishes that window with the cleaned prefixes under
`campaign.cleanup.excludedBy = "campaign-cleanup-window"`. The validator
refuses any case window that reaches into it, so no collector can attribute a
deletion to the last measured case.

A campaign owns `.harness/client-observed/<run-id>/staging`, which the runner
deletes, and `.harness/client-observed/<run-id>/artifacts`, which it never
touches. The raw client result and the server telemetry file live in
`artifacts`. Set `OVERMESH_CLIENT_OBSERVED_TELEMETRY` when the collector
writes the telemetry file elsewhere.

Credentials come from the environment only, and none is created here. The
driver prefers `AZURE_CLIENT_CERTIFICATE_PATH`, then
`AZURE_FEDERATED_TOKEN_FILE`, then `AZURE_CLIENT_SECRET`, and the bundle
records the mode it used. AzCopy authenticates entirely from environment
variables. The Azure CLI cannot read `--password` from standard input, so in
`client-secret` mode the secret is visible in the `az login` argv on the
operator host while the login runs; certificate mode passes a path instead.
The driver points `AZURE_CONFIG_DIR` at a private
`.harness/client-observed/<run-id>/azure-cli-config` directory — a sibling of
the staging tree, not a child of it, because the runner deletes staging while
the campaign is still logged in — and traps every exit path to run
`az account clear`, `az logout` and remove it, so the operator's own
`~/.azure` profile is untouched. The runner refuses to dispatch any command
unless that directory exists outside staging.

The campaign is excluded from `test-pre-pr-live`, `test-main` and
`test-release`. It is an operator-initiated exercise, never a release gate.
`harness/artifacts/client-observed/README.md` documents the protocol, the
required environment, the mandatory disclaimer, and the retention rules.
