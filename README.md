# Overmesh

Overmesh is a Microsoft Entra-native storage federation layer that provides a
strictly replicated Azure Blob-compatible surface across two Azure regions.

## Specifications

- [`OVERMESH_V1_SPECIFICATION.md`](OVERMESH_V1_SPECIFICATION.md)
- [`OVERMESH_DEVELOPMENT_HARNESS_SPECIFICATION_V1.md`](OVERMESH_DEVELOPMENT_HARNESS_SPECIFICATION_V1.md)
- [`COMPATIBILITY.md`](COMPATIBILITY.md)

Overmesh is distributed under the [MIT License](LICENSE).

## Development Harness

The harness deliberately has two validation layers:

- `run-all`: an environment-independent declarative state/fault model for
  protocol histories and invariant checking;
- `validate-system` plus the reconciler smoke: authoritative conformance
  against the real Gateway, Reconciler, and both storage replicas.

The Rust harness also includes:

- a strict declarative scenario loader and independent reference model;
- fourteen observation-based invariant checks;
- deterministic commit failpoints;
- ES256 canonicalization and signature tests;
- deterministic dataset generation;
- JSON reports;
- two Azurite Blob backends behind Toxiproxy.

Run the fast validation suite:

```bash
make test-pr
```

Start the local storage and fault-injection environment:

```bash
make dev-up
```

Stop it and remove only its named local volumes:

```bash
make dev-down
```

Control deterministic backend faults:

```bash
cargo run --quiet -p overmesh-harness -- fault disable a
cargo run --quiet -p overmesh-harness -- fault latency b --milliseconds 500
cargo run --quiet -p overmesh-harness -- fault reset
```

Run the process-level gateway authentication and signed dual-write smoke test:

```bash
make gateway-smoke
```

When a local Gateway is already running, execute the authoritative real-system
validator directly:

```bash
make validate-system
```

Run reconciliation, repair, quarantine, audit, and recovery against both local
replicas:

```bash
make reconciler-smoke
```

The optional local pre-PR Azure gate is `make test-pre-pr-live`. It is
deliberately absent from GitHub Actions because enterprise policy prohibits
Azure login from hosted workflows. Focused targets cover Storage posture,
negative RBAC posture, Gateway authorization, client compatibility, placement,
and reconciliation.

## Versioning

The project follows Semantic Versioning. `VERSION` is the canonical version,
and every Rust workspace module inherits the same version.

```bash
make version-check
```

The command verifies `VERSION`, all workspace packages, and the release state
in `roadmap.toml`.

## Current release state

Project version `0.9.0` has completed private Azure Container Apps
infrastructure, client compatibility, and live Azure conformance.
Milestone `0.8.0` completed listing, block APIs, and signed continuation
tokens. Milestone `0.7.0` completed signed tombstones,
retention, validate-plan-execute garbage collection, overwrite collection,
signed history compaction, streaming reconciliation, RF=2 placement across
N-node Rings, key-validity windows, Ring lineage, and the hardened live Azure
authorization gates.

Milestone `0.8.0` implements Azure-compatible logical container/blob listing,
W=2 signed block staging and block-list commits, committed/uncommitted block
inspection, and opaque signed continuation tokens. Blob listing uses a W=2
lexicographically ordered catalog whose mutable entries are the exact signed
current heads. Listing compares each selected catalog entry byte-for-byte on
both replicas, validates its signature and Ring placement, and excludes a
request-level union of quarantine keys. Full head, high-water,
sidecar, and compaction validation remains on HEAD, GET, and Reconciler paths
rather than adding eight control-plane reads to every listed item. Physical
customer paths, system objects, staged blocks, tombstones, and quarantined
records are never client-visible. Container listing filters catalog-derived
candidates with caller-authorized container probes, avoiding account-scoped
caller RBAC and keeping the system container inaccessible.

Milestone `0.10.0` established signed live performance baselines against
direct Azure Storage and Overmesh paths. Milestone `0.10.1` adds complete
request attribution, object-class budgets, overwrite workloads, and the first
blocking backend-request baseline. Milestone `0.11.0` will apply and remeasure
only optimizations justified by those historical results.

Milestone `0.9.0` deploys the Gateway and Reconciler on private Azure Container
Apps with managed identities and exposes them through Azure Front Door Premium
Private Link. Its retained live evidence covers authorization revocation,
three-account RF=2 placement and a single-account outage, plus Azure SDK .NET,
Python, JavaScript, Azure CLI, and AzCopy compatibility.

## Continuous integration

The GitHub Actions workflow in `.github/workflows/ci.yml` runs the complete
local release gate, including formatting, Clippy, workspace tests, declarative
scenarios, process-level system validation, and Reconciler smoke tests.

`.github/workflows/live-azure-gate.yml` also runs nightly and on demand against
the retained private Azure environment. The `live-azure` GitHub environment
must define the three Azure OIDC secrets and the
`OVERMESH_LIVE_RESOURCE_GROUP` and `OVERMESH_LIVE_VM` variables. Its federated
identity only needs permission to execute Run Command on the validation VM,
which must expose the managed identity used by the client compatibility gate.
