# Overmesh

Overmesh is a Microsoft Entra-native storage federation layer that provides a
strictly replicated Azure Blob-compatible surface across two Azure regions.

## Specifications

- [`OVERMESH_V1_SPECIFICATION.md`](OVERMESH_V1_SPECIFICATION.md)
- [`OVERMESH_DEVELOPMENT_HARNESS_SPECIFICATION_V1.md`](OVERMESH_DEVELOPMENT_HARNESS_SPECIFICATION_V1.md)

Overmesh is distributed under the [MIT License](LICENSE).

## Development Harness

The harness deliberately has two validation layers:

- `run-all`: an environment-independent declarative state/fault model for
  protocol histories and invariant checking;
- `validate-system` plus the reconciler smoke: authoritative conformance
  against the real Gateway, Reconciler, and both storage replicas.

The Rust harness also includes:

- a strict declarative scenario loader and independent reference model;
- twelve observation-based invariant checks;
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

Release validation fails closed until `HARNESS_LIVE_AZURE_COMMAND` is
configured to execute the live Azure conformance provider.

## Versioning

The project follows Semantic Versioning. `VERSION` is the canonical version,
and every Rust workspace module inherits the same version.

```bash
make version-check
```

The command verifies `VERSION`, all workspace packages, and the active
milestone in `roadmap.toml`.

## Current milestone

Project version `0.7.0` is the active milestone for `DELETE`, signed
tombstones, retention, and garbage collection. Milestone `0.6.0` completed
validated `HEAD` and `GET`, including strict two-replica metadata validation,
O(1) replay protection, block-level integrity checks, byte ranges, logical
ETag conditions, and primary content reads with availability-only fallback.
Before enabling DELETE, `0.7.0` also hardens the live Azure authorization gate,
uses paged integrity metadata, provides a block-manifest-free HEAD fast path,
and runs a Rust Validation Engine against the public Gateway and both replicas.
The implemented `0.7.0` path now publishes W=2 signed tombstones, preserves
physical content during configurable retention, prevents replay below signed
high-water and compaction checkpoints, and lets the Reconciler incrementally
collect only fully validated superseded committed generations. Destructive
collection is planned only after complete two-replica validation, content
validation streams without whole-blob buffering, RF=2 placement works across
N-node Rings, trusted keys have signed-time validity windows, and Ring updates
are cryptographically chained to their trusted predecessor.

Before V1 stabilization, milestone `0.10.0` will establish signed live
performance baselines against direct Azure Storage and Overmesh paths.
Milestone `0.11.0` will apply and remeasure only optimizations justified by
those historical results.

Milestone `0.9.0` starts by deploying the Gateway and Reconciler on private
Azure Container Apps with managed identities. Front Door and client
compatibility validation follow only after that runtime infrastructure is
operational.

## Continuous integration

The GitHub Actions workflow in `.github/workflows/ci.yml` runs the complete
local release gate, including formatting, Clippy, workspace tests, declarative
scenarios, process-level system validation, and Reconciler smoke tests.

`.github/workflows/live-azure-gate.yml` also runs nightly and on demand against
the retained private Azure environment. The `live-azure` GitHub environment
must define the three Azure OIDC secrets and the
`OVERMESH_LIVE_RESOURCE_GROUP` and `OVERMESH_LIVE_VM` variables. Its federated
identity only needs permission to execute Run Command on the validation VM.
