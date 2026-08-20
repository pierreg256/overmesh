# OVERMESH DEVELOPMENT HARNESS SPECIFICATION V1

## 1. Status

This document is the normative specification for the Overmesh V1 development
and validation harness.

It is subordinate to `OVERMESH_V1_SPECIFICATION.md`. If the two documents
conflict, the product specification takes precedence and the harness
specification MUST be corrected.

The terms **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are normative.

## 2. Purpose

The Development Harness is a first-class component of Overmesh.

It is not a secondary test utility. It is the primary mechanism used to prove
that the Overmesh contract remains stable as the implementation evolves.

Development follows a strict test-first process:

```text
No feature may be implemented without an associated validation scenario.
```

Every bug fix MUST add a regression scenario that fails before the fix and
passes after it.

## 3. Principles

The harness MUST be:

- deterministic for functional outcomes;
- reproducible from recorded inputs and seeds;
- automated;
- versioned;
- executable locally;
- executable in CI/CD;
- capable of running against Azurite;
- capable of running against real Azure resources;
- independent enough to detect defects in production code.

Functional results MUST not depend on:

- the developer;
- the workstation;
- execution order;
- the selected Azure region;
- wall-clock timing accidents.

Performance measurements, network latency, and Azure service timing are not
expected to be byte-for-byte identical across environments. They MUST be
evaluated using environment-specific thresholds and MUST NOT change functional
correctness expectations.

The harness MUST record all random seeds, virtual-clock values, component
versions, image digests, SDK versions, and fault schedules required to replay a
failure.

The project follows Semantic Versioning. Every module participating in a build
MUST use the same canonical project version and MUST correspond to the active
roadmap milestone.

## 4. Scope

The harness validates:

- the public Azure Blob-compatible HTTP contract;
- Microsoft Entra-only authentication behavior;
- compile-time caller/control credential separation;
- operation-specific Azure Storage authorization probes;
- exact canonical resource binding between authorization, placement, storage,
  manifests, and audit;
- the fixed RF=2, W=2, R=2 consistency model;
- the signed Ring;
- signed commit and block manifests;
- logical ETags and conditional operations;
- the `PREPARED`, `COMMITTED`, and `TOMBSTONED` state machine;
- idempotency and concurrency control;
- block-level content validation;
- reconciliation, repair, and quarantine;
- Ring migration and rollback protection;
- Azure SDK, Azure CLI, and AzCopy compatibility;
- recovery from deterministic failures at every commit transition.

The harness does not treat local emulation as proof of:

- Azure RBAC correctness;
- Microsoft Entra production integration;
- Azure Key Vault behavior;
- Azure Front Door limits;
- Azure regional failure behavior;
- Azure Storage performance or scalability;
- complete Azure Blob Storage compatibility.

Those properties require live Azure conformance tests.

## 5. Architecture

```text
                    +-----------------------+
                    | Scenario Orchestrator |
                    +-----------+-----------+
                                |
       +------------------------+------------------------+
       |                        |                        |
       v                        v                        v
+---------------+       +---------------+       +----------------+
| Reference     |       | Fault         |       | Validation     |
| Model / Oracle|       | Controller    |       | Engine         |
+---------------+       +---------------+       +----------------+
       |                        |                        |
       +------------+-----------+------------+-----------+
                    |                        |
                    v                        v
          +------------------+      +------------------+
          | Identity / PKI   |      | Ring Lab         |
          | Lab              |      |                  |
          +--------+---------+      +------------------+
                   |
                   v
          +------------------+
          | Overmesh Gateway |
          +--------+---------+
                   |
             +-----+-----+
             |           |
             v           v
        Storage A     Storage B
             |           |
             +-----+-----+
                   |
                   v
          +------------------+
          | Reconciler       |
          +------------------+
```

### 5.0 Two Validation Layers

The harness has two explicit, complementary layers:

1. **Declarative protocol model.** `overmesh-harness run-all` executes strict
   schema-versioned histories against an independent state/fault model. It
   records public observations, consistency decisions, repair attempts,
   retention time, and collection history, then evaluates protocol invariants.
   This layer is deterministic and environment-independent. It is not proof
   that the production Gateway or Reconciler conforms.
2. **Real-system conformance.** `overmesh-harness validate-system` drives the
   public Gateway and independently reads both physical replicas.
   `reconciler-smoke.sh` exercises the production Reconciler. These checks are
   the authoritative evidence for implementation conformance. Shell scripts
   may provision and orchestrate the processes, but the Rust validator owns
   the cross-layer assertions.

Scenarios SHOULD remain focused protocol histories rather than duplicating
every real-system smoke assertion. A scenario's `environment.providers` lists
the providers for which that history is applicable; the declarative runner
always executes the model provider.

### 5.0.1 Assistant Work Exchange

The harness exposes a local, file-backed work exchange for assistants operating
on the same repository. It is a typed work queue, not a chat or shared-memory
service.

- Each thread MUST live under `.overmesh/exchange/<nnnn>-<slug>/`.
- Each message MUST be one immutable, schema-versioned JSON file.
- Message creation MUST be append-only and safe under concurrent writers.
- Every accepted message MUST be staged with `git add` and MUST NOT be
  committed by the harness.
- Findings, corrections, reports, and verdicts MUST carry the repository refs
  required by their message kind, and refs MUST be validated before writing.
- Thread state and `waitingOn` MUST be derived from message files on every
  read; no derived state file is permitted.
- An unapproved `spec` body MUST be withheld from assistant readers.
- A `verdict` MUST NOT resolve a thread until an operator approval follows it.
- The sixth consecutive non-human message MUST be rejected. Five consecutive
  non-human messages place the thread in `escalated` state until a human posts.
- The assistant allowlist and consecutive-message limit MUST be loaded from the
  committed `.overmesh/exchange/config.json`. Environment variables MUST NOT
  override either control.
- The author of the last non-verdict message MUST NOT author the verdict.
- The stdio MCP server MUST reject a missing, unapproved, or `human` server
  identity. Human messages MUST enter through the operator CLI.

The version 1 MCP surface is limited to `exchange_list`, `exchange_read`,
`exchange_post`, and `exchange_resolve`. The operator surface is
`overmesh-harness exchange`; it owns approval and rejection. Neither surface
executes work instructions automatically.

### 5.1 Scenario Orchestrator

The Scenario Orchestrator:

- provisions the selected environment;
- loads a versioned scenario;
- configures identity, Ring, time, and fault state;
- executes client operations;
- invokes validation checkpoints;
- collects traces and backend state;
- runs cleanup;
- writes machine-readable reports.

### 5.2 Reference Model

The Reference Model is an independent executable specification of legal
Overmesh behavior.

It models at least:

- logical blob absence;
- replica-local immutable content versions;
- `PREPARED` manifests;
- `COMMITTED` heads;
- `TOMBSTONED` heads;
- logical versions;
- logical ETags;
- idempotency records;
- Ring placement;
- health states;
- quarantine state.

The Reference Model MUST NOT import gateway or reconciler business logic.
Reusing production state-transition code would make the harness incapable of
detecting matching implementation defects.

### 5.3 Fault Controller

The Fault Controller combines:

- deterministic network proxies;
- process lifecycle control;
- test-only protocol failpoints;
- virtual-clock control;
- backend mutation tools.

Test-only failpoints MUST NOT be exposed in production builds.

### 5.4 Validation Engine

The Validation Engine observes:

- the public Overmesh API;
- both backend Storage Accounts;
- signed system objects;
- gateway and reconciler events;
- audit and quarantine outputs.

It compares observed behavior with the Reference Model and the invariants in
the product specification.

The executable harness MUST include a real-system adapter that drives the
public Gateway, reads both backend replicas, independently verifies signed
objects, and compares the observations. Bash smoke scripts MAY orchestrate
processes, but MUST NOT be the sole implementation of validation assertions.

### 5.5 Identity and PKI Lab

The Identity and PKI Lab provides:

- deterministic local OAuth tokens;
- issuer, audience, tenant, expiry, and signature test cases;
- ES256 Ring signing;
- ES256 blob-manifest signing;
- independent Ring and blob keys;
- key rotation and trust-bundle scenarios;
- live Azure Key Vault integration tests.

### 5.6 Ring Lab

The Ring Lab builds, signs, publishes, migrates, and invalidates placement
Rings.

The term **Ring** MUST be used only for placement Rings. Test groupings are
called **suites**, not Rings.

## 6. Repository Structure

```text
repo/
  gateway/
  reconciler/
  ring-builder/
  harness/
    environments/
      azurite/
      azure/
    scenarios/
    model/
    faults/
    validators/
    datasets/
    identity/
    keys/
    rings/
    manifests/
    tombstones/
    sdk/
    azcopy/
    cli/
    traces/
    reports/
    artifacts/
```

The following content MUST be versioned:

- scenario definitions;
- schemas;
- deterministic dataset generators;
- expected hashes;
- canonical cryptographic vectors;
- Ring fixtures;
- manifest fixtures;
- fault schedules;
- SDK and tool version matrices;
- normalized golden responses.

Generated reports, traces, temporary credentials, downloaded tools, and large
runtime artifacts MUST NOT be committed.

## 7. Environment Provisioning

The harness supports two environment providers.

### 7.1 Local Azurite Environment

The local environment starts:

- Azurite A;
- Azurite B;
- backend fault proxies;
- a deterministic test identity issuer;
- a test ES256 signer;
- the Overmesh gateway;
- the Overmesh reconciler;
- the Scenario Orchestrator;
- the Validation Engine.

Azurite is used for fast protocol and failure testing. It MUST NOT be treated
as proof of production Microsoft Entra, RBAC, Azure Key Vault, regional,
performance, or complete REST API behavior.

Any emulator-specific credential used by harness control code MUST be isolated
from the system under test. The gateway and reconciler MUST use the configured
OAuth path.

### 7.2 Live Azure Environment

The live environment provisions:

- two Storage Accounts in distinct Azure regions;
- `AllowSharedKeyAccess = false`;
- private or appropriately restricted network access;
- Microsoft Entra role assignments;
- managed identities for internal components;
- Azure Key Vault or Managed HSM ES256 keys;
- optional Azure Front Door coverage;
- isolated resources identified by a harness run ID.

Live cleanup MUST target only the explicitly provisioned run resources.

### 7.3 Commands

The repository MUST expose stable entry points equivalent to:

```text
make dev-up
make dev-down
make dev-reset
make test-pr
make test-main
make test-nightly
make test-release
make test-live-azure
make validate-system
overmesh-harness validate-system
```

The concrete implementation may use another build tool internally, but these
entry points MUST remain suitable for local and CI execution.

`dev-down` and cleanup commands MUST refuse to delete resources that are not
tagged with the current harness run ID.

## 8. Determinism and Time

The harness MUST support an injectable clock for:

- lease expiry;
- token expiry scenarios;
- tombstone retention;
- abandoned `PREPARED` cleanup;
- Ring activation;
- key validity windows;
- reconciliation schedules.

Scenarios MUST NOT depend on arbitrary sleeps.

Eventual operations MUST be tested using:

- an explicit deadline;
- deterministic polling;
- an expected terminal state;
- captured intermediate states.

Randomized and property-based tests MUST record the seed in every report.

Container images, tools, SDK packages, and API versions MUST be pinned.

## 9. Dataset Lab

Datasets are generated deterministically from versioned specifications and
seeds.

Large files SHOULD be generated during the test run instead of committed to
the repository.

### 9.1 Dataset Classes

```text
tiny/
small/
medium/
large/
block-boundaries/
range-boundaries/
corruption/
metadata/
migration/
unicode/
listing/
```

### 9.2 Required Data Cases

The dataset MUST include:

- zero-length blobs;
- one-byte blobs;
- text and JSON;
- binary data;
- image and PDF fixtures with known hashes;
- highly compressible and incompressible content;
- content immediately below, at, and above configured block boundaries;
- multi-block content;
- large streamed content;
- Unicode blob names;
- escaped and ambiguous path characters;
- case-sensitive blob names;
- metadata near Azure limits;
- empty and complex metadata values;
- large listings with multiple continuation pages.

Each generated object MUST have a versioned expected SHA-256 hash.

The same logical datasets are used in local and live Azure tests, although
large-size profiles MAY differ for cost and execution-time reasons.

## 10. Ring and PKI Lab

### 10.1 Ring Fixtures

The Ring Lab includes:

```text
ring-v1.yaml
ring-v2.yaml
ring-v3.yaml
ring-invalid-schema.yaml
ring-invalid-signature.yaml
ring-corrupted.yaml
ring-rollback.yaml
ring-unknown-key.yaml
ring-invalid-topology.yaml
```

### 10.2 Algorithms

Test Rings and manifests use:

```text
ECDSA P-256 with SHA-256
JOSE algorithm identifier: ES256
```

Ed25519 MUST NOT be used because the production key-management design uses
Azure Key Vault or Managed HSM, which does not provide Ed25519 signing.

### 10.3 Key Separation

The harness MUST use separate keys for:

- Ring signatures;
- blob commit and block-manifest signatures.

It MUST test:

- active key acceptance;
- unknown key rejection;
- wrong-purpose key rejection;
- retired key rejection;
- overlapping rotation windows;
- inclusive `notBefore` and `notAfter` boundaries;
- rejection of signatures outside the selected key window;
- readability of old objects signed inside a historical key window;
- malformed signature encoding;
- canonicalization differences;
- cross-domain signature substitution.

Local private keys are test fixtures only. They MUST be clearly marked as
non-production and MUST never be accepted by production trust bundles.

### 10.4 Rollback Protection

Rollback tests MUST include a minimum trusted Ring version or hash supplied
outside the downloaded Ring.

A valid signature on an older Ring is insufficient for acceptance.
Root Rings MUST be tested with explicit null parent fields. Rotation tests MUST
bind and validate both the trusted predecessor version and hash, including
wrong-parent-version, wrong-parent-hash, and valid overlapping-key cases.

### 10.5 Commands

```text
make ring-build
make ring-sign
make ring-verify
make ring-publish
make ring-migrate
```

## 11. Fault Injection

Faults MUST be deterministic and addressable by stable identifiers.

### 11.1 Network Faults

```text
FAULT-NET-001  Backend A unavailable
FAULT-NET-002  Backend B unavailable
FAULT-NET-003  Deterministic backend timeout
FAULT-NET-004  Seeded packet loss
FAULT-NET-005  Connection reset
FAULT-NET-006  Delayed response
FAULT-NET-007  Truncated response body
FAULT-NET-008  HTTP error injection
```

### 11.2 Ring Faults

```text
FAULT-RING-001  Invalid Ring signature
FAULT-RING-002  Signed Ring rollback
FAULT-RING-003  Corrupted Ring document
FAULT-RING-004  Unknown Ring signing key
FAULT-RING-005  Invalid replica topology
FAULT-RING-006  Gateways temporarily load different Ring versions
FAULT-RING-007  Missing parent Ring during migration
FAULT-RING-008  Wrong parent Ring hash
FAULT-RING-009  Signature timestamp outside key validity
```

### 11.3 Process Faults

```text
FAULT-PROC-001  Gateway termination
FAULT-PROC-002  Gateway restart
FAULT-PROC-003  Reconciler termination
FAULT-PROC-004  Reconciler restart
FAULT-PROC-005  Signer unavailable
FAULT-PROC-006  Client disconnect
```

### 11.4 Commit Protocol Failpoints

Every write scenario MUST support interruption:

```text
FAULT-COMMIT-001  After lock acquisition
FAULT-COMMIT-002  After current-head validation
FAULT-COMMIT-003  After content write on replica A
FAULT-COMMIT-004  After content write on replica B
FAULT-COMMIT-005  After PREPARED publication on replica A
FAULT-COMMIT-006  After PREPARED publication on replica B
FAULT-COMMIT-007  After prepared-replica verification
FAULT-COMMIT-008  After COMMITTED publication on replica A
FAULT-COMMIT-009  After COMMITTED publication on replica B
FAULT-COMMIT-010  Before final replica verification
FAULT-COMMIT-011  Before the success response
FAULT-COMMIT-012  After commit but before the client receives the response
```

For every commit failpoint, the scenario MUST:

1. interrupt the operation;
2. inspect public visibility;
3. inspect both backend states;
4. restart the affected components;
5. retry with the same write ID;
6. run reconciliation;
7. validate all system invariants.

### 11.5 Concurrency Faults

```text
FAULT-RACE-001  Concurrent PUT operations
FAULT-RACE-002  PUT versus DELETE
FAULT-RACE-003  Concurrent conditional PUT operations
FAULT-RACE-004  Lock expiry during write
FAULT-RACE-005  Stale logical ETag
FAULT-RACE-006  Duplicate request delivery
FAULT-RACE-007  Retry with changed payload
```

### 11.6 Mutation Faults

The harness MUST be able to mutate:

- immutable content;
- individual content blocks;
- block order;
- commit manifests;
- block manifests;
- logical versions;
- logical ETags;
- predecessor ETags;
- Ring versions;
- write IDs;
- signing key IDs;
- signatures;
- tombstones.

## 12. Validation Engine

The Validation Engine is the authoritative test oracle adapter.

It MUST NOT validate a production-generated signature or canonical payload by
calling the same production verification function. Cryptographic test vectors
and canonical serialization MUST have an independent verifier.

### 12.1 Client-Surface Validation

The engine validates:

- HTTP status;
- Azure-compatible error code and body;
- required response headers;
- logical ETag;
- content length;
- range semantics;
- conditional request behavior;
- pagination behavior;
- absence of internal objects;
- client-visible content.

### 12.2 Backend Validation

For each replica, the engine validates:

- immutable content presence;
- immutable content SHA-256;
- block hashes and ordering;
- signed block manifest;
- signed commit manifest or tombstone;
- logical version;
- logical ETag;
- Ring version;
- write ID;
- commit state;
- absence of illegal references.

Raw backend Azure ETags are observations only. They MUST NOT be compared across
replicas as logical consistency identifiers.

### 12.3 Health States

The engine recognizes:

```text
HEALTHY
DRIFTED
MISSING
TAMPERED
QUARANTINED
```

### 12.4 HEAD Semantics

`HEAD` validates signed declarations and replica manifest agreement.

It does not read blob content and therefore MUST NOT be expected to recalculate
the complete content SHA-256 hash. The engine MUST prove that `HEAD` does not
depend on downloading block-manifest pages.

### 12.5 GET Semantics

`GET` MUST validate every affected content block before returning bytes from
that block.

Range scenarios MUST verify complete intersecting blocks, including ranges
that begin or end in the middle of a block.

### 12.6 Reconciliation Semantics

The Validation Engine verifies that:

- repair uses only a provably healthy source;
- tampered or quarantined objects are never repair sources;
- repair is conditional and cannot overwrite a newer head;
- abandoned `PREPARED` objects remain invisible;
- cleanup respects configured retention;
- audit and alert records are emitted;
- tombstoned content is never resurrected.

## 13. Scenario Format

Scenarios MUST be declarative, schema-versioned, and independently executable.

Example:

```yaml
apiVersion: harness.overmesh.io/v1
id: COMMIT-FAIL-008
suite: commit-state-machine
environment:
  providers:
    - azurite
    - azure
seed: 42008
initialState:
  blob: absent
operations:
  - action: putBlob
    blob: /images/photo.jpg
    dataset: medium/photo.bin
    writeId: 2f4d242f-0000-4000-8000-000000000000
faults:
  - id: FAULT-COMMIT-008
expected:
  clientOutcome:
    class: failure-or-ambiguous
  visibleBlob:
    mustNeverBePrepared: true
  retry:
    sameWriteId: succeeds-idempotently
  healthAfterReconciliation: HEALTHY
invariants:
  - INVARIANT-001
  - INVARIANT-006
  - INVARIANT-009
```

The schema MUST distinguish:

- guaranteed success;
- guaranteed failure;
- ambiguous client outcome;
- immediate state;
- expected state after reconciliation.

The V1 operation vocabulary includes conditional `HEAD` and `GET`, byte
ranges, signature invalidation, untrusted signing keys, prepared-publication
observation, replica consistency observation, explicit repair attempts,
virtual-time advancement, collection, resurrection attempts, and
delete/recreate histories. It also includes list-containers/list-blobs,
continuation tamper/reuse/expiry/Ring rollover, Put Block, ordered
Latest/Committed/Uncommitted Put Block List, Get Block List, staged-replica
removal/tampering, and staged collection. Unknown fields and unknown operation
variants MUST be rejected.

## 14. Test Suites

### Suite 0: Protocol Contract

Validates:

- `GET`;
- `HEAD`;
- `Put Blob`;
- `Put Block`;
- `Put Block List`;
- `Get Block List`;
- `DELETE`;
- metadata and properties;
- range reads;
- listing and continuation tokens;
- Azure-compatible errors and headers.

The deterministic model MUST assert that staged blocks remain invisible,
tampered stages are never commit/repair sources, stale stages cannot cross a
delete or overwrite base generation, and signed continuation markers reject
cross-request or Ring reuse.

### Suite 1: Identity and Security

Validates:

- bearer token required;
- token signature;
- issuer;
- audience;
- tenant;
- expiry and not-before time;
- wrong-token rejection;
- Shared Key rejection;
- account SAS rejection;
- service SAS rejection;
- user delegation SAS rejection;
- caller and control tokens are non-interchangeable types;
- caller tokens are never used for system-container operations;
- control tokens are never accepted for caller-authorized data operations;
- one exact authorization decision per supported `DataAction`, using the real
  immutable upload for writes and side-effect-free probes where logical and
  physical operations differ;
- denied-principal probes never return an authorized terminal status;
- caller tenant, object, subject, and application identity are signed into the
  committed write record;
- inherited RBAC assignments covering the system container are detected;
- corresponding replica RBAC assignments and conditions remain symmetric;
- the Reconciler Audit Engine fails readiness before its first successful ARM
  posture audit and whenever that audit is unavailable or unsafe;
- production Azure RBAC behavior in live tests.

Local emulator success MUST NOT satisfy the Azure RBAC release requirement.
Azurite tests MUST still verify credential routing and fail-closed response
classification with deterministic backend doubles.

### Suite 2: Commit State Machine

Validates:

- RF=2;
- W=2;
- R=2 manifest validation;
- `PREPARED` invisibility;
- `COMMITTED` visibility;
- failure at every commit transition;
- client timeout ambiguity;
- recovery and idempotent retry.

### Suite 3: Failure and Recovery

Validates:

- backend failures;
- network timeouts;
- process termination;
- gateway restart;
- reconciler restart;
- signer failure;
- abandoned object cleanup.

### Suite 4: Concurrency and Idempotency

Validates:

- concurrent PUT;
- PUT versus DELETE;
- concurrent reads;
- stale conditions;
- lock expiry;
- same write ID with identical content;
- same write ID with different content;
- missing stable write ID returns Azure-compatible 400;
- `x-ms-client-request-id` fallback and `x-overmesh-write-id` precedence;
- duplicate request delivery.

Concurrent histories MUST be checked against the consistency behavior declared
by the Reference Model.

### Suite 5: Client Compatibility

Validates:

- Azure SDK for .NET;
- Azure SDK for Python;
- Azure SDK for JavaScript;
- Azure CLI;
- AzCopy.

### Suite 6: Ring and Migration

Validates:

- Ring signature;
- schema;
- placement determinism;
- region separation;
- active and parent Ring behavior;
- migration;
- rollback protection;
- key rotation;
- root Ring semantics and parent version/hash continuity;
- signing-time validity boundaries;
- temporary gateway Ring disagreement.

### Suite 7: Manifest and Content Integrity

Validates:

- signed commit manifests;
- signed block manifests;
- block hashes;
- complete-content hashes;
- canonical serialization;
- cross-blob substitution;
- replay protection;
- immutable high-water history plus O(1) fixed-checkpoint publication and
  validation;
- signed W=2 history-compaction floors, exact-byte partial publication repair,
  and rejection of replay below the floor without a history scan;
- rejection of a byte-identical older signed head on both replicas;
- canonical authorization-path and placement-key equivalence;
- wrong-key and wrong-domain signatures.

### Suite 8: Delete and Garbage Collection

Validates:

- signed tombstones;
- W=2 tombstone commit;
- ambiguous delete retry;
- tombstone replay protection;
- anti-resurrection;
- retention enforcement;
- safe immutable-content collection.
- exact-path nonexistent-snapshot `DELETE` authorization with live `404`/`403`
  discrimination after the write probe;
- `404` reads after deletion while physical content remains during retention;
- signed garbage-collection markers on both replicas;
- signed compaction checkpoints before covered-history deletion;
- checkpoint-anchored first-successor validation, crash retry, and conflict
  fail-closed behavior;
- bounded repeated compaction with the current generation preserved;
- preservation of tombstone heads and current high-water evidence after
  collection;
- recreation as a strictly newer generation after deletion.

### Suite 9: Repair and Quarantine

Validates:

- `HEALTHY`;
- `DRIFTED`;
- `MISSING`;
- `TAMPERED`;
- `QUARANTINED`;
- safe source selection;
- conditional repair;
- audit and alert generation;
- administrator-authorized recovery.

### Suite 10: Live Azure Conformance

Validates:

- two real Azure regions;
- `AllowSharedKeyAccess = false`;
- real Microsoft Entra tokens;
- real Azure RBAC;
- allowed and denied authorization canary identities;
- conditional immutable `Put Blob` checks that require `201` for the first
  allowed upload, the API-version-specific `409` or `412` for its authorized
  idempotent retry, `403` for the denied identity, and `202` cleanup of the
  live-gate canary;
- exact-path absent-blob `HEAD` probes that require `404` for the allowed
  identity and `403` for the denied identity;
- exact-path absent-blob `DELETE` probes that require `404` for the allowed
  identity and `403` for the denied identity before logical DELETE is enabled;
- read, write, delete, list, metadata, and property probes for every supported
  Storage API version;
- exact-path Azure ABAC conditions;
- caller/control token routing;
- inherited-role and replica-role-symmetry posture checks;
- blob versioning, soft delete, and configured retention;
- real Key Vault signing;
- API and error compatibility;
- selected Azure Front Door paths;
- production SDK, CLI, and AzCopy behavior.

## 15. Manifest Integrity Scenarios

### META-001

Modify the logical version without recalculating the signature.

Expected result:

```text
TAMPERED
QUARANTINED
```

### META-002

Modify the Ring version without recalculating the signature.

Expected result:

```text
TAMPERED
QUARANTINED
```

### META-003

Modify the write ID without recalculating the signature.

Expected result:

```text
TAMPERED
QUARANTINED
```

### META-004

Remove the signature.

Expected result:

```text
TAMPERED
QUARANTINED
```

### META-005

Modify the declared complete-content SHA-256 hash.

Expected result:

```text
TAMPERED
QUARANTINED
```

### META-006

Modify blob content while retaining valid, unchanged manifests.

Expected behavior:

```text
HEAD:
Validates the signed declaration and may succeed if both manifests agree.

GET touching the modified block:
Fails before returning bytes from the corrupted block.

Reconciler:
Detects the SHA-256 mismatch and marks the blob TAMPERED and QUARANTINED.
```

### META-007

Modify the content and declared hash without recalculating the signature.

Expected result:

```text
TAMPERED
QUARANTINED
```

### META-008

Write a blob normally through Overmesh.

Expected result:

```text
HEALTHY
```

### META-009

Complete a valid Ring migration from version 12 to version 13.

Expected result after migration completion:

```text
HEALTHY
```

The scenario MUST also validate the defined transitional state before
migration completion.

### META-010

Present a correctly signed Ring version 12 after the minimum trusted version
has advanced to 13.

Expected result:

```text
REFUSED
```

### META-011

Replay a correctly signed older commit manifest over a newer committed head.

Expected result:

```text
TAMPERED
QUARANTINED
```

### META-012

Copy a valid manifest from one logical blob path to another.

Expected result:

```text
TAMPERED
QUARANTINED
```

### META-013

Reorder content blocks while preserving the original block manifest.

Expected result:

```text
TAMPERED
QUARANTINED
```

### META-014

Remove the block manifest.

Expected result:

```text
TAMPERED
QUARANTINED
```

### META-015

Create different but valid signed committed heads on replicas A and B.

Expected result:

```text
DRIFTED or QUARANTINED according to whether a unique authoritative head can be
proven.
No client content is returned until strict validation succeeds.
```

### META-016

Remove a valid tombstone and restore an older committed version.

Expected result:

```text
TAMPERED
QUARANTINED
No resurrection
```

### META-017

Offer a tampered replica as the only available repair source.

Expected result:

```text
Repair refused
Blob remains quarantined
Audit and alert emitted
```

## 16. SDK Compatibility Lab

The compatibility matrix MUST pin:

- SDK language;
- package name;
- package version;
- runtime version;
- requested Azure Storage API version;
- authentication mode;
- supported scenario IDs.
- supported listing parameters and include values;
- ordered W=2 catalog page bounds and absence of `heads/` discovery;
- two catalog object reads per returned blob plus request-level quarantine
  prefix scans, without per-item head, high-water, sidecar, or compaction reads;
- exact signed-head catalog publication, idempotent retry, one-sided recovery,
  backfill, conflict/tamper quarantine, delete, and recreation;
- catalog-derived container enumeration, explicit replica authorization
  asymmetry, system-container exclusion, no account-scoped caller listing, and
  list-only authorization without per-blob read probes;
- delimiter groups crossing backend page boundaries and continuation without
  duplicate or omitted entries for an unchanged catalog;
- decoded block-ID, block-size, block-count, and staging-retention limits;
- continuation-token lifetime and concurrent-write semantics.

Each supported SDK validates:

- upload;
- download;
- delete;
- metadata;
- properties and `HEAD`;
- block upload;
- range download;
- conditional operations;
- listing;
- continuation;
- expected errors.

Where practical, the harness SHOULD perform differential contract tests against
direct Azure Blob Storage and Overmesh, normalizing only documented Overmesh
differences such as logical ETag values.

## 17. AzCopy Compatibility Lab

The AzCopy version MUST be pinned.

Required scenarios:

```text
AZCOPY-001  Simple upload
AZCOPY-002  Simple download
AZCOPY-003  Recursive upload
AZCOPY-004  Sync
AZCOPY-005  Resume job
AZCOPY-006  Large file
AZCOPY-007  Interrupted block upload
AZCOPY-008  Conditional retry after ambiguous outcome
AZCOPY-009  Microsoft Entra authentication
AZCOPY-010  SAS and Shared Key rejection
```

AzCopy logs and job plans MUST be captured when a scenario fails.

Passing AzCopy tests against Azurite does not satisfy the release gate. The
mandatory matrix MUST pass against live Azure-backed Overmesh.

## 18. Azure CLI Compatibility Lab

The Azure CLI version and relevant extensions MUST be pinned.

The suite validates:

- login and bearer-token acquisition;
- upload;
- download;
- delete;
- metadata and properties;
- listing and pagination;
- negative Shared Key and SAS paths.

## 19. System Invariants

The Validation Engine MUST enforce all invariants from
`OVERMESH_V1_SPECIFICATION.md`.

### INVARIANT-001

Every acknowledged write exists as the same committed logical version on both
replicas.

### INVARIANT-002

For a healthy blob:

```text
SHA256(replica A content) = SHA256(replica B content)
```

### INVARIANT-003

For a healthy blob:

```text
Signed committed manifest A = Signed committed manifest B
```

### INVARIANT-004

For a healthy blob:

```text
RingVersion A = RingVersion B
```

### INVARIANT-005

For a healthy blob:

```text
LogicalVersion A = LogicalVersion B
```

### INVARIANT-006

Every value used for a consistency decision is covered by a valid signature.
The model evaluates recorded accepted consistency decisions; the presence of
an invalid object is not itself a violation when that object is refused.

### INVARIANT-007

Every accepted signature is verifiable using an active public key in the
trusted key bundle.
The model records whether each accepted consistency decision used a trusted
key.

### INVARIANT-008

Metadata alteration is detected by `GET`, `HEAD`, or reconciliation. Content
alteration is detected by an affected `GET` or complete reconciliation cycle.

### INVARIANT-009

No `PREPARED`, abandoned, or unsigned object is visible through the client API.
The model evaluates recorded public observations, including observations made
at deterministic commit faults.

### INVARIANT-010

A valid tombstone prevents resurrection of every earlier logical version.

### INVARIANT-011

Backend Azure ETags are never used as cross-replica consistency identifiers.
Replica backend ETags are modeled independently from the logical ETag, and
every consistency decision records which identifier class it used.

### INVARIANT-012

No tampered or quarantined object is used as an automatic repair source.
Every repair attempt records source eligibility and whether the repair was
applied.

## 20. Reports and Artifacts

Every run MUST produce:

- a summary report;
- JUnit-compatible results;
- a structured JSON report;
- the scenario version;
- the fault schedule;
- the random seed;
- component versions;
- environment information;
- logical operation history;
- correlation IDs and write IDs;
- invariant results;
- cleanup status.

On failure, the harness MUST additionally capture:

- gateway logs;
- reconciler logs;
- fault-controller logs;
- client logs;
- sanitized backend object inventories;
- signed manifest copies;
- process exit status;
- relevant traces;
- the exact replay command.

Reports MUST redact bearer tokens and any live credentials.

## 21. CI/CD Pipeline

### 21.1 Pull Request

Runs:

- unit tests;
- Reference Model tests;
- canonicalization and cryptographic vectors;
- protocol contract tests;
- deterministic commit failpoints;
- Ring validation;
- identity validation using the local issuer;
- fast property-based tests.

### 21.2 Main Branch

Runs:

- complete Azurite E2E suite;
- the real-system Validation Engine against the public Gateway and both
  Azurite replicas;
- commit state machine;
- concurrency and idempotency;
- tombstones;
- migration;
- reconciliation;
- manifest integrity;
- SDK smoke matrix;
- Azure CLI smoke tests;
- AzCopy smoke tests.

`test-main` and `test-release` MUST invoke the authoritative
`validate-system` path exactly once per run. Declarative scenarios and
real-system validation are separate gates and MUST NOT call each other
implicitly.

### 21.3 Nightly

Runs:

- seeded chaos profiles;
- process crash and restart combinations;
- long-running reconciliation;
- garbage collection;
- large listing and pagination;
- extended property-based histories;
- selected live Azure smoke tests.

### 21.4 Release Candidate

Runs:

- full deterministic chaos suite;
- full SDK matrix;
- full Azure CLI suite;
- full AzCopy suite;
- all commit failpoints;
- all integrity and replay scenarios;
- all Ring migration scenarios;
- two-region live Azure conformance;
- real Microsoft Entra and Azure RBAC validation;
- all allowed and denied authorization-canary probes;
- system-container RBAC posture validation;
- replica RBAC symmetry validation;
- replay of an older valid head against a newer high-water record;
- real Azure Key Vault signing;
- selected Azure Front Door validation;
- long-running stability tests.

The placement suite MUST include at least three independent Storage Accounts
with RF=2. It MUST prove that deterministic placement exercises every expected
replica pair and that disabling one Storage Account affects writes only for
logical blobs whose selected replica set contains that node. Reads and writes
for blobs placed on the two surviving nodes MUST remain functional.

The `0.9.0` live gate MUST also provide positive and negative startup fixtures
for actual Azure regions: three distinct matching Storage Account locations,
a signed-region mismatch, and two Ring nodes resolving to the same Azure
region. The latter two cases MUST prevent Gateway readiness.

### 21.5 Live Performance Baselines

Milestone `0.10.0` MUST execute equivalent operations directly against each
Azure Storage Account and through Overmesh from the same private benchmark
host. Runs MUST randomize direct and federated execution order and keep client,
token, API version, payload, network path, and concurrency settings equivalent.

The matrix MUST include representative `PUT`, `HEAD`, full `GET`, range `GET`,
`DELETE`, listing, and block API operations after those APIs are available.
It MUST cover multiple payload sizes and concurrency levels and report at least
p50, p95, and p99 latency, throughput, backend request counts, Key Vault signing
latency, process CPU, peak memory, and transferred bytes.

Every result set MUST be stored as a signed machine-readable artifact containing
the source commit, project version, Ring version and hash, deployment identity,
Storage API version, client versions, benchmark topology, warm-up policy,
sample count, and UTC interval. Results MUST be retained in a versioned
historical series so later releases can be compared with the original baseline.

Milestone `0.11.0` MUST use the unchanged `0.10.0` matrix to measure every
optimization. Optimizations MUST NOT weaken consistency, authorization,
signature validation, replay protection, or destructive-operation safeguards.
Stable measurements SHOULD become explicit performance regression budgets.

## 22. V1 Release Criteria

The V1 release MUST be rejected if:

- the canonical version, module versions, and active roadmap milestone differ;
- any system invariant fails;
- any mandatory deterministic scenario fails;
- any mandatory scenario is skipped;
- Ring signature validation fails;
- rollback protection fails;
- commit or block-manifest validation fails;
- content hash validation fails;
- tombstone anti-resurrection fails;
- a `PREPARED` object becomes client-visible;
- a tampered source is used for repair;
- replica convergence fails after an approved repair case;
- a required SDK scenario fails;
- an Azure CLI scenario fails;
- an AzCopy scenario fails;
- live Azure Entra or RBAC validation fails;
- a caller credential reaches a control operation;
- a control credential reaches a caller-authorized data operation;
- an unauthorized probe is classified as authorized;
- the canonical authorization resource differs from the placement or manifest
  resource;
- an older signed head is accepted below a valid high-water record;
- system-container RBAC posture or replica RBAC symmetry validation fails;
- the release has only been validated against Azurite.

Stochastic tests MUST use recorded seeds. Any invariant violation discovered by
a stochastic test is release-blocking and MUST be converted into a permanent
deterministic regression scenario.

## 23. Development Workflow

Every product change begins with:

1. a scenario identifier;
2. an expected Reference Model outcome;
3. one or more invariant assertions;
4. a failing harness execution;
5. the implementation;
6. a passing harness execution.

Unsupported Blob operations MUST have explicit rejection scenarios.

Changing expected behavior requires:

- a product specification change;
- a harness scenario change;
- review of compatibility impact;
- review of migration impact.

The harness contract MUST NOT be weakened solely to make an implementation
pass.

## 24. Final Principle

```text
The harness is part of the product.

The contract is more important than the implementation.

Every regression must be detected before release packaging.

The harness is the executable source of truth for expected Overmesh behavior.
```
