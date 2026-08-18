# OVERMESH V1

> No Region Left Behind.  
> No Storage Keys Required.

## 1. Status

This document is the normative architecture specification for Overmesh V1.

Overmesh V1 uses strict dual-region replication, signed placement and commit
metadata, Microsoft Entra ID authentication, and Azure Blob Storage backends.

The terms **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are normative.

## 2. Vision

Overmesh is a storage federation layer placed in front of multiple Azure
Storage Accounts.

It provides:

- an Azure Blob Storage-compatible surface;
- synchronous multi-region replication;
- a Microsoft Entra-native security model;
- no dependency on Azure GRS mechanisms;
- no dependency on Storage Account keys;
- no dependency on shared access signatures;
- deterministic data placement;
- a simple, strict, and verifiable consistency model.

Overmesh is a storage data plane, not a business application.

## 3. Fundamental Principles

### 3.1 Microsoft Entra Only

Authentication and authorization rely exclusively on:

- Microsoft Entra ID;
- OAuth 2.0 bearer tokens;
- Azure RBAC;
- managed identities for Overmesh internal components.

The following mechanisms are prohibited:

- Shared Key authorization;
- Storage Account keys;
- connection strings containing credentials;
- service SAS;
- account SAS;
- user delegation SAS;
- passwords;
- proprietary secrets.

Every backend Storage Account MUST be configured with:

```text
AllowSharedKeyAccess = false
```

Backend Storage Accounts MUST use private endpoints and MUST reject direct
public network access.

Client bearer tokens MUST target the Azure Storage resource:

```text
https://storage.azure.com/
```

The gateway MUST preserve the caller identity for customer-data authorization
and access. The caller therefore requires equivalent Azure RBAC data-plane
permissions on the corresponding logical container in both replicas.

Caller credentials MUST NOT be used for Overmesh control objects. The gateway
and reconciler MUST use separate dedicated managed identities for the reserved
Overmesh system container.

The implementation MUST distinguish caller and control credentials with
non-interchangeable types and operation-specific backend interfaces. A control
credential MUST NOT satisfy an API that requires a caller credential.

Backend accounts MUST be dedicated to Overmesh or otherwise enforce equivalent
isolation. Deployments MUST continuously detect inherited Azure data-plane
roles that grant unapproved principals access to the reserved system container.
The Reconciler Audit Engine owns this check. It MUST evaluate inherited
subscription, resource-group, and account-scope role assignments, custom-role
`DataActions`, assignment conditions, and replica symmetry through Azure ARM.
The first successful audit is a readiness prerequisite. An unavailable audit
or an unsafe result MUST fail closed, stop reconciliation work, and keep the
deployment not ready until the posture is safe again.
Subscription administrators remain part of the trusted infrastructure
boundary because they can alter RBAC, networking, and the Storage Accounts.

### 3.2 Stateless Gateways

Overmesh gateways MUST NOT persist durable business or consistency state on
local disk.

Gateway decisions are derived from:

- the signed Ring loaded in memory;
- the logical account, container, and blob names;
- signed manifests stored on the backend replicas;
- local runtime configuration.

Any gateway instance can be deleted or recreated without functional data loss.

Durable consistency state is stored as signed Overmesh system objects in the
backend Storage Accounts. This does not make the gateway stateful.

Beginning with milestone `0.9.0`, Gateway and Reconciler startup/readiness MUST
validate every Ring node against Azure Resource Manager. The actual Storage
Account location MUST match the signed `region` value, and all Storage Accounts
in one active Ring MUST occupy distinct Azure regions. A missing resource ID,
an unavailable or unauthorized ARM check, a region mismatch, or duplicate
actual regions MUST fail startup.

### 3.3 Fail Closed

The gateway and reconciler MUST reject operations when they cannot validate the
Ring, a required signature, a required manifest, or the consistency state.

They MUST NOT silently downgrade security or consistency guarantees.

## 4. V1 Components

V1 contains only:

- `overmesh-gateway`;
- `overmesh-reconciler`;
- `overmesh-ring-builder`.

### 4.1 Logical Architecture

```text
Azure Front Door
        |
        v
+-------------------+
| Overmesh Gateway  |
+-------------------+
        |
   +----+----+
   |         |
   v         v
Storage A  Storage B
```

Azure Front Door caching and content transformation MUST be disabled for Blob
data-plane routes.

### 4.2 Gateway Modules

The gateway is a single Rust binary containing:

- HTTP Layer;
- Authenticator;
- Router;
- Replica Manager;
- Metadata Engine;
- Manifest Engine;
- Conditional Request Engine;
- Ring Loader.

These modules are not independent microservices.

### 4.3 Reconciler Modules

The reconciler contains:

- Repair Engine;
- Consistency Validator;
- Drift Detector;
- Migration Engine;
- Audit Engine;
- Quarantine Engine.

The reconciler MUST NOT process client traffic.

## 5. Consistency Model

V1 does not implement Dynamo, Cassandra, or Riak quorum protocols.

It does not implement a general distributed quorum. It implements a fixed
two-replica commit protocol.

### 5.1 Fixed Parameters

```text
Replication Factor:              RF = 2
Write Requirement:               W  = 2
Replica Validation Requirement:  R  = 2
```

`R = 2` means both replica manifests are validated when an operation requires a
strict consistency decision. Blob content is read from one validated replica
and may fall back to the other validated replica.

### 5.2 Acknowledged Write Guarantee

A write succeeds only after:

1. the immutable content version exists on both replicas;
2. the block manifest exists on both replicas;
3. the signed commit manifest has been published on both replicas;
4. both replicas expose the same logical version and logical ETag.

```text
PUT
 |
 +---- Replica A: committed
 |
 +---- Replica B: committed

Result: SUCCESS
```

If either replica cannot complete the protocol, Overmesh MUST return failure.

```text
PUT
 |
 +---- Replica A: committed
 |
 +---- Replica B: failed

Result: FAILURE
```

The fundamental acknowledged-write invariant is:

```text
If Overmesh returns SUCCESS,
the committed blob version and its signed manifests exist on both replicas.
```

A failed or timed-out request MAY have committed. Clients MUST use the
Overmesh write ID for idempotent retries. Overmesh MUST never claim that a
failure proves the write was rolled back.

For every `PUT` and `DELETE`, the client MUST send a stable request ID.
`x-overmesh-write-id` takes precedence; `x-ms-client-request-id` is the
compatibility fallback. If neither is present, the gateway MUST return an
Azure-compatible `400 MissingRequiredHeader` and MUST NOT generate a random
client write ID. The accepted value is 1-128 ASCII characters from
`A-Z`, `a-z`, `0-9`, `-`, `.`, `_`, and `~`.

### 5.3 Read Strategy

For `GET` and `HEAD`, Overmesh MUST:

1. load and validate the signed commit manifests from both replicas;
2. require a consistent committed logical head;
3. reject or quarantine invalid manifests;
4. read content from the deterministic primary replica;
5. fall back to the secondary only after validating that it contains the same
   committed version.

Foreground reads do not vote between arbitrary values and do not perform read
repair.

If strict validation cannot be completed, the operation fails closed. Serving
potentially stale or uncommitted data is not an availability fallback.

### 5.4 Unsupported Distributed-System Features

V1 explicitly excludes:

- RF=3 or greater;
- configurable quorum reads;
- configurable quorum writes;
- read repair;
- hinted handoff;
- vector clocks;
- generic conflict resolution;
- active-active multi-master writes;
- Merkle Trees.

## 6. Ring

The Ring is the source of truth for placement.

The Ring is not a service. It is a signed document loaded into memory.

### 6.1 Example

```yaml
apiVersion: overmesh.io/v1
ringVersion: 12
root: false
parentRingVersion: 11
parentRingHash: sha256:yyyyyyyy
replicationFactor: 2
createdAt: 2026-08-15T10:00:00Z
signedAtUnixMs: 1786788000000
signingKeyId: ring-key-2026-01
ringHash: sha256:xxxxxxxx
nodes:
  - id: stfrance01
    region: francecentral
    weight: 100
  - id: stsweden01
    region: swedencentral
    weight: 100
```

### 6.2 Placement

Placement uses consistent hashing.

The canonical placement key is constructed once from the UTF-8 encoded logical
resource path:

```text
/{logical-account}/{container}/{blob}
```

The Ring specification MUST define:

- the exact hash algorithm;
- canonical path escaping and normalization;
- virtual-node construction;
- weight interpretation;
- deterministic tie-breaking;
- replica ordering;
- topology constraints.

Authorization probes, placement, physical-data references, manifests, locks,
audit records, and reconciliation MUST consume the same typed canonical
logical resource. Independently reconstructed path strings are prohibited.

Replica A and Replica B MUST belong to different Azure regions.

### 6.3 Ring Changes

The gateway MUST retain the active Ring and its declared parent Ring during a
migration.

The initial Ring is explicitly `root: true`, has `ringVersion: 1`, and has
null `parentRingVersion` and `parentRingHash`. Every later Ring is
`root: false` and MUST bind both the exact version and SHA-256 hash of a
trusted predecessor supplied outside the candidate Ring. The current Ring's
declared hash, signature, parent version, parent hash, and rollback floor MUST
all validate before activation.

The reconciler moves committed versions to their new placement before the old
placement is retired. Reads during migration MUST use deterministic active and
parent Ring lookup rules.

## 7. Cryptographic Model

### 7.1 Algorithms

Azure Key Vault does not support Ed25519 signing keys. Overmesh V1 therefore
uses:

```text
ECDSA P-256 with SHA-256
JOSE algorithm identifier: ES256
```

Private keys MUST be non-exportable Azure Key Vault or Managed HSM keys.

### 7.2 Key Separation

At least two independent signing authorities are required:

- Ring signing key;
- blob manifest and commit signing key.

Every signed object MUST contain a `signing_key_id`.

Gateways and reconcilers embed or securely load a trust bundle containing the
accepted public keys and their validity periods. Key rotation MUST support an
overlap period without accepting retired keys indefinitely.

Every signed Ring and manifest envelope MUST bind `signedAtUnixMs` in the
signed bytes. Verification checks that timestamp inclusively against the
selected key's explicit `notBeforeUnixMs` and `notAfterUnixMs`. It MUST NOT
reject a historical object merely because the reader's current wall clock is
after the key's validity period. Unknown keys, keys removed from the trust
bundle, and signatures whose bound time falls outside the selected key window
MUST be rejected. Overlapping windows permit safe rotation.

### 7.3 Canonicalization

All signed documents MUST use a single documented canonical serialization.

The signature specification MUST define:

- field ordering;
- UTF-8 normalization;
- integer and timestamp representation;
- omitted and null field handling;
- binary hash encoding;
- signature encoding;
- domain-separation prefix.

Ring signatures and blob signatures MUST use different domain-separation
prefixes.

## 8. Ring Signature and Rollback Protection

Example files:

```text
ring-v12.yaml
ring-v12.sig
```

At startup, a gateway MUST:

1. download the Ring;
2. verify its hash;
3. verify its signature;
4. verify the bound signing time against the exact trusted key window;
5. validate its schema and root/non-root invariants;
6. verify `parentRingVersion` and `parentRingHash` against the configured
   trusted predecessor;
7. validate its version against the minimum trusted Ring version;
8. load it into memory.

The gateway MUST refuse to start if any step fails.

A signature alone does not prevent replay. The deployment MUST provide a
durable minimum accepted Ring version or hash outside the downloaded Ring,
such as a protected deployment configuration value.

Example:

```text
Minimum trusted version: 12
Received signed version:  10
Result: REFUSED
```

## 9. Overmesh Storage Layout

Logical blobs are represented by immutable content versions and signed system
manifests.

Backends MUST contain a reserved Overmesh system container that is not exposed
through the client Blob API. It contains only Overmesh control objects and is
accessible only to approved gateway and reconciler managed identities.

Immutable customer content versions MUST be stored in the corresponding
customer container. Their writes use the caller bearer token so Azure Storage
enforces the caller's `DataActions` independently on both replicas.

A logical blob consists of:

- one immutable content version per replica;
- one signed block manifest per replica;
- one signed commit manifest per replica;
- an optional signed tombstone;
- an auditable write ID.

The customer-content namespace used for immutable versions MUST be
deterministic, reserved from the client-visible namespace, and covered by the
same Azure RBAC assignment as the logical blob.

Internal manifest/version names MUST be deterministic from the logical path and
write ID while preventing collisions and path ambiguity. The physical content
reservation within that signed version MUST be an unpredictable server-side
identifier; callers MUST NOT choose physical object names.

Uncommitted internal objects MUST never be returned by client reads or normal
list operations.

## 10. Signed Metadata and Manifests

Azure Blob metadata is not trusted because an identity with sufficient backend
permissions could modify it directly.

Every consistency-relevant Overmesh value MUST therefore be covered by a
signature.

The signature covers an envelope containing the payload and
`signedAtUnixMs`. This envelope time is the authority for signing-key validity
checks. A signed parent object time MAY be used for an unsigned child only when
the parent cryptographically binds the child's exact hash.

### 10.1 Required Logical Metadata

The signed commit manifest includes at least:

- `blob`;
- `write_id`;
- `logical_version`;
- `logical_etag`;
- `ring_version`;
- `content_length`;
- `content_sha256`;
- `block_manifest_sha256`;
- `committed_at`;
- `state`;
- `signing_key_id`.

Example:

```json
{
  "blob": "/images/photo.jpg",
  "write_id": "2f4d242f-0000-4000-8000-000000000000",
  "logical_version": 42,
  "logical_etag": "\"om-v42-a1b2c3d4\"",
  "ring_version": 12,
  "content_length": 7340032,
  "content_sha256": "sha256:...",
  "block_manifest_sha256": "sha256:...",
  "committed_at": "2026-08-15T10:00:00Z",
  "state": "COMMITTED",
  "signing_key_id": "blob-key-2026-01"
}
```

### 10.2 Block Manifest

The signed block-manifest root contains:

- blob identity;
- write ID;
- logical version;
- total content length;
- total block count;
- block-manifest page size;
- ordered page identities, byte ranges, object paths, and SHA-256 hashes;
- SHA-256 hash of the complete content;
- Ring version;
- signing key ID.

Each immutable JCS block-manifest page contains a bounded contiguous set of
ordered block boundaries and SHA-256 hashes. A page is authenticated by its
SHA-256 reference in the signed root; it does not require an additional Key
Vault signature.

The signed root MUST remain small relative to the blob. A range read MUST load
only the pages intersecting the requested range. A complete read and the
Reconciler MUST process pages incrementally with bounded memory.

The signed commit manifest additionally contains:

- the canonical logical account, container, and blob identity;
- the caller tenant ID;
- the caller object ID;
- the caller subject;
- the caller application or authorized-party ID when present;
- the physical customer container and immutable content object;
- the authorization operation and API version used.

Each block MUST be fully validated before its bytes are returned to the client.
A range request MUST validate every complete block intersecting the requested
range.

### 10.3 Logical ETags

Backend Azure ETags are replica-specific and MUST NOT be exposed as the logical
Overmesh ETag.

Overmesh generates a deterministic logical ETag from the committed manifest.
Conditional requests MUST be evaluated against the logical ETag.

### 10.4 Replay Protection

A valid signature does not by itself prevent replay of an older valid version.

Overmesh MUST compare:

- logical version;
- predecessor or prior logical ETag;
- Ring version;
- current committed head;
- tombstone state.

An older valid object MUST NOT replace a newer committed head.

Each committed logical version MUST also produce an immutable signed
high-water history object plus a fixed-name signed current checkpoint. The
checkpoint MAY reuse the exact signed committed manifest bytes. Gateways and
reconcilers MUST read the fixed checkpoint in O(1) backend operations and MUST
reject a head older than the valid checkpoint visible on either replica.
Success MUST NOT be returned until the history object and current checkpoint
are durable on both replicas. Publication and one-sided checkpoint repair MUST
be safe to complete idempotently after an ambiguous outcome.

The immutable history MAY be compacted only by the Reconciler after physical
collection has completed. A fixed-name, signed W=2 history-compaction
checkpoint anchors the removed prefix. Gateways read this checkpoint in O(1)
operations and reject any head or current high-water record at or below its
floor, or below the GC history head bound into the checkpoint. They never scan
history on a request path.

### 10.5 Authorization Probes

Operations whose logical effect differs from their physical Azure operation
MUST use an operation-specific Azure Storage authorization probe with the
caller token. Every separate probe MUST be side-effect-free.

Logical `PUT Blob` MUST NOT stage a synthetic authorization block. The actual
conditional upload of the unpredictable immutable content object is the write
authorization decision on each replica. A first upload requires `201 Created`;
an idempotent retry may accept Azure's `409 BlobAlreadyExists` or
`412 Precondition Failed` only after the immutable object is confirmed present
with the signed length. The conditional retry targets that existing immutable
object and cannot change its bytes. A denied caller returns `401` or `403`, which Overmesh
maps to `AuthorizationPermissionMismatch` without retryable server-error
semantics.

- `HEAD` uses the blob read `DataAction`;
- `DELETE` uses the blob delete `DataAction`;
- `LIST` uses the blob list suboperation;
- metadata and property changes use their corresponding write operation.

The probe MUST target the exact canonical logical resource and exercise the
same Azure `DataAction` as the logical operation. A generic write probe MUST
NOT authorize a delete or list operation.

Probe behavior is fail-closed. Only explicitly documented authorized terminal
statuses are accepted. Live Azure conformance MUST verify every probe with an
allowed principal and a deliberately denied principal. This check MUST run for
every supported Storage API version and periodically in deployed environments.

## 11. Write and Commit Protocol

### 11.1 Per-Blob Serialization

Only one write transaction may update a logical blob head at a time.

The gateway MUST acquire a deterministic per-blob lock or lease and MUST apply
conditional updates against the previous logical ETag. Lock acquisition alone
does not replace conditional checks.

### 11.2 Commit States

V1 defines:

```text
PREPARED
COMMITTED
TOMBSTONED
```

Only `COMMITTED` content is visible to `GET`, `HEAD`, and listing operations.
`TOMBSTONED` content is logically absent.

### 11.3 Write Sequence

For a write, the gateway MUST:

1. authenticate and authorize the caller;
2. construct one canonical typed logical resource;
3. resolve both replicas from the active Ring;
4. acquire per-blob serialization using the gateway managed identity;
5. read and validate control state using the gateway managed identity;
6. evaluate all client preconditions against the logical ETag;
7. authorize the exact customer-data write on both replicas;
8. validate the caller-supplied stable write ID and allocate the next logical
   version plus an unpredictable server-side physical reservation;
9. stream immutable customer content to both replicas with the caller token;
10. calculate block and complete-content SHA-256 hashes;
11. publish signed `PREPARED` manifests with the gateway managed identity;
12. verify both prepared replicas;
13. generate the signed `COMMITTED` manifest with caller attribution;
14. conditionally publish the committed head to both replicas;
15. publish the immutable high-water history object and conditionally replace
    the fixed current checkpoint on both replicas;
16. verify that both replicas expose the same committed head and current
    checkpoint;
17. return success.

The gateway MUST NOT overwrite the previous immutable content version during
this sequence.

### 11.4 Idempotency

The write ID is the idempotency key for retries.

A retry with the same write ID and identical payload MUST return the existing
outcome. A retry with the same write ID and a different payload MUST fail.
The gateway MUST preserve caller data-plane credentials for physical content
operations and separate control credentials for locks, manifests, heads,
high-water records, and quarantine state.

### 11.5 Partial Failure

Partial writes remain invisible unless a valid committed head references them.

The reconciler may remove abandoned `PREPARED` objects after a configured
retention period.

If a signed committed manifest exists on only one replica, the operation is not
healthy. The reconciler may repair it only after validating that:

- the commit signature is valid;
- the referenced immutable content exists and hashes correctly;
- the commit attests that both prepared replicas were completed;
- no newer committed head or tombstone exists.

## 12. Delete Protocol

Deletion is represented by a signed tombstone, not by immediate destructive
removal.

A tombstone includes:

- blob identity;
- write ID;
- logical version;
- previous logical ETag;
- Ring version;
- deletion timestamp;
- signing key ID;
- state `TOMBSTONED`.

The tombstone uses its own immutable version namespace and MUST be committed to
both replicas using the same W=2 protocol:

1. acquire the same per-blob lease used by `PUT`;
2. validate quarantine, both heads, and both current high-water checkpoints;
3. execute an exact-path blob-delete authorization probe on both replicas with
   the caller token;
4. publish and verify the signed prepared tombstone on both replicas;
5. publish the signed committed tombstone sidecar on both replicas;
6. conditionally replace both heads;
7. publish immutable high-water history and replace both current checkpoints;
8. verify identical tombstone heads and checkpoints before returning `202`.

The authorization probe MUST use `DELETE` against a syntactically valid but
nonexistent snapshot at the exact logical path. It MUST NOT target the current
blob, committed content, or uncommitted blocks and therefore cannot remove a
squatted backend object. The supported Azure Storage API versions MUST be
live-tested against the exact write-then-read-then-delete sequence to prove
that an allowed principal receives `404`, while a denied principal receives
`403`.

Retrying with the same write ID MUST return the existing tombstone. Reusing the
write ID for another logical operation MUST fail. Deleting an already
tombstoned or never-created blob returns `404` after authorization has been
validated. A later `PUT` may create a new logical generation whose version is
strictly greater than the tombstone; `If-None-Match: *` treats the tombstoned
blob as absent.

Physical content collection is asynchronous and MUST respect a configurable
retention period. The reconciler MUST NOT resurrect content older than a valid
tombstone. A committed generation becomes eligible only after a successor
authoritative signed history entry supersedes it and the delay measured from
that successor's `committedAtUnixMs` has elapsed. The active head generation
is never collectible.

Before any physical delete, the Reconciler MUST validate identical retained
history on both replicas, including signatures, blob and Ring binding, unique
contiguous versions, state transitions, `previousLogicalEtag` lineage,
monotonic timestamps, exact current head and high-water correspondence, and
every candidate content and version-metadata namespace. A compacted prefix is
accepted only when a valid W=2 compaction checkpoint anchors it. The first
retained successor MUST be exactly one logical version above the floor and
MUST link to the checkpoint logical ETag. Rollback, gaps above the floor,
broken first-successor links, conflicting terminal history, and replay below
the floor fail closed.

Incremental physical collection is recorded by chained signed immutable
garbage-collection watermarks on both replicas. A valid one-sided watermark
publication is repaired by copying its exact bytes; invalid or conflicting
watermarks fail closed. Superseded tombstone sidecars are collectible as
version metadata, but the active head, active sidecar, current high-water
checkpoint, and active high-water history entry are never collectible.

After a GC watermark is identical on both replicas, the Reconciler MAY publish
`high-water/{pathHash}/compaction/current.json`. The signed checkpoint MUST
bind:

- canonical blob identity, path hash, head object, and Ring version;
- checkpoint sequence version;
- compacted-through logical version, state, logical ETag, and commit time;
- SHA-256 of the covered terminal signed manifest;
- previous checkpoint SHA-256 and sequence version, when one exists;
- GC marker object, SHA-256, collected-through and history-head versions,
  collected committed-version set, retention delay, and collection time;
- compaction timestamp and signing key ID.

Checkpoint replacement is conditional. A one-sided newer checkpoint may be
recovered only when it is the signed direct descendant of the older checkpoint;
the exact signed bytes are copied to the lagging replica. Missing, invalid,
non-canonical, or conflicting checkpoints fail closed. Covered history is
deleted idempotently with validated backend ETags only after identical valid
checkpoint bytes are re-read from both replicas. A crash after checkpoint
publication therefore leaves a safe retryable cleanup, never an unanchored
history gap.

Only the latest authoritative fixed-name compaction checkpoint is retained.
After it is durable, older GC markers may be pruned while retaining the marker
bound by the checkpoint. Compaction work per cycle is configured and bounded,
but size alone never makes a version eligible: every removed version MUST
already be covered by durable W=2 GC evidence. This applies to live overwrite,
tombstone, and delete/recreate chains.

### 12.1 Backend Data Protection

Customer-data containers MUST enable blob versioning and blob soft delete with
a retention period no shorter than the Overmesh physical-collection delay.
Version soft delete MUST protect prior versions from immediate permanent
deletion.

Deployments SHOULD apply time-based version-level immutability to immutable
Overmesh content versions when the configured recovery and garbage-collection
workflow can operate without overwriting a protected version.

These Azure protections are defense in depth and MUST NOT replace Overmesh
signatures, W=2 commit validation, private endpoints, or reconciliation.

## 13. Validation Semantics

### 13.1 HEAD

`HEAD` validates:

- both signed commit manifests;
- logical version and ETag agreement;
- Ring version;
- declared content length and complete-content hash.

`HEAD` does not read the blob body and therefore cannot independently
recalculate the content SHA-256 hash. It validates the signed declaration of
that hash. It MUST NOT download the block-manifest root or its pages.

### 13.2 GET

`GET` performs all `HEAD` validations, validates the signed block-manifest
root, loads only the pages required by the requested byte range, and validates
content blocks before returning them.

Overmesh MUST NOT return bytes from a block before that block's SHA-256 hash
has been verified.

### 13.3 Reconciler

The reconciler performs complete content and manifest validation on both
replicas. It is responsible for detecting latent content corruption that has
not been observed by a client read. Complete-content and per-block hashing MUST
stream with bounded memory.

Normal cycles MUST use bounded incremental head discovery with a persisted
operational checkpoint and MUST NOT unconditionally process all heads. An
explicit full-scan audit mode MUST remain available. Each discovered logical
blob is reconciled only on the RF=2 replicas selected by the active Ring,
including when the Ring contains more than two nodes.

Discovery checkpoints MUST be signed, Ring-version-bound control-plane
objects replicated across the Ring backends. They MUST NOT depend on a local
gateway or job filesystem. Cursor publication MUST use backend preconditions,
must fail closed on invalid signatures or malformed state, and may replay work
after an interrupted or concurrent publication but MUST NOT silently skip
unprocessed pages.

The reconciler identity is the most privileged runtime data identity. It
requires read and repair access to every customer container and controlled
delete access for garbage collection. It MUST be distinct from the gateway
identity, MUST NOT have role-assignment management permission, and MUST NOT
have permanent-delete or immutability-superuser permission.

### 13.4 Logical Listing and Continuation

`List Blobs` uses a logical W=2 catalog in the isolated control namespace.
Each mutable catalog object contains the exact canonical bytes of the current
signed `COMMITTED` or `TOMBSTONED` head. Catalog object keys encode canonical
container and blob UTF-8 bytes with an order-preserving path-safe encoding, so
backend lexical order is logical container/blob order. Listing MUST read only
the bounded catalog pages required to produce `maxresults` and continuation;
it MUST NOT discover listing truth by scanning `heads/`, history, customer
data paths, or staged namespaces.

Before exposure, the selected catalog bytes MUST be identical on the active
RF=2 replicas and MUST validate as a canonical signed commit manifest. The
catalog key, signed blob/container path, logical version and ETag, state,
Ring version and selected replicas MUST agree. The same exact bytes MUST be
present on both selected replicas. Before processing entries, listing MUST
load the union of quarantine keys from every configured backend and MUST skip
every matching path hash. Per-item current-head, high-water, committed-sidecar,
and compaction reads are prohibited on the listing hot path; complete freshness
and anti-replay validation remains normative for HEAD, GET, and Reconciler
processing. Signature failure, key/path mismatch, Ring mismatch, quarantine,
one-sided catalog publication, and non-`COMMITTED` state are skipped.

Successful PUT, DELETE, recreation, Put Block List commit, idempotent retry,
and partial-publication recovery MUST conditionally publish and verify the
same exact catalog bytes on both selected replicas while holding the per-blob
lease. A valid one-sided entry is repaired by copying exact bytes; tampered,
conflicting, same-version-different, or newer catalog state fails closed.

`List Containers` derives candidates only from bounded pages of the signed
logical catalog, then performs a caller-authorized container listing probe on
both selected replicas. This avoids requiring account-scoped caller RBAC, which
would expose `overmesh-system`. Containers without any current visible catalog
entry are therefore outside the published `0.8.0` subset. `overmesh-system`,
one-sided authorization, invalid catalog entries, and unauthorized containers
are excluded explicitly. A failure
or authorization denial on either replica fails the request closed. Physical
blob paths are never used as listing truth.

Listing MUST exclude system-container objects, `.overmesh/*`, staged blocks,
`PREPARED` and `TOMBSTONED` state, incomplete publications, compacted/replayed
heads, and any missing, drifted, tampered, or quarantined candidate.

After a successful Azure container-list authorization, blob enumeration MUST
NOT issue per-blob caller read probes unless the exact Azure DataAction
requires one. Internal signature, head, high-water, compaction, sidecar, and
quarantine validation uses typed control operations under the gateway
identity and MUST NOT silently require a stronger caller role than direct
Azure `List Blobs`.

The 0.8 published subset supports:

- case-sensitive canonical names and Azure path percent decoding;
- `prefix`;
- an empty or one-character `delimiter`, with deduplicated `BlobPrefix`;
- `maxresults` from 1 through 5000;
- `include=metadata`, exposing the signed Overmesh SHA-256 as metadata;
- opaque signed `marker` values.

All other `include` values fail explicitly. Results and prefixes use
deterministic logical-name ordering. A continuation token binds the logical
account, optional container, operation scope, prefix, delimiter, normalized
include set, requested page size, Ring version and hash, last catalog ordering key,
issue and expiry times, signing key ID, token version, and signature domain.
Verification checks canonical encoding, signature/key validity at issue time,
expiry, complete request equality, ordering-key validity, and active Ring
binding. Rotation may retain overlapping verification keys.

For an unchanged catalog, continuation has no duplicates or omissions across
gateway restarts. Delimiter pagination consumes the complete contiguous
catalog range represented by a returned `BlobPrefix` before issuing its
marker, including when that range crosses backend page boundaries. Listing is
not a frozen snapshot during concurrent writes: inserts after the last
consumed catalog key may appear later, inserts before it require a new
enumeration, updates at or before it are not repeated, and concurrent deletes
or tombstones disappear.

The reconciler derives catalog truth only from an identical fully validated
W=2 current head. It backfills missing entries, repairs valid one-sided or
older identical entries with conditional writes, and quarantines tampered,
mis-keyed, conflicting, or newer catalog state. Catalog reconciliation occurs
before destructive collection for an already identical W=2 head and after
head/tombstone repair, so history compaction and garbage collection never
become alternate listing sources.

### 13.5 Public Block Operations

`Put Block` decodes canonical standard Base64 IDs, preserves the exact client
text in signed metadata, and uses only hashes plus unpredictable reservations
in physical paths. The 0.8 subset permits 64 decoded ID bytes, 100 MiB per
block, 50,000 blocks per committed list, and requires equal decoded ID lengths
within one `x-overmesh-upload-id` generation. The upload ID defaults to the
implicit hash-bound namespace for the caller, logical blob, and current base
generation, allowing standard clients to use different request IDs for
individual block calls. An explicit `x-overmesh-upload-id` isolates concurrent
application-managed generations.

Each stage binds the canonical blob, upload/write IDs, caller, base logical
version and ETag, block ID/hash/length, content hash/path, Ring version,
replicas, creation/expiry times, and signing key. Success requires immutable
caller-authorized bytes and identical signed metadata on both replicas.
Identical retries are idempotent; conflicting reuse fails. Staged blocks are
never visible to `GET`, `HEAD`, or logical listing.

Before publishing stage metadata, the Gateway uploads the unpredictable staged
content object with the caller token on both selected replicas. This actual
conditional upload is the write authorization decision; no synthetic Put Block
probe is permitted. Idempotent stage retries re-execute and validate that same
immutable upload. The live capability gate pins allowed and denied Put Block
behavior for every supported Storage API version.

`Put Block List` accepts ordered `Latest`, `Committed`, and `Uncommitted`
elements. Under the per-blob lease it validates quarantine, head/high-water
state, compaction floors, stage generation/base state, committed block pages,
both physical replicas, hashes, order, limits, conditions, and caller access.
It assembles selected blocks through bounded disk spooling and then executes
the normal paged-integrity PREPARED/COMMITTED W=2 publication. The same write
ID and ordered IDs return the committed result on retry.

`Get Block List` returns Azure XML for `committed`, `uncommitted`, or `all`.
Committed IDs come from signed committed block pages. Uncommitted IDs come
only from identical signed non-expired stage metadata whose base still equals
the current logical state. Divergence or tampering fails closed.

Overwrite/delete makes older stages stale; stale stages cannot commit or
resurrect a tombstoned generation. New stages may target the newer tombstone
base for an explicit recreate. Expired stages are repaired or collected only
after complete validation, and committed assembled content is outside the
staging namespace.

## 14. Blob Health States

A logical blob has one of the following health states:

### 14.1 HEALTHY

- both replicas expose the same committed head;
- signatures are valid;
- block manifests are identical and valid;
- complete content hashes are valid;
- Ring and logical versions match.

### 14.2 DRIFTED

- both replicas contain valid signed objects;
- committed heads or referenced immutable versions differ;
- neither side has been classified as tampered.

### 14.3 MISSING

- a required replica object is absent;
- a valid committed manifest or tombstone identifies the expected state.

### 14.4 TAMPERED

Any of the following results in `TAMPERED`:

- invalid signature;
- invalid block or complete-content hash;
- missing required signed field;
- inconsistent signed payload;
- illegal logical version regression;
- replay of an older valid object over a newer committed head;
- manifest referencing unexpected content.

### 14.5 QUARANTINED

`QUARANTINED` is an operational state applied to a logical blob after tampering
or an unsafe ambiguity has been detected.

Quarantined data MUST NOT be selected as an automatic repair source.

## 15. Repair Policy

The reconciler may automatically repair:

- `HEALTHY` objects requiring non-semantic maintenance;
- `DRIFTED` objects when a unique authoritative committed head is provable;
- `MISSING` objects when a valid healthy source is available.

The reconciler MUST never use `TAMPERED` or `QUARANTINED` data as a source.

A tampered blob MUST be:

- recorded in the audit log;
- reported through an alert;
- logically quarantined;
- withheld from automatic repair until an administrator validates a healthy
  source or authorizes recovery.

Repair operations MUST be idempotent and conditionally applied so they cannot
overwrite a newer committed version.

## 16. Azure Blob Compatibility Scope

Compatibility is defined by an explicit operation and API-version matrix, not
by protocol resemblance alone.

V1 MUST validate at least:

- Azure SDK for .NET;
- Azure SDK for Python;
- Azure SDK for JavaScript;
- AzCopy;
- Azure CLI.

The V1 compatibility suite MUST cover:

- `Put Blob`;
- `Put Block`;
- `Put Block List`;
- `Get Block List`;
- `Get Blob`;
- `Get Blob Properties`;
- range reads;
- delete;
- list containers and blobs;
- metadata and properties;
- logical ETags;
- `If-Match`;
- `If-None-Match`;
- `If-Modified-Since`;
- `If-Unmodified-Since`;
- pagination and continuation tokens;
- relevant `x-ms-*` request and response headers;
- Azure-compatible error status and error bodies.

Continuation tokens generated by Overmesh MUST be opaque, signed, and bound to
the complete listing request plus Ring version and hash.

Features not listed in the published V1 compatibility matrix MUST be rejected
explicitly rather than approximated silently.

Container creation and deletion are not part of the V1 compatibility matrix
unless a later milestone explicitly adds their W=2 state machine. Unsupported
container lifecycle requests MUST return an Azure-compatible explicit error.

## 17. Azure Front Door Constraints

The implementation MUST account for Azure Front Door request, upload, and
origin timeout limits.

Large uploads SHOULD use Azure Block Blob operations. The gateway MUST support
streaming and MUST NOT require complete in-memory buffering of a blob.

Timeouts can leave the client outcome ambiguous. The write ID and idempotent
retry protocol are therefore mandatory.

## 18. System Invariants

### Invariant 1

Every acknowledged write exists as the same committed logical version on both
replicas.

### Invariant 2

For a healthy blob:

```text
SHA256(replica A content) = SHA256(replica B content)
```

### Invariant 3

For a healthy blob:

```text
Signed manifest A = Signed manifest B
```

### Invariant 4

For a healthy blob:

```text
RingVersion A = RingVersion B
```

### Invariant 5

For a healthy blob:

```text
LogicalVersion A = LogicalVersion B
```

### Invariant 6

Every Overmesh value used for consistency decisions is covered by a valid
signature.

### Invariant 7

Every accepted Overmesh signature is verifiable through an active public key
in the trusted key bundle.

### Invariant 8

Metadata alteration is detected by `GET`, `HEAD`, or reconciliation.

Content alteration is detected by `GET` when the affected blocks are read, or
by a complete reconciliation validation cycle.

### Invariant 9

No `PREPARED`, abandoned, or unsigned object is visible through the client Blob
API.

### Invariant 10

A valid tombstone prevents automatic resurrection of every earlier logical
version.

### Invariant 11

Backend Azure ETags are never used as cross-replica consistency identifiers.

### Invariant 12

No tampered or quarantined object is used as an automatic repair source.

## 19. Development Method

Development follows a strict test-first approach.

No feature is complete without an associated end-to-end scenario.

The test suite MUST include:

- successful dual writes;
- every failure point in the commit state machine;
- gateway termination between commit steps;
- client timeout followed by idempotent retry;
- concurrent conditional writes;
- replica loss;
- Ring migration;
- signed Ring rollback attempts;
- manifest replay attempts;
- content and metadata tampering;
- tombstone and anti-resurrection behavior;
- range validation across block boundaries;
- compatibility tests for every supported client.

## 20. Semantic Versioning

Overmesh follows Semantic Versioning.

- `0.x.y` versions are V1 development milestones without a stable public
  compatibility guarantee.
- `1.0.0` is the first production release implementing the complete V1
  contract.
- `2.0.0` introduces the V2 Merkle Tree architecture.
- `3.0.0` introduces V3 content-addressable metadata.

All modules in a release MUST use the same project version. The canonical
version, workspace module versions, and active roadmap milestone MUST agree.

## 21. Roadmap

### V1 Development Milestones

- `0.3.0`: signed strict dual-write commit protocol;
- `0.4.0`: reconciliation, conditional repair, signed audit, quarantine, and
  administrator-authorized recovery;
- `0.5.0`: compile-time caller/control identity separation, customer-data and
  control-plane isolation, signed caller attribution, O(1) signed replay
  checkpoints, canonical Azure path binding, unpredictable physical content
  reservations, trust-bundle rotation foundations, Reconciler-owned RBAC
  posture readiness, and mandatory live Azure authorization-probe validation;
- `0.6.0`: validated client `HEAD` and `GET`, including ranges;
- `0.7.0`: `DELETE`, signed tombstones, retention, and garbage collection;
- `0.8.0`: validated logical listing, W=2 block staging/commit/inspection,
  signed continuation tokens, staged repair, retention, and garbage collection;
- `0.9.0`: private Azure Container Apps infrastructure for Gateway and
  Reconciler first; three-or-more-Storage-Account placement and single-node
  failure validation; ARM-backed distinct-region startup enforcement; followed
  by Front Door, SDK, Azure CLI, AzCopy, and live Azure conformance;
- `0.10.0`: live performance baselines comparing direct Azure Storage access
  with the same operations through Overmesh, with signed historical results;
- `0.11.0`: evidence-driven performance optimization and regression budgets;
- `1.0.0`: complete stable V1 contract.

### V1

- RF=2, W=2, R=2;
- strict dual-write commit protocol;
- Microsoft Entra-only authentication;
- signed Ring;
- signed metadata;
- signed commit manifests;
- signed block manifests;
- signed tombstones;
- logical ETags;
- conditional writes;
- per-blob write serialization;
- stateless gateways;
- automatic reconciliation.

### V2

- Merkle Trees;
- incremental reconciliation at large scale.

### V3

- content-addressable metadata;
- deduplication;
- advanced content verification.

## 22. Official Slogan

```text
OVERMESH

No Region Left Behind.
No Storage Keys Required.
```
