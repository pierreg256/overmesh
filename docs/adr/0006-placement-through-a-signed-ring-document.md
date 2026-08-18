# ADR-0006 — Placement through a signed Ring document

- **Status:** accepted
- **Date:** 2026-08-17
- **Milestone:** 0.2.0 → 0.6.0
- **Supersedes:** —
- **Superseded by:** —

## Context

Overmesh federates several storage accounts behind one endpoint. Something has
to decide, for every logical blob, which accounts hold it — and every gateway
instance has to reach the same answer, at the same moment, without talking to
any other gateway.

That decision is the foundation of the capacity argument. It is also the point
at which a federation layer can become a liability: a placement authority that
is wrong, unavailable, or forgeable takes the whole system with it.

## Options considered

**A placement service.** A component gateways query for "where does this blob
live". Supports dynamic membership and rebalancing. Introduces a runtime
dependency on the hot path of every request, a component that must be made
highly available, and a new thing to secure — the exact opposite of a stateless
gateway.

**Placement recorded per blob.** Store the chosen replicas in the object's own
metadata at write time. Simple and flexible. But the metadata lives on the
replicas, so you must already know where to look — circular. It also makes
placement unauditable in aggregate: there is no artefact describing the
topology.

**A signed document loaded into memory.** Placement is a pure function of the
logical path and the document. Any gateway derives the same answer offline. The
topology is a reviewable, signable, versionable artefact.

## Decision

Placement comes from a **signed Ring document**, loaded and verified at startup
and held in memory. The Ring is not a service.

### Placement function

Consistent hashing over virtual nodes, with region anti-affinity applied during
selection rather than merely validated afterwards: the circle is walked from
the blob's position and a node is skipped if its region has already been used.
The result is deterministic, derivable offline from the document alone, and
guaranteed to span distinct regions.

Consistent hashing is chosen over a simple modulo so that adding an account
displaces roughly one Nth of keys rather than nearly all of them — the property
the capacity story depends on.

### Trust chain

Four checks, each independent, all required before the gateway will start:

1. **Self-consistency.** The declared `ringHash` must equal the hash computed
   over a JCS-canonical payload that excludes the hash field itself.
2. **Signature.** ES256 over the canonical document, under the domain prefix
   `overmesh:ring:v1\0` so a Ring signature can never be replayed as a manifest
   signature.
3. **Key validity.** The signing key must be in the trust bundle *and*
   `signedAtUnixMs` must fall inside that key's validity window. A retired key
   cannot sign a new Ring, while Rings it signed while valid remain verifiable.
4. **Minimum version.** A floor supplied by deployment configuration, outside
   the document, so a correctly signed older Ring cannot be replayed.

### Lineage

A root Ring must be version 1 with no parent fields. Any successor must declare
`parentRingVersion` and `parentRingHash`, and both must match a **trusted
predecessor configured in the deployment**.

This makes Ring rollout deliberately two-handed: publishing a new signed
document is not enough, the operator must also advance the trusted predecessor.
A validly signed Ring that is not the declared successor of the configured one
is refused. It is the same principle as the minimum version — an anchor that
lives outside the artefact being verified.

### Node weights are reserved, not used

`weight` is present in the schema and is **not used for placement**. Placement
uses a fixed number of virtual nodes per node, set to 100 — the value every
current Ring already carries, so the placement function is unchanged and no
existing object moves.

The field is kept because capacity heterogeneity between accounts is plausible
eventually, and adding a field to a signed document later is more disruptive
than reserving one now. It is not implemented because per-account capacity
shares are an operational complication that no requirement currently justifies.

A Ring whose node weights are **not all equal is refused**, with a message
stating that the field is reserved. Silently ignoring an operator's attempt to
apportion capacity would be worse than not offering it.

### The virtual-node circle is computed once

The circle is a pure function of the Ring, and the Ring is immutable in memory.
It is built and cached at load. Recomputing it per request — one SHA-256 per
virtual node plus a sort — was a defect, not a design choice, and is corrected
as such.

## Consequences

### Accepted

**No dynamic membership.** Changing the topology means publishing a new signed
document and reloading gateways. There is no join or leave protocol. This is
the price of having no placement service, and it is worth it.

**Ring rollout is a two-step operator procedure.** The document and the trusted
predecessor must both be advanced. Getting it wrong means the gateway refuses to
start — fail-closed, but it needs to be in the runbook.

### Ring migration is not implemented, and is post-V1

This is the significant limitation and it should not be understated.

The lineage machinery *validates* a successor Ring. Nothing *migrates* data to
it. `SignedRing::load` holds exactly one Ring, there is no dual-ring resolution,
and `validate_committed_head` rejects any head whose `ringVersion` differs from
the active one. Changing the Ring on a system that already holds data therefore
makes that data unreachable.

Specification §6.3 describes the intended behaviour — retain the active Ring and
its declared parent during migration, with the reconciler moving committed
versions before the old placement is retired. That remains a specification of
intent.

**What V1 offers is therefore: deploy with N accounts, not grow from N to
N+1 on a live system.** Capacity must be planned at deployment time. Growing an
existing deployment is a post-V1 capability, deliberately deferred so that V1
ships something coherent rather than a half-built migration.

This qualification belongs in `docs/WHY_OVERMESH.md`, whose capacity section
currently describes adding an account as a Ring revision without noting that the
migration path does not yet exist.

### `replicationFactor` is fixed at two

`validate` refuses any other value. This is the extension point named in
ADR-0001: a coding scheme or a higher replication factor changes this line, and
carries a new `apiVersion`.

## When to revisit

**Migration, post-V1.** Dual-ring resolution on the read path, head rewriting,
and a reconciler that knows which ring to search. It is the prerequisite for
the capacity argument to hold operationally rather than only architecturally.

**Weights**, if and only if accounts of materially different capacity become a
real deployment shape. The change is to decouple the virtual-node count from
the weight, not to feed the weight into the existing loop.

**A two-level topology** — regions containing intra-region groups — if erasure
coding within a region is ever pursued. See ADR-0001.

## Implementation status

Two elements of this decision describe the intended state rather than the
current one, and are pending:

- placement must stop reading `weight` and use the fixed virtual-node count;
- non-uniform weights must be refused by `validate`.

The cached circle is a straightforward correction of the same area and should
land with them.

## Verified by

- `harness/scripts/placement-smoke.sh` — a three-node, three-region Ring;
  each object's head is asserted present on exactly its two assigned accounts
  and absent from the third; with one account offline, only the objects placed
  on it become unwritable
- `gateway/src/ring.rs::rejects_ring_rollback` — rollback below the minimum
  trusted version is refused
- `gateway/src/ring.rs::rejects_invalid_signature` — invalid signatures are
  refused
- `gateway/src/ring.rs::rejects_single_region_topology` — replica placement
  requires distinct regions
- `gateway/src/ring.rs::cached_signed_ring_placement_is_deterministic_and_cross_region`
  — cached placement is deterministic and cross-region
- `harness/rings/ring-rollback.yaml` — the rollback fixture
- `LIST-RING-ROLLOVER-001` — a continuation token issued under one Ring
  version is refused after a Ring change
