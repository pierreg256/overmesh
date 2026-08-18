# ADR-0001 — Strict replication rather than erasure coding for V1

- **Status:** accepted
- **Date:** 2026-08-15
- **Milestone:** 0.1.0
- **Supersedes:** —
- **Superseded by:** —

## Context

An earlier prototype of Overmesh stored every object as a 2+1 XOR erasure code:
each stripe was split into two data shards and one parity shard, placed across
three failure domains by rendezvous hashing. Storage overhead was 1.5×, and any
single shard could be reconstructed from the other two.

The V1 rewrite had to decide whether to carry that scheme forward.

The project's primary claim is that several Azure Storage Accounts, in regions
of the operator's choosing, can be federated behind a single Blob endpoint with
a consistency state that is provable. That claim is about placement, commit
protocol, and identity — not about storage efficiency. Erasure coding is
orthogonal to it, and considerably harder to get right.

## Options considered

**Carry the 2+1 erasure code into V1.** Retains the 1.5× storage overhead.
Requires the commit protocol, the read path, the reconciler, and the repair
logic to reason about shards rather than objects from day one, with every
correctness question asked twice — once about the logical object, once about
the coding scheme.

**Strict replication with a fixed replication factor of two.** Storage overhead
of 2×. Every replica holds the complete object. The commit protocol, the read
path, and repair all operate on whole objects, so the hard parts of the project
— two-replica commit, signed consistency metadata, replay protection,
reconciliation — can be proven without a coding scheme in the way.

**Replication in V1, erasure coding as a later generation.** The above, with an
explicit commitment not to foreclose the coding scheme.

## Decision

V1 uses strict replication with `replicationFactor = 2`. Erasure coding is
deferred, not rejected.

The goal of V1 is to demonstrate that the federation works at all: that a
strict cross-region commit, signed placement and consistency metadata, and an
automatic reconciler can be built and verified. Storage efficiency is a
second-order concern that would have doubled the V1 effort while adding nothing
to that demonstration.

**This decision carries a binding constraint on V1.** Erasure coding must
remain reachable. A V1 change that makes a future coding scheme materially
harder to introduce is to be refused on those grounds, and this ADR is the
authority for refusing it.

## Consequences

### Accepted

Storage cost is 2× rather than 1.5× — a 25% penalty against the erasure-coded
alternative, on top of whatever superseded generations have not yet been
collected.

In exchange, every replica is a complete, directly readable copy. A read can be
served entirely from one region. Repair is a copy, not a reconstruction. The
reconciler validates content by streaming and hashing, with no decode step. And
a replica remains a normal Azure blob that an operator can inspect with any
standard tool — which matters more than it sounds for a system that is asking
to be trusted.

### What stays open

Placement generalises. `RingDocument::replicas_for` already selects nodes across
distinct regions in ranked order; extending it from two replicas to `k + m`
shards is mechanical.

The consistency machinery is content-agnostic. Heads, the signed catalogue,
high-water records, quarantine and audit reference stored objects by key and by
digest. None of them counts the objects.

### What a future coding scheme will have to change

**The commit manifest schema.** `CommitManifest` carries a single
`content_object`. A coded object has `k + m` shards. Because signed documents
use `deny_unknown_fields` and per-type signature domains, this is necessarily a
new document type under a new domain and a new Ring `apiVersion` — which is the
clean path rather than an obstacle, but it is not a field addition.

**The fixed consistency parameters.** Specification §5.1 fixes `RF = 2`,
`W = 2`, `R = 2` as normative. A coding scheme replaces them with a durability
threshold, and that section becomes generation-specific.

**The read path.** `read_validated_block` fetches a block range from one
replica and falls back to the other. Under a coding scheme it must gather `k`
shards and reconstruct. The interface — return validated bytes for this block —
is the right seam, but the implementation behind it is different work.

**The identity story loses some of its simplicity.** Today the stored bytes are
exactly the bytes the caller uploaded, which is what makes it natural for the
caller to write them under their own credentials with Azure RBAC as the single
authorization authority. Parity shards are derived, not supplied. Whoever
computes them writes something the caller never sent.

## When to revisit

When storage cost becomes a decision-driving constraint at a real deployment
scale, and not before.

The likely shape is **not** a straight substitution. Spreading `k` shards across
regions means every read touches more than one region, trading storage against
cross-region egress and latency on the read path — the opposite of what
replication buys. The conventional answer is erasure coding *within* a region
and replication *across* regions, which turns the Ring from a flat node list
into a two-level topology of regions and intra-region groups.

That is a generation boundary, not a milestone. It should be evaluated after
the live performance baseline exists, so the trade is made against measured
egress and latency rather than against an estimate.

## Verified by

- `RingDocument::validate` rejects any Ring with `replicationFactor != 2`
  (`gateway/src/ring.rs`)
- `INVARIANT-002` — healthy replicas hold identical content digests
- `commit-success-001` — an acknowledged write is byte-identical on both
  replicas
- `harness/scripts/placement-smoke.sh` — three-node Ring, two replicas per
  object, in distinct regions
