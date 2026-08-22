# ADR-0012 — Consolidate gateway-owned commit state into one document

- **Status:** accepted
- **Date:** 2026-08-22
- **Milestone:** 0.11.0
- **Supersedes:** —
- **Superseded by:** —
- **Amends:** ADR-0002

## Context

The first signed performance campaigns measured a first `PUT` at 49 backend
requests and roughly 1 200 ms, independent of payload size below one megabyte.
A `DELETE`, which carries no bytes at all, costs 43. Gateway CPU during those
campaigns averaged 0.003 cores. The cost is not computation and it is not bulk
transfer; it is a queue of round trips.

Object-class telemetry, added in 0.10.1, decomposes the 28 control reads of a
first `PUT` across both replicas:

| Object class | Reads | Owner |
| --- | ---: | --- |
| high-water current | 6 | Gateway |
| catalogue | 4 | Gateway |
| head | 4 | Gateway |
| prepared manifest | 4 | Gateway |
| terminal manifest | 2 | Gateway |
| block manifest | 2 | Gateway |
| compaction checkpoint | 4 | Reconciler |
| quarantine | 2 | Reconciler |

Two facts stand out. Six of the eight classes belong to the same identity, are
written inside the same commit, and are read together. And several classes are
read more than once per replica within a single request — the high-water three
times, the head, catalogue and prepared manifest twice each.

The system has one object per concept. Each concept costs two reads because
`RF = 2`. That granularity was chosen concept by concept as each was introduced
and has never been reconsidered as a whole. **The 49 is largely the price of a
layout, not the price of the guarantees.**

## Options considered

**Cache control documents across requests.** Rejected on three independent
grounds. The Gateway is stateless by design and ran at 10, then 20, then 25
replicas; Front Door provides no per-blob affinity, so the hit rate on repeated
access to one blob is roughly one in twenty-five. A shared cache reintroduces
the runtime dependency ADR-0006 refused for placement. And for any mutable
document the time-to-live becomes a safety parameter, which is the reasoning
ADR-0010 already applied to quarantine.

**Relax `R` on metadata.** Admissible under the firm rule and refused for
reasons of detection latency. See ADR-0013.

**Read each document once per request.** Correct and insufficient on its own.
It removes the duplicate reads but leaves one round trip per concept per
replica. It is a prerequisite here rather than an alternative.

**Merge the gateway-owned commit state into one document.** Chosen.

## Decision

The Gateway-owned state describing a logical blob's current committed
generation is published as **one document per blob per replica**, replacing the
separate head, high-water current, prepared manifest and terminal manifest
objects.

The merged document asserts the same facts, signed together, under the same
identity, in the same commit. It is a change of layout, not of guarantee.

### What merges, and what does not

**Merged:** head, high-water current, prepared and terminal commit state.

**Not merged, and why:**

*Catalogue.* Its key is ordered lexicographically so that listing can page
through it; the head is keyed by `path_hash`. The two key spaces are
incompatible by construction. See ADR-0008.

*Block manifest and its pages.* Their size grows with block count. Merging them
would make every `HEAD` and `GET` read block integrity metadata it does not
need, which is exactly what ADR-0011 removed from the `HEAD` path.

*Quarantine and compaction checkpoint.* Owned by the Reconciler under ADR-0003,
and kept as explicit replicated reads by ADR-0010. Merging them would place
Reconciler-owned safety state inside a Gateway-written document, which is the
authority boundary both records exist to hold.

**Ownership therefore does not change.** Every merged element was already
written by the Gateway identity. This record does not amend ADR-0003.

### Two-phase commit becomes a state machine on one object

The prepared and committed states stop being two objects and become two states
of one. The transition is conditional on the prepared document's entity tag, so
a concurrent writer cannot skip the prepared state.

A recovery that finds `state = Prepared` knows a preparation was interrupted.
That information is not lost by overwriting — the absence of the overwrite *is*
the signal.

### The commit lease must be canonical first

This decision has a prerequisite that is currently unmet.

The Gateway always acquires `locks/{path_hash}` on the deterministic primary.
The Reconciler acquires the same key on the primary in its recovery path, but
in `reconcile_head` it may acquire it on `candidate.discovered_on` instead —
the replica where an anomalous head was found. When that branch is taken on the
secondary, the two components hold different leases for the same logical blob
and do not exclude one another.

With separate objects each component writes through its own conditional path
and the asymmetry is survivable. With a merged document containing the
high-water, which reconciliation updates, it produces a write conflict that
does not exist today.

**The commit lease is therefore defined as a single canonical lease, taken on
the deterministic primary of the Ring, by every component that writes
Gateway-owned commit state.** The `discovered_on` branch is a defect and is
corrected as one.

### What the canonical lease then makes true

Once both components exclude one another on a blob, Reconciler-owned state
cannot change inside a commit. The write path therefore reads the quarantine
record and the compaction checkpoint **once** per replica per request rather
than once at the start and again at the end. This is not the caching ADR-0010
forbids; see the amendment to that record.

## Consequences

### The expected budget

Derived from the object-class decomposition rather than measured, and
verifiable under Azurite before any live campaign:

| Stage | Control reads per `PUT` |
| --- | ---: |
| Today | 28 |
| Reading each document once per request | ~16 |
| After merging the four gateway-owned classes | ~10 |

With the corresponding reduction in control writes, a first `PUT` should fall
from 49 backend requests to roughly 25. Approximately half of that comes from
reading each document once, which requires no format change at all.

These are estimates. The blocking metric is exact and testable locally, so the
decision is falsifiable before a campaign is run.

### Byte-identical comparison, and what it now covers

ADR-0002 requires both committed heads to be byte-identical on read. That
invariant now applies to the merged document, which additionally carries the
high-water.

This holds because reconciliation never advances the high-water on one replica
alone: a reconciliation cycle ends with identical documents on both. During a
cycle the two may differ, and a read in that window fails closed — which is the
existing behaviour for heads and is unchanged.

**This is the amendment to ADR-0002.** The rule is the same; the document it
ranges over is larger.

### Greenfield only

There is no migration and no dual-read path. Existing deployments holding data
must be recreated. This is acceptable only because the project is in
development and in use nowhere.

The document carries a format version so that a later change is possible, but
**the window for making this change without a migration closes at V1.** After
that it becomes the same class of problem as Ring migration in ADR-0006:
described, deferred, and expensive.

### Contention

One document per blob is rewritten by every operation on that blob. Under the
canonical lease those operations are already serialised, so the change moves
contention rather than creating it. It should be measured rather than assumed.

## When to revisit

If listing ever requires the catalogue and the head to be consistent at a
single read, the catalogue's exclusion should be reconsidered together with
ADR-0008 — but that requires a key space that is both ordered and derivable
from `path_hash`, which no current scheme provides.

If block-level integrity moves off the read path entirely, the block manifest's
exclusion is worth reopening.

If the Reconciler ever needs to publish commit state independently of the
Gateway, the ownership assumption behind this record fails and ADR-0003 becomes
the constraint rather than a bystander.

## Implementation status

The merged state document is not implemented. Its canonical-lease prerequisite
is implemented: both Gateway and Reconciler route `locks/{path_hash}` to the
deterministic primary whenever a head identifies a canonical logical blob,
including anomalous heads that cannot be trusted as committed state.

## Verified by

- `gateway/src/commit/tests.rs::first_put_control_reads_have_a_closed_object_level_budget`
  — establishes the 28-read baseline this record reduces
- `harness/artifacts/live/0.11.0/performance-v011-v4-evidence.json` — the
  certified baseline recording 49 backend requests per first `PUT` and 43 per
  `DELETE`
- `gateway/src/commit/locking.rs` — the Gateway acquires the commit lease on
  the deterministic primary
- `reconciler/src/engine/orchestration.rs` — the Reconciler acquires the same
  key on the deterministic primary in recovery and for every identifiable head
  in `reconcile_head`
- `docs/adr/0010-keep-reconciler-safety-state-on-the-read-path.md` — the
  Reconciler-owned reads this record deliberately leaves outside the merge
