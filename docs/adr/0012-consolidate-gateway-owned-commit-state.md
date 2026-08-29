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
It removes reads of immutable pre-commit state that were genuinely duplicated
inside the canonical lease. A load before mutation and a verification after
mutation are not duplicates: ADR-0013 requires the latter to preserve
read-time divergence detection. Request-scoped reuse is therefore a
prerequisite here rather than an alternative.

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

### The lease stays create-first, and lease-first is deferred

Making the lease canonical raised the adjacent question 0.11.0 left open:
whether to acquire the Azure lease **first** and create the lock object only
when the lease attempt reports the object missing, rather than the current
order — conditional create, then acquire.

That question is now measured, and the answer is no, not on this evidence.

`control_acquire_lock` issues a conditional `PUT` with `If-None-Match: *` on
`locks/{path_hash}`, then a `PUT ?comp=lease`. A commit therefore spends three
lock requests: create, acquire, release. The corrected v5.1 fast campaign
decomposes them by operation and by status:

| Workload | create | acquire | release | per operation |
| --- | --- | --- | --- | ---: |
| First `PUT` (24 runs) | `201` | `201` | `200` | 3 |
| Established overwrite (9 runs) | `409` | `201` | `200` | 3 |
| `DELETE` (9 runs) | `409` | `201` | `200` | 3 |

**Both workloads cost three lock requests.** They differ only in the status of
the conditional create. Every run reports `control_put_bytes/lock = 10`,
`control_acquire_lock/lock = 10` and `control_release_lock/lock = 10` for ten
measured operations, with `"status": "valid"` and no failures.

**The `409` on an established overwrite is not a lease conflict.** It is the
conditional create refusing to recreate a lock object that already exists,
which is the expected steady-state result once a blob has been written once.
A genuine lease conflict is a different outcome: `BackendError::LeaseConflict`
from the `?comp=lease` request, surfaced as `CommitError::LockConflict`, which
fails the operation. The campaign records ten successful acquires per run and
zero failures, so no `409` in this evidence is a contended lease.

Lease-first would not remove a request; it would **move** one. On an
established blob the lease acquire would succeed immediately, giving two
requests instead of three. On a first write it would fail against a missing
object, requiring create, retry-acquire and release — four instead of three.
The trade is `-1` on overwrite and `+1` on first write, which is only
worthwhile under an explicit workload policy asserting that established
overwrites dominate first writes. No such policy exists, and nothing in the
0.11 campaigns measures the ratio.

**Decision: lease-first is deferred and the create-first order is retained.**
This closes the 0.11.1 deliverable by explicit deferral rather than by silence.
The runtime lock order is unchanged by this record, and the merged commit-state
document does not depend on it: the merge needs the lease to be *canonical*,
which it already is, not to be acquired in a particular order.

The corrected v5.1 fast campaign is `baselineEligible: false` and
`campaignPurpose: "diagnostic-fast"`, so it carries no latency conclusion. The
lock counts used here are structural integers reproduced identically across
every run, payload and concurrency level of each family, which is what a
request-order decision needs and all it is used for.

## Consequences

### The expected budget

Derived from the object-class decomposition rather than measured, and
verifiable under Azurite before any live campaign:

| Stage | Control reads per `PUT` |
| --- | ---: |
| Initial certified layout | 28 |
| Request-scoped reuse without changing verification | 24 |
| After merging the four gateway-owned classes | ~10 |

The request-scoped change removes four reads: the second replicated
compaction-checkpoint load and one duplicated high-water load. The remaining
multi-touch classes include their writes and mandatory post-write replicated
verification; removing those reads without the merge would weaken ADR-0013.
With the corresponding reduction in control writes, the merged layout is still
expected to take a first `PUT` toward roughly 25 backend requests.

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

If a workload policy is ever written that states established overwrites
dominate first writes, lease-first becomes a one-request saving on the dominant
path and should be reopened against that policy — with the first-write
regression stated, not hidden. Measuring the first-write to overwrite ratio is
the prerequisite, and no current campaign reports it.

## Implementation status

Implemented. The merged document is published at `heads/{path_hash}.json` as a
`SignedDocument<BlobCommitState>` under signature domain
`overmesh:blob-commit-state:v1`. It carries an API version, a format version,
the canonical blob and its path hash, the Ring version, the current committed
or tombstoned generation, and any interrupted preparation — signed together,
under the Gateway identity, in the same commit. It replaces the separate head,
high-water current, prepared manifest and terminal manifest objects. Catalogue
entries and high-water history objects hold the terminal form of the same
document, so the merge costs no additional signature per transition.

The canonical-lease prerequisite was already implemented: both Gateway and
Reconciler route `locks/{path_hash}` to the deterministic primary whenever a
head identifies a canonical logical blob.

Two-phase commit is now a conditional state machine on one object. The prepared
transition is conditional on each replica's loaded entity tag; the commit
transition is conditional on the entity tags that transition returned, so a
concurrent writer cannot skip the prepared state. A retry that finds a
`Prepared` generation for its own write ID reuses it, which is what the removed
immutable prepared sidecar previously provided.

A preparation that reaches only one replica leaves the same published
generation inside two documents whose bytes differ. Neither side is
authoritative over the other by logical version, so reconciliation converges
both onto the terminal form of the generation they already publish — the
durable high-water history entry — and discards the never-committed
preparation. No Gateway-owned state is minted to do it.

For the same reason an idempotent replay never republishes the loaded document:
it publishes that terminal form to the catalogue and the history, because the
loaded document may still carry an unrelated interrupted preparation.

The high-water assertion is no longer a second object, so the rollback witness
is the retained per-version history plus the Reconciler-owned compaction
checkpoint that ADR-0010 keeps replicated. Every write proves, on both
replicas, that the durable history entry for the generation it replaces exists,
and that no durable history entry exists above it. The second check is a narrow
prefix listing bounded to one logical version. A first write performs neither,
because it replaces no generation.

**One detection property moved.** Before this record, a `GET` or `HEAD`
compared the head against a separately published high-water object, so a head
replayed on both replicas without its high-water was rejected at read time.
The merged document is internally consistent by construction, so above the
ADR-0010 compaction floor a replayed document is now rejected by the next write
and by the Reconciler rather than by the read. Under W=2 both objects were
already written by the same identity in the same commit, so this removes a
duplicate rather than an independent witness; the independent witnesses are the
catalogue, the retained history, and the compaction floor.

### Measured budget

Closed object-level control-read budgets under Azurite, verified by the tests
below:

| Stage | First `PUT` | `DELETE` |
| --- | ---: | ---: |
| Certified 0.11.0 layout | 28 | 24 |
| Request-scoped reuse | 24 | 22 |
| Merged commit state | 16 | 16 |

The merged first `PUT` reads two catalogue, one compaction checkpoint, one
quarantine and one block-manifest object per replica, plus three reads of the
merged document: one load and the two post-write verifications ADR-0013
requires. `DELETE` additionally reads one high-water history object per replica
and performs one narrow prefix listing per replica. Control writes fall from
twelve to eight per first `PUT`, because the prepared, terminal and high-water
current objects are no longer written.

### Greenfield

There is no migration and no dual-read path. The document carries
`formatVersion: 1` so a later change is possible. Existing deployments holding
data must be recreated.

## Verified by

- `gateway/src/manifest.rs` — `BlobCommitState`, its signature domain and
  `validate_blob_commit_state`
- `gateway/src/commit.rs` — `load_state`, `publish_blob_state`,
  `resolve_write_state` and `adopt_interrupted_preparation`
- `gateway/src/commit/high_water.rs` — the durable-history witness and the
  replayed-generation rejection that replace the high-water current object
- `gateway/src/commit/tests.rs::first_put_control_reads_have_a_closed_object_level_budget`
  — the merged 16-read first-`PUT` budget
- `gateway/src/commit/tests.rs::delete_control_reads_have_a_closed_object_level_budget`
  — the merged 16-read `DELETE` budget
- `gateway/src/commit/tests.rs::a_replayed_generation_is_rejected_by_the_durable_history`
  — rollback rejection without a duplicated high-water object
- `gateway/src/commit/tests.rs::a_replayed_commit_state_is_rejected_by_the_next_write`
  — where the moved read-time detection now happens
- `gateway/src/commit/tests.rs::interrupted_preparation_is_visible_and_reused_by_the_same_write`
  — the prepared state of the merged document
- `reconciler/src/engine/validation.rs` — replica validation against the merged
  document and its durable history entry
- `reconciler/src/engine/tests/orchestration.rs::a_one_sided_preparation_is_repaired_rather_than_quarantined`
  — an asymmetric preparation is repairable drift, not a conflict
- `gateway/src/commit/tests.rs::put_replay_publishes_the_terminal_generation_despite_an_interrupted_preparation`
  — replays publish the terminal generation, not the loaded document
- `gateway/src/commit/tests.rs::listing_hides_a_commit_state_document_that_is_not_signed_overmesh_state`
  — listing treats the merged document as truth only after verifying it
- `reconciler/src/engine/tests/orchestration.rs::a_head_replayed_below_the_durable_history_is_quarantined`
  — the Reconciler's replacement for the removed high-water comparison
- `reconciler/src/engine/tests/orchestration.rs::an_interrupted_preparation_does_not_hide_the_published_generation`
  — an interrupted preparation is a state of the document, not a fault
- `reconciler/src/engine/tests/orchestration.rs::a_tombstone_published_before_its_history_entry_is_recoverable`
  — the tombstone crash window the merge preserves
- `harness/scripts/reconciler-smoke.sh` and `harness/scripts/gateway-smoke.sh` —
  the Azurite gates that assert the merged layout end to end
- `harness/artifacts/live/0.11.0/performance-v011-v4-evidence.json` — the
  certified baseline recording 49 backend requests per first `PUT` and 43 per
  `DELETE`
- `harness/artifacts/live/0.11.0/performance-v011-v5.1-fast-corrected-evidence.json`
  — the corrected v5.1 diagnostic campaign whose per-operation and per-status
  lock decomposition defers lease-first
- `gateway/src/backend.rs` — `control_acquire_lock` performs the conditional
  create and then the lease acquire, in that order
- `gateway/src/commit/locking.rs` — PUT and DELETE take the canonical lease on
  the deterministic primary
- `gateway/src/commit/tests.rs::rejects_a_write_when_the_blob_lease_is_held`
  — a contended lease is `CommitError::LockConflict`, not a conditional-create
  `409`
- `reconciler/src/engine/tests/orchestration.rs::anomalous_head_discovered_on_secondary_locks_deterministic_primary`
  — the canonical lease this record requires
- `docs/adr/0010-keep-reconciler-safety-state-on-the-read-path.md` — the
  Reconciler-owned reads this record deliberately leaves outside the merge
