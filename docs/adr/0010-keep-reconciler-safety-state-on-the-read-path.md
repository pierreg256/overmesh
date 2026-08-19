# ADR-0010 — Keep Reconciler safety state on the read path

- **Status:** accepted
- **Date:** 2026-08-19
- **Milestone:** 0.10.1
- **Supersedes:** —
- **Superseded by:** —

## Context

The first signed performance campaign decomposed a nominal `HEAD` into twelve
Storage requests. Four of them are negative reads on both replicas:

- `quarantine/{path_hash}.json`;
- `high-water/{path_hash}/compaction/current.json`.

These lookups normally return `404`, but absence is not incidental. A
quarantine record makes the blob unavailable after the Reconciler detects
tampering. A compaction checkpoint prevents a signed head and high-water pair
from being replayed below history that the Reconciler has already garbage
collected.

The object-level test also closes the 28 `control_get_object` requests measured
for a first `PUT`, across both replicas:

| Object class | Requests |
| --- | ---: |
| high-water current | 6 |
| catalogue | 4 |
| compaction checkpoint | 4 |
| head | 4 |
| prepared manifest | 4 |
| block manifest | 2 |
| quarantine | 2 |
| terminal manifest | 2 |

ADR-0003 assigns quarantine and garbage collection to the Reconciler identity.
The current head and high-water documents are written by the Gateway identity.
Copying either Reconciler decision into a Gateway-owned document would cross
that authority boundary.

## Options considered

**Copy the flags into the committed head.** This removes the reads but lets the
Gateway publish Reconciler-owned safety state, or requires the Reconciler to
rewrite a Gateway-owned head. Either choice weakens the identity split that
prevents a compromised Gateway from lifting quarantine or moving a recovery
floor.

**Cache the negative results in each Gateway.** This preserves object
ownership, but makes the cache TTL a safety parameter. During the stale window,
a newly quarantined blob can still be served and a newly published compaction
floor can still be ignored. Stateless replicas also need a coherent invalidation
or generation protocol before this is safe.

**Publish one signed Reconciler safety envelope per blob.** This can reduce four
reads to two while preserving identity ownership, but the envelope must exist
before a blob is served and must define atomic evolution of quarantine and
compaction state. Introducing that publication protocol is larger than a read
path refactor.

**Keep the two strict replicated lookups.** Chosen for 0.10.1.

## Decision

The Gateway continues to load quarantine and compaction checkpoint state from
both replicas on every metadata preparation path.

The four nominal negative reads are an explicit safety budget, not accidental
traffic. They must remain separately visible as `quarantine` and
`compaction_checkpoint` object classes in performance evidence.

Removing, caching or aggregating them requires a replacement decision that
specifies:

- which identity publishes the replacement state;
- the maximum interval before quarantine takes effect;
- the maximum interval before a compaction floor becomes authoritative;
- startup and invalidation behaviour for stateless Gateway replicas;
- fail-closed behaviour when replicas disagree or the replacement state is
  missing.

## Consequences

Nominal `HEAD` and `GET` retain four `404` responses. This cost is accepted
until a protocol can reduce it without weakening quarantine or rollback
protection.

The object-class telemetry makes the cost independently budgetable. A future
optimization can therefore demonstrate exactly which safety reads it replaces
rather than inferring them from generic `control_get_object` counts.

## When to revisit

Revisit when a signed Reconciler safety envelope or a push-invalidated
generation protocol has a complete failure model and live validation plan.
Latency alone is not sufficient evidence for changing this decision.

## Verified by

- `gateway/src/commit/tests.rs::rejects_writes_to_a_signed_quarantined_blob` —
  a valid Reconciler quarantine record blocks Gateway writes
- `gateway/src/commit/tests.rs::compaction_floor_rejects_replayed_head_and_high_water_below_the_floor`
  — a compacted recovery floor rejects replay
- `gateway/src/commit/tests.rs::first_put_control_reads_have_a_closed_object_level_budget`
  — closes all 28 first-PUT control reads by object class
- `harness/environments/azure/performance/collect_live_performance_telemetry.py`
  — publishes request counts by object class and status
- `harness/artifacts/live/0.10.1/performance-v010-evidence.json` — retained
  signed evidence closes every v2 request budget with 30 client fingerprints
  and zero unattributed backend requests per case
