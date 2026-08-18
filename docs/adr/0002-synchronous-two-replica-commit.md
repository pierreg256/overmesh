# ADR-0002 — Synchronous two-replica commit, with `W + R > RF` as the firm rule

- **Status:** accepted
- **Date:** 2026-08-15
- **Milestone:** 0.3.0
- **Supersedes:** —
- **Superseded by:** —

## Context

ADR-0001 fixes the replication factor at two. This decision is about the
remaining question: how many replicas must acknowledge before the client is
told the write succeeded.

The answer determines the system's headline guarantee, its write availability,
and how much reasoning the read path has to do.

## Options considered

**`W = 2`, synchronous.** The client waits for both replicas. An acknowledged
write exists, committed and identical, on both. A read that has validated both
heads needs no conflict resolution, because divergence is by definition a fault
rather than a normal state.

**`W = 1` with asynchronous replication.** The client waits for one replica;
the second catches up in the background. Better write latency, and writes
survive the loss of one region. In exchange, the read path must resolve
divergence, and the acknowledged-write guarantee weakens to "will converge".

**`W = 1` with hinted handoff.** As above, plus a durable hint recording where
the missing write belongs, replayed when the absent replica returns. It is the
correct answer for write availability during a regional outage, and it is
materially more work: hint durability, hint ownership, replay ordering,
anti-entropy against lost hints.

## Decision

V1 commits synchronously with `W = 2`.

The reason is the same as ADR-0001: V1 exists to demonstrate that the
federation works, and `W = 2` is the variant whose correctness is simplest to
state and to prove. Divergence between replicas is never a legitimate state, so
every read can fail closed on it, and every scenario has one expected outcome
rather than a window of acceptable ones. Hinted handoff is a feature worth
building; it is not a feature worth building while the commit protocol itself is
still being proven.

**The firm rule, binding on every future generation, is `W + R > RF`.** Any
configuration must guarantee that the set of replicas written and the set read
intersect, so that a read cannot miss an acknowledged write.

With `RF = 2` the admissible space is small and worth writing out:

| Configuration | `W + R` | Admissible | Character |
| --- | --- | --- | --- |
| `W=2, R=2` | 4 | yes | V1 today, metadata path |
| `W=2, R=1` | 3 | yes | V1 today, content path |
| `W=1, R=2` | 3 | yes | the shape any future async mode must take |
| `W=1, R=1` | 2 | **no** | no intersection guarantee |

The rule excludes exactly one configuration — and it is the one that
performance work drifts toward. That is why it is stated as firm.

### What V1 actually does

The read path already sits at `R = 2` for consistency metadata and `R = 1` for
bytes: both committed heads are loaded and required to be byte-identical, both
high-water records and both block-manifest roots are compared, and only then is
content streamed from the deterministic primary with fallback to the secondary.
This matches specification §5.1 and satisfies the rule with margin.

## Consequences

### Accepted

**Writes stop while either region is unavailable.** This is the one dimension
on which Overmesh is currently worse than the alternative it replaces: a single
ZRS account survives the loss of an availability zone and keeps accepting
writes. Overmesh does not. The trade is cross-region durability and a provable
consistency state against write availability, and it is made deliberately.

Write latency is bounded by the slower region on every operation.

### What it buys

Invariant 1 holds in its strongest form: an acknowledged write exists as the
same committed logical version on both replicas, with a signed manifest on each
proving it.

A cross-region RPO of zero for acknowledged writes, which no asynchronous
geo-replication offers. This is a by-product rather than a requirement — see
`docs/WHY_OVERMESH.md` — but it is a real one.

Failure reporting is honest by construction. A partial commit is reported as
ambiguous rather than as success, and the write ID makes the retry idempotent.

## What changes if `W = 1` is ever adopted

The change is smaller than it looks in one place and larger in another.

**Smaller: the read fan-out is already correct.** Both heads are already
fetched. The hinge is `strict_current_head`, which today rejects any difference
between the two heads. Under `W = 1` it must instead select the authoritative
head — and that logic already exists, in the reconciler's `authoritative_over`,
which requires `version + 1` and an explicit `previous_logical_etag` chain.
Adopting `W = 1` is largely a matter of moving that function onto the read path.

**Larger: the guarantee itself changes.** Invariant 1 would have to be restated
as "an acknowledged write exists on at least `W` replicas and will converge".
That is materially the promise asynchronous geo-replication already makes. The
differentiator does not disappear, but it narrows to "a degraded mode that is
entered explicitly and reported", rather than "the normal mode of operation".
Any proposal to adopt `W = 1` must say which of the two it is offering.

**A trap to avoid.** "Read the primary first" does not by itself make eventual
consistency safe. The primary is derived deterministically from the Ring hash;
it is not the node that received the write. Under hinted handoff a write may
land on the secondary while the primary is down, and reading the primary first
then returns a stale value or none. Primary-first is a latency and egress
optimisation for content bytes once the head has been validated — it is not a
consistency mechanism.

## When to revisit

When write availability during a regional outage becomes a blocking objection
from a real deployment, and not before. The natural sequencing is after the
live performance baseline exists, so the cost of `W = 2` is known in numbers
rather than argued in principle.

Any such proposal is expected to arrive as its own ADR superseding this one,
carrying: the target configuration, its `W + R > RF` justification, the
restated Invariant 1, and the hint durability and replay design.

## Verified by

- `commits_identical_heads_to_both_replicas` — both replicas carry the same
  committed head after an acknowledged write (`gateway/src/commit/tests.rs`)
- `reports_ambiguous_outcome_when_only_one_head_is_published` — a one-sided
  publication is never reported as success
- `retry_completes_a_single_head_publication` — the write ID makes the retry
  idempotent
- `INVARIANT-001` — acknowledged writes are committed on both replicas
- `commit-fail-008` — the commit-state-machine failpoint that leaves one
  replica behind resolves to `HEALTHY` only after reconciliation
- `harness/scripts/placement-smoke.sh` — with one account offline, objects
  placed on it return `503` while objects placed elsewhere still commit
