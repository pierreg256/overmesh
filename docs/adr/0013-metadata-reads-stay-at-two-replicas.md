# ADR-0013 — Metadata reads stay at two replicas

- **Status:** accepted
- **Date:** 2026-08-22
- **Milestone:** 0.11.0
- **Supersedes:** —
- **Superseded by:** —

## Context

This record exists because ADR-0002 makes a configuration look available that
should not be taken, and predicts that performance work will drift toward it.

The firm rule is `W + R > RF`. ADR-0002's own table of admissible
configurations lists `W = 2, R = 1` as **yes**, described as "V1 today, content
path". With `W = 2` both replicas were written, so reading one preserves the
intersection guarantee. Nothing in the firm rule forbids reading a single
replica's metadata.

Someone optimising the 49-request `PUT` or the 10-request `HEAD` will find that
table, observe that it permits `R = 1`, and halve every control read. The
purpose of this record is to make that a decision that has already been taken
rather than one that is taken quietly.

## What `R = 2` on metadata actually buys

Not consistency. `W = 2` supplies that.

ADR-0002 requires both committed heads to be **byte-identical**, both
high-water records compared, both block-manifest roots compared. That is
divergence detection: corruption, a partial write, or tampering by a principal
with write access to the system container. ADR-0003 is explicit that such
tampering is *detected rather than prevented*, because subscription
administrators are inside the trust boundary.

So the question is not whether `R = 1` is admissible. It is: **how long may a
diverged or tampered head be served before anything notices?**

## Options considered

**`R = 1` on all metadata.** Halves every control read on both the read and
write paths. Detection of divergence moves entirely to the Reconciler.

**`R = 1` on the high-water only, keeping the head at `R = 2`.** A partial
measure. It saves one read per replica and leaves the detection argument
unresolved, since a diverged high-water is exactly what rollback protection
depends on.

**Keep `R = 2` on metadata.** Chosen.

## Decision

Metadata continues to be read from both replicas and compared. `R = 1` remains
reserved for content bytes, which is what ADR-0002 already describes.

Two things decide it, and both are arithmetic rather than principle.

**Detection latency.** The Reconciler runs one cycle per day, a figure set by
cost rather than by correctness. Under `R = 1`, a diverged or tampered head
could therefore be served for up to twenty-four hours before anything observed
it. Under `R = 2` it fails closed on the next read.

**The saving is small, and ADR-0012 makes it smaller.** Once head, high-water,
prepared and terminal state are one document, `R = 2` costs two reads rather
than six. `R = 1` would then save a single request out of roughly twenty-five.

**One request against a twenty-four hour blind spot on the integrity claim the
system is built to make.** That is not a close call.

## Consequences

The control-read budget keeps its `R = 2` component. Performance work on the
metadata path must come from layout and from request-scoped reads, which is
where ADR-0012 puts it.

Read-time divergence detection remains a property of the system rather than a
scheduling artefact. This matters more than the request count: the guarantee
Overmesh sells is that a successful write means two durable, identical
replicas, and reading one of them would leave that claim checked only once a
day.

The Reconciler's cycle period becomes a documented input to an access-path
decision rather than an operational detail. Lengthening it further weakens
nothing today, because nothing depends on it; shortening it is what would make
this record worth reopening.

## When to revisit

**If the Reconciler cycle shortens materially** — minutes rather than a day —
the detection window narrows and the arithmetic changes. The reasoning above is
a function of that period and of nothing else about the design.

**If a future generation moves to `W = 1`**, the firm rule forces `R = 2` and
this record becomes redundant rather than wrong.

**If the merged document of ADR-0012 grows** to the point where reading it
twice is a material cost rather than one extra request, the trade is worth
recomputing — but the answer would more likely be to shrink the document than
to stop comparing it.

## Implementation status

No change. This record documents a decision not to change existing behaviour,
so that the change is not made silently later.

## Verified by

- `docs/adr/0002-synchronous-two-replica-commit.md` — the firm rule and the
  admissibility table this record narrows in practice
- `docs/adr/0003-separate-caller-and-control-identities.md` — tampering is
  detected rather than prevented, which is what read-time comparison implements
- `gateway/src/commit/tests.rs::first_put_control_reads_have_a_closed_object_level_budget`
  — the per-replica read budget that `R = 2` contributes to
- `harness/artifacts/live/0.11.0/performance-v011-v4-evidence.json` — the
  certified request budgets against which the saving was weighed
