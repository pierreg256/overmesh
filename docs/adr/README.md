# Architecture decision records

Why things are the way they are, and what it would cost to change them.

These records exist because several of the decisions below were **reversed**
during development. The reasoning behind a reversal is the part that
disappears fastest and is the most expensive to rediscover: without it, the
first thing a new contributor does is reintroduce the version that did not
work.

The normative documents remain
[`OVERMESH_V1_SPECIFICATION.md`](../../OVERMESH_V1_SPECIFICATION.md) and
[`OVERMESH_DEVELOPMENT_HARNESS_SPECIFICATION_V1.md`](../../OVERMESH_DEVELOPMENT_HARNESS_SPECIFICATION_V1.md).
An ADR explains a choice; it does not define behaviour.

## Index

| # | Decision | Status | Milestone | Fully implemented |
|---|---|---|---|---|
| [0001](0001-strict-replication-rather-than-erasure-coding.md) | Strict replication rather than erasure coding for V1 | accepted | 0.1.0 | yes |
| [0002](0002-synchronous-two-replica-commit.md) | Synchronous two-replica commit, with `W + R > RF` as the firm rule | accepted | 0.3.0 | yes |
| [0003](0003-separate-caller-and-control-identities.md) | Separate caller and control identities | accepted | 0.5.0 | yes |
| [0004](0004-immutable-content-naming-in-the-customer-container.md) | Immutable content naming in the customer container | accepted | 0.5.0 → 0.8.0 | partial |
| [0005](0005-authorization-probes-for-metadata-only-operations.md) | Authorization probes and the resource they check | accepted | 0.5.0 → 0.8.0 | partial |
| [0006](0006-placement-through-a-signed-ring-document.md) | Placement through a signed Ring document | accepted | 0.2.0 → 0.6.0 | partial |
| [0007](0007-canonical-logical-resource-identity.md) | Canonical logical resource identity | accepted | 0.5.0 → 0.8.0 | partial |

**0008 — the signed catalogue behind listing** is deferred until the listing
validation cost and the catalogue key encoding have been settled. Writing it
now would record a snapshot rather than a decision.

## Outstanding implementation

Records marked *partial* describe the intended state. What is pending, grouped
by the place it lands:

| Where | Change | From |
|---|---|---|
| `LogicalBlobId::parse` | validate length against the derived bound, not 1,024 characters | 0004, 0007 |
| `LogicalBlobId::parse` | refuse the reserved `.overmesh` prefix | 0004, 0007 |
| `commit/write.rs` | publish the catalogue entry in the same conditional sequence as the head | 0004 |
| `commit.rs` | rename `put_file_idempotent` / `put_bytes_idempotent` to name their credential | 0005 |
| live Azure gate | denied-principal cases for `Put Blob`, `Put Block`, `Put Block List` | 0005 |
| live Azure gate | idempotent replay by the original principal after write-permission revocation, asserting `403` and not `409`/`412` | 0005 |
| `reconciler/src/posture.rs` | fail closed on a path-predicate condition effective on a customer container, including inherited | 0005 |
| `ring.rs`, `reconciler` | carry the canonical identity as `LogicalBlobId` rather than `&str` past `parse` | 0007 |
| `ring.rs` | stop reading `weight`; use a fixed 100 virtual nodes per node | 0006 |
| `ring.rs` | refuse a Ring whose node weights are not all equal | 0006 |
| `ring.rs` | build and cache the virtual-node circle at load | 0006 |

The two `parse` changes are coupled to the catalogue encoding: if 0008 changes
it, the derived bound changes in `parse`, in 0007, and in
[`COMPATIBILITY.md`](../../COMPATIBILITY.md). Settle the encoding first.

## Conventions

One file per decision, `NNNN-title-in-kebab-case.md`, numbered in order of
writing rather than of decision. Records are written in English, like the
specifications.

Each record carries:

```
- **Status:** proposed | accepted | superseded by ADR-NNNN
- **Date:**
- **Milestone:**
- **Supersedes:**
- **Superseded by:**
```

followed by **Context**, **Options considered**, **Decision**,
**Consequences**, **When to revisit**, and **Verified by**.

**Options considered** is not decoration. A rejected option with its reason is
what stops the same proposal returning in six months.

**Verified by** names the tests, scenarios or gates that prevent the decision
being undone silently. The intent is that this eventually becomes checkable —
an `accepted` record referencing a test that no longer exists should fail
validation, in the same way `version-check` already refuses a roadmap that
disagrees with `VERSION`.

Where a record's rationale was reconstructed from the implementation rather
than recorded at the time, it says so in a **Provenance** note. ADR-0004 is the
only one so far.

Records are not edited to match reality once accepted. A decision that changes
gets a new record that supersedes the old one, and both stay.
