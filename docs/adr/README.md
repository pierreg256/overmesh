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
| [0004](0004-immutable-content-naming-in-the-customer-container.md) | Immutable content naming in the customer container | accepted | 0.5.0 → 0.8.0 | yes |
| [0005](0005-authorization-probes-for-metadata-only-operations.md) | Authorization probes and the resource they check | accepted | 0.5.0 → 0.8.0 | yes |
| [0006](0006-placement-through-a-signed-ring-document.md) | Placement through a signed Ring document | accepted | 0.2.0 → 0.6.0 | yes |
| [0007](0007-canonical-logical-resource-identity.md) | Canonical logical resource identity | accepted | 0.5.0 → 0.8.0 | yes |
| [0008](0008-listing-from-a-signed-catalogue.md) | Listing from a signed catalogue | accepted | 0.8.0 | partial |

## Implementation status

All runtime implementation items identified by the accepted records are
present in the current tree. The live authorization gate has been executed
against the deployed Gateway with controlled write-role revocation and
restoration, and the milestone 0.9 release evidence is signed.

Implementation-status sections inside accepted records remain the historical
snapshot from acceptance; the index above is the current implementation state.

0008 carries the one outstanding item: property tests for the catalogue
encoding, asserting round-trip and order preservation over random byte strings.
They are a prerequisite for any cheaper encoding, because an ordering bug there
corrupts pagination silently.

Two items are scheduled rather than pending. Parity with Azure on blob-name
length is to be reopened **before 1.0**; if the encoding changes, the derived
bound changes in `LogicalBlobId::parse`, in 0007, and in
[`COMPATIBILITY.md`](../../COMPATIBILITY.md). Reducing listing below four
backend reads per entry is a policy question for the **0.10** performance
baseline, not an ordering fix.

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
