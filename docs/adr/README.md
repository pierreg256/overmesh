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
| [0009](0009-redaction-policy-for-retained-live-evidence.md) | Redaction policy for retained live evidence | accepted | 0.9.0 → 0.9.1 | yes |
| [0010](0010-keep-reconciler-safety-state-on-the-read-path.md) | Keep Reconciler safety state on the read path | accepted | 0.10.1 | yes |
| [0011](0011-use-physical-content-heads-for-read-authorization.md) | Use physical content HEADs for read authorization | accepted | 0.10.1 | yes |
| [0012](0012-consolidate-gateway-owned-commit-state.md) | Consolidate gateway-owned commit state into one document | accepted | 0.11.0 | no |
| [0013](0013-metadata-reads-stay-at-two-replicas.md) | Metadata reads stay at two replicas | accepted | 0.11.0 | yes |

## Implementation status

Runtime decisions in the accepted records are implemented except for debt
called out explicitly below. The live authorization gate has been executed
against the deployed Gateway with controlled write-role revocation and
restoration. Signed retained evidence now includes the 0.9 release gates, the
certified 0.11 `live-v4` request-budget baseline, and the failed-closed
`live-v5` diagnostic.

The retained run covers authorization refusal and revocation, negative ARM
posture mutations, three-account RF=2 placement with single-account outage
isolation, the standard clients, and the Reconciler's repair, quarantine,
administrator recovery and retention-backed collection paths.

Implementation-status sections inside accepted records are maintained when
later work changes the implementation of that decision. The index above is the
compact current state; the record remains the detailed source.

0008 carries one outstanding item: property tests for the catalogue encoding,
asserting round-trip and order preservation over random byte strings. They are
a prerequisite for any cheaper encoding, because an ordering bug there corrupts
pagination silently.

0009 is implemented. The canonical 0.9.0 evidence is deterministically redacted
before signing, the detached signature covers those published bytes, the raw
archive is privately retained and replicated three ways, and `doc-check` R8
rejects unredacted Azure identifiers under `harness/artifacts/`.

0010 keeps quarantine and compaction floors as explicit replicated read-path
controls until a replacement protocol defines identity ownership and freshness.
0011 removes the redundant logical read probes from `HEAD` and `GET`; the
caller-authorized physical content `HEAD` now also carries the Azure RBAC check.

0012 is the only accepted record that is not fully implemented. It merges the
gateway-owned head, high-water, prepared and terminal state into one document.
Its canonical-lease prerequisite is implemented: Gateway and Reconciler route
the lease to the deterministic primary for every canonical logical blob,
including an anomalous head discovered on the secondary. Its amendment to
0010 — reading Reconciler-owned safety state once per request rather than
twice — is separable and does not require the merge.

0013 records a decision not to change anything: metadata continues to be read
from both replicas. It exists because 0002's admissibility table permits
`W = 2, R = 1`, and because 0002 itself observes that performance work drifts
toward that configuration.

Two items are scheduled rather than pending. Parity with Azure on blob-name
length is to be reopened **before 1.0**; if the encoding changes, the derived
bound changes in `LogicalBlobId::parse`, in 0007, and in
[`COMPATIBILITY.md`](../../COMPATIBILITY.md). Reducing listing below four
backend reads per validated entry remains an explicit policy question because
it changes what listing proves. The 0.11 implementation preserves those four
reads while scheduling validations with bounded ordered concurrency and
skipping validation for descendants of an already emitted hierarchical prefix.

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
