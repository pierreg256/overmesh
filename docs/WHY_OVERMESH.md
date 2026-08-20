# Why Overmesh

> No Region Left Behind.
> No Storage Keys Required.

---

**Status: measured prototype rationale.**

This document explains the current system and its measured tradeoffs. It is
descriptive rather than normative: specifications define behaviour and
accepted decision records constrain implementation. Performance figures below
come from retained signed evidence rather than estimates.

The references are
[`OVERMESH_V1_SPECIFICATION.md`](../OVERMESH_V1_SPECIFICATION.md) and
[`OVERMESH_DEVELOPMENT_HARNESS_SPECIFICATION_V1.md`](../OVERMESH_DEVELOPMENT_HARNESS_SPECIFICATION_V1.md),
which define behaviour, and the decision records in [`adr/`](adr/README.md),
which explain why it is what it is. Where this document and any of those
disagree, they are right and this is out of date.

---

## 1. What this document is

This is the rationale for Overmesh: the problem it addresses, the Azure
capabilities it does not duplicate, what it costs, and what it deliberately
does not do.

It is written for architects and engineers who already run Azure Blob Storage
at scale. It assumes you know what LRS, ZRS, GRS, and GZRS are.

Overmesh is an advanced prototype. It is published to start a conversation and
to be forked. Section 10 states precisely what has been validated and what has
not. Nothing in this document should be read as a production readiness claim.

The normative design lives in
[`OVERMESH_V1_SPECIFICATION.md`](../OVERMESH_V1_SPECIFICATION.md) and
[`OVERMESH_DEVELOPMENT_HARNESS_SPECIFICATION_V1.md`](../OVERMESH_DEVELOPMENT_HARNESS_SPECIFICATION_V1.md).
The reasoning behind individual choices — including several that were reversed
during development — is in [`adr/`](adr/README.md).

## 2. The problem

Three limits of Azure Storage compound at enterprise scale. Each is a
documented product behavior, not a defect.

**A storage account has a ceiling.** A standard general-purpose v2 account is
capped at five petabytes of capacity and 20,000 requests per second. Ingress
and egress limits can be raised by support request; capacity and request rate
are the hard boundary. Past that point you do not tune an account — you add
accounts.

**You do not choose the secondary region.** For GRS and GZRS, the secondary
region is derived from the primary region and cannot be changed. If data
residency, contractual commitments, or latency require a specific second
region, Azure geo-redundancy has no answer. You either accept the assigned
pair or you build the replication yourself.

**Failover is an event, and it is destructive.** A customer-managed unplanned
failover converts the storage account to LRS in the new primary region and
deletes the copy in the original primary region. Re-establishing geo-redundancy
afterwards is a separate operation that costs time and money. Meanwhile the
timing of a platform-initiated failover is not yours to decide. Customers
routinely raise both points: they have no hand on the switch, and no control
over how long it takes.

The organisations that hit these limits have already solved the first one by
hand. They shard across accounts in application code. Overmesh exists because
that hand-rolled solution is harder than it looks, and because solving it once
in front of the accounts is cheaper than solving it badly in every application.

## 3. What Azure already provides, and where it stops

| Option | Protects against | Secondary is | Cross-region RPO | Solves capacity | Endpoint |
| --- | --- | --- | --- | --- | --- |
| LRS | Disk, rack | — | — | No | One account |
| ZRS | Zone loss | — | — | No | One account |
| GRS / GZRS | Region loss | Assigned, not writable, not readable | Typically < 15 min, no SLA | No | One account |
| RA-GRS / RA-GZRS | Region loss | Assigned, readable, stale | Typically < 15 min, no SLA | No | One account |
| Geo priority replication | Region loss | Assigned | SLA: lag ≤ 15 min for 99% of the month | No | One account |
| Object replication | Region loss | Chosen, per container | Asynchronous, no lag guarantee | No | Separate endpoints |

Two observations follow.

Every geo option is **asynchronous**. Even geo priority replication, which adds
a service level agreement on replication lag, remains asynchronous by design;
an RPO of zero is not achievable through it. Under sustained write load the lag
is exactly when you least want it.

None of them addresses **capacity behind a single endpoint**. Object
replication lets you choose the destination and is the closest fit, but it is
per-container, asynchronous, and leaves you with two distinct account
endpoints — which pushes routing back into the application, which is the
problem you were trying to avoid.

ZRS deserves a specific note, because it is the honest benchmark for write
availability: a single ZRS account keeps accepting writes through the loss of
an availability zone. Overmesh, as described below, does not. Section 8 states
that trade explicitly.

## 4. What enterprises do instead

The usual answer is application-level sharding with a dual write: pick a naming
convention, route to account A or B in code, write to both, move on.

It works until the first partial failure. Then the following problems arrive at
once, and they are distributed-systems problems, not application problems:

- **Atomicity.** The write succeeded on A and failed on B. What is the state of
  the object now, and what does the reader see?
- **Ambiguity.** The client timed out. The write may or may not have committed.
  Retrying may create a second version.
- **Idempotency.** A retry must produce the original outcome, not a new one —
  which requires a stable write identity the SDK does not provide by default.
- **Divergence detection.** Nothing tells you that A and B stopped agreeing.
  You find out when a reader gets the wrong bytes.
- **Repair.** Once they diverge, which side is authoritative, and how do you
  prove it before overwriting the other?

In practice, very few application teams solve these correctly. The teams that
do solve them once, for one application, in one language, with no artefact that
demonstrates the two copies agree. The next application starts over.

Overmesh's position is that this belongs in front of the storage accounts, once,
in a component whose only job is to get it right.

## 5. What Overmesh is

A stateless gateway that presents an Azure Blob-compatible surface in front of
several Azure Storage Accounts.

**Placement comes from a signed Ring.** The Ring is a document, not a service:
it declares the accounts, their regions, and their weights, and it is signed
with an ECDSA P-256 key. Placement is consistent hashing over weighted virtual
nodes, with the two replicas of any object forced into distinct regions. Adding
an account is a Ring revision. **The client endpoint never changes.**

**Writes are a strict two-replica commit.** The gateway resolves both replicas,
takes a per-blob lease, streams the content to both accounts, publishes signed
block and `PREPARED` manifests, then publishes the signed `COMMITTED` head to
both replicas under a conditional compare-and-swap, and records an immutable
signed high-water mark. A write is acknowledged only when both replicas carry
the same committed logical version. A failure is reported as a failure — and
when the outcome is genuinely ambiguous, it is reported as ambiguous, with a
write identity the client can retry idempotently.

**Reads validate before they return bytes.** The gateway loads and verifies the
signed head on both replicas, checks it against the high-water record, resolves
the requested range to the block manifest pages it actually needs, and verifies
each block's SHA-256 against the signed manifest before a single byte reaches
the client.

**A separate reconciler owns repair.** It runs under its own managed identity,
never handles client traffic, validates the complete signed object graph and
content digests on both replicas, repairs `MISSING` and provably authoritative
`DRIFTED` replicas, quarantines anything tampered, and garbage-collects
superseded and tombstoned generations after a retention delay.

**The client changes one thing: the endpoint.** Same Azure SDK, same
`DefaultAzureCredential`, same Entra token for the standard
`https://storage.azure.com/` audience. No application change, no new library,
no proprietary protocol.

## 6. What it buys

**Capacity behind one endpoint.** Overmesh federates N storage accounts into a
single logical namespace. Capacity and request rate scale with the number of
accounts, not with the ceiling of one. Adding an account is a signed Ring
revision, and the endpoint that applications are configured with does not move.
This is the primary reason the project exists.

One qualification for V1: the Ring is fixed for the life of a deployment.
Objects are located through the active Ring, so revising it before the
migration path exists would leave existing objects unreachable. A V1 deployment
therefore sizes its account set up front; growing it in place is a post-V1
capability, and it is the one that turns this from an architectural property
into an operational one.

**You choose the regions.** The Ring declares them. Nothing forces you onto an
assigned pair. If residency requires France Central and Sweden Central, or if
latency requires a specific neighbour, that is a line in a YAML document rather
than a limitation to work around.

**There is no failover event.** Both replicas are always live and always
writable. Losing a region degrades the system — writes stop until hinted
handoff lands, see section 8 — but it does not convert an account to LRS, does
not delete a copy, and does not require you to re-establish and re-pay for
geo-redundancy afterwards. Recovery is the reconciler repairing replicas, not
an irreversible account operation.

**Adoption cost is close to zero.** This matters more than it sounds. Teams do
not adopt storage infrastructure that requires them to change code. Overmesh
requires an endpoint change, and nothing else. Just as importantly, it adds no
administration layer: authorization for customer data is the caller's own Azure
RBAC, enforced by Azure Storage itself on both replicas. There is no Overmesh
permission model to maintain, no mapping table, no second source of truth for
who can read what.

**Side effects worth having.** Because the federation metadata is the source of
truth for consistency, it must not be forgeable — so it is signed, and Overmesh
uses no account keys, no connection strings, and no shared access signatures of
any kind. A useful consequence is that acknowledged writes have a cross-region
RPO of zero, which asynchronous geo-replication cannot offer. None of these are
the reason to adopt Overmesh; they are what falls out of building the
federation layer correctly.

## 7. Sizing the Ring

This is where the interesting engineering lies, and where the design invites
scrutiny.

Replication factor is fixed at two in V1. Capacity therefore scales with the
number of accounts while every object keeps exactly two copies. The question is
not how many accounts, but **how many regions** those accounts span.

With all accounts in two regions, losing one region removes one replica of
*every* object. The system is intact and readable, but it is running at a
single copy until repair completes — and repair means re-reading the entire
dataset through the reconciler.

With three or more regions, consistent hashing spreads the pairs. Losing one
region affects only the objects that happened to be placed there, and the
repair volume drops proportionally.

| Accounts | Regions | Objects losing a replica when one region fails | Repair volume |
| --- | --- | --- | --- |
| 2 | 2 | 100% | Full dataset |
| 4 | 2 | 100% | Full dataset |
| 6 | 3 | ~67% | ~2/3 of dataset |
| 8 | 4 | ~50% | ~1/2 of dataset |

The figures above follow from the placement rule — two replicas in distinct
regions, chosen by hash — and are approximate: exact distribution depends on
weights and on the virtual-node count.

The practical guidance is that two regions is the minimum viable topology and
three is the first one that behaves well under regional loss. A V2 with a
configurable replication factor changes this calculus again, and is a natural
place for a contributor to start.

The behaviour described here is exercised, not assumed. The local harness runs
a three-node, three-region Ring and asserts that each object's head lands on
exactly its two assigned accounts and is absent from the third, then takes one
account offline and confirms that only the objects placed on it become
unwritable while the rest keep committing normally. See
`harness/scripts/placement-smoke.sh`.

## 8. What it costs

Nothing here is hidden. Each line states whether it is inherent or scheduled.

| Cost | Magnitude | Status |
| --- | --- | --- |
| Storage | 2×, plus uncollected generations | Inherent to RF=2 |
| Write latency | 49 backend requests and 3 Key Vault signatures per first PUT | Measured; request-budgeted |
| Read latency | 10 requests for `HEAD`, 15 for nominal `GET` and range reads | Measured after ADR-0011 |
| Listing | ~20,000 backend reads for a 5,000-entry page | Reduced 2.5× in 0.8; going further is a trade, not a fix |
| Write availability | Writes stop while either region is unavailable | Hinted handoff, post-V1 |
| Reconciliation | O(dataset) per cycle | Merkle trees in V2 |
| Operations | 2+ accounts, private endpoints, 3 managed identities, container-scoped RBAC, Key Vault, signed Ring distribution, reconciler scheduling, continuous RBAC posture auditing | Inherent |

The write-availability line deserves emphasis, because it is the one place
where Overmesh is currently *worse* than the alternative it replaces. A single
ZRS account survives the loss of an availability zone and keeps accepting
writes. Overmesh with a strict two-replica commit stops accepting writes if
either region is unavailable. That is a deliberate exchange — cross-region
durability and a provable consistency state, at the price of write
availability — and hinted handoff is the planned way to buy the availability
back without giving up the guarantee.

### The measured performance price

The final signed 0.10.1 campaign ran the same Azure SDK operations directly
against Storage and through Overmesh from an isolated France Central VM.
Writes use 30 measured operations per case; reads use 240 so their percentiles
are useful signals. There were zero client errors, zero backend transport
failures, and zero unattributed requests in every case. Front Door selected the
France Container App for the entire run, so these figures describe a France
client and its lowest-latency healthy origin, not balanced multi-origin traffic.

| Operation | Direct p50 | Overmesh p50 | Overmesh requests |
| --- | ---: | ---: | ---: |
| `Put Blob`, 1 KiB, c1 | 9.08 ms | 1,255.49 ms | 49 |
| `Put Blob`, 1 MiB, c1 | 20.06 ms | 1,379.71 ms | 49 |
| `Put Blob`, 16 MiB, c1 | 141.54 ms | 2,986.14 ms | 49 |
| `Delete Blob`, 1 KiB, c1 | 9.52 ms | 1,010.35 ms | 43 |
| `Get Blob`, 1 MiB, c1 | 15.36 ms | 230.09 ms | 15 |
| `Get Blob`, 16 MiB, c1 | 110.11 ms | 813.86 ms | 18 |
| `Range Get`, 1 MiB of 16 MiB, c1 | 15.74 ms | 276.78 ms | 15 |
| `Head Blob`, 1 MiB, c1 | 5.92 ms | 93.62 ms | 10 |

The 1 KiB and 1 MiB writes cost nearly the same despite a thousand-fold payload
difference, and DELETE pays the same order of latency while carrying no body.
The dominant cost is therefore fixed backend-request amplification, not CPU or
bulk throughput. Overmesh buys synchronous cross-region durability, signed
state validation and repairability at a severe small-operation latency price:
about 130× direct Storage for the measured 1 KiB PUT.

The deterministic request counts are the blocking non-regression measure for
0.11. Latency remains a signal because geography, Azure scheduling and network
variance are outside the code's control.

The added overwrite cases also close the locking question. A first `PUT` and an
established overwrite both cost 49 requests today, including three lock
requests. The difference is the conditional create result: `201` for the new
lock object, `409` when it already exists. Trying the lease first would reduce
the established path from three lock requests to two but raise the first-write
path to four. It is therefore a workload-policy decision for 0.11, not a global
optimization.

The listing line is the largest number in this table and deserves a word,
because it is not simply unfinished work. Each listed entry is validated across
both replicas and against its committed head before it is returned. Removing
either check halves the cost and buys a class of incorrect result — serving an
entry a reconciler has not yet repaired, or reporting a version that was never
committed. Write ordering does not resolve it: two objects that must agree
require a check at read time. The 0.10 performance baseline is where that trade
gets priced.

## 9. What Overmesh does not do

**Subscription administrators are inside the trust boundary.** Anyone who can
alter RBAC assignments, network configuration, or the storage accounts
themselves can defeat the model. Overmesh detects unapproved data-plane role
assignments on its system container and fails closed, but it does not and
cannot defend against the platform's own administrators.

**Content tampering is detected, not prevented.** Callers write their own bytes
to the customer container with their own credentials — that is what preserves
Azure RBAC as the single authorization authority. Direct modification of those
bytes is therefore possible for anyone already authorized to write them. It is
caught by block-level hash validation on read and by full reconciliation, and
the recovery path is Azure blob versioning and soft delete, which the
deployment is required to enable.

**One logical account per gateway, today.** The logical account is a deployment
setting. Multi-account hosting behind one gateway is post-V1.

**Authorization is granular to the container, not to the path.** Caller
authorization is the caller's own Azure RBAC, and container-scoped assignments
behave exactly as they do against Blob Storage. Role assignment conditions
whose predicate depends on the blob path are refused: content is written under
a derived key that no path predicate can match, so such a condition would be
enforced on reads and silently bypassed on writes, and a partially enforced
access rule is worse than an absent one. Path-independent conditions —
`@Environment`, `@Principal` — are unaffected, and because logical containers
map one-to-one onto backend containers, separation by container works with full
fidelity.

**Usable blob names are shorter than Azure's.** The catalogue that backs
listing encodes the name at two characters per byte and is bound by the same
1,024-character backend limit, so the usable budget is roughly 48% of Azure's
for ASCII names and less for other scripts. Parity is tracked as debt to be
reopened before 1.0. The published matrix carries the exact figures.

**The API surface is a published subset, not the whole of Azure Blob.** `PUT`,
`GET`, `HEAD`, `DELETE`, conditional requests, ranged reads, container and blob
listing with delimiters and continuation, `Put Block`, `Put Block List`, and
`Get Block List` are implemented. Client compatibility across the Azure SDKs,
the CLI, and AzCopy is validated live in milestone 0.9. The compatibility
matrix is explicit: anything not on it is rejected with an Azure-compatible
error rather than silently approximated.

**It is not a quorum system.** No RF≥3, no configurable quorums, no read
repair, no vector clocks, no active-active multi-master. V1 implements one
fixed two-replica commit protocol and says so.

## 10. Status

Overmesh is an advanced prototype in milestone 0.10.1 of a V1 plan that reaches
1.0.

**Implemented and tested:** the signed Ring with rollback and predecessor
validation, Entra-only authentication with explicit Shared Key and SAS
rejection, credential separation between caller and control identities enforced
by the type system, the strict two-replica commit with idempotent retry and
partial-publication recovery, validated `HEAD` and `GET` with block-level
verification and range pushdown, signed tombstones with retention, garbage
collection of superseded and deleted generations, container and blob listing
over a signed catalogue with delimiters and signed continuation tokens, the
block staging APIs with their own retention and collection, reconciliation with
repair and quarantine, and continuous RBAC posture auditing against Azure
Resource Manager.

**How it is validated:** 192 unit and integration tests, 23 declarative
scenarios against an independent reference model, three process-level suites
and a Rust system validator running against Azurite backends behind a fault
proxy, and a live Azure gate that verifies account posture, authorization
revocation, three-account RF=2 placement, single-account outage isolation,
repair, quarantine, administrator-authorised recovery, retention-backed
collection, and the Azure SDK .NET/Python/JavaScript, Azure CLI, and AzCopy
clients.

Why decisions were taken the way they were — including several that were
reversed during development — is recorded in
[`docs/adr/`](adr/README.md). That is the first thing to read if the design
looks surprising.

Placement across a multi-account, multi-region Ring is part of that local
suite. A three-node topology is exercised on every run: object placement is
asserted account by account, and a single-account outage is shown to affect
only the objects assigned to it.

**Demonstrated live.** Milestone 0.9 was exercised against real Azure
resources: three Storage Accounts in France Central, Sweden Central and Norway
East behind a signed RF=2 Ring, with Gateway and Reconciler deployed from
immutable image digests.

- **Authorization.** A denied principal is refused `403` on `Put Blob` and
  `Put Block`. After revoking write permission from the *original* principal,
  its idempotent replay and its `Put Block List` are refused `403`. Permissions
  were restored and canary resources removed.
- **Placement and outage.** Twenty-seven checks, no failures. Each committed
  head is present on exactly its two assigned accounts and absent from the
  third. With one account offline, objects placed on it become unwritable while
  objects placed elsewhere keep committing.
- **Clients.** Azure SDK for .NET, Python and JavaScript, Azure CLI and AzCopy,
  covering writes, block staging, reads, `HEAD`, listing, deletes and large
  objects.
- **Posture.** The nominal three-account ARM posture passes. A deliberately
  inherited account-level Blob role for an unapproved principal and a
  deliberately path-dependent ABAC condition both make the audit fail closed;
  the temporary assignments are then removed and nominal posture revalidated.
- **Reconciliation.** A missing replica is repaired from its validated peer,
  altered physical content is quarantined rather than repaired, explicit
  administrator recovery restores the selected healthy replica, and a
  superseded canary generation is retained until the configured test deadline
  before collection.

The capacity argument of section 6 is therefore measured against real Azure, in
the three-region topology section 7 recommends, rather than only designed.

The repository retains a deterministically redacted canonical bundle with
per-source hashes and a detached ES256 signature from a non-exportable Key
Vault key. The unredacted archive is retained privately on all three validation
accounts, and its hash is linked from the canonical bundle. This is the same
verification model the system claims for its own metadata:

```text
Runtime commit  26449d7e5775ac9d28dea38182f509c7528c57c3
Bundle SHA-256  547172399a2bc24ab494b41c9dd37e9b2ceaa054e6e37a17960e1c7e5e244bc9
```

A signed 0.10.1 performance baseline now measures the fixed request
amplification described in section 8, with attributed object-class budgets and
the read-stabilized blocking `live-v3` reference used by 0.11.

## 11. Where to start if you want to fork it

The three highest-value contributions, in order:

1. **Listing throughput.** Four backend reads per entry, twenty thousand for a
   default page. The two candidate reductions each buy a class of incorrect
   result rather than costing only engineering, so the work is to decide what a
   listing must prove — as opposed to what `GET`, `HEAD` and the reconciler
   already prove — and then to measure it. See ADR-0008.
2. **Catalogue correctness and name parity.** Reject names that cannot be
   catalogued before writing content, bring catalogue publication into the
   conditional commit sequence, and add randomised order-preservation tests
   before changing the encoding. See ADR-0004 and ADR-0008.
3. **Write-path latency.** Forty-nine backend requests and three Key Vault
   signatures per first write is the measured number that will decide whether
   this is usable. It is also the most tractable engineering problem in the
   repository.

## Sources

- [Scalability and performance targets for standard storage accounts](https://learn.microsoft.com/en-us/azure/storage/common/scalability-targets-standard-account)
- [Blob Storage scalability and performance targets](https://learn.microsoft.com/en-us/azure/storage/blobs/scalability-targets)
- [Data redundancy — Azure Storage](https://learn.microsoft.com/en-us/azure/storage/common/storage-redundancy)
- [How Azure Storage account customer-managed unplanned failover works](https://learn.microsoft.com/en-us/azure/storage/common/storage-failover-customer-managed-unplanned)
- [Azure Storage geo priority replication](https://learn.microsoft.com/en-us/azure/storage/common/storage-redundancy-priority-replication)
- [Azure region pairs and nonpaired regions](https://learn.microsoft.com/en-us/azure/reliability/regions-paired)
