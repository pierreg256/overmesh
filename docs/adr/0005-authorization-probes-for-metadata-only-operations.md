# ADR-0005 — Authorization probes and the resource they check

- **Status:** accepted
- **Date:** 2026-08-17
- **Milestone:** 0.5.0 → 0.8.0
- **Supersedes:** —
- **Superseded by:** —

> **Corrections, 2026-08-17.** This record was reviewed twice against the code
> and was wrong twice. Both errors are recorded rather than edited away.
>
> *First:* it claimed `Put Block List` performs no caller-authorized write and
> decided to add a probe. In fact the commit path reassembles the selected
> blocks and calls `put_blob_locked`, which writes the content object to both
> replicas with the caller's token. The escalation scenario described was not
> reachable. The error came from auditing credential use by searching for
> `caller_*` backend methods; `put_file_idempotent` wraps `caller_put_data_file`
> and hid the boundary.
>
> *Second:* the corrected version then claimed no write probe exists at all.
> `authorize_existing_blob_write` exists and is used by every idempotent replay
> path. It is documented in its own section below, and the governing principle
> is restated in terms of **which resource** is checked — which is what the two
> errors were really about.

## Context

ADR-0003 splits identities by object class: the caller's token for customer
data, dedicated managed identities for Overmesh's control objects, so that
Azure RBAC remains the sole authorization authority.

For that to hold, every client-facing operation must reach Azure with the
caller's credential at some point. Most do, incidentally: a `Put Blob` writes
content, a `Get Blob` reads it, and Azure refuses an unauthorized caller
without Overmesh deciding anything.

Some operations never touch customer bytes. Their whole effect lives in the
system container under an Overmesh identity:

- `DELETE` publishes a signed tombstone.
- `HEAD` answers from the signed manifest; §13.1 says it does not read the body.
- `List Blobs` is served from the signed catalogue.
- `Get Block List` is served from signed staged metadata and the block manifest.
- An **idempotent replay** returns an existing outcome and writes nothing at all.

Without a deliberate check, nothing would verify the caller for any of these.

## The constraint that shapes the answer

Azure Storage has no dry-run. Asking "may this principal do this" requires
doing something. The available choices differ per `DataAction`:

- `blobs/read` has a naturally non-mutating operation: `HEAD` a blob.
- `blobs/delete` has one: `DELETE` a snapshot that does not exist.
- `blobs/write` has none. Every operation carrying it writes something —
  **unless the target is known to already exist**, in which case a conditional
  create is refused by its own precondition.

## Options considered

**Probe everything uniformly.** Doubles the round trips on operations Azure
already arbitrates, and forces a side-effecting probe onto the write path.

`Put Block` as a generic write probe was briefly adopted and is rejected.
`Put Block` against a blob that does not exist creates a zero-length block
blob, returned by `List Blobs` with `include=uncommittedblobs`, and uncommitted
blocks survive a week. As a probe it would leave a phantom blob per replica on
the logical name, inside the customer's container, interacting with the listing
surface delivered in 0.8.

**A conditional create on a resource that does not exist.** `If-None-Match: *`
or an impossible `If-Match` against the logical blob path. The logical path
never physically exists in Overmesh, so `If-None-Match: *` would *succeed* and
create a phantom blob at the real logical name — worse than the block probe.

**A conditional create on a resource that is known to exist.** Refused by its
own precondition, so nothing is written. Available only where the target is
known present, which is exactly the replay case.

**Probe only where nothing else checks, on the resource that matters.** Chosen.

## Decision

**Probe an operation when no caller-authorized data-plane operation carrying
the same `DataAction` reaches Azure — and check it against the logical
resource wherever the two differ.**

Each probe uses the same HTTP verb as the operation it guards and targets the
exact canonical logical resource — **with one exception**: the idempotent
replay probe targets the physical content object rather than the logical path,
because it is the only target known to exist and therefore the only one a
conditional create can test without writing. That exception is what the
*Authorization granularity* section below is about. Probes are fail-closed:
only explicitly documented terminal statuses count as authorization.

| Operation | `DataAction` | Caller reaches Azure via | Probe |
| --- | --- | --- | --- |
| `Put Blob` | `blobs/write` | content object write | none |
| `Put Block` | `blobs/write` | staged content write | none |
| `Put Block List` | `blobs/write` | reassembled content write via `put_blob_locked` | none |
| Idempotent replay | `blobs/write` | **nothing — no write occurs** | conditional create on the existing content object |
| `Get Blob` | `blobs/read` | ranged content reads | `HEAD` on the logical blob |
| `HEAD` | `blobs/read` | nothing | `HEAD` on the logical blob |
| `Get Block List` | `blobs/read` | nothing | `HEAD` on the logical blob |
| `Delete Blob` | `blobs/delete` | nothing | `DELETE` of a non-existent snapshot |
| `List Blobs` | `blobs/read`, list suboperation | nothing | `List Blobs` with `maxresults=1` |

`List Containers` applies the container-list probe to each candidate container
and returns only those the caller can actually enumerate.

### Idempotent replays

A replay writes nothing: it recognises that the requested write is already the
committed head and returns the existing outcome. There is therefore no
caller-authorized operation, and it needs a check like any metadata-only
operation.

`authorize_replay` performs one, in two parts. It reads the committed content
object with the caller's token, and it attempts a conditional create —
`If-None-Match: *` — on that same object, accepting `409` or `412` as
authorization. Because the object is known to exist, the precondition
necessarily fails and nothing is written.

It additionally refuses to return an existing outcome to a principal other than
the one recorded in the committed manifest, which is a second, independent
binding.

**This probe carries an ordering dependency and it must be pinned.** It reads
`409`/`412` as "authorized". If Azure ever evaluated the precondition before
authorization, an unauthorized caller would receive `409` and be accepted. That
ordering is real today and is not contractual. Live conformance must assert
that a **denied principal replaying a committed write receives `403`, not
`409` or `412`**, for every supported Storage API version. Without that case
the check is an assumption, not a control.

### Authorization granularity, and why writes are the exception

Every read-side check in the table targets the **logical resource**. Every
write-side check targets the **derived content object** at
`.overmesh/objects/{path_hash}/{uuid}` — see ADR-0004.

Under role assignments scoped at container level, the two are equivalent: both
resources live in the same container, so authorizing one authorizes the other,
and Azure's decision is the same either way.

Under an attribute-based condition whose predicate depends on the blob path,
they are **not** equivalent. `path_hash` is a SHA-256 and matches no path
predicate, so ADR-0004 requires the administrator to authorize
`.overmesh/objects*` for the deployment to function at all. That grant is blind
to which logical blob the content belongs to.

The consequence is an asymmetry: a condition restricting a caller to
`finance/*` **is** honoured on read, `HEAD` and delete, which probe the logical
path — and is **not** honoured on write or on listing. A partially enforced
access-control condition is worse than an unsupported one, because it tests as
working.

**Therefore: role assignment conditions whose predicate depends on the blob
path are declared unsupported where they are effective on a customer
container**, including through inheritance from a higher scope.

The declaration is to be enforced rather than merely documented. The
reconciler's RBAC posture audit already enumerates every container per account
and records each assignment's condition and condition version; it will fail
closed on such an assignment, in the same manner as the existing
system-container check. Until that check ships, the restriction is documented
but undetected — see *Implementation status*.

This is not a narrowing of ABAC generally. `@Environment` conditions — private
endpoint, subnet, time — and `@Principal` conditions are path-independent and
compose normally. Container-scope assignments, the primary mechanism, are
untouched. And because Overmesh maps logical containers one-to-one onto backend
containers, separation by container works with full fidelity, which is the
conventional Azure pattern and predates ABAC.

## Consequences

Azure remains the authorization authority for every client-facing operation,
including those touching no customer bytes, and no Overmesh-side permission
model is introduced.

No probe writes anything into a customer container. The only write-side probe
targets an object that already exists and is refused by its own precondition.

**The absence of a probe is itself a claim requiring live coverage.** Every row
reading "none" asserts that Azure refuses an unauthorized caller through the
data operation. That assertion deserves the same treatment as the probes: a
denied principal calling `Put Blob`, `Put Block` and `Put Block List` must
receive `403 AuthorizationPermissionMismatch`. The absence of those cases is
how the first correction above became necessary.

**Credential use must be greppable.** `put_file_idempotent` and
`put_bytes_idempotent` are named for what they do rather than for which
credential they carry, which is what made the first analysis wrong. Renaming
them `caller_put_file_idempotent` and `control_put_bytes_idempotent` makes the
boundary visible to a plain search, in a codebase whose security argument rests
on that boundary.

## Implementation status

Complete. The live denied-principal cases were executed in milestone 0.9 and
are retained as evidence.

Their setup is recorded here because it is not obvious, and because anyone
re-running or extending them will hit the same traps. Each case must reach the
Azure call being tested rather than failing earlier inside Overmesh:

- `Put Blob` and `Put Block` — a principal without write permission reaches the
  content write directly. No special setup.
- `Put Block List` — a denied principal with no staged blocks fails on
  `MissingBlock` before any Azure write. The case must either stage blocks as
  the principal and then revoke its write permission, or commit a selection of
  already-`Committed` blocks with a read-only principal. Both reach
  `put_blob_locked` and must yield `403`.
- **Idempotent replay** — a *different* principal is refused by
  `committed.caller != principal.identity()` and returns `409`, never reaching
  Azure. The case must use the **original** principal with its **write**
  permission revoked and its **read** permission retained, since
  `authorize_replay` reads the content object before probing. It must assert
  `403`, not `409` or `412`.

The RBAC posture audit now fails closed on a path-predicate condition
effective on a customer container, including one inherited from a higher scope,
and that behaviour was exercised live against real ARM assignments. The
idempotent-write helpers carry their credential in their names.

## When to revisit

Path-based ABAC support and content naming are **one question, not two**. The
`{blob}/.overmesh/{uuid}` candidate in ADR-0004 would place content under a
path a condition can match, and writes would honour path predicates again.
Reopening either reopens both.

Even then, listing would still not honour path predicates, because it is served
from the catalogue behind a container-level probe. Full path-granular
authorization would additionally require per-entry authorization at
enumeration — the same cost being reduced in the listing path. Complete ABAC
support and fast listing pull in opposite directions, and neither should be
promised without the other being priced.

If Azure introduces a dry-run or permission-evaluation API, every check becomes
side-effect-free and this decision simplifies to "probe everything, on the
logical resource".

## Verified by

- `harness/environments/azure/validate-storage-authorization.sh` — `HEAD` on an
  absent blob and `DELETE` of an absent snapshot, each with an allowed and a
  deliberately denied principal, across supported Storage API versions
- `gateway/src/backend.rs::delete_authorization_probe_statuses_fail_closed` —
  only the documented terminal status counts as authorized
- `gateway/src/commit/tests.rs::put_blob_returns_forbidden_after_attempting_the_caller_data_write`
  — caller-authorized content writes fail closed
- `gateway/src/commit/tests.rs::put_blob_replay_reauthorizes_and_rejects_a_different_caller`
  — idempotent replay rechecks the caller's authorization
- `reconciler/src/posture.rs::rejects_unapproved_system_container_access` —
  posture validation fails closed on unapproved assignments
- `gateway/tests/auth_contract.rs::executes_declarative_gateway_authentication_contract`
  — the declarative authentication contract exercises denied principals
- `harness/src/identity.rs::local_runtime_principals_are_distinct` — local
  caller, gateway, reconciler and denied principals remain distinguishable
- `harness/artifacts/live/0.9.0/gateway-auth-v090-live-evidence.txt` — the live
  run: a denied principal refused `403` on `Put Blob` and `Put Block`, and the
  original principal refused `403` on idempotent replay and `Put Block List`
  after its write permission was revoked and before it was restored
- `harness/artifacts/live/0.9.0/posture-v090-live-evidence.json` — a
  deliberately inherited account-scope role and a deliberately path-dependent
  condition each make the posture audit fail closed, with baseline and cleanup
  snapshots hashing identically
