# ADR-0003 — Separate caller and control identities

- **Status:** accepted
- **Date:** 2026-08-15
- **Milestone:** 0.5.0
- **Supersedes:** —
- **Superseded by:** —

## Context

The governing principle is that **Overmesh must not become an authorization
authority.** Azure RBAC remains the single source of truth for who may read and
write customer data. Overmesh federates storage; it does not decide who is
allowed to use it.

Two consequences follow, and they are the reason the principle is worth stating
before the decision.

*Adoption.* An application adopting Overmesh changes its endpoint and nothing
else. Same SDK, same `DefaultAzureCredential`, same Entra token for the
standard `https://storage.azure.com/` audience. If Overmesh required its own
tokens, its own scopes, or its own application registration, every consuming
application would need code changes and the adoption argument would collapse.

*Attack surface and administration.* Every additional permission model is
another place where access can be granted by mistake, another artefact to audit,
and another thing an enterprise administrator must keep synchronised with the
identity system they already run. A mapping table of "who may read which blob",
maintained by Overmesh, would be a second source of truth about authorization —
and second sources of truth about authorization are how access leaks.

The initial design took this to its literal conclusion: the gateway forwarded
the caller's bearer token for *every* backend operation, including writes to the
reserved `overmesh-system` container holding heads, manifests, locks, and
quarantine records.

A review of the resulting threat model showed that this made the caller's rights
far broader than intended. Signatures prevent forgery, but they prevent neither
deletion nor replay. A caller holding ordinary data-plane rights could:

- delete a committed head, losing the object until the reconciler repaired it;
- overwrite a head with an older but validly signed committed manifest, rolling
  the logical version backwards;
- break the per-blob lease and interfere with an in-flight commit;
- delete their own quarantine record and resume writing to a blob that
  reconciliation had quarantined.

None of these require defeating the cryptography. They only require the write
permission the design was handing out.

## Options considered

**Keep full passthrough.** Simplest, and preserves the principle in its purest
form. Leaves the four failure modes above, and makes every Overmesh guarantee
contingent on callers behaving well or on network isolation being perfect.

**Give Overmesh its own permission model.** Map principals to logical
containers inside Overmesh, and use a single service identity for all backend
access. Closes the failure modes, and violates the governing principle
outright: a second authorization authority to administer, audit and keep in
step with Entra.

**Split by object class.** The caller's token for customer data, where Azure
RBAC should and does arbitrate. Dedicated managed identities for Overmesh's own
control objects, where no caller has any legitimate business.

## Decision

Identities are split by the class of object being accessed.

| Object class | Identity | Authorization decided by |
| --- | --- | --- |
| Customer blob content and properties | Caller bearer token | Azure RBAC on the customer container |
| Heads, manifests, high-water, catalogue, locks | Gateway managed identity | Container-scoped RBAC on `overmesh-system` |
| Quarantine, audit, garbage collection | Reconciler managed identity | Container-scoped RBAC, plus data-plane rights for repair |

The reserved system container is scoped to the Overmesh identities. Callers hold
no role on it.

**This does not add an authorization model.** The enterprise's users and
applications are governed by exactly the Azure RBAC they were already governed
by, on exactly the containers holding their data. The two managed identities are
deployment configuration for the Overmesh service itself — of the same nature as
its endpoint or its Key Vault key — and are invisible to the enterprise's own
authorization administration. Nothing an application team does changes.

**Credential confusion is prevented by the type system, not by convention.**
`CallerToken` and `ControlToken` are distinct, non-interchangeable types, and
the backend trait exposes operation-specific methods (`caller_*`, `control_*`,
`service_*`). A control credential cannot satisfy an API expecting a caller
credential. This matters because the failure is asymmetric: passing a caller
token where a control token is expected produces a loud `403`, while passing a
control token where a caller token belongs would silently bypass the caller's
RBAC and leave no trace.

## Consequences

### What it buys

The four failure modes above are closed. Deleting a head, replaying an older
head, breaking a lease and lifting a quarantine all now require compromising an
Overmesh managed identity rather than merely holding data-plane rights.

Azure remains the authorization authority for customer data, on both replicas,
with no Overmesh-side mapping to maintain.

The signed commit manifest records the caller's `oid` and `tid`, so the write is
attributable without Overmesh storing any identity state.

### What it costs

**Azure no longer arbitrates metadata-only operations.** For writes this is
covered incidentally, because the content write is itself caller-authorized and
happens first. For operations with no customer-data side effect — `DELETE`
producing a tombstone, `HEAD` served from the manifest, listing served from the
catalogue — nothing would check the caller at all. This is what forces the
authorization probe mechanism; see ADR-0005.

**Container-scoped RBAC becomes load-bearing, and Azure RBAC is additive.** A
role granted at account, resource group or subscription scope is inherited by
the system container, and Azure offers no generally available deny mechanism to
prevent it. The property is therefore *verified* rather than *enforced*: the
reconciler audits role assignments through Azure Resource Manager, checks that
no unapproved principal holds blob data actions on the system container, checks
symmetry between replicas, and fails closed. Deployments are additionally
expected to prevent account-scope data assignments by policy.

**The security model cannot be exercised locally.** Azurite accepts OAuth
tokens but does not enforce Azure RBAC. Neither "a caller cannot delete a head"
nor "a caller cannot lift a quarantine" is provable in the local harness. They
are provable only in the live Azure environment, which makes that gate a
prerequisite of the security claim rather than a nice-to-have.

### The trust boundary follows from the principle

Because security is carried by the enterprise's own identity system, the
administrators of that system are inside the trust boundary. Anyone able to
alter RBAC assignments, network configuration or the storage accounts
themselves can defeat the model.

This is not a V1 limitation to be lifted later. It is what "the enterprise
carries the security" means. Overmesh detects unapproved data-plane role
assignments and fails closed; it does not, and structurally cannot, defend
against the platform administrators who grant them.

Content tampering inherits the same shape. Callers write their own bytes with
their own credentials, so anyone authorized to write those bytes can modify them
directly. That is detected — by block-level hash validation on read and by
reconciliation — not prevented, and the recovery path is Azure blob versioning
and soft delete, which deployments are required to enable.

## When to revisit

If Azure makes deny assignments generally available, the RBAC posture audit
could move from detection to enforcement. That would be a refinement of this
decision, not a reversal.

A reversal would mean introducing an Overmesh-side authorization model, and
should be treated as a change of product rather than of implementation.

## Verified by

- `gateway/src/identity.rs` and `gateway/src/backend.rs` — caller and control
  credentials are distinct types and backend operations are split by
  credential class
- `gateway/tests/auth_contract.rs::executes_declarative_gateway_authentication_contract`
  — the authentication contract includes explicit Shared Key and SAS rejection
- `harness/scripts/gateway-smoke.sh` — the committed head carries the caller's
  object id, not the gateway's, with distinct local principals for caller,
  gateway and reconciler
- `reconciler/src/posture.rs::rejects_unapproved_system_container_access` —
  unapproved principals cannot hold blob data actions on the system container
- `reconciler/src/posture.rs::rejects_replica_role_asymmetry` — replica role
  assignments must remain symmetric
- `harness/environments/azure/validate-storage-authorization.sh` — probes every
  authorization capability with an allowed and a deliberately denied principal
- `harness/artifacts/live/0.9.0/posture-v090-live-evidence.json` — live ARM
  evidence that inherited unapproved access and path-dependent ABAC conditions
  both fail closed before nominal posture is restored
