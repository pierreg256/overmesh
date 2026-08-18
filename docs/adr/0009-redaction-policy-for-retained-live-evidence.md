# ADR-0009 — Redaction policy for retained live evidence

- **Status:** accepted
- **Date:** 2026-08-18
- **Milestone:** 0.9.0 → 0.9.1
- **Supersedes:** —
- **Superseded by:** —

## Context

Milestone 0.9 produced signed evidence of a live Azure run, and that evidence
is retained in the repository so that decision records can cite it and so that
a reader can check the claims rather than take them.

Retention creates a conflict the project has not had to face before. Evidence
is only worth keeping if it is **verifiable**, and only safe to keep if it does
not **disclose the environment that produced it**. Those pull in opposite
directions, and the repository is intended to be published and forked, so
whatever is committed is permanent for anyone who clones it.

The first bundles carry, in clear:

- the Azure subscription GUID;
- the resource group name;
- the Front Door hostname;
- the Key Vault host, key name and key version;
- Storage Account names, inside full ARM resource identifiers.

None of these grants access. The subscription GUID is the one that matters:
it enables targeted reconnaissance and credible phishing, and it stays true
indefinitely. The Front Door hostname is merely an invitation to scan.

## Options considered

**Retain the raw bundles.** Maximum verifiability, nothing to explain. Publishes
the tenant's infrastructure identifiers permanently, for every release, and
accumulates them one campaign at a time.

**Retain nothing; keep hashes in prose.** Safe and close to useless. An
unfalsifiable claim that evidence exists is weaker than no claim, and it
contradicts the argument the project makes about itself.

**Redact after signing.** Rejected outright, and worth naming so that nobody
proposes it: altering a signed document invalidates the signature and destroys
the whole chain. This is not a clean-up step before committing — it is a change
to how evidence is produced.

**Redact before signing, deterministically.** Chosen.

## Decision

**Redaction happens before signature. The redacted form is the canonical
evidence**, and the artefact that is signed, hashed and cited.

Sensitive values are **replaced, not removed**, by a deterministic pseudonym:

```
sha256(value) truncated to 16 hex characters, with a kind prefix

sub-4f2a9c1e8b7d3056     subscription
rg-a91c7f0e22b4d8f3      resource group
fd-6b3e05d7c4a19f28      front door hostname
st-0c5e93a71f28b4d6      storage account
kv-1d8f4b62e0a37c95      key vault host
```

Two properties follow, and they are the reason for replacement rather than
deletion.

**Correlation survives.** Two bundles from the same environment carry the same
pseudonym. An auditor can establish that two campaigns ran in the same place
without learning where.

**The redaction is verifiable.** Anyone holding the real value recomputes the
pseudonym and confirms the mapping. The evidence stays falsifiable, which is
the entire argument.

**No salt.** A salt would prevent confirming a guess, but it would also break
third-party verification unless published, at which point it protects nothing.
The high-entropy values it would matter for — subscription and tenant GUIDs —
gain nothing from it.

### What is redacted

Subscription and tenant GUIDs, resource group names, Storage Account names,
Front Door and Container Apps hostnames, Key Vault hosts, and the object
identifiers of the test principals.

### What is retained in clear, deliberately

This half matters as much. Over-redaction turns evidence back into assertion.

Azure regions, logical Ring node identifiers, Ring version and hash,
replication factor, container image digests, SDK, CLI and AzCopy versions,
HTTP status codes, check counts and outcomes, timestamps, run identifiers, and
the runtime commit.

**Public keys are published on purpose.** They are the opposite of a secret:
they let a third party verify every signature without asking anyone for
anything.

For the detached signature's `keyId`, the vault host is redacted while the key
name and version are kept. The version identifies which key signed, which is
evidentiary; a third party verifies against the published public key rather
than against a vault they cannot reach.

### Raw evidence is retained out of band

The unredacted bundle must continue to exist somewhere durable outside the
repository, and its location must be recorded in the repository. Without it,
no one — including the project — can recompute a pseudonym, and the redaction
stops being verifiable and becomes merely opaque.

## Consequences

**"The evidence" now means the redacted form.** It is what is signed, what is
hashed, and what records cite. Reconciling it with the raw bundle requires
recomputing the pseudonyms, which is a documented operation rather than a
comparison.

**Git history is permanent.** Artefacts already committed in unredacted form
are not fixed by a subsequent commit; anyone who has cloned retains them. They
require history rewriting, which is far cheaper now — a handful of commits, no
public remote — than after publication.

**The raw bundle becomes load-bearing.** Losing it makes every pseudonym in
every published bundle permanently opaque. Its retention needs an owner and a
location, not a habit.

**Compliance must be checked, not assumed.** The failure mode is not today's
oversight but the 0.10 campaign in two months, generated by a pipeline someone
adjusted. `doc-check` gains a rule that scans retained artefacts for
subscription paths, GUIDs, and Azure service hostnames, and fails closed.

## Implementation status

Implemented. The 0.9.0 source evidence is assembled privately, redacted into
its canonical published form, then signed. The raw archive is retained on all
three private validation accounts with a recorded SHA-256. No unredacted
release bundle entered Git history. `doc-check` R8 scans every retained
artefact and fails on GUIDs, subscription paths, or Azure service hostnames.

## When to revisit

If a reviewer or auditor requires unredacted evidence, deliver the raw bundle
out of band under whatever terms apply, rather than weakening the published
form. The policy exists because the artefact is public; a private disclosure
does not require changing it.

If the pseudonym function ever changes, previously published bundles stop
correlating with new ones. Treat it as a format version, not a refactor.

## Verified by

- `harness/environments/azure/assemble-live-evidence.py` — assembles the raw
  source hashes before the private bundle is retained
- `harness/environments/azure/build-live-evidence.py` — applies deterministic
  pseudonyms before the canonical bundle is signed
- `harness/environments/azure/sign-live-evidence.sh` — signs only the canonical
  redacted bundle and redacts the Key Vault host in the detached signature
- `harness/artifacts/README.md` — records what is retained, in which form, and
  where the unredacted originals live
- `harness/artifacts/live/0.9.0/manifest-v090-public.pem` — the published
  public key, which is what a third party verifies signatures against
- `harness/src/doc_check.rs::rejects_unredacted_live_evidence` — retained
  artefacts fail R8 when they expose subscription paths, GUIDs or Azure service
  hostnames
- `harness/artifacts/live/0.9.0/overmesh-v090-live-evidence.json` — canonical
  redacted bundle containing the raw bundle hash and every source hash
- `harness/artifacts/live/0.9.0/overmesh-v090-live-evidence.sig.json` —
  detached Key Vault signature over the canonical bundle
