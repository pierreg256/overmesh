# ADR-0007 — Canonical logical resource identity

- **Status:** accepted
- **Date:** 2026-08-17
- **Milestone:** 0.5.0 → 0.8.0
- **Supersedes:** —
- **Superseded by:** —

## Context

Almost everything in Overmesh is derived from the identity of a logical blob:

- `path_hash`, which selects the replicas through the Ring and names the head,
  the high-water record, the quarantine record and the lock;
- the logical ETag, which clients use for conditional requests;
- the `blob` field of every signed manifest;
- the catalogue key that backs listing;
- the content object prefix in the customer container;
- the target of every authorization probe.

If two requests that denote the same Azure blob produced two different
identities, they would become two different logical blobs: two heads, two
placements, two catalogue entries. A client that wrote with one SDK and read
with another would find nothing.

So the identity function has to be **total, deterministic and injective** over
the set of request paths Azure itself accepts — and it has to agree with
Azure's own notion of what a blob name *is*.

That notion is the **decoded** string. `PUT /c/a%2Fb.jpg` and `PUT /c/a/b.jpg`
address the same blob in Azure Blob Storage. Any identity function that treats
them as different is wrong.

## Options considered

**Use the raw request path.** What the implementation did before 0.8.0. Simple,
and wrong twice over: two clients differing on optional escaping — `~` versus
`%7E`, or uppercase versus lowercase hex — address different objects; and a
literal `/` inside a blob name, expressed as `%2F`, becomes a path separator.

**Use the decoded string as the canonical form.** Correct with respect to Azure
identity, and the simplest thing that works. But the canonical form is not only
compared — it is embedded in signed JSON manifests, hashed, written into logs
and reconstructed from stored documents. A raw decoded form carries arbitrary
bytes into all of those.

**Decode, validate, then re-encode into one fixed canonical form.** Agrees with
Azure identity because the decision is made on the decoded string, and yields a
single ASCII-only representation safe to embed anywhere.

## Decision

`LogicalBlobId::parse(account, request_path)` applies four steps in order:

1. **Split** the request path on its first `/` into container and blob. The
   blob may contain further separators.
2. **Percent-decode** both components. Both upper and lower case hexadecimal
   are accepted on input, so `%2f` and `%2F` converge.
3. **Validate** — see below.
4. **Re-encode** into the canonical form
   `/{account}/{container}/{blob}`, where each component is encoded with the
   RFC 3986 unreserved set — only `A-Za-z0-9-._~` survive, everything else
   becomes `%XX` with uppercase hexadecimal.

The blob is encoded **per segment**, so `/` survives as a structural separator
while every other byte is escaped. The encoding is injective because `%` itself
is escaped: a blob literally named `a%2Fb` canonicalises to `a%252Fb`, distinct
from the blob named `a/b`.

Everything else derives from this single string. There is no second notion of
identity anywhere in the system.

### The logical account is part of the identity

Even though the account comes from deployment configuration rather than from
the request, it is included in the canonical form and therefore in
`path_hash`. A gateway serves one logical account today; when one serves
several, no derivation changes and no hash collides.

### No Unicode normalisation

The canonical form is byte-preserving after decoding. There is no NFC, no NFD,
no case folding. Two Unicode representations of the same visible character —
`é` as U+00E9, or `e` followed by U+0301 — are two distinct blobs.

**This is correct, because it is what Azure does.** Azure Blob Storage does not
normalise blob names, and an identity function that normalised would merge
blobs Azure keeps separate.

This clause is the answer to specification §7.3, which requires the
canonicalisation specification to define UTF-8 normalisation. The answer, for
logical resource paths, is *none*. JSON document canonicalisation is a separate
matter and is handled by JCS.

**This choice is practically irreversible.** Introducing normalisation later
would change `path_hash` for every non-ASCII name, which changes both the
placement of those objects and the names of every object derived from them.
Without Ring migration — see ADR-0006 — that is unrecoverable.

### Validation, and two declared deviations

Container names are validated against Azure's own rules: 3 to 63 characters,
lowercase letters, digits and hyphens, no leading or trailing hyphen, no
consecutive hyphens. Overmesh therefore rejects at its edge exactly what Azure
would reject at its own, and the validated name is usable directly as a backend
container name with no escaping.

Blob names are rejected when empty, when longer than the derived limit, or when
they contain **control characters**.

Rejecting control characters is **stricter than Azure**, which tolerates them
with caveats. The restriction is deliberate: control characters in blob names
are a source of log injection, terminal escape issues, and ambiguity in every
tool that touches them, and no legitimate workload needs them. It is a declared
deviation and belongs in `COMPATIBILITY.md`.

Empty path segments are *not* rejected: `a//b` and `a/b/` are accepted, as they
are by Azure, and survive the decode-encode cycle unchanged. This was a
restriction in earlier milestones and its removal is a deliberate compatibility
gain.

### Length is validated against the derived limit, at parse time

Azure caps blob names at 1,024 characters, and a bare character count matches
that. But Overmesh derives storage keys from the name, and the longest of them
is the catalogue key, whose encoding costs two characters per UTF-8 byte. A
name that is legal for Azure can therefore produce an Overmesh key that is not.

The rule is that **`parse` rejects any name whose derived keys would exceed the
backend limit**, using the real derived bound rather than the raw character
count.

The validation belongs here and nowhere else, for three reasons: `parse` is the
only place where every derivation is known; it runs before any backend object
is written, so the request fails cleanly instead of part-way through a commit;
and it produces a `400` naming the offending header rather than a `503` after
the head has already been published. See ADR-0004 for the failure mode this
prevents, and ADR-0008 for why the catalogue key is encoded as it is.

## Consequences

One identity, derived once, used everywhere. A canonical string that is
ASCII-only and therefore safe in URLs, JSON, logs and hash inputs without
further escaping. Agreement with Azure on what constitutes the same blob,
including the `%2F` case.

The usable blob-name length is smaller than Azure's, and the reduction depends
on the script the name is written in. That is a compatibility regression, it is
quantified in ADR-0004, and the encoding decision that causes it is revisited
in ADR-0008.

Rejecting control characters is a second, smaller deviation, taken knowingly.

## Implementation status

Two elements describe the intended state rather than the current one:

- `parse` validates 1,024 **characters** rather than the derived bound;
- the reserved `.overmesh` prefix is filtered at listing time but not refused
  at parse time (ADR-0004's residual gap).

Both corrections land in the same function and should ship together.

A third gap is structural rather than a missing check. The canonical form is
the identity, but it is carried as a bare `&str` past the boundary where it was
validated: `RingDocument::replicas_for` takes `logical_blob: &str`, and the
reconciler resolves placement from the `blob` field of signed manifests as a
string. Nothing prevents a caller from passing an arbitrary string that was
never produced by `parse`.

`LogicalBlobId` exists and `from_canonical` already reconstructs it, so the
identity can be carried as a validated type end to end — the same argument
ADR-0003 makes for credentials, where the boundary is enforced by the type
system rather than by convention. Until then, "one identity, derived once" is a
property of the call sites rather than of the code.

## When to revisit

If the catalogue encoding changes — see ADR-0008 — the derived length bound
changes with it, and the constant in `parse` must follow. The rule stays; the
number moves.

Unicode normalisation should not be revisited without a Ring migration path,
because the change is not backward compatible for existing data.

## Verified by

- `gateway/src/resource.rs` unit tests — canonical account-aware identity,
  percent decoding of both cases, `%2F` convergence with the separator form,
  container name validation
- `gateway/src/catalog.rs` round-trip test —
  `logical_blob_from_catalog_key(catalog_key(b)) == b` for a set of blobs,
  which proves injectivity survives the catalogue encoding
- `harness/scripts/gateway-smoke.sh` — the head object key is computed
  independently by the script as `sha256("/local-overmesh" + path)` and matched
  against what the gateway wrote, proving the derivation end to end
