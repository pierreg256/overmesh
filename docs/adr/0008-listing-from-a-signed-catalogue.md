# ADR-0008 — Listing from a signed catalogue

- **Status:** accepted
- **Date:** 2026-08-18
- **Milestone:** 0.8.0
- **Supersedes:** —
- **Superseded by:** —

## Context

`List Blobs` has to return the logical blobs of a container, in Azure's order,
paginated, filtered by prefix and delimiter. Three constraints make it harder
than it sounds.

**Physical objects are not logical blobs.** Each replica's containers hold
immutable content generations, staged blocks, block manifest pages, heads,
high-water records and locks. Enumerating them would expose internal structure,
and — worse — would expose `PREPARED` content, which specification Invariant 9
forbids from ever being visible.

**Ordering must match Azure.** Clients page through results and resume from a
marker. Azure returns blobs in lexicographic order of the blob name, and
anything that reorders breaks resumable enumeration silently.

**Listing must not become a side channel.** It reports names, sizes and ETags.
Whatever it reads has to be as trustworthy as what `GET` and `HEAD` read, or
listing becomes the soft way to learn about state the other paths validate.

## Options considered

**Enumerate physical objects and filter.** No additional state to maintain. But
there is no reliable way to tell a committed content generation from an
abandoned prepared one by name alone, ordering follows the physical key rather
than the logical name, and every internal object class has to be excluded by a
rule that is one omission away from a leak.

**Maintain a separate summary index.** A compact record per blob — name,
length, ETag. Cheap to read. But it is a second document type with its own
schema, its own signature domain, and its own opportunity to drift from the
head it summarises.

**Make the index entry the committed head itself.** One document type, one
signature domain, nothing to keep in sync, and cross-replica agreement is a
byte comparison. Costs a duplicate of the head per blob and a republication on
every commit and delete.

## Decision

Listing is served from a **catalogue in the system container**, whose entries
are the signed committed head, byte for byte.

```
catalog/v1/{hex(container)}/{hex(blob)}.json
```

A blob has a catalogue entry only once it is committed. `PREPARED` content has
none, so Invariant 9 holds structurally rather than by exclusion rules.

### Why the key is hex encoded

Paging hands Azure a derived prefix and consumes results in Azure's order. The
encoding must therefore be **monotone** — preserving byte order — and
**prefix-preserving**, so that a logical prefix filter translates into a key
prefix filter.

That eliminates percent-encoding, which is not monotone: an escaped byte begins
with `%` (0x25), which sorts below every unreserved character, while the byte
it represents may not. `%7F` sorts before `-`, whereas 0x7F sorts after 0x2D.
Using it would corrupt pagination silently.

Hexadecimal satisfies both properties trivially — it is monotone on bytes and
aligned to byte boundaries — and decodes unambiguously. Its cost is two
characters per byte.

**Base32hex** was considered as the middle option: the RFC 4648 extended-hex
alphabet is also monotone, at 1.6 characters per byte, a 25% saving. It is
deferred because prefix preservation only holds at five-byte boundaries, and
the prefix path is exactly where a partial block would have to be handled
correctly.

**V1 keeps hexadecimal**, and the properties it relies on are captured as
property tests over random byte strings — round-trip, and `enc(a) < enc(b)` if
and only if `a < b` — rather than left as assumptions. Those tests are also
what would make a cheaper encoding safe to attempt later.

### What listing validates

Per page, the quarantine set is fetched once as a batched prefix listing rather
than per entry.

Per entry, the catalogue object is read from both replicas and compared byte
for byte, its signature and structure are validated, its state must be
`COMMITTED`, and its blob must not be quarantined.

The entry is also compared against the head object read from both replicas.
That check proves **freshness, not authenticity** — the signature already
proves authenticity — and it is load-bearing.

The catalogue entry is published *before* the head, unconditionally; the head
follows under a compare-and-swap on its backend ETag. The divergence window is
therefore not that the catalogue lags the head, but that it can **run ahead**
of it: a commit that publishes the catalogue and then fails at head
publication, or loses the compare-and-swap to a concurrent writer, leaves a
catalogue entry describing a version that was never committed.

Reporting a version that was never committed is worse than reporting a stale
one, so the head comparison stays.

**No write ordering removes it.** Publishing the head first inverts the window
rather than closing it, and Azure offers no way to make one object's write
conditional on another object's transition. Two objects that must agree require
a check at read time. That is the inherent price of a catalogue that duplicates
the head, and it is accepted here rather than engineered around.

What listing does *not* prove is that the content exists, that the block
manifest matches, or that the high-water record agrees. Those are the business
of `GET`, `HEAD` and the reconciler. Azure's own listing validates no content
either.

### Continuation tokens

Markers are Overmesh tokens, not Azure cursors and not blob names. A token is
signed and bound to the account, container, scope, prefix, delimiter, include
set, page size, and both the Ring version and Ring hash. Any change to the
request, or any Ring revision, invalidates it with `400 InvalidMarker`.

## Consequences

**The blob-name budget is reduced, and this is debt rather than an accepted
consequence.** Hex costs two characters per UTF-8 byte, and the catalogue key
is bound by the same 1,024-character backend limit as any blob name, so the
usable logical name is roughly 493 ASCII characters, 246 in accented Latin, 164
in CJK. Azure allows 1,024 in all cases. The published matrix states this, and
**parity with Azure is to be reopened before 1.0**.

**Listing remains the heaviest client operation.** At the default page size of
5,000 entries it is 20,000 backend reads, plus one batched quarantine listing.
That is the number the 0.10 performance baseline should measure first, and the
one most likely to force a design change.

**Every commit and delete republishes the catalogue entry**, which is a
duplicate of the head. The storage cost is small relative to content; the
round-trip cost is on the write path.

**Invariant 9 holds by construction.** Nothing that lacks a committed head has
a catalogue entry, so no exclusion rule stands between a `PREPARED` object and
a client.

## Implementation status

- Property tests for the catalogue encoding — round-trip and order preservation
  over random byte strings — are to be added.

## When to revisit

**Before 1.0, for the name-length budget.** Either base32hex with correct
partial-block prefix handling, or a monotone escaping scheme that passes most
bytes through. The second reaches near parity with Azure but an ordering bug in
it is silent, so the property tests above are a prerequisite, not an
afterthought.

**Reducing the four reads per entry is open, and it is a policy question
rather than an ordering one.** The two candidates are dropping the
cross-replica comparison of the catalogue object, and dropping the head
comparison. Each trades a class of incorrect result for half the cost: reading
a single replica may serve an entry the reconciler has not yet repaired;
dropping the head comparison may report a version that was never committed. A
third option is to keep both but reduce the default page size, which moves the
cost rather than removing it. All three belong with the 0.10 measurements.

**If path-granular authorization is ever required**, listing is the operation
that cannot honour it without per-entry authorization at enumeration. See
ADR-0005.

## Verified by

- `PREPARED-INVISIBILITY-008` — a `PREPARED` object is never visible through
  the client API
- `LIST-TOKEN-001` — a tampered marker is refused with `400`
- `LIST-RING-ROLLOVER-001` — a marker issued under one Ring version is
  refused after a Ring change
- `LIST-DELETE-RECREATE-001` — catalogue behaviour across a delete and
  recreate cycle
- `gateway/src/catalog.rs::ordered_keys_preserve_container_and_blob_utf8_order`
  — catalogue keys round trip and preserve UTF-8 ordering
- `gateway/src/catalog.rs::listing_prefix_is_a_physical_key_prefix` — logical
  prefixes map to physical key prefixes
- `gateway/src/commit/tests.rs::logical_listing_hides_stages_and_paginates_with_signed_markers`
  — staged objects remain hidden and continuation markers are signed
- `harness/scripts/gateway-smoke.sh` — delimiter grouping producing the
  expected `BlobPrefix`, pagination at `maxresults=1` following `NextMarker`,
  and `List Containers`
