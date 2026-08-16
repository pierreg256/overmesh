# Overmesh Gateway

The gateway is the Azure Blob-compatible HTTP data-plane entry point.

Milestones through `0.8.0` provide:

- fail-closed signed Ring startup;
- ES256 Ring signature validation;
- JWKS-based ES256 and RS256 bearer-token validation;
- issuer, audience, tenant, expiry, and not-before validation;
- explicit Shared Key and SAS rejection;
- Azure-compatible authentication and protocol error responses;
- Blob method and Storage API version recognition;
- strict W=2 Blob uploads;
- immutable content and manifest objects;
- signed block, prepared, committed, and head documents;
- logical ETags and conditional writes;
- idempotent retry and partial-head recovery;
- renewable Azure Blob leases for per-blob write serialization;
- Azure Key Vault ES256 signing through managed identity or workload identity.
- compile-time-separated caller and control credentials;
- caller-authorized immutable content in customer containers;
- managed-identity-only heads, manifests, locks, quarantine, and audit objects;
- exact canonical logical account/container/blob identities;
- signed caller tenant, object, subject, and application attribution;
- O(1) signed high-water and compaction checkpoints with checkpoint-anchored
  bounded history;
- server-randomized physical content keys reserved by signed block manifests;
- overlapping manifest and Ring trust bundles for key rotation;
- signed artifact timestamps checked against explicit key validity windows;
- cryptographically enforced Ring parent version and hash continuity;
- domain-separated Ring signatures;
- mandatory stable client write IDs with Azure client-request-ID fallback;
- validated `HEAD` and `GET` against identical signed heads on both replicas;
- O(1) high-water checkpoint validation on every read;
- compact signed block-manifest roots with hash-authenticated immutable pages;
- physical content-length validation on both replicas for `HEAD` and `GET`;
- block-by-block SHA-256 validation before bytes from a block are returned;
- closed, open-ended, and suffix byte ranges;
- complete validation of every block intersecting a requested range;
- deterministic primary content reads with secondary fallback only when the
  primary content read is unavailable;
- logical ETag conditions and Azure-compatible Blob read headers and errors.
- exact-path caller-authorized `DELETE`;
- signed prepared and committed tombstones published with W=2;
- idempotent delete retry and partial tombstone-publication recovery;
- durable tombstone high-water checkpoints and anti-resurrection enforcement;
- recreation after deletion as a strictly newer logical generation.
- logical `List Blobs` from bounded pages of an ordered W=2 catalog containing
  exact signed current-head bytes, revalidated against both heads, high-water
  checkpoints, compaction floors, sidecars, Ring placement, and quarantine;
- logical `List Containers` from bounded signed catalog pages with per-container
  caller authorization, excluding `overmesh-system`, unauthorized containers,
  and containers without a current visible catalog entry;
- `prefix`, one-character `delimiter`, opaque `marker`, `maxresults`, and
  `include=metadata`;
- signed continuation tokens bound to account, scope, normalized query,
  requested page size, Ring version/hash, last catalog ordering key,
  issue/expiry time, and signing key;
- no per-blob caller read authorization or content probe after successful
  container-list authorization; catalog validation uses typed control-plane
  reads under the gateway identity;
- W=2 `Put Block`, `Put Block List`, and `Get Block List` with exact Azure
  block IDs retained only in signed metadata;
- immutable unpredictable staged content paths, equal decoded block-ID length
  enforcement, strict size/count limits, and retry conflict detection;
- bounded-disk assembly of selected `Latest`, `Committed`, and `Uncommitted`
  blocks under the existing per-blob lease and commit protocol.

`HEAD` validates signed declarations and physical lengths without downloading
the block-manifest root, its pages, or the content body. `GET` loads only the
block-manifest pages intersecting the requested range and validates each
affected complete block before exposing bytes from that block. Full reads
process pages incrementally with bounded metadata memory. Replica metadata
disagreement, replay below high-water, missing signed objects, or content
corruption fails closed.

`DELETE` returns `202` only after both signed tombstone heads and both durable
high-water checkpoints are identical. It never synchronously removes immutable
content. Container creation/deletion and general metadata/property mutation
remain unsupported.

Listing is case-sensitive and uses canonical Azure path decoding. It omits
PREPARED and TOMBSTONED heads, staged blocks, `.overmesh/*`, system-container
objects, and any missing, quarantined, tampered, drifted, or replayed record.
`include` values other than `metadata` fail explicitly. Continuation over an
unchanged state has no duplicates or omissions. Concurrent inserts after the
last ordering key may appear on later pages; inserts before it are observed by
a new enumeration, and concurrent deletes disappear.

Block uploads use `x-overmesh-upload-id` to identify one staging generation.
When omitted, the Gateway derives an implicit generation from the caller,
logical blob, and current base generation so unmodified Azure SDK/CLI/AzCopy
requests with per-request client IDs still share one uncommitted namespace.
All `Put Block` requests in a generation must use the same decoded block-ID
length. The published subset supports at most 50,000 blocks, 64 decoded bytes
per block ID, and 100 MiB per staged block. Stages are retained for the
configured period and are never visible through blob reads or listing.
An explicit upload ID remains bound to its original base generation until its
stages expire; applications starting after overwrite/delete/recreate use a new
explicit ID.
`Put Block` authorization is enforced by the actual unpredictable staged
content upload. Idempotent committed-write replay first confirms that the
immutable content still exists, then uses a conditional `Put Blob` against that
existing object; Azure `409/412` proves current write permission without
changing bytes. Backend `401/403` maps to `AuthorizationPermissionMismatch`.

Every `PUT` and `DELETE` MUST provide a stable idempotency key. The gateway
uses `x-overmesh-write-id` when present, otherwise
`x-ms-client-request-id`. Values are 1-128 path-safe ASCII characters
(`A-Z`, `a-z`, `0-9`, `-`, `.`, `_`, `~`). A missing ID returns Azure-style
`400 MissingRequiredHeader`; the gateway never invents a client write ID.

Listing and staging retention are bounded in configuration:

```yaml
listing:
  continuationTokenLifetimeSeconds: 900
stagedBlocks:
  retentionSeconds: 604800
```

## Local execution

```bash
cargo run --quiet -p overmesh-gateway -- \
  --config gateway/config/local.yaml
```

Issue a deterministic local bearer token:

```bash
cargo run --quiet -p overmesh-harness -- issue-token valid
```

The local JWKS and signing keys are test-only fixtures and MUST NOT be accepted
by a production deployment.

## Production manifest signing

Production configuration uses a pinned, non-exportable P-256 Key Vault key:

```yaml
signing:
  provider: azureKeyVault
  keyId: https://example.vault.azure.net/keys/overmesh-manifests/KEY_VERSION
  notBeforeUnixMs: 1776000000000
  notAfterUnixMs: 1807536000000
  vaultUrl: https://example.vault.azure.net
  keyName: overmesh-manifests
  keyVersion: KEY_VERSION
  publicKeyPath: /etc/overmesh/manifest-public.pem
  trustedPublicKeys:
    - keyId: https://example.vault.azure.net/keys/overmesh-manifests/PREVIOUS_VERSION
      publicKeyPath: /etc/overmesh/manifest-public-previous.pem
      notBeforeUnixMs: 1744464000000
      notAfterUnixMs: 1776086400000
  credential: workloadIdentity
```

Use `managedIdentity` instead for App Service, virtual machines, or Azure Arc.
`managedIdentityClientId` selects a user-assigned identity when required.
Validity is evaluated against the timestamp covered by each artifact's
signature, not the reader's wall clock, so historical objects signed during a
key's valid period remain readable after rotation.

Commit responsibilities are split under `src/commit/` into write, delete,
high-water, recovery, locking, and quarantine modules. `commit.rs` retains the
stable public coordinator/service API and shared invariants. Listing,
continuation-token, and public block behavior live in `listing.rs`,
`continuation.rs`, and `block.rs`.
