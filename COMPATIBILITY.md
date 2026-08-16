# Overmesh 0.8 Compatibility Matrix

The published V1-development surface uses Microsoft Entra bearer tokens and
Storage API versions from `2017-11-09` through `2025-11-05`.

| Operation | 0.8.0 support |
|---|---|
| Put Blob, Get Blob, Head Blob, Delete Blob | Supported |
| List Containers | `GET /?comp=list`; catalog-backed non-empty logical containers; `prefix`, signed `marker`, `maxresults` |
| List Blobs | `prefix`, one-character `delimiter`, signed `marker`, `maxresults`, `include=metadata` |
| Put Block | Base64 IDs up to 64 decoded bytes; 100 MiB/block; W=2 signed staging |
| Put Block List | Ordered `Latest`, `Committed`, `Uncommitted`; 50,000 blocks |
| Get Block List | `committed`, `uncommitted`, `all` |
| Container create/delete | Explicitly unsupported |
| Snapshots, versions, tags, leases, copy, tiers | Explicitly unsupported |

Every write requires `x-overmesh-write-id` or `x-ms-client-request-id`.
Applications may send `x-overmesh-upload-id` to isolate a multi-request block
generation. Standard clients that omit it use an implicit caller/blob/base
generation and remain compatible with per-request client IDs.

Continuation markers are Overmesh tokens, not Azure backend cursors or blob
names. Tokens cannot be reused across accounts, containers, prefixes,
delimiters, include sets, page sizes, scopes, or Ring versions/hashes.

The local conformance gate covers real Gateway/replica pagination, delimiter
behavior, hidden internal/staged objects, block commit/retry, block-list
responses, tamper rejection, conditions, and reconciliation. The live Azure
gate pins allowed/denied list and block status/shape assumptions with explicit
canary cleanup.

Key error mappings are `400 InvalidMarker` for invalid/expired/reused tokens,
`400 InvalidBlockId` or `InvalidBlockList` for malformed block requests,
`404 ContainerNotFound`/`BlobNotFound`, `409 InvalidOperation` for idempotency
conflicts, `403 AuthorizationPermissionMismatch` for backend `401/403`, and
`503 ServerBusy` for ambiguous W=1 publication or fail-closed divergence.
