# Overmesh 0.9 Compatibility Matrix

The published V1-development surface uses Microsoft Entra bearer tokens and
Storage API versions from `2017-11-09` through `2025-11-05`.

| Operation | 0.9.0 support |
|---|---|
| Put Blob, Get Blob, Head Blob, Delete Blob | Supported |
| List Containers | `GET /?comp=list`; catalog-backed non-empty logical containers; `prefix`, signed `marker`, `maxresults` |
| List Blobs | `prefix`, one-character `delimiter`, signed `marker`, `maxresults`, `include=metadata` |
| Put Block | Base64 IDs up to 64 decoded bytes; 100 MiB/block; W=2 signed staging |
| Put Block List | Ordered `Latest`, `Committed`, `Uncommitted`; 50,000 blocks |
| Get Block List | `committed`, `uncommitted`, `all` |
| Container create/delete | Explicitly unsupported |
| Snapshots, versions, tags, leases, copy, tiers | Explicitly unsupported |

## Blob and container naming

Container names follow Azure's own rules: 3 to 63 characters, lowercase
letters, digits and hyphens, no leading or trailing hyphen, no consecutive
hyphens.

Blob names are percent-decoded before validation and re-encoded into a single
canonical form. Upper and lower case escapes are equivalent, and `a%2Fb`
denotes the same blob as `a/b`, matching Azure. Empty path segments such as
`a//b` and `a/b/` are accepted. **No Unicode normalisation is applied**: `é` as
U+00E9 and as `e` followed by U+0301 are distinct blobs, as they are in Azure.

`.overmesh` is a reserved prefix for internal objects. Logical names in that
namespace are refused, and internal physical objects are excluded from listings.

Two deviations from Azure:

**Control characters are refused** in blob names. Azure tolerates them; the
restriction is deliberate and permanent.

**Usable name length is shorter than Azure's 1,024 characters.** Overmesh
derives a catalogue key that encodes the name at two characters per UTF-8 byte,
and that key is bound by the same 1,024-character backend limit. The usable
budget therefore depends on the script the name is written in:

| Name content | Bytes per character | Overmesh limit | Share of Azure's 1,024 |
|---|---|---|---|
| ASCII | 1 | ~493 characters | 48% |
| Latin with accents | 2 | ~246 characters | 24% |
| CJK, Cyrillic, Greek | 3 | ~164 characters | 16% |
| Emoji, rare planes | 4 | ~123 characters | 12% |

Figures assume a ten-byte container name; a maximum-length 63-byte container
costs a further 53 characters of budget. The limit is enforced during request
validation with `400 InvalidRequest`, before any backend object is written.

## Authorization granularity

Caller authorization is Azure RBAC on the customer container. Role assignments
scoped at container level are honoured exactly as they are by Blob Storage.

**Role assignment conditions whose predicate depends on the blob path are not
supported on customer containers.** Overmesh checks reads, `HEAD` and deletes
against the logical blob path, but writes reach Azure on a derived content
object under `.overmesh/objects/`, which no path predicate can match. Such a
condition would be enforced on read and silently bypassed on write, so it is
refused rather than partially applied. The reconciler's RBAC posture audit
fails closed on such a condition effective on a customer container, including
one inherited from a higher scope.

Path-independent conditions are unaffected. `@Environment` predicates — private
endpoint, subnet, time — and `@Principal` predicates compose normally. Because
logical containers map one-to-one onto backend containers, separation by
container works with full fidelity.

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

## Validated clients

Milestone 0.9.0 ran the deployed Front Door surface with managed identity only:

| Client | Validated version | Live operations |
|---|---|---|
| Azure SDK .NET | Azure.Storage.Blobs 12.22.2 | Put Blob, block upload/commit/list, Get, Head, List, Delete |
| Azure SDK Python | azure-storage-blob 12.22.0 | Put Blob, block upload/commit/list, Get, Head, List, Delete |
| Azure SDK JavaScript | @azure/storage-blob 12.23.0 | Put Blob, block upload/commit/list, Get, Head, List, Delete |
| Azure CLI | 2.76.0 | Upload, download, show, list, delete |
| AzCopy | 10.27.1 | Upload, download, delete |

The Gateway accepts the canonical Azure Storage token audience both with and
without a trailing slash, matching the tokens emitted by these standard
clients while continuing to reject non-Storage audiences.

Key error mappings are `400 InvalidMarker` for invalid/expired/reused tokens,
`400 InvalidBlockId` or `InvalidBlockList` for malformed block requests,
`404 ContainerNotFound`/`BlobNotFound`, `409 InvalidOperation` for idempotency
conflicts, `403 AuthorizationPermissionMismatch` for backend `401/403`, and
`503 ServerBusy` for ambiguous W=1 publication or fail-closed divergence.
