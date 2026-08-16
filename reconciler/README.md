# Overmesh Reconciler

The reconciler is an independent data-plane process. It never handles client
traffic and authenticates to both Azure Storage replicas with its own managed
identity or workload identity.

Milestones through `0.8.0` provide:

- bounded, cursor-backed incremental head discovery across every Ring node;
- explicit full-scan audit mode;
- complete ES256 head, commit-manifest, and block-manifest validation;
- complete content and per-block SHA-256 validation;
- `HEALTHY`, `DRIFTED`, `MISSING`, `TAMPERED`, and `QUARANTINED`
  classification;
- conditional repair only from a unique fully validated source;
- signed W=2 quarantine and append-only audit records;
- gateway enforcement of logical quarantine;
- explicit administrator-authorized recovery;
- fail-closed behavior when either backend is unavailable.
- managed-identity access to customer content for validation and repair;
- replay detection when a signed head is older than its durable high-water
  record;
- quarantine of valid-signature replay instead of automatic repair;
- overlapping verification-key trust bundles.
- fail-closed Azure ARM auditing of inherited blob `DataActions`, custom-role
  exclusions, assignment conditions, and replica RBAC symmetry.
- signed tombstone validation and repair under the shared per-blob lease;
- configurable physical-content retention beginning at the successor's signed
  commit timestamp;
- fail-closed validate-plan-execute collection of superseded committed
  generations for live heads, tombstones, and delete/recreate chains;
- preservation of tombstone and high-water anti-replay evidence;
- chained signed immutable garbage-collection watermarks, including safe
  recovery of a valid one-sided marker publication;
- streaming complete-content and per-block validation without reconciliation
  buffering the full content object;
- RF=2 placement and reconciliation on Rings containing more than two nodes.
- backfill and conditional repair of exact signed-head catalog entries on the
  active RF=2 replicas;
- quarantine of tampered, conflicting, mis-keyed, or newer-than-head catalog
  state before garbage collection, while preserving catalog correctness
  through tombstone repair, history compaction, and collection;
- discovery and signature/structure validation of staged-block metadata;
- repair only from a signed stage whose physical bytes hash correctly;
- quarantine on staged metadata/content divergence or tampering;
- validate-plan-execute expiry collection of identical W=2 staged data and
  metadata, with signed GC markers and partial-cleanup recovery;
- bounded staged-block work through `stagedBlockGc.maxRecordsPerCycle`.

`physicalCollectionDelaySeconds` configures the minimum delay between a
generation being superseded and physical collection. Retention starts at the
successor history entry's signed `committedAtUnixMs`. Production values must
not be shorter than Azure Blob soft-delete retention. The local configuration
uses zero only to exercise collection deterministically.

`historyCompaction.maxVersionsPerCycle` bounds checkpoint advancement and
history deletion work per cycle. Compaction is never triggered by size alone:
only versions already covered by identical signed GC evidence on both replicas
can move below the signed fixed-name compaction floor.

`headDiscovery.batchSize` bounds normal-cycle work. The local operational
checkpoint at `headDiscovery.cursorPath` advances only after a successful
cycle. Normal cycles scan one bounded page from one Ring node, then continue
from the persisted cursor on the next cycle.

Staged blocks carry a signed expiry selected by the Gateway. The Reconciler
does not treat an unsigned or tampered stage as a repair source and performs no
stage delete until both metadata replicas, both physical replicas, hashes,
namespace guards, and backend ETags have been revalidated.

```yaml
stagedBlockGc:
  maxRecordsPerCycle: 256
```

Run one complete cycle:

```bash
cargo run --quiet -p overmesh-reconciler -- \
  --config reconciler/config/local.yaml once
```

Run an explicit complete audit without advancing the incremental cursor:

```bash
cargo run --quiet -p overmesh-reconciler -- \
  --config reconciler/config/local.yaml once --full-scan
```

Run scheduled cycles:

```bash
cargo run --quiet -p overmesh-reconciler -- \
  --config reconciler/config/key-vault.example.yaml run
```

Run only the readiness-blocking RBAC posture audit:

```bash
cargo run --quiet -p overmesh-reconciler -- \
  --config reconciler/config/key-vault.example.yaml audit-rbac
```

Production backend entries require their Azure `resourceId`. The Reconciler
identity requires ARM read access for storage containers, role assignments,
and role definitions. `rbacPosture.approvedSystemPrincipalIds` must contain
only the Gateway and Reconciler runtime identities authorized for
`overmesh-system`.

Recover a quarantined blob from an explicitly selected, fully validated
replica:

```bash
cargo run --quiet -p overmesh-reconciler -- \
  --config reconciler/config/key-vault.example.yaml recover \
  --blob /logical-account/container/blob \
  --source-replica storage-a
```

Production deployments use the Azure Key Vault manifest signer and either
managed identity or AKS workload identity. The local signer and token-file
provider require explicit test-only enablement.
