# ADR-0011 — Use physical content HEADs for read authorization

- **Status:** accepted
- **Date:** 2026-08-19
- **Milestone:** 0.10.1
- **Supersedes:** —
- **Superseded by:** —

## Context

ADR-0005 chose a caller-authorized `HEAD` against the logical blob path for
`HEAD` and `GET`, even though the logical path does not physically contain the
content. The read path then issued another caller-authorized `HEAD` against the
derived immutable content object on each replica to validate existence and
length.

The first performance campaign measured both pairs on every `HEAD` and `GET`.
The logical probes return `404`; the physical requests return `200`. Both are
evaluated by Azure Storage with the same caller token.

ADR-0005 subsequently established and live-tested a stronger deployment
constraint: blob-path-dependent role assignment conditions are unsupported and
the posture audit fails closed when one is effective on a customer container.
Within the supported posture, authorization is container-scoped or depends only
on path-independent environment or principal attributes. The logical and
physical objects therefore have the same read decision.

## Options considered

**Keep both pairs.** Preserves the original logical-resource wording but spends
two requests per operation on a distinction that supported deployments forbid
Azure RBAC from making.

**Use only the logical probes.** Cannot validate that both immutable content
objects exist with the length committed in the signed head.

**Use the physical content `HEAD` requests for both authorization and content
validation.** Chosen.

## Decision

For `HEAD` and `GET`, the Gateway no longer sends the separate
`authorize_blob_read` requests. After loading and validating the strict signed
head, it sends `caller_head_data_object` to both derived content objects with
the caller token.

Each successful physical `HEAD` simultaneously proves:

- Azure granted `blobs/read` to the caller on that customer container;
- the immutable content object exists on both replicas;
- both lengths equal the signed committed length.

Any `403`, absence, length mismatch or replica disagreement fails the operation.
Azure remains the authorization authority.

This amends only the `HEAD` and `GET` rows of ADR-0005. Metadata-only surfaces
that do not validate a physical content object, including `Get Block List`,
retain their logical-resource probe.

## Consequences

`HEAD` and `GET` each issue two fewer Storage requests. The nominal `HEAD`
budget moves from twelve requests to ten while ADR-0010 keeps the four
Reconciler safety reads.

The removed logical probes were one arm of the initial `tokio::try_join!`
fan-out. The physical content `HEAD` requests already ran after preparation,
and this decision does not move them or add a dependency edge. Two requests
leave a parallel fan-out; nothing becomes serial.

The expected benefit is therefore lower Storage request load, not lower
single-operation latency. A flat `HEAD` p50 confirms this execution model.
Latency remains an observed campaign signal, while requests per operation are
the deterministic non-regression measure.

Path-dependent ABAC has no partial read-only effect. This is more consistent
with the existing fail-closed unsupported posture, but it means a deployment
that bypasses that posture audit cannot rely on logical-path conditions.

## When to revisit

Revisit together with ADR-0004 and ADR-0005 if content naming changes so that
the immutable object preserves the logical path, or if Azure exposes a
side-effect-free authorization evaluation API.

## Verified by

- `gateway/src/commit/tests.rs::head_does_not_load_block_integrity_metadata` —
  `HEAD` performs the two physical caller requests and no logical read probe
- `reconciler/src/posture.rs::rejects_direct_path_dependent_customer_container_condition` —
  unsupported path-dependent conditions fail closed
- `harness/artifacts/live/0.9.0/posture-v090-live-evidence.json` — retained live
  evidence for the path-dependent-condition posture failure
- `harness/environments/azure/performance/collect_live_performance_telemetry.py`
  — distinguishes `logical_blob` and `content` request classes
