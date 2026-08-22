# Overmesh 0.11.0 v5.1 campaign analysis

This report is an interpretive companion to the signed canonical evidence. It
is not itself signed. Reviewers should verify every finding against:

- `performance-v011-v5.1-fast-evidence.json`
- `performance-v011-v5.1-fast-evidence.sig.json`
- `performance-v011-v5.1-listing-confirmation-evidence.json`
- `performance-v011-v5.1-listing-confirmation-evidence.sig.json`

## Fast diagnostic

- Run ID: `20260822T175920Z`
- Runtime release: `v0.11.0`
- Runtime commit: `5202eccff4b1e277342cf784dde285e891eb865b`
- Gateway image digest:
  `sha256:d716d876e2b6a4fa8749c2d2169c937cfc0e289fefda5df32be8388ccdba6bda`
- Canonical evidence SHA-256:
  `79e54525ffa263fe95aae9a7af5693a35756716a4fd0e297071faf36375878d9`
- Signed archive SHA-256:
  `161d99dff2d579618f1f1e53a2a746d6df215b283fb7bc2f1f2d32bd896cd6a7`
- Signature status: `verifiedByKeyVault=true`
- Client execution: 2,886 successful operations, zero client errors
- Client wall time excluding fixtures: 2,456.931015 seconds
- Evidence validity: 59 of 76 target cases valid; 17 Gateway cases invalid

The 17 invalid Gateway cases do not represent 17 observed runtime failures:

1. Fourteen read cases used the old fixed path policy. Each repeat exercised
   the same 20 paths from a required pool of 24, so the placement-coverage gate
   failed even though all client operations and backend-request counts
   completed.
2. `delete_blob-1kib-c1` contains three `system_container` backend requests
   outside the DELETE fingerprint. The collector records the same aggregate
   twice in the failure list. The other DELETE cases retain the exact
   43-request structural budget.
3. Four of 30 100 MiB block sequences used 443 requests instead of 442. The
   additional request is `control_renew_lock`, emitted when the long-running
   operation crosses the lock-renewal threshold. This is a duration-dependent
   safety operation and must be modelled separately from the structural
   request budget.

The primary runtime finding is backend control-path amplification:

- PUT and overwrite: 49 backend requests per client operation.
- DELETE: 43 structural backend requests per client operation.
- GET up to 1 MiB and range GET: 15 backend requests per operation.
- GET 16 MiB and Get Block List: 18 backend requests per operation.
- HEAD: 10 backend requests per operation.
- Put Block Sequence 16 MiB: 181 backend requests per operation.
- Put Block Sequence 100 MiB: 442 structural requests plus lock renewals.

A simple PUT performs 28 control reads and 17 control writes. Twenty responses
per operation are expected `404` absence checks. Backend response-header p50 is
roughly 33-39 ms, making serial dependency waves the main small-object latency
cost. Manifest signing succeeds throughout and is secondary: a simple PUT
performs three signatures with roughly 42-47 ms p50 duration each.

Representative Gateway median-per-run p50 values:

| Operation | Shape | p50 |
| --- | --- | ---: |
| PUT Blob | 1 KiB, c1 | 1,202.962 ms |
| PUT Blob | 1 MiB, c1 | 1,431.987 ms |
| PUT Blob | 16 MiB, c1 | 2,416.796 ms |
| GET Blob | 1 KiB, c1 | 219.130 ms |
| GET Blob | 1 MiB, c1 | 379.793 ms |
| GET Blob | 16 MiB, c1 | 979.252 ms |
| HEAD Blob | 1 MiB, c1 | 93.155 ms |
| Put Block Sequence | 16 MiB, c1 | 10,643.820 ms |
| Put Block Sequence | 100 MiB, c1 | 45,652.066 ms |

Only 15 of 38 Gateway p50 series satisfy the 1.10 within-campaign spread
threshold. The diagnostic contract is not baseline-eligible and keeps p50
signal-only. No release-to-release latency regression can be concluded from
this campaign.

## Listing confirmation

- Run ID: `20260822T203656Z`
- Tooling commit: `6aa2c897351730eb9f28b51c5bd209a89073fd5d`
- Runtime release and image: same `v0.11.0` runtime as the fast diagnostic
- Canonical evidence SHA-256:
  `9866c6b9ba60a0eaf9664c92c1744dab130659137cd4115ff6b1889b751e4f43`
- Signed archive SHA-256:
  `5eb65d9c16750ae3abc8030e4185abc642da21049526aa3e4d03c7d9927361a2`
- Signature status: `verifiedByKeyVault=true`
- Client wall time excluding fixtures: 367.137980 seconds
- Evidence validity: all four direct cases valid; all four Gateway cases
  invalid

All Gateway client calls completed, but Azure Monitor stabilization produced
partial or cross-window listing counts. Examples include 3,000 or 4,000
observed returned entries for client calls that returned 5,000. One flat-list
window also contains nine unattributed `validate_control_container` requests,
and the aggregate failure is duplicated.

The invalid evidence must not authorize an encoded-prefix range-skip change,
but it exposes a strong hypothesis for a corrected confirmation run:

- Full flat listing considers, validates, and returns nearly every entry.
  Validation remains the dominant backend cost.
- Hierarchical listing considered 13,010 catalogue entries while validating
  only 140 and returning 130. Its telemetry recorded 2,442 page-enumeration
  requests versus 560 entry-validation reads.

The hierarchical shape is therefore the scenario in which encoded-prefix
range skipping may materially help. A new confirmation run with corrected
fingerprint stabilization is required before implementing Part B.

## Recommended order

1. Model lock renewals separately from the structural request budget.
2. Keep repeat-strided read placement across repeats.
3. Record ambient system-container requests once and outside client budgets.
4. Reduce serial write-control dependency waves without weakening safety.
5. Bound listing validation concurrency globally or adaptively.
6. Capture Container Apps metrics per resource and scale transition.
7. Use warm-up and additional repeats for any baseline-eligible campaign.

