# Overmesh 0.11.0 v5.1 campaign analysis

This report is an interpretive companion to the signed canonical evidence. It
is not itself signed. Reviewers should verify every finding against:

- `performance-v011-v5.1-fast-evidence.json`
- `performance-v011-v5.1-fast-evidence.sig.json`
- `performance-v011-v5.1-fast-corrected-evidence.json`
- `performance-v011-v5.1-fast-corrected-evidence.sig.json`
- `performance-v011-v5.1-listing-confirmation-evidence.json`
- `performance-v011-v5.1-listing-confirmation-evidence.sig.json`
- `performance-v011-v5.1-listing-confirmation-corrected-evidence.json`
- `performance-v011-v5.1-listing-confirmation-corrected-evidence.sig.json`

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

## Corrected fast diagnostic

- Run ID: `20260823T090649Z`
- Runtime and tooling commit:
  `101a80dec20db7b34e785650f51672c2d0f024ce`
- Gateway image digest:
  `sha256:36afe79da65863e961fe8acfe0c819da356a165dcd58e6652076edbb707370c6`
- Contract SHA-256:
  `8e5f0f0414902beac5b694aeb3497dff78f72a75e7e0000e67053468b8e64043`
- Canonical evidence SHA-256:
  `db71294b206722f5edf17c0e339d5d92a5dcddc73bd26a56f709704608dceb7b`
- Signed archive SHA-256:
  `baac7fc38f4af338d8dbd9e0e826039a6a9877119e1b2ae7b2a2201543ed1702`
- Signature status: `verifiedByKeyVault=true`
- Client execution: 2,886 measured operations, zero client errors
- Client wall time excluding fixtures: 2,846.863107 seconds
- Evidence validity: all 76 target cases valid

The corrected protocol closes every failure mode exposed by the initial fast
run:

1. Every one of the 14 read cases covers all 24 required paths and all three
   placement pairs.
2. Seven known `validate_control_container/system_container` requests are
   retained once as ambient evidence and excluded from client budgets. No
   unknown unattributed request remains.
3. Every block sequence preserves its optimized structural budget: 177
   requests at 16 MiB and 438 at 100 MiB. One 100 MiB c4 repeat records a
   `control_renew_lock` request separately without invalidating the structural
   result.
4. PUT and overwrite preserve 45 requests per operation, down from 49.
5. Every listing repeat has an exact client/server returned-entry match and
   preserves four validation reads per validated entry.
6. Container Apps telemetry is split between two pseudonymous resources. One
   averaged 19 replicas; the other averaged 24.28125 and transitioned from 20
   to 25 replicas. The previous aggregate-only representation would have
   hidden that regional asymmetry.

The request-count reductions relative to the initial diagnostic are exact:

| Path | Initial | Corrected | Reduction |
| --- | ---: | ---: | ---: |
| PUT / overwrite | 49 | 45 | 8.16% |
| Put Block Sequence 16 MiB | 181 | 177 | 2.21% |
| Put Block Sequence 100 MiB | 442 | 438 | 0.90% |

Selected Gateway median-per-run p50 signals moved as follows:

| Operation | Shape | Initial | Corrected | Change |
| --- | --- | ---: | ---: | ---: |
| PUT Blob | 1 KiB, c1 | 1,202.962 ms | 1,108.475 ms | -7.85% |
| PUT Blob | 1 MiB, c1 | 1,431.987 ms | 1,308.829 ms | -8.60% |
| PUT Blob | 16 MiB, c1 | 2,416.796 ms | 2,607.457 ms | +7.89% |
| GET Blob | 1 MiB, c1 | 379.793 ms | 341.241 ms | -10.15% |
| GET Blob | 16 MiB, c1 | 979.252 ms | 908.349 ms | -7.24% |
| Put Block Sequence | 16 MiB, c1 | 10,643.820 ms | 9,535.688 ms | -10.41% |
| Put Block Sequence | 100 MiB, c1 | 45,652.066 ms | 43,717.441 ms | -4.24% |
| Flat list | 100 entries, c4 | 990.104 ms | 618.398 ms | -37.54% |
| Flat list | 1,000 entries, c4 | 3,731.175 ms | 3,678.644 ms | -1.41% |

These latency movements are diagnostic signals, not release gates. Only 16 of
38 Gateway p50 series and 15 of 38 direct series satisfy the 1.10
within-campaign spread threshold. The campaign remains explicitly
`baselineEligible=false`, and the observed autoscale transition makes a
cross-campaign causal claim inappropriate.

The corrected fast run validates the evidence protocol and the safe runtime
optimizations. It does not authorize encoded-prefix range skipping.

## Corrected 5,000-entry listing confirmation

- Run ID: `20260823T114125Z`
- Runtime and tooling commit:
  `101a80dec20db7b34e785650f51672c2d0f024ce`
- Gateway image digest:
  `sha256:36afe79da65863e961fe8acfe0c819da356a165dcd58e6652076edbb707370c6`
- Contract SHA-256:
  `5c613886f90282efdc9ec0af93b270ba7fb120f2a238d629e1c55fa36a4899d7`
- Canonical evidence SHA-256:
  `03518c8c51bd23a39e537ea2c66f71e402d2783d06567cfe4eb91656fd6daaa1`
- Signed archive SHA-256:
  `5885986227476bc59bd056bd56553a37e4425b9e62d924cf27be476552c1069c`
- Signature status: `verifiedByKeyVault=true`
- Client wall time excluding fixtures: 362.99555 seconds
- Evidence validity: all eight direct/Gateway target cases valid

The corrected pass removes the telemetry ambiguity from the first
confirmation. Every Gateway repetition has exact client/server returned-entry
agreement, zero unattributed requests and exactly four validation reads per
validated candidate.

| Gateway case | Considered/run | Validated/run | Returned/run | Backend requests/run | Median p50 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Flat 5,000, c1 | 5,004 | 5,004 | 5,000 | 20,088 | 23,367.326 ms |
| Flat 5,000, c4 | 5,004 | 5,004 | 5,000 | 20,088 | 24,682.883 ms |
| Hierarchical 5,000, c1 | 5,004 | 54 | 50 | 1,173 | 42,609.960 ms |
| Paginated flat 5,000, c1 | 5,004 | 5,004 | 5,000 | 20,088 | 25,916.769 ms |

Part A is therefore certified: hierarchical listing validates roughly one
candidate per returned prefix rather than every descendant. The remaining
cost is enumeration. Each hierarchical run spends 927 catalogue-page requests
plus 15 quarantine prefix listings against 216 entry-validation reads.
Catalogue enumeration alone is therefore 4.29 times the validation cost while
still walking 5,004 physical keys to return 50 prefixes.

That ratio no longer authorizes Part B. Source review established that Azure
List Blobs has no `start-after` primitive and exposes only opaque
service-generated markers. The encoded-range proposal was withdrawn and the
withdrawal approved. ADR-0014 records the successor decision: a
delimiter-safe ordered catalogue encoding for a later format generation,
rather than a synthetic cursor or secondary prefix index.

The latency values remain diagnostic. Gateway p50 spread is at most 1.128 in
this pass, while the direct hierarchical case reaches 2.021. The contract is
explicitly baseline-ineligible and no release-to-release latency conclusion is
drawn.

## Recommended order

1. Model lock renewals separately from the structural request budget.
2. Keep repeat-strided read placement across repeats.
3. Record ambient system-container requests once and outside client budgets.
4. Reduce serial write-control dependency waves without weakening safety.
5. Bound listing validation concurrency globally or adaptively.
6. Capture Container Apps metrics per resource and scale transition.
7. Use warm-up and additional repeats for any baseline-eligible campaign.
