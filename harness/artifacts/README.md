# Artifacts

Release evidence retained by the harness is versioned here.

Each release directory contains a signed consolidated bundle and the
machine-readable source evidence it hashes. Evidence must not contain access
tokens, credentials, secrets, or transient Azure request headers.

Temporary harness output remains under `.harness/` and is not versioned.

`releases.toml` binds every closed milestone tag to its immutable close commit
and retained evidence bundle names. Existing signed bundles remain byte-for-byte
unchanged; future campaign bundles also carry the release tag beside their
campaign commit.

## Canonical and raw forms

The committed form is deterministically redacted by
`harness/environments/azure/build-live-evidence.py` before it is signed. The
detached signature therefore covers the exact bytes published in this
directory.

The unredacted 0.9.0 archive is retained privately on the three validation
Storage Accounts identified as `st-f8e34c7193862137`,
`st-02cb721cacb9e6e0`, and `st-9a77cb7695b0adf4`, under:

```text
overmesh-system/release-evidence/0.9.0/raw/overmesh-v090-raw-evidence.tar.gz
```

Its SHA-256 is:

```text
3b3c9c0f338cc0c1aa2469f9027f06dc40e1ead161c77d81bd0908cbf7739ba6
```

The archive is replicated three ways on private endpoints with Blob versioning
and soft delete enabled. The Reconciler managed identity owns access. A holder
of the raw archive can recompute every published pseudonym and compare its raw
bundle hash with the `redaction.rawBundleSha256` field.

The unredacted 0.10.0 performance archive is retained under the same controls
on all three accounts at:

```text
overmesh-system/release-evidence/0.10.0/raw/performance-v010-raw-evidence.tar.gz
```

It contains `raw-performance.json` and `client-performance.json`. The raw JSON
hash recorded by the canonical evidence is:

```text
65a1e1d2b152d7cbf59f5c66a31af20c1095087c157a855f25fc141e93ad4748
```

The deterministic archive SHA-256 is:

```text
cd89ef907d5bcc906df6c504aedc81892ad58a0b698350b33a3b92acf7569e3c
```

The unredacted 0.10.1 request-budgeted performance archive is retained on all
three accounts at:

```text
overmesh-system/release-evidence/0.10.1/raw/performance-v0101-raw-evidence.tar.gz
```

It contains `raw-performance.json` and `client-performance.json`. The raw JSON
hash recorded by the canonical evidence is:

```text
3a8e0180cadf52f3232e916ac0e2653f26a3fc5486add8647ed1d3efa02d6a14
```

The deterministic archive SHA-256 is:

```text
2a0b34038982da78362745696af7488673b47a74d67e406332fc254e38bbd838
```

The read-stabilized 0.10.1 v3 archive is retained separately on all three
accounts at:

```text
overmesh-system/release-evidence/0.10.1/raw/performance-v0101-v3-raw-evidence.tar.gz
```

It contains the final `live-v3` `raw-performance.json` and
`client-performance.json`. The raw JSON hash recorded by the canonical
evidence is:

```text
53f1069f9b6f1e96290388932981a3c67b4edf08d77e084a1645c8e9494676c4
```

The client JSON and deterministic archive SHA-256 values are:

```text
057a43673e57c3033fcbbcabd1a5513722f2b2cdb43c5cdb07f5c4497c99e063  client-performance.json
af0f10b56e62fd6033dd56f943e8a34750ddc14b4711b68c07abaefa5c314e11  performance-v0101-v3-raw-evidence.tar.gz
```

The 0.11.0 `live-v4` request-budget baseline archive is retained on all three
accounts at:

```text
overmesh-system/release-evidence/0.11.0/raw/performance-v011-v4-raw-evidence.tar.gz
```

It contains the aggregate server telemetry reconstructed from the retained
Azure Monitor events and the original client campaign output. Their SHA-256
values and the deterministic archive hash are:

```text
018ec415b0669361c5aa67fbc6214241dde2f29aed3c662059f5c3e28146e20c  raw-performance.json
5025a60127a7ba77ddcdde04faad797b4ab6b40bb054ba2e92889a90960bad77  client-performance.json
76710de5bc2bff86a191097a50931c61d20fd44c870ba0fc870844020bb4a3e8  performance-v011-v4-raw-evidence.tar.gz
```

Independent review of the immutable v4 bundle found that 10 of 28 cases met
the p50 stability requirement on both targets: 22 qualified on Gateway and 11
on direct Storage. Direct Storage was the binding source of variance, reaching
a 2.103 within-campaign spread on `put_blob-16mib-c4`, while the Gateway maximum
was 1.259. These repeat spreads describe variation within the one-hour campaign
and do not estimate drift between campaigns run on different days. Schema v5
publishes this gate coverage and the direct-target maximum directly in its
machine-readable evidence.

The subsequent 0.11.0 `live-v5` workload completed all 9,600 measured client
operations without a client error, but its server evidence failed closed.
Three `put_block_sequence-16mib-c4` fingerprints were incomplete, and the
100 MiB block-sequence cases did not produce a stable integer backend request
budget. It is therefore retained as a signed failure diagnostic, explicitly
marked `diagnostic-not-baseline`, rather than as a performance baseline:

```text
performance-v011-v5-failed-campaign.json
performance-v011-v5-failed-campaign.sig.json
```

The corresponding unredacted diagnostic archive is retained on all three
private validation Storage Accounts at:

```text
overmesh-system/release-evidence/0.11.0/raw/performance-v011-v5-failed-campaign-raw-evidence.tar.gz
```

It contains the original `client-performance.json` and the raw failed-campaign
record. Their hashes, the archive hash, and the signed canonical hash are:

```text
4e771ee276e2d97c524522506ce1b89546314ad8b6b95e299f51f344cbcb7507  client-performance.json
54203173d215bd07c07255ed01be49fb96a5812134536f6a9a5238bef061303a  performance-v011-v5-failed-campaign.raw.json
65b06e3372be2a30d059878cf499e91a6fcbbd5e5e27b14dcf817eebc32e178a  performance-v011-v5-failed-campaign-raw-evidence.tar.gz
1b03c28e3d20015ae6558b141e6c6a66025ae4fe33ccc069af7430f92750a012  performance-v011-v5-failed-campaign.json
```
