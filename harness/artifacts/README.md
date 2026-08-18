# Artifacts

Release evidence retained by the harness is versioned here.

Each release directory contains a signed consolidated bundle and the
machine-readable source evidence it hashes. Evidence must not contain access
tokens, credentials, secrets, or transient Azure request headers.

Temporary harness output remains under `.harness/` and is not versioned.

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
