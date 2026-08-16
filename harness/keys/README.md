# Test Keys

The Rust harness uses a deterministic, test-only ES256 key for canonical
signature vectors.

Production trust bundles MUST never accept this key. Live Azure tests use
non-exportable Azure Key Vault or Managed HSM keys.

