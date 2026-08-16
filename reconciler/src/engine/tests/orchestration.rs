use super::*;

#[test]
fn requires_an_explicit_parent_link_for_authority() {
    let signer = overmesh_gateway::manifest::LocalTestManifestSigner::new(
        "test-blob-key-01",
        true,
        overmesh_gateway::manifest::KeyValidity::new(0, u64::MAX).expect("validity"),
    )
    .expect("signer");
    let older = test_head(1, "\"etag-1\"", None, &signer);
    let newer = test_head(2, "\"etag-2\"", Some("\"etag-1\""), &signer);
    let unrelated = test_head(2, "\"etag-2\"", Some("\"other\""), &signer);

    assert!(authoritative_over(&newer, &older));
    assert!(!authoritative_over(&unrelated, &older));
}

fn test_head(
    version: u64,
    etag: &str,
    previous: Option<&str>,
    signer: &dyn ManifestSigner,
) -> ValidatedHead {
    let payload = CommitManifest {
        blob: "/test-account/container/blob".to_owned(),
        caller: overmesh_gateway::identity::CallerIdentity {
            tenant_id: "test-tenant".to_owned(),
            object_id: "00000000-0000-0000-0000-000000000001".to_owned(),
            subject: "test-subject".to_owned(),
            authorized_party: None,
        },
        write_id: format!("write-{version}"),
        logical_version: version,
        logical_etag: etag.to_owned(),
        previous_logical_etag: previous.map(ToOwned::to_owned),
        ring_version: 1,
        content_length: 0,
        content_sha256: sha256_bytes(b""),
        content_container: "container".to_owned(),
        content_object: "blob/.overmesh/versions/write/content".to_owned(),
        block_manifest_object: "objects/x/block-manifest.json".to_owned(),
        block_manifest_sha256: sha256_bytes(b"block"),
        version_object_prefix: None,
        committed_at_unix_ms: 1,
        deleted_at_unix_ms: None,
        state: ManifestState::Committed,
        prepared_replicas: vec!["storage-a".to_owned(), "storage-b".to_owned()],
        signing_key_id: signer.key_id().to_owned(),
    };
    ValidatedHead {
        signed: SignedDocument {
            payload,
            signed_at_unix_ms: 1,
            signature_algorithm: "ES256".to_owned(),
            signature: String::new(),
        },
        bytes: Vec::new(),
        backend_etag: None,
    }
}
