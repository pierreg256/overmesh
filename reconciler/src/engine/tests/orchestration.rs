use super::*;
use overmesh_gateway::catalog::catalog_key_from_canonical;

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
        logical_blob: LogicalBlobId::parse_canonical(&payload.blob).expect("logical blob"),
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

#[tokio::test]
async fn anomalous_head_discovered_on_secondary_locks_deterministic_primary() {
    let fixture = Fixture::new(
        &[ManifestState::Committed],
        &[1],
        std::time::Duration::from_secs(60),
    )
    .await;
    let replicas = fixture
        .engine
        .ring
        .replicas_for(&fixture.logical_blob)
        .expect("placement");
    let primary_id = replicas[0].id.clone();
    let secondary_id = replicas[1].id.clone();
    let primary = if fixture.first.id() == primary_id {
        &fixture.first
    } else {
        &fixture.second
    };
    let secondary = if fixture.first.id() == secondary_id {
        &fixture.first
    } else {
        &fixture.second
    };
    let mut anomalous = fixture.history[0].signed.clone();
    anomalous.signature = "invalid-signature".to_owned();
    secondary.put_control(
        &fixture.head_object,
        anomalous.canonical_bytes().expect("anomalous head bytes"),
    );

    let report = fixture
        .engine
        .reconcile_head(
            &HeadCandidate {
                object_key: fixture.head_object.clone(),
                discovered_on: secondary_id,
            },
            &test_token().await,
        )
        .await
        .expect("quarantine anomalous head");

    assert_eq!(report.health_after, HealthState::Quarantined);
    assert_eq!(
        primary.acquired_locks(),
        [format!("locks/{}", fixture.logical_blob.path_hash())]
    );
    assert!(secondary.acquired_locks().is_empty());
}

#[tokio::test]
async fn catalog_backfill_and_one_sided_repair_copy_exact_current_head_bytes() {
    let fixture = Fixture::new(
        &[ManifestState::Committed],
        &[1],
        std::time::Duration::from_secs(60),
    )
    .await;
    let current = fixture.history[0].bytes.clone();
    fixture
        .first
        .put_control(&fixture.head_object, current.clone());
    fixture
        .second
        .put_control(&fixture.head_object, current.clone());
    let token = test_token().await;
    assert!(matches!(
        fixture
            .engine
            .reconcile_catalog_current(
                &fixture.logical_blob,
                &fixture.head_object,
                &fixture.first,
                &fixture.second,
                &token,
            )
            .await
            .expect("backfill"),
        CatalogReconciliation::Repaired
    ));
    let key = catalog_key_from_canonical(&fixture.blob).expect("catalog key");
    assert_eq!(
        fixture.first.control(&key).expect("first catalog").bytes,
        current
    );
    fixture.second.remove_control(&key);
    assert!(matches!(
        fixture
            .engine
            .reconcile_catalog_current(
                &fixture.logical_blob,
                &fixture.head_object,
                &fixture.first,
                &fixture.second,
                &token,
            )
            .await
            .expect("one-sided repair"),
        CatalogReconciliation::Repaired
    ));
    assert_eq!(
        fixture.first.control(&key).expect("first catalog").bytes,
        fixture.second.control(&key).expect("second catalog").bytes
    );
}

#[tokio::test]
async fn catalog_tamper_or_newer_state_is_reported_for_quarantine() {
    let fixture = Fixture::new(
        &[ManifestState::Committed, ManifestState::Committed],
        &[1, 2],
        std::time::Duration::from_secs(60),
    )
    .await;
    let current = fixture.history[0].bytes.clone();
    fixture
        .first
        .put_control(&fixture.head_object, current.clone());
    fixture
        .second
        .put_control(&fixture.head_object, current.clone());
    let key = catalog_key_from_canonical(&fixture.blob).expect("catalog key");
    fixture.first.put_control(&key, b"{}".to_vec());
    let token = test_token().await;
    assert!(matches!(
        fixture
            .engine
            .reconcile_catalog_current(
                &fixture.logical_blob,
                &fixture.head_object,
                &fixture.first,
                &fixture.second,
                &token,
            )
            .await
            .expect("tamper classification"),
        CatalogReconciliation::Conflict(reason) if reason.contains("tampered")
    ));

    fixture
        .first
        .put_control(&key, fixture.history[1].bytes.clone());
    fixture
        .second
        .put_control(&key, fixture.history[1].bytes.clone());
    assert!(matches!(
        fixture
            .engine
            .reconcile_catalog_current(
                &fixture.logical_blob,
                &fixture.head_object,
                &fixture.first,
                &fixture.second,
                &token,
            )
            .await
            .expect("newer classification"),
        CatalogReconciliation::Conflict(reason) if reason.contains("newer")
    ));
}

#[tokio::test]
async fn catalog_conflict_quarantines_before_tombstone_collection() {
    let fixture = Fixture::new(
        &[ManifestState::Committed, ManifestState::Tombstoned],
        &[1, 2],
        std::time::Duration::ZERO,
    )
    .await;
    let active = &fixture.history[1];
    let path_hash = fixture.logical_blob.path_hash();
    for backend in [&fixture.first, &fixture.second] {
        backend.put_control(&fixture.head_object, active.bytes.clone());
        backend.put_control(
            &format!("high-water/{path_hash}/current.json"),
            active.bytes.clone(),
        );
        backend.put_control(
            &committed_manifest_object(&active.signed.payload).expect("committed object"),
            active.bytes.clone(),
        );
    }
    let key = catalog_key_from_canonical(&fixture.blob).expect("catalog key");
    fixture.first.put_control(&key, b"{}".to_vec());
    let report = fixture
        .engine
        .reconcile_head_locked(
            &fixture.head_object,
            Some(&fixture.logical_blob),
            fixture.first.id(),
            &test_token().await,
        )
        .await
        .expect("quarantine");
    assert_eq!(report.health_after, HealthState::Quarantined);
    let quarantine_key = format!("quarantine/{path_hash}.json");
    assert!(fixture.first.control(&quarantine_key).is_some());
    assert!(fixture.second.control(&quarantine_key).is_some());
    assert!(fixture.marker_keys().is_empty());
}
