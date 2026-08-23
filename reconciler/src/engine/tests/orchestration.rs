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
        manifest: payload,
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
    let mut anomalous =
        signed_commit_state(&fixture.history[0].manifest, fixture.signer.as_ref()).await;
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

/// ADR-0012 makes the prepared manifest a state of the merged document. An
/// interrupted preparation must not hide the generation the document publishes.
#[tokio::test]
async fn an_interrupted_preparation_does_not_hide_the_published_generation() {
    let fixture = Fixture::new(
        &[ManifestState::Committed],
        &[1],
        std::time::Duration::from_secs(60),
    )
    .await;
    let active = &fixture.history[0];
    let mut prepared = active.manifest.clone();
    prepared.state = ManifestState::Prepared;
    prepared.prepared_replicas = Vec::new();
    prepared.write_id = "interrupted".to_owned();
    prepared.logical_version = active.manifest.logical_version + 1;
    prepared.previous_logical_etag = Some(active.manifest.logical_etag.clone());
    prepared.logical_etag = "\"om-v2-interrupted\"".to_owned();
    let signed = SignedDocument::create(
        BlobCommitState::new(
            &active.manifest.blob,
            active.manifest.ring_version,
            Some(active.manifest.clone()),
            Some(prepared),
            fixture.signer.key_id(),
        ),
        SignatureDomain::BlobCommitState,
        fixture.signer.as_ref(),
    )
    .await
    .expect("interrupted commit state");
    let bytes = signed.canonical_bytes().expect("interrupted bytes");
    for backend in [&fixture.first, &fixture.second] {
        backend.put_control(&fixture.head_object, bytes.clone());
    }
    let token = test_token().await;

    let validation = fixture
        .engine
        .validate_replica(&fixture.first, &fixture.head_object, &token)
        .await;
    // The commit-state and durable-history checks pass; only the block metadata
    // this fixture never publishes is missing.
    let ReplicaValidation::Incomplete { head, reason } = validation else {
        panic!("expected an incomplete replica");
    };
    assert_eq!(head.manifest, active.manifest);
    assert_eq!(reason, "the signed block manifest is missing");

    // The catalogue is repaired from the durable history entry, because a
    // non-terminal head is not catalogue truth and the Reconciler never mints
    // Gateway-owned commit state.
    let outcome = fixture
        .engine
        .reconcile_catalog_current(
            &fixture.logical_blob,
            &fixture.head_object,
            &fixture.first,
            &fixture.second,
            &token,
        )
        .await
        .expect("catalog reconciliation");
    assert!(matches!(outcome, CatalogReconciliation::Repaired));
    let catalog = catalog_key_from_canonical(&fixture.blob).expect("catalog key");
    for backend in [&fixture.first, &fixture.second] {
        assert_eq!(
            backend.control(&catalog).expect("catalog entry").bytes,
            active.bytes
        );
    }
}

/// ADR-0012 replaces the high-water current object with the per-version history
/// entry, so a head published before its history entry is incomplete rather
/// than a valid repair source.
#[tokio::test]
async fn a_head_published_before_its_history_entry_is_incomplete() {
    let fixture = Fixture::new(
        &[ManifestState::Committed],
        &[1],
        std::time::Duration::from_secs(60),
    )
    .await;
    let active = &fixture.history[0];
    for backend in [&fixture.first, &fixture.second] {
        backend.put_control(&fixture.head_object, active.bytes.clone());
    }
    fixture.first.remove_control(&active.object_key);

    let validation = fixture
        .engine
        .validate_replica(&fixture.first, &fixture.head_object, &test_token().await)
        .await;
    let ReplicaValidation::Incomplete { reason, .. } = validation else {
        panic!("expected an incomplete replica");
    };
    assert_eq!(reason, "the durable high-water checkpoint is missing");
}

/// A signed tombstone published before its durable history entry is recoverable
/// from its own terminal bytes.
#[tokio::test]
async fn a_tombstone_published_before_its_history_entry_is_recoverable() {
    let fixture = Fixture::new(
        &[ManifestState::Committed, ManifestState::Tombstoned],
        &[1, 2],
        std::time::Duration::ZERO,
    )
    .await;
    let active = &fixture.history[1];
    for backend in [&fixture.first, &fixture.second] {
        backend.put_control(&fixture.head_object, active.bytes.clone());
    }
    fixture.first.remove_control(&active.object_key);

    let validation = fixture
        .engine
        .validate_replica(&fixture.first, &fixture.head_object, &test_token().await)
        .await;
    let ReplicaValidation::RecoverableTombstone { replica, .. } = validation else {
        panic!("expected a recoverable tombstone");
    };
    assert_eq!(replica.high_water_checkpoint, active.bytes);
}

/// A head replayed below its durable history is tampered state, not an
/// incomplete publication.
#[tokio::test]
async fn a_head_replayed_below_the_durable_history_is_quarantined() {
    let fixture = Fixture::new(
        &[ManifestState::Committed, ManifestState::Committed],
        &[1, 2],
        std::time::Duration::from_secs(60),
    )
    .await;
    let replayed = &fixture.history[0];
    for backend in [&fixture.first, &fixture.second] {
        backend.put_control(&fixture.head_object, replayed.bytes.clone());
    }
    fixture.first.remove_control(&replayed.object_key);
    fixture.second.remove_control(&replayed.object_key);

    let report = fixture
        .engine
        .reconcile_head_locked(
            &fixture.head_object,
            Some(&fixture.logical_blob),
            fixture.first.id(),
            &test_token().await,
        )
        .await
        .expect("reconcile");
    assert_eq!(report.health_after, HealthState::Quarantined);
    assert!(
        report
            .detail
            .contains("replayed below the durable high-water")
    );
}

/// A PREPARED transition that reached only one replica leaves the same
/// published generation inside two different documents. ADR-0012 makes that
/// repairable drift, not an unresolvable conflict.
#[tokio::test]
async fn a_one_sided_preparation_is_repaired_rather_than_quarantined() {
    let fixture = Fixture::new(
        &[ManifestState::Committed, ManifestState::Tombstoned],
        &[1, 2],
        std::time::Duration::from_secs(60),
    )
    .await;
    let active = &fixture.history[1];
    for backend in [&fixture.first, &fixture.second] {
        backend.put_control(&fixture.head_object, active.bytes.clone());
    }

    // Only the first replica accepted the preparation.
    let mut prepared = active.manifest.clone();
    prepared.state = ManifestState::Prepared;
    prepared.prepared_replicas = Vec::new();
    prepared.write_id = "one-sided-preparation".to_owned();
    prepared.logical_version = active.manifest.logical_version + 1;
    prepared.previous_logical_etag = Some(active.manifest.logical_etag.clone());
    prepared.logical_etag = "\"om-v3-one-sided\"".to_owned();
    let one_sided = SignedDocument::create(
        BlobCommitState::new(
            &active.manifest.blob,
            active.manifest.ring_version,
            Some(active.manifest.clone()),
            Some(prepared),
            fixture.signer.key_id(),
        ),
        SignatureDomain::BlobCommitState,
        fixture.signer.as_ref(),
    )
    .await
    .expect("one-sided commit state");
    let one_sided_bytes = one_sided.canonical_bytes().expect("one-sided bytes");
    assert_ne!(one_sided_bytes, active.bytes);
    fixture
        .first
        .put_control(&fixture.head_object, one_sided_bytes.clone());

    let report = fixture
        .engine
        .reconcile_head_locked(
            &fixture.head_object,
            Some(&fixture.logical_blob),
            fixture.first.id(),
            &test_token().await,
        )
        .await
        .expect("reconcile");

    assert_ne!(report.health_after, HealthState::Quarantined);
    let quarantine_key = format!("quarantine/{}.json", fixture.logical_blob.path_hash());
    assert!(fixture.first.control(&quarantine_key).is_none());
    assert!(fixture.second.control(&quarantine_key).is_none());

    // Both replicas converged on the terminal form of the generation they
    // already published, and the never-committed preparation is gone.
    for backend in [&fixture.first, &fixture.second] {
        let bytes = backend
            .control(&fixture.head_object)
            .expect("converged commit state")
            .bytes;
        assert_eq!(bytes, active.bytes);
        let signed =
            SignedDocument::<BlobCommitState>::from_bytes(&bytes).expect("commit state document");
        assert!(signed.payload.prepared().is_none());
        assert_eq!(signed.payload.current(), Some(&active.manifest));
    }
    assert!(
        !fixture.first.control_keys("audit/").is_empty(),
        "the convergence must be recorded as a repair"
    );
}
