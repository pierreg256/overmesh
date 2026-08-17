use super::*;
use crate::engine::discovery::{HEAD_CURSOR_API_VERSION, HEAD_CURSOR_KEY};

#[tokio::test]
async fn incremental_discovery_is_bounded_and_n_node_reconciliation_uses_selected_rf2() {
    let signer = Arc::new(
        LocalTestManifestSigner::new(
            "test-blob-key-01",
            true,
            overmesh_gateway::manifest::KeyValidity::new(0, u64::MAX).expect("validity"),
        )
        .expect("test signer"),
    );
    let ring = signed_test_ring(&["storage-a", "storage-b", "storage-c"]);
    let backends = ["storage-a", "storage-b", "storage-c"]
        .into_iter()
        .map(|id| {
            let backend = TestBackend::new(id);
            (id.to_owned(), backend)
        })
        .collect::<Vec<_>>();
    for index in 0..5 {
        backends[0]
            .1
            .put_control(&format!("heads/{index:064}.json"), vec![1]);
    }
    let shared: HashMap<String, SharedBackend> = backends
        .iter()
        .map(|(id, backend)| (id.clone(), Arc::new(backend.clone()) as SharedBackend))
        .collect();
    let engine = ReconcilerEngine::new(
        ring.clone(),
        shared.clone(),
        signer.clone(),
        Arc::new(test_token_provider()),
        Arc::new(DisabledRbacPostureAuditor),
        ReconcilerOptions {
            physical_collection_delay: Duration::ZERO,
            history_compaction_max_versions_per_cycle: 64,
            head_discovery_batch_size: 2,
            staged_block_gc_max_records_per_cycle: 256,
        },
    );
    let token = test_token().await;
    let batch = engine
        .discover_heads(&token, HeadDiscoveryMode::Incremental)
        .await
        .expect("incremental discovery");
    assert_eq!(batch.candidates.len(), 2);
    assert_eq!(backends[0].1.state.list_calls.load(Ordering::SeqCst), 1);
    assert_eq!(backends[1].1.state.list_calls.load(Ordering::SeqCst), 0);
    assert_eq!(backends[2].1.state.list_calls.load(Ordering::SeqCst), 0);
    let expected_backend_cursor = batch
        .next_cursor
        .as_ref()
        .expect("next cursor")
        .cursor
        .backend_cursor
        .clone();
    engine
        .persist_cursor(batch.next_cursor.expect("next cursor"), &token)
        .await
        .expect("persist cursor");
    let persisted = engine
        .load_cursor(HEAD_CURSOR_KEY, HEAD_CURSOR_API_VERSION, &token)
        .await
        .expect("reload cursor");
    assert_eq!(persisted.cursor.backend_cursor, expected_backend_cursor);
    let audit = engine
        .discover_heads(&token, HeadDiscoveryMode::FullAudit)
        .await
        .expect("full audit discovery");
    assert_eq!(audit.candidates.len(), 5);
    assert!(audit.next_cursor.is_none());
    assert_eq!(backends[1].1.state.list_calls.load(Ordering::SeqCst), 1);
    assert_eq!(backends[2].1.state.list_calls.load(Ordering::SeqCst), 1);

    let blob = "/test-account/container/routed";
    let logical_blob = LogicalBlobId::parse_canonical(blob).expect("logical blob");
    let head = head_object_key(&logical_blob);
    let selected = ring.replicas_for(&logical_blob).expect("placement");
    let selected_ids = selected
        .iter()
        .map(|node| node.id.clone())
        .collect::<HashSet<_>>();
    let discovered_on = selected[0].id.clone();
    let report = engine
        .reconcile_head_locked(&head, Some(&logical_blob), &discovered_on, &token)
        .await
        .expect("selected RF2 reconciliation");
    assert_eq!(report.health_after, HealthState::Absent);
    for (_, backend) in &backends {
        assert_eq!(
            backend.head_get_calls(&head),
            usize::from(selected_ids.contains(backend.id())),
            "head validation did not follow Ring placement for {}",
            backend.id()
        );
    }

    let recreated = ReconcilerEngine::new(
        ring,
        shared,
        signer,
        Arc::new(test_token_provider()),
        Arc::new(DisabledRbacPostureAuditor),
        ReconcilerOptions {
            physical_collection_delay: Duration::ZERO,
            history_compaction_max_versions_per_cycle: 64,
            head_discovery_batch_size: 2,
            staged_block_gc_max_records_per_cycle: 256,
        },
    );
    let durable = recreated
        .load_cursor(HEAD_CURSOR_KEY, HEAD_CURSOR_API_VERSION, &token)
        .await
        .expect("cursor survives engine recreation");
    assert_eq!(durable.cursor.backend_cursor, expected_backend_cursor);
    backends[2]
        .1
        .put_control(HEAD_CURSOR_KEY, b"tampered".to_vec());
    assert!(
        recreated
            .load_cursor(HEAD_CURSOR_KEY, HEAD_CURSOR_API_VERSION, &token)
            .await
            .is_err(),
        "tampered cursor must fail closed"
    );
}

#[tokio::test]
async fn noncanonical_head_blob_is_quarantined_fail_closed() {
    let signer = Arc::new(
        LocalTestManifestSigner::new(
            "test-blob-key-01",
            true,
            overmesh_gateway::manifest::KeyValidity::new(0, u64::MAX).expect("validity"),
        )
        .expect("test signer"),
    );
    let ring = signed_test_ring(&["storage-a", "storage-b"]);
    let first = TestBackend::new("storage-a");
    let second = TestBackend::new("storage-b");
    let engine = ReconcilerEngine::new(
        ring,
        HashMap::from([
            (first.id.clone(), Arc::new(first.clone()) as SharedBackend),
            (second.id.clone(), Arc::new(second.clone()) as SharedBackend),
        ]),
        signer.clone(),
        Arc::new(test_token_provider()),
        Arc::new(DisabledRbacPostureAuditor),
        ReconcilerOptions {
            physical_collection_delay: Duration::ZERO,
            history_compaction_max_versions_per_cycle: 64,
            head_discovery_batch_size: 2,
            staged_block_gc_max_records_per_cycle: 256,
        },
    );
    let blob = "/test-account/container/%62lob";
    let path_hash = logical_path_hash(blob);
    let head_object = format!("heads/{path_hash}.json");
    let signed = signed_manifest(ManifestFixtureInput {
        blob,
        path_hash: &path_hash,
        version: 1,
        state: ManifestState::Committed,
        previous: None,
        committed_at: 1,
        signer: signer.as_ref(),
        replicas: &["storage-a", "storage-b"],
    })
    .await;
    first.put_control(
        &head_object,
        signed.canonical_bytes().expect("canonical head bytes"),
    );

    let report = engine
        .reconcile_head_locked(&head_object, None, first.id(), &test_token().await)
        .await
        .expect("quarantine");
    assert_eq!(report.health_after, HealthState::Quarantined);
    let quarantine_key = format!("quarantine/{path_hash}.json");
    assert!(first.control(&quarantine_key).is_some());
    assert!(second.control(&quarantine_key).is_some());
    assert_eq!(first.delete_calls(), 0);
    assert_eq!(second.delete_calls(), 0);
}
