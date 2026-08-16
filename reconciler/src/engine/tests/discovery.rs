use super::*;

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
    let ring = Arc::new(test_ring(&["storage-a", "storage-b", "storage-c"]));
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
    let shared = backends
        .iter()
        .map(|(id, backend)| (id.clone(), Arc::new(backend.clone()) as SharedBackend))
        .collect();
    let cursor_path = PathBuf::from("target/reconciler-test-state")
        .join(format!("head-discovery-{}.json", Uuid::new_v4()));
    let engine = ReconcilerEngine::new(
        ring.clone(),
        shared,
        signer,
        Arc::new(test_token_provider()),
        Arc::new(DisabledRbacPostureAuditor),
        ReconcilerOptions {
            physical_collection_delay: Duration::ZERO,
            history_compaction_max_versions_per_cycle: 64,
            head_discovery_batch_size: 2,
            head_discovery_cursor_path: cursor_path.clone(),
            staged_block_gc_max_records_per_cycle: 256,
            staged_block_metadata_cursor_path: PathBuf::from(
                "target/discovery-staged-metadata-cursor-unused.json",
            ),
            staged_block_marker_cursor_path: PathBuf::from(
                "target/discovery-staged-marker-cursor-unused.json",
            ),
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
    engine
        .persist_discovery_cursor(batch.next_cursor.as_ref().expect("next cursor"))
        .expect("persist cursor");
    let persisted = engine.load_discovery_cursor().expect("reload cursor");
    assert_eq!(
        persisted.backend_cursor,
        batch
            .next_cursor
            .as_ref()
            .expect("next cursor")
            .backend_cursor
    );
    let audit = engine
        .discover_heads(&token, HeadDiscoveryMode::FullAudit)
        .await
        .expect("full audit discovery");
    assert_eq!(audit.candidates.len(), 5);
    assert!(audit.next_cursor.is_none());
    assert_eq!(backends[1].1.state.list_calls.load(Ordering::SeqCst), 1);
    assert_eq!(backends[2].1.state.list_calls.load(Ordering::SeqCst), 1);

    let blob = "/test-account/container/routed";
    let head = head_object_key(blob);
    let selected = ring.replicas_for(blob).expect("placement");
    let selected_ids = selected
        .iter()
        .map(|node| node.id.clone())
        .collect::<HashSet<_>>();
    let discovered_on = selected[0].id.clone();
    let report = engine
        .reconcile_head_locked(&head, Some(blob), &discovered_on, &token)
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
    fs::remove_file(cursor_path).expect("remove cursor");
}
