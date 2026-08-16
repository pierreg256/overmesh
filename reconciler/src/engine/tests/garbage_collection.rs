use super::*;

#[tokio::test]
async fn collects_superseded_live_generation_without_collecting_active_head() {
    let fixture = Fixture::new(
        &[ManifestState::Committed, ManifestState::Committed],
        &[1, 2],
        Duration::ZERO,
    )
    .await;
    let old = &fixture.history[0].signed.payload;
    let active = &fixture.history[1].signed.payload;
    let report = fixture.reconcile().await.expect("collection");
    assert_eq!(report.action, ReconciliationAction::GarbageCollected);
    assert!(
        fixture
            .first
            .data(&old.content_container, &old.content_object)
            .is_none()
    );
    assert!(
        fixture
            .first
            .data(&active.content_container, &active.content_object)
            .is_some()
    );
    assert!(fixture.marker(1).is_some());
    assert_eq!(fixture.history_keys().len(), 1);
    assert_eq!(fixture.history_keys()[0], fixture.history[1].object_key);
    let checkpoint = fixture.checkpoint().expect("compaction checkpoint");
    assert_eq!(
        fixture
            .second
            .control(&history_compaction_checkpoint_key(&logical_path_hash(
                &fixture.blob
            )))
            .expect("second compaction checkpoint")
            .bytes,
        checkpoint.bytes
    );
}

#[tokio::test]
async fn retains_newer_superseded_generation_until_its_successor_ages() {
    let now = now_unix_ms();
    let fixture = Fixture::new(
        &[
            ManifestState::Committed,
            ManifestState::Committed,
            ManifestState::Committed,
        ],
        &[now - 300_000, now - 200_000, now - 50_000],
        Duration::from_secs(100),
    )
    .await;
    fixture.reconcile().await.expect("incremental collection");
    let first = &fixture.history[0].signed.payload;
    let second = &fixture.history[1].signed.payload;
    assert!(
        fixture
            .first
            .data(&first.content_container, &first.content_object)
            .is_none()
    );
    assert!(
        fixture
            .first
            .data(&second.content_container, &second.content_object)
            .is_some()
    );
    assert!(fixture.marker(1).is_some());
    assert!(fixture.marker(2).is_none());
}

#[tokio::test]
async fn advances_signed_watermark_when_more_generations_age() {
    let fixture = Fixture::new(
        &[
            ManifestState::Committed,
            ManifestState::Committed,
            ManifestState::Committed,
        ],
        &[1, 2, 3],
        Duration::ZERO,
    )
    .await;
    let path_hash = logical_path_hash(&fixture.blob);
    let first_key = garbage_collection_marker_key(&path_hash, 1);
    let first_bytes = fixture.marker_bytes(1, vec![1], now_unix_ms()).await;
    fixture.first.put_control(&first_key, first_bytes.clone());
    fixture.second.put_control(&first_key, first_bytes.clone());

    fixture.reconcile().await.expect("watermark advancement");
    let second_key = garbage_collection_marker_key(&path_hash, 2);
    let second = fixture.first.control(&second_key).expect("second marker");
    let second =
        SignedDocument::<GarbageCollectionMarker>::from_bytes(&second.bytes).expect("marker JSON");
    assert_eq!(second.payload.collected_committed_versions, vec![2]);
    assert_eq!(
        second.payload.previous_marker_sha256,
        Some(sha256_bytes(&first_bytes))
    );
    assert_eq!(
        fixture
            .second
            .control(&second_key)
            .expect("replicated second marker")
            .bytes,
        second.canonical_bytes().expect("canonical second marker")
    );
}

#[tokio::test]
async fn tombstone_and_delete_recreate_chains_collect_only_superseded_commits() {
    let tombstone = Fixture::new(
        &[ManifestState::Committed, ManifestState::Tombstoned],
        &[1, 2],
        Duration::ZERO,
    )
    .await;
    tombstone.reconcile().await.expect("tombstone collection");
    assert!(tombstone.marker(1).is_some());
    assert_eq!(
        tombstone.history[1].signed.payload.state,
        ManifestState::Tombstoned
    );

    let recreated = Fixture::new(
        &[
            ManifestState::Committed,
            ManifestState::Tombstoned,
            ManifestState::Committed,
        ],
        &[1, 2, 3],
        Duration::ZERO,
    )
    .await;
    recreated.reconcile().await.expect("recreated collection");
    let old = &recreated.history[0].signed.payload;
    let active = &recreated.history[2].signed.payload;
    assert!(
        recreated
            .first
            .data(&old.content_container, &old.content_object)
            .is_none()
    );
    assert!(
        recreated
            .first
            .data(&active.content_container, &active.content_object)
            .is_some()
    );
    let marker = recreated.marker(2).expect("recreated marker");
    let marker =
        SignedDocument::<GarbageCollectionMarker>::from_bytes(&marker.bytes).expect("marker JSON");
    assert_eq!(marker.payload.collected_committed_versions, vec![1]);
    assert_eq!(
        recreated.history_keys(),
        vec![recreated.history[2].object_key.clone()]
    );
    let checkpoint = SignedDocument::<HistoryCompactionCheckpoint>::from_bytes(
        &recreated.checkpoint().expect("checkpoint").bytes,
    )
    .expect("checkpoint JSON");
    assert_eq!(checkpoint.payload.compacted_through_logical_version, 2);
    assert_eq!(
        checkpoint.payload.compacted_through_state,
        ManifestState::Tombstoned
    );
    let tombstone_prefix = recreated.history[1]
        .signed
        .payload
        .version_object_prefix
        .as_deref()
        .expect("tombstone prefix");
    assert!(recreated.first.control_keys(tombstone_prefix).is_empty());
}

#[tokio::test]
async fn retention_not_elapsed_performs_no_delete() {
    let now = now_unix_ms();
    let fixture = Fixture::new(
        &[ManifestState::Committed, ManifestState::Committed],
        &[now - 1, now],
        Duration::from_secs(60),
    )
    .await;
    let report = fixture.reconcile().await.expect("retained");
    assert_eq!(report.action, ReconciliationAction::None);
    assert_eq!(fixture.total_deletes(), 0);
    assert!(fixture.marker(1).is_none());
    assert!(fixture.checkpoint().is_none());
    assert_eq!(fixture.history_keys().len(), 2);
}

#[tokio::test]
async fn valid_one_sided_marker_is_recovered_but_conflicting_or_invalid_markers_fail() {
    let fixture = Fixture::new(
        &[ManifestState::Committed, ManifestState::Committed],
        &[1, 2],
        Duration::ZERO,
    )
    .await;
    let marker_key = garbage_collection_marker_key(&logical_path_hash(&fixture.blob), 1);
    let bytes = fixture.marker_bytes(1, vec![1], now_unix_ms()).await;
    fixture.first.put_control(&marker_key, bytes.clone());
    let report = fixture.reconcile().await.expect("marker recovery");
    assert_eq!(report.action, ReconciliationAction::GarbageCollected);
    assert_eq!(
        fixture
            .second
            .control(&marker_key)
            .expect("repaired marker")
            .bytes,
        bytes
    );

    let conflict = Fixture::new(
        &[ManifestState::Committed, ManifestState::Committed],
        &[1, 2],
        Duration::ZERO,
    )
    .await;
    let key = garbage_collection_marker_key(&logical_path_hash(&conflict.blob), 1);
    conflict
        .first
        .put_control(&key, conflict.marker_bytes(1, vec![1], now_unix_ms()).await);
    conflict.second.put_control(
        &key,
        conflict.marker_bytes(1, vec![1], now_unix_ms() + 1).await,
    );
    assert!(conflict.reconcile().await.is_err());
    assert_eq!(conflict.total_deletes(), 0);

    let invalid = Fixture::new(
        &[ManifestState::Committed, ManifestState::Committed],
        &[1, 2],
        Duration::ZERO,
    )
    .await;
    let key = garbage_collection_marker_key(&logical_path_hash(&invalid.blob), 1);
    let mut marker = SignedDocument::<GarbageCollectionMarker>::from_bytes(
        &invalid.marker_bytes(1, vec![1], now_unix_ms()).await,
    )
    .expect("marker");
    marker.signature.push('x');
    invalid.first.put_control(
        &key,
        marker.canonical_bytes().expect("invalid marker bytes"),
    );
    assert!(invalid.reconcile().await.is_err());
    assert!(invalid.second.control(&key).is_none());
    assert_eq!(invalid.total_deletes(), 0);
}

#[tokio::test]
async fn partial_checkpoint_publication_recovers_exact_bytes_before_history_delete() {
    let fixture = Fixture::new(
        &[ManifestState::Committed, ManifestState::Committed],
        &[1, 2],
        Duration::ZERO,
    )
    .await;
    let checkpoint_key = history_compaction_checkpoint_key(&logical_path_hash(&fixture.blob));
    fixture.second.fail_put_once(&checkpoint_key);
    assert!(fixture.reconcile().await.is_err());
    let first = fixture
        .first
        .control(&checkpoint_key)
        .expect("one-sided checkpoint");
    assert!(fixture.second.control(&checkpoint_key).is_none());
    assert_eq!(fixture.history_keys().len(), 2);

    fixture
        .reconcile()
        .await
        .expect("partial checkpoint recovery");
    assert_eq!(
        fixture
            .second
            .control(&checkpoint_key)
            .expect("recovered checkpoint")
            .bytes,
        first.bytes
    );
    assert_eq!(fixture.history_keys().len(), 1);
}

#[tokio::test]
async fn conflicting_checkpoints_fail_closed_without_history_delete() {
    let fixture = Fixture::new(
        &[ManifestState::Committed, ManifestState::Committed],
        &[1, 2],
        Duration::ZERO,
    )
    .await;
    let marker_bytes = fixture.marker_bytes(1, vec![1], now_unix_ms()).await;
    let marker_key = garbage_collection_marker_key(&logical_path_hash(&fixture.blob), 1);
    fixture.first.put_control(&marker_key, marker_bytes.clone());
    fixture
        .second
        .put_control(&marker_key, marker_bytes.clone());
    let checkpoint_key = history_compaction_checkpoint_key(&logical_path_hash(&fixture.blob));
    fixture.first.put_control(
        &checkpoint_key,
        fixture
            .checkpoint_bytes(1, 1, None, &marker_bytes, now_unix_ms())
            .await,
    );
    fixture.second.put_control(
        &checkpoint_key,
        fixture
            .checkpoint_bytes(1, 1, None, &marker_bytes, now_unix_ms() + 1)
            .await,
    );

    assert!(fixture.reconcile().await.is_err());
    assert_eq!(fixture.history_keys().len(), 2);
}

#[tokio::test]
async fn crash_after_checkpoint_before_history_delete_is_idempotently_completed() {
    let fixture = Fixture::new(
        &[ManifestState::Committed, ManifestState::Committed],
        &[1, 2],
        Duration::ZERO,
    )
    .await;
    let marker_bytes = fixture.marker_bytes(1, vec![1], now_unix_ms()).await;
    let marker_key = garbage_collection_marker_key(&logical_path_hash(&fixture.blob), 1);
    fixture.first.put_control(&marker_key, marker_bytes.clone());
    fixture
        .second
        .put_control(&marker_key, marker_bytes.clone());
    let checkpoint_key = history_compaction_checkpoint_key(&logical_path_hash(&fixture.blob));
    let checkpoint = fixture
        .checkpoint_bytes(1, 1, None, &marker_bytes, now_unix_ms())
        .await;
    fixture
        .first
        .put_control(&checkpoint_key, checkpoint.clone());
    fixture.second.put_control(&checkpoint_key, checkpoint);
    assert_eq!(fixture.history_keys().len(), 2);

    fixture
        .reconcile()
        .await
        .expect("post-checkpoint history cleanup");
    assert_eq!(fixture.history_keys().len(), 1);
    fixture.reconcile().await.expect("idempotent cleanup retry");
    assert_eq!(fixture.history_keys().len(), 1);
}

#[tokio::test]
async fn broken_first_successor_link_beyond_checkpoint_fails_closed() {
    let fixture = Fixture::new(
        &[
            ManifestState::Committed,
            ManifestState::Committed,
            ManifestState::Committed,
        ],
        &[1, 2, 3],
        Duration::ZERO,
    )
    .await;
    let marker_bytes = fixture.marker_bytes(1, vec![1], now_unix_ms()).await;
    let marker_key = garbage_collection_marker_key(&logical_path_hash(&fixture.blob), 1);
    fixture.first.put_control(&marker_key, marker_bytes.clone());
    fixture
        .second
        .put_control(&marker_key, marker_bytes.clone());
    let checkpoint_key = history_compaction_checkpoint_key(&logical_path_hash(&fixture.blob));
    let checkpoint = fixture
        .checkpoint_bytes(1, 1, None, &marker_bytes, now_unix_ms())
        .await;
    fixture
        .first
        .put_control(&checkpoint_key, checkpoint.clone());
    fixture.second.put_control(&checkpoint_key, checkpoint);
    fixture.first.remove_control(&fixture.history[0].object_key);
    fixture
        .second
        .remove_control(&fixture.history[0].object_key);
    let mut broken = fixture.history[1].signed.clone();
    broken.payload.previous_logical_etag = Some("\"broken-floor-link\"".to_owned());
    broken = SignedDocument::create(
        broken.payload,
        SignatureDomain::CommitManifest,
        fixture.signer.as_ref(),
    )
    .await
    .expect("broken successor");
    fixture.replace_history(
        2,
        true,
        true,
        broken.canonical_bytes().expect("broken bytes"),
    );

    assert!(fixture.reconcile().await.is_err());
    assert!(
        fixture
            .first
            .control(&fixture.history[1].object_key)
            .is_some()
    );
}

#[tokio::test]
async fn repeated_compaction_keeps_history_and_checkpoint_metadata_bounded() {
    let states = vec![ManifestState::Committed; 10];
    let timestamps = (1..=10).collect::<Vec<_>>();
    let fixture = Fixture::new_with_compaction_limit(&states, &timestamps, Duration::ZERO, 2).await;
    for _ in 0..5 {
        fixture.reconcile().await.expect("bounded compaction cycle");
    }
    assert_eq!(
        fixture.history_keys(),
        vec![fixture.history[9].object_key.clone()]
    );
    assert_eq!(fixture.marker_keys().len(), 1);
    let checkpoint = SignedDocument::<HistoryCompactionCheckpoint>::from_bytes(
        &fixture.checkpoint().expect("checkpoint").bytes,
    )
    .expect("checkpoint JSON");
    assert_eq!(checkpoint.payload.checkpoint_version, 5);
    assert_eq!(checkpoint.payload.compacted_through_logical_version, 9);
    assert_eq!(
        checkpoint.payload.compacted_through_state,
        ManifestState::Committed
    );
}

#[tokio::test]
async fn partial_delete_and_marker_publication_retries_are_idempotent() {
    let fixture = Fixture::new(
        &[ManifestState::Committed, ManifestState::Committed],
        &[1, 2],
        Duration::ZERO,
    )
    .await;
    let old = &fixture.history[0].signed.payload;
    fixture.second.fail_delete_once(&format!(
        "data:{}/{}",
        old.content_container, old.content_object
    ));
    assert!(fixture.reconcile().await.is_err());
    assert!(
        fixture
            .first
            .data(&old.content_container, &old.content_object)
            .is_none()
    );
    fixture.reconcile().await.expect("partial delete retry");
    assert!(
        fixture
            .second
            .data(&old.content_container, &old.content_object)
            .is_none()
    );

    let publication = Fixture::new(
        &[ManifestState::Committed, ManifestState::Committed],
        &[1, 2],
        Duration::ZERO,
    )
    .await;
    let marker_key = garbage_collection_marker_key(&logical_path_hash(&publication.blob), 1);
    publication.second.fail_put_once(&marker_key);
    assert!(publication.reconcile().await.is_err());
    assert!(publication.first.control(&marker_key).is_some());
    assert!(publication.second.control(&marker_key).is_none());
    publication
        .reconcile()
        .await
        .expect("partial marker publication recovery");
    assert_eq!(
        publication
            .first
            .control(&marker_key)
            .expect("first marker")
            .bytes,
        publication
            .second
            .control(&marker_key)
            .expect("second marker")
            .bytes
    );
}

#[tokio::test]
async fn one_sided_divergent_and_malformed_history_fail_before_any_delete() {
    let one_sided = Fixture::new(
        &[ManifestState::Committed, ManifestState::Committed],
        &[1, 2],
        Duration::ZERO,
    )
    .await;
    let key = high_water_history_key(
        &logical_path_hash(&one_sided.blob),
        &one_sided.history[0].signed.payload,
    );
    one_sided.second.remove_control(&key);
    assert!(one_sided.reconcile().await.is_err());
    assert_eq!(one_sided.total_deletes(), 0);

    let divergent = Fixture::new(
        &[ManifestState::Committed, ManifestState::Committed],
        &[1, 2],
        Duration::ZERO,
    )
    .await;
    divergent.replace_history(1, false, true, b"{}".to_vec());
    assert!(divergent.reconcile().await.is_err());
    assert_eq!(divergent.total_deletes(), 0);

    let malformed = Fixture::new(
        &[ManifestState::Committed, ManifestState::Committed],
        &[1, 2],
        Duration::ZERO,
    )
    .await;
    malformed.replace_history(1, true, true, b"{".to_vec());
    assert!(malformed.reconcile().await.is_err());
    assert_eq!(malformed.total_deletes(), 0);
}

#[derive(Clone, Copy, Debug)]
enum InvalidHistory {
    Signature,
    Binding,
    Version,
    State,
    Lineage,
    Timestamps,
    ContentNamespace,
    MetadataNamespace,
}

#[tokio::test]
async fn invalid_history_invariants_fail_before_any_delete() {
    for invalid in [
        InvalidHistory::Signature,
        InvalidHistory::Binding,
        InvalidHistory::Version,
        InvalidHistory::State,
        InvalidHistory::Lineage,
        InvalidHistory::Timestamps,
        InvalidHistory::ContentNamespace,
        InvalidHistory::MetadataNamespace,
    ] {
        let fixture = Fixture::new(
            &[
                ManifestState::Committed,
                ManifestState::Committed,
                ManifestState::Committed,
            ],
            &[10, 20, 30],
            Duration::ZERO,
        )
        .await;
        let target = if matches!(
            invalid,
            InvalidHistory::Lineage | InvalidHistory::Timestamps
        ) {
            2
        } else {
            1
        };
        let mut signed = fixture.history[target - 1].signed.clone();
        match invalid {
            InvalidHistory::Signature => signed.signature.push('x'),
            InvalidHistory::Binding => signed.payload.blob = "/other/container/blob".to_owned(),
            InvalidHistory::Version => signed.payload.logical_version = 2,
            InvalidHistory::State => signed.payload.state = ManifestState::Prepared,
            InvalidHistory::Lineage => {
                signed.payload.previous_logical_etag = Some("\"wrong\"".to_owned())
            }
            InvalidHistory::Timestamps => signed.payload.committed_at_unix_ms = 5,
            InvalidHistory::ContentNamespace => {
                signed.payload.content_object = ".overmesh/objects/other/content".to_owned()
            }
            InvalidHistory::MetadataNamespace => {
                signed.payload.version_object_prefix = Some("objects/other/versions/x".to_owned())
            }
        }
        if !matches!(invalid, InvalidHistory::Signature) {
            signed = SignedDocument::create(
                signed.payload,
                SignatureDomain::CommitManifest,
                fixture.signer.as_ref(),
            )
            .await
            .expect("resign invalid history");
        }
        fixture.replace_history(
            target,
            true,
            true,
            signed.canonical_bytes().expect("invalid history bytes"),
        );
        assert!(
            fixture.reconcile().await.is_err(),
            "{invalid:?} unexpectedly passed"
        );
        assert_eq!(
            fixture.total_deletes(),
            0,
            "{invalid:?} issued a destructive call"
        );
    }
}

#[tokio::test]
async fn mismatched_current_high_water_and_unknown_candidate_namespace_fail_before_delete() {
    let high_water = Fixture::new(
        &[ManifestState::Committed, ManifestState::Committed],
        &[1, 2],
        Duration::ZERO,
    )
    .await;
    let (mut first, second) = high_water.active_replicas();
    first.high_water_checkpoint = high_water.history[0].bytes.clone();
    let token = test_token().await;
    assert!(
        high_water
            .engine
            .reconcile_garbage_collection(
                &high_water.head_object,
                &high_water.first,
                &first,
                &high_water.second,
                &second,
                &token,
            )
            .await
            .is_err()
    );
    assert_eq!(high_water.total_deletes(), 0);

    let namespace = Fixture::new(
        &[ManifestState::Committed, ManifestState::Committed],
        &[1, 2],
        Duration::ZERO,
    )
    .await;
    let prefix = expected_version_prefix(
        &logical_path_hash(&namespace.blob),
        &namespace.history[0].signed.payload,
    )
    .expect("prefix");
    namespace
        .first
        .put_control(&format!("{prefix}/unexpected.bin"), vec![1]);
    assert!(namespace.reconcile().await.is_err());
    assert_eq!(namespace.total_deletes(), 0);
}
