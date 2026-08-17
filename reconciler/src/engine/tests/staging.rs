use super::*;
use crate::engine::staging::{
    STAGED_BLOCK_GC_PREFIX, STAGED_BLOCK_PREFIX, STAGED_MARKER_CURSOR_API_VERSION,
    STAGED_MARKER_CURSOR_KEY, STAGED_METADATA_CURSOR_API_VERSION, STAGED_METADATA_CURSOR_KEY,
};
use overmesh_gateway::{
    identity::CallerIdentity,
    manifest::{KeyValidity, StagedBlock},
};

struct StageFixture {
    engine: ReconcilerEngine,
    first: TestBackend,
    second: TestBackend,
    metadata_key: String,
    container: String,
    content_key: String,
    metadata_bytes: Vec<u8>,
}

async fn stage_fixture(expires_at_unix_ms: u64) -> StageFixture {
    let ring = Arc::new(test_ring(&["storage-a", "storage-b"]));
    let first = TestBackend::new("storage-a");
    let second = TestBackend::new("storage-b");
    let backends = HashMap::from([
        (first.id.clone(), Arc::new(first.clone()) as SharedBackend),
        (second.id.clone(), Arc::new(second.clone()) as SharedBackend),
    ]);
    let signer: Arc<dyn ManifestSigner> = Arc::new(
        LocalTestManifestSigner::new(
            "test-blob-key-01",
            true,
            KeyValidity::new(0, u64::MAX).expect("validity"),
        )
        .expect("signer"),
    );
    let engine = ReconcilerEngine::new(
        ring,
        backends,
        signer.clone(),
        Arc::new(test_token_provider()),
        Arc::new(DisabledRbacPostureAuditor),
        ReconcilerOptions {
            physical_collection_delay: Duration::ZERO,
            history_compaction_max_versions_per_cycle: 64,
            head_discovery_batch_size: 64,
            staged_block_gc_max_records_per_cycle: 64,
        },
    );
    let blob = "/test-account/container/staged";
    let upload_id = "upload-1";
    let block_id = "YmxvY2stMDAwMQ==";
    let path_hash = logical_path_hash(blob);
    let metadata_key = format!(
        "staged-blocks/{}/{}/{}.json",
        path_hash,
        stable_component(upload_id),
        stable_component(block_id)
    );
    let container = "container".to_owned();
    let content_key = format!(
        ".overmesh/staged/{}/{}/00000000000000000001-test",
        path_hash,
        stable_component(upload_id)
    );
    let content = b"staged content".to_vec();
    let replicas = engine.ring.replicas_for(blob).expect("placement");
    let signed = SignedDocument::create(
        StagedBlock {
            api_version: "overmesh.io/staged-block/v1".to_owned(),
            blob: blob.to_owned(),
            upload_id: upload_id.to_owned(),
            write_id: upload_id.to_owned(),
            block_id: block_id.to_owned(),
            decoded_block_id_length: 10,
            block_id_sha256: sha256_bytes(b"block-0001"),
            content_container: container.clone(),
            content_object: content_key.clone(),
            content_length: u64::try_from(content.len()).expect("length"),
            content_sha256: sha256_bytes(&content),
            base_logical_version: 0,
            base_logical_etag: None,
            ring_version: 1,
            prepared_replicas: vec![replicas[0].id.clone(), replicas[1].id.clone()],
            created_at_unix_ms: 1,
            expires_at_unix_ms,
            caller: CallerIdentity {
                subject: "subject".to_owned(),
                tenant_id: "tenant".to_owned(),
                object_id: "object".to_owned(),
                authorized_party: None,
            },
            signing_key_id: signer.key_id().to_owned(),
        },
        SignatureDomain::StagedBlock,
        signer.as_ref(),
    )
    .await
    .expect("signed stage");
    let metadata_bytes = signed.canonical_bytes().expect("metadata");
    first.put_control(&metadata_key, metadata_bytes.clone());
    second.put_control(&metadata_key, metadata_bytes.clone());
    first.put_data(&container, &content_key, content.clone());
    second.put_data(&container, &content_key, content);
    StageFixture {
        engine,
        first,
        second,
        metadata_key,
        container,
        content_key,
        metadata_bytes,
    }
}

#[tokio::test]
async fn repairs_a_missing_signed_stage_without_deleting() {
    let fixture = stage_fixture(u64::MAX).await;
    fixture.second.remove_control(&fixture.metadata_key);
    fixture
        .engine
        .reconcile_staged_blocks(&test_token().await)
        .await
        .expect("reconcile");
    assert_eq!(
        fixture
            .second
            .control(&fixture.metadata_key)
            .expect("repaired metadata")
            .bytes,
        fixture.metadata_bytes
    );
    assert_eq!(fixture.first.delete_calls(), 0);
    assert_eq!(fixture.second.delete_calls(), 0);
}

#[tokio::test]
async fn performs_zero_deletes_when_stage_content_is_tampered() {
    let fixture = stage_fixture(u64::MAX).await;
    fixture.first.put_data(
        &fixture.container,
        &fixture.content_key,
        b"tampered".to_vec(),
    );
    fixture
        .engine
        .reconcile_staged_blocks(&test_token().await)
        .await
        .expect("reconcile");
    assert_eq!(fixture.first.delete_calls(), 0);
    assert_eq!(fixture.second.delete_calls(), 0);
    let quarantine_key = format!(
        "quarantine/{}.json",
        logical_path_hash("/test-account/container/staged")
    );
    assert!(fixture.first.control(&quarantine_key).is_some());
    assert!(fixture.second.control(&quarantine_key).is_some());
}

#[tokio::test]
async fn deletes_only_after_complete_expired_stage_validation() {
    let fixture = stage_fixture(2).await;
    fixture
        .engine
        .reconcile_staged_blocks(&test_token().await)
        .await
        .expect("reconcile");
    assert!(fixture.first.control(&fixture.metadata_key).is_none());
    assert!(fixture.second.control(&fixture.metadata_key).is_none());
    assert!(
        fixture
            .first
            .data(&fixture.container, &fixture.content_key)
            .is_none()
    );
    assert!(
        fixture
            .second
            .data(&fixture.container, &fixture.content_key)
            .is_none()
    );
}

#[tokio::test]
async fn signed_gc_marker_recovers_partial_cleanup_and_one_sided_publication() {
    let fixture = stage_fixture(2).await;
    fixture.first.fail_delete_once(&format!(
        "data:{}/{}",
        fixture.container, fixture.content_key
    ));
    fixture.second.fail_delete_once(&format!(
        "data:{}/{}",
        fixture.container, fixture.content_key
    ));
    fixture
        .engine
        .reconcile_staged_blocks(&test_token().await)
        .await
        .expect("first reconciliation");
    let marker_key = fixture
        .metadata_key
        .replacen("staged-blocks/", "staged-block-gc/", 1);
    assert!(fixture.first.control(&marker_key).is_some());
    assert!(fixture.second.control(&marker_key).is_some());
    fixture.second.remove_control(&marker_key);

    fixture
        .engine
        .reconcile_staged_blocks(&test_token().await)
        .await
        .expect("recovery reconciliation");
    assert!(fixture.first.control(&marker_key).is_none());
    assert!(fixture.second.control(&marker_key).is_none());
    assert!(!fixture.first.control_keys("audit/").is_empty());
    assert!(!fixture.second.control_keys("audit/").is_empty());
    assert!(fixture.first.control(&fixture.metadata_key).is_none());
    assert!(fixture.second.control(&fixture.metadata_key).is_none());
    assert!(
        fixture
            .first
            .data(&fixture.container, &fixture.content_key)
            .is_none()
    );
    assert!(
        fixture
            .second
            .data(&fixture.container, &fixture.content_key)
            .is_none()
    );
}

#[tokio::test]
async fn independent_staged_cursors_advance_past_persistent_early_records() {
    let mut fixture = stage_fixture(u64::MAX).await;
    fixture.engine.staged_block_gc_max_records_per_cycle = 1;
    fixture.first.remove_control(&fixture.metadata_key);
    fixture.second.remove_control(&fixture.metadata_key);
    for backend in [&fixture.first, &fixture.second] {
        backend.put_control("staged-blocks/000-early.json", b"{}".to_vec());
        backend.put_control("staged-blocks/zzz-late.json", b"{}".to_vec());
        backend.put_control("staged-block-gc/000-early.json", b"{}".to_vec());
        backend.put_control("staged-block-gc/zzz-late.json", b"{}".to_vec());
    }
    let token = test_token().await;
    let (first_metadata, metadata_cursor) = fixture
        .engine
        .discover_staged_page(
            STAGED_BLOCK_PREFIX,
            STAGED_METADATA_CURSOR_KEY,
            STAGED_METADATA_CURSOR_API_VERSION,
            &token,
        )
        .await
        .expect("first metadata page");
    assert_eq!(first_metadata, ["staged-blocks/000-early.json"]);
    fixture
        .engine
        .persist_cursor(metadata_cursor, &token)
        .await
        .expect("persist metadata cursor");
    let (second_metadata, _) = fixture
        .engine
        .discover_staged_page(
            STAGED_BLOCK_PREFIX,
            STAGED_METADATA_CURSOR_KEY,
            STAGED_METADATA_CURSOR_API_VERSION,
            &token,
        )
        .await
        .expect("second metadata page");
    assert_eq!(second_metadata, ["staged-blocks/zzz-late.json"]);

    let (first_marker, marker_cursor) = fixture
        .engine
        .discover_staged_page(
            STAGED_BLOCK_GC_PREFIX,
            STAGED_MARKER_CURSOR_KEY,
            STAGED_MARKER_CURSOR_API_VERSION,
            &token,
        )
        .await
        .expect("first marker page");
    assert_eq!(first_marker, ["staged-block-gc/000-early.json"]);
    fixture
        .engine
        .persist_cursor(marker_cursor, &token)
        .await
        .expect("persist marker cursor");
    let (second_marker, _) = fixture
        .engine
        .discover_staged_page(
            STAGED_BLOCK_GC_PREFIX,
            STAGED_MARKER_CURSOR_KEY,
            STAGED_MARKER_CURSOR_API_VERSION,
            &token,
        )
        .await
        .expect("second marker page");
    assert_eq!(second_marker, ["staged-block-gc/zzz-late.json"]);
}
