use std::{
    collections::HashMap,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use http::StatusCode;

use super::*;
use crate::{
    backend::{BackendLease, DataObjectProperties, PutResult},
    identity::{CallerToken, ControlToken, ControlTokenProvider},
    manifest::LocalTestManifestSigner,
    read::{ReadError, ReadService},
    resource::LogicalBlobId,
    ring::RingNode,
    upload::spool_body,
};

struct TestControlTokenProvider;

#[async_trait]
impl ControlTokenProvider for TestControlTokenProvider {
    async fn token(&self) -> anyhow::Result<ControlToken> {
        Ok(ControlToken::new("control-token".to_owned()))
    }
}

#[derive(Default)]
struct MemoryState {
    objects: Mutex<HashMap<String, ObjectValue>>,
    control_get_calls: Mutex<HashMap<String, u64>>,
    etag_counter: AtomicU64,
    digest_calls: AtomicU64,
    list_calls: AtomicU64,
    lease_held: AtomicBool,
    fail_prefix: Mutex<Option<String>>,
}

struct MemoryBackend {
    id: String,
    state: MemoryState,
}

impl MemoryBackend {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_owned(),
            state: MemoryState::default(),
        }
    }

    fn fail_on_prefix(&self, prefix: &str) {
        *self.state.fail_prefix.lock().expect("failure lock") = Some(prefix.to_owned());
    }

    fn clear_failure(&self) {
        *self.state.fail_prefix.lock().expect("failure lock") = None;
    }

    fn hold_lease(&self) {
        self.state.lease_held.store(true, Ordering::SeqCst);
    }

    fn object(&self, key: &str) -> Option<ObjectValue> {
        self.state
            .objects
            .lock()
            .expect("object lock")
            .get(key)
            .cloned()
    }

    fn control_get_count(&self, key: &str) -> u64 {
        self.state
            .control_get_calls
            .lock()
            .expect("control get call lock")
            .get(key)
            .copied()
            .unwrap_or(0)
    }

    fn maybe_fail(&self, key: &str) -> Result<(), BackendError> {
        if self
            .state
            .fail_prefix
            .lock()
            .expect("failure lock")
            .as_ref()
            .is_some_and(|prefix| key.starts_with(prefix))
        {
            return Err(BackendError::Http {
                status: StatusCode::SERVICE_UNAVAILABLE,
                message: "injected failure".to_owned(),
            });
        }
        Ok(())
    }

    fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: PutCondition,
    ) -> Result<PutResult, BackendError> {
        self.maybe_fail(key)?;
        let mut objects = self.state.objects.lock().expect("object lock");
        let existing = objects.get(key);
        match condition {
            PutCondition::None => {}
            PutCondition::IfAbsent if existing.is_some() => {
                return Err(BackendError::PreconditionFailed);
            }
            PutCondition::IfMatch(ref expected)
                if existing.and_then(|value| value.etag.as_ref()) != Some(expected) =>
            {
                return Err(BackendError::PreconditionFailed);
            }
            PutCondition::IfMatch(_) | PutCondition::IfAbsent => {}
        }
        let etag = format!(
            "\"memory-{}\"",
            self.state.etag_counter.fetch_add(1, Ordering::SeqCst)
        );
        objects.insert(
            key.to_owned(),
            ObjectValue {
                bytes,
                etag: Some(etag.clone()),
            },
        );
        Ok(PutResult { etag: Some(etag) })
    }
}

#[async_trait]
impl ReplicaBackend for MemoryBackend {
    fn id(&self) -> &str {
        &self.id
    }

    async fn validate_control_container(
        &self,
        _control_token: &ControlToken,
    ) -> Result<(), BackendError> {
        Ok(())
    }

    async fn control_put_bytes(
        &self,
        object_key: &str,
        bytes: Vec<u8>,
        _content_type: &'static str,
        condition: PutCondition,
        _control_token: &ControlToken,
    ) -> Result<PutResult, BackendError> {
        self.put(object_key, bytes, condition)
    }

    async fn authorize_blob_read(
        &self,
        _blob: &LogicalBlobId,
        _caller_token: &CallerToken,
    ) -> Result<(), BackendError> {
        Ok(())
    }

    async fn authorize_blob_delete(
        &self,
        _blob: &LogicalBlobId,
        _caller_token: &CallerToken,
    ) -> Result<(), BackendError> {
        Ok(())
    }

    async fn caller_head_data_object(
        &self,
        container: &str,
        object_key: &str,
        _caller_token: &CallerToken,
    ) -> Result<Option<DataObjectProperties>, BackendError> {
        let key = format!("data/{container}/{object_key}");
        self.maybe_fail(&key)?;
        Ok(self.object(&key).map(|value| DataObjectProperties {
            length: u64::try_from(value.bytes.len()).expect("test content length"),
        }))
    }

    async fn caller_get_data_range(
        &self,
        container: &str,
        object_key: &str,
        range: Option<(u64, u64)>,
        _caller_token: &CallerToken,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        let key = format!("data/{container}/{object_key}");
        self.maybe_fail(&key)?;
        let Some(value) = self.object(&key) else {
            return Ok(None);
        };
        let bytes = match range {
            Some((start, end)) => {
                let start = usize::try_from(start)
                    .map_err(|_| BackendError::InvalidResponse("invalid range".to_owned()))?;
                let end = usize::try_from(end)
                    .map_err(|_| BackendError::InvalidResponse("invalid range".to_owned()))?;
                value
                    .bytes
                    .get(start..=end)
                    .ok_or_else(|| {
                        BackendError::InvalidResponse("range outside content".to_owned())
                    })?
                    .to_vec()
            }
            None => value.bytes,
        };
        Ok(Some(bytes))
    }

    async fn caller_put_data_file(
        &self,
        container: &str,
        object_key: &str,
        path: &Path,
        _length: u64,
        condition: PutCondition,
        _caller_token: &CallerToken,
    ) -> Result<PutResult, BackendError> {
        self.put(
            &format!("data/{container}/{object_key}"),
            tokio::fs::read(path).await?,
            condition,
        )
    }

    async fn caller_digest_data_object(
        &self,
        container: &str,
        object_key: &str,
        _caller_token: &CallerToken,
    ) -> Result<Option<crate::backend::ObjectDigest>, BackendError> {
        self.state.digest_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .object(&format!("data/{container}/{object_key}"))
            .map(|value| crate::backend::ObjectDigest {
                length: u64::try_from(value.bytes.len()).expect("test length"),
                sha256: sha256_bytes(&value.bytes),
            }))
    }

    async fn control_get_object(
        &self,
        object_key: &str,
        _control_token: &ControlToken,
    ) -> Result<Option<ObjectValue>, BackendError> {
        *self
            .state
            .control_get_calls
            .lock()
            .expect("control get call lock")
            .entry(object_key.to_owned())
            .or_default() += 1;
        Ok(self.object(object_key))
    }

    async fn control_list_objects(
        &self,
        prefix: &str,
        _control_token: &ControlToken,
    ) -> Result<Vec<String>, BackendError> {
        self.state.list_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .state
            .objects
            .lock()
            .expect("object lock")
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect())
    }

    async fn control_delete_object(
        &self,
        object_key: &str,
        expected_etag: Option<&str>,
        _control_token: &ControlToken,
    ) -> Result<(), BackendError> {
        let mut objects = self.state.objects.lock().expect("object lock");
        if let Some(expected) = expected_etag
            && objects
                .get(object_key)
                .and_then(|value| value.etag.as_deref())
                != Some(expected)
        {
            return Err(BackendError::PreconditionFailed);
        }
        objects.remove(object_key);
        Ok(())
    }

    async fn control_acquire_lock(
        &self,
        object_key: &str,
        _control_token: &ControlToken,
    ) -> Result<BackendLease, BackendError> {
        self.state
            .lease_held
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| BackendError::LeaseConflict)?;
        Ok(BackendLease {
            object_key: object_key.to_owned(),
            lease_id: "memory-lease".to_owned(),
        })
    }

    async fn control_release_lock(
        &self,
        _lease: &BackendLease,
        _control_token: &ControlToken,
    ) -> Result<(), BackendError> {
        self.state.lease_held.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn control_renew_lock(
        &self,
        _lease: &BackendLease,
        _control_token: &ControlToken,
    ) -> Result<(), BackendError> {
        if self.state.lease_held.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(BackendError::LeaseConflict)
        }
    }

    async fn service_get_data_object(
        &self,
        container: &str,
        object_key: &str,
        _control_token: &ControlToken,
    ) -> Result<Option<ObjectValue>, BackendError> {
        Ok(self.object(&format!("data/{container}/{object_key}")))
    }

    async fn service_validate_data_object(
        &self,
        container: &str,
        object_key: &str,
        block_lengths: &[u64],
        _control_token: &ControlToken,
    ) -> Result<Option<crate::backend::DataObjectValidation>, BackendError> {
        let Some(value) = self.object(&format!("data/{container}/{object_key}")) else {
            return Ok(None);
        };
        let mut offset = 0_usize;
        let mut block_sha256 = Vec::with_capacity(block_lengths.len());
        for length in block_lengths {
            let length = usize::try_from(*length)
                .map_err(|_| BackendError::InvalidResponse("invalid block length".to_owned()))?;
            let end = offset
                .checked_add(length)
                .ok_or_else(|| BackendError::InvalidResponse("block length overflow".to_owned()))?;
            let block = value.bytes.get(offset..end).ok_or_else(|| {
                BackendError::InvalidResponse(
                    "content is shorter than the committed block layout".to_owned(),
                )
            })?;
            block_sha256.push(sha256_bytes(block));
            offset = end;
        }
        if offset != value.bytes.len() {
            return Err(BackendError::InvalidResponse(
                "content exceeds the committed block layout".to_owned(),
            ));
        }
        Ok(Some(crate::backend::DataObjectValidation {
            digest: crate::backend::ObjectDigest {
                length: u64::try_from(value.bytes.len()).expect("test content length"),
                sha256: sha256_bytes(&value.bytes),
            },
            block_sha256,
        }))
    }

    async fn service_put_data_bytes(
        &self,
        container: &str,
        object_key: &str,
        bytes: Vec<u8>,
        condition: PutCondition,
        _control_token: &ControlToken,
    ) -> Result<PutResult, BackendError> {
        self.put(&format!("data/{container}/{object_key}"), bytes, condition)
    }

    async fn service_delete_data_object(
        &self,
        container: &str,
        object_key: &str,
        expected_etag: Option<&str>,
        _control_token: &ControlToken,
    ) -> Result<(), BackendError> {
        let key = format!("data/{container}/{object_key}");
        let mut objects = self.state.objects.lock().expect("object lock");
        if let Some(expected) = expected_etag
            && objects.get(&key).and_then(|value| value.etag.as_deref()) != Some(expected)
        {
            return Err(BackendError::PreconditionFailed);
        }
        objects.remove(&key);
        Ok(())
    }
}

fn coordinator(primary: Arc<MemoryBackend>, secondary: Arc<MemoryBackend>) -> CommitCoordinator {
    CommitCoordinator::new(
        primary,
        secondary,
        Arc::new(
            LocalTestManifestSigner::new(
                "test-blob-key-01",
                true,
                crate::manifest::KeyValidity::new(0, u64::MAX).expect("validity"),
            )
            .expect("test signer"),
        ),
        Arc::new(TestControlTokenProvider),
        1,
    )
}

fn read_fixture(
    path: &str,
) -> (
    CommitCoordinator,
    ReadService,
    Arc<MemoryBackend>,
    Arc<MemoryBackend>,
) {
    let storage_a = Arc::new(MemoryBackend::new("storage-a"));
    let storage_b = Arc::new(MemoryBackend::new("storage-b"));
    let ring = Arc::new(RingDocument {
        api_version: "overmesh.io/v1".to_owned(),
        ring_version: 1,
        root: true,
        parent_ring_version: None,
        parent_ring_hash: None,
        replication_factor: 2,
        created_at: "2026-08-15T10:00:00Z".to_owned(),
        signed_at_unix_ms: 1_776_000_000_000,
        signing_key_id: "test-ring-key-01".to_owned(),
        ring_hash: "not-used-by-read-tests".to_owned(),
        nodes: vec![
            RingNode {
                id: "storage-a".to_owned(),
                region: "local-a".to_owned(),
                weight: 100,
            },
            RingNode {
                id: "storage-b".to_owned(),
                region: "local-b".to_owned(),
                weight: 100,
            },
        ],
    });
    let placed = ring
        .replicas_for(blob(path).canonical())
        .expect("read placement");
    let primary = if placed[0].id == "storage-a" {
        storage_a.clone()
    } else {
        storage_b.clone()
    };
    let secondary = if placed[1].id == "storage-a" {
        storage_a.clone()
    } else {
        storage_b.clone()
    };
    let signer: Arc<dyn ManifestSigner> = Arc::new(
        LocalTestManifestSigner::new(
            "test-blob-key-01",
            true,
            crate::manifest::KeyValidity::new(0, u64::MAX).expect("validity"),
        )
        .expect("test signer"),
    );
    let control_tokens: SharedControlTokenProvider = Arc::new(TestControlTokenProvider);
    let coordinator = CommitCoordinator::new(
        primary.clone(),
        secondary.clone(),
        signer.clone(),
        control_tokens.clone(),
        1,
    );
    let mut backends: HashMap<String, SharedBackend> = HashMap::new();
    backends.insert("storage-a".to_owned(), storage_a);
    backends.insert("storage-b".to_owned(), storage_b);
    let read_service = ReadService::new(ring, backends, signer, control_tokens);
    (coordinator, read_service, primary, secondary)
}

fn principal() -> AuthenticatedPrincipal {
    AuthenticatedPrincipal {
        subject: "test-subject".to_owned(),
        tenant_id: "test-tenant".to_owned(),
        object_id: "00000000-0000-0000-0000-000000000001".to_owned(),
        authorized_party: Some("00000000-0000-0000-0000-000000000002".to_owned()),
        access_token: CallerToken::new("caller-token".to_owned()),
    }
}

fn blob(path: &str) -> LogicalBlobId {
    LogicalBlobId::parse("test-account", path).expect("logical blob")
}

async fn commit(
    coordinator: &CommitCoordinator,
    path: &str,
    write_id: &str,
    content: &SpoolContent,
    condition: LogicalCondition,
) -> Result<CommitResult, CommitError> {
    coordinator
        .put_blob(&blob(path), &principal(), write_id, content, condition)
        .await
}

async fn delete(
    coordinator: &CommitCoordinator,
    path: &str,
    write_id: &str,
    condition: LogicalCondition,
) -> Result<DeleteResult, CommitError> {
    coordinator
        .delete_blob(&blob(path), &principal(), write_id, condition)
        .await
}

#[tokio::test]
async fn commits_signed_tombstone_to_both_replicas() {
    let (coordinator, read_service, primary, secondary) = read_fixture("/container/deleted");
    let content = spool_body(Body::from("hello"), 4).await.expect("content");
    commit(
        &coordinator,
        "/container/deleted",
        "write-1",
        &content,
        LogicalCondition::None,
    )
    .await
    .expect("commit");
    let head_key = format!("heads/{}.json", blob("/container/deleted").path_hash());
    let committed_head = primary.object(&head_key).expect("committed head");
    let committed =
        SignedDocument::<CommitManifest>::from_bytes(&committed_head.bytes).expect("manifest");
    let content_key = format!(
        "data/{}/{}",
        committed.payload.content_container, committed.payload.content_object
    );

    let result = delete(
        &coordinator,
        "/container/deleted",
        "delete-1",
        LogicalCondition::None,
    )
    .await
    .expect("delete");
    assert_eq!(result.logical_version, 2);
    assert!(!result.idempotent_replay);

    let primary_head = primary.object(&head_key).expect("primary tombstone");
    let secondary_head = secondary.object(&head_key).expect("secondary tombstone");
    assert_eq!(primary_head.bytes, secondary_head.bytes);
    let tombstone =
        SignedDocument::<CommitManifest>::from_bytes(&primary_head.bytes).expect("tombstone");
    tombstone
        .verify(
            SignatureDomain::CommitManifest,
            &tombstone.payload.signing_key_id,
            coordinator.signer.as_ref(),
        )
        .expect("tombstone signature");
    assert_eq!(tombstone.payload.state, ManifestState::Tombstoned);
    assert_eq!(
        tombstone.payload.previous_logical_etag,
        Some(committed.payload.logical_etag)
    );
    assert!(tombstone.payload.deleted_at_unix_ms.is_some());
    assert!(primary.object(&content_key).is_some());
    assert!(secondary.object(&content_key).is_some());
    assert!(matches!(
        read_service
            .head_blob(&blob("/container/deleted"), &principal())
            .await,
        Err(ReadError::NotFound)
    ));
}

#[tokio::test]
async fn delete_retry_is_idempotent_and_repairs_partial_head_publication() {
    let (coordinator, _, primary, secondary) = read_fixture("/container/delete-retry");
    let content = spool_body(Body::from("hello"), 4).await.expect("content");
    commit(
        &coordinator,
        "/container/delete-retry",
        "write-1",
        &content,
        LogicalCondition::None,
    )
    .await
    .expect("commit");
    let head_key = format!("heads/{}.json", blob("/container/delete-retry").path_hash());
    secondary.fail_on_prefix(&head_key);
    assert!(matches!(
        delete(
            &coordinator,
            "/container/delete-retry",
            "delete-1",
            LogicalCondition::None
        )
        .await,
        Err(CommitError::Ambiguous)
    ));
    secondary.clear_failure();

    let result = delete(
        &coordinator,
        "/container/delete-retry",
        "delete-1",
        LogicalCondition::None,
    )
    .await
    .expect("idempotent retry");
    assert!(result.idempotent_replay);
    assert_eq!(
        primary.object(&head_key).expect("primary").bytes,
        secondary.object(&head_key).expect("secondary").bytes
    );
}

#[tokio::test]
async fn tombstone_allows_a_new_if_absent_generation_without_resurrection() {
    let (coordinator, read_service, _, _) = read_fixture("/container/recreated");
    let first = spool_body(Body::from("old"), 4).await.expect("first");
    commit(
        &coordinator,
        "/container/recreated",
        "write-1",
        &first,
        LogicalCondition::None,
    )
    .await
    .expect("first commit");
    delete(
        &coordinator,
        "/container/recreated",
        "delete-1",
        LogicalCondition::None,
    )
    .await
    .expect("delete");
    let second = spool_body(Body::from("new"), 4).await.expect("second");
    let recreated = commit(
        &coordinator,
        "/container/recreated",
        "write-2",
        &second,
        LogicalCondition::IfAbsent,
    )
    .await
    .expect("recreate");
    assert_eq!(recreated.logical_version, 3);
    let read = read_service
        .get_blob(&blob("/container/recreated"), &principal(), None)
        .await
        .expect("read recreated blob");
    assert_eq!(to_bytes(read.body, usize::MAX).await.expect("body"), "new");
}

#[tokio::test]
async fn compaction_floor_rejects_replayed_head_and_high_water_below_the_floor() {
    let path = "/container/compacted-replay";
    let (coordinator, read_service, primary, secondary) = read_fixture(path);
    let first_content = spool_body(Body::from("first"), 4).await.expect("first");
    commit(
        &coordinator,
        path,
        "write-1",
        &first_content,
        LogicalCondition::None,
    )
    .await
    .expect("first commit");
    let path_hash = blob(path).path_hash();
    let head_key = format!("heads/{path_hash}.json");
    let first_head = primary.object(&head_key).expect("first head");
    let first =
        SignedDocument::<CommitManifest>::from_bytes(&first_head.bytes).expect("first manifest");
    let second_content = spool_body(Body::from("second"), 4).await.expect("second");
    commit(
        &coordinator,
        path,
        "write-2",
        &second_content,
        LogicalCondition::None,
    )
    .await
    .expect("second commit");

    let marker_bytes = b"signed-gc-marker-evidence";
    let checkpoint = SignedDocument::create(
        HistoryCompactionCheckpoint {
            api_version: "overmesh.io/history-compaction-checkpoint/v1".to_owned(),
            blob: blob(path).canonical().to_owned(),
            path_hash: path_hash.clone(),
            head_object: head_key.clone(),
            ring_version: 1,
            checkpoint_version: 1,
            compacted_through_logical_version: 1,
            compacted_through_state: ManifestState::Committed,
            compacted_through_logical_etag: first.payload.logical_etag.clone(),
            compacted_through_committed_at_unix_ms: first.payload.committed_at_unix_ms,
            covered_terminal_manifest_sha256: sha256_bytes(&first_head.bytes),
            previous_checkpoint_sha256: None,
            previous_checkpoint_version: None,
            garbage_collection_marker_object: format!(
                "garbage-collection/{path_hash}/00000000000000000001.json"
            ),
            garbage_collection_marker_sha256: sha256_bytes(marker_bytes),
            garbage_collection_through_logical_version: 1,
            garbage_collection_history_head_logical_version: 2,
            garbage_collected_committed_versions: vec![1],
            garbage_collection_delay_ms: 0,
            garbage_collected_at_unix_ms: first.payload.committed_at_unix_ms,
            compacted_at_unix_ms: first.payload.committed_at_unix_ms,
            signing_key_id: coordinator.signer.key_id().to_owned(),
        },
        SignatureDomain::HistoryCompactionCheckpoint,
        coordinator.signer.as_ref(),
    )
    .await
    .expect("checkpoint")
    .canonical_bytes()
    .expect("checkpoint bytes");
    let checkpoint_key = CommitCoordinator::history_compaction_checkpoint_key(&path_hash);
    primary
        .put(&checkpoint_key, checkpoint.clone(), PutCondition::None)
        .expect("primary checkpoint");
    secondary
        .put(&checkpoint_key, checkpoint, PutCondition::None)
        .expect("secondary checkpoint");
    let high_water_key = CommitCoordinator::high_water_current_key(&path_hash);
    for backend in [&primary, &secondary] {
        backend
            .put(&head_key, first_head.bytes.clone(), PutCondition::None)
            .expect("replayed head");
        backend
            .put(
                &high_water_key,
                first_head.bytes.clone(),
                PutCondition::None,
            )
            .expect("replayed high water");
    }

    assert!(matches!(
        read_service.head_blob(&blob(path), &principal()).await,
        Err(ReadError::VerificationFailed)
    ));
    let third_content = spool_body(Body::from("third"), 4).await.expect("third");
    assert!(matches!(
        commit(
            &coordinator,
            path,
            "write-3",
            &third_content,
            LogicalCondition::None
        )
        .await,
        Err(CommitError::VerificationFailed)
    ));
}

#[tokio::test]
async fn delete_requires_an_existing_matching_generation() {
    let primary = Arc::new(MemoryBackend::new("storage-a"));
    let secondary = Arc::new(MemoryBackend::new("storage-b"));
    let coordinator = coordinator(primary, secondary);
    assert!(matches!(
        delete(
            &coordinator,
            "/container/missing",
            "delete-1",
            LogicalCondition::None
        )
        .await,
        Err(CommitError::NotFound)
    ));

    let content = spool_body(Body::from("hello"), 4).await.expect("content");
    commit(
        &coordinator,
        "/container/existing",
        "write-1",
        &content,
        LogicalCondition::None,
    )
    .await
    .expect("commit");
    assert!(matches!(
        delete(
            &coordinator,
            "/container/existing",
            "delete-1",
            LogicalCondition::IfMatch("\"wrong\"".to_owned())
        )
        .await,
        Err(CommitError::ConditionFailed)
    ));
}

#[tokio::test]
async fn commits_identical_heads_to_both_replicas() {
    let primary = Arc::new(MemoryBackend::new("storage-a"));
    let secondary = Arc::new(MemoryBackend::new("storage-b"));
    let coordinator = coordinator(primary.clone(), secondary.clone());
    let content = spool_body(Body::from("hello"), 4).await.expect("content");

    let result = commit(
        &coordinator,
        "/container/blob",
        "write-1",
        &content,
        LogicalCondition::None,
    )
    .await
    .expect("commit");
    assert_eq!(result.logical_version, 1);
    assert!(!result.idempotent_replay);

    let head_key = format!("heads/{}.json", blob("/container/blob").path_hash());
    let primary_head = primary.object(&head_key).expect("primary head");
    let secondary_head = secondary.object(&head_key).expect("secondary head");
    assert_eq!(primary_head.bytes, secondary_head.bytes);
    let signed = SignedDocument::<CommitManifest>::from_bytes(&primary_head.bytes).expect("head");
    assert_eq!(signed.payload.state, ManifestState::Committed);
    assert_eq!(signed.payload.prepared_replicas, ["storage-a", "storage-b"]);
    assert_eq!(primary.state.digest_calls.load(Ordering::SeqCst), 0);
    assert_eq!(secondary.state.digest_calls.load(Ordering::SeqCst), 0);
    assert_eq!(primary.state.list_calls.load(Ordering::SeqCst), 0);
    assert_eq!(secondary.state.list_calls.load(Ordering::SeqCst), 0);
    assert!(signed.payload.content_object.starts_with(&format!(
        ".overmesh/objects/{}/",
        blob("/container/blob").path_hash()
    )));
    assert!(
        !signed
            .payload
            .content_object
            .contains(&stable_component("write-1"))
    );
    assert!(
        !signed
            .payload
            .content_object
            .contains(content.content_sha256.trim_start_matches("sha256:"))
    );
}

#[tokio::test]
async fn reads_validated_heads_and_ranges_across_block_boundaries() {
    let path = "/container/ranged";
    let (coordinator, read_service, _, _) = read_fixture(path);
    let content = spool_body(Body::from("abcdefghij"), 4)
        .await
        .expect("content");
    let committed = commit(
        &coordinator,
        path,
        "write-range",
        &content,
        LogicalCondition::None,
    )
    .await
    .expect("commit");

    let metadata = read_service
        .head_blob(&blob(path), &principal())
        .await
        .expect("HEAD");
    assert_eq!(metadata.logical_etag, committed.logical_etag);
    assert_eq!(metadata.content_length, 10);

    let read = read_service
        .get_blob(&blob(path), &principal(), Some("bytes=3-8"))
        .await
        .expect("GET range");
    assert_eq!(
        read.range,
        Some(crate::read::ResolvedRange {
            start: 3,
            end: 8,
            total_length: 10
        })
    );
    assert_eq!(to_bytes(read.body, 64).await.expect("range body"), "defghi");
}

#[tokio::test]
async fn head_does_not_load_block_integrity_metadata() {
    let path = "/container/head-fast-path";
    let (coordinator, read_service, primary, secondary) = read_fixture(path);
    let content = spool_body(Body::from("abcdefghij"), 1)
        .await
        .expect("content");
    commit(
        &coordinator,
        path,
        "write-head-fast-path",
        &content,
        LogicalCondition::None,
    )
    .await
    .expect("commit");
    let head = primary
        .object(&format!("heads/{}.json", blob(path).path_hash()))
        .expect("head");
    let signed = SignedDocument::<CommitManifest>::from_bytes(&head.bytes).expect("signed head");
    let root_key = signed.payload.block_manifest_object;
    let before_primary = primary.control_get_count(&root_key);
    let before_secondary = secondary.control_get_count(&root_key);

    read_service
        .head_blob(&blob(path), &principal())
        .await
        .expect("HEAD");

    assert_eq!(primary.control_get_count(&root_key), before_primary);
    assert_eq!(secondary.control_get_count(&root_key), before_secondary);
}

#[tokio::test]
async fn range_get_loads_only_the_intersecting_block_manifest_page() {
    let path = "/container/paged-range";
    let (coordinator, read_service, primary, secondary) = read_fixture(path);
    let bytes = vec![b'x'; 1025];
    let content = spool_body(Body::from(bytes.clone()), 1)
        .await
        .expect("content");
    commit(
        &coordinator,
        path,
        "write-paged-range",
        &content,
        LogicalCondition::None,
    )
    .await
    .expect("commit");
    let head = primary
        .object(&format!("heads/{}.json", blob(path).path_hash()))
        .expect("head");
    let signed = SignedDocument::<CommitManifest>::from_bytes(&head.bytes).expect("signed head");
    let root = primary
        .object(&signed.payload.block_manifest_object)
        .expect("block manifest root");
    let signed_root =
        SignedDocument::<BlockManifest>::from_bytes(&root.bytes).expect("signed root");
    assert_eq!(signed_root.payload.pages.len(), 2);
    let first_page = &signed_root.payload.pages[0].object;
    let second_page = &signed_root.payload.pages[1].object;
    let first_before = (
        primary.control_get_count(first_page),
        secondary.control_get_count(first_page),
    );
    let second_before = (
        primary.control_get_count(second_page),
        secondary.control_get_count(second_page),
    );

    let read = read_service
        .get_blob(&blob(path), &principal(), Some("bytes=1024-1024"))
        .await
        .expect("range GET");
    assert_eq!(
        to_bytes(read.body, 8).await.expect("range body"),
        bytes[1024..]
    );
    assert_eq!(
        (
            primary.control_get_count(first_page),
            secondary.control_get_count(first_page)
        ),
        first_before
    );
    assert_eq!(
        (
            primary.control_get_count(second_page),
            secondary.control_get_count(second_page)
        ),
        (second_before.0 + 1, second_before.1 + 1)
    );
}

#[tokio::test]
async fn falls_back_only_when_the_primary_content_read_is_unavailable() {
    let path = "/container/fallback";
    let (coordinator, read_service, primary, _) = read_fixture(path);
    let content = spool_body(Body::from("fallback-content"), 4)
        .await
        .expect("content");
    commit(
        &coordinator,
        path,
        "write-fallback",
        &content,
        LogicalCondition::None,
    )
    .await
    .expect("commit");
    let read = read_service
        .get_blob(&blob(path), &principal(), None)
        .await
        .expect("fallback GET");
    primary.fail_on_prefix("data/");
    assert_eq!(
        to_bytes(read.body, 64).await.expect("fallback body"),
        "fallback-content"
    );
}

#[tokio::test]
async fn rejects_a_corrupted_intersecting_block_before_returning_its_bytes() {
    let path = "/container/corrupted-range";
    let (coordinator, read_service, primary, _) = read_fixture(path);
    let content = spool_body(Body::from("abcdefghij"), 4)
        .await
        .expect("content");
    commit(
        &coordinator,
        path,
        "write-corrupted",
        &content,
        LogicalCondition::None,
    )
    .await
    .expect("commit");
    let head_key = format!("heads/{}.json", blob(path).path_hash());
    let head = SignedDocument::<CommitManifest>::from_bytes(
        &primary.object(&head_key).expect("head").bytes,
    )
    .expect("signed head");
    let data_key = format!(
        "data/{}/{}",
        head.payload.content_container, head.payload.content_object
    );
    let mut corrupted = primary.object(&data_key).expect("content").bytes;
    corrupted[3] = b'X';
    primary
        .put(&data_key, corrupted, PutCondition::None)
        .expect("corrupt content");

    let read = read_service
        .get_blob(&blob(path), &principal(), Some("bytes=1-1"))
        .await
        .expect("GET plan");
    assert!(to_bytes(read.body, 64).await.is_err());
}

#[tokio::test]
async fn rejects_reading_a_head_replayed_below_the_high_water_checkpoint() {
    let path = "/container/read-replay";
    let (coordinator, read_service, primary, secondary) = read_fixture(path);
    let first = spool_body(Body::from("first"), 4).await.expect("first");
    let second = spool_body(Body::from("second"), 4).await.expect("second");
    let first_result = commit(
        &coordinator,
        path,
        "write-1",
        &first,
        LogicalCondition::None,
    )
    .await
    .expect("first commit");
    let head_key = format!("heads/{}.json", blob(path).path_hash());
    let replayed = primary.object(&head_key).expect("first head").bytes;
    commit(
        &coordinator,
        path,
        "write-2",
        &second,
        LogicalCondition::IfMatch(first_result.logical_etag),
    )
    .await
    .expect("second commit");
    primary
        .put(&head_key, replayed.clone(), PutCondition::None)
        .expect("replay primary");
    secondary
        .put(&head_key, replayed, PutCondition::None)
        .expect("replay secondary");

    assert!(matches!(
        read_service.head_blob(&blob(path), &principal()).await,
        Err(ReadError::VerificationFailed)
    ));
}

#[tokio::test]
async fn verifies_an_existing_immutable_object_only_after_a_create_conflict() {
    let backend = MemoryBackend::new("storage-a");
    let content = spool_body(Body::from("hello"), 4).await.expect("content");
    backend
        .put(
            "data/container/existing",
            b"hello".to_vec(),
            PutCondition::IfAbsent,
        )
        .expect("seed content");

    put_file_idempotent(
        &backend,
        "container",
        "existing",
        &content,
        &principal().access_token,
    )
    .await
    .expect("idempotent content");

    assert_eq!(backend.state.digest_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn returns_the_committed_result_for_an_idempotent_retry() {
    let primary = Arc::new(MemoryBackend::new("storage-a"));
    let secondary = Arc::new(MemoryBackend::new("storage-b"));
    let coordinator = coordinator(primary, secondary);
    let content = spool_body(Body::from("hello"), 4).await.expect("content");

    commit(
        &coordinator,
        "/container/blob",
        "write-1",
        &content,
        LogicalCondition::None,
    )
    .await
    .expect("first commit");
    let retry = commit(
        &coordinator,
        "/container/blob",
        "write-1",
        &content,
        LogicalCondition::None,
    )
    .await
    .expect("idempotent retry");
    assert!(retry.idempotent_replay);
    assert_eq!(retry.logical_version, 1);
}

#[tokio::test]
async fn rejects_reused_write_id_with_different_payload() {
    let primary = Arc::new(MemoryBackend::new("storage-a"));
    let secondary = Arc::new(MemoryBackend::new("storage-b"));
    let coordinator = coordinator(primary, secondary);
    let first = spool_body(Body::from("hello"), 4).await.expect("first");
    let second = spool_body(Body::from("different"), 4)
        .await
        .expect("second");

    commit(
        &coordinator,
        "/container/blob",
        "write-1",
        &first,
        LogicalCondition::None,
    )
    .await
    .expect("first commit");
    assert!(matches!(
        commit(
            &coordinator,
            "/container/blob",
            "write-1",
            &second,
            LogicalCondition::None,
        )
        .await,
        Err(CommitError::IdempotencyConflict)
    ));
}

#[tokio::test]
async fn reports_ambiguous_outcome_when_only_one_head_is_published() {
    let primary = Arc::new(MemoryBackend::new("storage-a"));
    let secondary = Arc::new(MemoryBackend::new("storage-b"));
    secondary.fail_on_prefix("heads/");
    let coordinator = coordinator(primary.clone(), secondary.clone());
    let content = spool_body(Body::from("hello"), 4).await.expect("content");

    assert!(matches!(
        commit(
            &coordinator,
            "/container/blob",
            "write-1",
            &content,
            LogicalCondition::None,
        )
        .await,
        Err(CommitError::Ambiguous)
    ));
    let head_key = format!("heads/{}.json", blob("/container/blob").path_hash());
    assert!(primary.object(&head_key).is_some());
    assert!(secondary.object(&head_key).is_none());
}

#[tokio::test]
async fn retry_completes_a_single_head_publication() {
    let primary = Arc::new(MemoryBackend::new("storage-a"));
    let secondary = Arc::new(MemoryBackend::new("storage-b"));
    secondary.fail_on_prefix("heads/");
    let coordinator = coordinator(primary.clone(), secondary.clone());
    let content = spool_body(Body::from("hello"), 4).await.expect("content");

    assert!(matches!(
        commit(
            &coordinator,
            "/container/blob",
            "write-1",
            &content,
            LogicalCondition::None,
        )
        .await,
        Err(CommitError::Ambiguous)
    ));
    secondary.fail_on_prefix("disabled-failure-prefix");
    let retry = commit(
        &coordinator,
        "/container/blob",
        "write-1",
        &content,
        LogicalCondition::None,
    )
    .await
    .expect("retry");
    assert!(retry.idempotent_replay);

    let head_key = format!("heads/{}.json", blob("/container/blob").path_hash());
    assert_eq!(
        primary.object(&head_key).expect("primary").bytes,
        secondary.object(&head_key).expect("secondary").bytes
    );
}

#[tokio::test]
async fn enforces_logical_write_conditions() {
    let primary = Arc::new(MemoryBackend::new("storage-a"));
    let secondary = Arc::new(MemoryBackend::new("storage-b"));
    let coordinator = coordinator(primary, secondary);
    let first = spool_body(Body::from("first"), 4).await.expect("first");
    let second = spool_body(Body::from("second"), 4).await.expect("second");

    let committed = commit(
        &coordinator,
        "/container/blob",
        "write-1",
        &first,
        LogicalCondition::IfAbsent,
    )
    .await
    .expect("first commit");
    assert!(matches!(
        commit(
            &coordinator,
            "/container/blob",
            "write-2",
            &second,
            LogicalCondition::IfAbsent,
        )
        .await,
        Err(CommitError::ConditionFailed)
    ));
    assert!(matches!(
        commit(
            &coordinator,
            "/container/blob",
            "write-2",
            &second,
            LogicalCondition::IfMatch("\"stale\"".to_owned()),
        )
        .await,
        Err(CommitError::ConditionFailed)
    ));
    let updated = commit(
        &coordinator,
        "/container/blob",
        "write-2",
        &second,
        LogicalCondition::IfMatch(committed.logical_etag),
    )
    .await
    .expect("conditional update");
    assert_eq!(updated.logical_version, 2);
}

#[tokio::test]
async fn rejects_a_write_when_the_blob_lease_is_held() {
    let primary = Arc::new(MemoryBackend::new("storage-a"));
    let secondary = Arc::new(MemoryBackend::new("storage-b"));
    primary.hold_lease();
    let coordinator = coordinator(primary, secondary);
    let content = spool_body(Body::from("hello"), 4).await.expect("content");

    assert!(matches!(
        commit(
            &coordinator,
            "/container/blob",
            "write-1",
            &content,
            LogicalCondition::None,
        )
        .await,
        Err(CommitError::LockConflict)
    ));
}

#[tokio::test]
async fn rejects_a_valid_head_replayed_below_the_high_water_record() {
    let primary = Arc::new(MemoryBackend::new("storage-a"));
    let secondary = Arc::new(MemoryBackend::new("storage-b"));
    let coordinator = coordinator(primary.clone(), secondary.clone());
    let first = spool_body(Body::from("first"), 4).await.expect("first");
    let second = spool_body(Body::from("second"), 4).await.expect("second");
    let third = spool_body(Body::from("third"), 4).await.expect("third");

    let first_result = commit(
        &coordinator,
        "/container/blob",
        "write-1",
        &first,
        LogicalCondition::None,
    )
    .await
    .expect("first commit");
    let head_key = format!("heads/{}.json", blob("/container/blob").path_hash());
    let replayed_head = primary.object(&head_key).expect("version one head");
    commit(
        &coordinator,
        "/container/blob",
        "write-2",
        &second,
        LogicalCondition::IfMatch(first_result.logical_etag),
    )
    .await
    .expect("second commit");

    primary
        .put(&head_key, replayed_head.bytes.clone(), PutCondition::None)
        .expect("replay primary");
    secondary
        .put(&head_key, replayed_head.bytes, PutCondition::None)
        .expect("replay secondary");

    assert!(matches!(
        commit(
            &coordinator,
            "/container/blob",
            "write-3",
            &third,
            LogicalCondition::None,
        )
        .await,
        Err(CommitError::VerificationFailed)
    ));
}

#[tokio::test]
async fn rejects_writes_to_a_signed_quarantined_blob() {
    let primary = Arc::new(MemoryBackend::new("storage-a"));
    let secondary = Arc::new(MemoryBackend::new("storage-b"));
    let signer = LocalTestManifestSigner::new(
        "test-blob-key-01",
        true,
        crate::manifest::KeyValidity::new(0, u64::MAX).expect("validity"),
    )
    .expect("test signer");
    let blob_path = "/container/quarantined";
    let blob = blob(blob_path);
    let path_hash = blob.path_hash();
    let record = SignedDocument::create(
        ReconciliationRecord {
            api_version: "overmesh.io/reconciliation/v1".to_owned(),
            blob: Some(blob.canonical().to_owned()),
            head_object: format!("heads/{path_hash}.json"),
            ring_version: 1,
            observed_at_unix_ms: 1,
            classification: crate::manifest::ReconciliationClassification::Tampered,
            action: ReconciliationRecordAction::Quarantined,
            reason: "test quarantine".to_owned(),
            source_replica: None,
            target_replica: None,
            signing_key_id: signer.key_id().to_owned(),
        },
        SignatureDomain::ReconciliationRecord,
        &signer,
    )
    .await
    .expect("signed quarantine");
    primary
        .put(
            &format!("quarantine/{path_hash}.json"),
            record.canonical_bytes().expect("record bytes"),
            PutCondition::IfAbsent,
        )
        .expect("quarantine object");
    let coordinator = CommitCoordinator::new(
        primary,
        secondary,
        Arc::new(signer),
        Arc::new(TestControlTokenProvider),
        1,
    );
    let content = spool_body(Body::from("hello"), 4).await.expect("content");

    assert!(matches!(
        coordinator
            .put_blob(
                &blob,
                &principal(),
                "write-1",
                &content,
                LogicalCondition::None,
            )
            .await,
        Err(CommitError::Quarantined)
    ));
}
