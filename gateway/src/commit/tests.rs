use std::{
    collections::{BTreeMap, HashMap},
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
    backend::{BackendLease, DataObjectProperties, ObjectListPage, PutResult},
    identity::{CallerToken, ControlToken, ControlTokenProvider},
    manifest::LocalTestManifestSigner,
    read::{ReadError, ReadService},
    resource::LogicalBlobId,
    ring::{RingDocument, RingNode, SignedRing},
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
    control_page_calls: AtomicU64,
    max_control_page_limit: AtomicU64,
    blob_read_auth_calls: AtomicU64,
    blob_write_auth_calls: AtomicU64,
    caller_data_write_calls: AtomicU64,
    deny_blob_write: AtomicBool,
    deny_caller_data_write: AtomicBool,
    containers: Mutex<BTreeMap<String, u64>>,
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

    fn remove_object(&self, key: &str) {
        self.state.objects.lock().expect("object lock").remove(key);
    }

    fn deny_blob_write(&self, denied: bool) {
        self.state.deny_blob_write.store(denied, Ordering::SeqCst);
    }

    fn deny_caller_data_write(&self, denied: bool) {
        self.state
            .deny_caller_data_write
            .store(denied, Ordering::SeqCst);
    }

    fn caller_data_write_count(&self) -> u64 {
        self.state.caller_data_write_calls.load(Ordering::SeqCst)
    }

    fn add_container(&self, name: &str, last_modified_unix_ms: u64) {
        self.state
            .containers
            .lock()
            .expect("container lock")
            .insert(name.to_owned(), last_modified_unix_ms);
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
        self.state
            .blob_read_auth_calls
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn authorize_existing_blob_write(
        &self,
        _container: &str,
        _object_key: &str,
        _caller_token: &CallerToken,
    ) -> Result<(), BackendError> {
        self.state
            .blob_write_auth_calls
            .fetch_add(1, Ordering::SeqCst);
        if self.state.deny_blob_write.load(Ordering::SeqCst) {
            Err(BackendError::Http {
                status: StatusCode::FORBIDDEN,
                message: "write denied".to_owned(),
            })
        } else {
            Ok(())
        }
    }

    async fn authorize_account_list(
        &self,
        _caller_token: &CallerToken,
    ) -> Result<(), BackendError> {
        Ok(())
    }

    async fn authorize_container_list(
        &self,
        container: &str,
        _caller_token: &CallerToken,
    ) -> Result<(), BackendError> {
        if self
            .state
            .containers
            .lock()
            .expect("container lock")
            .contains_key(container)
        {
            Ok(())
        } else {
            Err(BackendError::Http {
                status: StatusCode::NOT_FOUND,
                message: "container not found".to_owned(),
            })
        }
    }

    async fn caller_list_containers_page(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
        _caller_token: &CallerToken,
    ) -> Result<crate::backend::BackendContainerListPage, BackendError> {
        let containers = self.state.containers.lock().expect("container lock");
        let start = parse_opaque_cursor(&self.id, cursor)?;
        let values = containers
            .iter()
            .filter(|(name, _)| name.starts_with(prefix))
            .skip(start)
            .take(limit)
            .map(
                |(name, last_modified_unix_ms)| crate::backend::BackendContainer {
                    name: name.clone(),
                    last_modified_unix_ms: *last_modified_unix_ms,
                },
            )
            .collect::<Vec<_>>();
        let total = containers
            .keys()
            .filter(|name| name.starts_with(prefix))
            .count();
        let end = start.saturating_add(values.len());
        Ok(crate::backend::BackendContainerListPage {
            next_cursor: (end < total).then(|| opaque_cursor(&self.id, end)),
            containers: values,
        })
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
        self.state
            .caller_data_write_calls
            .fetch_add(1, Ordering::SeqCst);
        if self.state.deny_caller_data_write.load(Ordering::SeqCst) {
            return Err(BackendError::Http {
                status: StatusCode::FORBIDDEN,
                message: "caller data write denied".to_owned(),
            });
        }
        self.add_container(container, 1);
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

    async fn control_list_objects_page(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
        _control_token: &ControlToken,
    ) -> Result<ObjectListPage, BackendError> {
        self.state.control_page_calls.fetch_add(1, Ordering::SeqCst);
        self.state
            .max_control_page_limit
            .fetch_max(u64::try_from(limit).expect("page limit"), Ordering::SeqCst);
        let objects = self.state.objects.lock().expect("object lock");
        let mut ordered = objects
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect::<Vec<_>>();
        ordered.sort();
        let start = parse_opaque_cursor(&self.id, cursor)?;
        let total = ordered.len();
        let values = ordered
            .into_iter()
            .skip(start)
            .take(limit)
            .collect::<Vec<_>>();
        let end = start.saturating_add(values.len());
        Ok(ObjectListPage {
            next_cursor: (end < total).then(|| opaque_cursor(&self.id, end)),
            objects: values,
        })
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
        self.add_container(container, 1);
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

fn opaque_cursor(backend_id: &str, offset: usize) -> String {
    format!("opaque::{backend_id}::{offset:08x}")
}

fn parse_opaque_cursor(backend_id: &str, cursor: Option<&str>) -> Result<usize, BackendError> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    let encoded = cursor
        .strip_prefix(&format!("opaque::{backend_id}::"))
        .ok_or_else(|| BackendError::InvalidResponse("invalid opaque test cursor".to_owned()))?;
    usize::from_str_radix(encoded, 16)
        .map_err(|_| BackendError::InvalidResponse("invalid opaque test cursor".to_owned()))
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
    let ring = Arc::new(
        SignedRing::from_document(RingDocument {
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
        })
        .expect("ring"),
    );
    let logical_blob = blob(path);
    let placed = ring.replicas_for(&logical_blob).expect("read placement");
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

fn service_fixture() -> Arc<CommitService> {
    service_fixture_parts().0
}

fn service_fixture_parts() -> (Arc<CommitService>, Arc<MemoryBackend>, Arc<MemoryBackend>) {
    service_fixture_parts_with_staging_lifetime(Duration::from_secs(7 * 24 * 60 * 60))
}

fn service_fixture_parts_with_staging_lifetime(
    staging_lifetime: Duration,
) -> (Arc<CommitService>, Arc<MemoryBackend>, Arc<MemoryBackend>) {
    let storage_a = Arc::new(MemoryBackend::new("storage-a"));
    let storage_b = Arc::new(MemoryBackend::new("storage-b"));
    let mut ring = RingDocument {
        api_version: "overmesh.io/v1".to_owned(),
        ring_version: 1,
        root: true,
        parent_ring_version: None,
        parent_ring_hash: None,
        replication_factor: 2,
        created_at: "2026-08-15T10:00:00Z".to_owned(),
        signed_at_unix_ms: 1_776_000_000_000,
        signing_key_id: "test-ring-key-01".to_owned(),
        ring_hash: String::new(),
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
    };
    ring.ring_hash = ring.computed_hash().expect("ring hash");
    let signer: Arc<dyn ManifestSigner> = Arc::new(
        LocalTestManifestSigner::new(
            "test-blob-key-01",
            true,
            crate::manifest::KeyValidity::new(0, u64::MAX).expect("validity"),
        )
        .expect("test signer"),
    );
    let backends: HashMap<String, SharedBackend> = HashMap::from([
        ("storage-a".to_owned(), storage_a.clone() as SharedBackend),
        ("storage-b".to_owned(), storage_b.clone() as SharedBackend),
    ]);
    let service = Arc::new(CommitService::new_with_options(
        Arc::new(SignedRing::from_document(ring).expect("ring")),
        backends,
        signer,
        Arc::new(TestControlTokenProvider),
        CommitServiceOptions {
            listing_token_lifetime: Duration::from_secs(15 * 60),
            staging_lifetime,
        },
    ));
    (service, storage_a, storage_b)
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

fn other_principal() -> AuthenticatedPrincipal {
    AuthenticatedPrincipal {
        subject: "other-subject".to_owned(),
        tenant_id: "test-tenant".to_owned(),
        object_id: "00000000-0000-0000-0000-000000000099".to_owned(),
        authorized_party: Some("00000000-0000-0000-0000-000000000002".to_owned()),
        access_token: CallerToken::new("other-caller-token".to_owned()),
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
async fn stages_commits_and_reports_client_block_ids() {
    use crate::block::{BlockListType, BlockSelection, BlockSelectionKind};

    let (service, storage_a, storage_b) = service_fixture_parts();
    let blocks = service.block_service();
    let logical_blob = blob("/container/blocked");
    let first = spool_body(Body::from("hello "), 4).await.expect("first");
    let second = spool_body(Body::from("world"), 4).await.expect("second");
    blocks
        .put_block(
            &logical_blob,
            &principal(),
            "upload-1",
            "upload-1",
            "YmxvY2stMDAwMQ==",
            &first,
        )
        .await
        .expect("first staged block");
    blocks
        .put_block(
            &logical_blob,
            &principal(),
            "upload-1",
            "upload-1",
            "YmxvY2stMDAwMg==",
            &second,
        )
        .await
        .expect("second staged block");
    let staged = blocks
        .get_block_list(
            &logical_blob,
            &principal(),
            Some("upload-1"),
            BlockListType::Uncommitted,
        )
        .await
        .expect("uncommitted list");
    assert_eq!(staged.uncommitted.len(), 2);
    let selections = [
        BlockSelection {
            kind: BlockSelectionKind::Latest,
            block_id: "YmxvY2stMDAwMQ==".to_owned(),
        },
        BlockSelection {
            kind: BlockSelectionKind::Latest,
            block_id: "YmxvY2stMDAwMg==".to_owned(),
        },
    ];
    blocks
        .put_block_list(
            &logical_blob,
            &principal(),
            "upload-1",
            "upload-1",
            &selections,
            LogicalCondition::None,
        )
        .await
        .expect("block list commit");
    assert!(
        blocks
            .put_block_list(
                &logical_blob,
                &principal(),
                "upload-1",
                "upload-1",
                &selections,
                LogicalCondition::None,
            )
            .await
            .expect("block list retry")
            .idempotent_replay
    );
    let catalog_key = crate::catalog::catalog_key(&logical_blob);
    let catalog_a = storage_a.object(&catalog_key).expect("catalog A");
    let catalog_b = storage_b.object(&catalog_key).expect("catalog B");
    assert_eq!(catalog_a.bytes, catalog_b.bytes);
    assert_eq!(
        catalog_a.bytes,
        storage_a
            .object(&format!("heads/{}.json", logical_blob.path_hash()))
            .expect("current head")
            .bytes
    );
    let committed = blocks
        .get_block_list(
            &logical_blob,
            &principal(),
            Some("upload-1"),
            BlockListType::Committed,
        )
        .await
        .expect("committed list");
    assert_eq!(
        committed
            .committed
            .iter()
            .map(|block| block.block_id.as_str())
            .collect::<Vec<_>>(),
        ["YmxvY2stMDAwMQ==", "YmxvY2stMDAwMg=="]
    );
    let read = service
        .read_service()
        .get_blob(&logical_blob, &principal(), None)
        .await
        .expect("read");
    assert_eq!(
        to_bytes(read.body, 1024).await.expect("body"),
        "hello world"
    );
}

#[tokio::test]
async fn tombstoned_blob_without_eligible_stages_has_no_block_list() {
    use crate::block::BlockListType;

    let service = service_fixture();
    let logical_blob = blob("/container/tombstoned-block-list");
    let content = spool_body(Body::from("content"), 4).await.expect("content");
    service
        .put_blob(
            &logical_blob,
            &principal(),
            "write",
            &content,
            LogicalCondition::None,
        )
        .await
        .expect("commit");
    service
        .delete_blob(
            &logical_blob,
            &principal(),
            "delete",
            LogicalCondition::None,
        )
        .await
        .expect("delete");
    let blocks = service.block_service();
    for list_type in [
        BlockListType::Committed,
        BlockListType::Uncommitted,
        BlockListType::All,
    ] {
        assert!(matches!(
            blocks
                .get_block_list(&logical_blob, &principal(), None, list_type)
                .await,
            Err(crate::block::BlockError::NotFound)
        ));
    }
}

#[tokio::test]
async fn put_block_retry_completes_signed_metadata_before_missing_data() {
    let (service, storage_a, _storage_b) = service_fixture_parts();
    storage_a.fail_on_prefix("data/container/.overmesh/staged/");
    let blocks = service.block_service();
    let logical_blob = blob("/container/retry-stage");
    let content = spool_body(Body::from("retry content"), 4)
        .await
        .expect("content");
    assert!(
        blocks
            .put_block(
                &logical_blob,
                &principal(),
                "retry-upload",
                "retry-upload",
                "YmxvY2stMDAwMQ==",
                &content,
            )
            .await
            .is_err()
    );
    let metadata_key = format!(
        "staged-blocks/{}/{}/{}.json",
        logical_blob.path_hash(),
        stable_component("retry-upload"),
        stable_component("YmxvY2stMDAwMQ==")
    );
    assert!(storage_a.object(&metadata_key).is_some());
    storage_a.clear_failure();
    assert!(
        blocks
            .put_block(
                &logical_blob,
                &principal(),
                "retry-upload",
                "retry-upload",
                "YmxvY2stMDAwMQ==",
                &content,
            )
            .await
            .expect("retry")
            .idempotent_replay
    );
}

#[tokio::test]
async fn put_block_retry_repairs_a_w1_upload_generation_before_replay() {
    let (service, storage_a, storage_b) = service_fixture_parts();
    let blocks = service.block_service();
    let logical_blob = blob("/container/retry-generation");
    let generation_prefix = format!("staged-uploads/{}/", logical_blob.path_hash());
    let content = spool_body(Body::from("content"), 4).await.expect("content");
    blocks
        .put_block(
            &logical_blob,
            &principal(),
            "upload",
            "write",
            "YQ==",
            &content,
        )
        .await
        .expect("initial stage");
    let generation_key = format!(
        "{}{}/generation.json",
        generation_prefix,
        stable_component("upload")
    );
    assert!(storage_a.object(&generation_key).is_some());
    storage_b.remove_object(&generation_key);
    assert!(storage_b.object(&generation_key).is_none());
    blocks
        .put_block(
            &logical_blob,
            &principal(),
            "upload",
            "write",
            "YQ==",
            &content,
        )
        .await
        .expect("retry");
    assert_eq!(
        storage_a
            .object(&generation_key)
            .expect("primary generation")
            .bytes,
        storage_b
            .object(&generation_key)
            .expect("secondary generation")
            .bytes
    );
}

#[tokio::test]
async fn put_block_list_retry_recovers_a_w1_head_publication() {
    use crate::{
        block::{BlockSelection, BlockSelectionKind},
        listing::{BlobListEntry, ListRequest},
    };

    let (service, storage_a, storage_b) = service_fixture_parts();
    let blocks = service.block_service();
    let logical_blob = blob("/container/w1-block-list");
    let content = spool_body(Body::from("w1 content"), 4)
        .await
        .expect("content");
    blocks
        .put_block(
            &logical_blob,
            &principal(),
            "w1-upload",
            "w1-upload",
            "YmxvY2stMDAwMQ==",
            &content,
        )
        .await
        .expect("stage");
    let selections = [BlockSelection {
        kind: BlockSelectionKind::Latest,
        block_id: "YmxvY2stMDAwMQ==".to_owned(),
    }];
    storage_b.fail_on_prefix("heads/");
    assert!(
        blocks
            .put_block_list(
                &logical_blob,
                &principal(),
                "w1-upload",
                "w1-upload",
                &selections,
                LogicalCondition::None,
            )
            .await
            .is_err()
    );
    storage_b.clear_failure();
    let catalog_key = crate::catalog::catalog_key(&logical_blob);
    storage_b.remove_object(&catalog_key);
    storage_b.fail_on_prefix(&catalog_key);
    assert!(
        blocks
            .put_block_list(
                &logical_blob,
                &principal(),
                "w1-upload",
                "w1-upload",
                &selections,
                LogicalCondition::None,
            )
            .await
            .is_err()
    );
    let head_key = format!("heads/{}.json", logical_blob.path_hash());
    assert!(storage_a.object(&head_key).is_some());
    assert!(storage_b.object(&head_key).is_none());
    storage_b.clear_failure();
    storage_b.deny_blob_write(true);
    assert!(matches!(
        blocks
            .put_block_list(
                &logical_blob,
                &principal(),
                "w1-upload",
                "w1-upload",
                &selections,
                LogicalCondition::None,
            )
            .await,
        Err(crate::block::BlockError::Commit(CommitError::Backend(
            BackendError::Http {
                status: StatusCode::FORBIDDEN,
                ..
            }
        )))
    ));
    assert!(storage_b.object(&head_key).is_none());
    storage_b.deny_blob_write(false);
    assert!(
        blocks
            .put_block_list(
                &logical_blob,
                &principal(),
                "w1-upload",
                "w1-upload",
                &selections,
                LogicalCondition::None,
            )
            .await
            .expect("recovered retry")
            .idempotent_replay
    );
    let page = service
        .listing_service("test-account")
        .list_blobs(
            "container",
            &ListRequest::new(String::new(), String::new(), None, Some(10), Vec::new())
                .expect("request"),
            &principal(),
        )
        .await
        .expect("listing after recovery");
    assert!(matches!(
        page.entries.as_slice(),
        [BlobListEntry::Blob(blob)] if blob.name == "w1-block-list"
    ));
}

#[tokio::test]
async fn retry_does_not_spread_an_invalid_one_sided_commit_manifest() {
    let (service, primary, secondary) = service_fixture_parts();
    let logical_blob = blob("/container/invalid-one-sided-manifest");
    let write_id = "invalid-one-sided-manifest";
    let content = spool_body(Body::from("content"), 4).await.expect("content");
    let digest = content
        .content_sha256
        .strip_prefix("sha256:")
        .expect("sha256 digest");
    let prepared_key = format!(
        "objects/{}/versions/{}/{digest}/prepared.json",
        logical_blob.path_hash(),
        stable_component(write_id)
    );
    primary
        .put(&prepared_key, b"{}".to_vec(), PutCondition::IfAbsent)
        .expect("tampered one-sided manifest");

    assert!(
        service
            .put_blob(
                &logical_blob,
                &principal(),
                write_id,
                &content,
                LogicalCondition::None,
            )
            .await
            .is_err()
    );
    assert!(secondary.object(&prepared_key).is_none());
}

#[tokio::test]
async fn implicit_block_generation_supports_per_request_client_ids() {
    use crate::block::{BlockSelection, BlockSelectionKind};

    let service = service_fixture();
    let blocks = service.block_service();
    let logical_blob = blob("/container/sdk-blocks");
    for (write_id, block_id, body) in [
        ("sdk-request-1", "YmxvY2stMDAwMQ==", "one"),
        ("sdk-request-2", "YmxvY2stMDAwMg==", "two"),
    ] {
        let content = spool_body(Body::from(body), 4).await.expect("content");
        blocks
            .put_block(
                &logical_blob,
                &principal(),
                "",
                write_id,
                block_id,
                &content,
            )
            .await
            .expect("implicit stage");
    }
    blocks
        .put_block_list(
            &logical_blob,
            &principal(),
            "",
            "sdk-request-3",
            &[
                BlockSelection {
                    kind: BlockSelectionKind::Latest,
                    block_id: "YmxvY2stMDAwMQ==".to_owned(),
                },
                BlockSelection {
                    kind: BlockSelectionKind::Latest,
                    block_id: "YmxvY2stMDAwMg==".to_owned(),
                },
            ],
            LogicalCondition::None,
        )
        .await
        .expect("implicit block commit");
}

#[tokio::test]
async fn implicit_block_list_namespace_is_scoped_to_caller_and_excludes_explicit_uploads() {
    use crate::block::BlockListType;

    let service = service_fixture();
    let blocks = service.block_service();
    let logical_blob = blob("/container/implicit-scope");
    let first = spool_body(Body::from("one"), 4).await.expect("first");
    let second = spool_body(Body::from("two"), 4).await.expect("second");
    let explicit = spool_body(Body::from("three"), 4).await.expect("explicit");
    blocks
        .put_block(
            &logical_blob,
            &principal(),
            "",
            "implicit-one",
            "YQ==",
            &first,
        )
        .await
        .expect("first implicit stage");
    blocks
        .put_block(
            &logical_blob,
            &other_principal(),
            "",
            "implicit-two",
            "Yg==",
            &second,
        )
        .await
        .expect("second caller implicit stage");
    blocks
        .put_block(
            &logical_blob,
            &principal(),
            "explicit-upload",
            "explicit",
            "Yw==",
            &explicit,
        )
        .await
        .expect("explicit stage");

    let first_list = blocks
        .get_block_list(
            &logical_blob,
            &principal(),
            None,
            BlockListType::Uncommitted,
        )
        .await
        .expect("first caller list");
    assert_eq!(
        first_list
            .uncommitted
            .iter()
            .map(|block| block.block_id.as_str())
            .collect::<Vec<_>>(),
        ["YQ=="]
    );
    let second_list = blocks
        .get_block_list(
            &logical_blob,
            &other_principal(),
            None,
            BlockListType::Uncommitted,
        )
        .await
        .expect("second caller list");
    assert_eq!(
        second_list
            .uncommitted
            .iter()
            .map(|block| block.block_id.as_str())
            .collect::<Vec<_>>(),
        ["Yg=="]
    );
}

#[tokio::test]
async fn concurrent_first_blocks_cannot_create_conflicting_upload_generations() {
    let service = service_fixture();
    let blocks = service.block_service();
    let logical_blob = blob("/container/concurrent-lengths");
    let first = spool_body(Body::from("one"), 4).await.expect("first");
    let second = spool_body(Body::from("two"), 4).await.expect("second");
    let caller = principal();
    let (first_result, second_result) = tokio::join!(
        blocks.put_block(
            &logical_blob,
            &caller,
            "shared-upload",
            "first",
            "YQ==",
            &first,
        ),
        blocks.put_block(
            &logical_blob,
            &caller,
            "shared-upload",
            "second",
            "YmI=",
            &second,
        )
    );
    assert_ne!(first_result.is_ok(), second_result.is_ok());
    let retry = if first_result.is_ok() {
        blocks
            .put_block(
                &logical_blob,
                &principal(),
                "shared-upload",
                "second",
                "YmI=",
                &second,
            )
            .await
    } else {
        blocks
            .put_block(
                &logical_blob,
                &principal(),
                "shared-upload",
                "first",
                "YQ==",
                &first,
            )
            .await
    };
    assert!(matches!(
        retry,
        Err(crate::block::BlockError::UnequalBlockIdLength)
    ));
}

#[tokio::test]
async fn concurrent_same_block_metadata_never_splits_across_replicas() {
    let (service, primary, secondary) = service_fixture_parts();
    let blocks = service.block_service();
    let logical_blob = blob("/container/concurrent-block");
    let first = spool_body(Body::from("one"), 4).await.expect("first");
    let second = spool_body(Body::from("two"), 4).await.expect("second");
    let caller = principal();
    let (first_result, second_result) = tokio::join!(
        blocks.put_block(
            &logical_blob,
            &caller,
            "shared-upload",
            "first",
            "YQ==",
            &first,
        ),
        blocks.put_block(
            &logical_blob,
            &caller,
            "shared-upload",
            "second",
            "YQ==",
            &second,
        )
    );
    assert_ne!(first_result.is_ok(), second_result.is_ok());
    let metadata_key = format!(
        "staged-blocks/{}/{}/{}.json",
        logical_blob.path_hash(),
        stable_component("shared-upload"),
        stable_component("YQ==")
    );
    assert_eq!(
        primary
            .object(&metadata_key)
            .expect("primary metadata")
            .bytes,
        secondary
            .object(&metadata_key)
            .expect("secondary metadata")
            .bytes
    );
}

#[tokio::test]
async fn retry_of_expired_staged_metadata_fails_explicitly() {
    let (service, _primary, secondary) =
        service_fixture_parts_with_staging_lifetime(Duration::ZERO);
    let blocks = service.block_service();
    let logical_blob = blob("/container/expired-stage");
    let content = spool_body(Body::from("expired"), 4).await.expect("content");
    blocks
        .put_block(
            &logical_blob,
            &principal(),
            "expired-upload",
            "expired-write",
            "YQ==",
            &content,
        )
        .await
        .expect("initial stage");
    let metadata_key = format!(
        "staged-blocks/{}/{}/{}.json",
        logical_blob.path_hash(),
        stable_component("expired-upload"),
        stable_component("YQ==")
    );
    secondary.remove_object(&metadata_key);
    tokio::time::sleep(Duration::from_millis(2)).await;
    assert!(matches!(
        blocks
            .put_block(
                &logical_blob,
                &principal(),
                "expired-upload",
                "expired-write",
                "YQ==",
                &content,
            )
            .await,
        Err(crate::block::BlockError::Expired)
    ));
    assert!(secondary.object(&metadata_key).is_none());
}

#[tokio::test]
async fn logical_listing_hides_stages_and_paginates_with_signed_markers() {
    use crate::listing::{BlobListEntry, ListRequest};

    let service = service_fixture();
    for (path, write_id) in [
        ("/container/a", "list-a"),
        ("/container/dir/b", "list-b"),
        ("/container/dir/c", "list-c"),
    ] {
        let content = spool_body(Body::from(path.to_owned()), 4)
            .await
            .expect("content");
        service
            .put_blob(
                &blob(path),
                &principal(),
                write_id,
                &content,
                LogicalCondition::None,
            )
            .await
            .expect("commit");
    }
    let staged = spool_body(Body::from("hidden"), 4).await.expect("stage");
    service
        .block_service()
        .put_block(
            &blob("/container/staged-only"),
            &principal(),
            "list-stage",
            "list-stage",
            "YmxvY2stMDAwMQ==",
            &staged,
        )
        .await
        .expect("stage");

    let listing = service.listing_service("test-account");
    let first = listing
        .list_blobs(
            "container",
            &ListRequest::new(String::new(), String::new(), None, Some(2), Vec::new())
                .expect("request"),
            &principal(),
        )
        .await
        .expect("first page");
    assert_eq!(first.entries.len(), 2);
    assert!(first.next_marker.is_some());
    assert!(first.entries.iter().all(|entry| {
        !matches!(entry, BlobListEntry::Blob(blob) if blob.name == "staged-only" || blob.name.starts_with(".overmesh"))
    }));
    let second = listing
        .list_blobs(
            "container",
            &ListRequest::new(
                String::new(),
                String::new(),
                first.next_marker,
                Some(2),
                Vec::new(),
            )
            .expect("continuation request"),
            &principal(),
        )
        .await
        .expect("second page");
    assert_eq!(second.entries.len(), 1);

    let delimited = listing
        .list_blobs(
            "container",
            &ListRequest::new(String::new(), "/".to_owned(), None, Some(5), Vec::new())
                .expect("delimiter request"),
            &principal(),
        )
        .await
        .expect("delimiter listing");
    assert!(
        delimited
            .entries
            .iter()
            .any(|entry| matches!(entry, BlobListEntry::Prefix(prefix) if prefix == "dir/"))
    );
}

#[tokio::test]
async fn listing_uses_bounded_catalog_pages_without_blob_read_probes() {
    use crate::listing::{BlobListEntry, ListRequest};

    let (service, primary, secondary) = service_fixture_parts();
    for index in 0..40 {
        let path = format!("/container/blob-{index:03}");
        let content = spool_body(Body::from(path.clone()), 4)
            .await
            .expect("content");
        service
            .put_blob(
                &blob(&path),
                &principal(),
                &format!("catalog-{index:03}"),
                &content,
                LogicalCondition::None,
            )
            .await
            .expect("commit");
    }
    let first_before = primary.state.control_page_calls.load(Ordering::SeqCst);
    let second_before = secondary.state.control_page_calls.load(Ordering::SeqCst);
    let primary_gets_before = primary
        .state
        .control_get_calls
        .lock()
        .expect("get calls")
        .values()
        .sum::<u64>();
    let secondary_gets_before = secondary
        .state
        .control_get_calls
        .lock()
        .expect("get calls")
        .values()
        .sum::<u64>();
    let page = service
        .listing_service("test-account")
        .list_blobs(
            "container",
            &ListRequest::new(String::new(), String::new(), None, Some(2), Vec::new())
                .expect("request"),
            &principal(),
        )
        .await
        .expect("listing");
    assert_eq!(page.entries.len(), 2);
    assert!(page.next_marker.is_some());
    assert!(
        page.entries
            .iter()
            .all(|entry| matches!(entry, BlobListEntry::Blob(_)))
    );
    assert_eq!(
        primary.state.control_page_calls.load(Ordering::SeqCst) - first_before,
        1
    );
    assert_eq!(
        secondary.state.control_page_calls.load(Ordering::SeqCst) - second_before,
        1
    );
    assert_eq!(
        primary.state.max_control_page_limit.load(Ordering::SeqCst),
        32
    );
    assert_eq!(primary.state.blob_read_auth_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        secondary.state.blob_read_auth_calls.load(Ordering::SeqCst),
        0
    );
    assert_eq!(primary.state.list_calls.load(Ordering::SeqCst), 1);
    assert_eq!(secondary.state.list_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        primary
            .state
            .control_get_calls
            .lock()
            .expect("get calls")
            .values()
            .sum::<u64>()
            - primary_gets_before,
        6
    );
    assert_eq!(
        secondary
            .state
            .control_get_calls
            .lock()
            .expect("get calls")
            .values()
            .sum::<u64>()
            - secondary_gets_before,
        6
    );
}

#[tokio::test]
async fn listing_excludes_the_union_of_quarantine_keys() {
    use crate::listing::{BlobListEntry, ListRequest};

    let (service, primary, _secondary) = service_fixture_parts();
    for name in ["visible", "quarantined"] {
        let content = spool_body(Body::from(name), 4).await.expect("content");
        service
            .put_blob(
                &blob(&format!("/container/{name}")),
                &principal(),
                &format!("catalog-{name}"),
                &content,
                LogicalCondition::None,
            )
            .await
            .expect("commit");
    }
    let path_hash = blob("/container/quarantined").path_hash();
    primary.state.objects.lock().expect("object lock").insert(
        format!("quarantine/{path_hash}.json"),
        ObjectValue {
            bytes: b"quarantine".to_vec(),
            etag: Some("\"quarantine\"".to_owned()),
        },
    );

    let page = service
        .listing_service("test-account")
        .list_blobs(
            "container",
            &ListRequest::new(String::new(), String::new(), None, Some(10), Vec::new())
                .expect("request"),
            &principal(),
        )
        .await
        .expect("listing");
    let names = page
        .entries
        .iter()
        .filter_map(|entry| match entry {
            BlobListEntry::Blob(blob) => Some(blob.name.as_str()),
            BlobListEntry::Prefix(_) => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(names, ["visible"]);
}

#[tokio::test]
async fn list_containers_uses_visible_catalog_entries_without_account_listing() {
    use crate::listing::ListRequest;

    let (service, primary, secondary) = service_fixture_parts();
    for backend in [&primary, &secondary] {
        backend.add_container("visible-customer", 10);
        backend.add_container("empty-customer", 10);
        backend.add_container("overmesh-system", 20);
    }
    primary.add_container("one-sided", 30);
    let content = spool_body(Body::from("visible"), 4).await.expect("content");
    service
        .put_blob(
            &blob("/visible-customer/blob"),
            &principal(),
            "visible-container",
            &content,
            LogicalCondition::None,
        )
        .await
        .expect("commit");

    let page = service
        .listing_service("test-account")
        .list_containers(
            &ListRequest::new(String::new(), String::new(), None, Some(10), Vec::new())
                .expect("request"),
            &principal(),
        )
        .await
        .expect("container listing");
    assert_eq!(
        page.containers
            .iter()
            .map(|container| container.name.as_str())
            .collect::<Vec<_>>(),
        ["visible-customer"]
    );
    assert_eq!(primary.state.list_calls.load(Ordering::SeqCst), 1);
    assert_eq!(secondary.state.list_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn container_continuation_uses_independent_opaque_backend_cursors() {
    use crate::listing::ListRequest;

    let (service, primary, secondary) = service_fixture_parts();
    let expected = (0..25)
        .map(|index| format!("container-{index:03}"))
        .collect::<Vec<_>>();
    for (index, name) in expected.iter().enumerate() {
        primary.add_container(name, 10);
        secondary.add_container(name, 20);
        let path = format!("/{name}/blob");
        let content = spool_body(Body::from(path.clone()), 4)
            .await
            .expect("content");
        service
            .put_blob(
                &blob(&path),
                &principal(),
                &format!("container-{index:03}"),
                &content,
                LogicalCondition::None,
            )
            .await
            .expect("commit");
    }
    let listing = service.listing_service("test-account");
    let mut marker = None;
    let mut actual = Vec::new();
    loop {
        let page = listing
            .list_containers(
                &ListRequest::new(String::new(), String::new(), marker, Some(7), Vec::new())
                    .expect("request"),
                &principal(),
            )
            .await
            .expect("page");
        actual.extend(page.containers.into_iter().map(|container| container.name));
        marker = page.next_marker;
        if marker.is_none() {
            break;
        }
    }
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn delimiter_continuation_consumes_a_prefix_group_across_catalog_pages() {
    use crate::listing::{BlobListEntry, ListRequest};

    let (service, primary, secondary) = service_fixture_parts();
    for index in 0..33 {
        let path = format!("/container/dir/blob-{index:03}");
        let content = spool_body(Body::from(path.clone()), 4)
            .await
            .expect("content");
        service
            .put_blob(
                &blob(&path),
                &principal(),
                &format!("delimiter-{index:03}"),
                &content,
                LogicalCondition::None,
            )
            .await
            .expect("commit");
    }
    let content = spool_body(Body::from("z"), 4).await.expect("content");
    service
        .put_blob(
            &blob("/container/z"),
            &principal(),
            "delimiter-z",
            &content,
            LogicalCondition::None,
        )
        .await
        .expect("commit");

    let listing = service.listing_service("test-account");
    let first_before = primary.state.control_page_calls.load(Ordering::SeqCst);
    let first = listing
        .list_blobs(
            "container",
            &ListRequest::new(String::new(), "/".to_owned(), None, Some(1), Vec::new())
                .expect("request"),
            &principal(),
        )
        .await
        .expect("first page");
    assert_eq!(first.entries, [BlobListEntry::Prefix("dir/".to_owned())]);
    assert!(first.next_marker.is_some());
    assert_eq!(
        primary.state.control_page_calls.load(Ordering::SeqCst) - first_before,
        2
    );
    assert_eq!(
        secondary.state.control_page_calls.load(Ordering::SeqCst),
        primary.state.control_page_calls.load(Ordering::SeqCst)
    );
    let second = listing
        .list_blobs(
            "container",
            &ListRequest::new(
                String::new(),
                "/".to_owned(),
                first.next_marker,
                Some(1),
                Vec::new(),
            )
            .expect("request"),
            &principal(),
        )
        .await
        .expect("second page");
    assert!(matches!(
        second.entries.as_slice(),
        [BlobListEntry::Blob(blob)] if blob.name == "z"
    ));
    assert!(second.next_marker.is_none());
}

#[tokio::test]
async fn continuation_has_no_duplicate_or_omitted_catalog_entries() {
    use crate::listing::{BlobListEntry, ListRequest};

    let service = service_fixture();
    let expected = (0..25)
        .map(|index| format!("blob-{index:03}"))
        .collect::<Vec<_>>();
    for name in &expected {
        let path = format!("/container/{name}");
        let content = spool_body(Body::from(path.clone()), 4)
            .await
            .expect("content");
        service
            .put_blob(
                &blob(&path),
                &principal(),
                &format!("page-{name}"),
                &content,
                LogicalCondition::None,
            )
            .await
            .expect("commit");
    }
    let listing = service.listing_service("test-account");
    let mut marker = None;
    let mut actual = Vec::new();
    loop {
        let page = listing
            .list_blobs(
                "container",
                &ListRequest::new(String::new(), String::new(), marker, Some(7), Vec::new())
                    .expect("request"),
                &principal(),
            )
            .await
            .expect("page");
        actual.extend(page.entries.into_iter().map(|entry| match entry {
            BlobListEntry::Blob(blob) => blob.name,
            BlobListEntry::Prefix(_) => panic!("unexpected prefix"),
        }));
        marker = page.next_marker;
        if marker.is_none() {
            break;
        }
    }
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn catalog_tamper_is_hidden_and_delete_recreate_updates_current_entry() {
    use crate::{
        catalog::catalog_key,
        listing::{BlobListEntry, ListRequest},
    };

    let (service, primary, secondary) = service_fixture_parts();
    let logical_blob = blob("/container/current");
    let first = spool_body(Body::from("first"), 4).await.expect("content");
    service
        .put_blob(
            &logical_blob,
            &principal(),
            "current-1",
            &first,
            LogicalCondition::None,
        )
        .await
        .expect("commit");
    let key = catalog_key(&logical_blob);
    secondary
        .put(&key, b"{}".to_vec(), PutCondition::None)
        .expect("tamper");
    let listing = service.listing_service("test-account");
    let request = ListRequest::new(String::new(), String::new(), None, Some(10), Vec::new())
        .expect("request");
    assert!(
        listing
            .list_blobs("container", &request, &principal())
            .await
            .expect("tampered listing")
            .entries
            .is_empty()
    );
    secondary
        .put(
            &key,
            primary.object(&key).expect("catalog").bytes,
            PutCondition::None,
        )
        .expect("restore");
    service
        .delete_blob(
            &logical_blob,
            &principal(),
            "current-delete",
            LogicalCondition::None,
        )
        .await
        .expect("delete");
    assert!(
        listing
            .list_blobs("container", &request, &principal())
            .await
            .expect("deleted listing")
            .entries
            .is_empty()
    );
    let second = spool_body(Body::from("second"), 4).await.expect("content");
    service
        .put_blob(
            &logical_blob,
            &principal(),
            "current-2",
            &second,
            LogicalCondition::IfAbsent,
        )
        .await
        .expect("recreate");
    assert!(matches!(
        listing
            .list_blobs("container", &request, &principal())
            .await
            .expect("recreated listing")
            .entries
            .as_slice(),
        [BlobListEntry::Blob(blob)] if blob.name == "current"
    ));
    assert_eq!(
        primary.object(&key).expect("primary catalog").bytes,
        secondary.object(&key).expect("secondary catalog").bytes
    );
}

#[tokio::test]
async fn retry_repairs_one_sided_catalog_publication_with_exact_signed_bytes() {
    use crate::catalog::catalog_key;

    let (service, primary, secondary) = service_fixture_parts();
    let logical_blob = blob("/container/catalog-retry");
    let key = catalog_key(&logical_blob);
    let content = spool_body(Body::from("content"), 4).await.expect("content");
    secondary.fail_on_prefix(&key);
    assert!(matches!(
        service
            .put_blob(
                &logical_blob,
                &principal(),
                "catalog-retry",
                &content,
                LogicalCondition::None,
            )
            .await,
        Err(CommitError::Ambiguous)
    ));
    let head_key = format!("heads/{}.json", logical_blob.path_hash());
    assert!(primary.object(&head_key).is_none());
    assert!(secondary.object(&head_key).is_none());
    secondary.clear_failure();
    let retry = service
        .put_blob(
            &logical_blob,
            &principal(),
            "catalog-retry",
            &content,
            LogicalCondition::None,
        )
        .await
        .expect("retry");
    assert!(!retry.idempotent_replay);
    let primary_catalog = primary.object(&key).expect("primary catalog");
    let secondary_catalog = secondary.object(&key).expect("secondary catalog");
    assert_eq!(primary_catalog.bytes, secondary_catalog.bytes);
    assert_eq!(
        primary_catalog.bytes,
        primary.object(&head_key).expect("head").bytes
    );
}

#[tokio::test]
async fn listing_hides_a_catalog_generation_until_both_heads_publish() {
    use crate::{catalog::catalog_key, listing::ListRequest};

    let (service, primary, secondary) = service_fixture_parts();
    let logical_blob = blob("/container/catalog-before-head");
    let key = catalog_key(&logical_blob);
    let head_key = format!("heads/{}.json", logical_blob.path_hash());
    let content = spool_body(Body::from("content"), 4).await.expect("content");
    secondary.fail_on_prefix(&head_key);

    assert!(matches!(
        service
            .put_blob(
                &logical_blob,
                &principal(),
                "catalog-before-head",
                &content,
                LogicalCondition::None,
            )
            .await,
        Err(CommitError::Ambiguous)
    ));
    assert_eq!(
        primary.object(&key).expect("primary catalog").bytes,
        secondary.object(&key).expect("secondary catalog").bytes
    );
    assert!(primary.object(&head_key).is_some());
    assert!(secondary.object(&head_key).is_none());

    let page = service
        .listing_service("test-account")
        .list_blobs(
            "container",
            &ListRequest::new(String::new(), String::new(), None, Some(10), Vec::new())
                .expect("request"),
            &principal(),
        )
        .await
        .expect("listing");
    assert!(page.entries.is_empty());
}

#[tokio::test]
async fn delete_retry_repairs_catalog_before_publishing_tombstone_heads() {
    use crate::{
        catalog::catalog_key,
        listing::{BlobListEntry, ListRequest},
    };

    let (service, primary, secondary) = service_fixture_parts();
    let logical_blob = blob("/container/delete-catalog-retry");
    let content = spool_body(Body::from("content"), 4).await.expect("content");
    service
        .put_blob(
            &logical_blob,
            &principal(),
            "delete-catalog-base",
            &content,
            LogicalCondition::None,
        )
        .await
        .expect("commit");
    let key = catalog_key(&logical_blob);
    let old_head = primary
        .object(&format!("heads/{}.json", logical_blob.path_hash()))
        .expect("old head")
        .bytes;
    secondary.fail_on_prefix(&key);

    assert!(matches!(
        service
            .delete_blob(
                &logical_blob,
                &principal(),
                "delete-catalog-retry",
                LogicalCondition::None,
            )
            .await,
        Err(CommitError::Ambiguous)
    ));
    let head_key = format!("heads/{}.json", logical_blob.path_hash());
    assert_eq!(
        primary.object(&head_key).expect("primary head").bytes,
        old_head
    );
    assert_eq!(
        secondary.object(&head_key).expect("secondary head").bytes,
        old_head
    );

    secondary.clear_failure();
    let result = service
        .delete_blob(
            &logical_blob,
            &principal(),
            "delete-catalog-retry",
            LogicalCondition::None,
        )
        .await
        .expect("retry");
    assert!(!result.idempotent_replay);
    assert_eq!(
        primary.object(&key).expect("primary catalog").bytes,
        secondary.object(&key).expect("secondary catalog").bytes
    );
    let page = service
        .listing_service("test-account")
        .list_blobs(
            "container",
            &ListRequest::new(String::new(), String::new(), None, Some(10), Vec::new())
                .expect("request"),
            &principal(),
        )
        .await
        .expect("listing");
    assert!(!page.entries.iter().any(
        |entry| matches!(entry, BlobListEntry::Blob(blob) if blob.name == logical_blob.blob())
    ));
}

#[tokio::test]
async fn put_blob_replay_reauthorizes_and_rejects_a_different_caller() {
    let (service, primary, secondary) = service_fixture_parts();
    let logical_blob = blob("/container/replay-auth");
    let content = spool_body(Body::from("content"), 4).await.expect("content");
    service
        .put_blob(
            &logical_blob,
            &principal(),
            "replay-auth",
            &content,
            LogicalCondition::None,
        )
        .await
        .expect("commit");

    let high_water_key = CommitCoordinator::high_water_current_key(&logical_blob.path_hash());
    secondary.remove_object(&high_water_key);
    secondary.deny_blob_write(true);
    assert!(matches!(
        service
            .put_blob(
                &logical_blob,
                &principal(),
                "replay-auth",
                &content,
                LogicalCondition::None,
            )
            .await,
        Err(CommitError::Backend(BackendError::Http {
            status: StatusCode::FORBIDDEN,
            ..
        }))
    ));
    assert!(secondary.object(&high_water_key).is_none());
    secondary.deny_blob_write(false);
    assert!(matches!(
        service
            .put_blob(
                &logical_blob,
                &other_principal(),
                "replay-auth",
                &content,
                LogicalCondition::None,
            )
            .await,
        Err(CommitError::IdempotencyConflict)
    ));
    assert!(primary.state.blob_write_auth_calls.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn w1_put_blob_recovery_reauthorizes_before_repairing_the_head() {
    let (service, _primary, secondary) = service_fixture_parts();
    let logical_blob = blob("/container/w1-auth");
    let content = spool_body(Body::from("content"), 4).await.expect("content");
    let head_key = format!("heads/{}.json", logical_blob.path_hash());
    secondary.fail_on_prefix(&head_key);
    assert!(matches!(
        service
            .put_blob(
                &logical_blob,
                &principal(),
                "w1-auth",
                &content,
                LogicalCondition::None,
            )
            .await,
        Err(CommitError::Ambiguous)
    ));
    secondary.clear_failure();
    secondary.deny_blob_write(true);
    assert!(matches!(
        service
            .put_blob(
                &logical_blob,
                &principal(),
                "w1-auth",
                &content,
                LogicalCondition::None,
            )
            .await,
        Err(CommitError::Backend(BackendError::Http {
            status: StatusCode::FORBIDDEN,
            ..
        }))
    ));
    assert!(secondary.object(&head_key).is_none());
}

#[tokio::test]
async fn put_blob_returns_forbidden_after_attempting_the_caller_data_write() {
    let (service, primary, secondary) = service_fixture_parts();
    let logical_blob = blob("/container/direct-write-denied");
    let content = spool_body(Body::from("content"), 4).await.expect("content");
    let before = primary.caller_data_write_count() + secondary.caller_data_write_count();
    primary.deny_caller_data_write(true);
    secondary.deny_caller_data_write(true);

    assert!(matches!(
        service
            .put_blob(
                &logical_blob,
                &principal(),
                "direct-write-denied",
                &content,
                LogicalCondition::None,
            )
            .await,
        Err(CommitError::Backend(BackendError::Http {
            status: StatusCode::FORBIDDEN,
            ..
        }))
    ));
    assert!(primary.caller_data_write_count() + secondary.caller_data_write_count() > before);
}

#[tokio::test]
async fn put_block_returns_forbidden_after_attempting_the_staged_content_write() {
    let (service, primary, secondary) = service_fixture_parts();
    let blocks = service.block_service();
    let logical_blob = blob("/container/staged-write-denied");
    let content = spool_body(Body::from("content"), 4).await.expect("content");
    let before = primary.caller_data_write_count() + secondary.caller_data_write_count();
    primary.deny_caller_data_write(true);
    secondary.deny_caller_data_write(true);

    assert!(matches!(
        blocks
            .put_block(
                &logical_blob,
                &principal(),
                "upload",
                "stage",
                "YQ==",
                &content,
            )
            .await,
        Err(crate::block::BlockError::Commit(CommitError::Backend(
            BackendError::Http {
                status: StatusCode::FORBIDDEN,
                ..
            }
        )))
    ));
    assert!(primary.caller_data_write_count() + secondary.caller_data_write_count() > before);
}

#[tokio::test]
async fn put_block_list_returns_forbidden_after_loading_uncommitted_blocks() {
    use crate::block::{BlockListType, BlockSelection, BlockSelectionKind};

    let (service, primary, secondary) = service_fixture_parts();
    let blocks = service.block_service();
    let logical_blob = blob("/container/block-list-write-denied");
    let content = spool_body(Body::from("content"), 4).await.expect("content");
    blocks
        .put_block(
            &logical_blob,
            &principal(),
            "upload",
            "stage",
            "YQ==",
            &content,
        )
        .await
        .expect("stage");
    let selections = [BlockSelection {
        kind: BlockSelectionKind::Latest,
        block_id: "YQ==".to_owned(),
    }];
    primary.deny_caller_data_write(true);
    secondary.deny_caller_data_write(true);

    let staged = blocks
        .get_block_list(
            &logical_blob,
            &principal(),
            Some("upload"),
            BlockListType::Uncommitted,
        )
        .await
        .expect("uncommitted list");
    assert_eq!(
        staged
            .uncommitted
            .iter()
            .map(|block| block.block_id.as_str())
            .collect::<Vec<_>>(),
        ["YQ=="]
    );

    let before = primary.caller_data_write_count() + secondary.caller_data_write_count();
    assert!(matches!(
        blocks
            .put_block_list(
                &logical_blob,
                &principal(),
                "upload",
                "commit",
                &selections,
                LogicalCondition::None,
            )
            .await,
        Err(crate::block::BlockError::Commit(CommitError::Backend(
            BackendError::Http {
                status: StatusCode::FORBIDDEN,
                ..
            }
        )))
    ));
    assert!(primary.caller_data_write_count() + secondary.caller_data_write_count() > before);
}

#[tokio::test]
async fn put_block_list_replay_reauthorizes_and_repairs_catalog_visibility() {
    use crate::{
        block::{BlockSelection, BlockSelectionKind},
        catalog::catalog_key,
        listing::{BlobListEntry, ListRequest},
    };

    let (service, primary, secondary) = service_fixture_parts();
    let blocks = service.block_service();
    let logical_blob = blob("/container/block-replay-auth");
    let content = spool_body(Body::from("content"), 4).await.expect("content");
    blocks
        .put_block(
            &logical_blob,
            &principal(),
            "upload",
            "stage",
            "YQ==",
            &content,
        )
        .await
        .expect("stage");
    let selections = [BlockSelection {
        kind: BlockSelectionKind::Latest,
        block_id: "YQ==".to_owned(),
    }];
    blocks
        .put_block_list(
            &logical_blob,
            &principal(),
            "upload",
            "commit",
            &selections,
            LogicalCondition::None,
        )
        .await
        .expect("commit");
    let catalog_key = catalog_key(&logical_blob);
    let high_water_key = CommitCoordinator::high_water_current_key(&logical_blob.path_hash());
    primary.remove_object(&catalog_key);
    secondary.remove_object(&catalog_key);
    secondary.remove_object(&high_water_key);

    secondary.deny_blob_write(true);
    assert!(matches!(
        blocks
            .put_block_list(
                &logical_blob,
                &principal(),
                "upload",
                "commit",
                &selections,
                LogicalCondition::None,
            )
            .await,
        Err(crate::block::BlockError::Commit(CommitError::Backend(
            BackendError::Http {
                status: StatusCode::FORBIDDEN,
                ..
            }
        )))
    ));
    assert!(primary.object(&catalog_key).is_none());
    assert!(secondary.object(&catalog_key).is_none());
    assert!(secondary.object(&high_water_key).is_none());
    secondary.deny_blob_write(false);
    assert!(matches!(
        blocks
            .put_block_list(
                &logical_blob,
                &other_principal(),
                "upload",
                "commit",
                &selections,
                LogicalCondition::None,
            )
            .await,
        Err(crate::block::BlockError::Commit(
            CommitError::IdempotencyConflict
        ))
    ));
    blocks
        .put_block_list(
            &logical_blob,
            &principal(),
            "upload",
            "commit",
            &selections,
            LogicalCondition::None,
        )
        .await
        .expect("authorized replay");
    assert_eq!(
        primary.object(&catalog_key).expect("primary catalog").bytes,
        secondary
            .object(&catalog_key)
            .expect("secondary catalog")
            .bytes
    );
    let page = service
        .listing_service("test-account")
        .list_blobs(
            "container",
            &ListRequest::new(String::new(), String::new(), None, Some(10), Vec::new())
                .expect("request"),
            &principal(),
        )
        .await
        .expect("listing");
    assert!(matches!(
        page.entries.as_slice(),
        [BlobListEntry::Blob(blob)] if blob.name == "block-replay-auth"
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
async fn stale_w1_retry_after_delete_recreate_cannot_roll_back_head_or_high_water() {
    let (coordinator, _, primary, secondary) = read_fixture("/container/stale-retry");
    let old = spool_body(Body::from("old"), 4).await.expect("old");
    commit(
        &coordinator,
        "/container/stale-retry",
        "old-write",
        &old,
        LogicalCondition::None,
    )
    .await
    .expect("old commit");
    let logical_blob = blob("/container/stale-retry");
    let head_key = format!("heads/{}.json", logical_blob.path_hash());
    let high_water_key = CommitCoordinator::high_water_current_key(&logical_blob.path_hash());
    let old_head = primary.object(&head_key).expect("old head");
    delete(
        &coordinator,
        "/container/stale-retry",
        "delete",
        LogicalCondition::None,
    )
    .await
    .expect("delete");
    let recreated_content = spool_body(Body::from("new"), 4).await.expect("new");
    commit(
        &coordinator,
        "/container/stale-retry",
        "recreated",
        &recreated_content,
        LogicalCondition::IfAbsent,
    )
    .await
    .expect("recreate");
    let current_high_water = primary.object(&high_water_key).expect("current high water");
    primary
        .put(&head_key, old_head.bytes, PutCondition::None)
        .expect("stale primary");
    secondary.remove_object(&head_key);

    assert!(matches!(
        commit(
            &coordinator,
            "/container/stale-retry",
            "old-write",
            &old,
            LogicalCondition::None,
        )
        .await,
        Err(CommitError::VerificationFailed)
    ));
    assert!(secondary.object(&head_key).is_none());
    assert_eq!(
        primary.object(&high_water_key).expect("primary high").bytes,
        current_high_water.bytes
    );
    assert_eq!(
        secondary
            .object(&high_water_key)
            .expect("secondary high")
            .bytes,
        current_high_water.bytes
    );
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
async fn compaction_floor_rejects_w1_recovery_before_mutating_the_missing_head() {
    let path = "/container/compacted-w1-retry";
    let (coordinator, _, primary, secondary) = read_fixture(path);
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
    let logical_blob = blob(path);
    let path_hash = logical_blob.path_hash();
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
    let checkpoint = SignedDocument::create(
        HistoryCompactionCheckpoint {
            api_version: "overmesh.io/history-compaction-checkpoint/v1".to_owned(),
            blob: logical_blob.canonical().to_owned(),
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
            garbage_collection_marker_sha256: sha256_bytes(b"marker"),
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
    for backend in [&primary, &secondary] {
        backend
            .put(&checkpoint_key, checkpoint.clone(), PutCondition::None)
            .expect("checkpoint");
        backend.remove_object(&CommitCoordinator::high_water_current_key(&path_hash));
    }
    primary
        .put(&head_key, first_head.bytes, PutCondition::None)
        .expect("stale primary");
    secondary.remove_object(&head_key);

    assert!(matches!(
        commit(
            &coordinator,
            path,
            "write-1",
            &first_content,
            LogicalCondition::None,
        )
        .await,
        Err(CommitError::VerificationFailed)
    ));
    assert!(secondary.object(&head_key).is_none());
    assert_eq!(
        primary
            .object(&checkpoint_key)
            .expect("primary checkpoint")
            .bytes,
        checkpoint
    );
    assert_eq!(
        secondary
            .object(&checkpoint_key)
            .expect("secondary checkpoint")
            .bytes,
        checkpoint
    );
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

    caller_put_file_idempotent(
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
async fn high_water_publication_never_replaces_a_higher_version_by_etag() {
    let primary = Arc::new(MemoryBackend::new("storage-a"));
    let secondary = Arc::new(MemoryBackend::new("storage-b"));
    let coordinator = coordinator(primary.clone(), secondary.clone());
    let logical_blob = blob("/container/high-water-rollback");
    let first = spool_body(Body::from("first"), 4).await.expect("first");
    let second = spool_body(Body::from("second"), 4).await.expect("second");
    commit(
        &coordinator,
        "/container/high-water-rollback",
        "write-1",
        &first,
        LogicalCondition::None,
    )
    .await
    .expect("first commit");
    let head_key = format!("heads/{}.json", logical_blob.path_hash());
    let first_head = primary.object(&head_key).expect("first head");
    let first_signed =
        SignedDocument::<CommitManifest>::from_bytes(&first_head.bytes).expect("first signed");
    commit(
        &coordinator,
        "/container/high-water-rollback",
        "write-2",
        &second,
        LogicalCondition::None,
    )
    .await
    .expect("second commit");
    let high_water_key = CommitCoordinator::high_water_current_key(&logical_blob.path_hash());
    let higher = primary.object(&high_water_key).expect("higher high water");
    assert!(matches!(
        CommitCoordinator::publish_high_water(
            primary.as_ref(),
            secondary.as_ref(),
            &logical_blob.path_hash(),
            &first_signed,
            &first_head.bytes,
            &ControlToken::new("control-token".to_owned()),
            coordinator.signer.as_ref(),
        )
        .await,
        Err(CommitError::VerificationFailed)
    ));
    assert_eq!(
        primary.object(&high_water_key).expect("primary high").bytes,
        higher.bytes
    );
    assert_eq!(
        secondary
            .object(&high_water_key)
            .expect("secondary high")
            .bytes,
        higher.bytes
    );
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
