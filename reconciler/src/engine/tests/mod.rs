use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use overmesh_gateway::{
    backend::{
        BackendError, BackendLease, DataObjectProperties, DataObjectValidation, ObjectDigest,
        ObjectValue, PutCondition, PutResult, ReplicaBackend, SharedBackend,
    },
    identity::{CallerToken, ControlToken, ControlTokenProvider, LocalControlTokenProvider},
    manifest::{
        CommitManifest, GarbageCollectionMarker, HistoryCompactionCheckpoint,
        LocalTestManifestSigner, ManifestSigner, ManifestState, SignatureDomain, SignedDocument,
        logical_etag, sha256_bytes,
    },
    resource::{LogicalBlobId, stable_component},
    ring::{RingDocument, RingNode},
};

use super::*;
use crate::posture::DisabledRbacPostureAuditor;

#[derive(Default)]
struct TestState {
    control: Mutex<BTreeMap<String, ObjectValue>>,
    data: Mutex<BTreeMap<(String, String), ObjectValue>>,
    fail_put_once: Mutex<HashSet<String>>,
    fail_delete_once: Mutex<HashSet<String>>,
    delete_calls: AtomicUsize,
    list_calls: AtomicUsize,
    head_get_calls: Mutex<HashMap<String, usize>>,
    etag: AtomicUsize,
}

#[derive(Clone)]
struct TestBackend {
    id: String,
    state: Arc<TestState>,
}

impl TestBackend {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_owned(),
            state: Arc::new(TestState::default()),
        }
    }

    fn put_control(&self, key: &str, bytes: Vec<u8>) {
        self.state.control.lock().expect("control lock").insert(
            key.to_owned(),
            ObjectValue {
                bytes,
                etag: Some(format!(
                    "\"{}\"",
                    self.state.etag.fetch_add(1, Ordering::SeqCst)
                )),
            },
        );
    }

    fn put_data(&self, container: &str, key: &str, bytes: Vec<u8>) {
        self.state.data.lock().expect("data lock").insert(
            (container.to_owned(), key.to_owned()),
            ObjectValue {
                bytes,
                etag: Some(format!(
                    "\"{}\"",
                    self.state.etag.fetch_add(1, Ordering::SeqCst)
                )),
            },
        );
    }

    fn control(&self, key: &str) -> Option<ObjectValue> {
        self.state
            .control
            .lock()
            .expect("control lock")
            .get(key)
            .cloned()
    }

    fn data(&self, container: &str, key: &str) -> Option<ObjectValue> {
        self.state
            .data
            .lock()
            .expect("data lock")
            .get(&(container.to_owned(), key.to_owned()))
            .cloned()
    }

    fn remove_control(&self, key: &str) {
        self.state.control.lock().expect("control lock").remove(key);
    }

    fn control_keys(&self, prefix: &str) -> Vec<String> {
        self.state
            .control
            .lock()
            .expect("control lock")
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect()
    }

    fn fail_put_once(&self, key: &str) {
        self.state
            .fail_put_once
            .lock()
            .expect("put failure lock")
            .insert(key.to_owned());
    }

    fn fail_delete_once(&self, key: &str) {
        self.state
            .fail_delete_once
            .lock()
            .expect("delete failure lock")
            .insert(key.to_owned());
    }

    fn delete_calls(&self) -> usize {
        self.state.delete_calls.load(Ordering::SeqCst)
    }

    fn head_get_calls(&self, key: &str) -> usize {
        *self
            .state
            .head_get_calls
            .lock()
            .expect("head call lock")
            .get(key)
            .unwrap_or(&0)
    }

    fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: PutCondition,
    ) -> Result<PutResult, BackendError> {
        if self
            .state
            .fail_put_once
            .lock()
            .expect("put failure lock")
            .remove(key)
        {
            return Err(server_error("injected put failure"));
        }
        let mut objects = self.state.control.lock().expect("control lock");
        match condition {
            PutCondition::IfAbsent if objects.contains_key(key) => {
                return Err(BackendError::AlreadyExists);
            }
            PutCondition::IfMatch(expected)
                if objects.get(key).and_then(|value| value.etag.as_deref())
                    != Some(expected.as_str()) =>
            {
                return Err(BackendError::PreconditionFailed);
            }
            PutCondition::None | PutCondition::IfAbsent | PutCondition::IfMatch(_) => {}
        }
        let etag = format!("\"{}\"", self.state.etag.fetch_add(1, Ordering::SeqCst));
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
impl ReplicaBackend for TestBackend {
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
        Ok(self
            .data(container, object_key)
            .map(|value| DataObjectProperties {
                length: u64::try_from(value.bytes.len()).expect("test length"),
            }))
    }

    async fn caller_get_data_range(
        &self,
        container: &str,
        object_key: &str,
        range: Option<(u64, u64)>,
        _caller_token: &CallerToken,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        let Some(value) = self.data(container, object_key) else {
            return Ok(None);
        };
        if let Some((start, end)) = range {
            let start = usize::try_from(start)
                .map_err(|_| BackendError::InvalidResponse("invalid range".to_owned()))?;
            let end = usize::try_from(end)
                .map_err(|_| BackendError::InvalidResponse("invalid range".to_owned()))?;
            return Ok(Some(
                value
                    .bytes
                    .get(start..=end)
                    .ok_or_else(|| {
                        BackendError::InvalidResponse("range exceeds content".to_owned())
                    })?
                    .to_vec(),
            ));
        }
        Ok(Some(value.bytes))
    }

    async fn caller_put_data_file(
        &self,
        _container: &str,
        _object_key: &str,
        _path: &Path,
        _length: u64,
        _condition: PutCondition,
        _caller_token: &CallerToken,
    ) -> Result<PutResult, BackendError> {
        Err(BackendError::InvalidResponse(
            "unused test operation".to_owned(),
        ))
    }

    async fn caller_digest_data_object(
        &self,
        container: &str,
        object_key: &str,
        _caller_token: &CallerToken,
    ) -> Result<Option<ObjectDigest>, BackendError> {
        Ok(self.data(container, object_key).map(|value| ObjectDigest {
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
            .head_get_calls
            .lock()
            .expect("head call lock")
            .entry(object_key.to_owned())
            .or_default() += 1;
        Ok(self.control(object_key))
    }

    async fn control_list_objects(
        &self,
        prefix: &str,
        _control_token: &ControlToken,
    ) -> Result<Vec<String>, BackendError> {
        self.state.list_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .state
            .control
            .lock()
            .expect("control lock")
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
    ) -> Result<overmesh_gateway::backend::ObjectListPage, BackendError> {
        self.state.list_calls.fetch_add(1, Ordering::SeqCst);
        let start = parse_test_cursor(&self.id, cursor)?;
        let objects = self.state.control.lock().expect("control lock");
        let ordered = objects
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect::<Vec<_>>();
        let total = ordered.len();
        let page = ordered
            .into_iter()
            .skip(start)
            .take(limit)
            .collect::<Vec<_>>();
        let end = start.saturating_add(page.len());
        Ok(overmesh_gateway::backend::ObjectListPage {
            objects: page,
            next_cursor: (end < total).then(|| test_cursor(&self.id, end)),
        })
    }

    async fn control_delete_object(
        &self,
        object_key: &str,
        _expected_etag: Option<&str>,
        _control_token: &ControlToken,
    ) -> Result<(), BackendError> {
        self.state.delete_calls.fetch_add(1, Ordering::SeqCst);
        let failure_key = format!("control:{object_key}");
        if self
            .state
            .fail_delete_once
            .lock()
            .expect("delete failure lock")
            .remove(&failure_key)
        {
            return Err(server_error("injected control delete failure"));
        }
        self.remove_control(object_key);
        Ok(())
    }

    async fn control_acquire_lock(
        &self,
        object_key: &str,
        _control_token: &ControlToken,
    ) -> Result<BackendLease, BackendError> {
        Ok(BackendLease {
            object_key: object_key.to_owned(),
            lease_id: "test-lease".to_owned(),
        })
    }

    async fn control_release_lock(
        &self,
        _lease: &BackendLease,
        _control_token: &ControlToken,
    ) -> Result<(), BackendError> {
        Ok(())
    }

    async fn control_renew_lock(
        &self,
        _lease: &BackendLease,
        _control_token: &ControlToken,
    ) -> Result<(), BackendError> {
        Ok(())
    }

    async fn service_get_data_object(
        &self,
        container: &str,
        object_key: &str,
        _control_token: &ControlToken,
    ) -> Result<Option<ObjectValue>, BackendError> {
        Ok(self.data(container, object_key))
    }

    async fn service_validate_data_object(
        &self,
        container: &str,
        object_key: &str,
        block_lengths: &[u64],
        _control_token: &ControlToken,
    ) -> Result<Option<DataObjectValidation>, BackendError> {
        let Some(value) = self.data(container, object_key) else {
            return Ok(None);
        };
        let mut offset = 0_usize;
        let mut block_sha256 = Vec::new();
        for length in block_lengths {
            let end = offset
                .checked_add(usize::try_from(*length).map_err(|_| {
                    BackendError::InvalidResponse("invalid block length".to_owned())
                })?)
                .ok_or_else(|| BackendError::InvalidResponse("block overflow".to_owned()))?;
            block_sha256.push(sha256_bytes(value.bytes.get(offset..end).ok_or_else(
                || BackendError::InvalidResponse("short content".to_owned()),
            )?));
            offset = end;
        }
        if offset != value.bytes.len() {
            return Err(BackendError::InvalidResponse("long content".to_owned()));
        }
        Ok(Some(DataObjectValidation {
            digest: ObjectDigest {
                length: u64::try_from(value.bytes.len()).expect("test length"),
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
        let mut data = self.state.data.lock().expect("data lock");
        let key = (container.to_owned(), object_key.to_owned());
        if matches!(condition, PutCondition::IfAbsent) && data.contains_key(&key) {
            return Err(BackendError::AlreadyExists);
        }
        let etag = format!("\"{}\"", self.state.etag.fetch_add(1, Ordering::SeqCst));
        data.insert(
            key,
            ObjectValue {
                bytes,
                etag: Some(etag.clone()),
            },
        );
        Ok(PutResult { etag: Some(etag) })
    }

    async fn service_delete_data_object(
        &self,
        container: &str,
        object_key: &str,
        _expected_etag: Option<&str>,
        _control_token: &ControlToken,
    ) -> Result<(), BackendError> {
        self.state.delete_calls.fetch_add(1, Ordering::SeqCst);
        let failure_key = format!("data:{container}/{object_key}");
        if self
            .state
            .fail_delete_once
            .lock()
            .expect("delete failure lock")
            .remove(&failure_key)
        {
            return Err(server_error("injected data delete failure"));
        }
        self.state
            .data
            .lock()
            .expect("data lock")
            .remove(&(container.to_owned(), object_key.to_owned()));
        Ok(())
    }
}

fn server_error(message: &str) -> BackendError {
    BackendError::Http {
        status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        message: message.to_owned(),
    }
}

fn test_cursor(backend_id: &str, offset: usize) -> String {
    format!("opaque::{backend_id}::{offset:08x}")
}

fn parse_test_cursor(backend_id: &str, cursor: Option<&str>) -> Result<usize, BackendError> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    let value = cursor
        .strip_prefix(&format!("opaque::{backend_id}::"))
        .ok_or_else(|| BackendError::InvalidResponse("invalid test cursor".to_owned()))?;
    usize::from_str_radix(value, 16)
        .map_err(|_| BackendError::InvalidResponse("invalid test cursor".to_owned()))
}

struct Fixture {
    engine: ReconcilerEngine,
    first: TestBackend,
    second: TestBackend,
    signer: Arc<LocalTestManifestSigner>,
    blob: String,
    head_object: String,
    history: Vec<ValidatedHistoryEntry>,
}

impl Fixture {
    async fn new(states: &[ManifestState], timestamps: &[u64], delay: Duration) -> Self {
        Self::new_with_compaction_limit(states, timestamps, delay, 64).await
    }

    async fn new_with_compaction_limit(
        states: &[ManifestState],
        timestamps: &[u64],
        delay: Duration,
        history_compaction_max_versions_per_cycle: usize,
    ) -> Self {
        assert_eq!(states.len(), timestamps.len());
        let signer = Arc::new(
            LocalTestManifestSigner::new(
                "test-blob-key-01",
                true,
                overmesh_gateway::manifest::KeyValidity::new(0, u64::MAX).expect("validity"),
            )
            .expect("test signer"),
        );
        let ring = Arc::new(test_ring(&["storage-a", "storage-b"]));
        let first = TestBackend::new("storage-a");
        let second = TestBackend::new("storage-b");
        let backends = HashMap::from([
            (first.id.clone(), Arc::new(first.clone()) as SharedBackend),
            (second.id.clone(), Arc::new(second.clone()) as SharedBackend),
        ]);
        let engine = ReconcilerEngine::new(
            ring,
            backends,
            signer.clone(),
            Arc::new(test_token_provider()),
            Arc::new(DisabledRbacPostureAuditor),
            ReconcilerOptions {
                physical_collection_delay: delay,
                history_compaction_max_versions_per_cycle,
                head_discovery_batch_size: 10,
                head_discovery_cursor_path: PathBuf::from("target/test-head-cursor-unused.json"),
                staged_block_gc_max_records_per_cycle: 256,
                staged_block_metadata_cursor_path: PathBuf::from(
                    "target/test-staged-metadata-cursor-unused.json",
                ),
                staged_block_marker_cursor_path: PathBuf::from(
                    "target/test-staged-marker-cursor-unused.json",
                ),
            },
        );
        let blob = "/test-account/container/blob".to_owned();
        let head_object = head_object_key(&blob);
        let path_hash = logical_path_hash(&blob);
        let mut previous = None;
        let mut history = Vec::new();
        for (index, state) in states.iter().enumerate() {
            let version = u64::try_from(index).expect("version") + 1;
            let signed = signed_manifest(ManifestFixtureInput {
                blob: &blob,
                path_hash: &path_hash,
                version,
                state: *state,
                previous: previous.as_deref(),
                committed_at: timestamps[index],
                signer: signer.as_ref(),
                replicas: &["storage-a", "storage-b"],
            })
            .await;
            let bytes = signed.canonical_bytes().expect("history bytes");
            let history_key = high_water_history_key(&path_hash, &signed.payload);
            first.put_control(&history_key, bytes.clone());
            second.put_control(&history_key, bytes.clone());
            let first_etag = first
                .control(&history_key)
                .and_then(|value| value.etag)
                .expect("first history ETag");
            let second_etag = second
                .control(&history_key)
                .and_then(|value| value.etag)
                .expect("second history ETag");
            if signed.payload.state == ManifestState::Committed {
                let committed_key = format!(
                    "{}/committed.json",
                    expected_version_prefix(&path_hash, &signed.payload).expect("prefix")
                );
                first.put_control(&committed_key, bytes.clone());
                second.put_control(&committed_key, bytes.clone());
                let content = format!("content-{version}").into_bytes();
                first.put_data(
                    &signed.payload.content_container,
                    &signed.payload.content_object,
                    content.clone(),
                );
                second.put_data(
                    &signed.payload.content_container,
                    &signed.payload.content_object,
                    content,
                );
            }
            previous = Some(signed.payload.logical_etag.clone());
            history.push(ValidatedHistoryEntry {
                signed,
                bytes,
                object_key: history_key,
                first_etag: Some(first_etag),
                second_etag: Some(second_etag),
            });
        }
        Self {
            engine,
            first,
            second,
            signer,
            blob,
            head_object,
            history,
        }
    }

    fn active_replicas(&self) -> (ValidatedReplica, ValidatedReplica) {
        let active = self.history.last().expect("active history");
        let replica = || ValidatedReplica {
            head: ValidatedHead {
                signed: active.signed.clone(),
                bytes: active.bytes.clone(),
                backend_etag: Some("\"head\"".to_owned()),
            },
            block_manifest: None,
            block_pages: Vec::new(),
            committed_manifest: active.bytes.clone(),
            high_water_checkpoint: active.bytes.clone(),
        };
        (replica(), replica())
    }

    fn replace_history(&self, version: usize, first: bool, second: bool, bytes: Vec<u8>) {
        let original = &self.history[version - 1];
        let key = high_water_history_key(&logical_path_hash(&self.blob), &original.signed.payload);
        if first {
            self.first.put_control(&key, bytes.clone());
        }
        if second {
            self.second.put_control(&key, bytes);
        }
    }

    async fn reconcile(&self) -> Result<BlobReport> {
        let token = test_token().await;
        let (first, second) = self.active_replicas();
        self.engine
            .reconcile_garbage_collection(
                &self.head_object,
                &self.first,
                &first,
                &self.second,
                &second,
                &token,
            )
            .await
    }

    fn total_deletes(&self) -> usize {
        self.first.delete_calls() + self.second.delete_calls()
    }

    fn marker(&self, through: u64) -> Option<ObjectValue> {
        self.first.control(&garbage_collection_marker_key(
            &logical_path_hash(&self.blob),
            through,
        ))
    }

    fn checkpoint(&self) -> Option<ObjectValue> {
        self.first
            .control(&history_compaction_checkpoint_key(&logical_path_hash(
                &self.blob,
            )))
    }

    fn history_keys(&self) -> Vec<String> {
        self.first.control_keys(&format!(
            "high-water/{}/history/",
            logical_path_hash(&self.blob)
        ))
    }

    fn marker_keys(&self) -> Vec<String> {
        self.first.control_keys(&format!(
            "garbage-collection/{}/",
            logical_path_hash(&self.blob)
        ))
    }

    async fn marker_bytes(&self, through: u64, collected: Vec<u64>, collected_at: u64) -> Vec<u8> {
        SignedDocument::create(
            GarbageCollectionMarker {
                api_version: "overmesh.io/garbage-collection-marker/v1".to_owned(),
                blob: self.blob.clone(),
                head_object: self.head_object.clone(),
                ring_version: 1,
                history_head_logical_version: self
                    .history
                    .last()
                    .expect("active")
                    .signed
                    .payload
                    .logical_version,
                collected_through_logical_version: through,
                collected_committed_versions: collected,
                previous_marker_sha256: None,
                physical_collection_delay_ms: 0,
                collected_at_unix_ms: collected_at,
                signing_key_id: self.signer.key_id().to_owned(),
            },
            SignatureDomain::GarbageCollectionMarker,
            self.signer.as_ref(),
        )
        .await
        .expect("marker")
        .canonical_bytes()
        .expect("marker bytes")
    }

    async fn checkpoint_bytes(
        &self,
        through: u64,
        checkpoint_version: u64,
        previous: Option<(u64, &[u8])>,
        marker_bytes: &[u8],
        compacted_at: u64,
    ) -> Vec<u8> {
        let marker = SignedDocument::<GarbageCollectionMarker>::from_bytes(marker_bytes)
            .expect("marker JSON");
        let terminal = &self.history[usize::try_from(through - 1).expect("history index")];
        SignedDocument::create(
            HistoryCompactionCheckpoint {
                api_version: HISTORY_COMPACTION_API_VERSION.to_owned(),
                blob: self.blob.clone(),
                path_hash: logical_path_hash(&self.blob),
                head_object: self.head_object.clone(),
                ring_version: 1,
                checkpoint_version,
                compacted_through_logical_version: through,
                compacted_through_state: terminal.signed.payload.state,
                compacted_through_logical_etag: terminal.signed.payload.logical_etag.clone(),
                compacted_through_committed_at_unix_ms: terminal
                    .signed
                    .payload
                    .committed_at_unix_ms,
                covered_terminal_manifest_sha256: sha256_bytes(&terminal.bytes),
                previous_checkpoint_sha256: previous.map(|(_, bytes)| sha256_bytes(bytes)),
                previous_checkpoint_version: previous.map(|(version, _)| version),
                garbage_collection_marker_object: garbage_collection_marker_key(
                    &logical_path_hash(&self.blob),
                    marker.payload.collected_through_logical_version,
                ),
                garbage_collection_marker_sha256: sha256_bytes(marker_bytes),
                garbage_collection_through_logical_version: marker
                    .payload
                    .collected_through_logical_version,
                garbage_collection_history_head_logical_version: marker
                    .payload
                    .history_head_logical_version,
                garbage_collected_committed_versions: marker
                    .payload
                    .collected_committed_versions
                    .clone(),
                garbage_collection_delay_ms: marker.payload.physical_collection_delay_ms,
                garbage_collected_at_unix_ms: marker.payload.collected_at_unix_ms,
                compacted_at_unix_ms: compacted_at,
                signing_key_id: self.signer.key_id().to_owned(),
            },
            SignatureDomain::HistoryCompactionCheckpoint,
            self.signer.as_ref(),
        )
        .await
        .expect("checkpoint")
        .canonical_bytes()
        .expect("checkpoint bytes")
    }
}

struct ManifestFixtureInput<'a> {
    blob: &'a str,
    path_hash: &'a str,
    version: u64,
    state: ManifestState,
    previous: Option<&'a str>,
    committed_at: u64,
    signer: &'a dyn ManifestSigner,
    replicas: &'a [&'a str],
}

async fn signed_manifest(input: ManifestFixtureInput<'_>) -> SignedDocument<CommitManifest> {
    let ManifestFixtureInput {
        blob,
        path_hash,
        version,
        state,
        previous,
        committed_at,
        signer,
        replicas,
    } = input;
    let write_id = format!("write-{version}");
    let (
        content_length,
        content_sha256,
        content_container,
        content_object,
        block_object,
        block_sha,
        prefix,
        deleted_at,
    ) = if state == ManifestState::Tombstoned {
        (
            0,
            sha256_bytes(b"overmesh:tombstone:v1"),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            format!(
                "objects/{path_hash}/tombstones/{}",
                stable_component(&write_id)
            ),
            Some(committed_at),
        )
    } else {
        let content = format!("content-{version}").into_bytes();
        let content_sha256 = sha256_bytes(&content);
        let prefix = format!(
            "objects/{path_hash}/versions/{}/{}",
            stable_component(&write_id),
            content_sha256.trim_start_matches("sha256:")
        );
        (
            u64::try_from(content.len()).expect("content length"),
            content_sha256,
            "container".to_owned(),
            format!(".overmesh/objects/{path_hash}/{version:032x}"),
            format!("{prefix}/block-manifest.json"),
            sha256_bytes(format!("block-{version}").as_bytes()),
            prefix,
            None,
        )
    };
    let payload = CommitManifest {
        blob: blob.to_owned(),
        caller: overmesh_gateway::identity::CallerIdentity {
            tenant_id: "test-tenant".to_owned(),
            object_id: "00000000-0000-0000-0000-000000000001".to_owned(),
            subject: "test-subject".to_owned(),
            authorized_party: None,
        },
        write_id,
        logical_version: version,
        logical_etag: logical_etag(blob, version, &format!("write-{version}"), &content_sha256),
        previous_logical_etag: previous.map(ToOwned::to_owned),
        ring_version: 1,
        content_length,
        content_sha256,
        content_container,
        content_object,
        block_manifest_object: block_object,
        block_manifest_sha256: block_sha,
        version_object_prefix: Some(prefix),
        committed_at_unix_ms: committed_at,
        deleted_at_unix_ms: deleted_at,
        state,
        prepared_replicas: replicas.iter().map(|value| (*value).to_owned()).collect(),
        signing_key_id: signer.key_id().to_owned(),
    };
    SignedDocument::create(payload, SignatureDomain::CommitManifest, signer)
        .await
        .expect("signed manifest")
}

fn test_ring(ids: &[&str]) -> RingDocument {
    RingDocument {
        api_version: "overmesh.io/v1".to_owned(),
        ring_version: 1,
        root: true,
        parent_ring_version: None,
        parent_ring_hash: None,
        replication_factor: 2,
        created_at: "2026-08-16T00:00:00Z".to_owned(),
        signed_at_unix_ms: 1_776_000_000_000,
        signing_key_id: "test-ring-key".to_owned(),
        ring_hash: String::new(),
        nodes: ids
            .iter()
            .enumerate()
            .map(|(index, id)| RingNode {
                id: (*id).to_owned(),
                region: format!("region-{index}"),
                weight: 10,
            })
            .collect(),
    }
}

fn test_token_provider() -> LocalControlTokenProvider {
    LocalControlTokenProvider::new(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.harness/reconciler-token.jwt"),
        true,
    )
    .expect("test token provider")
}

async fn test_token() -> ControlToken {
    test_token_provider().token().await.expect("test token")
}

mod discovery;
mod garbage_collection;
mod orchestration;
mod staging;
