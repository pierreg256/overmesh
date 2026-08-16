use std::{path::Path, sync::Arc, time::SystemTime};

use async_trait::async_trait;
use http::StatusCode;
use reqwest::{
    Client, RequestBuilder,
    header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, ETAG, IF_MATCH, IF_NONE_MATCH, RANGE},
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::fs::File;
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use crate::app::SUPPORTED_STORAGE_VERSION;
use crate::{
    identity::{CallerToken, ControlToken},
    resource::{LogicalBlobId, encode_blob_path, encode_path_component},
};

const SYSTEM_CONTAINER: &str = "overmesh-system";
#[derive(Debug, Clone)]
pub enum PutCondition {
    None,
    IfAbsent,
    IfMatch(String),
}

#[derive(Debug, Clone)]
pub struct PutResult {
    pub etag: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ObjectValue {
    pub bytes: Vec<u8>,
    pub etag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectDigest {
    pub length: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataObjectValidation {
    pub digest: ObjectDigest,
    pub block_sha256: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectListPage {
    pub objects: Vec<String>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendContainer {
    pub name: String,
    pub last_modified_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendContainerListPage {
    pub containers: Vec<BackendContainer>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataObjectProperties {
    pub length: u64,
}

#[derive(Debug, Clone)]
pub struct BackendLease {
    pub object_key: String,
    pub lease_id: String,
}

#[derive(Debug, Error)]
pub enum BackendError {
    #[error("backend precondition failed")]
    PreconditionFailed,
    #[error("backend object already exists")]
    AlreadyExists,
    #[error("backend object is locked")]
    LeaseConflict,
    #[error("backend request failed with status {status}: {message}")]
    Http { status: StatusCode, message: String },
    #[error("backend transport failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("backend I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("backend response is missing required header {0}")]
    MissingHeader(&'static str),
    #[error("backend response is invalid: {0}")]
    InvalidResponse(String),
}

impl BackendError {
    pub fn is_unavailable(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::Http { status, .. } => {
                status.is_server_error()
                    || *status == StatusCode::REQUEST_TIMEOUT
                    || *status == StatusCode::TOO_MANY_REQUESTS
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthorizationProbeOutcome {
    Authorized,
    Denied,
    UnexpectedStatus,
}

#[async_trait]
pub trait ReplicaBackend: Send + Sync {
    fn id(&self) -> &str;

    async fn validate_control_container(
        &self,
        control_token: &ControlToken,
    ) -> Result<(), BackendError>;

    async fn control_put_bytes(
        &self,
        object_key: &str,
        bytes: Vec<u8>,
        content_type: &'static str,
        condition: PutCondition,
        control_token: &ControlToken,
    ) -> Result<PutResult, BackendError>;

    async fn authorize_blob_read(
        &self,
        blob: &LogicalBlobId,
        caller_token: &CallerToken,
    ) -> Result<(), BackendError>;

    async fn authorize_existing_blob_write(
        &self,
        _container: &str,
        _object_key: &str,
        _caller_token: &CallerToken,
    ) -> Result<(), BackendError> {
        Err(BackendError::InvalidResponse(
            "existing-blob write authorization is not implemented by this backend".to_owned(),
        ))
    }

    async fn authorize_account_list(
        &self,
        _caller_token: &CallerToken,
    ) -> Result<(), BackendError> {
        Err(BackendError::InvalidResponse(
            "account listing authorization is not implemented by this backend".to_owned(),
        ))
    }

    async fn authorize_container_list(
        &self,
        _container: &str,
        _caller_token: &CallerToken,
    ) -> Result<(), BackendError> {
        Err(BackendError::InvalidResponse(
            "container listing authorization is not implemented by this backend".to_owned(),
        ))
    }

    async fn caller_list_containers_page(
        &self,
        _prefix: &str,
        _after: Option<&str>,
        _limit: usize,
        _caller_token: &CallerToken,
    ) -> Result<BackendContainerListPage, BackendError> {
        Err(BackendError::InvalidResponse(
            "typed account container listing is not implemented by this backend".to_owned(),
        ))
    }

    async fn authorize_blob_delete(
        &self,
        blob: &LogicalBlobId,
        caller_token: &CallerToken,
    ) -> Result<(), BackendError>;

    async fn caller_head_data_object(
        &self,
        container: &str,
        object_key: &str,
        caller_token: &CallerToken,
    ) -> Result<Option<DataObjectProperties>, BackendError>;

    async fn caller_get_data_range(
        &self,
        container: &str,
        object_key: &str,
        range: Option<(u64, u64)>,
        caller_token: &CallerToken,
    ) -> Result<Option<Vec<u8>>, BackendError>;

    async fn caller_put_data_file(
        &self,
        container: &str,
        object_key: &str,
        path: &Path,
        length: u64,
        condition: PutCondition,
        caller_token: &CallerToken,
    ) -> Result<PutResult, BackendError>;

    async fn caller_digest_data_object(
        &self,
        container: &str,
        object_key: &str,
        caller_token: &CallerToken,
    ) -> Result<Option<ObjectDigest>, BackendError>;

    async fn control_get_object(
        &self,
        object_key: &str,
        control_token: &ControlToken,
    ) -> Result<Option<ObjectValue>, BackendError>;

    async fn control_list_objects(
        &self,
        prefix: &str,
        control_token: &ControlToken,
    ) -> Result<Vec<String>, BackendError>;

    async fn control_list_objects_page(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
        control_token: &ControlToken,
    ) -> Result<ObjectListPage, BackendError>;

    async fn control_delete_object(
        &self,
        object_key: &str,
        expected_etag: Option<&str>,
        control_token: &ControlToken,
    ) -> Result<(), BackendError>;

    async fn control_acquire_lock(
        &self,
        object_key: &str,
        control_token: &ControlToken,
    ) -> Result<BackendLease, BackendError>;

    async fn control_release_lock(
        &self,
        lease: &BackendLease,
        control_token: &ControlToken,
    ) -> Result<(), BackendError>;

    async fn control_renew_lock(
        &self,
        lease: &BackendLease,
        control_token: &ControlToken,
    ) -> Result<(), BackendError>;

    async fn service_get_data_object(
        &self,
        container: &str,
        object_key: &str,
        control_token: &ControlToken,
    ) -> Result<Option<ObjectValue>, BackendError>;

    async fn service_validate_data_object(
        &self,
        container: &str,
        object_key: &str,
        block_lengths: &[u64],
        control_token: &ControlToken,
    ) -> Result<Option<DataObjectValidation>, BackendError>;

    async fn service_put_data_bytes(
        &self,
        container: &str,
        object_key: &str,
        bytes: Vec<u8>,
        condition: PutCondition,
        control_token: &ControlToken,
    ) -> Result<PutResult, BackendError>;

    async fn service_delete_data_object(
        &self,
        container: &str,
        object_key: &str,
        expected_etag: Option<&str>,
        control_token: &ControlToken,
    ) -> Result<(), BackendError>;
}

#[derive(Clone)]
pub struct HttpBlobBackend {
    id: String,
    endpoint: String,
    client: Client,
}

impl HttpBlobBackend {
    pub fn new(
        id: impl Into<String>,
        endpoint: impl Into<String>,
        danger_accept_invalid_certificates: bool,
    ) -> anyhow::Result<Self> {
        let endpoint = endpoint.into().trim_end_matches('/').to_owned();
        let client = Client::builder()
            .danger_accept_invalid_certs(danger_accept_invalid_certificates)
            .build()?;
        Ok(Self {
            id: id.into(),
            endpoint,
            client,
        })
    }

    fn container_url(&self) -> String {
        format!("{}/{SYSTEM_CONTAINER}", self.endpoint)
    }

    fn object_url(&self, object_key: &str) -> String {
        format!("{}/{SYSTEM_CONTAINER}/{object_key}", self.endpoint)
    }

    fn data_object_url(&self, container: &str, object_key: &str) -> String {
        format!(
            "{}/{}/{}",
            self.endpoint,
            encode_path_component(container),
            encode_blob_path(object_key)
        )
    }

    fn authorized(&self, request: RequestBuilder, bearer_token: &str) -> RequestBuilder {
        request
            .header(AUTHORIZATION, format!("Bearer {bearer_token}"))
            .header("x-ms-version", SUPPORTED_STORAGE_VERSION)
            .header("x-ms-date", httpdate::fmt_http_date(SystemTime::now()))
    }

    async fn finish_put(response: reqwest::Response) -> Result<PutResult, BackendError> {
        if response.status().is_success() {
            return Ok(PutResult {
                etag: response
                    .headers()
                    .get(ETAG)
                    .and_then(|value| value.to_str().ok())
                    .map(ToOwned::to_owned),
            });
        }
        Err(response_error(response).await)
    }
}

#[async_trait]
impl ReplicaBackend for HttpBlobBackend {
    fn id(&self) -> &str {
        &self.id
    }

    async fn validate_control_container(
        &self,
        control_token: &ControlToken,
    ) -> Result<(), BackendError> {
        let response = self
            .authorized(
                self.client
                    .head(format!("{}?restype=container", self.container_url())),
                control_token.expose(),
            )
            .send()
            .await?;
        if response.status().is_success() {
            return Ok(());
        }
        Err(response_error(response).await)
    }

    async fn control_put_bytes(
        &self,
        object_key: &str,
        bytes: Vec<u8>,
        content_type: &'static str,
        condition: PutCondition,
        control_token: &ControlToken,
    ) -> Result<PutResult, BackendError> {
        let length = bytes.len();
        let request = self
            .authorized(
                self.client.put(self.object_url(object_key)),
                control_token.expose(),
            )
            .header("x-ms-blob-type", "BlockBlob")
            .header(CONTENT_TYPE, content_type)
            .header(CONTENT_LENGTH, length)
            .body(bytes);
        Self::finish_put(apply_condition(request, condition).send().await?).await
    }

    async fn authorize_blob_read(
        &self,
        blob: &LogicalBlobId,
        caller_token: &CallerToken,
    ) -> Result<(), BackendError> {
        let response = self
            .authorized(
                self.client
                    .head(self.data_object_url(blob.container(), blob.blob())),
                caller_token.expose(),
            )
            .send()
            .await?;
        if response.status().is_success() || response.status() == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(response_error(response).await)
        }
    }

    async fn authorize_existing_blob_write(
        &self,
        container: &str,
        object_key: &str,
        caller_token: &CallerToken,
    ) -> Result<(), BackendError> {
        let response = self
            .authorized(
                self.client.put(self.data_object_url(container, object_key)),
                caller_token.expose(),
            )
            .header("x-ms-blob-type", "BlockBlob")
            .header(CONTENT_LENGTH, 0)
            .header(IF_NONE_MATCH, "*")
            .body(Vec::new())
            .send()
            .await?;
        if matches!(
            response.status(),
            StatusCode::CONFLICT | StatusCode::PRECONDITION_FAILED
        ) {
            return Ok(());
        }
        Err(response_error(response).await)
    }

    async fn authorize_account_list(&self, caller_token: &CallerToken) -> Result<(), BackendError> {
        let response = self
            .authorized(
                self.client
                    .get(&self.endpoint)
                    .query(&[("comp", "list"), ("maxresults", "1")]),
                caller_token.expose(),
            )
            .send()
            .await?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(response_error(response).await)
        }
    }

    async fn authorize_container_list(
        &self,
        container: &str,
        caller_token: &CallerToken,
    ) -> Result<(), BackendError> {
        let response = self
            .authorized(
                self.client
                    .get(format!(
                        "{}/{}",
                        self.endpoint,
                        encode_path_component(container)
                    ))
                    .query(&[
                        ("restype", "container"),
                        ("comp", "list"),
                        ("maxresults", "1"),
                    ]),
                caller_token.expose(),
            )
            .send()
            .await?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(response_error(response).await)
        }
    }

    async fn caller_list_containers_page(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
        caller_token: &CallerToken,
    ) -> Result<BackendContainerListPage, BackendError> {
        let max_results = limit.clamp(1, 5_000).to_string();
        let mut request = self.authorized(
            self.client.get(&self.endpoint).query(&[
                ("comp", "list"),
                ("prefix", prefix),
                ("maxresults", max_results.as_str()),
            ]),
            caller_token.expose(),
        );
        if let Some(value) = cursor {
            request = request.query(&[("marker", value)]);
        }
        let response = request.send().await?;
        if !response.status().is_success() {
            return Err(response_error(response).await);
        }
        let page: ContainerListResponse =
            quick_xml::de::from_reader(response.bytes().await?.as_ref())
                .map_err(|error| BackendError::InvalidResponse(error.to_string()))?;
        let containers = page
            .containers
            .entries
            .into_iter()
            .map(|entry| {
                let modified = httpdate::parse_http_date(&entry.properties.last_modified)
                    .map_err(|error| BackendError::InvalidResponse(error.to_string()))?;
                let last_modified_unix_ms = u64::try_from(
                    modified
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_err(|error| BackendError::InvalidResponse(error.to_string()))?
                        .as_millis(),
                )
                .map_err(|_| {
                    BackendError::InvalidResponse(
                        "container Last-Modified exceeds u64 milliseconds".to_owned(),
                    )
                })?;
                Ok(BackendContainer {
                    name: entry.name,
                    last_modified_unix_ms,
                })
            })
            .collect::<Result<Vec<_>, BackendError>>()?;
        Ok(BackendContainerListPage {
            containers,
            next_cursor: page.next_marker.filter(|value| !value.is_empty()),
        })
    }

    async fn authorize_blob_delete(
        &self,
        blob: &LogicalBlobId,
        caller_token: &CallerToken,
    ) -> Result<(), BackendError> {
        let response = self
            .authorized(
                self.client
                    .delete(self.data_object_url(blob.container(), blob.blob())),
                caller_token.expose(),
            )
            .query(&[("snapshot", "2000-01-01T00:00:00.0000000Z")])
            .send()
            .await?;
        match delete_authorization_probe_outcome(response.status()) {
            AuthorizationProbeOutcome::Authorized => Ok(()),
            AuthorizationProbeOutcome::UnexpectedStatus => {
                Err(BackendError::InvalidResponse(format!(
                    "delete authorization probe returned unexpected success status {}",
                    response.status()
                )))
            }
            AuthorizationProbeOutcome::Denied => Err(response_error(response).await),
        }
    }

    async fn caller_head_data_object(
        &self,
        container: &str,
        object_key: &str,
        caller_token: &CallerToken,
    ) -> Result<Option<DataObjectProperties>, BackendError> {
        let response = self
            .authorized(
                self.client
                    .head(self.data_object_url(container, object_key)),
                caller_token.expose(),
            )
            .send()
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(response_error(response).await);
        }
        let length = response
            .headers()
            .get(CONTENT_LENGTH)
            .ok_or(BackendError::MissingHeader("Content-Length"))?
            .to_str()
            .map_err(|_| BackendError::InvalidResponse("invalid Content-Length".to_owned()))?
            .parse::<u64>()
            .map_err(|_| BackendError::InvalidResponse("invalid Content-Length".to_owned()))?;
        Ok(Some(DataObjectProperties { length }))
    }

    async fn caller_get_data_range(
        &self,
        container: &str,
        object_key: &str,
        range: Option<(u64, u64)>,
        caller_token: &CallerToken,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        let mut request = self.authorized(
            self.client.get(self.data_object_url(container, object_key)),
            caller_token.expose(),
        );
        if let Some((start, end)) = range {
            request = request.header(RANGE, format!("bytes={start}-{end}"));
        }
        let response = request.send().await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let expected_status = if range.is_some() {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        };
        if response.status() != expected_status {
            return Err(response_error(response).await);
        }
        Ok(Some(response.bytes().await?.to_vec()))
    }

    async fn caller_put_data_file(
        &self,
        container: &str,
        object_key: &str,
        path: &Path,
        length: u64,
        condition: PutCondition,
        caller_token: &CallerToken,
    ) -> Result<PutResult, BackendError> {
        let file = File::open(path).await?;
        let body = reqwest::Body::wrap_stream(ReaderStream::new(file));
        let request = self
            .authorized(
                self.client.put(self.data_object_url(container, object_key)),
                caller_token.expose(),
            )
            .header("x-ms-blob-type", "BlockBlob")
            .header(CONTENT_TYPE, "application/octet-stream")
            .header(CONTENT_LENGTH, length)
            .body(body);
        Self::finish_put(apply_condition(request, condition).send().await?).await
    }

    async fn caller_digest_data_object(
        &self,
        container: &str,
        object_key: &str,
        caller_token: &CallerToken,
    ) -> Result<Option<ObjectDigest>, BackendError> {
        let response = self
            .authorized(
                self.client.get(self.data_object_url(container, object_key)),
                caller_token.expose(),
            )
            .send()
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(response_error(response).await);
        }
        let mut stream = response.bytes_stream();
        let mut hasher = Sha256::new();
        let mut length = 0_u64;
        while let Some(chunk) = futures_util::StreamExt::next(&mut stream).await {
            let chunk = chunk?;
            length = length
                .checked_add(u64::try_from(chunk.len()).map_err(|_| {
                    BackendError::InvalidResponse("content length exceeds u64".to_owned())
                })?)
                .ok_or_else(|| {
                    BackendError::InvalidResponse("content length overflow".to_owned())
                })?;
            hasher.update(&chunk);
        }
        Ok(Some(ObjectDigest {
            length,
            sha256: format!("sha256:{}", hex::encode(hasher.finalize())),
        }))
    }

    async fn control_get_object(
        &self,
        object_key: &str,
        control_token: &ControlToken,
    ) -> Result<Option<ObjectValue>, BackendError> {
        let response = self
            .authorized(
                self.client.get(self.object_url(object_key)),
                control_token.expose(),
            )
            .send()
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(response_error(response).await);
        }
        let etag = response
            .headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned);
        let bytes = response.bytes().await?.to_vec();
        Ok(Some(ObjectValue { bytes, etag }))
    }

    async fn control_list_objects(
        &self,
        prefix: &str,
        control_token: &ControlToken,
    ) -> Result<Vec<String>, BackendError> {
        let mut marker: Option<String> = None;
        let mut objects = Vec::new();
        loop {
            let page = self
                .control_list_objects_page(prefix, marker.as_deref(), 5_000, control_token)
                .await?;
            objects.extend(page.objects);
            marker = page.next_cursor;
            if marker.is_none() {
                break;
            }
        }
        Ok(objects)
    }

    async fn control_list_objects_page(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
        control_token: &ControlToken,
    ) -> Result<ObjectListPage, BackendError> {
        let max_results = limit.clamp(1, 5_000).to_string();
        let mut request = self.authorized(
            self.client.get(self.container_url()).query(&[
                ("restype", "container"),
                ("comp", "list"),
                ("prefix", prefix),
                ("maxresults", max_results.as_str()),
            ]),
            control_token.expose(),
        );
        if let Some(value) = cursor {
            request = request.query(&[("marker", value)]);
        }
        let response = request.send().await?;
        if !response.status().is_success() {
            return Err(response_error(response).await);
        }
        let page: BlobListResponse =
            quick_xml::de::from_reader(response.bytes().await?.as_ref())
                .map_err(|error| BackendError::InvalidResponse(error.to_string()))?;
        Ok(ObjectListPage {
            objects: page
                .blobs
                .entries
                .into_iter()
                .map(|entry| entry.name)
                .collect(),
            next_cursor: page.next_marker.filter(|value| !value.is_empty()),
        })
    }

    async fn control_delete_object(
        &self,
        object_key: &str,
        expected_etag: Option<&str>,
        control_token: &ControlToken,
    ) -> Result<(), BackendError> {
        let mut request = self.authorized(
            self.client.delete(self.object_url(object_key)),
            control_token.expose(),
        );
        if let Some(etag) = expected_etag {
            request = request.header(IF_MATCH, etag);
        }
        let response = request.send().await?;
        if response.status().is_success() || response.status() == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(response_error(response).await)
        }
    }

    async fn control_acquire_lock(
        &self,
        object_key: &str,
        control_token: &ControlToken,
    ) -> Result<BackendLease, BackendError> {
        match self
            .control_put_bytes(
                object_key,
                Vec::new(),
                "application/octet-stream",
                PutCondition::IfAbsent,
                control_token,
            )
            .await
        {
            Ok(_) | Err(BackendError::PreconditionFailed | BackendError::AlreadyExists) => {}
            Err(error) => return Err(error),
        }

        let proposed_lease_id = Uuid::new_v4().to_string();
        let response = self
            .authorized(
                self.client
                    .put(format!("{}?comp=lease", self.object_url(object_key))),
                control_token.expose(),
            )
            .header("x-ms-lease-action", "acquire")
            .header("x-ms-lease-duration", "60")
            .header("x-ms-proposed-lease-id", &proposed_lease_id)
            .header(CONTENT_LENGTH, 0)
            .send()
            .await?;
        if response.status().is_success() {
            let lease_id = response
                .headers()
                .get("x-ms-lease-id")
                .and_then(|value| value.to_str().ok())
                .unwrap_or(&proposed_lease_id)
                .to_owned();
            return Ok(BackendLease {
                object_key: object_key.to_owned(),
                lease_id,
            });
        }
        if response.status() == StatusCode::CONFLICT {
            return Err(BackendError::LeaseConflict);
        }
        Err(response_error(response).await)
    }

    async fn control_release_lock(
        &self,
        lease: &BackendLease,
        control_token: &ControlToken,
    ) -> Result<(), BackendError> {
        let response = self
            .authorized(
                self.client
                    .put(format!("{}?comp=lease", self.object_url(&lease.object_key))),
                control_token.expose(),
            )
            .header("x-ms-lease-action", "release")
            .header("x-ms-lease-id", &lease.lease_id)
            .header(CONTENT_LENGTH, 0)
            .send()
            .await?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(response_error(response).await)
        }
    }

    async fn control_renew_lock(
        &self,
        lease: &BackendLease,
        control_token: &ControlToken,
    ) -> Result<(), BackendError> {
        let response = self
            .authorized(
                self.client
                    .put(format!("{}?comp=lease", self.object_url(&lease.object_key))),
                control_token.expose(),
            )
            .header("x-ms-lease-action", "renew")
            .header("x-ms-lease-id", &lease.lease_id)
            .header(CONTENT_LENGTH, 0)
            .send()
            .await?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(response_error(response).await)
        }
    }

    async fn service_get_data_object(
        &self,
        container: &str,
        object_key: &str,
        control_token: &ControlToken,
    ) -> Result<Option<ObjectValue>, BackendError> {
        let response = self
            .authorized(
                self.client.get(self.data_object_url(container, object_key)),
                control_token.expose(),
            )
            .send()
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(response_error(response).await);
        }
        let etag = response
            .headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned);
        let bytes = response.bytes().await?.to_vec();
        Ok(Some(ObjectValue { bytes, etag }))
    }

    async fn service_validate_data_object(
        &self,
        container: &str,
        object_key: &str,
        block_lengths: &[u64],
        control_token: &ControlToken,
    ) -> Result<Option<DataObjectValidation>, BackendError> {
        let response = self
            .authorized(
                self.client.get(self.data_object_url(container, object_key)),
                control_token.expose(),
            )
            .send()
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(response_error(response).await);
        }

        let mut stream = response.bytes_stream();
        let mut complete_hasher = Sha256::new();
        let mut complete_length = 0_u64;
        let mut block_hashes = Vec::with_capacity(block_lengths.len());
        let mut block_index = 0_usize;
        let mut block_remaining = block_lengths.first().copied().unwrap_or(0);
        let mut block_hasher = Sha256::new();

        while block_index < block_lengths.len() && block_remaining == 0 {
            block_hashes.push(format!(
                "sha256:{}",
                hex::encode(block_hasher.finalize_reset())
            ));
            block_index += 1;
            block_remaining = block_lengths.get(block_index).copied().unwrap_or(0);
        }

        while let Some(chunk) = futures_util::StreamExt::next(&mut stream).await {
            let chunk = chunk?;
            complete_length = complete_length
                .checked_add(u64::try_from(chunk.len()).map_err(|_| {
                    BackendError::InvalidResponse("content length exceeds u64".to_owned())
                })?)
                .ok_or_else(|| {
                    BackendError::InvalidResponse("content length overflow".to_owned())
                })?;
            complete_hasher.update(&chunk);
            let mut remaining = chunk.as_ref();
            while !remaining.is_empty() {
                if block_index >= block_lengths.len() {
                    return Err(BackendError::InvalidResponse(
                        "content exceeds the committed block layout".to_owned(),
                    ));
                }
                let take =
                    usize::try_from(block_remaining.min(u64::try_from(remaining.len()).map_err(
                        |_| BackendError::InvalidResponse("chunk length exceeds u64".to_owned()),
                    )?))
                    .map_err(|_| {
                        BackendError::InvalidResponse("block length exceeds usize".to_owned())
                    })?;
                block_hasher.update(&remaining[..take]);
                remaining = &remaining[take..];
                block_remaining -= u64::try_from(take).map_err(|_| {
                    BackendError::InvalidResponse("block length exceeds u64".to_owned())
                })?;
                if block_remaining == 0 {
                    block_hashes.push(format!(
                        "sha256:{}",
                        hex::encode(block_hasher.finalize_reset())
                    ));
                    block_index += 1;
                    block_remaining = block_lengths.get(block_index).copied().unwrap_or(0);
                    while block_index < block_lengths.len() && block_remaining == 0 {
                        block_hashes.push(format!(
                            "sha256:{}",
                            hex::encode(block_hasher.finalize_reset())
                        ));
                        block_index += 1;
                        block_remaining = block_lengths.get(block_index).copied().unwrap_or(0);
                    }
                }
            }
        }
        if block_index != block_lengths.len() {
            return Err(BackendError::InvalidResponse(
                "content is shorter than the committed block layout".to_owned(),
            ));
        }
        Ok(Some(DataObjectValidation {
            digest: ObjectDigest {
                length: complete_length,
                sha256: format!("sha256:{}", hex::encode(complete_hasher.finalize())),
            },
            block_sha256: block_hashes,
        }))
    }

    async fn service_put_data_bytes(
        &self,
        container: &str,
        object_key: &str,
        bytes: Vec<u8>,
        condition: PutCondition,
        control_token: &ControlToken,
    ) -> Result<PutResult, BackendError> {
        let request = self
            .authorized(
                self.client.put(self.data_object_url(container, object_key)),
                control_token.expose(),
            )
            .header("x-ms-blob-type", "BlockBlob")
            .header(CONTENT_TYPE, "application/octet-stream")
            .header(CONTENT_LENGTH, bytes.len())
            .body(bytes);
        Self::finish_put(apply_condition(request, condition).send().await?).await
    }

    async fn service_delete_data_object(
        &self,
        container: &str,
        object_key: &str,
        expected_etag: Option<&str>,
        control_token: &ControlToken,
    ) -> Result<(), BackendError> {
        let mut request = self.authorized(
            self.client
                .delete(self.data_object_url(container, object_key)),
            control_token.expose(),
        );
        if let Some(etag) = expected_etag {
            request = request.header(IF_MATCH, etag);
        }
        let response = request.send().await?;
        if response.status().is_success() || response.status() == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(response_error(response).await)
        }
    }
}

fn apply_condition(request: RequestBuilder, condition: PutCondition) -> RequestBuilder {
    match condition {
        PutCondition::None => request,
        PutCondition::IfAbsent => request.header(IF_NONE_MATCH, "*"),
        PutCondition::IfMatch(etag) => request.header(IF_MATCH, etag),
    }
}

fn delete_authorization_probe_outcome(status: StatusCode) -> AuthorizationProbeOutcome {
    if status == StatusCode::NOT_FOUND {
        AuthorizationProbeOutcome::Authorized
    } else if status.is_success() {
        AuthorizationProbeOutcome::UnexpectedStatus
    } else {
        AuthorizationProbeOutcome::Denied
    }
}

async fn response_error(response: reqwest::Response) -> BackendError {
    let status = response.status();
    if status == StatusCode::PRECONDITION_FAILED {
        return BackendError::PreconditionFailed;
    }

    let error_code = response
        .headers()
        .get("x-ms-error-code")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("UnknownStorageError")
        .to_owned();
    if status == StatusCode::CONFLICT && error_code == "BlobAlreadyExists" {
        return BackendError::AlreadyExists;
    }
    let body = response.text().await.unwrap_or_default();
    BackendError::Http {
        status,
        message: if body.is_empty() {
            error_code
        } else {
            format!("{error_code}: {body}")
        },
    }
}

pub type SharedBackend = Arc<dyn ReplicaBackend>;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct BlobListResponse {
    #[serde(default)]
    blobs: BlobEntries,
    next_marker: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct BlobEntries {
    #[serde(rename = "Blob", default)]
    entries: Vec<BlobEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct BlobEntry {
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ContainerListResponse {
    #[serde(default)]
    containers: ContainerEntries,
    next_marker: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ContainerEntries {
    #[serde(rename = "Container", default)]
    entries: Vec<ContainerEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ContainerEntry {
    name: String,
    properties: ContainerProperties,
}

#[derive(Debug, Deserialize)]
struct ContainerProperties {
    #[serde(rename = "Last-Modified")]
    last_modified: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_authorization_probe_statuses_fail_closed() {
        assert_eq!(
            delete_authorization_probe_outcome(StatusCode::NOT_FOUND),
            AuthorizationProbeOutcome::Authorized
        );
        assert_eq!(
            delete_authorization_probe_outcome(StatusCode::FORBIDDEN),
            AuthorizationProbeOutcome::Denied
        );
        assert_eq!(
            delete_authorization_probe_outcome(StatusCode::PRECONDITION_FAILED),
            AuthorizationProbeOutcome::Denied
        );
        assert_eq!(
            delete_authorization_probe_outcome(StatusCode::ACCEPTED),
            AuthorizationProbeOutcome::UnexpectedStatus
        );
    }
}
