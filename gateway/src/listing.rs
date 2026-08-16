use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use thiserror::Error;

use crate::{
    auth::AuthenticatedPrincipal,
    backend::{BackendError, SharedBackend},
    catalog::{
        catalog_containers_prefix, catalog_listing_prefix, logical_blob_from_catalog_key,
        validate_catalog_entry,
    },
    commit::CommitCoordinator,
    continuation::{
        ContinuationBinding, ContinuationError, ContinuationScope, ContinuationState, issue, verify,
    },
    identity::SharedControlTokenProvider,
    manifest::{ManifestSigner, ManifestState, commit_manifest_object_prefix},
    read::BlobMetadata,
    ring::RingDocument,
};

pub const DEFAULT_MAX_RESULTS: u32 = 5_000;
pub const MAX_RESULTS: u32 = 5_000;
const MIN_CATALOG_PAGE_SIZE: usize = 32;
const SYSTEM_CONTAINER: &str = "overmesh-system";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListRequest {
    pub prefix: String,
    pub delimiter: String,
    pub marker: Option<String>,
    pub max_results: u32,
    pub include: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedBlob {
    pub name: String,
    pub metadata: BlobMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobListEntry {
    Blob(ListedBlob),
    Prefix(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobListPage {
    pub container: String,
    pub prefix: String,
    pub delimiter: String,
    pub max_results: u32,
    pub marker: String,
    pub entries: Vec<BlobListEntry>,
    pub next_marker: Option<String>,
    pub include_metadata: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedContainer {
    pub name: String,
    pub last_modified_unix_ms: u64,
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerListPage {
    pub prefix: String,
    pub max_results: u32,
    pub marker: String,
    pub containers: Vec<ListedContainer>,
    pub next_marker: Option<String>,
}

#[derive(Debug, Error)]
pub enum ListingError {
    #[error("listing request is invalid: {0}")]
    InvalidRequest(String),
    #[error("listing continuation marker is invalid: {0}")]
    InvalidMarker(#[from] ContinuationError),
    #[error("container does not exist")]
    ContainerNotFound,
    #[error("listing authorization failed")]
    Authorization,
    #[error("listing backend failed: {0}")]
    Backend(#[from] BackendError),
}

#[derive(Clone)]
pub struct ListingService {
    logical_account: String,
    ring: Arc<RingDocument>,
    backends: BTreeMap<String, SharedBackend>,
    signer: Arc<dyn ManifestSigner>,
    control_tokens: SharedControlTokenProvider,
    token_lifetime: Duration,
}

impl ListRequest {
    pub fn new(
        prefix: String,
        delimiter: String,
        marker: Option<String>,
        max_results: Option<u32>,
        include: Vec<String>,
    ) -> Result<Self, ListingError> {
        let max_results = max_results.unwrap_or(DEFAULT_MAX_RESULTS);
        if !(1..=MAX_RESULTS).contains(&max_results) {
            return Err(ListingError::InvalidRequest(
                "maxresults must be between 1 and 5000".to_owned(),
            ));
        }
        if delimiter.chars().count() > 1 {
            return Err(ListingError::InvalidRequest(
                "the published V1 subset supports only an empty or one-character delimiter"
                    .to_owned(),
            ));
        }
        let mut include = include;
        include.sort();
        include.dedup();
        if include.iter().any(|value| value != "metadata") {
            return Err(ListingError::InvalidRequest(
                "only include=metadata is supported".to_owned(),
            ));
        }
        Ok(Self {
            prefix,
            delimiter,
            marker,
            max_results,
            include,
        })
    }
}

impl ListingService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        logical_account: impl Into<String>,
        ring: Arc<RingDocument>,
        backends: std::collections::HashMap<String, SharedBackend>,
        signer: Arc<dyn ManifestSigner>,
        control_tokens: SharedControlTokenProvider,
        token_lifetime: Duration,
    ) -> Self {
        Self {
            logical_account: logical_account.into(),
            ring,
            backends: backends.into_iter().collect(),
            signer,
            control_tokens,
            token_lifetime,
        }
    }

    pub async fn list_blobs(
        &self,
        container: &str,
        request: &ListRequest,
        principal: &AuthenticatedPrincipal,
    ) -> Result<BlobListPage, ListingError> {
        if container == SYSTEM_CONTAINER {
            return Err(ListingError::ContainerNotFound);
        }
        self.authorize_container(container, principal).await?;
        let binding = self.binding(
            ContinuationScope::Blobs,
            Some(container.to_owned()),
            request,
        );
        let physical_prefix = catalog_listing_prefix(container, &request.prefix);
        let backend_cursors = self.initial_backend_cursors();
        let state = request
            .marker
            .as_deref()
            .map(|marker| verify(marker, &binding, self.signer.as_ref()))
            .transpose()?;
        if let Some(state) = &state {
            self.validate_backend_cursors(&state.backend_cursors)?;
        }
        let mut after = state.as_ref().map(|state| state.last_ordering_key.clone());
        if after.as_ref().is_some_and(|key| {
            !key.starts_with(&catalog_listing_prefix(container, &request.prefix))
        }) {
            return Err(ListingError::InvalidMarker(ContinuationError::Ordering));
        }
        let control_token = self
            .control_tokens
            .token()
            .await
            .map_err(|error| BackendError::InvalidResponse(error.to_string()))?;
        let limit = usize::try_from(request.max_results).expect("maxresults fits usize");
        let batch_size = limit.saturating_add(1).max(MIN_CATALOG_PAGE_SIZE);
        let mut backend_cursors = state.map_or(backend_cursors, |state| state.backend_cursors);
        let mut entries = Vec::with_capacity(limit);
        let mut last_emitted_prefix = None;
        let mut has_more = false;
        let mut continuation_cursors = backend_cursors.clone();
        'catalog: loop {
            let page_start_cursors = backend_cursors.clone();
            let (keys, next_cursors) = self
                .catalog_keys_page(
                    &physical_prefix,
                    &backend_cursors,
                    batch_size,
                    &control_token,
                )
                .await?;
            if keys.is_empty() {
                break;
            }
            let mut consumed_any = false;
            for key in keys {
                if after.as_ref().is_some_and(|cursor| key <= *cursor) {
                    continue;
                }
                let candidate = self.validated_catalog_blob(&key, &control_token).await?;
                let Some((name, metadata)) = candidate else {
                    after = Some(key);
                    consumed_any = true;
                    continue;
                };
                if is_internal_blob_name(&name) {
                    after = Some(key);
                    consumed_any = true;
                    continue;
                }
                let output = if !request.delimiter.is_empty()
                    && let Some(index) = name[request.prefix.len()..].find(&request.delimiter)
                {
                    let end = request.prefix.len() + index + request.delimiter.len();
                    BlobListEntry::Prefix(name[..end].to_owned())
                } else {
                    BlobListEntry::Blob(ListedBlob { name, metadata })
                };
                let duplicate_prefix = match &output {
                    BlobListEntry::Prefix(prefix) => {
                        last_emitted_prefix.as_deref() == Some(prefix.as_str())
                    }
                    BlobListEntry::Blob(_) => false,
                };
                if duplicate_prefix {
                    after = Some(key.clone());
                    consumed_any = true;
                    continue;
                }
                if entries.len() == limit {
                    has_more = true;
                    continuation_cursors = page_start_cursors;
                    break 'catalog;
                }
                if let BlobListEntry::Prefix(prefix) = &output {
                    last_emitted_prefix = Some(prefix.clone());
                } else {
                    last_emitted_prefix = None;
                }
                entries.push(output);
                after = Some(key.clone());
                consumed_any = true;
            }
            backend_cursors = next_cursors;
            if !consumed_any || backend_cursors.values().all(Option::is_none) {
                break;
            }
        }
        let next_marker = if has_more {
            let last_key = after
                .as_ref()
                .expect("a page with more results consumed a catalog item");
            Some(
                issue(
                    &binding,
                    &ContinuationState {
                        last_ordering_key: last_key.clone(),
                        backend_cursors: continuation_cursors,
                    },
                    self.token_lifetime,
                    self.signer.as_ref(),
                )
                .await?,
            )
        } else {
            None
        };
        Ok(BlobListPage {
            container: container.to_owned(),
            prefix: request.prefix.clone(),
            delimiter: request.delimiter.clone(),
            max_results: request.max_results,
            marker: request.marker.clone().unwrap_or_default(),
            entries,
            next_marker,
            include_metadata: request.include.iter().any(|value| value == "metadata"),
        })
    }

    pub async fn list_containers(
        &self,
        request: &ListRequest,
        principal: &AuthenticatedPrincipal,
    ) -> Result<ContainerListPage, ListingError> {
        if !request.delimiter.is_empty() || !request.include.is_empty() {
            return Err(ListingError::InvalidRequest(
                "delimiter and include are not supported for container listing".to_owned(),
            ));
        }
        let binding = self.binding(ContinuationScope::Containers, None, request);
        let physical_prefix = catalog_containers_prefix(&request.prefix);
        let initial_cursors = self.initial_backend_cursors();
        let state = request
            .marker
            .as_deref()
            .map(|marker| verify(marker, &binding, self.signer.as_ref()))
            .transpose()?;
        if let Some(state) = &state {
            self.validate_backend_cursors(&state.backend_cursors)?;
        }
        let after = state
            .as_ref()
            .map(|state| state.last_ordering_key.as_str())
            .map(container_name_from_ordering_key)
            .transpose()?;
        let mut backend_cursors = state.map_or(initial_cursors, |state| state.backend_cursors);
        let control_token = self
            .control_tokens
            .token()
            .await
            .map_err(|error| BackendError::InvalidResponse(error.to_string()))?;
        let limit = usize::try_from(request.max_results).expect("maxresults fits usize");
        let batch_size = limit.saturating_add(1).max(MIN_CATALOG_PAGE_SIZE);
        let mut containers = Vec::with_capacity(limit);
        let mut last_container = after.clone();
        let mut continuation_cursors = backend_cursors.clone();
        let mut has_more = false;
        'catalog: loop {
            let page_start_cursors = backend_cursors.clone();
            let (keys, next_cursors) = self
                .catalog_keys_page(
                    &physical_prefix,
                    &backend_cursors,
                    batch_size,
                    &control_token,
                )
                .await?;
            if keys.is_empty() {
                break;
            }
            let mut consumed_any = false;
            for key in keys {
                let Ok(logical_blob) = logical_blob_from_catalog_key(&self.logical_account, &key)
                else {
                    consumed_any = true;
                    continue;
                };
                let candidate = logical_blob.container().to_owned();
                if candidate == SYSTEM_CONTAINER
                    || !candidate.starts_with(&request.prefix)
                    || last_container
                        .as_ref()
                        .is_some_and(|previous| candidate <= *previous)
                {
                    consumed_any = true;
                    continue;
                }
                let Some((_blob, metadata)) =
                    self.validated_catalog_blob(&key, &control_token).await?
                else {
                    consumed_any = true;
                    continue;
                };
                match self.authorize_container(&candidate, principal).await {
                    Ok(()) => {}
                    Err(ListingError::Authorization | ListingError::ContainerNotFound) => {
                        consumed_any = true;
                        continue;
                    }
                    Err(error) => return Err(error),
                }
                if containers.len() == limit {
                    has_more = true;
                    continuation_cursors = page_start_cursors;
                    break 'catalog;
                }
                containers.push(ListedContainer {
                    name: candidate.clone(),
                    last_modified_unix_ms: metadata.committed_at_unix_ms,
                    etag: container_etag(&self.logical_account, &candidate, self.ring.ring_version),
                });
                last_container = Some(candidate);
                consumed_any = true;
            }
            backend_cursors = next_cursors;
            if !consumed_any || backend_cursors.values().all(Option::is_none) {
                break;
            }
        }
        let next_marker = if has_more {
            Some(
                issue(
                    &binding,
                    &ContinuationState {
                        last_ordering_key: container_ordering_key(
                            &containers.last().expect("visible continuation item").name,
                        ),
                        backend_cursors: continuation_cursors,
                    },
                    self.token_lifetime,
                    self.signer.as_ref(),
                )
                .await?,
            )
        } else {
            None
        };
        Ok(ContainerListPage {
            prefix: request.prefix.clone(),
            max_results: request.max_results,
            marker: request.marker.clone().unwrap_or_default(),
            containers,
            next_marker,
        })
    }

    async fn authorize_container(
        &self,
        container: &str,
        principal: &AuthenticatedPrincipal,
    ) -> Result<(), ListingError> {
        let mut missing = 0_usize;
        for backend in self.backends.values() {
            match backend
                .authorize_container_list(container, &principal.access_token)
                .await
            {
                Ok(()) => {}
                Err(BackendError::Http { status, .. }) if status == http::StatusCode::NOT_FOUND => {
                    missing += 1;
                }
                Err(error) => return Err(map_authorization_error(error)),
            }
        }
        if missing != 0 {
            return Err(ListingError::ContainerNotFound);
        }
        Ok(())
    }

    async fn catalog_keys_page(
        &self,
        prefix: &str,
        cursors: &BTreeMap<String, Option<String>>,
        limit: usize,
        token: &crate::identity::ControlToken,
    ) -> Result<(Vec<String>, BTreeMap<String, Option<String>>), ListingError> {
        let mut keys = BTreeSet::new();
        let mut next_cursors = BTreeMap::new();
        for backend in self.backends.values() {
            let cursor = cursors
                .get(backend.id())
                .ok_or(ListingError::InvalidMarker(ContinuationError::Binding))?;
            let page = backend
                .control_list_objects_page(prefix, cursor.as_deref(), limit, token)
                .await?;
            if page.next_cursor.is_some() && page.next_cursor.as_ref() == cursor.as_ref() {
                return Err(ListingError::Backend(BackendError::InvalidResponse(
                    "catalog listing cursor did not advance".to_owned(),
                )));
            }
            next_cursors.insert(backend.id().to_owned(), page.next_cursor);
            keys.extend(page.objects);
        }
        Ok((keys.into_iter().collect(), next_cursors))
    }

    async fn validated_catalog_blob(
        &self,
        object_key: &str,
        token: &crate::identity::ControlToken,
    ) -> Result<Option<(String, BlobMetadata)>, ListingError> {
        let logical_blob = match logical_blob_from_catalog_key(&self.logical_account, object_key) {
            Ok(value) => value,
            Err(_) => return Ok(None),
        };
        let replicas = match self.ring.replicas_for(logical_blob.canonical()) {
            Ok(value) if value.len() == 2 => value,
            _ => return Ok(None),
        };
        let Some(primary) = self.backends.get(&replicas[0].id) else {
            return Ok(None);
        };
        let Some(secondary) = self.backends.get(&replicas[1].id) else {
            return Ok(None);
        };
        let (primary_catalog, secondary_catalog) = tokio::try_join!(
            primary.control_get_object(object_key, token),
            secondary.control_get_object(object_key, token)
        )?;
        let (Some(primary_catalog), Some(secondary_catalog)) = (primary_catalog, secondary_catalog)
        else {
            return Ok(None);
        };
        if primary_catalog.bytes != secondary_catalog.bytes {
            return Ok(None);
        }
        let entry = match validate_catalog_entry(
            &self.logical_account,
            object_key,
            &primary_catalog.bytes,
            self.ring.ring_version,
            [primary.id(), secondary.id()],
            self.signer.as_ref(),
        ) {
            Ok(value) => value,
            Err(_) => return Ok(None),
        };
        let path_hash = entry.logical_blob.path_hash();
        let head_key = format!("heads/{path_hash}.json");
        let high_water_key = format!("high-water/{path_hash}/current.json");
        let quarantine_key = format!("quarantine/{path_hash}.json");
        let committed_key = match commit_manifest_object_prefix(&entry.signed_head.payload) {
            Ok(prefix) => format!("{prefix}/committed.json"),
            Err(_) => return Ok(None),
        };
        let (
            primary_head,
            secondary_head,
            primary_high,
            secondary_high,
            primary_quarantine,
            secondary_quarantine,
            primary_committed,
            secondary_committed,
        ) = tokio::try_join!(
            primary.control_get_object(&head_key, token),
            secondary.control_get_object(&head_key, token),
            primary.control_get_object(&high_water_key, token),
            secondary.control_get_object(&high_water_key, token),
            primary.control_get_object(&quarantine_key, token),
            secondary.control_get_object(&quarantine_key, token),
            primary.control_get_object(&committed_key, token),
            secondary.control_get_object(&committed_key, token)
        )?;
        let expected = primary_catalog.bytes.as_slice();
        if primary_quarantine.is_some()
            || secondary_quarantine.is_some()
            || [primary_head, secondary_head, primary_high, secondary_high]
                .iter()
                .any(|value| value.as_ref().map(|value| value.bytes.as_slice()) != Some(expected))
            || [primary_committed, secondary_committed]
                .iter()
                .any(|value| value.as_ref().map(|value| value.bytes.as_slice()) != Some(expected))
        {
            return Ok(None);
        }
        let compaction = CommitCoordinator::strict_compaction_checkpoint(
            primary.as_ref(),
            secondary.as_ref(),
            &path_hash,
            entry.logical_blob.canonical(),
            self.ring.ring_version,
            token,
            self.signer.as_ref(),
        )
        .await;
        let Ok(compaction) = compaction else {
            return Ok(None);
        };
        if compaction.as_ref().is_some_and(|checkpoint| {
            entry.signed_head.payload.logical_version
                <= checkpoint.signed.payload.compacted_through_logical_version
                || entry.signed_head.payload.logical_version
                    < checkpoint
                        .signed
                        .payload
                        .garbage_collection_history_head_logical_version
        }) {
            return Ok(None);
        }
        let head = entry.signed_head.payload;
        if head.state != ManifestState::Committed {
            return Ok(None);
        }
        Ok(Some((
            entry.logical_blob.blob().to_owned(),
            BlobMetadata {
                logical_etag: head.logical_etag,
                logical_version: head.logical_version,
                write_id: head.write_id,
                ring_version: head.ring_version,
                content_length: head.content_length,
                content_sha256: head.content_sha256,
                committed_at_unix_ms: head.committed_at_unix_ms,
            },
        )))
    }

    fn binding(
        &self,
        scope: ContinuationScope,
        container: Option<String>,
        request: &ListRequest,
    ) -> ContinuationBinding {
        ContinuationBinding {
            account: self.logical_account.clone(),
            container,
            scope,
            prefix: request.prefix.clone(),
            delimiter: request.delimiter.clone(),
            include: request.include.clone(),
            max_results: request.max_results,
            ring_version: self.ring.ring_version,
            ring_hash: self.ring.ring_hash.clone(),
        }
    }

    fn initial_backend_cursors(&self) -> BTreeMap<String, Option<String>> {
        self.backends
            .keys()
            .cloned()
            .map(|backend| (backend, None))
            .collect()
    }

    fn validate_backend_cursors(
        &self,
        cursors: &BTreeMap<String, Option<String>>,
    ) -> Result<(), ListingError> {
        if cursors.keys().eq(self.backends.keys()) {
            Ok(())
        } else {
            Err(ListingError::InvalidMarker(ContinuationError::Binding))
        }
    }
}

impl BlobListPage {
    pub fn to_xml(&self, service_endpoint: &str) -> String {
        let mut xml = format!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?><EnumerationResults ServiceEndpoint=\"{}\" ContainerName=\"{}\"><Prefix>{}</Prefix><Marker>{}</Marker><MaxResults>{}</MaxResults><Delimiter>{}</Delimiter><Blobs>",
            escape_xml(service_endpoint),
            escape_xml(&self.container),
            escape_xml(&self.prefix),
            escape_xml(&self.marker),
            self.max_results,
            escape_xml(&self.delimiter)
        );
        for entry in &self.entries {
            match entry {
                BlobListEntry::Prefix(prefix) => {
                    xml.push_str("<BlobPrefix><Name>");
                    xml.push_str(&escape_xml(prefix));
                    xml.push_str("</Name></BlobPrefix>");
                }
                BlobListEntry::Blob(blob) => {
                    let modified =
                        UNIX_EPOCH + Duration::from_millis(blob.metadata.committed_at_unix_ms);
                    xml.push_str("<Blob><Name>");
                    xml.push_str(&escape_xml(&blob.name));
                    xml.push_str("</Name><Properties><Creation-Time>");
                    xml.push_str(&httpdate::fmt_http_date(modified));
                    xml.push_str("</Creation-Time><Last-Modified>");
                    xml.push_str(&httpdate::fmt_http_date(modified));
                    xml.push_str("</Last-Modified><Etag>");
                    xml.push_str(&escape_xml(&blob.metadata.logical_etag));
                    xml.push_str("</Etag><Content-Length>");
                    xml.push_str(&blob.metadata.content_length.to_string());
                    xml.push_str("</Content-Length><Content-Type>application/octet-stream</Content-Type><BlobType>BlockBlob</BlobType><ServerEncrypted>true</ServerEncrypted></Properties>");
                    if self.include_metadata {
                        xml.push_str("<Metadata><overmesh_sha256>");
                        xml.push_str(&escape_xml(&blob.metadata.content_sha256));
                        xml.push_str("</overmesh_sha256></Metadata>");
                    }
                    xml.push_str("</Blob>");
                }
            }
        }
        xml.push_str("</Blobs><NextMarker>");
        if let Some(marker) = &self.next_marker {
            xml.push_str(&escape_xml(marker));
        }
        xml.push_str("</NextMarker></EnumerationResults>");
        xml
    }
}

impl ContainerListPage {
    pub fn to_xml(&self, service_endpoint: &str) -> String {
        let mut xml = format!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?><EnumerationResults ServiceEndpoint=\"{}\"><Prefix>{}</Prefix><Marker>{}</Marker><MaxResults>{}</MaxResults><Containers>",
            escape_xml(service_endpoint),
            escape_xml(&self.prefix),
            escape_xml(&self.marker),
            self.max_results
        );
        for container in &self.containers {
            let modified = UNIX_EPOCH + Duration::from_millis(container.last_modified_unix_ms);
            xml.push_str("<Container><Name>");
            xml.push_str(&escape_xml(&container.name));
            xml.push_str("</Name><Properties><Last-Modified>");
            xml.push_str(&httpdate::fmt_http_date(modified));
            xml.push_str("</Last-Modified><Etag>");
            xml.push_str(&escape_xml(&container.etag));
            xml.push_str("</Etag><LeaseStatus>unlocked</LeaseStatus><LeaseState>available</LeaseState></Properties></Container>");
        }
        xml.push_str("</Containers><NextMarker>");
        if let Some(marker) = &self.next_marker {
            xml.push_str(&escape_xml(marker));
        }
        xml.push_str("</NextMarker></EnumerationResults>");
        xml
    }
}

fn container_ordering_key(value: &str) -> String {
    format!("containers/v1/{}", hex::encode(value.as_bytes()))
}

fn is_internal_blob_name(value: &str) -> bool {
    value == ".overmesh" || value.starts_with(".overmesh/")
}

fn container_name_from_ordering_key(value: &str) -> Result<String, ListingError> {
    let encoded = value
        .strip_prefix("containers/v1/")
        .ok_or(ListingError::InvalidMarker(ContinuationError::Ordering))?;
    let bytes = hex::decode(encoded)
        .map_err(|_| ListingError::InvalidMarker(ContinuationError::Ordering))?;
    String::from_utf8(bytes).map_err(|_| ListingError::InvalidMarker(ContinuationError::Ordering))
}

fn map_authorization_error(error: BackendError) -> ListingError {
    match error {
        BackendError::Http { status, .. }
            if status == http::StatusCode::UNAUTHORIZED
                || status == http::StatusCode::FORBIDDEN =>
        {
            ListingError::Authorization
        }
        other => ListingError::Backend(other),
    }
}

fn container_etag(account: &str, container: &str, ring_version: u64) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(format!("{account}\0{container}\0{ring_version}").as_bytes());
    format!("\"om-container-{}\"", hex::encode(&digest[..8]))
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
