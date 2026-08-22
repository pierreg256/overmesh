use std::sync::Arc;
use std::time::SystemTime;

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::State,
    http::{
        HeaderValue, Method, Request, StatusCode,
        header::{
            ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_MATCH,
            IF_NONE_MATCH, LAST_MODIFIED, RANGE,
        },
    },
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Serialize;
use tracing::info;
use uuid::Uuid;

use crate::{
    auth::{AuthenticatedPrincipal, Authenticator},
    backend::BackendError,
    block::{BlockError, BlockListType, MAX_BLOCK_SIZE, PutBlockResult, parse_block_list_xml},
    commit::{CommitError, CommitService, LogicalCondition},
    error::StorageError,
    listing::{ListRequest, ListingError},
    read::{BlobMetadata, ReadError, ReadService},
    request_context::{client_request_fingerprint, current_client_request_fingerprint, scope},
    resource::LogicalBlobId,
    ring::SignedRing,
    upload::{DEFAULT_BLOCK_SIZE, SpoolBodyError, spool_body, spool_body_limited},
};

pub const SUPPORTED_STORAGE_VERSION: &str = "2025-11-05";
const MINIMUM_OAUTH_STORAGE_VERSION: &str = "2017-11-09";
const MAX_BLOCK_LIST_XML_SIZE: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct AppState {
    pub authenticator: Authenticator,
    pub logical_account: String,
    pub ring: Arc<SignedRing>,
    pub commit_service: Option<Arc<CommitService>>,
    pub read_service: Option<Arc<ReadService>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
    ring_version: u64,
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .fallback(blob_request)
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
        ring_version: state.ring.ring_version,
    })
}

async fn blob_request(State(state): State<AppState>, request: Request<Body>) -> Response {
    let fingerprint = request
        .headers()
        .get("x-ms-client-request-id")
        .and_then(|value| value.to_str().ok())
        .map(client_request_fingerprint)
        .unwrap_or_else(|| "missing".to_owned());
    scope(fingerprint, blob_request_scoped(state, request)).await
}

async fn blob_request_scoped(state: AppState, request: Request<Body>) -> Response {
    let principal = match state
        .authenticator
        .authenticate(request.headers(), request.uri())
    {
        Ok(principal) => principal,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = validate_storage_version(request.headers()) {
        return error.into_response();
    }
    if let Err(error) = validate_logical_account(request.headers(), &state.logical_account) {
        return error.into_response();
    }
    let query = match parse_query(request.uri().query()) {
        Ok(query) => query,
        Err(error) => return error.into_response(),
    };
    if request.method() == Method::GET
        && request.uri().path() == "/"
        && query.single("comp") == Some("list")
    {
        return list_containers(state, request, principal, &query).await;
    }
    if request.method() == Method::GET
        && query.single("restype") == Some("container")
        && query.single("comp") == Some("list")
    {
        return list_blobs(state, request, principal, &query).await;
    }
    if request.method() == Method::PUT && query.single("comp") == Some("block") {
        return put_block(state, request, principal, &query).await;
    }
    if request.method() == Method::PUT && query.single("comp") == Some("blocklist") {
        return put_block_list(state, request, principal).await;
    }
    if request.method() == Method::GET && query.single("comp") == Some("blocklist") {
        return get_block_list(state, request, principal, &query).await;
    }
    if request.method() == Method::PUT && request.uri().query().is_none() {
        return put_blob(state, request, principal).await;
    }

    async fn list_containers(
        state: AppState,
        _request: Request<Body>,
        principal: AuthenticatedPrincipal,
        query: &QueryParameters,
    ) -> Response {
        let Some(commit_service) = state.commit_service else {
            return StorageError::feature_not_supported().into_response();
        };
        let list_request = match listing_request(query) {
            Ok(request) => request,
            Err(error) => return error.into_response(),
        };
        let service = commit_service.listing_service(state.logical_account.clone());
        match service.list_containers(&list_request, &principal).await {
            Ok(page) => {
                emit_listing_scan(
                    "containers",
                    page.containers.len(),
                    page.entries_considered,
                    page.entries_validated,
                    page.validation_concurrency,
                );
                listing_response(page.to_xml(&service_endpoint(&state.logical_account)))
            }
            Err(error) => listing_error_response(error),
        }
    }

    async fn list_blobs(
        state: AppState,
        request: Request<Body>,
        principal: AuthenticatedPrincipal,
        query: &QueryParameters,
    ) -> Response {
        let Some(commit_service) = state.commit_service else {
            return StorageError::feature_not_supported().into_response();
        };
        let container = match LogicalBlobId::parse_container_path(request.uri().path()) {
            Ok(container) => container,
            Err(error) => return StorageError::invalid_request(error.to_string()).into_response(),
        };
        let list_request = match listing_request(query) {
            Ok(request) => request,
            Err(error) => return error.into_response(),
        };
        let service = commit_service.listing_service(state.logical_account.clone());
        match service
            .list_blobs(&container, &list_request, &principal)
            .await
        {
            Ok(page) => {
                emit_listing_scan(
                    "blobs",
                    page.entries.len(),
                    page.entries_considered,
                    page.entries_validated,
                    page.validation_concurrency,
                );
                listing_response(page.to_xml(&service_endpoint(&state.logical_account)))
            }
            Err(error) => listing_error_response(error),
        }
    }

    async fn put_block(
        state: AppState,
        request: Request<Body>,
        principal: AuthenticatedPrincipal,
        query: &QueryParameters,
    ) -> Response {
        let Some(commit_service) = state.commit_service else {
            return StorageError::feature_not_supported().into_response();
        };
        let logical_blob = match LogicalBlobId::parse(&state.logical_account, request.uri().path())
        {
            Ok(blob) => blob,
            Err(error) => return StorageError::invalid_request(error.to_string()).into_response(),
        };
        let block_id = match query.single("blockid") {
            Some(block_id) => block_id.to_owned(),
            None => return StorageError::invalid_query_parameter("blockid").into_response(),
        };
        let write_id = match request_write_id(request.headers()) {
            Ok(write_id) => write_id,
            Err(error) => return error.into_response(),
        };
        let upload_id = match request_upload_id(request.headers()) {
            Ok(upload_id) => upload_id,
            Err(error) => return error.into_response(),
        };
        let content =
            match spool_body_limited(request.into_body(), DEFAULT_BLOCK_SIZE, MAX_BLOCK_SIZE).await
            {
                Ok(content) => content,
                Err(SpoolBodyError::TooLarge) => {
                    return StorageError::request_body_too_large().into_response();
                }
                Err(error) => {
                    return StorageError::invalid_request(format!(
                        "The request body could not be read: {error}"
                    ))
                    .into_response();
                }
            };
        let service = commit_service.block_service();
        match service
            .put_block(
                &logical_blob,
                &principal,
                upload_id.as_deref().unwrap_or_default(),
                &write_id,
                &block_id,
                &content,
            )
            .await
        {
            Ok(result) => put_block_success_response(result),
            Err(error) => block_error_response(error),
        }
    }

    async fn put_block_list(
        state: AppState,
        request: Request<Body>,
        principal: AuthenticatedPrincipal,
    ) -> Response {
        let Some(commit_service) = state.commit_service else {
            return StorageError::feature_not_supported().into_response();
        };
        let logical_blob = match LogicalBlobId::parse(&state.logical_account, request.uri().path())
        {
            Ok(blob) => blob,
            Err(error) => return StorageError::invalid_request(error.to_string()).into_response(),
        };
        let write_id = match request_write_id(request.headers()) {
            Ok(write_id) => write_id,
            Err(error) => return error.into_response(),
        };
        let upload_id = match request_upload_id(request.headers()) {
            Ok(upload_id) => upload_id,
            Err(error) => return error.into_response(),
        };
        let condition = match logical_condition(request.headers()) {
            Ok(condition) => condition,
            Err(error) => return error.into_response(),
        };
        let body = match to_bytes(request.into_body(), MAX_BLOCK_LIST_XML_SIZE).await {
            Ok(body) => body,
            Err(error) => {
                return StorageError::invalid_request(format!(
                    "The block list body could not be read: {error}"
                ))
                .into_response();
            }
        };
        let selections = match parse_block_list_xml(&body) {
            Ok(selections) => selections,
            Err(error) => return block_error_response(error),
        };
        let service = commit_service.block_service();
        match service
            .put_block_list(
                &logical_blob,
                &principal,
                upload_id.as_deref().unwrap_or_default(),
                &write_id,
                &selections,
                condition,
            )
            .await
        {
            Ok(result) => put_success_response(result),
            Err(error) => block_error_response(error),
        }
    }

    async fn get_block_list(
        state: AppState,
        request: Request<Body>,
        principal: AuthenticatedPrincipal,
        query: &QueryParameters,
    ) -> Response {
        let Some(commit_service) = state.commit_service else {
            return StorageError::feature_not_supported().into_response();
        };
        let logical_blob = match LogicalBlobId::parse(&state.logical_account, request.uri().path())
        {
            Ok(blob) => blob,
            Err(error) => return StorageError::invalid_request(error.to_string()).into_response(),
        };
        let list_type = match BlockListType::parse(query.single("blocklisttype")) {
            Ok(list_type) => list_type,
            Err(error) => return block_error_response(error),
        };
        let upload_id = match request_upload_id(request.headers()) {
            Ok(upload_id) => upload_id,
            Err(error) => return error.into_response(),
        };
        let service = commit_service.block_service();
        match service
            .get_block_list(&logical_blob, &principal, upload_id.as_deref(), list_type)
            .await
        {
            Ok(result) => listing_response(result.to_xml()),
            Err(error) => block_error_response(error),
        }
    }
    if request.method() == Method::HEAD && request.uri().query().is_none() {
        return head_blob(state, request, principal).await;
    }
    if request.method() == Method::GET && request.uri().query().is_none() {
        return get_blob(state, request, principal).await;
    }
    if request.method() == Method::DELETE && request.uri().query().is_none() {
        return delete_blob(state, request, principal).await;
    }
    if !matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::DELETE
    ) {
        return StorageError::unsupported_method().into_response();
    }

    StorageError::feature_not_supported().into_response()
}

async fn head_blob(
    state: AppState,
    request: Request<Body>,
    principal: AuthenticatedPrincipal,
) -> Response {
    let Some(read_service) = state.read_service else {
        return StorageError::feature_not_supported().into_response();
    };
    let logical_blob = match LogicalBlobId::parse(&state.logical_account, request.uri().path()) {
        Ok(blob) => blob,
        Err(error) => return StorageError::invalid_request(error.to_string()).into_response(),
    };
    match read_service.head_blob(&logical_blob, &principal).await {
        Ok(metadata) => match evaluate_read_conditions(request.headers(), &metadata.logical_etag) {
            Ok(ReadCondition::Proceed) => read_response(metadata, None, Body::empty(), true),
            Ok(ReadCondition::NotModified) => not_modified_response(&metadata),
            Err(error) => error.into_response(),
        },
        Err(error) => read_error_response(error),
    }
}

async fn get_blob(
    state: AppState,
    request: Request<Body>,
    principal: AuthenticatedPrincipal,
) -> Response {
    let Some(read_service) = state.read_service else {
        return StorageError::feature_not_supported().into_response();
    };
    let logical_blob = match LogicalBlobId::parse(&state.logical_account, request.uri().path()) {
        Ok(blob) => blob,
        Err(error) => return StorageError::invalid_request(error.to_string()).into_response(),
    };
    let range = match request
        .headers()
        .get("x-ms-range")
        .or_else(|| request.headers().get(RANGE))
        .map(|value| value.to_str())
        .transpose()
    {
        Ok(value) => value.map(ToOwned::to_owned),
        Err(_) => return StorageError::invalid_header("Range").into_response(),
    };
    match read_service
        .get_blob(&logical_blob, &principal, range.as_deref())
        .await
    {
        Ok(read) => {
            match evaluate_read_conditions(request.headers(), &read.metadata.logical_etag) {
                Ok(ReadCondition::Proceed) => {
                    read_response(read.metadata, read.range, read.body, false)
                }
                Ok(ReadCondition::NotModified) => not_modified_response(&read.metadata),
                Err(error) => error.into_response(),
            }
        }
        Err(error) => read_error_response(error),
    }
}

async fn put_blob(
    state: AppState,
    request: Request<Body>,
    principal: AuthenticatedPrincipal,
) -> Response {
    let Some(commit_service) = state.commit_service else {
        return StorageError::feature_not_supported().into_response();
    };
    let logical_blob = match LogicalBlobId::parse(&state.logical_account, request.uri().path()) {
        Ok(blob) => blob,
        Err(error) => return StorageError::invalid_request(error.to_string()).into_response(),
    };
    let write_id = match request_write_id(request.headers()) {
        Ok(write_id) => write_id,
        Err(error) => return error.into_response(),
    };
    let logical_condition = match logical_condition(request.headers()) {
        Ok(condition) => condition,
        Err(error) => return error.into_response(),
    };
    let content = match spool_body(request.into_body(), DEFAULT_BLOCK_SIZE).await {
        Ok(content) => content,
        Err(error) => {
            return StorageError::invalid_request(format!(
                "The request body could not be read: {error}"
            ))
            .into_response();
        }
    };
    match commit_service
        .put_blob(
            &logical_blob,
            &principal,
            &write_id,
            &content,
            logical_condition,
        )
        .await
    {
        Ok(result) => put_success_response(result),
        Err(error) => commit_error_response(error),
    }
}

async fn delete_blob(
    state: AppState,
    request: Request<Body>,
    principal: AuthenticatedPrincipal,
) -> Response {
    let Some(commit_service) = state.commit_service else {
        return StorageError::feature_not_supported().into_response();
    };
    let logical_blob = match LogicalBlobId::parse(&state.logical_account, request.uri().path()) {
        Ok(blob) => blob,
        Err(error) => return StorageError::invalid_request(error.to_string()).into_response(),
    };
    let write_id = match request_write_id(request.headers()) {
        Ok(write_id) => write_id,
        Err(error) => return error.into_response(),
    };
    let logical_condition = match delete_condition(request.headers()) {
        Ok(condition) => condition,
        Err(error) => return error.into_response(),
    };
    match commit_service
        .delete_blob(&logical_blob, &principal, &write_id, logical_condition)
        .await
    {
        Ok(result) => delete_success_response(result),
        Err(error) => commit_error_response(error),
    }
}

fn request_write_id(headers: &http::HeaderMap) -> Result<String, StorageError> {
    let (header_name, value) = if let Some(value) = headers.get("x-overmesh-write-id") {
        ("x-overmesh-write-id", value)
    } else if let Some(value) = headers.get("x-ms-client-request-id") {
        ("x-ms-client-request-id", value)
    } else {
        return Err(StorageError::stable_request_id_required());
    };
    let write_id = value
        .to_str()
        .map_err(|_| StorageError::invalid_header(header_name))?;
    if write_id.is_empty()
        || write_id.len() > 128
        || !write_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~'))
    {
        return Err(StorageError::invalid_header(header_name));
    }
    Ok(write_id.to_owned())
}

fn request_upload_id(headers: &http::HeaderMap) -> Result<Option<String>, StorageError> {
    let Some(value) = headers.get("x-overmesh-upload-id") else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| StorageError::invalid_header("x-overmesh-upload-id"))?;
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~'))
    {
        return Err(StorageError::invalid_header("x-overmesh-upload-id"));
    }
    Ok(Some(value.to_owned()))
}

fn logical_condition(headers: &http::HeaderMap) -> Result<LogicalCondition, StorageError> {
    if let Some(value) = headers.get("if-none-match") {
        if value == "*" {
            return Ok(LogicalCondition::IfAbsent);
        }
        return Err(StorageError::invalid_header("If-None-Match"));
    }
    if let Some(value) = headers.get("if-match") {
        let value = value
            .to_str()
            .map_err(|_| StorageError::invalid_header("If-Match"))?;
        if value.is_empty() || value == "*" {
            return Err(StorageError::invalid_header("If-Match"));
        }
        return Ok(LogicalCondition::IfMatch(value.to_owned()));
    }
    Ok(LogicalCondition::None)
}

fn delete_condition(headers: &http::HeaderMap) -> Result<LogicalCondition, StorageError> {
    if headers.contains_key(IF_NONE_MATCH) {
        return Err(StorageError::invalid_header("If-None-Match"));
    }
    if let Some(value) = headers.get(IF_MATCH) {
        let value = value
            .to_str()
            .map_err(|_| StorageError::invalid_header("If-Match"))?;
        if value.is_empty() {
            return Err(StorageError::invalid_header("If-Match"));
        }
        return Ok(LogicalCondition::IfMatch(value.to_owned()));
    }
    Ok(LogicalCondition::None)
}

fn put_success_response(result: crate::commit::CommitResult) -> Response {
    let request_id = Uuid::new_v4().to_string();
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::CREATED;
    let headers = response.headers_mut();
    headers.insert(
        ETAG,
        HeaderValue::from_str(&result.logical_etag).expect("logical ETag header"),
    );
    headers.insert(
        "x-ms-request-id",
        HeaderValue::from_str(&request_id).expect("request id header"),
    );
    headers.insert(
        "x-ms-version",
        HeaderValue::from_static(SUPPORTED_STORAGE_VERSION),
    );
    headers.insert(
        "x-overmesh-write-id",
        HeaderValue::from_str(&result.write_id).expect("write id header"),
    );
    headers.insert(
        "x-overmesh-logical-version",
        HeaderValue::from_str(&result.logical_version.to_string()).expect("version header"),
    );
    headers.insert(
        "x-overmesh-idempotent-replay",
        HeaderValue::from_static(if result.idempotent_replay {
            "true"
        } else {
            "false"
        }),
    );
    headers.insert(
        http::header::DATE,
        HeaderValue::from_str(&httpdate::fmt_http_date(SystemTime::now())).expect("date header"),
    );
    response
}

fn put_block_success_response(result: PutBlockResult) -> Response {
    let request_id = Uuid::new_v4().to_string();
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::CREATED;
    let headers = response.headers_mut();
    headers.insert(
        "x-ms-request-id",
        HeaderValue::from_str(&request_id).expect("request id header"),
    );
    headers.insert(
        "x-ms-version",
        HeaderValue::from_static(SUPPORTED_STORAGE_VERSION),
    );
    headers.insert(
        "x-overmesh-write-id",
        HeaderValue::from_str(&result.write_id).expect("write id header"),
    );
    headers.insert(
        "x-overmesh-idempotent-replay",
        HeaderValue::from_static(if result.idempotent_replay {
            "true"
        } else {
            "false"
        }),
    );
    headers.insert(
        http::header::DATE,
        HeaderValue::from_str(&httpdate::fmt_http_date(SystemTime::now())).expect("date header"),
    );
    response
}

fn delete_success_response(result: crate::commit::DeleteResult) -> Response {
    let request_id = Uuid::new_v4().to_string();
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::ACCEPTED;
    let headers = response.headers_mut();
    headers.insert(
        ETAG,
        HeaderValue::from_str(&result.logical_etag).expect("logical ETag header"),
    );
    headers.insert(
        "x-ms-request-id",
        HeaderValue::from_str(&request_id).expect("request id header"),
    );
    headers.insert(
        "x-ms-version",
        HeaderValue::from_static(SUPPORTED_STORAGE_VERSION),
    );
    headers.insert(
        "x-ms-delete-type-permanent",
        HeaderValue::from_static("false"),
    );
    headers.insert(
        "x-overmesh-write-id",
        HeaderValue::from_str(&result.write_id).expect("write id header"),
    );
    headers.insert(
        "x-overmesh-logical-version",
        HeaderValue::from_str(&result.logical_version.to_string()).expect("version header"),
    );
    headers.insert(
        "x-overmesh-deleted-at-unix-ms",
        HeaderValue::from_str(&result.deleted_at_unix_ms.to_string())
            .expect("deletion timestamp header"),
    );
    headers.insert(
        "x-overmesh-idempotent-replay",
        HeaderValue::from_static(if result.idempotent_replay {
            "true"
        } else {
            "false"
        }),
    );
    headers.insert(
        http::header::DATE,
        HeaderValue::from_str(&httpdate::fmt_http_date(SystemTime::now())).expect("date header"),
    );
    response
}

fn commit_error_response(error: CommitError) -> Response {
    match error {
        CommitError::ConditionFailed => StorageError::condition_not_met(),
        CommitError::IdempotencyConflict => StorageError::invalid_operation(
            "The write ID is already associated with different content.",
        ),
        CommitError::LockConflict => StorageError::lease_conflict(),
        CommitError::Quarantined => StorageError::blob_quarantined(),
        CommitError::NotFound => StorageError::blob_not_found(),
        CommitError::Ambiguous => StorageError::server_busy(
            "The write outcome is ambiguous. Retry with the same Overmesh write ID.",
        ),
        CommitError::ReplicaDrift | CommitError::VerificationFailed => {
            StorageError::server_busy("Replica consistency validation failed.")
        }
        CommitError::Backend(BackendError::Http { status, .. })
            if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN =>
        {
            StorageError::authorization_permission_mismatch()
        }
        CommitError::Backend(_)
        | CommitError::Manifest(_)
        | CommitError::Catalog(_)
        | CommitError::Serialization(_) => {
            StorageError::server_busy("The dual-write commit could not be completed.")
        }
    }
    .into_response()
}

fn block_error_response(error: BlockError) -> Response {
    match error {
        BlockError::InvalidBlockId => StorageError::invalid_block_id().into_response(),
        BlockError::InvalidBlockList(_) | BlockError::UnequalBlockIdLength => {
            StorageError::invalid_block_list().into_response()
        }
        BlockError::BlockCountExceedsLimit => {
            StorageError::block_count_exceeds_limit().into_response()
        }
        BlockError::BlockTooLarge => StorageError::request_body_too_large().into_response(),
        BlockError::Conflict => StorageError::invalid_operation(
            "The staged block ID is already associated with different content or write identity.",
        )
        .into_response(),
        BlockError::MissingBlock => StorageError::new(
            StatusCode::BAD_REQUEST,
            "InvalidBlockList",
            "The specified block list references a block that does not exist.",
        )
        .into_response(),
        BlockError::NotFound => StorageError::blob_not_found().into_response(),
        BlockError::Expired => StorageError::invalid_block_list().into_response(),
        BlockError::Commit(error) => commit_error_response(error),
        BlockError::Backend(BackendError::Http { status, .. })
            if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN =>
        {
            StorageError::authorization_permission_mismatch().into_response()
        }
        BlockError::VerificationFailed
        | BlockError::Backend(_)
        | BlockError::Manifest(_)
        | BlockError::Spool(_) => {
            StorageError::server_busy("Staged block validation failed closed.").into_response()
        }
    }
}

fn listing_error_response(error: ListingError) -> Response {
    match error {
        ListingError::InvalidRequest(message) => StorageError::invalid_request(message),
        ListingError::InvalidMarker(error) => StorageError::invalid_marker(error.to_string()),
        ListingError::ContainerNotFound => StorageError::container_not_found(),
        ListingError::Authorization => StorageError::authorization_permission_mismatch(),
        ListingError::Backend(BackendError::Http { status, .. })
            if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN =>
        {
            StorageError::authorization_permission_mismatch()
        }
        ListingError::Backend(_) => StorageError::server_busy("Logical listing failed closed."),
    }
    .into_response()
}

fn listing_response(xml: String) -> Response {
    let request_id = Uuid::new_v4().to_string();
    let mut response = Response::new(Body::from(xml));
    *response.status_mut() = StatusCode::OK;
    let headers = response.headers_mut();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/xml; charset=utf-8"),
    );
    headers.insert(
        "x-ms-request-id",
        HeaderValue::from_str(&request_id).expect("request id header"),
    );
    headers.insert(
        "x-ms-version",
        HeaderValue::from_static(SUPPORTED_STORAGE_VERSION),
    );
    headers.insert(
        http::header::DATE,
        HeaderValue::from_str(&httpdate::fmt_http_date(SystemTime::now())).expect("date header"),
    );
    response
}

fn emit_listing_scan(
    scope: &str,
    entries_returned: usize,
    entries_considered: u64,
    entries_validated: u64,
    validation_concurrency: usize,
) {
    let client_request_fingerprint = current_client_request_fingerprint();
    info!(
        event = "overmesh_listing_scan",
        client_request_fingerprint = %client_request_fingerprint,
        scope,
        entries_returned,
        entries_considered,
        entries_validated,
        validation_concurrency,
    );
}

fn listing_request(query: &QueryParameters) -> Result<ListRequest, StorageError> {
    let max_results = query
        .single("maxresults")
        .map(|value| {
            value
                .parse::<u32>()
                .map_err(|_| StorageError::invalid_query_parameter("maxresults"))
        })
        .transpose()?;
    let include = query
        .values("include")
        .flat_map(|value| value.split(','))
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect();
    ListRequest::new(
        query.single("prefix").unwrap_or_default().to_owned(),
        query.single("delimiter").unwrap_or_default().to_owned(),
        query.single("marker").map(str::to_owned),
        max_results,
        include,
    )
    .map_err(|error| StorageError::invalid_request(error.to_string()))
}

#[derive(Default)]
struct QueryParameters {
    values: std::collections::BTreeMap<String, Vec<String>>,
}

impl QueryParameters {
    fn single(&self, name: &str) -> Option<&str> {
        self.values
            .get(name)
            .and_then(|values| (values.len() == 1).then(|| values[0].as_str()))
    }

    fn values(&self, name: &str) -> impl Iterator<Item = &str> {
        self.values
            .get(name)
            .into_iter()
            .flatten()
            .map(String::as_str)
    }
}

fn parse_query(query: Option<&str>) -> Result<QueryParameters, StorageError> {
    let mut parameters = QueryParameters::default();
    for (name, value) in url::form_urlencoded::parse(query.unwrap_or_default().as_bytes()) {
        parameters
            .values
            .entry(name.into_owned().to_ascii_lowercase())
            .or_default()
            .push(value.into_owned());
    }
    for (name, values) in &parameters.values {
        if name != "include" && values.len() > 1 {
            return Err(StorageError::invalid_query_parameter(name));
        }
    }
    Ok(parameters)
}

fn service_endpoint(account: &str) -> String {
    format!("https://{account}.blob.core.windows.net/")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadCondition {
    Proceed,
    NotModified,
}

fn evaluate_read_conditions(
    headers: &http::HeaderMap,
    logical_etag: &str,
) -> Result<ReadCondition, StorageError> {
    if let Some(value) = headers.get(IF_MATCH) {
        let value = value
            .to_str()
            .map_err(|_| StorageError::invalid_header("If-Match"))?;
        if !etag_list_matches(value, logical_etag) {
            return Err(StorageError::condition_not_met());
        }
    }
    if let Some(value) = headers.get(IF_NONE_MATCH) {
        let value = value
            .to_str()
            .map_err(|_| StorageError::invalid_header("If-None-Match"))?;
        if etag_list_matches(value, logical_etag) {
            return Ok(ReadCondition::NotModified);
        }
    }
    Ok(ReadCondition::Proceed)
}

fn etag_list_matches(value: &str, logical_etag: &str) -> bool {
    value
        .split(',')
        .map(str::trim)
        .any(|candidate| candidate == "*" || candidate == logical_etag)
}

fn read_response(
    metadata: BlobMetadata,
    range: Option<crate::read::ResolvedRange>,
    body: Body,
    is_head: bool,
) -> Response {
    let mut response = Response::new(body);
    *response.status_mut() = if range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    add_read_headers(&mut response, &metadata);
    let headers = response.headers_mut();
    headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert("x-ms-blob-type", HeaderValue::from_static("BlockBlob"));
    headers.insert("x-ms-server-encrypted", HeaderValue::from_static("true"));
    let content_length = range.map_or(metadata.content_length, |value| value.length());
    headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&content_length.to_string()).expect("content length header"),
    );
    if let Some(range) = range {
        headers.insert(
            CONTENT_RANGE,
            HeaderValue::from_str(&format!(
                "bytes {}-{}/{}",
                range.start, range.end, range.total_length
            ))
            .expect("content range header"),
        );
    }
    if is_head {
        *response.body_mut() = Body::empty();
    }
    response
}

fn not_modified_response(metadata: &BlobMetadata) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::NOT_MODIFIED;
    add_read_headers(&mut response, metadata);
    response
}

fn add_read_headers(response: &mut Response, metadata: &BlobMetadata) {
    let request_id = Uuid::new_v4().to_string();
    let committed_at =
        std::time::UNIX_EPOCH + std::time::Duration::from_millis(metadata.committed_at_unix_ms);
    let headers = response.headers_mut();
    headers.insert(
        ETAG,
        HeaderValue::from_str(&metadata.logical_etag).expect("logical ETag header"),
    );
    headers.insert(
        LAST_MODIFIED,
        HeaderValue::from_str(&httpdate::fmt_http_date(committed_at))
            .expect("last modified header"),
    );
    headers.insert(
        "x-ms-request-id",
        HeaderValue::from_str(&request_id).expect("request id header"),
    );
    headers.insert(
        "x-ms-version",
        HeaderValue::from_static(SUPPORTED_STORAGE_VERSION),
    );
    headers.insert(
        "x-overmesh-write-id",
        HeaderValue::from_str(&metadata.write_id).expect("write id header"),
    );
    headers.insert(
        "x-overmesh-logical-version",
        HeaderValue::from_str(&metadata.logical_version.to_string()).expect("version header"),
    );
    headers.insert(
        "x-overmesh-ring-version",
        HeaderValue::from_str(&metadata.ring_version.to_string()).expect("Ring version header"),
    );
    headers.insert(
        "x-overmesh-content-sha256",
        HeaderValue::from_str(&metadata.content_sha256).expect("content hash header"),
    );
    headers.insert(
        http::header::DATE,
        HeaderValue::from_str(&httpdate::fmt_http_date(SystemTime::now())).expect("date header"),
    );
}

fn read_error_response(error: ReadError) -> Response {
    match error {
        ReadError::NotFound => StorageError::blob_not_found().into_response(),
        ReadError::Quarantined => StorageError::blob_quarantined().into_response(),
        ReadError::InvalidRange { content_length } => {
            let mut response = StorageError::invalid_range().into_response();
            response.headers_mut().insert(
                CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes */{content_length}"))
                    .expect("invalid range header"),
            );
            response
        }
        ReadError::Backend(BackendError::Http { status, .. })
            if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN =>
        {
            StorageError::authorization_permission_mismatch().into_response()
        }
        ReadError::Backend(_)
        | ReadError::Manifest(_)
        | ReadError::Serialization(_)
        | ReadError::ReplicaDrift
        | ReadError::VerificationFailed => {
            StorageError::server_busy("Replica read validation failed.").into_response()
        }
    }
}

fn validate_storage_version(headers: &http::HeaderMap) -> Result<(), StorageError> {
    let version = headers
        .get("x-ms-version")
        .ok_or_else(|| StorageError::missing_header("x-ms-version"))?
        .to_str()
        .map_err(|_| StorageError::invalid_header("x-ms-version"))?;
    if !is_storage_version(version) || version < MINIMUM_OAUTH_STORAGE_VERSION {
        return Err(StorageError::invalid_header("x-ms-version"));
    }
    Ok(())
}

fn is_storage_version(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| index == 4 || index == 7 || byte.is_ascii_digit())
}

fn validate_logical_account(
    headers: &http::HeaderMap,
    logical_account: &str,
) -> Result<(), StorageError> {
    let Some(host) = headers.get(http::header::HOST) else {
        return Ok(());
    };
    let host = host
        .to_str()
        .map_err(|_| StorageError::invalid_header("Host"))?
        .split(':')
        .next()
        .unwrap_or_default();
    if let Some(account) = host.strip_suffix(".blob.core.windows.net")
        && account != logical_account
    {
        return Err(StorageError::account_not_found());
    }
    Ok(())
}

pub fn status_code(response: &Response) -> StatusCode {
    response.status()
}

#[cfg(test)]
mod tests {
    use http::{HeaderMap, HeaderValue};

    use super::*;

    #[test]
    fn write_id_prefers_overmesh_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-overmesh-write-id",
            HeaderValue::from_static("overmesh-write-1"),
        );
        headers.insert(
            "x-ms-client-request-id",
            HeaderValue::from_static("azure-request-1"),
        );
        assert_eq!(
            request_write_id(&headers).expect("write id"),
            "overmesh-write-1"
        );
    }

    #[test]
    fn write_id_accepts_azure_client_request_id_fallback() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-ms-client-request-id",
            HeaderValue::from_static("azure.request_1~retry"),
        );
        assert_eq!(
            request_write_id(&headers).expect("write id"),
            "azure.request_1~retry"
        );
    }

    #[test]
    fn write_id_is_required_and_must_be_path_safe() {
        let missing = request_write_id(&HeaderMap::new()).expect_err("missing id");
        assert_eq!(missing.status, StatusCode::BAD_REQUEST);
        assert_eq!(missing.code, "MissingRequiredHeader");
        assert!(missing.message.contains("stable request ID"));

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-overmesh-write-id",
            HeaderValue::from_static("contains a space"),
        );
        let invalid = request_write_id(&headers).expect_err("invalid id");
        assert_eq!(invalid.code, "InvalidHeaderValue");
    }

    #[test]
    fn commit_authorization_failures_are_not_retryable_server_errors() {
        for status in [StatusCode::UNAUTHORIZED, StatusCode::FORBIDDEN] {
            let response = commit_error_response(CommitError::Backend(BackendError::Http {
                status,
                message: "denied".to_owned(),
            }));
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        }
    }

    #[test]
    fn oversized_blocks_use_the_azure_request_body_too_large_response() {
        let response = block_error_response(BlockError::BlockTooLarge);
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(response.headers()["x-ms-error-code"], "RequestBodyTooLarge");
    }

    #[test]
    fn azure_host_account_mismatch_is_not_found_but_local_hosts_are_allowed() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::HOST,
            HeaderValue::from_static("other.blob.core.windows.net"),
        );
        let error = validate_logical_account(&headers, "expected").expect_err("mismatch");
        assert_eq!(error.code, "AccountNotFound");
        headers.insert(
            http::header::HOST,
            HeaderValue::from_static("127.0.0.1:18080"),
        );
        validate_logical_account(&headers, "expected").expect("local endpoint");
    }
}
