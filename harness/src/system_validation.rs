use std::time::SystemTime;

use anyhow::{Context, Result, bail, ensure};
use http::Method;
use overmesh_gateway::manifest::{ManifestState, sha256_bytes};
use overmesh_gateway::resource::stable_component;
use reqwest::{Client, Response, StatusCode, header};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    identity::{TestPrincipal, TestTokenKind, issue_test_token},
    manifest_validation::{
        verify_block_manifest_page, verify_local_block_manifest, verify_local_commit_manifest_bytes,
    },
};

const STORAGE_VERSION: &str = "2025-11-05";

pub struct SystemValidationConfig {
    pub gateway_url: String,
    pub backend_a_url: String,
    pub backend_b_url: String,
    pub logical_account: String,
}

pub async fn validate_system(config: &SystemValidationConfig) -> Result<()> {
    let client = Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .context("failed to build validation HTTP client")?;
    let caller_token = issue_test_token(TestTokenKind::Valid, TestPrincipal::Caller)?;
    let control_token = issue_test_token(TestTokenKind::Valid, TestPrincipal::Gateway)?;
    let run_id = Uuid::new_v4().simple().to_string();
    let blob_path = format!("/commit/harness-system-{run_id}");
    let payload = b"validated by the real Overmesh harness";
    let write_id = format!("harness-system-{run_id}");

    let missing_write_id = gateway_request(
        &client,
        &config.gateway_url,
        Method::PUT,
        &format!("/commit/missing-write-id-{run_id}"),
        &caller_token,
    )
    .body(payload.to_vec())
    .send()
    .await?;
    ensure_status(
        missing_write_id,
        StatusCode::BAD_REQUEST,
        "gateway PUT without stable request ID",
    )
    .await?;

    let put = gateway_request(
        &client,
        &config.gateway_url,
        Method::PUT,
        &blob_path,
        &caller_token,
    )
    .header("x-ms-client-request-id", &write_id)
    .body(payload.to_vec())
    .send()
    .await?;
    ensure_status(put, StatusCode::CREATED, "gateway PUT").await?;

    let head = gateway_request(
        &client,
        &config.gateway_url,
        Method::HEAD,
        &blob_path,
        &caller_token,
    )
    .send()
    .await?;
    ensure_status_code(head.status(), StatusCode::OK, "gateway HEAD")?;
    ensure!(
        head.headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            == Some(payload.len().to_string().as_str()),
        "gateway HEAD returned an unexpected content length"
    );
    let logical_etag = head
        .headers()
        .get(header::ETAG)
        .context("gateway HEAD omitted ETag")?
        .to_str()?
        .to_owned();

    let head_not_modified = gateway_request(
        &client,
        &config.gateway_url,
        Method::HEAD,
        &blob_path,
        &caller_token,
    )
    .header(header::IF_NONE_MATCH, &logical_etag)
    .send()
    .await?;
    ensure_status_code(
        head_not_modified.status(),
        StatusCode::NOT_MODIFIED,
        "gateway conditional HEAD",
    )?;

    let head_precondition_failed = gateway_request(
        &client,
        &config.gateway_url,
        Method::HEAD,
        &blob_path,
        &caller_token,
    )
    .header(header::IF_MATCH, "\"stale-logical-etag\"")
    .send()
    .await?;
    ensure_status_code(
        head_precondition_failed.status(),
        StatusCode::PRECONDITION_FAILED,
        "gateway stale conditional HEAD",
    )?;

    let full = gateway_request(
        &client,
        &config.gateway_url,
        Method::GET,
        &blob_path,
        &caller_token,
    )
    .send()
    .await?;
    ensure_status_code(full.status(), StatusCode::OK, "gateway GET")?;
    ensure!(
        full.bytes().await?.as_ref() == payload,
        "gateway GET returned unexpected bytes"
    );

    let range = gateway_request(
        &client,
        &config.gateway_url,
        Method::GET,
        &blob_path,
        &caller_token,
    )
    .header(header::RANGE, "bytes=1-8")
    .send()
    .await?;
    ensure_status_code(
        range.status(),
        StatusCode::PARTIAL_CONTENT,
        "gateway range GET",
    )?;
    ensure!(
        range.bytes().await?.as_ref() == &payload[1..=8],
        "gateway range GET returned unexpected bytes"
    );

    let not_modified = gateway_request(
        &client,
        &config.gateway_url,
        Method::GET,
        &blob_path,
        &caller_token,
    )
    .header(header::IF_NONE_MATCH, &logical_etag)
    .send()
    .await?;
    ensure_status_code(
        not_modified.status(),
        StatusCode::NOT_MODIFIED,
        "gateway conditional GET",
    )?;

    let get_precondition_failed = gateway_request(
        &client,
        &config.gateway_url,
        Method::GET,
        &blob_path,
        &caller_token,
    )
    .header(header::IF_MATCH, "\"stale-logical-etag\"")
    .send()
    .await?;
    ensure_status_code(
        get_precondition_failed.status(),
        StatusCode::PRECONDITION_FAILED,
        "gateway stale conditional GET",
    )?;

    let canonical_blob = format!("/{}{}", config.logical_account, blob_path);
    let path_hash = hex::encode(Sha256::digest(canonical_blob.as_bytes()));
    let head_object = format!("heads/{path_hash}.json");
    let head_a = backend_get(
        &client,
        &config.backend_a_url,
        "overmesh-system",
        &head_object,
        &control_token,
    )
    .await?;
    let head_b = backend_get(
        &client,
        &config.backend_b_url,
        "overmesh-system",
        &head_object,
        &control_token,
    )
    .await?;
    ensure!(head_a == head_b, "replica heads differ");
    let backend_head_etag_a = backend_head_etag(
        &client,
        &config.backend_a_url,
        "overmesh-system",
        &head_object,
        &control_token,
    )
    .await?;
    let backend_head_etag_b = backend_head_etag(
        &client,
        &config.backend_b_url,
        "overmesh-system",
        &head_object,
        &control_token,
    )
    .await?;
    ensure!(
        backend_head_etag_a != logical_etag && backend_head_etag_b != logical_etag,
        "a backend ETag leaked into the public logical ETag"
    );
    let commit = verify_local_commit_manifest_bytes(&head_a)?;
    ensure!(
        commit.blob == canonical_blob
            && commit.content_length == u64::try_from(payload.len())?
            && commit.logical_etag == logical_etag,
        "signed head does not match the public response"
    );
    let commit_history_object = format!(
        "high-water/{path_hash}/history/{:020}-{}.json",
        commit.logical_version,
        stable_component(&commit.write_id)
    );
    for backend_url in [&config.backend_a_url, &config.backend_b_url] {
        ensure!(
            backend_get(
                &client,
                backend_url,
                "overmesh-system",
                &commit_history_object,
                &control_token,
            )
            .await?
                == head_a,
            "immutable commit high-water history does not match the signed head"
        );
    }

    let root_a = backend_get(
        &client,
        &config.backend_a_url,
        "overmesh-system",
        &commit.block_manifest_object,
        &control_token,
    )
    .await?;
    let root_b = backend_get(
        &client,
        &config.backend_b_url,
        "overmesh-system",
        &commit.block_manifest_object,
        &control_token,
    )
    .await?;
    ensure!(root_a == root_b, "replica block-manifest roots differ");
    let block_manifest = verify_local_block_manifest(&commit, &root_a)?;
    for (index, reference) in block_manifest.pages.iter().enumerate() {
        let page_a = backend_get(
            &client,
            &config.backend_a_url,
            "overmesh-system",
            &reference.object,
            &control_token,
        )
        .await?;
        let page_b = backend_get(
            &client,
            &config.backend_b_url,
            "overmesh-system",
            &reference.object,
            &control_token,
        )
        .await?;
        ensure!(page_a == page_b, "replica block-manifest pages differ");
        verify_block_manifest_page(&block_manifest, index, &page_a)?;
    }

    let content_a = backend_get(
        &client,
        &config.backend_a_url,
        &commit.content_container,
        &commit.content_object,
        &caller_token,
    )
    .await?;
    let content_b = backend_get(
        &client,
        &config.backend_b_url,
        &commit.content_container,
        &commit.content_object,
        &caller_token,
    )
    .await?;
    ensure!(
        content_a == content_b
            && content_a.as_slice() == payload
            && sha256_bytes(&content_a) == commit.content_sha256,
        "physical replica content does not match the signed declaration"
    );

    let first_page = block_manifest
        .pages
        .first()
        .context("block manifest has no page")?;
    let first_page_bytes = backend_get(
        &client,
        &config.backend_b_url,
        "overmesh-system",
        &first_page.object,
        &control_token,
    )
    .await?;
    backend_delete(
        &client,
        &config.backend_b_url,
        "overmesh-system",
        &first_page.object,
        &control_token,
    )
    .await?;

    let head_without_page = gateway_request(
        &client,
        &config.gateway_url,
        Method::HEAD,
        &blob_path,
        &caller_token,
    )
    .send()
    .await?;
    ensure_status_code(
        head_without_page.status(),
        StatusCode::OK,
        "HEAD with an unavailable block page",
    )?;

    let get_without_page = gateway_request(
        &client,
        &config.gateway_url,
        Method::GET,
        &blob_path,
        &caller_token,
    )
    .send()
    .await?;
    if get_without_page.status().is_success() && get_without_page.bytes().await.is_ok() {
        bail!("GET succeeded after a block-manifest page was removed");
    }
    backend_put(
        &client,
        &config.backend_b_url,
        "overmesh-system",
        &first_page.object,
        &control_token,
        first_page_bytes,
    )
    .await?;

    let delete_write_id = format!("harness-system-delete-{run_id}");
    let deleted = gateway_request(
        &client,
        &config.gateway_url,
        Method::DELETE,
        &blob_path,
        &caller_token,
    )
    .header("x-overmesh-write-id", &delete_write_id)
    .header(header::IF_MATCH, &logical_etag)
    .send()
    .await?;
    ensure_status_code(deleted.status(), StatusCode::ACCEPTED, "gateway DELETE")?;
    let deleted_version = deleted
        .headers()
        .get("x-overmesh-logical-version")
        .context("gateway DELETE omitted logical version")?
        .to_str()?
        .parse::<u64>()?;

    let delete_retry = gateway_request(
        &client,
        &config.gateway_url,
        Method::DELETE,
        &blob_path,
        &caller_token,
    )
    .header("x-overmesh-write-id", &delete_write_id)
    .send()
    .await?;
    ensure_status_code(
        delete_retry.status(),
        StatusCode::ACCEPTED,
        "gateway DELETE retry",
    )?;
    ensure!(
        delete_retry
            .headers()
            .get("x-overmesh-idempotent-replay")
            .and_then(|value| value.to_str().ok())
            == Some("true"),
        "gateway DELETE retry was not identified as idempotent"
    );

    for method in [Method::HEAD, Method::GET] {
        let response = gateway_request(
            &client,
            &config.gateway_url,
            method.clone(),
            &blob_path,
            &caller_token,
        )
        .send()
        .await?;
        ensure_status_code(
            response.status(),
            StatusCode::NOT_FOUND,
            "gateway read after DELETE",
        )?;
    }
    let tombstone_a = backend_get(
        &client,
        &config.backend_a_url,
        "overmesh-system",
        &head_object,
        &control_token,
    )
    .await?;
    let tombstone_b = backend_get(
        &client,
        &config.backend_b_url,
        "overmesh-system",
        &head_object,
        &control_token,
    )
    .await?;
    ensure!(tombstone_a == tombstone_b, "replica tombstones differ");
    let tombstone = verify_local_commit_manifest_bytes(&tombstone_a)?;
    ensure!(
        tombstone.state == ManifestState::Tombstoned
            && tombstone.logical_version == deleted_version
            && tombstone.previous_logical_etag.as_deref() == Some(&logical_etag)
            && tombstone.deleted_at_unix_ms.is_some(),
        "signed tombstone does not describe the deleted generation"
    );
    let high_water_object = format!("high-water/{path_hash}/current.json");
    let high_water_a = backend_get(
        &client,
        &config.backend_a_url,
        "overmesh-system",
        &high_water_object,
        &control_token,
    )
    .await?;
    let high_water_b = backend_get(
        &client,
        &config.backend_b_url,
        "overmesh-system",
        &high_water_object,
        &control_token,
    )
    .await?;
    ensure!(
        high_water_a == tombstone_a && high_water_b == tombstone_a,
        "durable high-water checkpoint does not match the tombstone"
    );
    let tombstone_history_object = format!(
        "high-water/{path_hash}/history/{:020}-{}.json",
        tombstone.logical_version,
        stable_component(&tombstone.write_id)
    );
    for backend_url in [&config.backend_a_url, &config.backend_b_url] {
        ensure!(
            backend_get(
                &client,
                backend_url,
                "overmesh-system",
                &tombstone_history_object,
                &control_token,
            )
            .await?
                == tombstone_a,
            "immutable tombstone high-water history does not match the signed head"
        );
    }
    for backend_url in [&config.backend_a_url, &config.backend_b_url] {
        let retained = backend_get(
            &client,
            backend_url,
            &commit.content_container,
            &commit.content_object,
            &control_token,
        )
        .await?;
        ensure!(
            retained.as_slice() == payload,
            "physical content was removed synchronously by DELETE"
        );
    }

    println!(
        "system-validation\tPASS\t{}\t{}\t{}",
        commit.logical_version,
        tombstone.logical_version,
        block_manifest.pages.len()
    );
    Ok(())
}

fn gateway_request(
    client: &Client,
    gateway_url: &str,
    method: Method,
    path: &str,
    token: &str,
) -> reqwest::RequestBuilder {
    client
        .request(
            method,
            format!("{}{path}", gateway_url.trim_end_matches('/')),
        )
        .bearer_auth(token)
        .header("x-ms-version", STORAGE_VERSION)
}

async fn backend_get(
    client: &Client,
    backend_url: &str,
    container: &str,
    object: &str,
    token: &str,
) -> Result<Vec<u8>> {
    let response = backend_request(client, backend_url, Method::GET, container, object, token)
        .send()
        .await?;
    ensure_status_code(response.status(), StatusCode::OK, "backend GET")?;
    Ok(response.bytes().await?.to_vec())
}

async fn backend_head_etag(
    client: &Client,
    backend_url: &str,
    container: &str,
    object: &str,
    token: &str,
) -> Result<String> {
    let response = backend_request(client, backend_url, Method::HEAD, container, object, token)
        .send()
        .await?;
    ensure_status_code(response.status(), StatusCode::OK, "backend HEAD")?;
    Ok(response
        .headers()
        .get(header::ETAG)
        .context("backend HEAD omitted ETag")?
        .to_str()?
        .to_owned())
}

async fn backend_delete(
    client: &Client,
    backend_url: &str,
    container: &str,
    object: &str,
    token: &str,
) -> Result<()> {
    let response = backend_request(
        client,
        backend_url,
        Method::DELETE,
        container,
        object,
        token,
    )
    .send()
    .await?;
    ensure_status(response, StatusCode::ACCEPTED, "backend DELETE").await
}

async fn backend_put(
    client: &Client,
    backend_url: &str,
    container: &str,
    object: &str,
    token: &str,
    bytes: Vec<u8>,
) -> Result<()> {
    let response = backend_request(client, backend_url, Method::PUT, container, object, token)
        .header("x-ms-blob-type", "BlockBlob")
        .body(bytes)
        .send()
        .await?;
    ensure_status(response, StatusCode::CREATED, "backend PUT").await
}

fn backend_request(
    client: &Client,
    backend_url: &str,
    method: Method,
    container: &str,
    object: &str,
    token: &str,
) -> reqwest::RequestBuilder {
    client
        .request(
            method,
            format!("{}/{container}/{object}", backend_url.trim_end_matches('/')),
        )
        .bearer_auth(token)
        .header("x-ms-version", STORAGE_VERSION)
        .header("x-ms-date", httpdate::fmt_http_date(SystemTime::now()))
}

async fn ensure_status(response: Response, expected: StatusCode, operation: &str) -> Result<()> {
    if response.status() == expected {
        return Ok(());
    }
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    bail!("{operation} returned {status}, expected {expected}: {body}")
}

fn ensure_status_code(actual: StatusCode, expected: StatusCode, operation: &str) -> Result<()> {
    ensure!(
        actual == expected,
        "{operation} returned {actual}, expected {expected}"
    );
    Ok(())
}
