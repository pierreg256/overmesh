use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::manifest::{ManifestError, ManifestSigner, SignatureDomain, SignedDocument};

const TOKEN_API_VERSION: &str = "overmesh.io/continuation-token/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ContinuationScope {
    Containers,
    Blobs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContinuationBinding {
    pub account: String,
    pub container: Option<String>,
    pub scope: ContinuationScope,
    pub prefix: String,
    pub delimiter: String,
    pub include: Vec<String>,
    pub max_results: u32,
    pub ring_version: u64,
    pub ring_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ContinuationState {
    pub last_ordering_key: String,
    pub backend_cursors: BTreeMap<String, Option<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ContinuationPayload {
    api_version: String,
    account: String,
    container: Option<String>,
    scope: ContinuationScope,
    prefix: String,
    delimiter: String,
    include: Vec<String>,
    max_results: u32,
    ring_version: u64,
    ring_hash: String,
    state: ContinuationState,
    issued_at_unix_ms: u64,
    expires_at_unix_ms: u64,
    signing_key_id: String,
}

#[derive(Debug, Error)]
pub enum ContinuationError {
    #[error("continuation token encoding is invalid")]
    Encoding,
    #[error("continuation token document is invalid")]
    Document,
    #[error("continuation token signature is invalid: {0}")]
    Signature(#[from] ManifestError),
    #[error("continuation token is expired")]
    Expired,
    #[error("continuation token issue time is in the future")]
    NotYetValid,
    #[error("continuation token does not match this request")]
    Binding,
    #[error("continuation token ordering key is invalid")]
    Ordering,
}

pub async fn issue(
    binding: &ContinuationBinding,
    state: &ContinuationState,
    lifetime: Duration,
    signer: &dyn ManifestSigner,
) -> Result<String, ContinuationError> {
    issue_at(binding, state, lifetime, now_unix_ms(), signer).await
}

pub async fn issue_at(
    binding: &ContinuationBinding,
    state: &ContinuationState,
    lifetime: Duration,
    issued_at_unix_ms: u64,
    signer: &dyn ManifestSigner,
) -> Result<String, ContinuationError> {
    validate_state(state)?;
    let expires_at_unix_ms = issued_at_unix_ms
        .checked_add(u64::try_from(lifetime.as_millis()).map_err(|_| ContinuationError::Document)?)
        .ok_or(ContinuationError::Document)?;
    let signed = SignedDocument::create_at(
        ContinuationPayload {
            api_version: TOKEN_API_VERSION.to_owned(),
            account: binding.account.clone(),
            container: binding.container.clone(),
            scope: binding.scope,
            prefix: binding.prefix.clone(),
            delimiter: binding.delimiter.clone(),
            include: binding.include.clone(),
            max_results: binding.max_results,
            ring_version: binding.ring_version,
            ring_hash: binding.ring_hash.clone(),
            state: state.clone(),
            issued_at_unix_ms,
            expires_at_unix_ms,
            signing_key_id: signer.key_id().to_owned(),
        },
        SignatureDomain::ContinuationToken,
        signer,
        issued_at_unix_ms,
    )
    .await?;
    Ok(URL_SAFE_NO_PAD.encode(signed.canonical_bytes()?))
}

pub fn verify(
    marker: &str,
    binding: &ContinuationBinding,
    signer: &dyn ManifestSigner,
) -> Result<ContinuationState, ContinuationError> {
    verify_at(marker, binding, now_unix_ms(), signer)
}

pub fn verify_at(
    marker: &str,
    binding: &ContinuationBinding,
    now_unix_ms: u64,
    signer: &dyn ManifestSigner,
) -> Result<ContinuationState, ContinuationError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(marker)
        .map_err(|_| ContinuationError::Encoding)?;
    let signed = SignedDocument::<ContinuationPayload>::from_bytes(&bytes)
        .map_err(|_| ContinuationError::Document)?;
    if signed.canonical_bytes()? != bytes {
        return Err(ContinuationError::Document);
    }
    signed.verify(
        SignatureDomain::ContinuationToken,
        &signed.payload.signing_key_id,
        signer,
    )?;
    let payload = &signed.payload;
    if payload.api_version != TOKEN_API_VERSION
        || payload.issued_at_unix_ms != signed.signed_at_unix_ms
        || payload.expires_at_unix_ms < payload.issued_at_unix_ms
    {
        return Err(ContinuationError::Document);
    }
    if now_unix_ms < payload.issued_at_unix_ms {
        return Err(ContinuationError::NotYetValid);
    }
    if now_unix_ms > payload.expires_at_unix_ms {
        return Err(ContinuationError::Expired);
    }
    if payload.account != binding.account
        || payload.container != binding.container
        || payload.scope != binding.scope
        || payload.prefix != binding.prefix
        || payload.delimiter != binding.delimiter
        || payload.include != binding.include
        || payload.max_results != binding.max_results
        || payload.ring_version != binding.ring_version
        || payload.ring_hash != binding.ring_hash
    {
        return Err(ContinuationError::Binding);
    }
    validate_state(&payload.state)?;
    Ok(payload.state.clone())
}

fn validate_ordering_key(value: &str) -> Result<(), ContinuationError> {
    if value.is_empty() || value.len() > 4_096 || value.chars().any(char::is_control) {
        return Err(ContinuationError::Ordering);
    }
    Ok(())
}

fn validate_state(state: &ContinuationState) -> Result<(), ContinuationError> {
    validate_ordering_key(&state.last_ordering_key)?;
    if state.backend_cursors.is_empty()
        || state.backend_cursors.len() > 256
        || state.backend_cursors.iter().any(|(backend, cursor)| {
            backend.is_empty()
                || backend.len() > 256
                || backend.chars().any(char::is_control)
                || cursor.as_ref().is_some_and(|value| {
                    value.is_empty() || value.len() > 4_096 || value.chars().any(char::is_control)
                })
        })
    {
        return Err(ContinuationError::Ordering);
    }
    Ok(())
}

fn now_unix_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after Unix epoch")
            .as_millis(),
    )
    .expect("Unix timestamp milliseconds fit in u64")
}

#[cfg(test)]
mod tests {
    use crate::manifest::{KeyValidity, LocalTestManifestSigner};

    use super::*;

    fn binding() -> ContinuationBinding {
        ContinuationBinding {
            account: "account".to_owned(),
            container: Some("container".to_owned()),
            scope: ContinuationScope::Blobs,
            prefix: "a/".to_owned(),
            delimiter: "/".to_owned(),
            include: vec!["metadata".to_owned()],
            max_results: 2,
            ring_version: 7,
            ring_hash: format!("sha256:{}", "1".repeat(64)),
        }
    }

    #[tokio::test]
    async fn binds_and_expires_tokens() {
        let signer =
            LocalTestManifestSigner::new("key", true, KeyValidity::new(0, u64::MAX).expect("key"))
                .expect("signer");
        let token = issue_at(
            &binding(),
            &ContinuationState {
                last_ordering_key: "blob:a".to_owned(),
                backend_cursors: BTreeMap::from([
                    ("storage-a".to_owned(), Some("opaque-a".to_owned())),
                    ("storage-b".to_owned(), Some("opaque-b".to_owned())),
                ]),
            },
            Duration::from_millis(100),
            1_000,
            &signer,
        )
        .await
        .expect("token");
        let verified = verify_at(&token, &binding(), 1_050, &signer).expect("valid");
        assert_eq!(verified.last_ordering_key, "blob:a");
        assert_eq!(
            verified.backend_cursors["storage-a"].as_deref(),
            Some("opaque-a")
        );
        assert!(matches!(
            verify_at(&token, &binding(), 1_101, &signer),
            Err(ContinuationError::Expired)
        ));
        let mut reused = binding();
        reused.prefix = "b/".to_owned();
        assert!(matches!(
            verify_at(&token, &reused, 1_050, &signer),
            Err(ContinuationError::Binding)
        ));
    }

    #[tokio::test]
    async fn rejects_tampering() {
        let signer =
            LocalTestManifestSigner::new("key", true, KeyValidity::new(0, u64::MAX).expect("key"))
                .expect("signer");
        let mut token = issue_at(
            &binding(),
            &ContinuationState {
                last_ordering_key: "blob:a".to_owned(),
                backend_cursors: BTreeMap::from([("storage-a".to_owned(), None)]),
            },
            Duration::from_millis(100),
            1_000,
            &signer,
        )
        .await
        .expect("token");
        token.push('A');
        assert!(verify_at(&token, &binding(), 1_050, &signer).is_err());
    }
}
