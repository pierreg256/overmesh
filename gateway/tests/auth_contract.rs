use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header::AUTHORIZATION},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use overmesh_gateway::{
    AppState, Authenticator, RingDocument, SignedRing, build_router, ring::RingNode,
};
use p256::{
    ecdsa::SigningKey,
    pkcs8::{EncodePrivateKey, LineEnding},
};
use rand_chacha::{ChaCha8Rng, rand_core::SeedableRng};
use rsa::{
    RsaPrivateKey,
    pkcs8::{EncodePrivateKey as EncodeRsaPrivateKey, LineEnding as RsaLineEnding},
    traits::PublicKeyParts,
};
use serde::{Deserialize, Serialize};
use tower::ServiceExt;

const ISSUER: &str = "https://sts.windows.net/test-tenant/";
const AUDIENCE: &str = "https://storage.azure.com/";
const TENANT: &str = "test-tenant";
const KEY_ID: &str = "test-auth-key-01";
const RSA_KEY_ID: &str = "test-auth-rsa-key-01";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Contract {
    api_version: String,
    cases: Vec<ContractCase>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContractCase {
    id: String,
    description: String,
    method: String,
    path: String,
    authentication: AuthenticationCase,
    storage_version: Option<String>,
    expected_status: u16,
    expected_error_code: String,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum AuthenticationCase {
    None,
    SharedKey,
    ValidBearer,
    ValidBearerRs256,
    WrongAudience,
    WrongTenant,
    Expired,
}

#[derive(Serialize)]
struct TokenClaims<'a> {
    iss: &'a str,
    aud: &'a str,
    sub: &'a str,
    tid: &'a str,
    oid: &'a str,
    azp: &'a str,
    exp: u64,
    nbf: u64,
    iat: u64,
}

struct TestIdentity {
    signing_key: SigningKey,
    rsa_signing_key: RsaPrivateKey,
    authenticator: Authenticator,
}

impl TestIdentity {
    fn new() -> Self {
        let signing_key = SigningKey::from_bytes((&[9_u8; 32]).into()).expect("test auth key");
        let encoded = signing_key.verifying_key().to_encoded_point(false);
        let mut random = ChaCha8Rng::seed_from_u64(20_260_815);
        let rsa_signing_key = RsaPrivateKey::new(&mut random, 2048).expect("test RSA signing key");
        let rsa_public_key = rsa_signing_key.to_public_key();
        let jwks = serde_json::json!({
            "keys": [
                {
                    "kty": "EC",
                    "use": "sig",
                    "kid": KEY_ID,
                    "alg": "ES256",
                    "crv": "P-256",
                    "x": URL_SAFE_NO_PAD.encode(encoded.x().expect("x coordinate")),
                    "y": URL_SAFE_NO_PAD.encode(encoded.y().expect("y coordinate"))
                },
                {
                    "kty": "RSA",
                    "use": "sig",
                    "kid": RSA_KEY_ID,
                    "n": URL_SAFE_NO_PAD.encode(rsa_public_key.n().to_bytes_be()),
                    "e": URL_SAFE_NO_PAD.encode(rsa_public_key.e().to_bytes_be())
                }
            ]
        });
        let authenticator =
            Authenticator::from_jwks_json(ISSUER, AUDIENCE, TENANT, &jwks.to_string())
                .expect("test authenticator");
        Self {
            signing_key,
            rsa_signing_key,
            authenticator,
        }
    }

    fn issue(&self, authentication: AuthenticationCase) -> Option<String> {
        match authentication {
            AuthenticationCase::None | AuthenticationCase::SharedKey => None,
            AuthenticationCase::ValidBearer
            | AuthenticationCase::ValidBearerRs256
            | AuthenticationCase::WrongAudience
            | AuthenticationCase::WrongTenant
            | AuthenticationCase::Expired => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("current Unix time")
                    .as_secs();
                let audience = if matches!(authentication, AuthenticationCase::WrongAudience) {
                    "https://management.azure.com/"
                } else {
                    AUDIENCE
                };
                let tenant = if matches!(authentication, AuthenticationCase::WrongTenant) {
                    "another-tenant"
                } else {
                    TENANT
                };
                let expiration = if matches!(authentication, AuthenticationCase::Expired) {
                    now.saturating_sub(300)
                } else {
                    now + 300
                };
                let claims = TokenClaims {
                    iss: ISSUER,
                    aud: audience,
                    sub: "test-subject",
                    tid: tenant,
                    oid: "00000000-0000-0000-0000-000000000001",
                    azp: "00000000-0000-0000-0000-000000000002",
                    exp: expiration,
                    nbf: now.saturating_sub(30),
                    iat: now,
                };
                let (encoding_key, mut header) =
                    if matches!(authentication, AuthenticationCase::ValidBearerRs256) {
                        let private_pem = EncodeRsaPrivateKey::to_pkcs8_pem(
                            &self.rsa_signing_key,
                            RsaLineEnding::LF,
                        )
                        .expect("RSA private key PEM");
                        (
                            EncodingKey::from_rsa_pem(private_pem.as_bytes())
                                .expect("RSA encoding key"),
                            Header::new(Algorithm::RS256),
                        )
                    } else {
                        let private_pem = self
                            .signing_key
                            .to_pkcs8_pem(LineEnding::LF)
                            .expect("private key PEM");
                        (
                            EncodingKey::from_ec_pem(private_pem.as_bytes())
                                .expect("EC encoding key"),
                            Header::new(Algorithm::ES256),
                        )
                    };
                header.kid = Some(
                    if matches!(authentication, AuthenticationCase::ValidBearerRs256) {
                        RSA_KEY_ID
                    } else {
                        KEY_ID
                    }
                    .to_owned(),
                );
                Some(encode(&header, &claims, &encoding_key).expect("test bearer token"))
            }
        }
    }
}

fn test_ring() -> RingDocument {
    RingDocument {
        api_version: "overmesh.io/v1".to_owned(),
        ring_version: 1,
        root: true,
        parent_ring_version: None,
        parent_ring_hash: None,
        replication_factor: 2,
        created_at: "2026-08-15T10:00:00Z".to_owned(),
        signed_at_unix_ms: 1_776_000_000_000,
        signing_key_id: "test-ring-key-01".to_owned(),
        ring_hash: "not-used-by-in-process-contract".to_owned(),
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
    }
}

#[tokio::test]
async fn executes_declarative_gateway_authentication_contract() {
    let contract_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../harness/contracts/gateway-auth-v1.yaml");
    let contract: Contract =
        serde_yaml::from_str(&std::fs::read_to_string(contract_path).expect("contract fixture"))
            .expect("valid contract");
    assert_eq!(
        contract.api_version,
        "harness.overmesh.io/gateway-auth-contract/v1"
    );

    let identity = TestIdentity::new();
    let app = build_router(AppState {
        authenticator: identity.authenticator.clone(),
        logical_account: "test-account".to_owned(),
        ring: std::sync::Arc::new(SignedRing::from_document(test_ring()).expect("ring")),
        commit_service: None,
        read_service: None,
    });

    for case in contract.cases {
        let mut builder = Request::builder()
            .method(case.method.as_str())
            .uri(case.path.as_str());
        if let Some(version) = &case.storage_version {
            builder = builder.header("x-ms-version", version);
        }
        match case.authentication {
            AuthenticationCase::SharedKey => {
                builder = builder.header(AUTHORIZATION, "SharedKey account:invalid-test-signature");
            }
            authentication => {
                if let Some(token) = identity.issue(authentication) {
                    builder = builder.header(AUTHORIZATION, format!("Bearer {token}"));
                }
            }
        }
        let response = app
            .clone()
            .oneshot(builder.body(Body::empty()).expect("request"))
            .await
            .expect("gateway response");
        assert_eq!(
            response.status(),
            StatusCode::from_u16(case.expected_status).expect("expected status"),
            "{}: {}",
            case.id,
            case.description
        );
        assert_eq!(
            response
                .headers()
                .get("x-ms-error-code")
                .expect("x-ms-error-code"),
            case.expected_error_code.as_str(),
            "{}: {}",
            case.id,
            case.description
        );
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("response body");
        assert!(
            !body.is_empty(),
            "{} must return an Azure-compatible error body",
            case.id
        );
    }
}

#[tokio::test]
async fn health_endpoint_does_not_require_client_authentication() {
    let identity = TestIdentity::new();
    let app = build_router(AppState {
        authenticator: identity.authenticator,
        logical_account: "test-account".to_owned(),
        ring: std::sync::Arc::new(SignedRing::from_document(test_ring()).expect("ring")),
        commit_service: None,
        read_service: None,
    });
    let response = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("gateway response");
    assert_eq!(response.status(), StatusCode::OK);
}
