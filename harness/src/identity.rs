use std::time::{SystemTime, UNIX_EPOCH};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use p256::{
    ecdsa::SigningKey,
    pkcs8::{EncodePrivateKey, LineEnding},
};
use serde::Serialize;

pub const TEST_ISSUER: &str = "https://sts.windows.net/test-tenant/";
pub const TEST_AUDIENCE: &str = "https://storage.azure.com/";
pub const TEST_TENANT: &str = "test-tenant";
pub const TEST_AUTH_KEY_ID: &str = "test-auth-key-01";

#[derive(Debug, Clone, Copy)]
pub enum TestTokenKind {
    Valid,
    WrongAudience,
    WrongTenant,
    Expired,
}

#[derive(Debug, Clone, Copy)]
pub enum TestPrincipal {
    Caller,
    Gateway,
    Reconciler,
    Denied,
}

impl TestPrincipal {
    fn claims(self) -> (&'static str, &'static str, &'static str) {
        match self {
            Self::Caller => (
                "local-caller",
                "00000000-0000-0000-0000-000000000001",
                "00000000-0000-0000-0000-000000000002",
            ),
            Self::Gateway => (
                "local-gateway",
                "00000000-0000-0000-0000-000000000101",
                "00000000-0000-0000-0000-000000000102",
            ),
            Self::Reconciler => (
                "local-reconciler",
                "00000000-0000-0000-0000-000000000201",
                "00000000-0000-0000-0000-000000000202",
            ),
            Self::Denied => (
                "local-denied",
                "00000000-0000-0000-0000-000000000301",
                "00000000-0000-0000-0000-000000000302",
            ),
        }
    }
}

#[derive(Serialize)]
struct Claims<'a> {
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

pub fn issue_test_token(kind: TestTokenKind, principal: TestPrincipal) -> anyhow::Result<String> {
    let signing_key = SigningKey::from_bytes((&[9_u8; 32]).into())?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let audience = if matches!(kind, TestTokenKind::WrongAudience) {
        "https://management.azure.com/"
    } else {
        TEST_AUDIENCE
    };
    let tenant = if matches!(kind, TestTokenKind::WrongTenant) {
        "another-tenant"
    } else {
        TEST_TENANT
    };
    let expiration = if matches!(kind, TestTokenKind::Expired) {
        now.saturating_sub(300)
    } else {
        now + 300
    };
    let (subject, object_id, authorized_party) = principal.claims();
    let claims = Claims {
        iss: TEST_ISSUER,
        aud: audience,
        sub: subject,
        tid: tenant,
        oid: object_id,
        azp: authorized_party,
        exp: expiration,
        nbf: now.saturating_sub(30),
        iat: now,
    };
    let private_pem = signing_key.to_pkcs8_pem(LineEnding::LF)?;
    let encoding_key = EncodingKey::from_ec_pem(private_pem.as_bytes())?;
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(TEST_AUTH_KEY_ID.to_owned());
    Ok(encode(&header, &claims, &encoding_key)?)
}

pub fn test_jwks() -> serde_json::Value {
    let signing_key = SigningKey::from_bytes((&[9_u8; 32]).into()).expect("test auth key");
    let encoded = signing_key.verifying_key().to_encoded_point(false);
    serde_json::json!({
        "keys": [{
            "kty": "EC",
            "use": "sig",
            "kid": TEST_AUTH_KEY_ID,
            "alg": "ES256",
            "crv": "P-256",
            "x": URL_SAFE_NO_PAD.encode(encoded.x().expect("x coordinate")),
            "y": URL_SAFE_NO_PAD.encode(encoded.y().expect("y coordinate"))
        }]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims(token: &str) -> serde_json::Value {
        let payload = token.split('.').nth(1).expect("JWT payload");
        let bytes = URL_SAFE_NO_PAD.decode(payload).expect("base64url payload");
        serde_json::from_slice(&bytes).expect("JSON claims")
    }

    #[test]
    fn local_runtime_principals_are_distinct() {
        let caller = claims(
            &issue_test_token(TestTokenKind::Valid, TestPrincipal::Caller).expect("caller token"),
        );
        let gateway = claims(
            &issue_test_token(TestTokenKind::Valid, TestPrincipal::Gateway).expect("gateway token"),
        );
        let reconciler = claims(
            &issue_test_token(TestTokenKind::Valid, TestPrincipal::Reconciler)
                .expect("reconciler token"),
        );
        assert_ne!(caller["oid"], gateway["oid"]);
        assert_ne!(caller["oid"], reconciler["oid"]);
        assert_ne!(gateway["oid"], reconciler["oid"]);
        assert_ne!(caller["azp"], gateway["azp"]);
        assert_ne!(gateway["azp"], reconciler["azp"]);
    }
}
