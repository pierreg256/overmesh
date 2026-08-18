use std::collections::HashMap;

use http::{HeaderMap, Uri, header::AUTHORIZATION};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header, errors::ErrorKind};
use serde::Deserialize;
use url::form_urlencoded;

use crate::error::StorageError;
use crate::identity::{CallerIdentity, CallerToken};

const SAS_QUERY_PARAMETERS: &[&str] = &[
    "sig", "se", "sp", "sv", "sr", "si", "sip", "spr", "skoid", "sktid", "skt", "ske", "sks",
    "skv", "rscc", "rscd", "rsce", "rscl", "rsct",
];

#[derive(Debug, Clone)]
pub struct AuthenticatedPrincipal {
    pub subject: String,
    pub tenant_id: String,
    pub object_id: String,
    pub authorized_party: Option<String>,
    pub access_token: CallerToken,
}

impl AuthenticatedPrincipal {
    pub fn identity(&self) -> CallerIdentity {
        CallerIdentity {
            tenant_id: self.tenant_id.clone(),
            object_id: self.object_id.clone(),
            subject: self.subject.clone(),
            authorized_party: self.authorized_party.clone(),
        }
    }
}

#[derive(Clone)]
pub struct Authenticator {
    issuer: String,
    audiences: Vec<String>,
    tenant_id: String,
    keys: HashMap<String, TrustedKey>,
}

#[derive(Clone)]
struct TrustedKey {
    algorithm: Algorithm,
    decoding_key: DecodingKey,
}

#[derive(Debug, Deserialize)]
struct JwksDocument {
    keys: Vec<JsonWebKey>,
}

#[derive(Debug, Deserialize)]
struct JsonWebKey {
    kty: String,
    kid: String,
    #[serde(default)]
    alg: Option<String>,
    #[serde(default)]
    crv: Option<String>,
    #[serde(default)]
    x: Option<String>,
    #[serde(default)]
    y: Option<String>,
    #[serde(default)]
    n: Option<String>,
    #[serde(default)]
    e: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Claims {
    sub: String,
    tid: String,
    oid: String,
    #[serde(default)]
    azp: Option<String>,
    #[serde(default)]
    appid: Option<String>,
}

impl Authenticator {
    pub fn from_jwks_json(
        issuer: impl Into<String>,
        audience: impl Into<String>,
        tenant_id: impl Into<String>,
        jwks_json: &str,
    ) -> anyhow::Result<Self> {
        let document: JwksDocument = serde_json::from_str(jwks_json)?;
        let mut keys = HashMap::new();
        for key in document.keys {
            let (algorithm, decoding_key) = match (key.kty.as_str(), key.alg.as_deref()) {
                ("EC", Some("ES256") | None) if key.crv.as_deref() == Some("P-256") => {
                    let x = key
                        .x
                        .as_deref()
                        .ok_or_else(|| anyhow::anyhow!("EC JWK {} is missing x", key.kid))?;
                    let y = key
                        .y
                        .as_deref()
                        .ok_or_else(|| anyhow::anyhow!("EC JWK {} is missing y", key.kid))?;
                    (Algorithm::ES256, DecodingKey::from_ec_components(x, y)?)
                }
                ("RSA", Some("RS256") | None) => {
                    let modulus = key
                        .n
                        .as_deref()
                        .ok_or_else(|| anyhow::anyhow!("RSA JWK {} is missing n", key.kid))?;
                    let exponent = key
                        .e
                        .as_deref()
                        .ok_or_else(|| anyhow::anyhow!("RSA JWK {} is missing e", key.kid))?;
                    (
                        Algorithm::RS256,
                        DecodingKey::from_rsa_components(modulus, exponent)?,
                    )
                }
                _ => {
                    anyhow::bail!(
                        "unsupported JWK {} with type {} and algorithm {}",
                        key.kid,
                        key.kty,
                        key.alg.as_deref().unwrap_or("<unspecified>")
                    );
                }
            };
            if keys
                .insert(
                    key.kid.clone(),
                    TrustedKey {
                        algorithm,
                        decoding_key,
                    },
                )
                .is_some()
            {
                anyhow::bail!("duplicate JWK key id {}", key.kid);
            }
        }
        if keys.is_empty() {
            anyhow::bail!("JWKS must contain at least one supported key");
        }

        let audience = audience.into();
        let audiences = if audience.trim_end_matches('/') == "https://storage.azure.com" {
            vec![
                "https://storage.azure.com/".to_owned(),
                "https://storage.azure.com".to_owned(),
            ]
        } else {
            vec![audience]
        };

        Ok(Self {
            issuer: issuer.into(),
            audiences,
            tenant_id: tenant_id.into(),
            keys,
        })
    }

    pub fn authenticate(
        &self,
        headers: &HeaderMap,
        uri: &Uri,
    ) -> Result<AuthenticatedPrincipal, StorageError> {
        if contains_sas(uri) {
            return Err(StorageError::sas_not_permitted());
        }

        let authorization = headers
            .get(AUTHORIZATION)
            .ok_or_else(|| {
                StorageError::authentication_failed("Bearer authentication is required.")
            })?
            .to_str()
            .map_err(|_| {
                StorageError::authentication_failed("The Authorization header is invalid.")
            })?;
        let (scheme, credential) = authorization.split_once(' ').ok_or_else(|| {
            StorageError::authentication_failed("The Authorization header is invalid.")
        })?;
        if scheme.eq_ignore_ascii_case("SharedKey") || scheme.eq_ignore_ascii_case("SharedKeyLite")
        {
            return Err(StorageError::key_authentication_not_permitted());
        }
        if !scheme.eq_ignore_ascii_case("Bearer") || credential.trim().is_empty() {
            return Err(StorageError::authentication_failed(
                "OAuth 2.0 bearer authentication is required.",
            ));
        }

        self.validate_token(credential.trim())
    }

    fn validate_token(&self, token: &str) -> Result<AuthenticatedPrincipal, StorageError> {
        let header = decode_header(token)
            .map_err(|_| StorageError::authentication_failed("The bearer token is invalid."))?;
        let key_id = header.kid.ok_or_else(|| {
            StorageError::authentication_failed("The bearer token does not identify a signing key.")
        })?;
        let trusted_key = self.keys.get(&key_id).ok_or_else(|| {
            StorageError::authentication_failed("The bearer token signing key is not trusted.")
        })?;
        if header.alg != trusted_key.algorithm {
            return Err(StorageError::authentication_failed(
                "The bearer token algorithm does not match its trusted key.",
            ));
        }

        let mut validation = Validation::new(trusted_key.algorithm);
        validation.set_audience(&self.audiences);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.leeway = 30;
        let token_data =
            decode::<Claims>(token, &trusted_key.decoding_key, &validation).map_err(|error| {
                let message = match error.kind() {
                    ErrorKind::ExpiredSignature => "The bearer token has expired.",
                    ErrorKind::ImmatureSignature => "The bearer token is not valid yet.",
                    ErrorKind::InvalidAudience => "The bearer token audience is invalid.",
                    ErrorKind::InvalidIssuer => "The bearer token issuer is invalid.",
                    _ => "The bearer token is invalid.",
                };
                StorageError::authentication_failed(message)
            })?;
        if token_data.claims.tid != self.tenant_id {
            return Err(StorageError::authentication_failed(
                "The bearer token tenant is invalid.",
            ));
        }

        Ok(AuthenticatedPrincipal {
            subject: token_data.claims.sub,
            tenant_id: token_data.claims.tid,
            object_id: token_data.claims.oid,
            authorized_party: token_data.claims.azp.or(token_data.claims.appid),
            access_token: CallerToken::new(token.to_owned()),
        })
    }
}

fn contains_sas(uri: &Uri) -> bool {
    uri.query().is_some_and(|query| {
        form_urlencoded::parse(query.as_bytes())
            .any(|(key, _)| SAS_QUERY_PARAMETERS.contains(&key.as_ref()))
    })
}
