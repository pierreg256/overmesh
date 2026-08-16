use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use p256::ecdsa::{
    Signature, SigningKey, VerifyingKey,
    signature::{Signer, Verifier},
};
use serde::Serialize;
use thiserror::Error;

const TEST_SIGNING_KEY_BYTES: [u8; 32] = [7; 32];

#[derive(Debug, Error)]
pub enum CryptoError {
    #[error("canonical serialization failed: {0}")]
    Canonicalization(#[from] serde_json::Error),
    #[error("invalid ES256 signature encoding")]
    InvalidSignatureEncoding,
    #[error("ES256 signature verification failed")]
    VerificationFailed,
}

pub fn canonicalize<T: Serialize>(value: &T) -> Result<Vec<u8>, CryptoError> {
    serde_jcs::to_vec(value).map_err(CryptoError::Canonicalization)
}

pub struct TestEs256Signer {
    signing_key: SigningKey,
}

impl Default for TestEs256Signer {
    fn default() -> Self {
        let signing_key =
            SigningKey::from_bytes((&TEST_SIGNING_KEY_BYTES).into()).expect("valid test key");
        Self { signing_key }
    }
}

impl TestEs256Signer {
    pub fn verifying_key(&self) -> VerifyingKey {
        *self.signing_key.verifying_key()
    }

    pub fn sign<T: Serialize>(&self, value: &T) -> Result<String, CryptoError> {
        let payload = canonicalize(value)?;
        let signature: Signature = self.signing_key.sign(&payload);
        Ok(URL_SAFE_NO_PAD.encode(signature.to_bytes()))
    }
}

pub fn verify<T: Serialize>(
    verifying_key: &VerifyingKey,
    value: &T,
    encoded_signature: &str,
) -> Result<(), CryptoError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded_signature)
        .map_err(|_| CryptoError::InvalidSignatureEncoding)?;
    let signature =
        Signature::from_slice(&bytes).map_err(|_| CryptoError::InvalidSignatureEncoding)?;
    verifying_key
        .verify(&canonicalize(value)?, &signature)
        .map_err(|_| CryptoError::VerificationFailed)
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::*;

    #[derive(Serialize)]
    struct Fixture<'a> {
        blob: &'a str,
        logical_version: u64,
    }

    #[test]
    fn signs_and_verifies_canonical_payload() {
        let signer = TestEs256Signer::default();
        let fixture = Fixture {
            blob: "/container/blob",
            logical_version: 42,
        };
        let signature = signer.sign(&fixture).expect("signature");
        verify(&signer.verifying_key(), &fixture, &signature).expect("valid signature");
    }

    #[test]
    fn rejects_modified_payload() {
        let signer = TestEs256Signer::default();
        let original = Fixture {
            blob: "/container/blob",
            logical_version: 42,
        };
        let modified = Fixture {
            blob: "/container/blob",
            logical_version: 43,
        };
        let signature = signer.sign(&original).expect("signature");
        assert!(verify(&signer.verifying_key(), &modified, &signature).is_err());
    }
}
