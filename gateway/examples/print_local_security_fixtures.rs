use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use overmesh_gateway::ring::{RingDocument, RingNode, ring_signature_input};
use p256::{
    ecdsa::{Signature, SigningKey, signature::Signer},
    pkcs8::{EncodePublicKey, LineEnding},
};

fn main() -> anyhow::Result<()> {
    let mut ring = RingDocument {
        api_version: "overmesh.io/v1".to_owned(),
        ring_version: 1,
        root: true,
        parent_ring_version: None,
        parent_ring_hash: None,
        replication_factor: 2,
        created_at: "2026-08-15T10:00:00Z".to_owned(),
        signed_at_unix_ms: 1_776_000_000_000,
        signing_key_id: "test-ring-key-01".to_owned(),
        ring_hash: String::new(),
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
    };
    ring.ring_hash = ring.computed_hash()?;

    let signing_key = SigningKey::from_bytes((&[7_u8; 32]).into())?;
    let signature: Signature = signing_key.sign(&ring_signature_input(&ring)?);
    let public_key = signing_key
        .verifying_key()
        .to_public_key_pem(LineEnding::LF)?;

    println!("---RING_YAML---");
    print!("{}", serde_yaml::to_string(&ring)?);
    println!("---RING_SIGNATURE---");
    println!("{}", URL_SAFE_NO_PAD.encode(signature.to_bytes()));
    println!("---RING_PUBLIC_KEY---");
    print!("{public_key}");
    println!("---AUTH_JWKS---");
    let auth_key = SigningKey::from_bytes((&[9_u8; 32]).into())?;
    let auth_point = auth_key.verifying_key().to_encoded_point(false);
    let auth_jwks = serde_json::json!({
        "keys": [{
            "kty": "EC",
            "use": "sig",
            "kid": "test-auth-key-01",
            "alg": "ES256",
            "crv": "P-256",
            "x": URL_SAFE_NO_PAD.encode(auth_point.x().expect("x coordinate")),
            "y": URL_SAFE_NO_PAD.encode(auth_point.y().expect("y coordinate"))
        }]
    });
    println!("{}", serde_json::to_string_pretty(&auth_jwks)?);
    Ok(())
}
