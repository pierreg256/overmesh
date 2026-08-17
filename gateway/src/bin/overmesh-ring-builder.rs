use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result, bail, ensure};
use azure_core::credentials::TokenCredential;
use azure_identity::{
    AzureCliCredential, ManagedIdentityCredential, ManagedIdentityCredentialOptions, UserAssignedId,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use clap::{Parser, Subcommand, ValueEnum};
use overmesh_gateway::ring::{RingDocument, ring_signature_input};
use p256::{
    EncodedPoint,
    ecdsa::{Signature, VerifyingKey, signature::Verifier},
    pkcs8::{EncodePublicKey, LineEnding},
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const KEY_VAULT_SCOPE: &str = "https://vault.azure.net/.default";

#[derive(Debug, Parser)]
#[command(name = "overmesh-ring-builder")]
#[command(about = "Build and sign canonical Overmesh Ring documents")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Finalize {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    Sign {
        #[arg(long)]
        ring: PathBuf,
        #[arg(long)]
        signature: PathBuf,
        #[arg(long)]
        public_key: PathBuf,
        #[arg(long)]
        key_id: String,
        #[arg(long, value_enum, default_value_t = CredentialProvider::ManagedIdentity)]
        credential: CredentialProvider,
        #[arg(long)]
        managed_identity_client_id: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CredentialProvider {
    ManagedIdentity,
    AzureCli,
}

#[derive(Debug, Deserialize)]
struct KeyResponse {
    key: JsonWebKey,
}

#[derive(Debug, Deserialize)]
struct JsonWebKey {
    kid: String,
    kty: String,
    crv: String,
    x: String,
    y: String,
    key_ops: Vec<String>,
}

#[derive(Debug, Serialize)]
struct SignRequest {
    alg: &'static str,
    value: String,
}

#[derive(Debug, Deserialize)]
struct SignResponse {
    value: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Finalize { input, output } => finalize(&input, &output),
        Command::Sign {
            ring,
            signature,
            public_key,
            key_id,
            credential,
            managed_identity_client_id,
        } => {
            sign(
                &ring,
                &signature,
                &public_key,
                &key_id,
                credential,
                managed_identity_client_id,
            )
            .await
        }
    }
}

fn finalize(input: &PathBuf, output: &PathBuf) -> Result<()> {
    let mut ring: RingDocument = serde_yaml::from_slice(
        &std::fs::read(input)
            .with_context(|| format!("failed to read Ring draft {}", input.display()))?,
    )
    .with_context(|| format!("failed to parse Ring draft {}", input.display()))?;
    ring.ring_hash = ring.computed_hash()?;
    ring.validate(1, None)?;
    std::fs::write(output, serde_yaml::to_string(&ring)?)
        .with_context(|| format!("failed to write finalized Ring {}", output.display()))?;
    println!("{}", ring.ring_hash);
    Ok(())
}

async fn sign(
    ring_path: &PathBuf,
    signature_path: &PathBuf,
    public_key_path: &PathBuf,
    key_id: &str,
    provider: CredentialProvider,
    managed_identity_client_id: Option<String>,
) -> Result<()> {
    let ring_bytes = std::fs::read(ring_path)
        .with_context(|| format!("failed to read Ring {}", ring_path.display()))?;
    let ring: RingDocument = serde_yaml::from_slice(&ring_bytes)
        .with_context(|| format!("failed to parse Ring {}", ring_path.display()))?;
    ring.validate(1, None)?;
    ensure!(
        ring.signing_key_id == key_id,
        "Ring signingKeyId does not match --key-id"
    );

    let credential: Arc<dyn TokenCredential> = match provider {
        CredentialProvider::ManagedIdentity => {
            ManagedIdentityCredential::new(Some(ManagedIdentityCredentialOptions {
                user_assigned_id: managed_identity_client_id.map(UserAssignedId::ClientId),
                ..Default::default()
            }))?
        }
        CredentialProvider::AzureCli => AzureCliCredential::new(None)?,
    };
    let token = credential
        .get_token(&[KEY_VAULT_SCOPE], None)
        .await
        .context("failed to acquire Key Vault token")?
        .token
        .secret()
        .to_owned();
    let client = reqwest::Client::new();
    let key_url = format!("{}?api-version=7.4", key_id.trim_end_matches('/'));
    let response = client.get(&key_url).bearer_auth(&token).send().await?;
    let status = response.status();
    if status != StatusCode::OK {
        bail!(
            "Key Vault returned {status} while loading the Ring key: {}",
            response.text().await.unwrap_or_default()
        );
    }
    let key: KeyResponse = response.json().await?;
    ensure!(
        key.key.kid == key_id,
        "Key Vault returned an unexpected key ID"
    );
    ensure!(
        key.key.kty == "EC" && key.key.crv == "P-256",
        "Ring key must be an EC P-256 key"
    );
    ensure!(
        key.key.key_ops.iter().any(|operation| operation == "sign")
            && key
                .key
                .key_ops
                .iter()
                .any(|operation| operation == "verify"),
        "Ring key must permit sign and verify"
    );
    let verifying_key = verifying_key(&key.key)?;
    let input = ring_signature_input(&ring)?;
    let digest = URL_SAFE_NO_PAD.encode(Sha256::digest(&input));
    let sign_url = format!("{}/sign?api-version=7.4", key_id.trim_end_matches('/'));
    let response = client
        .post(sign_url)
        .bearer_auth(&token)
        .json(&SignRequest {
            alg: "ES256",
            value: digest,
        })
        .send()
        .await?;
    let status = response.status();
    if status != StatusCode::OK {
        bail!(
            "Key Vault returned {status} while signing the Ring: {}",
            response.text().await.unwrap_or_default()
        );
    }
    let signed: SignResponse = response.json().await?;
    let signature_bytes = URL_SAFE_NO_PAD
        .decode(&signed.value)
        .context("Key Vault returned an invalid base64url signature")?;
    let signature = Signature::from_slice(&signature_bytes)
        .context("Key Vault returned an invalid ES256 signature")?;
    verifying_key
        .verify(&input, &signature)
        .context("Key Vault Ring signature verification failed")?;
    std::fs::write(signature_path, &signed.value).with_context(|| {
        format!(
            "failed to write Ring signature {}",
            signature_path.display()
        )
    })?;
    std::fs::write(
        public_key_path,
        verifying_key.to_public_key_pem(LineEnding::LF)?,
    )
    .with_context(|| {
        format!(
            "failed to write Ring public key {}",
            public_key_path.display()
        )
    })?;
    println!("{key_id}");
    Ok(())
}

fn verifying_key(key: &JsonWebKey) -> Result<VerifyingKey> {
    let x = URL_SAFE_NO_PAD
        .decode(&key.x)
        .context("Key Vault Ring key x coordinate is invalid")?;
    let y = URL_SAFE_NO_PAD
        .decode(&key.y)
        .context("Key Vault Ring key y coordinate is invalid")?;
    ensure!(
        x.len() == 32 && y.len() == 32,
        "Key Vault Ring key coordinates must be 32 bytes"
    );
    let point =
        EncodedPoint::from_affine_coordinates(x.as_slice().into(), y.as_slice().into(), false);
    VerifyingKey::from_encoded_point(&point).context("Key Vault Ring public key is invalid")
}
