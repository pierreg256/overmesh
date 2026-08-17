use std::{
    collections::HashSet,
    fs,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use p256::{
    ecdsa::{Signature, VerifyingKey, signature::Verifier},
    pkcs8::DecodePublicKey,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::resource::LogicalBlobId;

const RING_SIGNATURE_DOMAIN: &[u8] = b"overmesh:ring:v1\0";
const FIXED_VIRTUAL_NODES_PER_NODE: usize = 100;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RingDocument {
    pub api_version: String,
    pub ring_version: u64,
    pub root: bool,
    pub parent_ring_version: Option<u64>,
    pub parent_ring_hash: Option<String>,
    pub replication_factor: u8,
    pub created_at: String,
    pub signed_at_unix_ms: u64,
    pub signing_key_id: String,
    pub ring_hash: String,
    pub nodes: Vec<RingNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RingNode {
    pub id: String,
    pub region: String,
    pub weight: u32,
}

#[derive(Debug, Clone)]
pub struct SignedRing {
    pub document: RingDocument,
    pub document_path: PathBuf,
    placement: RingPlacement,
}

#[derive(Debug, Clone)]
pub struct TrustedRingKey {
    pub key_id: String,
    pub public_key_path: PathBuf,
    pub not_before_unix_ms: u64,
    pub not_after_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustedRingPredecessor {
    pub ring_version: u64,
    pub ring_hash: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RingHashPayload<'a> {
    api_version: &'a str,
    ring_version: u64,
    root: bool,
    parent_ring_version: Option<u64>,
    parent_ring_hash: Option<&'a str>,
    replication_factor: u8,
    created_at: &'a str,
    signed_at_unix_ms: u64,
    signing_key_id: &'a str,
    nodes: &'a [RingNode],
}

#[derive(Debug, Clone)]
struct RingPlacement {
    points: Arc<[RingPlacementPoint]>,
}

#[derive(Debug, Clone, Copy)]
struct RingPlacementPoint {
    position: u64,
    node_index: usize,
}

impl RingPlacement {
    fn build(document: &RingDocument) -> Result<Self> {
        validate_uniform_weights(&document.nodes)?;
        let capacity = document
            .nodes
            .len()
            .checked_mul(FIXED_VIRTUAL_NODES_PER_NODE)
            .context("Ring virtual-node circle exceeds the supported size")?;
        let mut points = Vec::with_capacity(capacity);
        for (node_index, node) in document.nodes.iter().enumerate() {
            for virtual_node in 0..FIXED_VIRTUAL_NODES_PER_NODE {
                let digest = Sha256::digest(format!("{}\0{virtual_node}", node.id).as_bytes());
                let position =
                    u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 prefix length"));
                points.push(RingPlacementPoint {
                    position,
                    node_index,
                });
            }
        }
        points.sort_by_key(|point| point.position);
        Ok(Self {
            points: points.into(),
        })
    }

    fn replicas_for<'a>(
        &self,
        document: &'a RingDocument,
        logical_blob: &LogicalBlobId,
    ) -> Result<Vec<&'a RingNode>> {
        if self.points.is_empty() {
            bail!("Ring does not contain any virtual nodes");
        }
        let blob_digest = Sha256::digest(logical_blob.canonical().as_bytes());
        let blob_position =
            u64::from_be_bytes(blob_digest[..8].try_into().expect("SHA-256 prefix length"));
        let start = self
            .points
            .partition_point(|point| point.position < blob_position)
            % self.points.len();
        let mut selected = Vec::new();
        let mut node_ids = HashSet::new();
        let mut regions = HashSet::new();
        for offset in 0..self.points.len() {
            let node =
                &document.nodes[self.points[(start + offset) % self.points.len()].node_index];
            if node_ids.contains(node.id.as_str()) || regions.contains(node.region.as_str()) {
                continue;
            }
            node_ids.insert(node.id.as_str());
            regions.insert(node.region.as_str());
            selected.push(node);
            if selected.len() == usize::from(document.replication_factor) {
                return Ok(selected);
            }
        }
        bail!("Ring cannot place two replicas in distinct regions")
    }
}

impl RingDocument {
    pub fn computed_hash(&self) -> Result<String> {
        let payload = RingHashPayload {
            api_version: &self.api_version,
            ring_version: self.ring_version,
            root: self.root,
            parent_ring_version: self.parent_ring_version,
            parent_ring_hash: self.parent_ring_hash.as_deref(),
            replication_factor: self.replication_factor,
            created_at: &self.created_at,
            signed_at_unix_ms: self.signed_at_unix_ms,
            signing_key_id: &self.signing_key_id,
            nodes: &self.nodes,
        };
        let canonical = serde_jcs::to_vec(&payload)?;
        Ok(format!("sha256:{}", hex::encode(Sha256::digest(canonical))))
    }

    pub fn validate(
        &self,
        minimum_version: u64,
        trusted_predecessor: Option<&TrustedRingPredecessor>,
    ) -> Result<()> {
        if self.api_version != "overmesh.io/v1" {
            bail!("unsupported Ring apiVersion {}", self.api_version);
        }
        if self.ring_version < minimum_version {
            bail!(
                "Ring version {} is below minimum trusted version {}",
                self.ring_version,
                minimum_version
            );
        }
        if self.created_at.trim().is_empty() || self.signed_at_unix_ms == 0 {
            bail!("Ring createdAt and signedAtUnixMs must be explicit");
        }
        match (
            self.root,
            self.parent_ring_version,
            self.parent_ring_hash.as_deref(),
            trusted_predecessor,
        ) {
            (true, None, None, None) if self.ring_version == 1 => {}
            (true, _, _, _) => {
                bail!("a root Ring must be version 1 with no parent fields or trusted predecessor")
            }
            (false, Some(parent_version), Some(parent_hash), Some(trusted)) => {
                if parent_version >= self.ring_version {
                    bail!("parent Ring version must be lower than the active Ring version");
                }
                if !valid_sha256(parent_hash) {
                    bail!("parent Ring hash is not a valid sha256 value");
                }
                if parent_version != trusted.ring_version {
                    bail!(
                        "Ring parent version {} does not match trusted predecessor version {}",
                        parent_version,
                        trusted.ring_version
                    );
                }
                if parent_hash != trusted.ring_hash {
                    bail!("Ring parent hash does not match the trusted predecessor");
                }
            }
            (false, None, None, _) => {
                bail!("a non-root Ring must declare parentRingVersion and parentRingHash")
            }
            (false, _, _, None) => {
                bail!("a non-root Ring requires a configured trusted predecessor")
            }
            (false, _, _, Some(_)) => {
                bail!("Ring parent version and hash must both be present")
            }
        }
        if self.replication_factor != 2 {
            bail!("V1 requires replicationFactor 2");
        }
        if self.nodes.len() < usize::from(self.replication_factor) {
            bail!("Ring does not contain enough nodes for replicationFactor 2");
        }
        let mut node_ids = HashSet::new();
        let mut regions = HashSet::new();
        for node in &self.nodes {
            if node.id.trim().is_empty() || node.region.trim().is_empty() {
                bail!("Ring node id and region must not be empty");
            }
            if node.weight == 0 {
                bail!("Ring node {} has zero weight", node.id);
            }
            if node.weight > 10_000 {
                bail!("Ring node {} weight exceeds the V1 limit", node.id);
            }
            if !node_ids.insert(node.id.as_str()) {
                bail!("Ring contains duplicate node id {}", node.id);
            }
            regions.insert(node.region.as_str());
        }
        validate_uniform_weights(&self.nodes)?;
        if regions.len() < 2 {
            bail!("V1 replicas must span at least two distinct regions");
        }
        let computed_hash = self.computed_hash()?;
        if self.ring_hash != computed_hash {
            bail!(
                "Ring hash mismatch: declared {}, computed {}",
                self.ring_hash,
                computed_hash
            );
        }
        Ok(())
    }

    pub fn replicas_for(&self, logical_blob: &LogicalBlobId) -> Result<Vec<&RingNode>> {
        RingPlacement::build(self)?.replicas_for(self, logical_blob)
    }
}

impl SignedRing {
    pub fn from_document(document: RingDocument) -> Result<Self> {
        Self::with_placement(document, PathBuf::new())
    }

    fn with_placement(document: RingDocument, document_path: PathBuf) -> Result<Self> {
        Ok(Self {
            placement: RingPlacement::build(&document)?,
            document,
            document_path,
        })
    }

    pub fn load(
        document_path: &Path,
        signature_path: &Path,
        trusted_keys: &[TrustedRingKey],
        trusted_predecessor: Option<&TrustedRingPredecessor>,
        minimum_version: u64,
    ) -> Result<Self> {
        let document_bytes = fs::read(document_path)
            .with_context(|| format!("failed to read Ring {}", document_path.display()))?;
        let document: RingDocument = serde_yaml::from_slice(&document_bytes)
            .with_context(|| format!("failed to parse Ring {}", document_path.display()))?;
        document.validate(minimum_version, trusted_predecessor)?;

        let signature_text = fs::read_to_string(signature_path).with_context(|| {
            format!("failed to read Ring signature {}", signature_path.display())
        })?;
        let signature_bytes = URL_SAFE_NO_PAD
            .decode(signature_text.trim())
            .context("Ring signature is not valid base64url")?;
        let signature =
            Signature::from_slice(&signature_bytes).context("Ring signature is not valid ES256")?;
        let trusted_key = trusted_keys
            .iter()
            .find(|trusted| trusted.key_id == document.signing_key_id)
            .with_context(|| {
                format!(
                    "Ring signing key {} is unknown or retired",
                    document.signing_key_id
                )
            })?;
        if trusted_key.not_before_unix_ms > trusted_key.not_after_unix_ms {
            bail!("Ring signing key validity period is invalid");
        }
        if !(trusted_key.not_before_unix_ms..=trusted_key.not_after_unix_ms)
            .contains(&document.signed_at_unix_ms)
        {
            bail!(
                "Ring signature timestamp {} is outside key validity period {}..={}",
                document.signed_at_unix_ms,
                trusted_key.not_before_unix_ms,
                trusted_key.not_after_unix_ms
            );
        }
        let public_key_pem =
            fs::read_to_string(&trusted_key.public_key_path).with_context(|| {
                format!(
                    "failed to read Ring public key {}",
                    trusted_key.public_key_path.display()
                )
            })?;
        let verifying_key = VerifyingKey::from_public_key_pem(public_key_pem.trim())
            .context("Ring public key is not a valid P-256 public key")?;
        verifying_key
            .verify(&ring_signature_input(&document)?, &signature)
            .context("Ring signature verification failed")?;

        Self::with_placement(document, document_path.to_path_buf())
    }

    pub fn replicas_for(&self, logical_blob: &LogicalBlobId) -> Result<Vec<&RingNode>> {
        self.placement.replicas_for(&self.document, logical_blob)
    }
}

impl Deref for SignedRing {
    type Target = RingDocument;

    fn deref(&self) -> &Self::Target {
        &self.document
    }
}

pub fn ring_signature_input(document: &RingDocument) -> Result<Vec<u8>> {
    let canonical = serde_jcs::to_vec(document)?;
    let mut input = Vec::with_capacity(RING_SIGNATURE_DOMAIN.len() + canonical.len());
    input.extend_from_slice(RING_SIGNATURE_DOMAIN);
    input.extend_from_slice(&canonical);
    Ok(input)
}

fn valid_sha256(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

fn validate_uniform_weights(nodes: &[RingNode]) -> Result<()> {
    let Some(first_weight) = nodes.first().map(|node| node.weight) else {
        return Ok(());
    };
    if nodes.iter().any(|node| node.weight != first_weight) {
        bail!("Ring node weights are reserved and must all be equal");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use p256::{
        ecdsa::{Signature, SigningKey, signature::Signer},
        pkcs8::{EncodePublicKey, LineEnding},
    };
    use tempfile::{TempDir, tempdir_in};

    use super::*;

    const SIGNED_AT: u64 = 1_776_000_000_000;

    fn logical_blob(path: &str) -> LogicalBlobId {
        LogicalBlobId::parse("test-account", path).expect("logical blob")
    }

    fn root_ring() -> RingDocument {
        let mut ring = RingDocument {
            api_version: "overmesh.io/v1".to_owned(),
            ring_version: 1,
            root: true,
            parent_ring_version: None,
            parent_ring_hash: None,
            replication_factor: 2,
            created_at: "2026-04-13T00:00:00Z".to_owned(),
            signed_at_unix_ms: SIGNED_AT,
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
        ring.ring_hash = ring.computed_hash().expect("ring hash");
        ring
    }

    fn rotated_ring(parent: &RingDocument) -> RingDocument {
        let mut ring = RingDocument {
            api_version: "overmesh.io/v1".to_owned(),
            ring_version: 2,
            root: false,
            parent_ring_version: Some(parent.ring_version),
            parent_ring_hash: Some(parent.ring_hash.clone()),
            replication_factor: 2,
            created_at: "2026-04-14T00:00:00Z".to_owned(),
            signed_at_unix_ms: SIGNED_AT + 1,
            signing_key_id: "test-ring-key-02".to_owned(),
            ring_hash: String::new(),
            nodes: parent.nodes.clone(),
        };
        ring.ring_hash = ring.computed_hash().expect("ring hash");
        ring
    }

    fn balanced_three_node_ring(weight: u32) -> RingDocument {
        let mut ring = RingDocument {
            api_version: "overmesh.io/v1".to_owned(),
            ring_version: 1,
            root: true,
            parent_ring_version: None,
            parent_ring_hash: None,
            replication_factor: 2,
            created_at: "2026-04-13T00:00:00Z".to_owned(),
            signed_at_unix_ms: SIGNED_AT,
            signing_key_id: "test-ring-key-01".to_owned(),
            ring_hash: String::new(),
            nodes: vec![
                RingNode {
                    id: "storage-a".to_owned(),
                    region: "local-a".to_owned(),
                    weight,
                },
                RingNode {
                    id: "storage-b".to_owned(),
                    region: "local-b".to_owned(),
                    weight,
                },
                RingNode {
                    id: "storage-c".to_owned(),
                    region: "local-c".to_owned(),
                    weight,
                },
            ],
        };
        ring.ring_hash = ring.computed_hash().expect("ring hash");
        ring
    }

    fn fixture_files(
        ring: &RingDocument,
        key_bytes: [u8; 32],
    ) -> (TempDir, PathBuf, PathBuf, TrustedRingKey) {
        let target = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target");
        let directory = tempdir_in(target).expect("fixture directory");
        let document_path = directory.path().join("ring.yaml");
        let signature_path = directory.path().join("ring.sig");
        let public_key_path = directory.path().join("ring-public.pem");
        let signing_key = SigningKey::from_bytes((&key_bytes).into()).expect("test key");
        let signature: Signature =
            signing_key.sign(&ring_signature_input(ring).expect("signature input"));
        let public_key = signing_key
            .verifying_key()
            .to_public_key_pem(LineEnding::LF)
            .expect("public key");
        fs::write(
            &document_path,
            serde_yaml::to_string(ring).expect("Ring YAML"),
        )
        .expect("write Ring");
        fs::write(
            &signature_path,
            URL_SAFE_NO_PAD.encode(signature.to_bytes()),
        )
        .expect("write signature");
        fs::write(&public_key_path, public_key).expect("write public key");
        (
            directory,
            document_path,
            signature_path,
            TrustedRingKey {
                key_id: ring.signing_key_id.clone(),
                public_key_path,
                not_before_unix_ms: ring.signed_at_unix_ms,
                not_after_unix_ms: ring.signed_at_unix_ms + 100,
            },
        )
    }

    #[test]
    fn loads_valid_signed_root_ring() {
        let ring = root_ring();
        let (_directory, document_path, signature_path, key) = fixture_files(&ring, [7; 32]);
        let loaded = SignedRing::load(&document_path, &signature_path, &[key], None, 1)
            .expect("valid signed root Ring");
        assert_eq!(loaded.document.ring_version, 1);
    }

    #[test]
    fn rejects_ring_rollback() {
        let ring = root_ring();
        let (_directory, document_path, signature_path, key) = fixture_files(&ring, [7; 32]);
        let error = SignedRing::load(&document_path, &signature_path, &[key], None, 2)
            .expect_err("rollback must fail");
        assert!(error.to_string().contains("minimum trusted version"));
    }

    #[test]
    fn rejects_invalid_signature() {
        let ring = root_ring();
        let (_directory, document_path, signature_path, key) = fixture_files(&ring, [7; 32]);
        fs::write(&signature_path, URL_SAFE_NO_PAD.encode([0_u8; 64])).expect("replace signature");
        assert!(SignedRing::load(&document_path, &signature_path, &[key], None, 1).is_err());
    }

    #[test]
    fn rejects_tampered_signed_timestamp() {
        let ring = root_ring();
        let (_directory, document_path, signature_path, key) = fixture_files(&ring, [7; 32]);
        let mut tampered = ring;
        tampered.signed_at_unix_ms += 1;
        tampered.ring_hash = tampered.computed_hash().expect("ring hash");
        fs::write(
            &document_path,
            serde_yaml::to_string(&tampered).expect("Ring YAML"),
        )
        .expect("write tampered Ring");
        let error = SignedRing::load(&document_path, &signature_path, &[key], None, 1)
            .expect_err("timestamp is signature-bound");
        assert!(error.to_string().contains("signature verification failed"));
    }

    #[test]
    fn rejects_unknown_or_retired_signing_key() {
        let ring = root_ring();
        let (_directory, document_path, signature_path, mut key) = fixture_files(&ring, [7; 32]);
        key.key_id = "some-other-key".to_owned();
        let error = SignedRing::load(&document_path, &signature_path, &[key], None, 1)
            .expect_err("unknown key must fail");
        assert!(error.to_string().contains("unknown or retired"));
    }

    #[test]
    fn key_validity_boundaries_are_inclusive() {
        let ring = root_ring();
        let (_directory, document_path, signature_path, mut key) = fixture_files(&ring, [7; 32]);
        key.not_after_unix_ms = ring.signed_at_unix_ms;
        SignedRing::load(&document_path, &signature_path, &[key.clone()], None, 1)
            .expect("upper boundary");
        key.not_before_unix_ms = ring.signed_at_unix_ms;
        SignedRing::load(&document_path, &signature_path, &[key], None, 1).expect("lower boundary");
    }

    #[test]
    fn rejects_signature_outside_key_validity() {
        let ring = root_ring();
        let (_directory, document_path, signature_path, mut key) = fixture_files(&ring, [7; 32]);
        key.not_before_unix_ms = ring.signed_at_unix_ms + 1;
        let error = SignedRing::load(&document_path, &signature_path, &[key], None, 1)
            .expect_err("early signature must fail");
        assert!(error.to_string().contains("outside key validity"));
    }

    #[test]
    fn accepts_valid_ring_and_key_rotation() {
        let parent = root_ring();
        let ring = rotated_ring(&parent);
        let (_directory, document_path, signature_path, key) = fixture_files(&ring, [8; 32]);
        let predecessor = TrustedRingPredecessor {
            ring_version: parent.ring_version,
            ring_hash: parent.ring_hash,
        };
        let loaded = SignedRing::load(
            &document_path,
            &signature_path,
            &[key],
            Some(&predecessor),
            2,
        )
        .expect("valid rotation");
        assert_eq!(loaded.document.ring_version, 2);
    }

    #[test]
    fn rejects_parent_version_discontinuity() {
        let parent = root_ring();
        let mut ring = rotated_ring(&parent);
        ring.parent_ring_version = Some(0);
        ring.ring_hash = ring.computed_hash().expect("ring hash");
        let (_directory, document_path, signature_path, key) = fixture_files(&ring, [8; 32]);
        let predecessor = TrustedRingPredecessor {
            ring_version: parent.ring_version,
            ring_hash: parent.ring_hash,
        };
        let error = SignedRing::load(
            &document_path,
            &signature_path,
            &[key],
            Some(&predecessor),
            2,
        )
        .expect_err("discontinuity must fail");
        assert!(error.to_string().contains("trusted predecessor version"));
    }

    #[test]
    fn rejects_wrong_parent_hash() {
        let parent = root_ring();
        let mut ring = rotated_ring(&parent);
        ring.parent_ring_hash = Some(format!("sha256:{}", "0".repeat(64)));
        ring.ring_hash = ring.computed_hash().expect("ring hash");
        let (_directory, document_path, signature_path, key) = fixture_files(&ring, [8; 32]);
        let predecessor = TrustedRingPredecessor {
            ring_version: parent.ring_version,
            ring_hash: parent.ring_hash,
        };
        let error = SignedRing::load(
            &document_path,
            &signature_path,
            &[key],
            Some(&predecessor),
            2,
        )
        .expect_err("wrong parent hash must fail");
        assert!(error.to_string().contains("parent hash"));
    }

    #[test]
    fn rejects_root_with_parent() {
        let mut ring = root_ring();
        ring.parent_ring_version = Some(0);
        ring.parent_ring_hash = Some(format!("sha256:{}", "0".repeat(64)));
        ring.ring_hash = ring.computed_hash().expect("ring hash");
        assert!(ring.validate(1, None).is_err());
    }

    #[test]
    fn rejects_single_region_topology() {
        let mut ring = root_ring();
        ring.nodes[1].region = ring.nodes[0].region.clone();
        ring.ring_hash = ring.computed_hash().expect("ring hash");
        assert!(ring.validate(1, None).is_err());
    }

    #[test]
    fn rejects_non_uniform_reserved_weights() {
        let mut ring = balanced_three_node_ring(100);
        ring.nodes[2].weight = 101;
        ring.ring_hash = ring.computed_hash().expect("ring hash");
        let error = ring
            .validate(1, None)
            .expect_err("non-uniform weights must fail");
        assert!(error.to_string().contains("reserved"));
        assert!(SignedRing::from_document(ring).is_err());
    }

    #[test]
    fn placement_ignores_uniform_reserved_weight_values() {
        let baseline = balanced_three_node_ring(100);
        let reserved = balanced_three_node_ring(7);
        for blob in [
            "/container/blob-a",
            "/container/blob-b",
            "/container/blob-c",
            "/container/blob-d",
        ] {
            let blob = logical_blob(blob);
            let baseline_ids = baseline
                .replicas_for(&blob)
                .expect("baseline placement")
                .into_iter()
                .map(|node| node.id.clone())
                .collect::<Vec<_>>();
            let reserved_ids = reserved
                .replicas_for(&blob)
                .expect("reserved placement")
                .into_iter()
                .map(|node| node.id.clone())
                .collect::<Vec<_>>();
            assert_eq!(reserved_ids, baseline_ids);
        }
    }

    #[test]
    fn cached_signed_ring_placement_is_deterministic_and_cross_region() {
        let ring = SignedRing::from_document(balanced_three_node_ring(7)).expect("signed ring");
        assert_eq!(
            ring.placement.points.len(),
            ring.document.nodes.len() * FIXED_VIRTUAL_NODES_PER_NODE
        );
        let blob = logical_blob("/container/blob");
        let first = ring.replicas_for(&blob).expect("placement");
        let second = ring.replicas_for(&blob).expect("placement");
        assert_eq!(first[0].id, second[0].id);
        assert_eq!(first[1].id, second[1].id);
        assert_ne!(first[0].region, first[1].region);
    }
}
