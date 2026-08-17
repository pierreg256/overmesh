use super::*;

pub fn verify_reconciliation_record(
    bytes: &[u8],
    signer: &dyn ManifestSigner,
    expected_ring_version: u64,
) -> Result<ReconciliationRecord> {
    let record = SignedDocument::<ReconciliationRecord>::from_bytes(bytes)
        .context("reconciliation record is not valid JSON")?;
    ensure!(
        record.payload.ring_version == expected_ring_version,
        "reconciliation record Ring version mismatch"
    );
    record
        .verify(
            SignatureDomain::ReconciliationRecord,
            &record.payload.signing_key_id,
            signer,
        )
        .context("reconciliation record signature validation failed")?;
    Ok(record.payload)
}

impl ReconcilerEngine {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn write_audit(
        &self,
        blob: Option<&LogicalBlobId>,
        head_object: &str,
        classification: ReconciliationClassification,
        action: ReconciliationRecordAction,
        reason: &str,
        source_replica: Option<&str>,
        target_replica: Option<&str>,
        token: &ControlToken,
    ) -> Result<()> {
        let record = self
            .signed_record(
                blob,
                head_object,
                classification,
                action,
                reason,
                source_replica,
                target_replica,
            )
            .await?;
        let bytes = record.canonical_bytes()?;
        let object_key = format!(
            "{AUDIT_PREFIX}{}-{}-{}.json",
            now_unix_ms(),
            head_hash(head_object)?,
            Uuid::new_v4()
        );
        for backend in self.target_backends(blob)? {
            put_immutable(
                backend.as_ref(),
                &object_key,
                bytes.clone(),
                "application/json",
                token,
            )
            .await?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn signed_record(
        &self,
        blob: Option<&LogicalBlobId>,
        head_object: &str,
        classification: ReconciliationClassification,
        action: ReconciliationRecordAction,
        reason: &str,
        source_replica: Option<&str>,
        target_replica: Option<&str>,
    ) -> Result<SignedDocument<ReconciliationRecord>> {
        Ok(SignedDocument::create(
            ReconciliationRecord {
                api_version: "overmesh.io/reconciliation/v1".to_owned(),
                blob: blob.map(|logical_blob| logical_blob.canonical().to_owned()),
                head_object: head_object.to_owned(),
                ring_version: self.ring.ring_version,
                observed_at_unix_ms: now_unix_ms(),
                classification,
                action,
                reason: reason.to_owned(),
                source_replica: source_replica.map(ToOwned::to_owned),
                target_replica: target_replica.map(ToOwned::to_owned),
                signing_key_id: self.signer.key_id().to_owned(),
            },
            SignatureDomain::ReconciliationRecord,
            self.signer.as_ref(),
        )
        .await?)
    }
}
