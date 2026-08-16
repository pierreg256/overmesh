use super::*;

pub(crate) async fn ensure_not_quarantined(
    primary: &dyn ReplicaBackend,
    secondary: &dyn ReplicaBackend,
    path_hash: &str,
    control_token: &ControlToken,
    signer: &dyn ManifestSigner,
) -> Result<(), CommitError> {
    let object_key = format!("quarantine/{path_hash}.json");
    let (primary_record, secondary_record) = tokio::try_join!(
        primary.control_get_object(&object_key, control_token),
        secondary.control_get_object(&object_key, control_token)
    )?;
    if let Some(value) = [primary_record, secondary_record]
        .into_iter()
        .flatten()
        .next()
    {
        let signed = SignedDocument::<ReconciliationRecord>::from_bytes(&value.bytes)?;
        if signed.payload.action != ReconciliationRecordAction::Quarantined {
            return Err(CommitError::VerificationFailed);
        }
        signed.verify(
            SignatureDomain::ReconciliationRecord,
            &signed.payload.signing_key_id,
            signer,
        )?;
        return Err(CommitError::Quarantined);
    }
    Ok(())
}
