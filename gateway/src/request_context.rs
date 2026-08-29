use std::{
    collections::BTreeSet,
    future::Future,
    sync::{Arc, Mutex},
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tracing::{error, info};

const TELEMETRY_CHUNK_BYTES: usize = 8 * 1024;

tokio::task_local! {
    static CLIENT_REQUEST_FINGERPRINT: String;
    static REQUEST_EVENT_ID: String;
    static REQUEST_TELEMETRY: SharedRequestTelemetry;
}

#[derive(Debug)]
pub(crate) struct BackendRequestRecord {
    pub backend_id: String,
    pub operation: &'static str,
    pub object_class: &'static str,
    pub status: u16,
    pub response_headers_duration_us: u64,
    pub transport_success: bool,
}

#[derive(Debug)]
pub(crate) struct RequestTelemetry {
    client_request_fingerprint: String,
    request_event_id: String,
    backend_requests: Mutex<Vec<BackendRequestRecord>>,
}

pub(crate) type SharedRequestTelemetry = Arc<RequestTelemetry>;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BackendRequestBatch<'a> {
    schema_version: u8,
    client_request_fingerprint: &'a str,
    request_event_id: &'a str,
    backends: Vec<&'a str>,
    operations: Vec<&'a str>,
    object_classes: Vec<&'a str>,
    records: Vec<(usize, usize, usize, u16, u64, bool)>,
}

impl RequestTelemetry {
    fn record(&self, record: BackendRequestRecord) {
        self.backend_requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(record);
    }

    fn encoded_backend_requests(&self) -> Result<Vec<u8>, serde_json::Error> {
        let records = self
            .backend_requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let backends = records
            .iter()
            .map(|record| record.backend_id.as_str())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let operations = records
            .iter()
            .map(|record| record.operation)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let object_classes = records
            .iter()
            .map(|record| record.object_class)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let encoded_records = records
            .iter()
            .map(|record| {
                (
                    backends
                        .binary_search(&record.backend_id.as_str())
                        .expect("backend dictionary contains every record"),
                    operations
                        .binary_search(&record.operation)
                        .expect("operation dictionary contains every record"),
                    object_classes
                        .binary_search(&record.object_class)
                        .expect("object-class dictionary contains every record"),
                    record.status,
                    record.response_headers_duration_us,
                    record.transport_success,
                )
            })
            .collect();
        serde_json::to_vec(&BackendRequestBatch {
            schema_version: 1,
            client_request_fingerprint: &self.client_request_fingerprint,
            request_event_id: &self.request_event_id,
            backends,
            operations,
            object_classes,
            records: encoded_records,
        })
    }
}

impl Drop for RequestTelemetry {
    fn drop(&mut self) {
        let record_total = self
            .backend_requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        if record_total == 0 {
            return;
        }
        let payload = match self.encoded_backend_requests() {
            Ok(payload) => payload,
            Err(error) => {
                error!(
                    event = "overmesh_backend_request_batch_error",
                    request_event_id = %self.request_event_id,
                    client_request_fingerprint = %self.client_request_fingerprint,
                    error = %error,
                    "Overmesh backend request batch serialization failed"
                );
                return;
            }
        };
        let chunk_count = payload.len().div_ceil(TELEMETRY_CHUNK_BYTES);
        for (chunk_index, chunk) in payload.chunks(TELEMETRY_CHUNK_BYTES).enumerate() {
            let payload_base64 = URL_SAFE_NO_PAD.encode(chunk);
            info!(
                event = "overmesh_backend_request_batch",
                request_event_id = %self.request_event_id,
                client_request_fingerprint = %self.client_request_fingerprint,
                chunk_index,
                chunk_count,
                record_total,
                payload_base64,
                "Overmesh backend request batch emitted"
            );
        }
    }
}

pub(crate) fn request_telemetry(
    client_request_fingerprint: String,
    request_event_id: String,
) -> SharedRequestTelemetry {
    Arc::new(RequestTelemetry {
        client_request_fingerprint,
        request_event_id,
        backend_requests: Mutex::new(Vec::new()),
    })
}

pub async fn scope<T>(telemetry: SharedRequestTelemetry, future: impl Future<Output = T>) -> T {
    let fingerprint = telemetry.client_request_fingerprint.clone();
    let request_event_id = telemetry.request_event_id.clone();
    CLIENT_REQUEST_FINGERPRINT
        .scope(
            fingerprint,
            REQUEST_EVENT_ID.scope(request_event_id, REQUEST_TELEMETRY.scope(telemetry, future)),
        )
        .await
}

pub fn current_client_request_fingerprint() -> String {
    CLIENT_REQUEST_FINGERPRINT
        .try_with(Clone::clone)
        .unwrap_or_else(|_| "missing".to_owned())
}

pub fn current_request_event_id() -> String {
    REQUEST_EVENT_ID
        .try_with(Clone::clone)
        .unwrap_or_else(|_| "missing".to_owned())
}

pub(crate) fn current_request_telemetry() -> Option<SharedRequestTelemetry> {
    REQUEST_TELEMETRY.try_with(Arc::clone).ok()
}

pub(crate) fn record_backend_request(record: BackendRequestRecord) -> bool {
    REQUEST_TELEMETRY
        .try_with(|telemetry| telemetry.record(record))
        .is_ok()
}

pub fn client_request_fingerprint(request_id: &str) -> String {
    let digest = Sha256::digest(request_id.as_bytes());
    hex::encode(&digest[..8])
}

pub fn request_target_fingerprint(request_target: &str) -> String {
    let digest = Sha256::digest(request_target.as_bytes());
    hex::encode(&digest[..8])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_request_fingerprints_are_stable_and_redacted() {
        let fingerprint = client_request_fingerprint("caller-request-id");
        assert_eq!(fingerprint, "adc990656428ce0a");
        assert!(!fingerprint.contains("caller"));
    }

    #[test]
    fn request_target_fingerprints_are_stable_and_redacted() {
        let fingerprint =
            request_target_fingerprint("/container/private/blob?comp=block&blockid=secret");
        assert_eq!(fingerprint, "9141dee529ee2571");
        assert!(!fingerprint.contains("private"));
        assert!(!fingerprint.contains("secret"));
    }
}
