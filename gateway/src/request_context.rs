use std::future::Future;

use sha2::{Digest, Sha256};

tokio::task_local! {
    static CLIENT_REQUEST_FINGERPRINT: String;
    static REQUEST_EVENT_ID: String;
}

pub async fn scope<T>(
    fingerprint: String,
    request_event_id: String,
    future: impl Future<Output = T>,
) -> T {
    CLIENT_REQUEST_FINGERPRINT
        .scope(
            fingerprint,
            REQUEST_EVENT_ID.scope(request_event_id, future),
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
