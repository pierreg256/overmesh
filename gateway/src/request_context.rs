use std::future::Future;

use sha2::{Digest, Sha256};

tokio::task_local! {
    static CLIENT_REQUEST_FINGERPRINT: String;
}

pub async fn scope<T>(fingerprint: String, future: impl Future<Output = T>) -> T {
    CLIENT_REQUEST_FINGERPRINT.scope(fingerprint, future).await
}

pub fn current_client_request_fingerprint() -> String {
    CLIENT_REQUEST_FINGERPRINT
        .try_with(Clone::clone)
        .unwrap_or_else(|_| "missing".to_owned())
}

pub fn client_request_fingerprint(request_id: &str) -> String {
    let digest = Sha256::digest(request_id.as_bytes());
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
}
