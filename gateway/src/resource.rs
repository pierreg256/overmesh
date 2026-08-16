use std::fmt;

use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LogicalResourceError {
    #[error("the logical account name is empty")]
    EmptyAccount,
    #[error("the request path must identify a container and blob")]
    MissingBlob,
    #[error("the request path contains an invalid percent escape")]
    InvalidPercentEscape,
    #[error("the request path is not valid UTF-8 after percent decoding")]
    InvalidUtf8,
    #[error("the container name is not valid for Azure Blob Storage")]
    InvalidContainer,
    #[error("the blob name is not valid for Azure Blob Storage")]
    InvalidBlob,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct LogicalBlobId {
    account: String,
    container: String,
    blob: String,
    canonical: String,
}

impl LogicalBlobId {
    pub fn parse(account: &str, request_path: &str) -> Result<Self, LogicalResourceError> {
        if account.is_empty() {
            return Err(LogicalResourceError::EmptyAccount);
        }
        let path = request_path
            .strip_prefix('/')
            .ok_or(LogicalResourceError::MissingBlob)?;
        let (encoded_container, encoded_blob) = path
            .split_once('/')
            .ok_or(LogicalResourceError::MissingBlob)?;
        if encoded_container.is_empty() || encoded_blob.is_empty() {
            return Err(LogicalResourceError::MissingBlob);
        }
        let container = percent_decode(encoded_container)?;
        let blob = percent_decode(encoded_blob)?;
        if !valid_container_name(&container) {
            return Err(LogicalResourceError::InvalidContainer);
        }
        if blob.is_empty() || blob.chars().count() > 1_024 || blob.chars().any(char::is_control) {
            return Err(LogicalResourceError::InvalidBlob);
        }
        let canonical = format!(
            "/{}/{}/{}",
            encode_path_component(account),
            encode_path_component(&container),
            encode_blob_path(&blob)
        );
        Ok(Self {
            account: account.to_owned(),
            container,
            blob,
            canonical,
        })
    }

    pub fn from_canonical(account: &str, canonical: &str) -> Result<Self, LogicalResourceError> {
        let account_prefix = format!("/{}", encode_path_component(account));
        let path = canonical
            .strip_prefix(&account_prefix)
            .filter(|value| value.starts_with('/'))
            .ok_or(LogicalResourceError::EmptyAccount)?;
        Self::parse(account, path)
    }

    pub fn account(&self) -> &str {
        &self.account
    }

    pub fn parse_container_path(request_path: &str) -> Result<String, LogicalResourceError> {
        let encoded = request_path
            .strip_prefix('/')
            .ok_or(LogicalResourceError::InvalidContainer)?;
        if encoded.is_empty() || encoded.contains('/') {
            return Err(LogicalResourceError::InvalidContainer);
        }
        let container = percent_decode(encoded)?;
        if !valid_container_name(&container) {
            return Err(LogicalResourceError::InvalidContainer);
        }
        Ok(container)
    }

    pub fn container(&self) -> &str {
        &self.container
    }

    pub fn blob(&self) -> &str {
        &self.blob
    }

    pub fn canonical(&self) -> &str {
        &self.canonical
    }

    pub fn path_hash(&self) -> String {
        hex::encode(Sha256::digest(self.canonical.as_bytes()))
    }

    pub fn immutable_content_key(&self, content_id: &str) -> String {
        format!(".overmesh/objects/{}/{content_id}", self.path_hash(),)
    }
}

pub fn stable_component(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

impl fmt::Debug for LogicalBlobId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogicalBlobId")
            .field("canonical", &self.canonical)
            .finish()
    }
}

pub(crate) fn encode_path_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(*byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

pub(crate) fn encode_blob_path(value: &str) -> String {
    value
        .split('/')
        .map(encode_path_component)
        .collect::<Vec<_>>()
        .join("/")
}

fn percent_decode(value: &str) -> Result<String, LogicalResourceError> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(LogicalResourceError::InvalidPercentEscape);
            }
            let high =
                hex_value(bytes[index + 1]).ok_or(LogicalResourceError::InvalidPercentEscape)?;
            let low =
                hex_value(bytes[index + 2]).ok_or(LogicalResourceError::InvalidPercentEscape)?;
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| LogicalResourceError::InvalidUtf8)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn valid_container_name(value: &str) -> bool {
    (3..=63).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
        && !value.contains("--")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_account_aware_canonical_identity() {
        let blob = LogicalBlobId::parse("account-a", "/photos/a%20b%2F~.jpg").expect("valid blob");
        assert_eq!(blob.canonical(), "/account-a/photos/a%20b/~.jpg");
        assert_eq!(blob.container(), "photos");
        assert_eq!(blob.blob(), "a b/~.jpg");
    }

    #[test]
    fn equivalent_wire_encodings_have_one_identity() {
        let literal = LogicalBlobId::parse("account-a", "/photos/a~b/c").expect("valid blob");
        let encoded =
            LogicalBlobId::parse("account-a", "/photos/a%7eb%2fc").expect("valid encoded blob");
        assert_eq!(literal, encoded);
        assert_eq!(literal.path_hash(), encoded.path_hash());
    }

    #[test]
    fn preserves_azure_supported_empty_blob_segments() {
        let blob = LogicalBlobId::parse("account-a", "/photos/a//b/").expect("valid blob");
        assert_eq!(blob.blob(), "a//b/");
        assert_eq!(blob.canonical(), "/account-a/photos/a//b/");
    }

    #[test]
    fn rejects_invalid_percent_escapes_and_utf8() {
        assert!(matches!(
            LogicalBlobId::parse("account-a", "/photos/a%2"),
            Err(LogicalResourceError::InvalidPercentEscape)
        ));
        assert!(matches!(
            LogicalBlobId::parse("account-a", "/photos/%FF"),
            Err(LogicalResourceError::InvalidUtf8)
        ));
    }
}
