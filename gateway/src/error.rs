use std::time::SystemTime;

use axum::{
    body::Body,
    response::{IntoResponse, Response},
};
use http::{
    HeaderValue, StatusCode,
    header::{CONTENT_TYPE, DATE},
};
use uuid::Uuid;

use crate::app::SUPPORTED_STORAGE_VERSION;

#[derive(Debug, Clone)]
pub struct StorageError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
}

impl StorageError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    pub fn authentication_failed(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "AuthenticationFailed", message)
    }

    pub fn key_authentication_not_permitted() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "KeyBasedAuthenticationNotPermitted",
            "Key-based authentication is not permitted on this Overmesh endpoint.",
        )
    }

    pub fn sas_not_permitted() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "AuthenticationFailed",
            "Shared access signatures are not permitted on this Overmesh endpoint.",
        )
    }

    pub fn missing_header(name: &str) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "MissingRequiredHeader",
            format!("A required HTTP header was not specified: {name}."),
        )
    }

    pub fn invalid_header(name: &str) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "InvalidHeaderValue",
            format!("The value for HTTP header {name} is invalid."),
        )
    }

    pub fn stable_request_id_required() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "MissingRequiredHeader",
            "A stable request ID is required for writes. Specify x-overmesh-write-id or \
             x-ms-client-request-id and reuse the same value when retrying the request.",
        )
    }

    pub fn unsupported_method() -> Self {
        Self::new(
            StatusCode::METHOD_NOT_ALLOWED,
            "UnsupportedHttpVerb",
            "The specified HTTP method is not supported.",
        )
    }

    pub fn feature_not_supported() -> Self {
        Self::new(
            StatusCode::NOT_IMPLEMENTED,
            "FeatureNotSupported",
            "The authenticated Blob operation is recognized but is not implemented by this milestone.",
        )
    }

    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "InvalidRequest", message)
    }

    pub fn condition_not_met() -> Self {
        Self::new(
            StatusCode::PRECONDITION_FAILED,
            "ConditionNotMet",
            "The condition specified using HTTP conditional headers was not met.",
        )
    }

    pub fn authorization_permission_mismatch() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "AuthorizationPermissionMismatch",
            "This request is not authorized to perform this operation.",
        )
    }

    pub fn blob_not_found() -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "BlobNotFound",
            "The specified blob does not exist.",
        )
    }

    pub fn invalid_range() -> Self {
        Self::new(
            StatusCode::RANGE_NOT_SATISFIABLE,
            "InvalidRange",
            "The requested range is not satisfiable.",
        )
    }

    pub fn invalid_operation(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "InvalidOperation", message)
    }

    pub fn lease_conflict() -> Self {
        Self::new(
            StatusCode::CONFLICT,
            "LeaseAlreadyPresent",
            "A write is already in progress for this blob.",
        )
    }

    pub fn blob_quarantined() -> Self {
        Self::new(
            StatusCode::CONFLICT,
            "BlobQuarantined",
            "The logical blob is quarantined and requires administrator-authorized recovery.",
        )
    }

    pub fn server_busy(message: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, "ServerBusy", message)
    }
}

impl IntoResponse for StorageError {
    fn into_response(self) -> Response {
        let request_id = Uuid::new_v4().to_string();
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>\
             <Error><Code>{}</Code><Message>{}</Message><RequestId>{}</RequestId></Error>",
            self.code,
            escape_xml(&self.message),
            request_id
        );
        let mut response = Response::new(Body::from(body));
        *response.status_mut() = self.status;
        let headers = response.headers_mut();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/xml; charset=utf-8"),
        );
        headers.insert("x-ms-error-code", HeaderValue::from_static(self.code));
        headers.insert(
            "x-ms-request-id",
            HeaderValue::from_str(&request_id).expect("UUID is a valid header value"),
        );
        headers.insert(
            "x-ms-version",
            HeaderValue::from_static(SUPPORTED_STORAGE_VERSION),
        );
        headers.insert(
            DATE,
            HeaderValue::from_str(&httpdate::fmt_http_date(SystemTime::now()))
                .expect("HTTP date is a valid header value"),
        );
        response
    }
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
