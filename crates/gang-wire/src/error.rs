//! AIP-193 error envelope: the `{"error": …}` body of every non-2xx answer on `/v1/**`.
//!
//! Responses never reject unknown fields, so a client built against an older schema still reads
//! errors from a newer server: unknown detail types round-trip as [`Detail::Unrecognized`], and
//! unknown reasons and status codes are kept as text.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use serde::de::{self, DeserializeOwned};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};

/// Domain stamped on every [`ErrorInfo`] Gang emits.
pub const ERROR_DOMAIN: &str = "gang";

const ERROR_INFO_TYPE: &str = "type.googleapis.com/google.rpc.ErrorInfo";
const BAD_REQUEST_TYPE: &str = "type.googleapis.com/google.rpc.BadRequest";
const RETRY_INFO_TYPE: &str = "type.googleapis.com/google.rpc.RetryInfo";

wire_enum! {
    /// `google.rpc.Code` names that Gang answers with.
    pub enum Code {
        /// The request is malformed; HTTP 400.
        InvalidArgument = "INVALID_ARGUMENT",
        /// The request carries no valid credentials; HTTP 401.
        Unauthenticated = "UNAUTHENTICATED",
        /// The credentials may not perform the request; HTTP 403.
        PermissionDenied = "PERMISSION_DENIED",
        /// The named resource does not exist; HTTP 404.
        NotFound = "NOT_FOUND",
        /// The caller sent too many requests; HTTP 429.
        ResourceExhausted = "RESOURCE_EXHAUSTED",
        /// The server cannot serve the request now; HTTP 503.
        Unavailable = "UNAVAILABLE",
        /// The server did not answer in time; HTTP 504.
        DeadlineExceeded = "DEADLINE_EXCEEDED",
        /// The server failed unexpectedly; HTTP 500.
        Internal = "INTERNAL",
    }
}

impl Code {
    /// The HTTP status code the code is answered with; a code this version does not know
    /// answers as 500.
    #[must_use]
    pub const fn http(&self) -> u16 {
        match self {
            Self::InvalidArgument => 400,
            Self::Unauthenticated => 401,
            Self::PermissionDenied => 403,
            Self::NotFound => 404,
            Self::ResourceExhausted => 429,
            Self::Unavailable => 503,
            Self::DeadlineExceeded => 504,
            Self::Internal | Self::Unrecognized(_) => 500,
        }
    }
}

/// Declares [`ErrorReason`] and its catalog from one row per reason: the variant with its doc,
/// its wire name, its generic message, the `google.rpc.Code` it is answered with, and the keys
/// of its metadata. Rows are listed in catalog order.
macro_rules! reasons {
    (@unit $variant:ident) => {
        ()
    };
    (
        $(#[$enum_meta:meta])*
        pub enum ErrorReason {
            $(
                $(#[$variant_meta:meta])*
                $variant:ident = $name:literal, $message:literal, $code:ident, [$($key:literal),* $(,)?];
            )*
        }
    ) => {
        $(#[$enum_meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum ErrorReason {
            $(
                $(#[$variant_meta])*
                $variant,
            )*
        }

        impl ErrorReason {
            /// Every reason, in catalog order.
            pub const ALL: [Self; <[()]>::len(&[$(reasons!(@unit $variant)),*])] = [
                $(Self::$variant,)*
            ];

            /// Wire spelling of the reason, `UPPER_SNAKE`.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $name,)*
                }
            }

            /// Generic, caller-facing message for the reason. Carries no detail about the
            /// request or the server, so it is safe to return to any caller.
            #[must_use]
            pub const fn message(self) -> &'static str {
                match self {
                    $(Self::$variant => $message,)*
                }
            }

            /// `google.rpc.Code` that the reason is answered with.
            #[must_use]
            pub const fn status(self) -> Code {
                match self {
                    $(Self::$variant => Code::$code,)*
                }
            }

            /// Keys of the [`ErrorInfo::metadata`] the reason carries, `lowerCamelCase`.
            #[must_use]
            pub const fn metadata_keys(self) -> &'static [&'static str] {
                match self {
                    $(Self::$variant => &[$($key),*],)*
                }
            }
        }

        impl std::str::FromStr for ErrorReason {
            type Err = UnknownErrorReason;

            fn from_str(name: &str) -> Result<Self, Self::Err> {
                match name {
                    $($name => Ok(Self::$variant),)*
                    _ => Err(UnknownErrorReason),
                }
            }
        }
    };
}

reasons! {
    /// Stable machine-readable cause of an error, carried in [`ErrorInfo::reason`].
    pub enum ErrorReason {
        /// The body or parameters are malformed, incomplete or of the wrong type.
        MalformedRequest = "MALFORMED_REQUEST", "invalid request", InvalidArgument, [];
        /// The request body exceeds the size limit.
        RequestTooLarge = "REQUEST_TOO_LARGE", "request body too large", InvalidArgument, [];
        /// The page token is not one the server issued.
        InvalidPageToken = "INVALID_PAGE_TOKEN", "invalid page token", InvalidArgument, [];
        /// The request names an identity provider the server does not configure.
        UnknownProvider = "UNKNOWN_PROVIDER", "unknown identity provider", InvalidArgument, ["provider"];
        /// The identity provider rejected the submitted proof of identity.
        ProofRejected = "PROOF_REJECTED", "proof rejected", Unauthenticated, [];
        /// The request presents no credentials.
        CredentialsMissing = "CREDENTIALS_MISSING", "authentication required", Unauthenticated, [];
        /// The request presents credentials that are expired, malformed or for another audience.
        CredentialsInvalid = "CREDENTIALS_INVALID", "invalid credentials", Unauthenticated, [];
        /// The issuance policy refuses the request.
        PolicyDenied = "POLICY_DENIED", "policy denied", PermissionDenied, [];
        /// The caller's principal is revoked.
        PrincipalRevoked = "PRINCIPAL_REVOKED", "principal revoked", PermissionDenied, [];
        /// The caller lacks a permission the operation requires.
        MissingPermission = "MISSING_PERMISSION", "missing permission", PermissionDenied, ["permission"];
        /// No route matches the method and path.
        RouteNotFound = "ROUTE_NOT_FOUND", "no such route", NotFound, ["method", "path"];
        /// No certificate with the requested serial exists.
        CertificateNotFound = "CERTIFICATE_NOT_FOUND", "certificate not found", NotFound, ["certificate"];
        /// No principal with the requested name exists.
        PrincipalNotFound = "PRINCIPAL_NOT_FOUND", "principal not found", NotFound, ["principal"];
        /// The caller sent too many requests to a route that is open to anyone.
        RateLimited = "RATE_LIMITED", "too many requests", ResourceExhausted, [];
        /// A backing dependency (ledger, identity provider or signer) is unavailable.
        DependencyUnavailable = "DEPENDENCY_UNAVAILABLE", "service unavailable", Unavailable, [];
        /// The request did not finish within the server's deadline.
        RequestTimedOut = "REQUEST_TIMED_OUT", "request timed out", DeadlineExceeded, [];
        /// The server failed unexpectedly; the cause is in its logs.
        InternalError = "INTERNAL_ERROR", "internal error", Internal, [];
    }
}

/// An error reason not recognized by this version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("unknown error reason")]
pub struct UnknownErrorReason;

impl ErrorReason {
    /// HTTP status code the reason is answered with.
    #[must_use]
    pub fn http(self) -> u16 {
        self.status().http()
    }
}

impl fmt::Display for ErrorReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The `google.rpc.ErrorInfo` detail: why the request failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorInfo {
    /// `UPPER_SNAKE` reason. Kept as text so a reason from a newer server still parses.
    pub reason: String,
    /// Domain of the reason; always [`ERROR_DOMAIN`] for Gang.
    pub domain: String,
    /// Dynamic values behind the reason, keyed `lowerCamelCase`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

impl crate::Known for ErrorInfo {
    fn is_known(&self) -> bool {
        self.reason.parse::<ErrorReason>().is_ok() && crate::Known::is_known(&self.metadata)
    }
}

/// One field-level problem in a [`BadRequest`].
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldViolation {
    /// Name of the offending field, `lowerCamelCase`.
    pub field: String,
    /// Generic explanation of what is wrong with the field.
    pub description: String,
}

/// The `google.rpc.BadRequest` detail: the request fields that failed validation.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BadRequest {
    /// Every field that failed validation, in the order they were found.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub field_violations: Vec<FieldViolation>,
}

/// The `google.rpc.RetryInfo` detail: how long the caller should wait before it retries.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetryInfo {
    /// Delay before the retry, as a Google `Duration` string such as `3s`.
    #[serde(with = "crate::duration")]
    pub retry_delay: Duration,
}

/// One entry of [`Status::details`].
///
/// Serialized as an object whose `@type` names the detail type. Types Gang does not model are
/// kept as the raw JSON value, so they round-trip unchanged.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq)]
pub enum Detail {
    /// The `google.rpc.ErrorInfo` detail.
    ErrorInfo(ErrorInfo),
    /// The `google.rpc.BadRequest` detail.
    BadRequest(BadRequest),
    /// The `google.rpc.RetryInfo` detail.
    RetryInfo(RetryInfo),
    /// A detail of a type this crate does not model, kept verbatim.
    Unrecognized(Value),
}

/// Borrowed, tagged view of the modelled details, so serialization writes `@type` first.
#[derive(Serialize)]
#[serde(tag = "@type")]
enum DetailRef<'a> {
    #[serde(rename = "type.googleapis.com/google.rpc.ErrorInfo")]
    ErrorInfo(&'a ErrorInfo),
    #[serde(rename = "type.googleapis.com/google.rpc.BadRequest")]
    BadRequest(&'a BadRequest),
    #[serde(rename = "type.googleapis.com/google.rpc.RetryInfo")]
    RetryInfo(&'a RetryInfo),
}

impl Serialize for Detail {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::ErrorInfo(info) => DetailRef::ErrorInfo(info).serialize(serializer),
            Self::BadRequest(bad_request) => {
                DetailRef::BadRequest(bad_request).serialize(serializer)
            }
            Self::RetryInfo(retry_info) => DetailRef::RetryInfo(retry_info).serialize(serializer),
            Self::Unrecognized(value) => value.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Detail {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        let Value::Object(map) = value else {
            return Ok(Self::Unrecognized(value));
        };
        let detail_type = map.get("@type").and_then(Value::as_str).map(str::to_owned);
        match detail_type.as_deref() {
            Some(ERROR_INFO_TYPE) => typed_detail(map, Self::ErrorInfo),
            Some(BAD_REQUEST_TYPE) => typed_detail(map, Self::BadRequest),
            Some(RETRY_INFO_TYPE) => typed_detail(map, Self::RetryInfo),
            _ => Ok(Self::Unrecognized(Value::Object(map))),
        }
    }
}

/// Builds a modelled detail from the object `map` with its `@type` key removed.
fn typed_detail<T: DeserializeOwned, E: de::Error>(
    mut map: Map<String, Value>,
    wrap: fn(T) -> Detail,
) -> Result<Detail, E> {
    map.remove("@type");
    serde_json::from_value(Value::Object(map))
        .map(wrap)
        .map_err(E::custom)
}

/// The `google.rpc.Status` object: the body of the error envelope.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Status {
    /// HTTP status code of the answer.
    pub code: u16,
    /// Generic, human-readable message. Never carries secrets or internal causes.
    pub message: String,
    /// `google.rpc.Code` name that corresponds to [`code`](Self::code).
    pub status: Code,
    /// Structured details; [`Detail::ErrorInfo`] appears exactly once for Gang errors.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub details: Vec<Detail>,
}

/// The `{"error": …}` envelope of every non-2xx answer on `/v1/**`.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorResponse {
    /// The error itself.
    pub error: Status,
}

impl Status {
    /// Status for `reason`, with `message` and one [`ErrorInfo`] carrying `metadata`.
    ///
    /// The HTTP code and `google.rpc.Code` follow from the reason.
    #[must_use]
    pub fn new(
        reason: ErrorReason,
        message: impl Into<String>,
        metadata: impl IntoIterator<Item = (&'static str, String)>,
    ) -> Self {
        let metadata = metadata
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect();
        Self {
            code: reason.http(),
            message: message.into(),
            status: reason.status(),
            details: vec![Detail::ErrorInfo(ErrorInfo {
                reason: reason.as_str().to_owned(),
                domain: ERROR_DOMAIN.to_owned(),
                metadata,
            })],
        }
    }

    /// Adds a field-level problem to the [`BadRequest`] detail, creating it on first use.
    #[must_use]
    pub fn with_field_violation(
        mut self,
        field: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        let violation = FieldViolation {
            field: field.into(),
            description: description.into(),
        };
        if let Some(bad_request) = self.details.iter_mut().find_map(|detail| match detail {
            Detail::BadRequest(bad_request) => Some(bad_request),
            _ => None,
        }) {
            bad_request.field_violations.push(violation);
        } else {
            self.details.push(Detail::BadRequest(BadRequest {
                field_violations: vec![violation],
            }));
        }
        self
    }

    /// The first [`ErrorInfo`] detail, if any.
    #[must_use]
    pub fn error_info(&self) -> Option<&ErrorInfo> {
        self.details.iter().find_map(|detail| match detail {
            Detail::ErrorInfo(info) => Some(info),
            _ => None,
        })
    }

    /// The reason named by [`error_info`](Self::error_info), when this crate knows it.
    #[must_use]
    pub fn reason(&self) -> Option<ErrorReason> {
        self.error_info().and_then(|info| info.reason.parse().ok())
    }

    /// Adds a retry hint: a [`RetryInfo`] detail appended after the existing details.
    #[must_use]
    pub fn with_retry_delay(mut self, delay: Duration) -> Self {
        self.details
            .push(Detail::RetryInfo(RetryInfo { retry_delay: delay }));
        self
    }

    /// The delay of the first [`RetryInfo`] detail, if any.
    #[must_use]
    pub fn retry_delay(&self) -> Option<Duration> {
        self.details.iter().find_map(|detail| match detail {
            Detail::RetryInfo(info) => Some(info.retry_delay),
            _ => None,
        })
    }
}

impl From<Status> for ErrorResponse {
    fn from(error: Status) -> Self {
        Self { error }
    }
}

impl From<ErrorReason> for Status {
    /// A status for a reason that carries no metadata: its generic message and one `ErrorInfo`
    /// without metadata. Reasons that carry metadata keys are built with `Status::new`.
    fn from(reason: ErrorReason) -> Self {
        Self::new(reason, reason.message(), [])
    }
}

/// Exact body served when an internal error occurs and no other body can be built.
///
/// It is the serialized form of `ErrorResponse::from(Status::new(InternalError, "internal
/// error", []))`; a test pins the two together.
pub const INTERNAL_ERROR_BODY: &str = r#"{"error":{"code":500,"message":"internal error","status":"INTERNAL","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"INTERNAL_ERROR","domain":"gang"}]}}"#;

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn unknown_detail_and_reason_are_not_known() {
        use crate::Known;

        let base = json!({"error": {"code": 400, "message": "invalid request", "status": "INVALID_ARGUMENT",
            "details": [{"@type": "type.googleapis.com/google.rpc.ErrorInfo",
                "reason": "MALFORMED_REQUEST", "domain": "gang"}]}});
        let known: ErrorResponse =
            serde_json::from_value(base.clone()).expect("known error parses");
        assert!(known.is_known());
        let mut unknown_detail = base.clone();
        unknown_detail["error"]["details"][0]["@type"] = json!("type.example/Unknown");
        let unknown_detail: ErrorResponse =
            serde_json::from_value(unknown_detail).expect("unknown detail parses");
        assert!(!unknown_detail.is_known());
        let mut unknown_reason = base;
        unknown_reason["error"]["details"][0]["reason"] = json!("TYPO");
        let unknown_reason: ErrorResponse =
            serde_json::from_value(unknown_reason).expect("unknown reason parses");
        assert!(!unknown_reason.is_known());
    }

    #[test]
    fn all_reasons_are_distinct_upper_snake_and_round_trip_by_name() {
        let names: std::collections::BTreeSet<&str> = ErrorReason::ALL
            .iter()
            .map(|reason| reason.as_str())
            .collect();
        assert_eq!(names.len(), 17);
        for reason in ErrorReason::ALL {
            let name = reason.as_str();
            assert!(
                name.chars().all(|c| c.is_ascii_uppercase() || c == '_'),
                "{name} is not UPPER_SNAKE"
            );
            assert_eq!(name.parse::<ErrorReason>(), Ok(reason));
            assert_eq!(reason.to_string(), name);
        }
        assert!("NO_SUCH_REASON".parse::<ErrorReason>().is_err());
    }

    #[test]
    fn messages_are_distinct() {
        let messages: std::collections::BTreeSet<&str> = ErrorReason::ALL
            .iter()
            .map(|reason| reason.message())
            .collect();
        assert_eq!(messages.len(), ErrorReason::ALL.len());
    }

    #[test]
    fn http_and_status_follow_the_catalog() {
        let table = [
            (ErrorReason::MalformedRequest, 400, Code::InvalidArgument),
            (ErrorReason::RequestTooLarge, 400, Code::InvalidArgument),
            (ErrorReason::InvalidPageToken, 400, Code::InvalidArgument),
            (ErrorReason::UnknownProvider, 400, Code::InvalidArgument),
            (ErrorReason::ProofRejected, 401, Code::Unauthenticated),
            (ErrorReason::CredentialsMissing, 401, Code::Unauthenticated),
            (ErrorReason::CredentialsInvalid, 401, Code::Unauthenticated),
            (ErrorReason::PolicyDenied, 403, Code::PermissionDenied),
            (ErrorReason::PrincipalRevoked, 403, Code::PermissionDenied),
            (ErrorReason::MissingPermission, 403, Code::PermissionDenied),
            (ErrorReason::RouteNotFound, 404, Code::NotFound),
            (ErrorReason::CertificateNotFound, 404, Code::NotFound),
            (ErrorReason::PrincipalNotFound, 404, Code::NotFound),
            (ErrorReason::RateLimited, 429, Code::ResourceExhausted),
            (ErrorReason::DependencyUnavailable, 503, Code::Unavailable),
            (ErrorReason::RequestTimedOut, 504, Code::DeadlineExceeded),
            (ErrorReason::InternalError, 500, Code::Internal),
        ];
        assert_eq!(table.len(), ErrorReason::ALL.len());
        for (reason, http, status) in table {
            assert_eq!(reason.http(), http, "{reason}");
            assert_eq!(reason.status(), status, "{reason}");
        }
    }

    #[test]
    fn metadata_keys_follow_the_catalog() {
        let table: [(ErrorReason, &[&str]); 17] = [
            (ErrorReason::MalformedRequest, &[]),
            (ErrorReason::RequestTooLarge, &[]),
            (ErrorReason::InvalidPageToken, &[]),
            (ErrorReason::UnknownProvider, &["provider"]),
            (ErrorReason::ProofRejected, &[]),
            (ErrorReason::CredentialsMissing, &[]),
            (ErrorReason::CredentialsInvalid, &[]),
            (ErrorReason::PolicyDenied, &[]),
            (ErrorReason::PrincipalRevoked, &[]),
            (ErrorReason::MissingPermission, &["permission"]),
            (ErrorReason::RouteNotFound, &["method", "path"]),
            (ErrorReason::CertificateNotFound, &["certificate"]),
            (ErrorReason::PrincipalNotFound, &["principal"]),
            (ErrorReason::RateLimited, &[]),
            (ErrorReason::DependencyUnavailable, &[]),
            (ErrorReason::RequestTimedOut, &[]),
            (ErrorReason::InternalError, &[]),
        ];
        for (reason, keys) in table {
            assert_eq!(reason.metadata_keys(), keys, "{reason}");
        }
    }

    #[test]
    fn adr_example_parses_and_matches_the_typed_constructor() {
        let value = json!({
            "error": {
                "code": 403,
                "message": "Missing permission events.read.",
                "status": "PERMISSION_DENIED",
                "details": [{
                    "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                    "reason": "MISSING_PERMISSION",
                    "domain": "gang",
                    "metadata": {"permission": "events.read"}
                }]
            }
        });
        let response: ErrorResponse =
            serde_json::from_value(value.clone()).expect("example parses");
        assert_eq!(
            serde_json::to_value(&response).expect("example serializes"),
            value
        );
        let expected = ErrorResponse::from(Status::new(
            ErrorReason::MissingPermission,
            "Missing permission events.read.",
            [("permission", "events.read".to_owned())],
        ));
        assert_eq!(response, expected);
        assert_eq!(
            response.error.reason(),
            Some(ErrorReason::MissingPermission)
        );
    }

    #[test]
    fn unknown_fields_status_detail_type_and_reason_are_tolerated() {
        let value = json!({
            "error": {
                "code": 400,
                "message": "bad",
                "status": "SOMETHING_NEW",
                "futureField": 1,
                "details": [
                    {
                        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                        "reason": "FUTURE_REASON",
                        "domain": "gang",
                        "extra": true
                    },
                    {
                        "@type": "type.googleapis.com/google.rpc.Help",
                        "links": []
                    }
                ]
            },
            "topLevel": true
        });
        let response: ErrorResponse =
            serde_json::from_value(value.clone()).expect("tolerant read succeeds");
        assert_eq!(
            response.error.status,
            Code::Unrecognized("SOMETHING_NEW".to_owned())
        );
        assert_eq!(response.error.reason(), None);
        assert_eq!(
            response.error.error_info().map(|info| info.reason.as_str()),
            Some("FUTURE_REASON")
        );
        assert_eq!(
            response.error.details[1],
            Detail::Unrecognized(value["error"]["details"][1].clone())
        );
        let detail_json = serde_json::to_value(&response.error.details[1]).expect("serializes");
        assert_eq!(detail_json, value["error"]["details"][1]);
    }

    #[test]
    fn with_field_violation_builds_one_bad_request_after_error_info() {
        let status = Status::new(
            ErrorReason::MalformedRequest,
            "invalid request",
            std::iter::empty(),
        )
        .with_field_violation("pageSize", "must not be negative")
        .with_field_violation("filter", "is malformed");
        assert_eq!(status.details.len(), 2);
        assert!(matches!(status.details[0], Detail::ErrorInfo(_)));
        let Detail::BadRequest(bad_request) = &status.details[1] else {
            panic!("second detail is not a BadRequest");
        };
        assert_eq!(
            bad_request.field_violations,
            vec![
                FieldViolation {
                    field: "pageSize".to_owned(),
                    description: "must not be negative".to_owned(),
                },
                FieldViolation {
                    field: "filter".to_owned(),
                    description: "is malformed".to_owned(),
                },
            ]
        );
        let value = serde_json::to_value(&status).expect("status serializes");
        assert_eq!(
            value["details"][1]["@type"],
            "type.googleapis.com/google.rpc.BadRequest"
        );
    }

    #[test]
    fn internal_error_body_is_pinned_to_the_typed_value() {
        let typed = ErrorResponse::from(Status::new(
            ErrorReason::InternalError,
            "internal error",
            std::iter::empty(),
        ));
        assert_eq!(
            INTERNAL_ERROR_BODY,
            serde_json::to_string(&typed).expect("envelope serializes")
        );
        let parsed: ErrorResponse =
            serde_json::from_str(INTERNAL_ERROR_BODY).expect("constant parses");
        assert_eq!(parsed, typed);
    }

    #[test]
    fn details_are_omitted_when_empty_and_metadata_when_empty() {
        let status = Status {
            code: 500,
            message: "internal error".to_owned(),
            status: Code::Internal,
            details: Vec::new(),
        };
        let value = serde_json::to_value(&status).expect("status serializes");
        assert_eq!(
            value,
            json!({"code": 500, "message": "internal error", "status": "INTERNAL"})
        );
        let info = ErrorInfo {
            reason: "INTERNAL_ERROR".to_owned(),
            domain: ERROR_DOMAIN.to_owned(),
            metadata: BTreeMap::new(),
        };
        let value = serde_json::to_value(&info).expect("info serializes");
        assert_eq!(value, json!({"reason": "INTERNAL_ERROR", "domain": "gang"}));
    }

    #[test]
    fn detail_type_is_the_first_key_on_the_wire() {
        let detail = Detail::ErrorInfo(ErrorInfo {
            reason: "INTERNAL_ERROR".to_owned(),
            domain: ERROR_DOMAIN.to_owned(),
            metadata: BTreeMap::new(),
        });
        let text = serde_json::to_string(&detail).expect("detail serializes");
        assert!(
            text.starts_with(r#"{"@type":"type.googleapis.com/google.rpc.ErrorInfo","#),
            "{text}"
        );
    }

    #[test]
    fn retry_delay_serializes_as_retry_info_and_reads_back() {
        let status = Status::new(
            ErrorReason::RateLimited,
            "too many requests",
            std::iter::empty(),
        )
        .with_retry_delay(Duration::from_secs(3));
        let value = serde_json::to_value(&status).expect("status serializes");
        assert_eq!(
            value,
            json!({
                "code": 429,
                "message": "too many requests",
                "status": "RESOURCE_EXHAUSTED",
                "details": [
                    {
                        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                        "reason": "RATE_LIMITED",
                        "domain": "gang"
                    },
                    {
                        "@type": "type.googleapis.com/google.rpc.RetryInfo",
                        "retryDelay": "3s"
                    }
                ]
            })
        );
        let parsed: Status = serde_json::from_value(value).expect("status parses");
        assert_eq!(parsed, status);
        assert_eq!(parsed.retry_delay(), Some(Duration::from_secs(3)));
    }

    #[test]
    fn status_http_matches_http_for_every_reason() {
        for reason in ErrorReason::ALL {
            assert_eq!(reason.status().http(), reason.http(), "{reason}");
        }
    }

    #[test]
    fn code_http_maps_each_code_and_answers_unknown_as_500() {
        let table = [
            (Code::InvalidArgument, 400),
            (Code::Unauthenticated, 401),
            (Code::PermissionDenied, 403),
            (Code::NotFound, 404),
            (Code::ResourceExhausted, 429),
            (Code::Unavailable, 503),
            (Code::DeadlineExceeded, 504),
            (Code::Internal, 500),
            (Code::Unrecognized("FUTURE_CODE".to_owned()), 500),
        ];
        for (code, http) in table {
            assert_eq!(code.http(), http, "{code}");
        }
    }

    #[test]
    fn from_reason_builds_a_status_with_one_metadata_free_error_info() {
        let status = Status::from(ErrorReason::RateLimited);
        assert_eq!(status.code, 429);
        assert_eq!(status.status, Code::ResourceExhausted);
        assert_eq!(status.message, "too many requests");
        assert_eq!(
            status.details,
            vec![Detail::ErrorInfo(ErrorInfo {
                reason: "RATE_LIMITED".to_owned(),
                domain: ERROR_DOMAIN.to_owned(),
                metadata: BTreeMap::new(),
            })]
        );
    }
}
