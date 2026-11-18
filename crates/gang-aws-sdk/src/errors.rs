use aws_runtime::retries::classifiers::{THROTTLING_ERRORS, TRANSIENT_ERRORS};
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use gang_spi::{BoxedError, PortFailure};

/// HTTP status that signals throttling regardless of the service error code.
const TOO_MANY_REQUESTS: u16 = 429;
/// First HTTP status of the server-error range; every status at or above it is transient.
const SERVER_ERROR_FLOOR: u16 = 500;

/// Service error codes Gang classifies as transient beyond the SDK's lists.
const GANG_TRANSIENT_CODES: &[&str] = &[
    "ServiceUnavailable",
    "InternalFailure",
    "InternalError",
    // DynamoDB marks this retryable: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html#Programming.Errors.MessagesAndCodes.http5xx
    "InternalServerError",
    "ResourceContention",
    "ScalingActivityInProgress",
    "KMSInternalException",
    "DependencyTimeoutException",
    "TransactionConflict",
    "TransactionConflictException",
];

/// Whether a service error code is transient by the SDK's lists or Gang's own.
fn transient_code(code: &str) -> bool {
    THROTTLING_ERRORS
        .iter()
        .chain(TRANSIENT_ERRORS)
        .chain(GANG_TRANSIENT_CODES)
        .any(|known| *known == code)
}

/// A failed AWS SDK call classified by retryability.
#[derive(Debug, thiserror::Error, PortFailure)]
pub enum CallError {
    /// An AWS call did not complete; retrying may succeed.
    #[error("{operation} did not complete: {source}")]
    #[failure(unavailable)]
    Transient {
        /// The AWS operation that failed, for example `DescribeAutoScalingGroups`.
        operation: &'static str,
        /// The underlying SDK failure.
        source: BoxedError,
    },
    /// AWS refused an AWS call or the response could not be built; retrying will not help.
    #[error("{operation} was refused: {source}")]
    Refused {
        /// The AWS operation that failed, for example `Sign`.
        operation: &'static str,
        /// The underlying SDK failure.
        source: BoxedError,
    },
}

impl CallError {
    /// Constructs a call error according to its retryability.
    #[must_use]
    pub fn failed(operation: &'static str, retryability: Retryability, source: BoxedError) -> Self {
        match retryability {
            Retryability::Transient => Self::Transient { operation, source },
            Retryability::Permanent => Self::Refused { operation, source },
        }
    }
}

/// Whether a failed call may succeed when repeated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retryability {
    /// Repeating the call may succeed.
    Transient,
    /// Repeating the call will fail the same way.
    Permanent,
}

/// Classifies a failed AWS call from its service error code and HTTP status.
///
/// A throttling status, a server-error status, or a code on the SDK's throttling or transient list, or on Gang's own list, is transient.
#[must_use]
pub fn classify(code: Option<&str>, status: Option<u16>) -> Retryability {
    let transient_status =
        status.is_some_and(|status| status == TOO_MANY_REQUESTS || status >= SERVER_ERROR_FLOOR);
    let transient_code = code.is_some_and(transient_code);
    if transient_status || transient_code {
        Retryability::Transient
    } else {
        Retryability::Permanent
    }
}

/// Converts a failed SDK call into a `CallError`, keeping the SDK error as the source.
#[must_use]
pub fn call_error<E>(operation: &'static str, error: SdkError<E, HttpResponse>) -> CallError
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    let retryability = match &error {
        // Smithy also treats an unparseable or interrupted response as transient.
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) | SdkError::ResponseError(_) => {
            Retryability::Transient
        }
        SdkError::ServiceError(_) => classify(
            error.code(),
            error.raw_response().map(|raw| raw.status().as_u16()),
        ),
        _ => Retryability::Permanent,
    };
    CallError::failed(operation, retryability, Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_smithy_runtime_api::client::result::ConnectorError;
    use aws_smithy_types::error::metadata::ErrorMetadata;
    use std::error::Error as _;

    #[derive(Debug, thiserror::Error)]
    #[error("test service error")]
    struct TestError(ErrorMetadata);

    impl ProvideErrorMetadata for TestError {
        fn meta(&self) -> &ErrorMetadata {
            &self.0
        }
    }

    #[test]
    fn classify_follows_code_and_status() {
        let cases: &[(Option<&str>, Option<u16>, Retryability)] = &[
            (Some("AccessDenied"), Some(403), Retryability::Permanent),
            (Some("AccessDenied"), Some(500), Retryability::Transient),
            (Some("AccessDenied"), Some(503), Retryability::Transient),
            (Some("AccessDenied"), Some(429), Retryability::Transient),
            (
                Some("EC2ThrottledException"),
                Some(400),
                Retryability::Transient,
            ),
            (
                Some("ServiceUnavailable"),
                Some(400),
                Retryability::Transient,
            ),
            (Some("InternalFailure"), Some(400), Retryability::Transient),
            (Some("InternalError"), Some(400), Retryability::Transient),
            (
                Some("ResourceContention"),
                Some(400),
                Retryability::Transient,
            ),
            (
                Some("ScalingActivityInProgress"),
                Some(400),
                Retryability::Transient,
            ),
            (
                Some("KMSInternalException"),
                Some(400),
                Retryability::Transient,
            ),
            (
                Some("DependencyTimeoutException"),
                Some(400),
                Retryability::Transient,
            ),
            (
                Some("TransactionConflict"),
                Some(400),
                Retryability::Transient,
            ),
            (
                Some("TransactionConflictException"),
                Some(400),
                Retryability::Transient,
            ),
            (
                Some("ValidationException"),
                Some(400),
                Retryability::Permanent,
            ),
            (
                Some("UnknownServiceCode"),
                Some(400),
                Retryability::Permanent,
            ),
            (None, Some(502), Retryability::Transient),
            (None, None, Retryability::Permanent),
        ];
        for (code, status, expected) in cases {
            assert_eq!(
                classify(*code, *status),
                *expected,
                "code {code:?}, status {status:?}"
            );
        }
    }

    #[test]
    fn timeouts_are_transient() {
        let error: SdkError<TestError, HttpResponse> =
            SdkError::timeout_error(std::io::Error::other("slow"));
        assert!(matches!(
            call_error("DescribeInstances", error),
            CallError::Transient {
                operation: "DescribeInstances",
                ..
            }
        ));
    }

    #[test]
    fn dispatch_failures_are_transient() {
        let connector = ConnectorError::io(Box::new(std::io::Error::other("reset")));
        let error: SdkError<TestError, HttpResponse> = SdkError::dispatch_failure(connector);
        let converted = call_error("Sign", error);
        assert!(matches!(
            converted,
            CallError::Transient {
                operation: "Sign",
                ..
            }
        ));
        assert!(converted.source().is_some());
    }

    #[test]
    fn response_errors_are_transient() -> Result<(), Box<dyn std::error::Error>> {
        let error: SdkError<TestError, HttpResponse> = SdkError::response_error(
            std::io::Error::other("cut off"),
            HttpResponse::new(
                aws_smithy_runtime_api::http::StatusCode::try_from(200)?,
                aws_smithy_types::body::SdkBody::empty(),
            ),
        );
        assert!(matches!(
            call_error("DescribeInstances", error),
            CallError::Transient {
                operation: "DescribeInstances",
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn failed_is_transient_or_refused_by_retryability() {
        let transient = CallError::failed(
            "DescribeInstances",
            Retryability::Transient,
            Box::new(std::io::Error::other("reset")),
        );
        assert!(matches!(
            transient,
            CallError::Transient {
                operation: "DescribeInstances",
                ..
            }
        ));
        assert!(transient.source().is_some());

        let refused = CallError::failed(
            "Sign",
            Retryability::Permanent,
            Box::new(std::io::Error::other("denied")),
        );
        assert!(matches!(
            refused,
            CallError::Refused {
                operation: "Sign",
                ..
            }
        ));
        assert!(refused.source().is_some());
    }
}
