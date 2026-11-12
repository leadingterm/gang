use aws_runtime::retries::classifiers::{THROTTLING_ERRORS, TRANSIENT_ERRORS};
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use gang_os::PathIo;
use gang_spi::{BoxedError, PortFailure};
use gang_types::FleetName;
use std::path::PathBuf;

/// HTTP status that signals throttling regardless of the service error code.
const TOO_MANY_REQUESTS: u16 = 429;
/// First HTTP status of the server-error range; every status at or above it is transient.
const SERVER_ERROR_FLOOR: u16 = 500;

/// Service error codes Gang retries on top of the SDK's own throttling and transient lists.
const GANG_TRANSIENT_CODES: &[&str] = &[
    "ServiceUnavailable",
    "InternalFailure",
    "InternalError",
    "ResourceContention",
    "ScalingActivityInProgress",
    "KMSInternalException",
    "DependencyTimeoutException",
];

/// Whether a service error code is transient by the SDK's lists or Gang's own.
fn transient_code(code: &str) -> bool {
    THROTTLING_ERRORS
        .iter()
        .chain(TRANSIENT_ERRORS)
        .chain(GANG_TRANSIENT_CODES)
        .any(|known| *known == code)
}

/// Every failure the AWS adapters can report. Converted into `PortError` at the port boundary.
#[derive(Debug, thiserror::Error, PortFailure)]
pub enum AwsError {
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
    /// The configured auto scaling group does not exist.
    #[error("auto scaling group {0} does not exist")]
    #[failure(config)]
    MissingGroup(FleetName),
    /// An AWS response lacked a field the adapter needs, or carried an unexpected value.
    #[error("{operation} returned no {what}")]
    MalformedResponse {
        /// The AWS operation whose response was incomplete.
        operation: &'static str,
        /// The response content that is missing or unexpected.
        what: &'static str,
    },
    /// A configuration value cannot be used.
    #[error("invalid {field}: {reason}")]
    #[failure(config)]
    Config {
        /// The configuration field that is invalid.
        field: &'static str,
        /// Why the value is invalid.
        reason: String,
    },
    /// The KMS key cannot be used to sign certificates.
    #[error("kms key {key_id} cannot be used as the certificate authority key: {reason}")]
    #[failure(config)]
    UnsuitableKey {
        /// The key id, key ARN or alias that was configured.
        key_id: String,
        /// Which property of the key rules it out.
        reason: &'static str,
    },
    /// The scratch volume is not usable.
    #[error(transparent)]
    Scratch(#[from] ScratchError),
}

impl AwsError {
    /// A response that lacks the named content.
    pub(crate) fn malformed(operation: &'static str, what: &'static str) -> Self {
        Self::MalformedResponse { operation, what }
    }

    /// A failed call, transient or refused according to `retryability`.
    pub(crate) fn failed(
        operation: &'static str,
        retryability: Retryability,
        source: BoxedError,
    ) -> Self {
        match retryability {
            Retryability::Transient => Self::Transient { operation, source },
            Retryability::Permanent => Self::Refused { operation, source },
        }
    }
}

/// Why the scratch volume cannot be adopted. All messages are lowercase.
#[derive(Debug, thiserror::Error)]
pub enum ScratchError {
    /// The configured path is not a mount point, so no volume has been prepared there.
    #[error("{} is not a mount point; the scratch volume has not been prepared", .path.display())]
    NotMounted {
        /// The configured mount point.
        path: PathBuf,
    },
    /// The mounted filesystem is not xfs.
    #[error("{} is not an xfs filesystem", .path.display())]
    NotXfs {
        /// The configured mount point.
        path: PathBuf,
    },
    /// The filesystem cannot clone file data by reference.
    #[error("the filesystem at {} does not support reflinks", .path.display())]
    NoReflink {
        /// The configured mount point.
        path: PathBuf,
    },
    /// The filesystem could not be inspected.
    #[error("cannot inspect {}: {source}", .path.display())]
    Probe {
        /// The configured mount point.
        path: PathBuf,
        /// The operating system error from the probe.
        source: std::io::Error,
    },
    /// The space available on the scratch volume could not be read.
    #[error("cannot read the scratch volume's size")]
    Space(#[from] PathIo),
}

/// Whether a failed call may succeed when repeated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Retryability {
    /// Repeating the call may succeed.
    Transient,
    /// Repeating the call will fail the same way.
    Permanent,
}

/// Classifies a failed AWS call from its service error code and HTTP status.
///
/// A throttling status, a server-error status, or a code on the SDK's throttling or transient list, or on Gang's own list, is transient.
#[must_use]
pub(crate) fn classify(code: Option<&str>, status: Option<u16>) -> Retryability {
    let transient_status =
        status.is_some_and(|status| status == TOO_MANY_REQUESTS || status >= SERVER_ERROR_FLOOR);
    let transient_code = code.is_some_and(transient_code);
    if transient_status || transient_code {
        Retryability::Transient
    } else {
        Retryability::Permanent
    }
}

/// Converts a failed SDK call into an `AwsError`, keeping the SDK error as the source.
pub(crate) fn sdk_error<E>(operation: &'static str, error: SdkError<E, HttpResponse>) -> AwsError
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
    AwsError::failed(operation, retryability, Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_smithy_runtime_api::client::result::ConnectorError;
    use aws_smithy_types::error::metadata::ErrorMetadata;
    use gang_spi::PortError;
    use std::error::Error as _;

    #[derive(Debug, thiserror::Error)]
    #[error("test service error")]
    struct TestError(ErrorMetadata);

    impl ProvideErrorMetadata for TestError {
        fn meta(&self) -> &ErrorMetadata {
            &self.0
        }
    }

    fn transient_error() -> AwsError {
        AwsError::Transient {
            operation: "Sign",
            source: Box::new(std::io::Error::other("reset")),
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
    fn transient_maps_to_unavailable() {
        assert!(matches!(
            PortError::from(transient_error()),
            PortError::Unavailable(_)
        ));
    }

    #[test]
    fn refused_maps_to_backend() {
        let refused = AwsError::Refused {
            operation: "Sign",
            source: Box::new(std::io::Error::other("denied")),
        };
        assert!(matches!(PortError::from(refused), PortError::Backend(_)));
    }

    #[test]
    fn malformed_response_maps_to_backend() {
        let malformed = AwsError::MalformedResponse {
            operation: "GetWebIdentityToken",
            what: "token",
        };
        assert!(matches!(PortError::from(malformed), PortError::Backend(_)));
    }

    #[test]
    fn scratch_maps_to_backend() {
        let scratch = AwsError::Scratch(ScratchError::NotXfs {
            path: PathBuf::from("/scratch"),
        });
        assert!(matches!(PortError::from(scratch), PortError::Backend(_)));
    }

    #[test]
    fn missing_group_maps_to_config_naming_the_group() -> Result<(), Box<dyn std::error::Error>> {
        let group = FleetName::try_new("gang-fleet")?;
        let port = PortError::from(AwsError::MissingGroup(group));
        assert!(matches!(
            &port,
            PortError::Config(cause) if cause.to_string().contains("gang-fleet")
        ));
        Ok(())
    }

    #[test]
    fn config_maps_to_config_naming_the_field() {
        let config = AwsError::Config {
            field: "region",
            reason: "must not be empty".to_owned(),
        };
        assert!(matches!(
            &PortError::from(config),
            PortError::Config(cause) if cause.to_string().contains("region")
        ));
    }

    #[test]
    fn unsuitable_key_maps_to_config_naming_the_key() {
        let key = AwsError::UnsuitableKey {
            key_id: "alias/signing".to_owned(),
            reason: "key usage is not SIGN_VERIFY",
        };
        assert!(matches!(
            &PortError::from(key),
            PortError::Config(cause) if cause.to_string().contains("alias/signing")
        ));
    }

    #[test]
    fn timeouts_are_transient() {
        let error: SdkError<TestError, HttpResponse> =
            SdkError::timeout_error(std::io::Error::other("slow"));
        assert!(matches!(
            sdk_error("DescribeInstances", error),
            AwsError::Transient {
                operation: "DescribeInstances",
                ..
            }
        ));
    }

    #[test]
    fn dispatch_failures_are_transient() {
        let connector = ConnectorError::io(Box::new(std::io::Error::other("reset")));
        let error: SdkError<TestError, HttpResponse> = SdkError::dispatch_failure(connector);
        let converted = sdk_error("Sign", error);
        assert!(matches!(
            converted,
            AwsError::Transient {
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
            sdk_error("DescribeInstances", error),
            AwsError::Transient {
                operation: "DescribeInstances",
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn failed_is_transient_or_refused_by_retryability() {
        let transient = AwsError::failed(
            "DescribeInstances",
            Retryability::Transient,
            Box::new(std::io::Error::other("reset")),
        );
        assert!(matches!(
            transient,
            AwsError::Transient {
                operation: "DescribeInstances",
                ..
            }
        ));
        assert!(transient.source().is_some());

        let refused = AwsError::failed(
            "Sign",
            Retryability::Permanent,
            Box::new(std::io::Error::other("denied")),
        );
        assert!(matches!(
            refused,
            AwsError::Refused {
                operation: "Sign",
                ..
            }
        ));
        assert!(refused.source().is_some());
    }

    #[test]
    fn malformed_names_operation_and_content() {
        let malformed = AwsError::malformed("GetWebIdentityToken", "token");
        assert!(matches!(
            malformed,
            AwsError::MalformedResponse {
                operation: "GetWebIdentityToken",
                what: "token",
            }
        ));
        let text = malformed.to_string();
        assert!(text.contains("GetWebIdentityToken"));
        assert!(text.contains("token"));
    }
}
