//! Maps failures to the API's error envelope. Messages are generic; the details of a failure go
//! to the trace log, never to the caller.

use std::time::Duration;

use gang_spi::{PortError, Rejection, Subject};

use gang_types::Permission;
use gang_wire::name::{self, Certificates, Principals};
use gang_wire::{ErrorReason, Status};

/// The status for `reason` with its generic message and `metadata`.
pub(crate) fn status_of(
    reason: ErrorReason,
    metadata: impl IntoIterator<Item = (&'static str, String)>,
) -> Status {
    Status::new(reason, reason.message(), metadata)
}

/// The status of a request that names no route of the API.
pub(crate) fn route_not_found(method: &str, path: &str) -> Status {
    status_of(
        ErrorReason::RouteNotFound,
        [("method", method.to_owned()), ("path", path.to_owned())],
    )
}

/// The status of a caller that lacks `permission`; the message names it.
pub(crate) fn missing_permission(permission: Permission) -> Status {
    Status::new(
        ErrorReason::MissingPermission,
        format!("{} {permission}", ErrorReason::MissingPermission.message()),
        [("permission", permission.to_string())],
    )
}

/// The whole seconds a caller waits when refused for `retry_after`: rounded up, and at least one.
pub(crate) fn retry_seconds(retry_after: Duration) -> u64 {
    (retry_after.as_secs() + u64::from(retry_after.subsec_nanos() > 0)).max(1)
}

/// The status of a request refused by the rate limit: the caller waits `retry_after`, rounded up
/// to whole seconds and at least one second, before trying again.
pub(crate) fn rate_limited(retry_after: Duration) -> Status {
    Status::from(ErrorReason::RateLimited)
        .with_retry_delay(Duration::from_secs(retry_seconds(retry_after)))
}

/// The status of a request with the field problems `violations`, each a field and what is wrong
/// with it.
pub(crate) fn malformed_request<'v>(
    violations: impl IntoIterator<Item = (&'v str, &'v str)>,
) -> Status {
    violations.into_iter().fold(
        Status::from(ErrorReason::MalformedRequest),
        |status, (field, description)| status.with_field_violation(field, description),
    )
}

/// The status a port failure is answered with.
pub(crate) fn port_status(error: &PortError) -> Status {
    let (reason, metadata) = match error {
        PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        } => (ErrorReason::ProofRejected, None),
        PortError::Rejected {
            reason: Rejection::PolicyDenied,
            ..
        } => (ErrorReason::PolicyDenied, None),
        PortError::Rejected {
            reason: Rejection::PrincipalRevoked,
            ..
        } => (ErrorReason::PrincipalRevoked, None),
        PortError::Rejected {
            reason: Rejection::MalformedRequest,
            ..
        } => (ErrorReason::MalformedRequest, None),
        PortError::NotFound(Subject::Provider(provider)) => (
            ErrorReason::UnknownProvider,
            Some(("provider", provider.to_string())),
        ),
        PortError::NotFound(Subject::Certificate(serial)) => (
            ErrorReason::CertificateNotFound,
            Some(("certificate", name::of::<Certificates>(serial))),
        ),
        PortError::NotFound(Subject::Principal(identity)) => (
            ErrorReason::PrincipalNotFound,
            Some(("principal", name::of::<Principals>(identity))),
        ),
        PortError::Unavailable(_) => (ErrorReason::DependencyUnavailable, None),
        // No route of the API reaches a worker or an instance, and a plugin that cannot issue a
        // credential is not mounted: either would be a bug of the server.
        PortError::NotFound(_)
        | PortError::Unsupported { .. }
        | PortError::Config(_)
        | PortError::Backend(_) => (ErrorReason::InternalError, None),
    };
    log(reason, error);
    status_of(reason, metadata)
}

/// Answers a port failure with the status that describes it.
pub(crate) trait PortResultExt<T> {
    /// The success value, or the status `port_status` gives the failure.
    fn or_status(self) -> Result<T, Status>;
}

impl<T> PortResultExt<T> for Result<T, PortError> {
    fn or_status(self) -> Result<T, Status> {
        self.map_err(|error| port_status(&error))
    }
}

/// The status a failure of the gate is answered with: a credential the plugin refuses is
/// `CREDENTIALS_INVALID`; a revoked principal and a dependency failure are answered as for any
/// other request.
pub(crate) fn gate_status(error: &PortError) -> Status {
    match error {
        PortError::Rejected {
            reason: Rejection::PrincipalRevoked,
            ..
        } => port_status(error),
        PortError::Rejected { .. } => {
            log(ErrorReason::CredentialsInvalid, error);
            Status::from(ErrorReason::CredentialsInvalid)
        }
        _ => port_status(error),
    }
}

fn log(reason: ErrorReason, error: &PortError) {
    match reason {
        ErrorReason::InternalError => {
            tracing::error!(error = %gang_types::error_chain(error), "request failed");
        }
        ErrorReason::DependencyUnavailable => {
            tracing::warn!(error = %gang_types::error_chain(error), "request failed");
        }
        _ => {
            tracing::debug!(error = %gang_types::error_chain(error), "request refused");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use gang_spi::{PortError, Rejection, Subject};
    use gang_types::{CertSerial, Identity, Permission, Principal, ProviderName};
    use gang_wire::{Code, ErrorReason};

    use super::{gate_status, missing_permission, port_status, rate_limited};

    fn provider() -> ProviderName {
        ProviderName::try_new("example-idp").expect("provider name is valid")
    }

    fn identity() -> Identity {
        Identity::new(
            provider(),
            Principal::try_new("ops@example.org").expect("principal is valid"),
        )
    }

    #[test]
    fn port_failures_map_to_reasons() {
        let serial = CertSerial::try_new("3fa9").expect("serial is valid");
        let cases = [
            (
                PortError::refused(Rejection::InvalidProof),
                ErrorReason::ProofRejected,
            ),
            (
                PortError::refused(Rejection::PolicyDenied),
                ErrorReason::PolicyDenied,
            ),
            (
                PortError::refused(Rejection::PrincipalRevoked),
                ErrorReason::PrincipalRevoked,
            ),
            (
                PortError::refused(Rejection::MalformedRequest),
                ErrorReason::MalformedRequest,
            ),
            (
                PortError::NotFound(Subject::Provider(provider())),
                ErrorReason::UnknownProvider,
            ),
            (
                PortError::NotFound(Subject::Certificate(serial)),
                ErrorReason::CertificateNotFound,
            ),
            (
                PortError::NotFound(Subject::Principal(identity())),
                ErrorReason::PrincipalNotFound,
            ),
            (
                PortError::Unsupported {
                    operation: "issue_certificate",
                },
                ErrorReason::InternalError,
            ),
            (
                PortError::unavailable("ledger is down"),
                ErrorReason::DependencyUnavailable,
            ),
            (
                PortError::Config("bad section".into()),
                ErrorReason::InternalError,
            ),
            (
                PortError::backend("disk is full"),
                ErrorReason::InternalError,
            ),
        ];
        for (error, reason) in cases {
            let status = port_status(&error);
            assert_eq!(status.reason(), Some(reason), "{error}");
            assert_eq!(status.code, reason.http(), "{error}");
            assert_eq!(status.status, reason.status(), "{error}");
        }
    }

    #[test]
    fn rejection_cause_is_in_the_error_chain_but_not_the_response() {
        let error = PortError::rejected(Rejection::PolicyDenied, "private backend explanation");
        assert!(
            gang_types::error_chain(&error)
                .to_string()
                .contains("private backend explanation")
        );
        let status = port_status(&error);
        assert_eq!(status.reason(), Some(ErrorReason::PolicyDenied));
        assert!(!format!("{status:?}").contains("private backend explanation"));
    }

    #[test]
    fn not_found_names_the_missing_resource() {
        let serial = CertSerial::try_new("3fa9").expect("serial is valid");
        let status = port_status(&PortError::NotFound(Subject::Certificate(serial)));
        let info = status.error_info().expect("error info is present");
        assert_eq!(
            info.metadata.get("certificate").map(String::as_str),
            Some("certificates/3fa9")
        );

        let status = port_status(&PortError::NotFound(Subject::Principal(identity())));
        let info = status.error_info().expect("error info is present");
        assert_eq!(
            info.metadata.get("principal").map(String::as_str),
            Some("principals/example-idp%3Aops%40example.org")
        );

        let status = port_status(&PortError::NotFound(Subject::Provider(provider())));
        let info = status.error_info().expect("error info is present");
        assert_eq!(
            info.metadata.get("provider").map(String::as_str),
            Some("example-idp")
        );
    }

    #[test]
    fn gate_refusals_distinguish_revoked_from_invalid_credentials() {
        let revoked = gate_status(&PortError::refused(Rejection::PrincipalRevoked));
        assert_eq!(revoked.reason(), Some(ErrorReason::PrincipalRevoked));
        assert_eq!(revoked.code, 403);

        let denied = gate_status(&PortError::refused(Rejection::PolicyDenied));
        assert_eq!(denied.reason(), Some(ErrorReason::CredentialsInvalid));
        assert_eq!(denied.code, 401);

        let invalid = gate_status(&PortError::refused(Rejection::InvalidProof));
        assert_eq!(invalid.reason(), Some(ErrorReason::CredentialsInvalid));
        assert_eq!(invalid.code, 401);

        let unavailable = gate_status(&PortError::unavailable("identity provider is down"));
        assert_eq!(
            unavailable.reason(),
            Some(ErrorReason::DependencyUnavailable)
        );
        assert_eq!(unavailable.status, Code::Unavailable);
    }

    #[test]
    fn messages_are_generic() {
        let status = port_status(&PortError::backend("secret table name"));
        assert_eq!(status.reason(), Some(ErrorReason::InternalError));
        assert!(!status.message.contains("secret"));
    }

    #[test]
    fn missing_permission_names_the_permission_in_message_and_metadata() {
        let status = missing_permission(Permission::EventsRead);
        assert_eq!(status.message, "missing permission events.read");
        let info = status.error_info().expect("error info is present");
        assert_eq!(
            info.metadata.get("permission").map(String::as_str),
            Some("events.read")
        );
    }

    #[test]
    fn rate_limited_rounds_the_retry_delay_up_to_whole_seconds() {
        let delay = |retry_after: Duration| {
            rate_limited(retry_after)
                .retry_delay()
                .expect("retry delay is present")
        };
        assert_eq!(delay(Duration::from_millis(300)), Duration::from_secs(1));
        assert_eq!(delay(Duration::from_millis(2500)), Duration::from_secs(3));
        assert_eq!(delay(Duration::from_secs(4)), Duration::from_secs(4));
        assert_eq!(delay(Duration::ZERO), Duration::from_secs(1));
    }
}
