//! `#[derive(PortFailure)]`: each variant class converts to its `PortError` variant, and `?` uses the
//! generated `From` impl.

use std::error::Error as _;

use gang_spi::{PortError, PortFailure, Rejection, Subject};
use gang_types::ProviderName;

/// Errors covering every failure class.
#[derive(Debug, thiserror::Error, PortFailure)]
pub enum Failure {
    /// A configuration the plugin cannot use.
    #[error("bad value")]
    #[failure(config)]
    BadValue,
    /// A transient failure that a later try may clear.
    #[error("timed out after {0} ms")]
    #[failure(unavailable)]
    Timeout(u32),
    /// A permanent failure with no marker, so it is a backend failure.
    #[error("crashed: {reason}")]
    Crash {
        /// Why the backend crashed.
        reason: String,
    },
    /// A permanent failure with a source cause.
    #[error("storage write failed")]
    #[failure(backend)]
    Io {
        /// The I/O error that caused the failure.
        #[source]
        source: std::io::Error,
    },
    /// The backend refused the request under its policy.
    #[error("denied")]
    #[failure(rejected(PolicyDenied))]
    Denied,
    /// A missing provider.
    #[error("provider missing")]
    #[failure(not_found(Provider))]
    Missing(ProviderName),
    /// A missing provider stored in a named field.
    #[error("named provider missing")]
    #[failure(not_found(Provider))]
    MissingNamed {
        /// The missing provider.
        provider: ProviderName,
    },
    /// An operation the plugin cannot perform.
    #[error("cannot sign")]
    #[failure(unsupported("sign"))]
    CannotSign,
    /// A preclassified port failure.
    #[error(transparent)]
    #[failure(transparent)]
    Port(PortError),
}

/// Propagates a `Failure` with `?` into a `PortError`.
fn fails_with(error: Failure) -> Result<(), PortError> {
    Err(error)?;
    Ok(())
}

#[test]
fn config_class_becomes_config_with_the_original_error_as_source() {
    let port_error = PortError::from(Failure::BadValue);

    assert!(matches!(port_error, PortError::Config(_)));
    let source = port_error.source().expect("config carries its cause");
    assert!(matches!(
        source.downcast_ref::<Failure>(),
        Some(Failure::BadValue)
    ));
}

#[test]
fn unavailable_class_becomes_unavailable() {
    let port_error = PortError::from(Failure::Timeout(5));

    assert!(matches!(port_error, PortError::Unavailable(_)));
}

#[test]
fn default_class_becomes_backend() {
    let port_error = PortError::from(Failure::Crash {
        reason: "oom".to_owned(),
    });

    assert!(matches!(port_error, PortError::Backend(_)));
    let chain = gang_types::error_chain(&port_error).to_string();
    assert!(chain.contains("crashed: oom"), "chain was {chain:?}");
}

#[test]
fn backend_class_keeps_the_io_error_text_exactly_once() {
    let io = std::io::Error::other("disk offline");
    let port_error = PortError::from(Failure::Io { source: io });

    assert!(matches!(port_error, PortError::Backend(_)));
    let chain = gang_types::error_chain(&port_error).to_string();
    assert_eq!(
        chain.matches("disk offline").count(),
        1,
        "chain was {chain:?}"
    );
}

#[test]
fn rejected_class_becomes_the_named_rejection() {
    let port_error = PortError::from(Failure::Denied);

    assert!(matches!(
        &port_error,
        PortError::Rejected {
            reason: Rejection::PolicyDenied,
            ..
        }
    ));
    assert!(matches!(
        port_error
            .source()
            .and_then(|source| source.downcast_ref::<Failure>()),
        Some(Failure::Denied)
    ));
}

#[test]
fn not_found_class_preserves_the_subject_field() {
    let provider = ProviderName::try_new("example").expect("valid provider name");
    let port_error = PortError::from(Failure::Missing(provider.clone()));
    assert!(
        matches!(port_error, PortError::NotFound(Subject::Provider(found)) if found == provider)
    );
    let port_error = PortError::from(Failure::MissingNamed {
        provider: provider.clone(),
    });
    assert!(
        matches!(port_error, PortError::NotFound(Subject::Provider(found)) if found == provider)
    );
}

#[test]
fn unsupported_class_preserves_the_operation() {
    assert!(matches!(
        PortError::from(Failure::CannotSign),
        PortError::Unsupported { operation: "sign" }
    ));
}

#[test]
fn transparent_class_preserves_the_inner_port_error() {
    assert!(matches!(
        PortError::from(Failure::Port(PortError::refused(Rejection::InvalidProof))),
        PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        }
    ));
}

#[test]
fn refusal_without_cause_has_no_source() {
    let error = PortError::refused(Rejection::InvalidProof);
    assert!(error.source().is_none());
}

#[test]
fn question_mark_converts_into_port_error() {
    let port_error = fails_with(Failure::Timeout(7)).expect_err("the failure propagates");

    assert!(matches!(port_error, PortError::Unavailable(_)));
}
