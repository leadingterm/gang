//! Errors of the end-to-end harness.
//!
//! These are failures of the harness or of its plumbing (a process that cannot start, output that
//! cannot be parsed). A system that behaves wrongly is not an error: it is a failed
//! [`Check`](crate::Check).

use std::borrow::Cow;
use std::io;
use std::time::Duration;

/// The boxed cause kept by the variants of [`E2eError`] that wrap another error.
type BoxedError = Box<dyn std::error::Error + Send + Sync>;

/// Why a scenario could not run to completion.
///
/// [`Display`](std::fmt::Display) prints only the message of the variant itself; the underlying
/// cause, when there is one, is available through [`std::error::Error::source`]. Use
/// [`gang_types::error_chain`] to print the whole chain as one sentence.
#[derive(Debug, thiserror::Error)]
pub enum E2eError {
    /// A file or process operation failed.
    #[error("{operation}")]
    Io {
        /// What was being done, such as `reading /tmp/x`.
        operation: String,
        /// The underlying error.
        #[source]
        source: io::Error,
    },
    /// An external program ran and ended unsuccessfully.
    #[error("{program} {outcome}: {output}")]
    Command {
        /// The program that was run.
        program: String,
        /// How it ended, such as `exited with status 1`.
        outcome: String,
        /// Its captured output.
        output: String,
    },
    /// Text that should have had a known shape did not.
    #[error("cannot parse {subject}")]
    Parse {
        /// What was being parsed.
        subject: &'static str,
        /// What was wrong with it.
        #[source]
        source: BoxedError,
    },
    /// The `gang.toml` template cannot be adapted to the scenario.
    #[error("invalid configuration template: {0}")]
    Template(String),
    /// An observed condition did not come true in time.
    #[error("timed out after {waited:?} waiting for {condition}")]
    Timeout {
        /// What was awaited.
        condition: String,
        /// How long the harness waited.
        waited: Duration,
    },
    /// The system under test cannot be observed or driven as the harness requires.
    #[error("system under test: {context}")]
    System {
        /// What the harness was doing when the system under test failed it.
        context: Cow<'static, str>,
        /// The underlying error, when there is one.
        #[source]
        source: Option<BoxedError>,
    },
    /// The in-process `OpenID` Connect issuer failed.
    #[error("in-process issuer: {context}")]
    Issuer {
        /// What the harness was doing when the issuer failed.
        context: Cow<'static, str>,
        /// The underlying error, when there is one.
        #[source]
        source: Option<BoxedError>,
    },
}

/// The result type of every fallible harness operation.
pub type Result<T, E = E2eError> = std::result::Result<T, E>;

impl E2eError {
    /// Builds the error mapping for an [`io::Error`] raised while doing `operation`.
    ///
    /// ```
    /// use gang_e2e::E2eError;
    ///
    /// let error = std::fs::read("/definitely/not/here").map_err(E2eError::io("reading the fixture"));
    /// assert!(error.is_err());
    /// ```
    pub fn io(operation: impl Into<String>) -> impl FnOnce(io::Error) -> Self {
        let operation = operation.into();
        move |source| Self::Io { operation, source }
    }

    /// Builds a timeout error for a condition that did not occur within `waited`.
    ///
    /// ```
    /// use std::time::Duration;
    /// use gang_e2e::E2eError;
    ///
    /// let error = E2eError::timeout("the server to start", Duration::from_secs(1));
    /// assert!(matches!(error, E2eError::Timeout { .. }));
    /// ```
    #[must_use]
    pub fn timeout(condition: impl Into<String>, waited: Duration) -> Self {
        Self::Timeout {
            condition: condition.into(),
            waited,
        }
    }

    /// Builds a [`System`](Self::System) error with no underlying cause.
    #[must_use]
    pub fn system(context: impl Into<Cow<'static, str>>) -> Self {
        Self::System {
            context: context.into(),
            source: None,
        }
    }

    /// Builds the error mapping for a failure of the system under test while doing `context`.
    ///
    /// The error that was caught is kept as the [`source`](std::error::Error::source).
    ///
    /// ```
    /// use gang_e2e::E2eError;
    ///
    /// let error = std::fs::read("/definitely/not/here")
    ///     .map_err(E2eError::system_from("reading the fixture"));
    /// assert!(error.is_err());
    /// ```
    pub fn system_from<E: Into<BoxedError>>(
        context: impl Into<Cow<'static, str>>,
    ) -> impl FnOnce(E) -> Self {
        let context = context.into();
        move |error| Self::System {
            context,
            source: Some(error.into()),
        }
    }

    /// Builds an [`Issuer`](Self::Issuer) error with no underlying cause.
    #[must_use]
    pub fn issuer(context: impl Into<Cow<'static, str>>) -> Self {
        Self::Issuer {
            context: context.into(),
            source: None,
        }
    }

    /// Builds the error mapping for a failure of the in-process issuer while doing `context`.
    ///
    /// The error that was caught is kept as the [`source`](std::error::Error::source).
    pub fn issuer_from<E: Into<BoxedError>>(
        context: impl Into<Cow<'static, str>>,
    ) -> impl FnOnce(E) -> Self {
        let context = context.into();
        move |error| Self::Issuer {
            context,
            source: Some(error.into()),
        }
    }

    /// Builds the error mapping for text that failed to parse as `subject`.
    ///
    /// The parse error that was caught is kept as the [`source`](std::error::Error::source).
    pub fn parse<E: Into<BoxedError>>(subject: &'static str) -> impl FnOnce(E) -> Self {
        move |error| Self::Parse {
            subject,
            source: error.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io;

    use super::E2eError;

    #[test]
    fn system_from_keeps_the_cause_as_source() {
        let error = E2eError::system_from("reading x")(io::Error::other("boom"));

        let source = error.source().map(ToString::to_string);
        assert_eq!(source.as_deref(), Some("boom"));
    }

    #[test]
    fn error_chain_prints_context_then_cause() {
        let error = E2eError::system_from("reading x")(io::Error::other("boom"));

        assert_eq!(
            gang_types::error_chain(&error).to_string(),
            "system under test: reading x: boom"
        );
    }

    #[test]
    fn io_error_chain_includes_the_cause_once() {
        let error = E2eError::io("reading x")(io::Error::other("boom"));

        assert_eq!(error.to_string(), "reading x");
        assert_eq!(
            gang_types::error_chain(&error).to_string(),
            "reading x: boom"
        );
    }

    #[test]
    fn system_without_cause_has_no_source() {
        assert!(E2eError::system("x").source().is_none());
    }
}
