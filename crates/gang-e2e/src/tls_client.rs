//! TLS configuration and deadline helpers for the harness's raw TLS connections.

use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use gang_clock::Clock;
use gang_tls::ServerTrust;
use gang_types::CertificatePem;
use rustls::ClientConfig;

use crate::errors::{E2eError, Result};

/// The `h2` application protocol, which the server's API listener negotiates.
pub(crate) const ALPN_H2: &[u8] = b"h2";

/// Bounds an asynchronous operation and reports what did not complete.
pub(crate) async fn within<T>(
    budget: Duration,
    condition: impl Into<String>,
    operation: impl Future<Output = Result<T>>,
) -> Result<T> {
    tokio::time::timeout(budget, operation)
        .await
        .map_err(|_| E2eError::timeout(condition, budget))?
}

/// Builds the server-only TLS client configuration for one connection attempt.
///
/// When `authority` is given, the server must chain to the Gang CA certificate at that path.
/// Otherwise the server must chain to the public web PKI.
///
/// Certificate validity is judged at the time `clock` reports.
///
/// # Errors
///
/// Returns [`E2eError::Io`] when the authority file cannot be read, and [`E2eError::System`]
/// when it is not a PEM certificate or the TLS configuration is rejected.
pub fn client_config(
    authority: Option<&Path>,
    alpn: &[&[u8]],
    clock: Arc<dyn Clock>,
) -> Result<ClientConfig> {
    let pem = authority.map(certificate_pem).transpose()?;
    let trust = match pem.as_ref() {
        Some(pem) => ServerTrust::Ca(pem),
        None => ServerTrust::System,
    };
    gang_tls::client_config(trust, None, alpn, clock)
        .map_err(E2eError::system_from("configuring tls"))
}

/// Reads the PEM certificate file at `path`.
pub(crate) fn certificate_pem(path: &Path) -> Result<CertificatePem> {
    let text = std::fs::read_to_string(path)
        .map_err(E2eError::io(format!("reading {}", path.display())))?;
    CertificatePem::try_new(text).map_err(E2eError::system_from(format!(
        "reading the pem file `{}`",
        path.display()
    )))
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use gang_clock::{Clock, TestClock, Timestamp};

    use super::{ALPN_H2, client_config};
    use crate::errors::E2eError;

    /// A clock frozen at a fixed instant, so the tests never depend on the host time.
    fn clock() -> Arc<dyn Clock> {
        Arc::new(TestClock::new(Timestamp::from_unix_millis(
            1_700_000_000_000,
        )))
    }

    #[test]
    fn unreadable_authority_is_io_error() {
        let error = client_config(
            Some(Path::new("/definitely/not/an/authority.pem")),
            &[ALPN_H2],
            clock(),
        )
        .expect_err("a missing authority is an error");
        assert!(matches!(error, E2eError::Io { .. }));
    }

    #[test]
    fn authority_that_is_not_a_certificate_is_system_error() {
        let directory = tempfile::tempdir().expect("a temporary directory is created");
        let authority = directory.path().join("authority.pem");
        std::fs::write(&authority, "not a certificate\n").expect("the file is written");
        let error = client_config(Some(&authority), &[ALPN_H2], clock())
            .expect_err("a non-PEM authority is an error");
        assert!(matches!(error, E2eError::System { .. }));
    }
}
