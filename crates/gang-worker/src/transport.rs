//! Transport security for a worker's scheduler connection.

use gang_spi::BoxedError;
use gang_types::CertificatePem;

/// How the worker secures its connection to the scheduler.
pub enum Transport<E, C> {
    /// No TLS: the worker endpoint is `grpc`.
    Plain,
    /// TLS authenticating only the server: the worker endpoint is `grpcs`. The source is asked
    /// for the CA certificate the server's chain must reach once the node serves.
    ServerOnly(C),
    /// Mutual TLS: every generation enrolls through the enroller for its certificate.
    Mutual(E),
}

/// The CA certificate a server-only transport trusts.
///
/// The worker asks once, after the node stops being parked, so a parked node makes no call.
pub trait ServerCaSource: Send + Sync {
    /// The CA certificate the server's chain must reach, or `None` to trust the system roots.
    ///
    /// # Errors
    ///
    /// [`ServerCaError::Missing`] when the server's CA is pinned but the server advertises
    /// none; [`ServerCaError::Fetch`] when the certificate cannot be obtained.
    fn server_ca(
        &self,
    ) -> impl Future<Output = Result<Option<CertificatePem>, ServerCaError>> + Send;
}

/// No source: the system roots.
impl<C: ServerCaSource> ServerCaSource for Option<C> {
    async fn server_ca(&self) -> Result<Option<CertificatePem>, ServerCaError> {
        match self {
            Some(source) => source.server_ca().await,
            None => Ok(None),
        }
    }
}

/// The server's CA certificate cannot be obtained.
#[derive(Debug, thiserror::Error, gang_types::Fix)]
pub enum ServerCaError {
    /// The server's CA is pinned, but the server advertises no CA certificate.
    #[error("the server advertised no CA certificate although ca_pin is set")]
    #[fix("check server.ca_pin and the server's TLS setup")]
    Missing,
    /// The server could not be asked for its CA certificate.
    #[error("cannot fetch the server's CA certificate")]
    Fetch(#[source] BoxedError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use gang_testkit::must_ready;

    struct MissingCa;

    impl ServerCaSource for MissingCa {
        async fn server_ca(&self) -> Result<Option<CertificatePem>, ServerCaError> {
            Err(ServerCaError::Missing)
        }
    }

    #[test]
    fn no_source_trusts_the_system_roots() {
        assert!(matches!(
            must_ready(None::<MissingCa>.server_ca()),
            Ok(None)
        ));
    }

    #[test]
    fn a_source_answers_for_itself() {
        assert!(matches!(
            must_ready(Some(MissingCa).server_ca()),
            Err(ServerCaError::Missing)
        ));
    }
}
