//! Client-side TLS: TLS 1.3 only, optionally presents a client certificate, resumption disabled.

use std::fmt;
use std::sync::Arc;

use gang_clock::Clock;
use gang_types::NonZeroDuration;
use rustls::client::Resumption;
use rustls_pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::acceptor::Alpn;
use crate::client_config::{ClientIdentity, ServerTrust, client_config};
use crate::errors::TransportError;

/// Server trust, client identity and handshake limit for a [`TlsConnector`].
#[derive(Clone)]
pub struct ConnectorSettings<'a> {
    /// How the server certificate is trusted.
    pub trust: ServerTrust<'a>,
    /// Client certificate and key presented to the server; `None` presents no certificate.
    pub identity: Option<ClientIdentity<'a>>,
    /// The longest a handshake may take.
    pub handshake_timeout: NonZeroDuration,
    /// Clock used for certificate validity and handshake deadlines.
    pub clock: Arc<dyn Clock>,
    /// Application protocols offered during the handshake, in preference order.
    pub alpn: Alpn,
}

/// Connects to servers over TLS 1.3, presenting the configured client identity if any.
///
/// Session resumption is disabled, so every connection runs a full handshake.
#[derive(Clone)]
pub struct TlsConnector {
    connector: tokio_rustls::TlsConnector,
    handshake_timeout: NonZeroDuration,
    clock: Arc<dyn Clock>,
}

impl fmt::Debug for TlsConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsConnector")
            .field("handshake_timeout", &self.handshake_timeout)
            .finish_non_exhaustive()
    }
}

impl TlsConnector {
    /// Builds the client configuration from `settings`.
    ///
    /// # Errors
    ///
    /// Returns the material or configuration error when the identity or trust anchor is
    /// unusable.
    pub fn try_new(settings: &ConnectorSettings<'_>) -> Result<Self, TransportError> {
        let mut config = client_config(
            settings.trust,
            settings.identity,
            settings.alpn.as_slices(),
            Arc::clone(&settings.clock),
        )?;
        config.resumption = Resumption::disabled();

        Ok(Self {
            connector: tokio_rustls::TlsConnector::from(Arc::new(config)),
            handshake_timeout: settings.handshake_timeout,
            clock: Arc::clone(&settings.clock),
        })
    }

    /// Runs the client handshake for `server_name` on `io`.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::HandshakeTimeout`] when the handshake exceeds the configured
    /// timeout and [`TransportError::Handshake`] when the handshake fails, including a server
    /// certificate that does not match `server_name`.
    pub async fn connect<IO>(
        &self,
        server_name: ServerName<'_>,
        io: IO,
    ) -> Result<tokio_rustls::client::TlsStream<IO>, TransportError>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        Clock::timeout(
            &self.clock,
            self.handshake_timeout.get(),
            self.connector.connect(server_name.to_owned(), io),
        )
        .await
        .map_err(|_| TransportError::HandshakeTimeout)?
        .map_err(TransportError::Handshake)
    }
}

#[cfg(test)]
mod tests {
    use gang_clock::{TestClock, Timestamp};
    use gang_testkit::{ExtendedKeyUsagePurpose, TestAuthority};

    use crate::tls_material::key_body;

    use super::*;

    #[test]
    fn debug_output_never_prints_the_client_private_key() {
        let authority = TestAuthority::new("gang test ca").expect("test ca builds");
        let client = authority
            .issue(
                vec!["client".to_owned()],
                vec![ExtendedKeyUsagePurpose::ClientAuth],
                [0x02; 16],
                "client",
            )
            .expect("test client leaf issues");
        let connector = TlsConnector::try_new(&ConnectorSettings {
            trust: ServerTrust::Ca(&authority.pem),
            identity: Some(ClientIdentity {
                certificate: &client.pem,
                private_key: &client.key_pem,
            }),
            handshake_timeout: NonZeroDuration::from_secs(10),
            clock: Arc::new(TestClock::new(Timestamp::UNIX_EPOCH)),
            alpn: Alpn::H2,
        })
        .expect("test connector builds");

        let debug = format!("{connector:?}");
        assert!(debug.contains("TlsConnector"));
        assert!(!debug.contains(&key_body(&client.key_pem)));
    }
}
