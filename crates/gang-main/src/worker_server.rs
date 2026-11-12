//! The server a worker reaches on the public endpoint before it holds a certificate: its enrollment
//! and the server-only CA fetch. The server CA pin, and with it the client, is resolved on the first
//! call, so a worker that waits parked reads no signer key and calls no server.

use std::sync::Arc;

use gang_client::{ClientError, ClientOptions, ServerApi};
use gang_clock::Clock;
use gang_config::CaPinSetting;
use gang_spi::{PluginContext, PortError};
use gang_tls::ServerTrust;
use gang_types::{CaPin, CertificatePem, Url};
use gang_wire::{Certificate, CreateCertificateRequest, Server};
use gang_worker::{ServerCaError, ServerCaSource};
use tokio::sync::OnceCell;

use crate::plugins::{Signer, SignerConfig};
use crate::signer_pin::signer_ca_pin;

/// The configured trust anchor for the worker's server connection.
pub(crate) enum ServerPin {
    /// Trust the system roots.
    SystemRoots,
    /// Trust a configured pin.
    Pinned(CaPin),
    /// Derive the pin from the signer's public key on first use.
    Signer(Box<Signer>),
}

impl ServerPin {
    /// Prepares the configured pin without reading the signer's key.
    ///
    /// # Errors
    ///
    /// Returns an error if the configured signer cannot be built.
    pub(crate) fn try_new(
        setting: Option<CaPinSetting>,
        signer: SignerConfig,
        context: &PluginContext<'_>,
    ) -> crate::errors::Result<Self> {
        match setting {
            None => Ok(Self::SystemRoots),
            Some(CaPinSetting::Pinned(pin)) => Ok(Self::Pinned(pin)),
            Some(CaPinSetting::Signer) => {
                Ok(Self::Signer(Box::new(Signer::build(signer, context)?)))
            }
        }
    }

    /// Whether the connection uses a pin rather than system roots.
    pub(crate) fn is_pinned(&self) -> bool {
        !matches!(self, Self::SystemRoots)
    }

    /// Resolves the pin, reading the signer's public key when configured.
    ///
    /// # Errors
    ///
    /// Returns an error if the signer's public key cannot be read.
    pub(crate) async fn resolve(&self) -> Result<Option<CaPin>, PortError> {
        match self {
            Self::SystemRoots => Ok(None),
            Self::Pinned(pin) => Ok(Some(*pin)),
            Self::Signer(signer) => Ok(Some(signer_ca_pin(signer).await?)),
        }
    }
}

/// A failure to prepare or call the worker's server client.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ServerCallError {
    /// The signer's public key could not be read.
    #[error("cannot read the signer's public key to pin the server CA")]
    SignerPin(#[source] PortError),
    /// The client could not be built or the server call failed.
    #[error(transparent)]
    Client(#[from] ClientError),
}

/// The server calls used by worker enrollment and CA discovery.
pub(crate) trait ServerCalls: Send + Sync {
    /// Fetches the server's public configuration.
    fn server(
        &self,
    ) -> impl Future<Output = Result<Server<serde_json::Value>, ServerCallError>> + Send;

    /// Exchanges a workload proof for a certificate.
    fn create_certificate(
        &self,
        request: &CreateCertificateRequest,
    ) -> impl Future<Output = Result<Certificate, ServerCallError>> + Send;
}

/// A lazily initialized client for the worker's public server endpoint.
pub(crate) struct WorkerServer {
    endpoint: Url,
    pin: ServerPin,
    options: ClientOptions,
    clock: Arc<dyn Clock>,
    api: OnceCell<ServerApi>,
}

impl WorkerServer {
    /// Prepares a server connection without reading keys or making requests.
    pub(crate) fn new(
        endpoint: Url,
        pin: ServerPin,
        options: ClientOptions,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            endpoint,
            pin,
            options,
            clock,
            api: OnceCell::new(),
        }
    }

    /// Whether the server connection uses a CA pin.
    pub(crate) fn is_pinned(&self) -> bool {
        self.pin.is_pinned()
    }

    /// Resolves the pin and initializes the client, retrying initialization after a failure.
    async fn api(&self) -> Result<&ServerApi, ServerCallError> {
        self.api
            .get_or_try_init(|| async {
                let pin = self
                    .pin
                    .resolve()
                    .await
                    .map_err(ServerCallError::SignerPin)?;
                Ok(ServerApi::try_new(
                    &self.endpoint,
                    ServerTrust::from(pin.as_ref()),
                    &self.options,
                    Arc::clone(&self.clock),
                )?)
            })
            .await
    }
}

impl ServerCalls for WorkerServer {
    async fn server(&self) -> Result<Server<serde_json::Value>, ServerCallError> {
        Ok(ServerApi::server(self.api().await?, None).await?)
    }

    async fn create_certificate(
        &self,
        request: &CreateCertificateRequest,
    ) -> Result<Certificate, ServerCallError> {
        Ok(ServerApi::create_certificate(self.api().await?, None, request).await?)
    }
}

/// The CA certificate the server advertises, for a server-only transport: a pinned server must
/// advertise one.
pub(crate) struct AdvertisedCa<S = WorkerServer> {
    server: S,
    pinned: bool,
}

impl AdvertisedCa {
    /// Creates a deferred source for the server's advertised CA.
    pub(crate) fn new(server: WorkerServer) -> Self {
        let pinned = server.is_pinned();
        Self { server, pinned }
    }
}

impl<S: ServerCalls> ServerCaSource for AdvertisedCa<S> {
    async fn server_ca(&self) -> Result<Option<CertificatePem>, ServerCaError> {
        let advertised = self
            .server
            .server()
            .await
            .map_err(|error| ServerCaError::Fetch(Box::new(error)))?
            .ca_certificate;
        if self.pinned && advertised.is_none() {
            return Err(ServerCaError::Missing);
        }
        Ok(advertised)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use gang_clock::{TestClock, Timestamp};
    use gang_spi::CertificateSignerPort;

    use super::*;
    use crate::signer_pin::file_signer;

    #[tokio::test]
    async fn a_pinned_setting_resolves_to_its_pin() -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let config_path = dir.path().join("gang.toml");
        let context = PluginContext {
            config_path: &config_path,
        };
        let pin = CaPin::new([0x42; 32]);
        let resolved = ServerPin::try_new(
            Some(CaPinSetting::Pinned(pin)),
            file_signer(dir.path()),
            &context,
        )?
        .resolve()
        .await?;
        assert_eq!(resolved, Some(pin));
        Ok(())
    }

    #[tokio::test]
    async fn the_signer_setting_resolves_to_the_pin_of_the_signers_public_key()
    -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let config_path = dir.path().join("gang.toml");
        let context = PluginContext {
            config_path: &config_path,
        };
        let resolved = ServerPin::try_new(
            Some(CaPinSetting::Signer),
            file_signer(dir.path()),
            &context,
        )?
        .resolve()
        .await?;
        let signer = Signer::build(file_signer(dir.path()), &context)?;
        let spki = signer.public_key().await?;
        assert_eq!(resolved, Some(CaPin::of_spki(&spki)));
        Ok(())
    }

    #[tokio::test]
    async fn no_setting_resolves_to_no_pin() -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let config_path = dir.path().join("gang.toml");
        let context = PluginContext {
            config_path: &config_path,
        };
        let resolved = ServerPin::try_new(None, file_signer(dir.path()), &context)?
            .resolve()
            .await?;
        assert_eq!(resolved, None);
        Ok(())
    }

    #[tokio::test]
    async fn a_signer_pin_reads_no_key_until_it_is_resolved() -> Result<(), Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let config_path = dir.path().join("gang.toml");
        let context = PluginContext {
            config_path: &config_path,
        };
        let pin = ServerPin::try_new(
            Some(CaPinSetting::Signer),
            file_signer(dir.path()),
            &context,
        )?;
        let _server = WorkerServer::new(
            Url::parse("https://gang.example.com")?,
            pin,
            ClientOptions::default(),
            Arc::new(TestClock::new(Timestamp::UNIX_EPOCH)),
        );
        assert!(!dir.path().join("signer").exists());
        let pin = ServerPin::try_new(
            Some(CaPinSetting::Signer),
            file_signer(dir.path()),
            &context,
        )?;
        pin.resolve().await?;
        assert!(dir.path().join("signer").join("ca.key").exists());
        Ok(())
    }

    const CERTIFICATE: &str = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";

    struct FakeServer {
        ca: Option<CertificatePem>,
        fail: bool,
        calls: AtomicUsize,
    }

    impl ServerCalls for FakeServer {
        async fn server(&self) -> Result<Server<serde_json::Value>, ServerCallError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(ClientError::Server {
                    status: 500,
                    message: "internal error".to_owned(),
                }
                .into());
            }
            let mut server = serde_json::json!({
                "name": "server",
                "remoteExecutor": "grpcs://example.com",
                "remoteCache": "grpcs://example.com",
                "defaultIsa": "x86-64",
                "authentication": {"none": {}},
            });
            if let Some(ca) = &self.ca {
                server["caCertificate"] = ca.as_str().into();
            }
            serde_json::from_value(server).map_err(|error| ClientError::Body(error).into())
        }

        async fn create_certificate(
            &self,
            _request: &CreateCertificateRequest,
        ) -> Result<Certificate, ServerCallError> {
            Err(ClientError::Server {
                status: 500,
                message: "unexpected certificate request".to_owned(),
            }
            .into())
        }
    }

    fn source(pinned: bool, ca: Option<CertificatePem>, fail: bool) -> AdvertisedCa<FakeServer> {
        AdvertisedCa {
            server: FakeServer {
                ca,
                fail,
                calls: AtomicUsize::new(0),
            },
            pinned,
        }
    }

    #[tokio::test]
    async fn a_pinned_server_without_a_ca_is_missing() {
        let source = source(true, None, false);
        assert!(matches!(
            source.server_ca().await,
            Err(ServerCaError::Missing)
        ));
        assert_eq!(source.server.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn an_unpinned_server_without_a_ca_trusts_the_system_roots() {
        let source = source(false, None, false);
        assert!(matches!(source.server_ca().await, Ok(None)));
        assert_eq!(source.server.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn the_advertised_ca_is_trusted() -> Result<(), Box<dyn Error>> {
        let source = source(true, Some(CertificatePem::try_new(CERTIFICATE)?), false);
        let ca = source.server_ca().await?.ok_or("missing advertised CA")?;
        assert_eq!(ca.as_str(), CERTIFICATE);
        assert_eq!(source.server.calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn a_failed_call_is_a_fetch_failure() {
        let source = source(true, None, true);
        assert!(matches!(
            source.server_ca().await,
            Err(ServerCaError::Fetch(_))
        ));
        assert_eq!(source.server.calls.load(Ordering::SeqCst), 1);
    }
}
