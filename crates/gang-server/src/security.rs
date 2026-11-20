//! The transport security of a server: none, or TLS under a certificate authority whose key the
//! signer holds. TLS and its authority, names and pin exist together or not at all.

use std::sync::Arc;

use gang_mtls::{CertificateAuthority, SubjectName};
use gang_types::{CaPin, CertificatePem};

use crate::deployment::Stack;
use crate::endpoint::{PublicEndpoint, WorkerEndpoint};
use crate::errors::ServerError;
use crate::settings::{TlsSettings, TransportSettings};

/// The certificate authority of a stack.
type Authority<K> = CertificateAuthority<Arc<<K as Stack>::Signer>, Arc<<K as Stack>::Clock>>;

/// How a server's listeners are secured.
pub(crate) enum Security<K: Stack> {
    /// Connections are served as received.
    Plaintext,
    /// Connections complete a TLS handshake under the server's certificate authority.
    Tls(Box<TlsIdentity<K>>),
}

/// What TLS needs: its settings, the names the listeners present, the authority that issues
/// certificates, and the pin clients trust it by.
pub(crate) struct TlsIdentity<K: Stack> {
    /// How certificates are issued and handshakes bounded.
    pub(crate) settings: TlsSettings,
    /// The names the listener certificates are issued for.
    pub(crate) names: Names,
    /// The authority that issues every certificate.
    pub(crate) authority: Arc<Authority<K>>,
    /// The pin of the authority's certificate.
    pub(crate) pin: CaPin,
}

/// The certificate names the TLS listeners present.
pub(crate) struct Names {
    /// The name the public listener's certificate is issued for.
    pub(crate) public: SubjectName,
    /// The name the worker listener's certificate is issued for.
    pub(crate) worker: SubjectName,
}

impl<K: Stack> Security<K> {
    /// Establishes the security `transport` asks for. Under TLS this names the listeners after
    /// the endpoints and establishes the certificate authority over `signer`.
    ///
    /// # Errors
    ///
    /// Returns an error when a TLS endpoint has no host name a certificate can name, or the
    /// certificate authority cannot be established.
    pub(crate) async fn establish(
        transport: TransportSettings,
        public: &PublicEndpoint,
        worker: &WorkerEndpoint,
        signer: Arc<K::Signer>,
        clock: Arc<K::Clock>,
    ) -> Result<Self, ServerError> {
        let TransportSettings::Tls(settings) = transport else {
            return Ok(Self::Plaintext);
        };
        let names = Names {
            public: public.server_name()?,
            worker: worker.server_name()?,
        };
        let authority = CertificateAuthority::establish(
            signer,
            clock,
            settings.trust_domain.clone(),
            settings.certificate_backdate,
        )
        .await
        .map_err(ServerError::Authority)?;
        let pin = gang_tls::ca_pin_of_certificate(authority.certificate())?;
        Ok(Self::Tls(Box::new(TlsIdentity {
            settings,
            names,
            authority: Arc::new(authority),
            pin,
        })))
    }

    /// The certificate authority; `None` under plaintext.
    #[must_use]
    pub(crate) fn authority(&self) -> Option<&Authority<K>> {
        match self {
            Self::Plaintext => None,
            Self::Tls(tls) => Some(&tls.authority),
        }
    }

    /// The pin of the certificate authority; `None` under plaintext.
    #[must_use]
    pub(crate) fn ca_pin(&self) -> Option<CaPin> {
        match self {
            Self::Plaintext => None,
            Self::Tls(tls) => Some(tls.pin),
        }
    }

    /// The certificate of the authority, which the server information carries; `None` under
    /// plaintext.
    #[must_use]
    pub(crate) fn certificate(&self) -> Option<CertificatePem> {
        self.authority()
            .map(|authority| authority.certificate().clone())
    }

    /// Whether the listeners are secured by TLS.
    #[must_use]
    pub(crate) fn is_tls(&self) -> bool {
        matches!(self, Self::Tls(_))
    }
}
