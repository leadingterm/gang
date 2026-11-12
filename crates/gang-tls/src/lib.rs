//! TLS transport for Gang's mutual TLS identities, with live certificate revocation.
//!
//! Every connection re-verifies its client certificate against the revocation set, and a stream
//! whose peer is revoked fails its next I/O call with [`std::io::ErrorKind::ConnectionAborted`]:
//!
//! A client chooses how it trusts servers with [`ServerTrust`]: a private CA document, a pinned
//! CA, or the roots the operating system trusts (its platform certificate store, read with
//! `rustls-native-certs`). Every configuration names its crypto provider explicitly, so no
//! process-wide provider is required.
//!
//! ```
//! use gang_tls::RevocationSet;
//! use gang_types::CertSerial;
//!
//! let serial = CertSerial::try_new("4a1f0c9e2b7d33a85f10e6c2d9b04a71")?;
//! let revocations = RevocationSet::from_serials(Vec::new());
//! assert!(!revocations.is_revoked(&serial));
//! revocations.revoke(serial.clone());
//! assert!(revocations.is_revoked(&serial));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod acceptor;
mod client_config;
mod connector;
mod errors;
mod http_client;
mod incoming;
mod revocation;
mod stream;
mod time;
mod tls_material;
mod verifier;

pub use acceptor::{AcceptorSettings, Alpn, ClientAuth, TlsAcceptor};
pub use client_config::{ClientIdentity, ServerTrust, ca_pin_of_certificate, client_config};
pub use connector::{ConnectorSettings, TlsConnector};
pub use errors::{TransportError, is_certificate_refused, is_tls_failure};
pub use http_client::{
    HttpClient, HttpError, HttpProtocol, HttpReply, HttpRequest, HttpScheme, HttpSettings,
    HttpStream, HttpTransport, RequestBody,
};
pub use incoming::{Failure, Incoming, IncomingSettings, Listener, accept_retrying};
pub use revocation::{RevocationSet, RevocationWatch};
pub use rustls_pki_types::ServerName;
pub use stream::RevocableStream;
