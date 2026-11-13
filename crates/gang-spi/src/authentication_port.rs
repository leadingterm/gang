//! Authentication: who a caller is, and which credentials a plugin can issue.
//!
//! A *surface* is an audience (clients, or workers). Each surface selects one authentication
//! plugin. The plugin decides how a caller presents an identity ([`AuthenticationPort`]) and which
//! credentials it issues and revokes ([`CredentialLifecyclePort`]). It reaches everything else
//! (identity providers, the certificate authority, the ledger, the clock) through the
//! [`AuthenticationHost`] the server lends on each call, so plugins never depend on each other.

use std::time::Duration;

use gang_clock::Timestamp;
use gang_types::{
    AccessToken, CertSerial, CertificatePem, CertificateRequestPem, Identity, NonZeroDuration,
    Principal, Proof, ProviderName, Url,
};

use crate::{CertificateSignerPort, IssuedCertRecord, PortError, StateRepositoryPort, port};

/// The facts about a connection the transport knows. The certificate is verified by the TLS layer;
/// the address is a transport fact and is not verified.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Peer {
    /// The serial of the client certificate the TLS layer verified, when the client presented one.
    pub certificate: Option<CertSerial>,
    /// The IP address of the connection's remote end, when the transport knows it. It is the address of the directly connected peer: a connection through a proxy shows the proxy's address.
    pub address: Option<std::net::IpAddr>,
}

/// What a plugin sees of one request.
#[derive(Debug, Clone, Copy)]
pub struct Evidence<'a> {
    /// The connection the request arrived on.
    pub peer: &'a Peer,
    /// The request headers.
    pub headers: &'a http::HeaderMap,
    /// The value of the session cookie. Set by the server only for requests allowed to
    /// authenticate by cookie; `None` otherwise.
    pub session: Option<&'a str>,
}

/// A caller the surface's plugin recognised, and how long the credential that proved it stays valid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Authenticated {
    /// Who the caller is.
    pub identity: std::sync::Arc<Identity>,
    /// The last instant the credential is accepted; `None` when it does not expire.
    pub valid_until: Option<Timestamp>,
}

/// Whether the transport must ask clients for a certificate on a listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientCertificates {
    /// The listener does not ask for a certificate.
    Never,
    /// The listener accepts a certificate but also connections without one.
    Optional,
    /// A connection without a certificate fails its handshake.
    Required,
}

/// Which credentials a plugin issues. The server mounts a route only when a surface offers it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Capabilities {
    /// The plugin issues client certificates, which implies revocation by serial and the live
    /// cut-off of established connections.
    pub certificates: bool,
    /// The plugin mints bearer tokens.
    pub tokens: bool,
    /// The plugin mints session tokens for browsers and accepts them from the session cookie.
    pub sessions: bool,
}

/// How a plugin presents itself to clients in the server information.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    /// Gang-signed bearer tokens.
    Token,
    /// Client certificates issued by Gang's authority.
    Mtls,
    /// No authentication.
    None,
}

/// The collaborators the server lends to a plugin for the duration of one call.
pub trait AuthenticationHost: Send + Sync {
    /// The ledger of certificates and principals.
    type Repository: StateRepositoryPort;
    /// The signer that holds the authority's key.
    type Signer: CertificateSignerPort;

    /// The ledger.
    fn repository(&self) -> &Self::Repository;

    /// The authority's signer.
    fn signer(&self) -> &Self::Signer;

    /// The current wall-clock time.
    fn now(&self) -> Timestamp;

    /// The public URL clients reach the server on; tokens name its origin as their issuer.
    fn endpoint(&self) -> &Url;

    /// Verifies `proof` with the identity provider named `provider` and returns who it identifies.
    ///
    /// The host refuses a revoked principal here and records the principal in the registry, so a
    /// plugin that issues a credential after `verify` never issues one to a revoked identity.
    ///
    /// # Errors
    ///
    /// [`PortError::NotFound`] with [`Subject::Provider`](crate::Subject::Provider) for an unknown
    /// provider; [`PortError::Rejected`] when the proof fails or the principal is revoked;
    /// [`PortError::Unavailable`] when the provider cannot be reached.
    fn verify(
        &self,
        provider: &ProviderName,
        proof: &Proof,
    ) -> impl Future<Output = Result<Principal, PortError>> + Send;

    /// Signs a client certificate and supplies its record for the plugin to write to the ledger.
    ///
    /// # Errors
    ///
    /// [`PortError`] when the request is malformed or the authority cannot sign.
    fn issue_certificate(
        &self,
        request: &AuthorityRequest<'_>,
    ) -> impl Future<Output = Result<IssuedCertificate, PortError>> + Send;
}

/// What the authority signs: the public key of `csr`, bound to `identity`, valid for `lifetime`.
#[derive(Debug, Clone, Copy)]
pub struct AuthorityRequest<'a> {
    /// The certificate signing request.
    pub csr: &'a CertificateRequestPem,
    /// Who the certificate identifies.
    pub identity: &'a Identity,
    /// How long the certificate is valid.
    pub lifetime: NonZeroDuration,
}

/// A client certificate the authority signed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuedCertificate {
    /// The certificate.
    pub certificate: CertificatePem,
    /// The certificate of the authority that signed it.
    pub ca_certificate: CertificatePem,
    /// The certificate's record, persisted by the issuing plugin.
    pub record: IssuedCertRecord,
}

/// A request for a client certificate.
#[derive(Debug, Clone, Copy)]
pub struct CertificateIssue<'a> {
    /// The identity provider whose proof is presented.
    pub provider: &'a ProviderName,
    /// The proof.
    pub proof: &'a Proof,
    /// The certificate signing request.
    pub csr: &'a CertificateRequestPem,
}

/// A request for a bearer token.
#[derive(Debug, Clone, Copy)]
pub struct TokenIssue<'a> {
    /// The identity provider whose proof is presented.
    pub provider: &'a ProviderName,
    /// The proof.
    pub proof: &'a Proof,
}

/// A minted bearer token.
#[derive(Debug, Clone)]
pub struct IssuedToken {
    /// The token, opaque to the client.
    pub token: AccessToken,
    /// How long from now the token is valid; a duration so client clock skew cannot change it.
    pub expires_in: Duration,
}

/// Which certificates to revoke.
#[derive(Debug, Clone, Copy)]
pub enum CertificateTarget<'a> {
    /// One certificate.
    Serial(&'a CertSerial),
    /// Every active certificate issued to an identity.
    Identity(&'a Identity),
}

/// How a surface identifies its callers.
#[port]
pub trait AuthenticationPort: Send + Sync {
    /// How the plugin presents itself to clients.
    fn scheme(&self) -> Scheme;

    /// Which credentials the plugin issues.
    fn capabilities(&self) -> Capabilities;

    /// What the surface's listener must ask of clients at the TLS layer.
    fn client_certificates(&self) -> ClientCertificates;

    /// Identifies the caller of one request. `Ok(None)` means the request carries no credential
    /// of this plugin's kind. Otherwise the caller comes with the instant its credential stops
    /// being accepted.
    ///
    /// # Errors
    ///
    /// [`PortError::Rejected`] when a credential is present but not valid;
    /// [`PortError::Unavailable`] when a dependency cannot be reached.
    async fn authenticate<H: AuthenticationHost>(
        &self,
        host: &H,
        evidence: &Evidence<'_>,
    ) -> Result<Option<Authenticated>, PortError>;
}

/// The credentials a plugin issues and revokes. Each operation defaults to
/// [`PortError::Unsupported`]; a plugin overrides the operations its [`Capabilities`] offer.
#[port]
pub trait CredentialLifecyclePort: Send + Sync {
    /// Issues a client certificate against a proof and records it in the ledger.
    ///
    /// # Errors
    ///
    /// [`PortError::Unsupported`] (the default) without the `certificates` capability;
    /// [`PortError::NotFound`] for an unknown provider; [`PortError::Rejected`] when the proof is
    /// refused; other [`PortError`] variants when a dependency fails.
    #[optional]
    async fn issue_certificate<H: AuthenticationHost>(
        &self,
        host: &H,
        request: &CertificateIssue<'_>,
    ) -> Result<IssuedCertificate, PortError>;

    /// Mints a bearer token against a proof.
    ///
    /// # Errors
    ///
    /// [`PortError::Unsupported`] (the default) without the `tokens` capability;
    /// [`PortError::NotFound`] for an unknown provider; [`PortError::Rejected`] when the proof is
    /// refused; other [`PortError`] variants when a dependency fails.
    #[optional]
    async fn issue_token<H: AuthenticationHost>(
        &self,
        host: &H,
        request: &TokenIssue<'_>,
    ) -> Result<IssuedToken, PortError>;

    /// Mints a session token for a browser against a proof. The token is presented only in the
    /// session cookie, never in an `Authorization` header.
    ///
    /// # Errors
    ///
    /// [`PortError::Unsupported`] (the default) without the `sessions` capability;
    /// [`PortError::NotFound`] for an unknown provider; [`PortError::Rejected`] when the proof is
    /// refused; other [`PortError`] variants when a dependency fails.
    #[optional]
    async fn issue_session<H: AuthenticationHost>(
        &self,
        host: &H,
        request: &TokenIssue<'_>,
    ) -> Result<IssuedToken, PortError>;

    /// Revokes certificates in the ledger and returns their revoked records, never empty.
    ///
    /// # Errors
    ///
    /// [`PortError::Unsupported`] (the default) without the `certificates` capability;
    /// [`PortError::NotFound`] when the target names no certificate; other [`PortError`]
    /// variants when the ledger fails.
    #[optional]
    async fn revoke_certificates<H: AuthenticationHost>(
        &self,
        host: &H,
        target: &CertificateTarget<'_>,
    ) -> Result<Vec<IssuedCertRecord>, PortError>;

    /// Every serial revoked so far, for loading the live revocation set at start.
    ///
    /// # Errors
    ///
    /// [`PortError::Unsupported`] (the default) without the `certificates` capability;
    /// other [`PortError`] variants when the ledger fails.
    #[optional]
    async fn revoked_serials<H: AuthenticationHost>(
        &self,
        host: &H,
    ) -> Result<Vec<CertSerial>, PortError>;
}
