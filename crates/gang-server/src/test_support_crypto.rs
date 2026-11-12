//! Test certificate authority, client certificates, and TLS clients for the `gang-server` tests.

use std::sync::Arc;
use std::time::Duration;

use gang_clock::TestClock;
use gang_mtls::{CertificateAuthority, ClientCertificateRequest, IssuedCertificate, KeyPair};
use gang_testkit::TestSigner;
use gang_tls::{
    Alpn, ClientIdentity, ConnectorSettings, ServerName, ServerTrust, TlsConnector, TransportError,
};
use gang_types::{
    CertificatePem, CertificateRequestPem, NonZeroDuration, Principal, PrivateKeyPem, ProviderName,
    TrustDomain,
};
use tokio::io::DuplexStream;

use crate::test_support_memory::MemoryDialer;

/// Handshake timeout of every test TLS client.
const HANDSHAKE_TIMEOUT: NonZeroDuration = NonZeroDuration::from_secs(10);

/// Lifetime of a client certificate issued by [`client_certificate`].
const CLIENT_CERTIFICATE_LIFETIME: NonZeroDuration = NonZeroDuration::from_hours(1);

/// The test certificate authority over `clock`: trust domain `example.test`, backdated 60 s.
pub async fn authority(clock: &Arc<TestClock>) -> CertificateAuthority<TestSigner, Arc<TestClock>> {
    let trust_domain = TrustDomain::try_new("example.test").expect("test trust domain is valid");
    CertificateAuthority::establish(
        TestSigner::authority().expect("the test signer builds"),
        clock.clone(),
        trust_domain,
        Duration::from_mins(1),
    )
    .await
    .expect("test authority establishes")
}

/// Connects to the server named `host` over `stream` using TLS 1.3, trusting the certificate
/// authority `ca` and presenting `identity` when one is given.
///
/// # Errors
///
/// Returns the transport error when the handshake fails or times out.
pub async fn tls_client_over(
    stream: DuplexStream,
    host: &str,
    ca: &CertificatePem,
    identity: Option<(&CertificatePem, &PrivateKeyPem)>,
    clock: &Arc<TestClock>,
) -> Result<tokio_rustls::client::TlsStream<DuplexStream>, TransportError> {
    let identity = identity.map(|(certificate, private_key)| ClientIdentity {
        certificate,
        private_key,
    });
    let connector = TlsConnector::try_new(&ConnectorSettings {
        trust: ServerTrust::Ca(ca),
        identity,
        handshake_timeout: HANDSHAKE_TIMEOUT,
        alpn: Alpn::H2,
        clock: clock.clone(),
    })?;
    let name = ServerName::try_from(host).expect("test host is a valid server name");
    connector.connect(name, stream).await
}

/// Connects to the server named `host` through `dialer` over TLS 1.3, trusting the certificate
/// authority `ca` and presenting `identity` when one is given.
///
/// # Errors
///
/// Returns the transport error when the handshake fails or times out.
pub async fn tls_client(
    dialer: &MemoryDialer,
    host: &str,
    ca: &CertificatePem,
    identity: Option<(&CertificatePem, &PrivateKeyPem)>,
    clock: &Arc<TestClock>,
) -> Result<tokio_rustls::client::TlsStream<DuplexStream>, TransportError> {
    tls_client_over(dialer.dial(), host, ca, identity, clock).await
}

/// A client certificate for `principal` under `provider`, issued by `authority` for one hour,
/// together with the private key it was issued for.
pub async fn client_certificate(
    authority: &CertificateAuthority<TestSigner, Arc<TestClock>>,
    provider: &str,
    principal: &str,
) -> (IssuedCertificate, PrivateKeyPem) {
    let key = KeyPair::generate().expect("test key pair generates");
    let csr = key
        .certificate_request()
        .expect("test key pair signs a request");
    let provider_name = ProviderName::try_new(provider).expect("test provider name is valid");
    let principal_name = Principal::try_new(principal).expect("test principal is valid");
    let issued = authority
        .issue_client(&ClientCertificateRequest {
            csr: &csr,
            provider: &provider_name,
            principal: &principal_name,
            lifetime: CLIENT_CERTIFICATE_LIFETIME,
        })
        .await
        .expect("test authority issues a client certificate");
    (issued, key.private_key().clone())
}

/// A PKCS#10 signing request for a fresh key, as a worker sends with an issuance request.
pub fn signing_request() -> CertificateRequestPem {
    KeyPair::generate()
        .expect("test key pair generates")
        .certificate_request()
        .expect("test key pair signs a request")
}
