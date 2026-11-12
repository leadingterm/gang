//! Shared fixture for the `gang-tls` integration tests: a throwaway PKI, in-memory connections and
//! a fake listener, so that no test touches a socket.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use gang_clock::{TestClock, Timestamp};
use gang_testkit::{ExtendedKeyUsagePurpose, TestAuthority, TestLeaf, Window};

use gang_tls::{
    AcceptorSettings, Alpn, ClientAuth, ClientIdentity, ConnectorSettings, Listener,
    RevocableStream, RevocationSet, ServerName, ServerTrust, TlsAcceptor, TlsConnector,
    TransportError, ca_pin_of_certificate, client_config,
};
use gang_types::{CaPin, CertSerial, CertificatePem, NonZeroDuration, PrivateKeyPem};
use rustls::client::Resumption;
use rustls::crypto::ring::default_provider;
use rustls::version::{TLS12, TLS13};
use rustls::{
    CertificateError, ClientConfig, Error as RustlsError, RootCertStore, ServerConfig,
    SupportedProtocolVersion,
};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream};
use tokio::sync::{mpsc, oneshot};

/// Application protocol every test connection negotiates.
pub const ALPN_H2: &[u8] = b"h2";

/// Application protocol of HTTP/1.1.
pub const ALPN_HTTP1: &[u8] = b"http/1.1";

/// Handshake deadline of every fixture connection that does not choose its own.
const HANDSHAKE_TIMEOUT: NonZeroDuration = NonZeroDuration::from_secs(10);

/// The line [`echo_once`] sends and reads back.
const ECHO: &[u8] = b"gang-tls echo\n";

/// Buffer size of each in-memory connection.
const PIPE_CAPACITY: usize = 64 * 1024;

/// Wall time of the fixture's clock when a test starts: 2030-06-01T00:00:00Z.
const FIXTURE_NOW_UNIX_MILLIS: u64 = 1_906_502_400_000;

/// Common name of the fixture's certificate authority. A forging authority reuses it, so that a
/// forged chain names the same issuer as the genuine one.
const CA_COMMON_NAME: &str = "gang test ca";

/// The clock every fixture starts from, at 2030-06-01T00:00:00Z.
pub fn fixture_clock() -> Arc<TestClock> {
    Arc::new(TestClock::new(Timestamp::from_unix_millis(
        FIXTURE_NOW_UNIX_MILLIS,
    )))
}

/// A serial of 16 bytes, unique per `counter` value.
fn serial_bytes(counter: &AtomicU32, tag: u8) -> [u8; 16] {
    let count = counter.fetch_add(1, Ordering::Relaxed).to_be_bytes();
    [
        0x00, tag, count[0], count[1], count[2], count[3], 0x5a, 0x3c, 0x91, 0x07, 0xe2, 0x4b,
        0x6d, 0x18, 0xf0, 0x2a,
    ]
}

/// Builds the server acceptor that presents `leaf` and admits clients of `ca` as `client_auth`
/// allows and offers `alpn` during the handshake.
fn acceptor_for(
    ca: &TestAuthority,
    leaf: &TestLeaf,
    revocations: &RevocationSet,
    handshake_timeout: NonZeroDuration,
    client_auth: ClientAuth,
    clock: Arc<TestClock>,
    alpn: Alpn,
) -> Result<TlsAcceptor, TransportError> {
    TlsAcceptor::try_new(&AcceptorSettings {
        certificate: &leaf.pem,
        private_key: &leaf.key_pem,
        client_ca: &ca.pem,
        revocations,
        handshake_timeout,
        client_auth,
        clock,
        alpn,
    })
}

/// A TLS 1.3 server configuration presenting `chain`, whose leaf key is `key_der`. It negotiates
/// h2 and asks for no client certificate.
fn server_config(chain: Vec<CertificateDer<'static>>, key_der: Vec<u8>) -> Arc<ServerConfig> {
    let mut config = ServerConfig::builder_with_provider(Arc::new(default_provider()))
        .with_protocol_versions(&[&TLS13])
        .expect("tls 1.3 is supported by the ring provider")
        .with_no_client_auth()
        .with_single_cert(
            chain,
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_der)),
        )
        .expect("test server key matches its certificate");
    config.alpn_protocols = vec![ALPN_H2.to_vec()];
    Arc::new(config)
}

/// A client certificate and the connector that presents it.
pub struct TestClient {
    /// Serial of the client's certificate.
    pub serial: CertSerial,
    /// A connector presenting this client's certificate to the fixture's server.
    pub connector: TlsConnector,
    pem: CertificatePem,
    key_pem: PrivateKeyPem,
    der: CertificateDer<'static>,
    key_der: Vec<u8>,
}

impl TestClient {
    /// This client's certificate and key, as the identity a connector presents.
    pub fn identity(&self) -> ClientIdentity<'_> {
        ClientIdentity {
            certificate: &self.pem,
            private_key: &self.key_pem,
        }
    }
}

/// A server, its certificate authority and the revocation set, for one test.
pub struct TransportFixture {
    ca: TestAuthority,
    server: TestLeaf,
    acceptor: TlsAcceptor,
    revocations: RevocationSet,
    client_auth: ClientAuth,
    handshake_timeout: NonZeroDuration,
    serials: AtomicU32,
    clock: Arc<TestClock>,
    alpn: Alpn,
}

/// One established in-memory connection, client side and server side.
pub struct Connection {
    /// The client's end of the connection.
    pub client: Result<tokio_rustls::client::TlsStream<DuplexStream>, TransportError>,
    /// The server's end of the connection.
    pub server:
        Result<RevocableStream<tokio_rustls::server::TlsStream<DuplexStream>>, TransportError>,
}

/// The certificates a raw server presents: its leaf alone, or its leaf followed by the CA.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerChain {
    /// The server certificate only.
    LeafOnly,
    /// The server certificate followed by the authority that issued it.
    LeafAndCa,
}

impl TransportFixture {
    /// A fixture whose acceptor requires client certificates.
    pub fn new() -> Self {
        Self::with_client_auth(ClientAuth::Required)
    }

    /// A fixture whose acceptor applies `auth` to client certificates.
    pub fn with_client_auth(auth: ClientAuth) -> Self {
        Self::try_build(HANDSHAKE_TIMEOUT, auth, None, Alpn::H2).expect("test fixture builds")
    }

    /// A fixture whose acceptor offers `alpn` during the handshake and requests no client
    /// certificate, so clients that present none still reach the protocol negotiation.
    pub fn with_alpn(alpn: Alpn) -> Self {
        Self::try_build(HANDSHAKE_TIMEOUT, ClientAuth::None, None, alpn)
            .expect("test fixture builds")
    }

    /// A fixture whose server certificate is valid within `window`; the acceptor still requires
    /// client certificates.
    pub fn with_server_window(window: Window) -> Self {
        Self::try_build(
            HANDSHAKE_TIMEOUT,
            ClientAuth::Required,
            Some(window),
            Alpn::H2,
        )
        .expect("test fixture builds")
    }

    /// The clock that judges certificate validity for this fixture's acceptor and connectors.
    pub fn clock(&self) -> Arc<TestClock> {
        Arc::clone(&self.clock)
    }

    /// A fixture whose connectors and acceptor use `handshake_timeout`.
    pub fn with_handshake_timeout(handshake_timeout: NonZeroDuration) -> Self {
        Self::try_build(handshake_timeout, ClientAuth::Required, None, Alpn::H2)
            .expect("test fixture builds")
    }

    fn try_build(
        handshake_timeout: NonZeroDuration,
        client_auth: ClientAuth,
        server_window: Option<Window>,
        alpn: Alpn,
    ) -> Result<Self, TransportError> {
        let serials = AtomicU32::new(0);
        let ca = TestAuthority::new(CA_COMMON_NAME).expect("test ca builds");
        let server = ca
            .issue_within(
                vec!["localhost".to_owned(), "127.0.0.1".to_owned()],
                vec![ExtendedKeyUsagePurpose::ServerAuth],
                serial_bytes(&serials, 0),
                "localhost",
                server_window,
            )
            .expect("test server leaf issues");
        let revocations = RevocationSet::default();
        let clock = fixture_clock();
        let acceptor = acceptor_for(
            &ca,
            &server,
            &revocations,
            handshake_timeout,
            client_auth,
            Arc::clone(&clock),
            alpn,
        )?;
        Ok(Self {
            ca,
            server,
            acceptor,
            revocations,
            client_auth,
            handshake_timeout,
            serials,
            clock,
            alpn,
        })
    }

    /// A fixture whose client certificate `(tag, name)` is already revoked.
    pub fn with_revoked_client(tag: u8, name: &str) -> (Self, TestClient) {
        let fixture = Self::new();
        let client = fixture.client(tag, name);
        fixture.revocations.revoke(client.serial.clone());
        (fixture, client)
    }

    /// A client certificate issued by this fixture's authority, with the clientAuth usage.
    pub fn client(&self, tag: u8, name: &str) -> TestClient {
        let leaf = self
            .ca
            .issue(
                Vec::new(),
                vec![ExtendedKeyUsagePurpose::ClientAuth],
                serial_bytes(&self.serials, tag),
                name,
            )
            .expect("test client leaf issues");
        self.client_from(leaf)
    }

    /// A client certificate issued by this fixture's authority, valid only within `window`.
    pub fn client_within(&self, tag: u8, name: &str, window: Window) -> TestClient {
        let leaf = self
            .ca
            .issue_within(
                Vec::new(),
                vec![ExtendedKeyUsagePurpose::ClientAuth],
                serial_bytes(&self.serials, tag),
                name,
                Some(window),
            )
            .expect("test client leaf issues");
        self.client_from(leaf)
    }

    /// A client certificate issued by this fixture's authority, whose extended key usage lists
    /// serverAuth only, so it is not valid for client authentication.
    pub fn client_lacking_client_auth(&self, tag: u8, name: &str) -> TestClient {
        let leaf = self
            .ca
            .issue(
                Vec::new(),
                vec![ExtendedKeyUsagePurpose::ServerAuth],
                serial_bytes(&self.serials, tag),
                name,
            )
            .expect("test client leaf issues");
        self.client_from(leaf)
    }

    /// A client certificate with the clientAuth usage, issued by a different authority that the
    /// fixture's server does not trust.
    pub fn foreign_client(&self, tag: u8, name: &str) -> TestClient {
        let foreign = TestAuthority::new("gang foreign ca").expect("test ca builds");
        let leaf = foreign
            .issue(
                Vec::new(),
                vec![ExtendedKeyUsagePurpose::ClientAuth],
                serial_bytes(&self.serials, tag),
                name,
            )
            .expect("test client leaf issues");
        self.client_from(leaf)
    }

    /// The server acceptor, which presents `localhost` and `127.0.0.1`
    /// and admits clients of this authority.
    pub fn acceptor(&self) -> &TlsAcceptor {
        &self.acceptor
    }

    /// An acceptor presenting a server certificate valid for `dns_name` only, so clients that
    /// connect as `localhost` must refuse it.
    pub fn acceptor_presenting(&self, dns_name: &str) -> TlsAcceptor {
        let leaf = self
            .ca
            .issue(
                vec![dns_name.to_owned()],
                vec![ExtendedKeyUsagePurpose::ServerAuth],
                serial_bytes(&self.serials, 0),
                dns_name,
            )
            .expect("test leaf issues");
        acceptor_for(
            &self.ca,
            &leaf,
            &self.revocations,
            self.handshake_timeout,
            self.client_auth,
            self.clock(),
            self.alpn,
        )
        .expect("test acceptor builds")
    }

    /// The revocation set shared with the acceptor.
    pub fn revocations(&self) -> &RevocationSet {
        &self.revocations
    }

    /// The certificate of this fixture's authority: the CA trust anchor of a [`ServerTrust::Ca`]
    /// client.
    pub fn ca_certificate(&self) -> &CertificatePem {
        &self.ca.pem
    }

    /// The pin of this fixture's authority: the SHA-256 of its `SubjectPublicKeyInfo`.
    pub fn ca_pin(&self) -> CaPin {
        ca_pin_of_certificate(&self.ca.pem).expect("test ca certificate yields a pin")
    }

    /// A connector that trusts the server only through `pin`, presenting `client`'s certificate
    /// when one is given.
    pub fn pinned_connector(&self, pin: &CaPin, client: Option<&TestClient>) -> TlsConnector {
        self.build_connector(
            ServerTrust::Pin(pin),
            client.map(TestClient::identity),
            self.handshake_timeout,
        )
        .expect("test pinned connector builds")
    }

    /// A connector that trusts this fixture's authority and presents no client certificate.
    pub fn anonymous_connector(&self) -> TlsConnector {
        self.build_connector(
            ServerTrust::Ca(self.ca_certificate()),
            None,
            self.handshake_timeout,
        )
        .expect("test anonymous connector builds")
    }

    /// The name every test client connects to: `localhost`.
    pub fn server_name() -> ServerName<'static> {
        ServerName::try_from("localhost").expect("localhost is a valid dns name")
    }

    /// A new pair of connected in-memory streams, `(client, server)`.
    pub fn pipe() -> (DuplexStream, DuplexStream) {
        tokio::io::duplex(PIPE_CAPACITY)
    }

    /// Completes a handshake between `client` and the server over a new in-memory connection.
    pub async fn connect(&self, client: &TestClient) -> Connection {
        self.connect_with(&client.connector).await
    }

    /// Completes a handshake between the connector's client and the server over a new in-memory
    /// connection.
    pub async fn connect_with(&self, connector: &TlsConnector) -> Connection {
        let (client_io, server_io) = Self::pipe();
        let (server, client_stream) = tokio::join!(
            self.acceptor.accept(server_io),
            connector.connect(Self::server_name(), client_io),
        );
        Connection {
            client: client_stream,
            server,
        }
    }

    /// Completes a handshake in which the client offers `protocols` in order and trusts this
    /// fixture's authority, over a new in-memory connection.
    pub async fn connect_offering(&self, protocols: &[&[u8]]) -> Connection {
        let config = client_config(
            ServerTrust::Ca(self.ca_certificate()),
            None,
            protocols,
            self.clock(),
        )
        .expect("test client config builds");
        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        let (client_io, server_io) = Self::pipe();
        let (server, client) = tokio::join!(self.acceptor.accept(server_io), async move {
            connector
                .connect(Self::server_name(), client_io)
                .await
                .map_err(TransportError::Handshake)
        },);
        Connection { client, server }
    }

    /// A raw rustls client configuration for negative tests.
    ///
    /// `auth` selects the client certificate, or none. `tls12_only` restricts the protocol to
    /// TLS 1.2; otherwise it is TLS 1.3, which is the only version the transport accepts.
    /// `resumption` keeps rustls' default session resumption, or disables it.
    pub fn raw_client_config(
        &self,
        auth: Option<&TestClient>,
        tls12_only: bool,
        resumption: bool,
    ) -> Arc<ClientConfig> {
        let mut roots = RootCertStore::empty();
        roots
            .add(self.ca.der.clone())
            .expect("test ca is a valid trust anchor");
        let versions: &[&SupportedProtocolVersion] = if tls12_only { &[&TLS12] } else { &[&TLS13] };
        let builder = ClientConfig::builder_with_provider(Arc::new(default_provider()))
            .with_protocol_versions(versions)
            .expect("test protocol versions are supported")
            .with_root_certificates(roots);
        let mut config = match auth {
            Some(client) => builder
                .with_client_auth_cert(
                    vec![client.der.clone()],
                    PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(client.key_der.clone())),
                )
                .expect("test client key matches its certificate"),
            None => builder.with_no_client_auth(),
        };
        config.alpn_protocols = vec![ALPN_H2.to_vec()];
        if !resumption {
            config.resumption = Resumption::disabled();
        }
        Arc::new(config)
    }

    /// A raw rustls server configuration presenting this fixture's server certificate, with the
    /// certificates selected by `chain`. It negotiates TLS 1.3 and h2 and asks for no client
    /// certificate.
    pub fn raw_server_config(&self, chain: ServerChain) -> Arc<ServerConfig> {
        let mut certificates = vec![self.server.der.clone()];
        if chain == ServerChain::LeafAndCa {
            certificates.push(self.ca.der.clone());
        }
        server_config(certificates, self.server.key_der.clone())
    }

    /// A raw rustls server configuration presenting a leaf and CA issued by an authority other
    /// than this fixture's. Its certificates are valid for `localhost`.
    pub fn foreign_server_config(&self) -> Arc<ServerConfig> {
        let foreign = TestAuthority::new("gang foreign ca").expect("test ca builds");
        let leaf = foreign
            .issue(
                vec!["localhost".to_owned()],
                vec![ExtendedKeyUsagePurpose::ServerAuth],
                serial_bytes(&self.serials, 0),
                "localhost",
            )
            .expect("test leaf issues");
        server_config(vec![leaf.der, foreign.der], leaf.key_der)
    }

    /// A raw rustls server configuration presenting a leaf for `dns_name` with `usages`, issued by
    /// this fixture's authority, followed by that authority's certificate.
    pub fn issued_server_config(
        &self,
        dns_name: &str,
        usages: Vec<ExtendedKeyUsagePurpose>,
    ) -> Arc<ServerConfig> {
        let leaf = self
            .ca
            .issue(
                vec![dns_name.to_owned()],
                usages,
                serial_bytes(&self.serials, 0x31),
                dns_name,
            )
            .expect("test leaf issues");
        server_config(vec![leaf.der, self.ca.der.clone()], leaf.key_der)
    }

    /// A raw rustls server configuration presenting a `localhost` leaf signed by an attacker key,
    /// under an authority that reuses the fixture CA's name, followed by the genuine CA
    /// certificate. Only the appended CA matches the pin; the leaf does not chain to it.
    pub fn forged_server_config(&self) -> Arc<ServerConfig> {
        let attacker = TestAuthority::new(CA_COMMON_NAME).expect("test ca builds");
        let leaf = attacker
            .issue(
                vec!["localhost".to_owned()],
                vec![ExtendedKeyUsagePurpose::ServerAuth],
                serial_bytes(&self.serials, 0x32),
                "localhost",
            )
            .expect("test leaf issues");
        server_config(vec![leaf.der, self.ca.der.clone()], leaf.key_der)
    }

    fn client_from(&self, leaf: TestLeaf) -> TestClient {
        let identity = ClientIdentity {
            certificate: &leaf.pem,
            private_key: &leaf.key_pem,
        };
        let connector = self
            .build_connector(
                ServerTrust::Ca(self.ca_certificate()),
                Some(identity),
                self.handshake_timeout,
            )
            .expect("test client connector builds");
        TestClient {
            serial: leaf.serial,
            connector,
            pem: leaf.pem,
            key_pem: leaf.key_pem,
            der: leaf.der,
            key_der: leaf.key_der,
        }
    }

    fn build_connector(
        &self,
        trust: ServerTrust<'_>,
        identity: Option<ClientIdentity<'_>>,
        handshake_timeout: NonZeroDuration,
    ) -> Result<TlsConnector, TransportError> {
        TlsConnector::try_new(&ConnectorSettings {
            trust,
            identity,
            handshake_timeout,
            clock: self.clock(),
            alpn: Alpn::H2,
        })
    }
}

/// Drives a handshake from `connector` against `server`, returning the client result.
pub async fn refused_by(
    connector: TlsConnector,
    server: tokio_rustls::TlsAcceptor,
) -> Result<(), TransportError> {
    let (client_io, server_io) = TransportFixture::pipe();
    let (client, _server) = tokio::join!(
        connector.connect(TransportFixture::server_name(), client_io),
        server.accept(server_io),
    );
    client.map(drop)
}

/// The rustls certificate error at the root of a failed client handshake.
pub fn certificate_refusal<T>(result: Result<T, TransportError>) -> CertificateError {
    let Err(TransportError::Handshake(error)) = result else {
        panic!("the handshake is refused with a handshake error");
    };
    match error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<RustlsError>())
    {
        Some(RustlsError::InvalidCertificate(reason)) => reason.clone(),
        other => panic!("the refusal is not a certificate error: {other:?}"),
    }
}

/// Sends one line from `client` to `server` and back, asserting the echo is intact.
pub async fn echo_once(
    client: &mut (impl AsyncRead + AsyncWrite + Unpin),
    server: &mut (impl AsyncRead + AsyncWrite + Unpin),
) {
    client
        .write_all(ECHO)
        .await
        .expect("client writes the echo line");
    client.flush().await.expect("client flushes the echo line");

    let mut received = vec![0u8; ECHO.len()];
    server
        .read_exact(&mut received)
        .await
        .expect("server reads the echo line");
    server
        .write_all(&received)
        .await
        .expect("server writes the echo line back");
    server.flush().await.expect("server flushes the echo line");

    let mut echoed = vec![0u8; ECHO.len()];
    client
        .read_exact(&mut echoed)
        .await
        .expect("client reads the echo line back");
    assert_eq!(echoed.as_slice(), ECHO);
}

/// A [`Listener`] fed by a test through [`FakeFeed`], with no sockets.
pub struct FakeListener {
    connections: mpsc::UnboundedReceiver<io::Result<(DuplexStream, u32)>>,
    _dropped: oneshot::Sender<()>,
}

/// The test side of a [`FakeListener`]: queues connections and observes when the listener is
/// dropped.
pub struct FakeFeed {
    connections: mpsc::UnboundedSender<io::Result<(DuplexStream, u32)>>,
    dropped: oneshot::Receiver<()>,
}

impl FakeListener {
    /// A listener and the feed that controls it.
    pub fn new() -> (Self, FakeFeed) {
        let (connections_tx, connections_rx) = mpsc::unbounded_channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        (
            Self {
                connections: connections_rx,
                _dropped: dropped_tx,
            },
            FakeFeed {
                connections: connections_tx,
                dropped: dropped_rx,
            },
        )
    }
}

impl FakeFeed {
    /// Queues the next result of `accept`. Queuing after the listener was dropped has no effect.
    pub fn push(&self, connection: io::Result<(DuplexStream, u32)>) {
        let _ = self.connections.send(connection);
    }

    /// Completes once the listener has been dropped.
    pub async fn listener_dropped(&mut self) {
        let _ = (&mut self.dropped).await;
    }
}

impl Listener for FakeListener {
    type Io = DuplexStream;
    type Addr = u32;

    /// Fails with `UnexpectedEof` once the feed is dropped, so an accept loop that outlives its
    /// feed reports a closed listener instead of waiting forever.
    async fn accept(&mut self) -> io::Result<(DuplexStream, u32)> {
        match self.connections.recv().await {
            Some(connection) => connection,
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the fake feed was dropped",
            )),
        }
    }
}
