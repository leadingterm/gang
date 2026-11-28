//! Trusted PROXY v2 prefaces, before either HTTP or TLS sees a connection.

use std::io;
use std::net::IpAddr;
use std::sync::Arc;

use gang_clock::Clock;
use gang_config::ProxyProtocolConfig;
use gang_tls::Listener;
use proxy_header::{ParseConfig, Protocol, ProxyHeader};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::task::JoinSet;

use crate::client_address::canonical_network;
use crate::errors::ProxyProtocolError;

const SIGNATURE: &[u8; 12] = b"\r\n\r\n\0\r\nQUIT\n";

#[derive(Clone, Copy)]
pub(crate) enum HeaderKind {
    Application,
    Health,
}

fn trusted(config: &ProxyProtocolConfig, raw: Option<IpAddr>) -> bool {
    raw.is_some_and(|ip| {
        let ip = ip.to_canonical();
        config
            .trusted_cidrs
            .iter()
            .any(|network| canonical_network(*network).contains(&ip))
    })
}

pub(crate) async fn prepare<IO: AsyncRead + Unpin>(
    io: IO,
    raw: Option<IpAddr>,
    config: &ProxyProtocolConfig,
    kind: HeaderKind,
) -> Result<(IO, Option<IpAddr>), ProxyProtocolError> {
    let raw = raw.map(|ip| ip.to_canonical());
    let is_trusted = trusted(config, raw);
    // An otherwise untrusted local health probe may not claim a proxy source.
    let local_only =
        !is_trusted && matches!(kind, HeaderKind::Health) && raw.is_some_and(|ip| ip.is_loopback());
    if !is_trusted && !local_only {
        return Err(ProxyProtocolError::Untrusted);
    }
    prepare_trusted(io, raw, config, kind, local_only).await
}

async fn prepare_trusted<IO: AsyncRead + Unpin>(
    mut io: IO,
    raw: Option<IpAddr>,
    config: &ProxyProtocolConfig,
    kind: HeaderKind,
    local_only: bool,
) -> Result<(IO, Option<IpAddr>), ProxyProtocolError> {
    let mut stack = [0u8; 52];
    io.read_exact(&mut stack[..12]).await?;
    if &stack[..12] != SIGNATURE {
        return Err(ProxyProtocolError::Invalid);
    }
    io.read_exact(&mut stack[12..16]).await?;
    if !matches!(
        (kind, stack[12]),
        (HeaderKind::Application, 0x21) | (HeaderKind::Health, 0x20 | 0x21)
    ) {
        return Err(ProxyProtocolError::Invalid);
    }
    if local_only && stack[12] != 0x20 {
        return Err(ProxyProtocolError::Invalid);
    }
    let length = 16 + usize::from(u16::from_be_bytes([stack[14], stack[15]]));
    if u64::try_from(length).map_err(|_| ProxyProtocolError::TooLarge)?
        > config.max_header_size.get()
    {
        return Err(ProxyProtocolError::TooLarge);
    }
    let source = if length <= stack.len() {
        io.read_exact(&mut stack[16..length]).await?;
        decode(&stack[..length], kind)?
    } else {
        let mut frame = vec![0; length];
        frame[..16].copy_from_slice(&stack[..16]);
        io.read_exact(&mut frame[16..]).await?;
        decode(&frame, kind)?
    };
    Ok((io, source.or(raw)))
}

fn decode(frame: &[u8], kind: HeaderKind) -> Result<Option<IpAddr>, ProxyProtocolError> {
    let (header, consumed) = ProxyHeader::parse(
        frame,
        ParseConfig {
            allow_v1: false,
            allow_v2: true,
            include_tlvs: true,
        },
    )
    .map_err(|_| ProxyProtocolError::Invalid)?;
    if consumed != frame.len() {
        return Err(ProxyProtocolError::Invalid);
    }
    for tlv in header.tlvs() {
        tlv.map_err(|_| ProxyProtocolError::Invalid)?;
    }
    match header.proxied_address() {
        Some(address) if address.protocol == Protocol::Stream => {
            Ok(Some(address.source.ip().to_canonical()))
        }
        None if matches!(kind, HeaderKind::Health) => Ok(None),
        _ => Err(ProxyProtocolError::Source),
    }
}

pub(crate) struct ProxyAddress<A> {
    raw: A,
    effective: Option<IpAddr>,
}

impl<A: std::fmt::Debug> std::fmt::Debug for ProxyAddress<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyAddress")
            .field("raw", &self.raw)
            .field("effective", &self.effective)
            .finish()
    }
}

type PendingConnection<I, A> = Result<(I, A, Option<IpAddr>), ProxyProtocolError>;

pub(crate) struct ProxyListener<L: Listener, C> {
    listener: L,
    clock: Arc<C>,
    config: Option<Arc<ProxyProtocolConfig>>,
    pending: JoinSet<PendingConnection<L::Io, L::Addr>>,
}

impl<L: Listener, C: Clock> ProxyListener<L, C> {
    pub(crate) fn new(
        listener: L,
        clock: Arc<C>,
        config: Option<Arc<ProxyProtocolConfig>>,
    ) -> Self {
        Self {
            listener,
            clock,
            config,
            pending: JoinSet::new(),
        }
    }
}

impl<L: Listener, C: Clock + 'static> Listener for ProxyListener<L, C> {
    type Io = L::Io;
    type Addr = ProxyAddress<L::Addr>;

    async fn accept(&mut self) -> io::Result<(Self::Io, Self::Addr)> {
        loop {
            tokio::select! {
                result = self.pending.join_next(), if !self.pending.is_empty() => match result {
                    Some(Ok(Ok((io, raw, effective)))) => return Ok((io, ProxyAddress { raw, effective })),
                    Some(Ok(Err(error))) => tracing::debug!(%error, "proxy preface rejected"),
                    Some(Err(error)) => tracing::debug!(%error, "proxy preface task failed"),
                    None => {},
                },
                accepted = self.listener.accept(), if self.config.as_ref().is_none_or(|config| self.pending.len() < config.max_pending_headers.get()) => {
                    let (io, raw) = accepted?;
                    let ip = L::peer_ip(&raw).map(|ip| ip.to_canonical());
                    let Some(config) = self.config.as_ref() else {
                        return Ok((io, ProxyAddress { raw, effective: ip }));
                    };
                    if !trusted(config, ip) {
                        tracing::debug!(?raw, "untrusted proxy peer rejected");
                        continue;
                    }
                    let config = Arc::clone(config);
                    let deadline = self.clock.timeout(config.header_timeout.get(), async move {
                        let (io, effective) = prepare_trusted(io, ip, &config, HeaderKind::Application, false).await?;
                        Ok((io, raw, effective))
                    });
                    self.pending.spawn(async move { deadline.await.map_err(|_| ProxyProtocolError::Timeout)? });
                }
            }
        }
    }

    fn peer_ip(addr: &Self::Addr) -> Option<IpAddr> {
        addr.effective
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use gang_config::ProxyProtocolConfig;
    use gang_tls::Listener;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::DuplexStream;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::mpsc;

    use super::{HeaderKind, ProxyListener, SIGNATURE, prepare};

    fn config() -> ProxyProtocolConfig {
        ProxyProtocolConfig {
            trusted_cidrs: vec!["10.0.1.4/32".parse().expect("valid test network")],
            ..ProxyProtocolConfig::default()
        }
    }

    fn ip(last: u8) -> IpAddr {
        Ipv4Addr::new(10, 0, 1, last).into()
    }

    fn frame() -> Vec<u8> {
        let mut bytes = SIGNATURE.to_vec();
        bytes.extend_from_slice(&[0x21, 0x11, 0, 12, 192, 0, 2, 1, 192, 0, 2, 2, 0, 1, 0, 2]);
        bytes
    }

    #[tokio::test]
    async fn loopback_local_exception_is_only_for_health() {
        let config = config();
        for raw in [
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            IpAddr::V6(Ipv4Addr::LOCALHOST.to_ipv6_mapped()),
        ] {
            let (mut client, server) = tokio::io::duplex(64);
            client
                .write_all(b"\r\n\r\n\0\r\nQUIT\n\x20\0\0\0")
                .await
                .expect("write local frame");
            let (_, source) = prepare(server, Some(raw), &config, HeaderKind::Health)
                .await
                .expect("loopback health local frame");
            assert_eq!(source, Some(raw.to_canonical()));

            let (mut client, server) = tokio::io::duplex(64);
            client.write_all(&frame()).await.expect("write proxy frame");
            assert!(
                prepare(server, Some(raw), &config, HeaderKind::Health)
                    .await
                    .is_err()
            );

            let (mut client, server) = tokio::io::duplex(64);
            client
                .write_all(b"\r\n\r\n\0\r\nQUIT\n\x20\0\0\0")
                .await
                .expect("write local frame");
            assert!(
                prepare(server, Some(raw), &config, HeaderKind::Application)
                    .await
                    .is_err()
            );
        }
        let (mut client, server) = tokio::io::duplex(64);
        client
            .write_all(b"\r\n\r\n\0\r\nQUIT\n\x20\0\0\0")
            .await
            .expect("write local frame");
        assert!(
            prepare(server, Some(ip(4)), &config, HeaderKind::Application)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn explicitly_trusted_loopback_health_can_claim_proxy_source() {
        let mut config = config();
        config
            .trusted_cidrs
            .push("127.0.0.1/32".parse().expect("valid test network"));
        let (mut client, server) = tokio::io::duplex(64);
        client.write_all(&frame()).await.expect("write proxy frame");
        let (_, source) = prepare(
            server,
            Some(Ipv4Addr::LOCALHOST.into()),
            &config,
            HeaderKind::Health,
        )
        .await
        .expect("trusted loopback proxy frame");
        assert_eq!(source, Some(Ipv4Addr::new(192, 0, 2, 1).into()));
    }

    #[tokio::test]
    async fn only_the_exact_proxy_peer_can_claim_a_source() {
        let config = config();
        let bytes = frame();
        let (mut client, server) = tokio::io::duplex(128);
        client.write_all(&bytes).await.expect("write frame");
        client.write_all(b"GET").await.expect("write request");
        let (mut stream, actual) = prepare(server, Some(ip(4)), &config, HeaderKind::Application)
            .await
            .expect("prepare");
        assert_eq!(actual, Some(Ipv4Addr::new(192, 0, 2, 1).into()));
        let mut remaining = [0; 3];
        stream
            .read_exact(&mut remaining)
            .await
            .expect("read request");
        assert_eq!(&remaining, b"GET");
        for raw in [Some(ip(99)), None] {
            let (_client, server) = tokio::io::duplex(64);
            assert!(
                prepare(server, raw, &config, HeaderKind::Application)
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn mapped_peers_and_networks_use_canonical_ipv4_trust() {
        let mapped = |last| IpAddr::V6(Ipv4Addr::new(10, 0, 1, last).to_ipv6_mapped());
        for (network, raw) in [
            ("10.0.1.4/32", mapped(4)),
            ("::ffff:10.0.1.4/128", ip(4)),
            ("::ffff:10.0.1.4/128", mapped(4)),
            ("::ffff:10.0.1.0/120", ip(4)),
            ("::ffff:10.0.1.0/120", mapped(4)),
        ] {
            let mut config = config();
            config.trusted_cidrs = vec![network.parse().expect("valid test network")];
            let (mut client, server) = tokio::io::duplex(128);
            client.write_all(&frame()).await.expect("write frame");
            client.write_all(b"GET").await.expect("write request");
            let (mut stream, source) = prepare(server, Some(raw), &config, HeaderKind::Application)
                .await
                .expect("trusted peer");
            assert_eq!(source, Some(Ipv4Addr::new(192, 0, 2, 1).into()));
            let mut request = [0; 3];
            stream.read_exact(&mut request).await.expect("read request");
            assert_eq!(&request, b"GET");
        }

        let (_client, server) = tokio::io::duplex(128);
        assert!(
            prepare(server, Some(mapped(5)), &config(), HeaderKind::Application,)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn ipv6_frames_canonicalize_only_mapped_sources() {
        for (source, expected) in [
            (
                Ipv4Addr::new(192, 0, 2, 1).to_ipv6_mapped(),
                IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
            ),
            (Ipv6Addr::LOCALHOST, IpAddr::V6(Ipv6Addr::LOCALHOST)),
        ] {
            let mut bytes = SIGNATURE.to_vec();
            bytes.extend_from_slice(&[0x21, 0x21, 0, 36]);
            bytes.extend_from_slice(&source.octets());
            bytes.extend_from_slice(&Ipv6Addr::LOCALHOST.octets());
            bytes.extend_from_slice(&[0, 1, 0, 2]);
            let (mut client, server) = tokio::io::duplex(128);
            client.write_all(&bytes).await.expect("write frame");
            client.write_all(b"GET").await.expect("write request");
            let (mut stream, actual) =
                prepare(server, Some(ip(4)), &config(), HeaderKind::Application)
                    .await
                    .expect("trusted peer");
            assert_eq!(actual, Some(expected));
            let mut request = [0; 3];
            stream.read_exact(&mut request).await.expect("read request");
            assert_eq!(&request, b"GET");
        }
    }

    #[tokio::test]
    async fn recognized_bad_headers_fail() {
        let config = config();
        for (command, family, length) in [
            (0x20, 0x11, 12),
            (0x31, 0x11, 12),
            (0x21, 0x12, 12),
            (0x21, 0, 0),
            (0x21, 0x11, 1),
        ] {
            let mut bytes = frame();
            bytes[12] = command;
            bytes[13] = family;
            bytes[15] = length;
            let (mut client, server) = tokio::io::duplex(128);
            client
                .write_all(&bytes)
                .await
                .expect("write malformed frame");
            assert!(
                prepare(server, Some(ip(4)), &config, HeaderKind::Application)
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn invalid_or_truncated_frames_never_reach_the_application() {
        let mut bad_tlv = frame();
        bad_tlv[15] = 15;
        bad_tlv.extend_from_slice(&[0x05, 0, 2]);
        let mut unix = SIGNATURE.to_vec();
        unix.extend_from_slice(&[0x21, 0x31, 0, 0]);
        for bytes in [
            b"PROXY TCP4 192.0.2.1 192.0.2.2 1 2\r\n".to_vec(),
            SIGNATURE[..7].to_vec(),
            bad_tlv,
            unix,
        ] {
            let (mut client, server) = tokio::io::duplex(128);
            client
                .write_all(&bytes)
                .await
                .expect("write invalid preface");
            client.shutdown().await.expect("end input");
            assert!(
                prepare(server, Some(ip(4)), &config(), HeaderKind::Application)
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn invalid_command_and_oversize_fail_without_waiting_for_payload() {
        for fixed in [[0x31, 0x11, 0, 100], [0x21, 0x11, 0xff, 0xff]] {
            let (mut client, server) = tokio::io::duplex(64);
            client.write_all(SIGNATURE).await.expect("write signature");
            client.write_all(&fixed).await.expect("write fixed header");
            assert!(
                prepare(server, Some(ip(4)), &config(), HeaderKind::Application)
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn health_accepts_a_local_frame_without_a_source() {
        let mut bytes = SIGNATURE.to_vec();
        bytes.extend_from_slice(&[0x20, 0, 0, 0]);
        let (mut client, server) = tokio::io::duplex(64);
        client.write_all(&bytes).await.expect("write local frame");
        client.write_all(b"GET").await.expect("write request");
        let (mut stream, source) = prepare(server, Some(ip(4)), &config(), HeaderKind::Health)
            .await
            .expect("local health");
        assert_eq!(source, Some(ip(4)));
        let mut request = [0; 3];
        stream.read_exact(&mut request).await.expect("read request");
        assert_eq!(&request, b"GET");
        let (mut client, server) = tokio::io::duplex(64);
        bytes[12] = 0x21;
        client
            .write_all(&bytes)
            .await
            .expect("write sourceless proxy frame");
        let (_, source) = prepare(server, Some(ip(4)), &config(), HeaderKind::Health)
            .await
            .expect("sourceless health proxy");
        assert_eq!(source, Some(ip(4)));
    }

    #[tokio::test]
    async fn fragmented_ipv6_header_with_tlv_leaves_application_bytes() {
        let mut bytes = SIGNATURE.to_vec();
        bytes.extend_from_slice(&[0x21, 0x21, 0, 40]);
        bytes.extend_from_slice(&Ipv6Addr::LOCALHOST.octets());
        bytes.extend_from_slice(&Ipv6Addr::UNSPECIFIED.octets());
        bytes.extend_from_slice(&[0, 1, 0, 2, 0x05, 0, 1, 42]);
        let (mut client, server) = tokio::io::duplex(8);
        let writer = async move {
            for byte in bytes {
                client.write_all(&[byte]).await.expect("write fragment");
            }
            client
                .write_all(b"TLS")
                .await
                .expect("write application bytes");
        };
        let reader = async {
            let (mut stream, source) =
                prepare(server, Some(ip(4)), &config(), HeaderKind::Application)
                    .await
                    .expect("parse ipv6");
            assert_eq!(source, Some(Ipv6Addr::LOCALHOST.into()));
            let mut data = [0; 3];
            stream
                .read_exact(&mut data)
                .await
                .expect("read application bytes");
            assert_eq!(&data, b"TLS");
        };
        tokio::join!(writer, reader);
    }

    #[tokio::test]
    async fn fragmented_ipv4_header_accepts_noop_and_aws_tlvs() {
        let mut bytes = frame();
        bytes[15] = 21;
        bytes.extend_from_slice(&[0x04, 0, 0, 0xea, 0, 3, 1, 2, 3]);
        let (mut client, server) = tokio::io::duplex(8);
        let writer = async move {
            for byte in bytes {
                client.write_all(&[byte]).await.expect("write fragment");
            }
            client.write_all(b"HTTP").await.expect("write payload");
        };
        let reader = async {
            let (mut stream, source) =
                prepare(server, Some(ip(4)), &config(), HeaderKind::Application)
                    .await
                    .expect("parse ipv4 with tlvs");
            assert_eq!(source, Some(Ipv4Addr::new(192, 0, 2, 1).into()));
            let mut payload = [0; 4];
            stream.read_exact(&mut payload).await.expect("read payload");
            assert_eq!(&payload, b"HTTP");
        };
        tokio::join!(writer, reader);
    }

    #[tokio::test]
    async fn malformed_proxy_commands_families_and_tlvs_are_rejected() {
        let mut cases = Vec::new();
        for (command, family) in [
            (0x11, 0x11), // version 1
            (0x22, 0x11), // unknown command
            (0x2f, 0x11),
            (0x21, 0x22), // IPv6 datagram
            (0x21, 0x32), // Unix datagram
            (0x21, 0x41), // unknown address family
            (0x21, 0x10), // unspecified transport
        ] {
            let mut bytes = frame();
            bytes[12] = command;
            bytes[13] = family;
            cases.push(bytes);
        }
        for tlv in [&[0x05][..], &[0x05, 0][..], &[0x05, 0, 2, 42][..]] {
            let mut bytes = frame();
            bytes[15] = 12 + u8::try_from(tlv.len()).expect("small TLV");
            bytes.extend_from_slice(tlv);
            cases.push(bytes);
        }
        for bytes in cases {
            let (mut client, server) = tokio::io::duplex(128);
            client
                .write_all(&bytes)
                .await
                .expect("write malformed header");
            client.shutdown().await.expect("finish malformed header");
            assert!(
                prepare(server, Some(ip(4)), &config(), HeaderKind::Application)
                    .await
                    .is_err(),
                "accepted malformed frame {bytes:?}"
            );
        }
    }

    struct AddressedListener {
        incoming: mpsc::UnboundedReceiver<DuplexStream>,
        accepted: mpsc::UnboundedSender<()>,
        peer: IpAddr,
    }

    impl Listener for AddressedListener {
        type Io = DuplexStream;
        type Addr = IpAddr;

        async fn accept(&mut self) -> std::io::Result<(Self::Io, Self::Addr)> {
            match self.incoming.recv().await {
                Some(io) => {
                    let _ = self.accepted.send(());
                    Ok((io, self.peer))
                }
                None => std::future::pending().await,
            }
        }

        fn peer_ip(address: &Self::Addr) -> Option<IpAddr> {
            Some(*address)
        }
    }

    fn addressed() -> (
        AddressedListener,
        mpsc::UnboundedSender<DuplexStream>,
        mpsc::UnboundedReceiver<()>,
    ) {
        let (sender, incoming) = mpsc::unbounded_channel();
        let (accepted, notifications) = mpsc::unbounded_channel();
        (
            AddressedListener {
                incoming,
                accepted,
                peer: ip(4),
            },
            sender,
            notifications,
        )
    }

    fn dial(sender: &mpsc::UnboundedSender<DuplexStream>) -> DuplexStream {
        let (client, server) = tokio::io::duplex(128);
        sender.send(server).expect("listener alive");
        client
    }

    #[tokio::test]
    async fn listener_canonicalizes_mapped_direct_and_trusted_peers() {
        for configured in [false, true] {
            let (mut inner, sender, _) = addressed();
            inner.peer = IpAddr::V6(Ipv4Addr::new(10, 0, 1, 4).to_ipv6_mapped());
            let policy = configured.then(|| Arc::new(config()));
            let mut listener =
                ProxyListener::new(inner, crate::test_support_clock::start(), policy);
            let mut client = dial(&sender);
            if configured {
                client.write_all(&frame()).await.expect("write frame");
            }
            client.write_all(b"GET").await.expect("write request");
            let (mut stream, address) = listener.accept().await.expect("accept connection");
            let expected = if configured {
                Ipv4Addr::new(192, 0, 2, 1).into()
            } else {
                ip(4)
            };
            assert_eq!(
                ProxyListener::<AddressedListener, gang_clock::TestClock>::peer_ip(&address),
                Some(expected)
            );
            let mut remaining = [0; 3];
            stream
                .read_exact(&mut remaining)
                .await
                .expect("read stream");
            assert_eq!(&remaining, b"GET");
        }
    }

    struct ObservedStream {
        io: DuplexStream,
        read: usize,
        completed: mpsc::UnboundedSender<()>,
    }

    impl tokio::io::AsyncRead for ObservedStream {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let before = buf.filled().len();
            let result = std::pin::Pin::new(&mut self.io).poll_read(cx, buf);
            if matches!(result, std::task::Poll::Ready(Ok(()))) {
                let previous = self.read;
                self.read += buf.filled().len() - before;
                if previous < 28 && self.read >= 28 {
                    let _ = self.completed.send(());
                }
            }
            result
        }
    }

    impl tokio::io::AsyncWrite for ObservedStream {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::pin::Pin::new(&mut self.io).poll_write(cx, buf)
        }

        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.io).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.io).poll_shutdown(cx)
        }
    }

    struct ObservedListener {
        inner: AddressedListener,
        completed: mpsc::UnboundedSender<()>,
    }

    impl Listener for ObservedListener {
        type Io = ObservedStream;
        type Addr = IpAddr;

        async fn accept(&mut self) -> std::io::Result<(Self::Io, Self::Addr)> {
            let (io, address) = self.inner.accept().await?;
            Ok((
                ObservedStream {
                    io,
                    read: 0,
                    completed: self.completed.clone(),
                },
                address,
            ))
        }

        fn peer_ip(address: &Self::Addr) -> Option<IpAddr> {
            Some(*address)
        }
    }

    #[tokio::test]
    async fn completed_undelivered_headers_hold_pending_capacity() {
        let (inner, sender, mut accepted) = addressed();
        let (completed, mut completions) = mpsc::unbounded_channel();
        let mut config = config();
        config.max_pending_headers = std::num::NonZeroUsize::new(2).expect("nonzero");
        let mut listener = ProxyListener::new(
            ObservedListener { inner, completed },
            crate::test_support_clock::start(),
            Some(Arc::new(config)),
        );
        let mut first = dial(&sender);
        let mut second = dial(&sender);
        let mut third = dial(&sender);
        let mut accept = Box::pin(listener.accept());
        for _ in 0..2 {
            tokio::select! {
                notification = accepted.recv() => { notification.expect("raw accept"); }
                _ = &mut accept => panic!("incomplete header was delivered"),
            }
        }
        drop(accept);
        first
            .write_all(&frame())
            .await
            .expect("complete first header");
        second
            .write_all(&frame())
            .await
            .expect("complete second header");
        completions.recv().await.expect("first header read");
        completions.recv().await.expect("second header read");
        assert_eq!(listener.pending.len(), 2);
        assert!(
            accepted.try_recv().is_err(),
            "third raw stream was accepted at capacity"
        );

        let (first_result, _) = listener.accept().await.expect("deliver completed header");
        assert!(
            accepted.try_recv().is_err(),
            "third raw stream was accepted before a result was consumed"
        );
        drop(first_result);
        third
            .write_all(&frame())
            .await
            .expect("complete third header");
        let (second_result, _) = listener.accept().await.expect("deliver another header");
        drop(second_result);
        let (third_result, _) = listener.accept().await.expect("deliver remaining header");
        drop(third_result);
        accepted
            .recv()
            .await
            .expect("third raw accept after capacity freed");
    }

    #[tokio::test]
    async fn stalled_header_does_not_block_another_connection() {
        let (listener, sender, _) = addressed();
        let mut listener = ProxyListener::new(
            listener,
            crate::test_support_clock::start(),
            Some(Arc::new(config())),
        );
        let mut stalled = dial(&sender);
        stalled
            .write_all(&SIGNATURE[..3])
            .await
            .expect("partial signature");
        let mut ready = dial(&sender);
        ready.write_all(&frame()).await.expect("complete frame");
        let (mut stream, address) = listener.accept().await.expect("ready connection");
        assert_eq!(
            ProxyListener::<AddressedListener, gang_clock::TestClock>::peer_ip(&address),
            Some(Ipv4Addr::new(192, 0, 2, 1).into())
        );
        ready.write_all(b"x").await.expect("send data");
        assert_eq!(stream.read_u8().await.expect("receive data"), b'x');
    }

    #[tokio::test]
    async fn deadline_frees_pending_capacity_and_drop_closes_pending_connections() {
        let (listener, sender, mut accepted) = addressed();
        let clock = crate::test_support_clock::start();
        let mut config = config();
        config.max_pending_headers = std::num::NonZeroUsize::new(1).expect("nonzero");
        let mut listener = ProxyListener::new(listener, Arc::clone(&clock), Some(Arc::new(config)));
        let mut first = dial(&sender);
        first
            .write_all(&SIGNATURE[..2])
            .await
            .expect("partial signature");
        let mut second = dial(&sender);
        second.write_all(&frame()).await.expect("complete frame");
        let mut accept = Box::pin(listener.accept());
        tokio::select! {
            _ = accepted.recv() => {},
            _ = &mut accept => panic!("stalled header was accepted"),
        }
        clock.advance(Duration::from_secs(10));
        let (stream, _) = accept.await.expect("second connection");
        accepted.recv().await.expect("second raw accept");
        drop(stream);
        let mut byte = [0; 1];
        assert_eq!(
            first
                .read(&mut byte)
                .await
                .expect("closed first connection"),
            0
        );
        let mut third = dial(&sender);
        third
            .write_all(&SIGNATURE[..2])
            .await
            .expect("partial signature");
        let mut accept = Box::pin(listener.accept());
        tokio::select! {
            _ = accepted.recv() => {},
            _ = &mut accept => panic!("partial header was accepted"),
        }
        drop(accept);
        drop(listener);
        assert_eq!(
            third
                .read(&mut byte)
                .await
                .expect("closed pending connection"),
            0
        );
    }
}
