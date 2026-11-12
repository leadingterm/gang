//! Pinned server trust: `ServerTrust::Pin` accepts a leaf only when its chain carries the CA whose
//! `SubjectPublicKeyInfo` digest is pinned, and `ca_pin` derivation is stable and round-trips.

use std::str::FromStr;
use std::sync::Arc;

use gang_testkit::{ExtendedKeyUsagePurpose, TestAuthority};
use gang_tls::{ClientAuth, ServerTrust, TransportError, ca_pin_of_certificate, client_config};
use gang_types::CaPin;
use rustls::CertificateError;
use x509_cert::der::{Decode, Encode};

use crate::support::{ServerChain, TransportFixture, certificate_refusal, echo_once, refused_by};

#[tokio::test(start_paused = true)]
async fn pinned_client_without_identity_reaches_optional_acceptor() {
    let fixture = TransportFixture::with_client_auth(ClientAuth::Optional);
    let pin = fixture.ca_pin();
    let connector = fixture.pinned_connector(&pin, None);
    let (client_io, server_io) = TransportFixture::pipe();

    let (client, server) = tokio::join!(
        connector.connect(TransportFixture::server_name(), client_io),
        fixture.acceptor().accept(server_io),
    );
    let mut client = client.expect("pinned client handshake completes");
    let mut server = server.expect("optional acceptor admits a client without a certificate");

    assert!(
        server.peer_serial().is_none(),
        "no client certificate was presented"
    );
    echo_once(&mut client, &mut server).await;
}

#[tokio::test(start_paused = true)]
async fn pinned_client_with_identity_reaches_optional_acceptor_with_peer() {
    let fixture = TransportFixture::with_client_auth(ClientAuth::Optional);
    let pin = fixture.ca_pin();
    let identity = fixture.client(30, "pinned");
    let connector = fixture.pinned_connector(&pin, Some(&identity));
    let (client_io, server_io) = TransportFixture::pipe();

    let (client, server) = tokio::join!(
        connector.connect(TransportFixture::server_name(), client_io),
        fixture.acceptor().accept(server_io),
    );
    let mut client = client.expect("pinned client handshake completes");
    let mut server = server.expect("optional acceptor admits the client certificate");

    assert_eq!(
        server.peer_serial(),
        Some(&identity.serial),
        "the presented client certificate is the peer"
    );
    echo_once(&mut client, &mut server).await;
}

#[tokio::test(start_paused = true)]
async fn wrong_pin_is_refused_by_the_client() {
    let fixture = TransportFixture::with_client_auth(ClientAuth::Optional);
    let wrong = CaPin::new([0x42; 32]);
    assert_ne!(
        wrong,
        fixture.ca_pin(),
        "the wrong pin must differ from the CA pin"
    );
    let connector = fixture.pinned_connector(&wrong, None);
    let (client_io, server_io) = TransportFixture::pipe();

    let (client, _server) = tokio::join!(
        connector.connect(TransportFixture::server_name(), client_io),
        fixture.acceptor().accept(server_io),
    );

    assert!(
        matches!(client, Err(TransportError::Handshake(_))),
        "a pin that matches no certificate in the chain refuses the handshake"
    );
}

#[tokio::test(start_paused = true)]
async fn leaf_only_chain_is_refused_under_the_pin() {
    let fixture = TransportFixture::with_client_auth(ClientAuth::Optional);
    let pin = fixture.ca_pin();
    let server = tokio_rustls::TlsAcceptor::from(fixture.raw_server_config(ServerChain::LeafOnly));

    let result = refused_by(fixture.pinned_connector(&pin, None), server).await;

    assert!(
        matches!(result, Err(TransportError::Handshake(_))),
        "without the CA in the chain the pin cannot be satisfied"
    );
}

#[tokio::test(start_paused = true)]
async fn foreign_chain_is_refused_under_the_pin() {
    let fixture = TransportFixture::with_client_auth(ClientAuth::Optional);
    let pin = fixture.ca_pin();
    let server = tokio_rustls::TlsAcceptor::from(fixture.foreign_server_config());

    let result = refused_by(fixture.pinned_connector(&pin, None), server).await;

    assert!(
        matches!(result, Err(TransportError::Handshake(_))),
        "a chain issued by another CA is refused under the fixture's pin"
    );
}

#[tokio::test(start_paused = true)]
async fn forged_leaf_is_refused_despite_a_genuine_ca_in_the_chain() {
    let fixture = TransportFixture::with_client_auth(ClientAuth::Optional);
    let pin = fixture.ca_pin();
    let genuine =
        tokio_rustls::TlsAcceptor::from(fixture.raw_server_config(ServerChain::LeafAndCa));
    let forged = tokio_rustls::TlsAcceptor::from(fixture.forged_server_config());

    let control = refused_by(fixture.pinned_connector(&pin, None), genuine).await;
    assert!(
        control.is_ok(),
        "control: the genuine leaf under the same CA chain is accepted, got {control:?}"
    );

    let result = refused_by(fixture.pinned_connector(&pin, None), forged).await;
    let refusal = certificate_refusal(result);
    assert!(
        matches!(refusal, CertificateError::BadSignature),
        "the forged leaf names the pinned CA as issuer but fails signature verification, got {refusal:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn leaf_for_another_name_is_refused_under_the_pin() {
    let fixture = TransportFixture::with_client_auth(ClientAuth::Optional);
    let pin = fixture.ca_pin();
    let matching = tokio_rustls::TlsAcceptor::from(
        fixture.issued_server_config("localhost", vec![ExtendedKeyUsagePurpose::ServerAuth]),
    );
    let wrong_name = tokio_rustls::TlsAcceptor::from(fixture.issued_server_config(
        "other.example.test",
        vec![ExtendedKeyUsagePurpose::ServerAuth],
    ));

    let control = refused_by(fixture.pinned_connector(&pin, None), matching).await;
    assert!(
        control.is_ok(),
        "control: a localhost leaf from the pinned CA is accepted, got {control:?}"
    );

    let result = refused_by(fixture.pinned_connector(&pin, None), wrong_name).await;
    let refusal = certificate_refusal(result);
    assert!(
        matches!(refusal, CertificateError::NotValidForNameContext { .. }),
        "a leaf for another DNS name is refused for localhost, got {refusal:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn leaf_without_server_auth_usage_is_refused_under_the_pin() {
    let fixture = TransportFixture::with_client_auth(ClientAuth::Optional);
    let pin = fixture.ca_pin();
    let server_auth = tokio_rustls::TlsAcceptor::from(
        fixture.issued_server_config("localhost", vec![ExtendedKeyUsagePurpose::ServerAuth]),
    );
    let client_auth_only = tokio_rustls::TlsAcceptor::from(
        fixture.issued_server_config("localhost", vec![ExtendedKeyUsagePurpose::ClientAuth]),
    );

    let control = refused_by(fixture.pinned_connector(&pin, None), server_auth).await;
    assert!(
        control.is_ok(),
        "control: a serverAuth leaf from the pinned CA is accepted, got {control:?}"
    );

    let result = refused_by(fixture.pinned_connector(&pin, None), client_auth_only).await;
    let refusal = certificate_refusal(result);
    assert!(
        matches!(refusal, CertificateError::InvalidPurposeContext { .. }),
        "a leaf without the serverAuth usage is refused as a server certificate, got {refusal:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn pin_config_negotiates_alpn_with_the_fixture_acceptor() {
    let fixture = TransportFixture::with_client_auth(ClientAuth::Optional);
    let pin = fixture.ca_pin();
    let config = client_config(
        ServerTrust::Pin(&pin),
        None,
        &[&b"h2"[..], &b"http/1.1"[..]],
        fixture.clock(),
    )
    .expect("pin client config builds");
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let (client_io, server_io) = TransportFixture::pipe();

    let (client, server) = tokio::join!(
        connector.connect(TransportFixture::server_name(), client_io),
        fixture.acceptor().accept(server_io),
    );
    let client = client.expect("pin client config handshake completes");
    let _server = server.expect("fixture acceptor completes the handshake");

    assert_eq!(client.get_ref().1.alpn_protocol(), Some(&b"h2"[..]));
}

#[test]
fn ca_pin_of_certificate_matches_spki_and_round_trips_text() {
    let authority = TestAuthority::new("gang pin ca").expect("test ca builds");
    let pin = ca_pin_of_certificate(&authority.pem).expect("ca certificate yields a pin");

    let parsed = x509_cert::Certificate::from_der(authority.der.as_ref())
        .expect("test ca certificate parses");
    let spki = parsed
        .tbs_certificate()
        .subject_public_key_info()
        .to_der()
        .expect("test ca spki encodes");
    assert_eq!(pin, CaPin::of_spki(&spki));
    let text = pin.to_string();
    assert_eq!(
        CaPin::from_str(&text).expect("pin text parses back"),
        pin,
        "the pin round-trips through its text form"
    );
}

#[test]
fn fixture_ca_pin_is_derived_from_its_certificate() {
    let fixture = TransportFixture::new();

    let derived = ca_pin_of_certificate(fixture.ca_certificate())
        .expect("fixture ca certificate yields a pin");

    assert_eq!(derived, fixture.ca_pin());
}
