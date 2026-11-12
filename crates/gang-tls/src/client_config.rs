//! Client TLS configuration: how the server is trusted and whether the client presents an identity.
//!
//! [`ServerTrust::System`] trusts the roots the operating system trusts, so enterprise and proxy
//! CAs installed in the platform store work. The roots are read with `rustls-native-certs`: the
//! macOS Keychain, or certificate files on Linux (including `SSL_CERT_FILE` and `SSL_CERT_DIR`).
//! They feed a `RootCertStore` and a `WebPkiServerVerifier`, exactly as the other trust modes do,
//! so the injected clock still judges validity and the CA-pin path is unchanged.
//! `rustls-platform-verifier` is not used: it ignores the injected time provider and links the
//! platform verification libraries.
//!
//! [`ServerTrust::Bundled`] trusts the Mozilla root programme compiled into the binary through
//! `webpki-roots`, for servers on hosts without a platform trust store. It feeds the same
//! verifier as the other root-store modes and never reads the platform store.

use std::sync::Arc;

use gang_clock::Clock;
use gang_types::{CaPin, CertificatePem, PrivateKeyPem};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::{ClientConfig, WebPkiServerVerifier};
use rustls::crypto::ring::default_provider;
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::version::TLS13;
use rustls::{CertificateError, DigitallySignedStruct, Error, RootCertStore, SignatureScheme};
use rustls_native_certs::CertificateResult;
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use x509_cert::der::asn1::{AnyRef, BitStringRef, IntRef};
use x509_cert::der::{Decode, Reader, SliceReader, Tag, TagMode, TagNumber};
use x509_cert::spki::{AlgorithmIdentifierRef, SubjectPublicKeyInfoRef};
use x509_cert::{Certificate, Version};

use crate::errors::TransportError;
use crate::time::time_provider;
use crate::tls_material::{certificate_chain, private_key_der, root_store};

/// How a client decides that a server certificate is trusted.
#[derive(Debug, Clone, Copy)]
pub enum ServerTrust<'a> {
    /// Trusts the certificates of this CA document.
    Ca(&'a CertificatePem),
    /// Trusts the CA whose `SubjectPublicKeyInfo` hashes to this pin. The server must present that
    /// CA certificate in its chain, so first-contact clients need no CA file.
    Pin(&'a CaPin),
    /// Trusts the roots the operating system trusts: its platform certificate store, which holds
    /// enterprise and proxy CAs as well as the public roots. A server certificate issued by a CA
    /// the store does not hold is refused. Fails with [`TransportError::NoSystemRoots`] when the
    /// store holds no usable root.
    System,
    /// Trusts the Mozilla root programme compiled into the binary through `webpki-roots`, for
    /// servers on hosts without a platform trust store. The platform store is never read, so the
    /// trusted roots do not depend on the host.
    Bundled,
}

/// Turns a client's optional configured CA pin into a trust mode.
///
/// A configured pin selects [`ServerTrust::Pin`]. No pin means the platform trust store, so the
/// client selects [`ServerTrust::System`].
impl<'a> From<Option<&'a CaPin>> for ServerTrust<'a> {
    fn from(pin: Option<&'a CaPin>) -> Self {
        match pin {
            Some(pin) => ServerTrust::Pin(pin),
            None => ServerTrust::System,
        }
    }
}

/// A client certificate and the key that proves possession of it.
#[derive(Debug, Clone, Copy)]
pub struct ClientIdentity<'a> {
    /// The client certificate, leaf first.
    pub certificate: &'a CertificatePem,
    /// The PKCS#8 private key matching `certificate`.
    pub private_key: &'a PrivateKeyPem,
}

/// Builds a TLS 1.3 client configuration.
///
/// The configuration offers `alpn` protocols in order, presents `identity` as a client
/// certificate when one is given, and trusts the server according to `trust`. The crypto
/// provider is passed explicitly, so no process-wide provider needs to be installed. Every
/// certificate's validity is judged at the time `clock` reports.
///
/// # Errors
///
/// Returns the material error when a certificate, key or CA document is unusable, and
/// [`TransportError::Tls`] when the configuration is rejected by the TLS engine, and
/// [`TransportError::NoSystemRoots`] when `trust` is [`ServerTrust::System`] and the operating
/// system's store holds no usable root.
pub fn client_config(
    trust: ServerTrust<'_>,
    identity: Option<ClientIdentity<'_>>,
    alpn: &[&[u8]],
    clock: Arc<dyn Clock>,
) -> Result<ClientConfig, TransportError> {
    client_config_from(
        trust,
        identity,
        alpn,
        clock,
        rustls_native_certs::load_native_certs,
    )
}

/// Builds the configuration of [`client_config`], reading the operating system's store through
/// `platform_roots` when `trust` is [`ServerTrust::System`]. The loader runs at most once.
fn client_config_from<L>(
    trust: ServerTrust<'_>,
    identity: Option<ClientIdentity<'_>>,
    alpn: &[&[u8]],
    clock: Arc<dyn Clock>,
    platform_roots: L,
) -> Result<ClientConfig, TransportError>
where
    L: FnOnce() -> CertificateResult,
{
    let provider = Arc::new(default_provider());
    let builder = ClientConfig::builder_with_details(Arc::clone(&provider), time_provider(clock))
        .with_protocol_versions(&[&TLS13])
        .map_err(TransportError::Tls)?;
    let builder = match trust {
        ServerTrust::Ca(ca) => builder.with_root_certificates(Arc::new(root_store(ca)?)),
        ServerTrust::Pin(pin) => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(PinnedServerVerifier {
                pin: *pin,
                provider: Arc::clone(&provider),
            })),
        ServerTrust::System => {
            builder.with_root_certificates(Arc::new(system_roots(platform_roots())?))
        }
        ServerTrust::Bundled => builder.with_root_certificates(Arc::new(bundled_roots())),
    };
    let mut config = match identity {
        Some(identity) => {
            let chain = certificate_chain(identity.certificate)?;
            let key = private_key_der(identity.private_key)?;
            builder
                .with_client_auth_cert(chain, key)
                .map_err(TransportError::Tls)?
        }
        None => builder.with_no_client_auth(),
    };
    config.alpn_protocols = alpn.iter().map(|protocol| protocol.to_vec()).collect();
    Ok(config)
}

/// Builds the root store of [`ServerTrust::System`] from the certificates the operating system
/// trusts.
///
/// Every certificate the platform store yielded that parses as a trust anchor is added. Load
/// errors and unusable certificates are skipped, so one unreadable entry does not hide the rest
/// of the store. The loader is expensive, so it runs once per client configuration.
///
/// # Errors
///
/// Returns [`TransportError::NoSystemRoots`] when no usable root remains.
fn system_roots(result: CertificateResult) -> Result<RootCertStore, TransportError> {
    let mut roots = RootCertStore::empty();
    for certificate in result.certs {
        // An unusable certificate is skipped; the loader's own errors are dropped with `result`.
        let _ = roots.add(certificate);
    }
    if roots.is_empty() {
        return Err(TransportError::NoSystemRoots);
    }
    Ok(roots)
}

/// Builds the root store of [`ServerTrust::Bundled`] from the Mozilla root programme compiled
/// into the binary. The platform store is never read.
fn bundled_roots() -> RootCertStore {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    roots
}

/// Computes the trust-anchor pin of the first certificate in a PEM document.
///
/// # Errors
///
/// Returns the PEM error when the document cannot be decoded, [`TransportError::NoCertificate`]
/// when it holds no certificate, and [`TransportError::Certificate`] when the certificate is not
/// valid X.509.
pub fn ca_pin_of_certificate(certificate: &CertificatePem) -> Result<CaPin, TransportError> {
    let chain = certificate_chain(certificate)?;
    let ca = chain.first().ok_or(TransportError::NoCertificate)?;
    Certificate::from_der(ca.as_ref()).map_err(TransportError::Certificate)?;
    Ok(CaPin::of_spki(spki_der(ca)?))
}

/// Borrows the encoded `SubjectPublicKeyInfo` TLV from a DER-encoded certificate.
fn spki_der<'a>(certificate: &'a CertificateDer<'_>) -> Result<&'a [u8], TransportError> {
    let parse = || -> x509_cert::der::Result<&'a [u8]> {
        let mut reader = SliceReader::new(certificate.as_ref())?;
        let spki = reader.sequence(|certificate| {
            let spki = certificate.sequence(|tbs| {
                tbs.context_specific::<Version>(TagNumber(0), TagMode::Explicit)?;
                tbs.decode::<IntRef<'_>>()?;
                tbs.decode::<AlgorithmIdentifierRef<'_>>()?;
                for _ in 0..3 {
                    tbs.decode::<AnyRef<'_>>()?
                        .header()
                        .tag()
                        .assert_eq(Tag::Sequence)?;
                }
                let spki = tbs.tlv_bytes()?;
                SubjectPublicKeyInfoRef::from_der(spki)?;
                while !tbs.is_finished() {
                    tbs.decode::<AnyRef<'_>>()?;
                }
                Ok::<_, x509_cert::der::Error>(spki)
            })?;
            certificate.decode::<AlgorithmIdentifierRef<'_>>()?;
            certificate.decode::<BitStringRef<'_>>()?;
            Ok::<_, x509_cert::der::Error>(spki)
        })?;
        reader.finish()?;
        Ok(spki)
    };
    parse().map_err(TransportError::Certificate)
}

/// Finds the intermediate whose key matches `pin`.
///
/// Only the server's presented chain is searched, so a certificate that is merely the leaf
/// never satisfies a pin.
fn pinned_authority<'c>(
    pin: &CaPin,
    intermediates: &'c [CertificateDer<'c>],
) -> Result<&'c CertificateDer<'c>, Error> {
    intermediates
        .iter()
        .find(|candidate| spki_der(candidate).is_ok_and(|spki| CaPin::of_spki(spki) == *pin))
        .ok_or(Error::InvalidCertificate(CertificateError::UnknownIssuer))
}

/// Verifies a server against one pinned CA found in its presented chain.
///
/// Leaf verification (chain, validity at the current time, name and key usage) is delegated to a
/// [`WebPkiServerVerifier`] whose only trust anchor is the pinned CA.
#[derive(Debug)]
struct PinnedServerVerifier {
    pin: CaPin,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        let authority = pinned_authority(&self.pin, intermediates)?;
        let mut roots = RootCertStore::empty();
        roots.add(authority.clone())?;
        WebPkiServerVerifier::builder_with_provider(Arc::new(roots), Arc::clone(&self.provider))
            .build()
            .map_err(|error| Error::General(error.to_string()))?
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::time::Duration;

    use gang_clock::{TestClock, Timestamp};
    use gang_testkit::{ExtendedKeyUsagePurpose, TestAuthority};

    use super::*;

    #[test]
    fn spki_der_borrows_the_certificate_encoding() {
        use x509_cert::der::Encode;

        let authority = TestAuthority::new("gang pinned ca").expect("test ca builds");
        let expected = Certificate::from_der(authority.der.as_ref())
            .expect("certificate parses")
            .tbs_certificate()
            .subject_public_key_info()
            .to_der()
            .expect("spki encodes");
        let spki = spki_der(&authority.der).expect("spki parses");
        assert_eq!(spki, expected);
        let offset = authority
            .der
            .as_ref()
            .windows(spki.len())
            .position(|bytes| bytes == spki)
            .expect("spki is in certificate");
        assert_eq!(spki.as_ptr(), authority.der.as_ref()[offset..].as_ptr());
    }

    #[test]
    fn spki_der_rejects_truncated_certificate() {
        let authority = TestAuthority::new("gang pinned ca").expect("test ca builds");
        let der = authority.der.as_ref();
        let truncated = CertificateDer::from(&der[..der.len() - 1]);
        assert!(spki_der(&truncated).is_err());
    }

    /// A platform store result holding exactly `certs`, as the loader reports it.
    fn store_of(certs: Vec<CertificateDer<'static>>) -> CertificateResult {
        let mut result = CertificateResult::default();
        result.certs = certs;
        result
    }

    /// Verifies `leaf` for `localhost` against `roots`, at a fixed time inside every test window.
    fn verify(
        roots: RootCertStore,
        leaf: &CertificateDer<'_>,
    ) -> Result<ServerCertVerified, Error> {
        let verifier = WebPkiServerVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::new(default_provider()),
        )
        .build()
        .expect("verifier builds");
        let name = ServerName::try_from("localhost").expect("localhost is a valid name");
        verifier.verify_server_cert(
            leaf,
            &[],
            &name,
            &[],
            UnixTime::since_unix_epoch(Duration::from_secs(1_900_000_000)),
        )
    }

    #[test]
    fn empty_platform_store_has_no_system_roots() {
        let outcome = system_roots(CertificateResult::default());
        assert!(matches!(outcome, Err(TransportError::NoSystemRoots)));
    }

    #[test]
    fn unusable_entries_are_skipped_and_the_usable_roots_are_kept() {
        let authority = TestAuthority::new("gang system ca").expect("test ca builds");
        let garbage = CertificateDer::from(vec![1u8, 2, 3]);
        let roots = system_roots(store_of(vec![garbage, authority.der.clone()]))
            .expect("one usable root remains");
        assert_eq!(roots.len(), 1);
    }

    #[test]
    fn a_store_of_only_unusable_entries_has_no_system_roots() {
        let garbage = CertificateDer::from(vec![1u8, 2, 3]);
        let outcome = system_roots(store_of(vec![garbage]));
        assert!(matches!(outcome, Err(TransportError::NoSystemRoots)));
    }

    #[test]
    fn system_roots_accept_their_authority_and_refuse_another() {
        let authority = TestAuthority::new("gang system ca").expect("test ca builds");
        let other = TestAuthority::new("gang other ca").expect("test ca builds");
        let leaf = authority
            .issue(
                vec!["localhost".to_owned()],
                vec![ExtendedKeyUsagePurpose::ServerAuth],
                [0x01; 16],
                "localhost",
            )
            .expect("test leaf issues")
            .der;

        let trusted =
            system_roots(store_of(vec![authority.der.clone()])).expect("store holds a root");
        verify(trusted, &leaf).expect("the authority's leaf is accepted");

        let untrusted =
            system_roots(store_of(vec![other.der.clone()])).expect("store holds a root");
        let refusal = verify(untrusted, &leaf);
        assert!(
            matches!(
                refusal,
                Err(Error::InvalidCertificate(CertificateError::UnknownIssuer))
            ),
            "a leaf of another authority is refused, got {refusal:?}"
        );
    }

    /// A clock at the Unix epoch, for configurations whose validity checks are not under test.
    fn epoch_clock() -> Arc<dyn Clock> {
        Arc::new(TestClock::new(Timestamp::UNIX_EPOCH))
    }

    #[test]
    fn system_trust_reads_the_platform_store_once_and_keeps_the_offered_alpn() {
        let authority = TestAuthority::new("gang system ca").expect("test ca builds");
        let calls = Cell::new(0);
        let alpn: &[&[u8]] = &[b"h2".as_slice(), b"gang/1".as_slice()];
        let config = client_config_from(ServerTrust::System, None, alpn, epoch_clock(), || {
            calls.set(calls.get() + 1);
            store_of(vec![authority.der.clone()])
        })
        .expect("the store holds a usable root");
        assert_eq!(calls.get(), 1);
        assert_eq!(
            config.alpn_protocols,
            vec![b"h2".to_vec(), b"gang/1".to_vec()]
        );
    }

    #[test]
    fn system_trust_with_an_empty_platform_store_has_no_system_roots() {
        let calls = Cell::new(0);
        let outcome = client_config_from(ServerTrust::System, None, &[], epoch_clock(), || {
            calls.set(calls.get() + 1);
            CertificateResult::default()
        });
        assert!(matches!(outcome, Err(TransportError::NoSystemRoots)));
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn system_trust_with_only_unusable_entries_has_no_system_roots() {
        let garbage = CertificateDer::from(vec![1u8, 2, 3]);
        let outcome = client_config_from(ServerTrust::System, None, &[], epoch_clock(), || {
            store_of(vec![garbage])
        });
        assert!(matches!(outcome, Err(TransportError::NoSystemRoots)));
    }

    #[test]
    fn ca_and_pin_trust_never_read_the_platform_store() {
        let authority = TestAuthority::new("gang pinned ca").expect("test ca builds");
        let pin = CaPin::new([7; 32]);
        let calls = Cell::new(0);
        let loader = || {
            calls.set(calls.get() + 1);
            CertificateResult::default()
        };
        client_config_from(
            ServerTrust::Ca(&authority.pem),
            None,
            &[],
            epoch_clock(),
            loader,
        )
        .expect("the CA document is usable");
        client_config_from(ServerTrust::Pin(&pin), None, &[], epoch_clock(), loader)
            .expect("a pin needs no platform roots");
        assert_eq!(calls.get(), 0);
    }

    #[test]
    fn some_pin_selects_pin_trust() {
        let pin = CaPin::new([9; 32]);
        assert!(matches!(ServerTrust::from(Some(&pin)), ServerTrust::Pin(_)));
    }

    #[test]
    fn no_pin_selects_system_trust() {
        assert!(matches!(ServerTrust::from(None), ServerTrust::System));
    }
}
