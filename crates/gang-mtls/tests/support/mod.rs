//! Fixtures shared by the `gang-mtls` integration tests.

/// Certificate signing requests built with `x509-cert`, and tampering with them.
pub mod csr {
    use std::str::FromStr;

    use const_oid::ObjectIdentifier;
    use gang_testkit::signing_key;
    use gang_types::CertificateRequestPem;
    use p256::ecdsa::{DerSignature, VerifyingKey};
    use x509_cert::builder::{Builder, RequestBuilder};
    use x509_cert::der::asn1::Ia5String;
    use x509_cert::der::pem::{LineEnding, decode_vec, encode_string};
    use x509_cert::der::{Decode, Encode};
    use x509_cert::ext::pkix::name::GeneralName;
    use x509_cert::ext::pkix::{BasicConstraints, SubjectAltName};
    use x509_cert::name::Name;
    use x509_cert::request::CertReq;

    const LABEL: &str = "CERTIFICATE REQUEST";

    /// Builds a CSR for the P-256 key derived from `key_seed` with subject `CN={subject_cn}`.
    #[must_use]
    pub fn csr_pem(key_seed: u8, subject_cn: &str) -> (CertificateRequestPem, VerifyingKey) {
        signed_request(key_seed, subject_cn, |_| {})
    }

    /// Builds a CSR that requests `BasicConstraints` CA:true and a DNS subject alternative name.
    #[must_use]
    pub fn csr_with_extensions(
        key_seed: u8,
        subject_cn: &str,
        san_dns: &str,
    ) -> (CertificateRequestPem, VerifyingKey) {
        let san = Ia5String::new(san_dns).expect("test DNS name is ASCII");
        signed_request(key_seed, subject_cn, |builder| {
            builder
                .add_extension(&BasicConstraints {
                    ca: true,
                    path_len_constraint: None,
                })
                .expect("basic constraints extension encodes");
            builder
                .add_extension(&SubjectAltName(vec![GeneralName::DnsName(san)]))
                .expect("subject alternative name extension encodes");
        })
    }

    fn signed_request(
        key_seed: u8,
        subject_cn: &str,
        configure: impl FnOnce(&mut RequestBuilder),
    ) -> (CertificateRequestPem, VerifyingKey) {
        let key = signing_key(key_seed).expect("test seed is a valid P-256 scalar");
        let public_key = *key.verifying_key();
        let subject = Name::from_str(&format!("CN={subject_cn}"))
            .expect("test subject is a valid distinguished name");
        let mut builder =
            RequestBuilder::new(subject).expect("request builder accepts the test subject");
        configure(&mut builder);
        let request = builder
            .build::<_, DerSignature>(&key)
            .expect("request signs with the test key");
        (encode_request(&request), public_key)
    }

    /// Returns the DER bytes inside a CSR PEM block.
    #[must_use]
    pub fn csr_der(csr: &CertificateRequestPem) -> Vec<u8> {
        let (label, der) = decode_vec(csr.as_str().as_bytes()).expect("request is a PEM block");
        assert_eq!(label, LABEL);
        der
    }

    /// PEM-encodes `request` under the `CERTIFICATE REQUEST` label.
    #[must_use]
    pub fn encode_request(request: &CertReq) -> CertificateRequestPem {
        let der = request.to_der().expect("request encodes as DER");
        encode_der(&der)
    }

    fn encode_der(der: &[u8]) -> CertificateRequestPem {
        let pem = encode_string(LABEL, LineEnding::LF, der).expect("request encodes as PEM");
        CertificateRequestPem::try_new(pem).expect("encoded request is a CERTIFICATE REQUEST block")
    }

    /// Returns `csr` with its version INTEGER replaced by `version`. The signature is not
    /// recomputed, and the version is patched in the DER because `x509-cert` only models v1.
    #[must_use]
    pub fn with_version(csr: &CertificateRequestPem, version: u8) -> CertificateRequestPem {
        let mut der = csr_der(csr);
        let position = der
            .windows(3)
            .position(|window| window == [0x02, 0x01, 0x00])
            .expect("request has a version integer");
        der[position + 2] = version;
        encode_der(&der)
    }

    /// Returns `csr` with `change` applied to its decoded request. The signature is not recomputed.
    #[must_use]
    pub fn modified(
        csr: &CertificateRequestPem,
        change: impl FnOnce(&mut CertReq),
    ) -> CertificateRequestPem {
        let mut request = CertReq::from_der(&csr_der(csr)).expect("test request decodes");
        change(&mut request);
        encode_request(&request)
    }

    /// Returns `csr` with its subject changed, so its signature no longer covers the request.
    #[must_use]
    pub fn tampered(csr: &CertificateRequestPem) -> CertificateRequestPem {
        modified(csr, |request| {
            request.info.subject =
                Name::from_str("CN=tampered").expect("tampered subject is a valid name");
        })
    }

    /// Parses a dotted object identifier for crafting bad requests.
    #[must_use]
    pub fn oid(dotted: &str) -> ObjectIdentifier {
        ObjectIdentifier::new_unwrap(dotted)
    }
}

/// Parsing and checking of certificates returned by the authority.
pub mod certificates {
    use gang_types::CertificatePem;
    use p256::ecdsa::signature::Verifier;
    use p256::ecdsa::{Signature, VerifyingKey};
    use x509_cert::Certificate;
    use x509_cert::der::pem::decode_vec;
    use x509_cert::der::{Decode, Encode};

    /// Parses the single `CERTIFICATE` block in `pem`.
    #[must_use]
    pub fn parse_certificate(pem: &CertificatePem) -> Certificate {
        let (label, der) = decode_vec(pem.as_str().as_bytes()).expect("certificate is a PEM block");
        assert_eq!(label, "CERTIFICATE");
        Certificate::from_der(&der).expect("certificate decodes")
    }

    /// Asserts that the certificate's signature verifies under `key`.
    pub fn verify_signed_by(certificate: &Certificate, key: &VerifyingKey) {
        let tbs = certificate
            .tbs_certificate()
            .to_der()
            .expect("TBS certificate re-encodes");
        let signature = Signature::from_der(certificate.signature().raw_bytes())
            .expect("certificate signature is DER ECDSA");
        key.verify(&tbs, &signature)
            .expect("certificate signature verifies under the key");
    }
}

/// Fixed clock, identity, and provider values shared by the tests.
pub mod fixtures {
    use std::time::Duration;

    use gang_clock::{TestClock, Timestamp};
    use gang_types::{Principal, ProviderName, TrustDomain};

    /// The instant every test clock starts at, a whole second.
    #[must_use]
    pub fn now() -> Timestamp {
        Timestamp::from_unix_millis(1_800_000_000_000)
    }

    /// A test clock reading [`now`].
    #[must_use]
    pub fn clock() -> TestClock {
        TestClock::new(now())
    }

    /// The trust domain of the test authority.
    #[must_use]
    pub fn trust_domain() -> TrustDomain {
        TrustDomain::try_new("example.test").expect("fixture trust domain is valid")
    }

    /// The identity provider of the test principal.
    #[must_use]
    pub fn provider() -> ProviderName {
        ProviderName::try_new("alpha").expect("fixture provider name is valid")
    }

    /// The principal the test certificates are issued to.
    #[must_use]
    pub fn principal() -> Principal {
        Principal::try_new("dev@example.test").expect("fixture principal is valid")
    }

    /// The certificate backdating of the test authority, matching the `[identity]` default.
    #[must_use]
    pub fn backdate() -> Duration {
        Duration::from_mins(1)
    }
}
