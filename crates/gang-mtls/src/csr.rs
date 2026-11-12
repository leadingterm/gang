//! Verification of PKCS#10 certificate signing requests, including proof of possession.

use der::asn1::AnyRef;
use der::pem::PemLabel;
use der::{Decode, Encode, Reader, SliceReader, Tag};
use gang_types::CertificateRequestPem;
use p256::ecdsa::Signature;
use p256::ecdsa::signature::Verifier;
use spki::SubjectPublicKeyInfoOwned;
use x509_cert::request::CertReq;

use crate::errors::CsrError;
use crate::signing::{ecdsa_sha256, verifying_key};

/// A certificate signing request whose signature proves possession of its public key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedCsr {
    pub(crate) public_key: SubjectPublicKeyInfoOwned,
}

/// Verifies a PEM certification request and returns its P-256 public key.
///
/// Only the public key is taken from the request: its subject, attributes and requested
/// extensions are ignored.
///
/// # Errors
///
/// Returns [`CsrError::Pem`] unless the text is one `CERTIFICATE REQUEST` PEM block,
/// [`CsrError::Malformed`] when the DER does not parse, [`CsrError::UnsupportedVersion`]
/// for a version other than v1, [`CsrError::UnsupportedAlgorithm`] unless the signature
/// algorithm is ecdsa-with-SHA256 without parameters, [`CsrError::UnsupportedKey`] unless
/// the key is a parseable P-256 point, and [`CsrError::BadSignature`] when the signature
/// does not verify under the request's own key.
pub fn verify_csr(csr: &CertificateRequestPem) -> Result<VerifiedCsr, CsrError> {
    let (label, der) = der::pem::decode_vec(csr.as_str().as_bytes())
        .map_err(|error| CsrError::Pem(error.into()))?;
    if label != CertReq::PEM_LABEL {
        return Err(CsrError::Pem(der::pem::Error::Label.into()));
    }

    // The version is read before typed decoding: a version other than v1 has no typed
    // representation, so it must be reported as unsupported rather than as malformed DER.
    let (tag, version) = raw_version(&der).map_err(CsrError::Malformed)?;
    if tag == Tag::Integer && version != [0] {
        return Err(CsrError::UnsupportedVersion);
    }

    let request = CertReq::from_der(&der).map_err(CsrError::Malformed)?;

    if request.algorithm != ecdsa_sha256() {
        return Err(CsrError::UnsupportedAlgorithm);
    }
    let key = verifying_key(&request.info.public_key).map_err(|_| CsrError::UnsupportedKey)?;

    let info_der = raw_request_info(&der).map_err(CsrError::Malformed)?;
    let signature =
        Signature::from_der(request.signature.raw_bytes()).map_err(|_| CsrError::BadSignature)?;
    key.verify(info_der, &signature)
        .map_err(|_| CsrError::BadSignature)?;

    Ok(VerifiedCsr {
        public_key: request.info.public_key,
    })
}

/// Borrows the encoded `CertificationRequestInfo` TLV from a certification request.
fn raw_request_info(der: &[u8]) -> der::Result<&[u8]> {
    let mut reader = SliceReader::new(der)?;
    reader.decode::<der::Header>()?;
    let header = der::Header::peek(&reader)?;
    reader.read_slice((header.encoded_len()? + header.length())?)
}

/// Returns the tag and content bytes of the `version` field of a `CertificationRequestInfo`
/// without decoding the rest of the request.
fn raw_version(der: &[u8]) -> der::Result<(Tag, &[u8])> {
    let mut reader = SliceReader::new(der)?;
    reader.decode::<der::Header>()?;
    reader.decode::<der::Header>()?;
    let version = reader.decode::<AnyRef<'_>>()?;
    Ok((version.header().tag(), version.value()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A PKCS#10 request for an RSA-2048 key, signed with sha256WithRSAEncryption.
    const RSA_CSR: &str = "-----BEGIN CERTIFICATE REQUEST-----
MIICUTCCATkCAQAwDDEKMAgGA1UEAwwBeDCCASIwDQYJKoZIhvcNAQEBBQADggEP
ADCCAQoCggEBAKyLxxq5HLR03WJFUWOQJ4R5VV7VJOK4sp5u3q3V4dSGXh+70ebp
iEDB13o8WLGgSlIqbre93Hli1EryLVYAZXbHbQajftMlBP7Gb9gPjYhvAt8T77NM
uZImklXGTBGuX3mOhcByCywcyrjB0Rv84yHg/k7ot0KtdZtuSNqABKZNvrBZ63x1
9HSr8FtDW0Fk5DRY5iX+FGZVQ16eOaFid92JLcPOAg9CiRv5LbgZFO0HBrOBL03N
h7oEQjHkmQRi/3LkaLMqXLNs9o79Od45bcP8RFK93ozRXoPzf7ByimvUacfn1si/
v64lUyYB6QdjLkN9CY7mP/2RemaTyUasAYcCAwEAAaAAMA0GCSqGSIb3DQEBCwUA
A4IBAQBVRob5jbBJsJxflCdud5xXUnaNj8sB3PHLD9VVtu12B3f7xwwzHCDHEQMG
AMRlSkj94l2Wa3XqXZ3zryECvwtCENbJl944AOl3fwf2nt+x0Krw/vmvrCEvXhjs
F1qYrRiBLy7xfDOEPwWUIhMtaQXJWBy3EQ2DnR/nAK/ly5xeFlAwHELznZWdiuyQ
NB4ldohOTz2cNSc5F/LUzMiIF0K3lstMV3IPFQV7myOToo5Ozymz7QzAs2nEP7ld
HTZLhaz6wqPNX9azl96dtvF5wTmynvgAT88IN8KNxBB2EhGnh1dgEobfJTC7JO2w
kr05iKrVBX+LDKZzZCA8bjMwXJNd
-----END CERTIFICATE REQUEST-----
";

    /// A P-384 request signed with ECDSA-SHA256, from pyca/cryptography's test vectors.
    const P384_CSR: &str = "-----BEGIN CERTIFICATE REQUEST-----
MIIBTzCB1gIBADBXMRgwFgYDVQQDDA9jcnlwdG9ncmFwaHkuaW8xDTALBgNVBAoM
BFB5Q0ExCzAJBgNVBAYTAlVTMQ4wDAYDVQQIDAVUZXhhczEPMA0GA1UEBwwGQXVz
dGluMHYwEAYHKoZIzj0CAQYFK4EEACIDYgAE3hm1FMCzw66bOY6j4mtegWvc+RAs
rY8S/gL55MkkhySzkpftdYLgTYsypVEDjQkIaAOm0/uRoaEWfsAhWLAO+tOck5ZG
L6zP8P+vcVWBKQnTcmvVn94AHP9LubL1r4y6oAAwCgYIKoZIzj0EAwIDaAAwZQIw
LBqffejBeHMy0jB6iGtHalnxcrmw4lAmLzI4sbRe4RK7brNbD7VqEjuSlushLf/D
AjEAlM9EDJXFKCfVVq5tdlAOMAglXUfCn37ngu11WOUb/XaqRd9tmZ7VxGM0f+I4
LRdR
-----END CERTIFICATE REQUEST-----
";

    #[test]
    fn request_info_is_borrowed_with_its_original_long_form_header() {
        let (_, der) = der::pem::decode_vec(RSA_CSR.as_bytes()).expect("fixture decodes");
        let info = raw_request_info(&der).expect("request info is present");
        assert_eq!(info, &der[4..4 + 317]);
        assert_eq!(info.as_ptr(), der[4..].as_ptr());
        assert_eq!(&info[..4], &[0x30, 0x82, 0x01, 0x39]);
    }

    #[test]
    fn truncated_request_info_is_rejected() {
        let der = [0x30, 0x04, 0x30, 0x03, 0x02, 0x01];
        assert!(raw_request_info(&der).is_err());
    }

    #[test]
    fn p384_request_is_rejected_as_unsupported_key() {
        let csr = CertificateRequestPem::try_new(P384_CSR).expect("fixture is a PEM block");
        assert!(matches!(verify_csr(&csr), Err(CsrError::UnsupportedKey)));
    }

    #[test]
    fn p384_request_is_rejected_as_unsupported_algorithm() {
        let (_, der) = der::pem::decode_vec(P384_CSR.as_bytes()).expect("fixture decodes");
        let mut request = CertReq::from_der(&der).expect("fixture is a request");
        // The signature is not recomputed: algorithm rejection precedes signature verification.
        request.algorithm.oid = const_oid::ObjectIdentifier::new("1.2.840.10045.4.3.3")
            .expect("ECDSA-SHA384 is a valid object identifier");
        let der = request.to_der().expect("request encodes as DER");
        let pem = der::pem::encode_string("CERTIFICATE REQUEST", der::pem::LineEnding::LF, &der)
            .expect("request encodes as PEM");
        let csr = CertificateRequestPem::try_new(pem).expect("request is a PEM block");
        assert!(matches!(
            verify_csr(&csr),
            Err(CsrError::UnsupportedAlgorithm)
        ));
    }

    #[test]
    fn rsa_request_is_rejected_as_unsupported_algorithm() {
        let csr = CertificateRequestPem::try_new(RSA_CSR).expect("fixture is a PEM block");
        assert!(matches!(
            verify_csr(&csr),
            Err(CsrError::UnsupportedAlgorithm)
        ));
    }
}
