//! Worker certificates: the issuance request and the `certificates/{serial}` resource.

use gang_clock::Timestamp;
use gang_types::{CertificatePem, CertificateRequestPem, Proof, ProviderName};
use serde::{Deserialize, Serialize};

/// Body of `POST /v1/certificates`.
///
/// A worker enrolls with a proof of identity from the named identity provider together with a
/// PKCS#10 certificate signing request; the private key never leaves the worker.
#[derive(gang_macros::Known, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateCertificateRequest {
    /// Identity provider that vouches for the worker.
    pub provider: ProviderName,
    /// Proof of identity issued by `provider`.
    pub proof: Proof,
    /// PKCS#10 certificate signing request.
    pub pem_csr: CertificateRequestPem,
}

/// A worker certificate, named `certificates/{serial}`.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Certificate {
    /// Resource name, `certificates/{serial}`.
    pub name: String,
    /// Reference `principals/{id}` of the principal the certificate was issued to.
    pub principal: String,
    /// When the certificate was issued.
    #[serde(with = "crate::time")]
    pub create_time: Timestamp,
    /// When the certificate expires.
    #[serde(with = "crate::time")]
    pub expire_time: Timestamp,
    /// When the certificate was revoked; absent while it is valid.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::time::option"
    )]
    pub revoke_time: Option<Timestamp>,
    /// The issued certificate. Present only in the answer to `POST /v1/certificates`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pem_certificate: Option<CertificatePem>,
    /// The certificate of the authority that issued it. Present only in the answer to
    /// `POST /v1/certificates`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_certificate: Option<CertificatePem>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn request_parses_and_round_trips_exactly() {
        let value = json!({
            "provider": "aws-workers",
            "proof": "tok-123",
            "pemCsr": "-----BEGIN CERTIFICATE REQUEST-----\nAAAA\n-----END CERTIFICATE REQUEST-----",
        });
        let request: CreateCertificateRequest =
            serde_json::from_value(value.clone()).expect("request parses");
        assert_eq!(
            serde_json::to_value(&request).expect("request serializes"),
            value
        );
        assert!(!format!("{request:?}").contains("tok-123"));
    }

    #[test]
    fn request_rejects_unknown_fields_and_the_old_csr_name() {
        let value = json!({
            "provider": "aws-workers",
            "proof": "tok-123",
            "pemCsr": "-----BEGIN CERTIFICATE REQUEST-----\nAAAA\n-----END CERTIFICATE REQUEST-----",
            "unexpected": true,
        });
        assert!(serde_json::from_value::<CreateCertificateRequest>(value).is_err());
        let old = json!({
            "provider": "aws-workers",
            "proof": "tok-123",
            "csr": "-----BEGIN CERTIFICATE REQUEST-----\nAAAA\n-----END CERTIFICATE REQUEST-----",
        });
        assert!(serde_json::from_value::<CreateCertificateRequest>(old).is_err());
    }

    #[test]
    fn created_certificate_round_trips_exactly() {
        let value = json!({
            "name": "certificates/3fa9",
            "principal": "principals/aws-workers%3Ai-0e10",
            "createTime": "2026-01-14T09:00:00.000Z",
            "expireTime": "2026-01-21T09:00:00.000Z",
            "pemCertificate": "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----",
            "caCertificate": "-----BEGIN CERTIFICATE-----\nBBBB\n-----END CERTIFICATE-----",
        });
        let certificate: Certificate =
            serde_json::from_value(value.clone()).expect("certificate parses");
        assert_eq!(certificate.revoke_time, None);
        assert!(certificate.ca_certificate.is_some());
        assert_eq!(
            serde_json::to_value(&certificate).expect("certificate serializes"),
            value
        );
    }

    #[test]
    fn revoked_certificate_omits_the_pem_and_keeps_the_revoke_time() {
        let value = json!({
            "name": "certificates/3fa9",
            "principal": "principals/aws-workers%3Ai-0e10",
            "createTime": "2026-01-14T09:00:00.000Z",
            "expireTime": "2026-01-21T09:00:00.000Z",
            "revokeTime": "2026-01-14T09:30:00.000Z",
        });
        let certificate: Certificate =
            serde_json::from_value(value.clone()).expect("certificate parses");
        assert_eq!(certificate.pem_certificate, None);
        assert_eq!(certificate.ca_certificate, None);
        assert!(certificate.revoke_time.is_some());
        assert_eq!(
            serde_json::to_value(&certificate).expect("certificate serializes"),
            value
        );
    }

    #[test]
    fn certificate_ignores_unknown_fields() {
        let value = json!({
            "name": "certificates/3fa9",
            "principal": "principals/aws-workers%3Ai-0e10",
            "createTime": "2026-01-14T09:00:00.000Z",
            "expireTime": "2026-01-21T09:00:00.000Z",
            "future": [],
        });
        assert!(serde_json::from_value::<Certificate>(value).is_ok());
    }
}
