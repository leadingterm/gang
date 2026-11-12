//! Certificate bodies: validity windows, TBS assembly and signing.

use std::time::Duration;

use der::asn1::{ObjectIdentifier, Utf8StringRef};
use der::pem::{self, LineEnding};
use der::{Decode, Encode, Length};
use gang_clock::Timestamp;
use gang_spi::CertificateSignerPort;
use gang_types::{CertificatePem, NonZeroDuration};
use p256::ecdsa::VerifyingKey;
use spki::SubjectPublicKeyInfoOwned;
use x509_cert::certificate::Version;
use x509_cert::ext::Extension;
use x509_cert::name::Name;
use x509_cert::serial_number::SerialNumber;
use x509_cert::time::Validity;

use crate::errors::MtlsError;
use crate::signing;
use crate::time;

const COMMON_NAME: ObjectIdentifier = ObjectIdentifier::new_unwrap("2.5.4.3");

/// A certificate validity window, already truncated to whole seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Window {
    pub(crate) not_before: Timestamp,
    pub(crate) not_after: Timestamp,
}

/// Computes the validity window of a certificate issued at `now` for `lifetime`, starting
/// `backdate` before `now`.
pub(crate) fn window(now: Timestamp, lifetime: NonZeroDuration, backdate: Duration) -> Window {
    Window {
        not_before: whole_seconds(now - backdate),
        not_after: whole_seconds(now + lifetime.get()),
    }
}

/// Converts a window to X.509 validity times, failing when either bound is unrepresentable.
pub(crate) fn validity(window: Window) -> Result<Validity, MtlsError> {
    let not_before = time::x509_time(window.not_before)?;
    let not_after = time::x509_time(window.not_after)?;
    Ok(Validity::new(not_before, not_after))
}

/// Builds a v3 `TBSCertificate` DER, to be signed with ECDSA P-256 / SHA-256.
///
/// The structure is assembled from its component encodings because `x509-cert` exposes no
/// constructors for certificate bodies.
pub(crate) fn tbs_certificate(
    serial_number: &SerialNumber,
    issuer: &Name,
    subject: &Name,
    validity: &Validity,
    subject_public_key_info: &SubjectPublicKeyInfoOwned,
    extensions: &[Extension],
) -> Result<Vec<u8>, MtlsError> {
    let extensions = extensions
        .iter()
        .map(Encode::to_der)
        .collect::<der::Result<Vec<_>>>()?
        .concat();
    let body = [
        tlv(0xa0, &Version::V3.to_der()?)?,
        serial_number.to_der()?,
        signing::ecdsa_sha256().to_der()?,
        issuer.to_der()?,
        validity.to_der()?,
        subject.to_der()?,
        subject_public_key_info.to_der()?,
        tlv(0xa3, &tlv(0x30, &extensions)?)?,
    ]
    .concat();
    tlv(0x30, &body)
}

/// Signs the DER `tbs_der` through the signer and returns the PEM certificate.
pub(crate) async fn sign_certificate<S: CertificateSignerPort>(
    signer: &S,
    verifying_key: &VerifyingKey,
    tbs_der: &[u8],
) -> Result<CertificatePem, MtlsError> {
    let signature = signing::sign(signer, verifying_key, tbs_der).await?;
    let certificate_der = tlv(
        0x30,
        &[
            tbs_der.to_vec(),
            signing::ecdsa_sha256().to_der()?,
            signature.to_der()?,
        ]
        .concat(),
    )?;
    let pem = encode_pem("CERTIFICATE", &certificate_der)?;
    Ok(CertificatePem::try_new(pem)?)
}

/// PEM-encodes `der` under `label`.
pub(crate) fn encode_pem(label: &str, der: &[u8]) -> Result<String, MtlsError> {
    Ok(pem::encode_string(label, LineEnding::LF, der).map_err(der::Error::from)?)
}

/// Builds a distinguished name holding a single `CN` attribute with the given UTF-8 value.
///
/// The name is assembled from DER attribute structures rather than parsed from a string, so
/// characters such as `,` or `+` in `value` remain part of the one attribute value.
pub(crate) fn common_name(value: &str) -> Result<Name, MtlsError> {
    let attribute_value = Utf8StringRef::new(value)?.to_der()?;
    let attribute = [COMMON_NAME.to_der()?, attribute_value].concat();
    let relative_distinguished_name = tlv(0x31, &tlv(0x30, &attribute)?)?;
    let name = tlv(0x30, &relative_distinguished_name)?;
    Ok(Name::from_der(&name)?)
}

/// Encodes one DER TLV with a definite length.
fn tlv(tag: u8, content: &[u8]) -> Result<Vec<u8>, MtlsError> {
    let mut encoded = vec![tag];
    Length::try_from(content.len())?.encode(&mut encoded)?;
    encoded.extend_from_slice(content);
    Ok(encoded)
}

/// Rounds a timestamp down to whole seconds.
fn whole_seconds(timestamp: Timestamp) -> Timestamp {
    Timestamp::from_unix_millis(time::unix_seconds(timestamp).saturating_mul(1000))
}

#[cfg(test)]
mod tests {
    use super::*;

    const BACKDATE: Duration = Duration::from_mins(1);

    fn now(millis: u64) -> Timestamp {
        Timestamp::from_unix_millis(millis)
    }

    #[test]
    fn window_is_backdated_and_truncated_to_whole_seconds() {
        let window = window(
            now(1_000_500),
            NonZeroDuration::from_millis(3_600_250),
            BACKDATE,
        );
        assert_eq!(window.not_before, now(940_000));
        assert_eq!(window.not_after, now(4_600_000));
    }

    #[test]
    fn backdating_saturates_at_the_epoch() {
        let window = window(now(10_000), NonZeroDuration::from_secs(1), BACKDATE);
        assert_eq!(window.not_before, Timestamp::UNIX_EPOCH);
    }

    #[test]
    fn zero_backdate_starts_at_issuance() {
        let window = window(
            now(1_000_500),
            NonZeroDuration::from_secs(1),
            Duration::ZERO,
        );
        assert_eq!(window.not_before, now(1_000_000));
    }

    #[test]
    fn backdating_from_the_epoch_keeps_the_start_at_the_epoch() {
        let window = window(
            Timestamp::UNIX_EPOCH,
            NonZeroDuration::from_secs(1),
            BACKDATE,
        );
        assert_eq!(window.not_before, Timestamp::UNIX_EPOCH);
        assert_eq!(window.not_after, now(1_000));
        assert!(validity(window).is_ok());
    }

    #[test]
    fn maximum_lifetime_saturates_and_reports_a_time_range_error() {
        let window = window(
            now(1_000_000),
            NonZeroDuration::from_millis(u64::MAX),
            BACKDATE,
        );
        assert!(matches!(validity(window), Err(MtlsError::TimeRange(_))));
    }

    #[test]
    fn tlv_uses_short_and_long_definite_lengths() {
        assert_eq!(
            tlv(0x30, &[0; 0x7f]).expect("short form")[..2],
            [0x30, 0x7f]
        );
        assert_eq!(
            tlv(0x30, &[0; 0x80]).expect("long form")[..3],
            [0x30, 0x81, 0x80]
        );
        assert_eq!(
            tlv(0x30, &[0; 0x100]).expect("long form")[..4],
            [0x30, 0x82, 0x01, 0x00]
        );
    }
}
