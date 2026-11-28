//! Checked conversions between [`gang_types::Digest`] and `REAPI v2` protobuf digests.

use gang_types::{Digest, DigestError};
use prost::Message as _;

use crate::build::bazel::remote::execution::v2::Digest as ProtoDigest;
use crate::google::rpc::{Code, PreconditionFailure, Status, precondition_failure::Violation};

/// Type URL for a packed Google RPC precondition failure.
pub const PRECONDITION_FAILURE_TYPE_URL: &str =
    "type.googleapis.com/google.rpc.PreconditionFailure";

/// Maximum number of missing blobs carried in one status.
pub const MAX_REPORTED_MISSING_BLOBS: usize = 1024;
const MAX_PRECONDITION_BYTES: usize = 256 * 1024;
const MAX_MISSING_MESSAGE_BYTES: usize = 4096;

/// Builds REAPI missing-blob violations, retaining first-seen digest order.
#[must_use]
pub fn missing_blobs_precondition_failure(digests: &[Digest]) -> PreconditionFailure {
    let mut seen = std::collections::HashSet::new();
    let violations = digests
        .iter()
        .filter(|digest| !digest.is_empty() && seen.insert(**digest))
        .take(MAX_REPORTED_MISSING_BLOBS)
        .map(|digest| Violation {
            r#type: "MISSING".to_owned(),
            subject: format!("blobs/{}/{}", digest.hex(), digest.size()),
            description: "missing CAS blob".to_owned(),
        })
        .collect();
    PreconditionFailure { violations }
}

/// Builds a rich status reporting missing CAS blobs.
#[must_use]
pub fn missing_blobs_rpc_status(digests: &[Digest], message: impl Into<String>) -> Status {
    let mut message = message.into();
    if message.len() > MAX_MISSING_MESSAGE_BYTES {
        let mut end = MAX_MISSING_MESSAGE_BYTES;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
    }
    Status {
        code: Code::FailedPrecondition as i32,
        message,
        details: vec![prost_types::Any {
            type_url: PRECONDITION_FAILURE_TYPE_URL.to_owned(),
            value: missing_blobs_precondition_failure(digests).encode_to_vec(),
        }],
    }
}

/// Extracts valid, unique missing blob digests from a rich RPC status.
#[must_use]
pub fn extract_missing_blobs(status: &Status) -> Vec<Digest> {
    if status.code != Code::FailedPrecondition as i32 {
        return Vec::new();
    }
    let mut seen = std::collections::HashSet::new();
    let mut digests = Vec::new();
    for detail in &status.details {
        if detail.type_url != PRECONDITION_FAILURE_TYPE_URL || !bounded_precondition(&detail.value)
        {
            continue;
        }
        let Ok(failure) = PreconditionFailure::decode(detail.value.as_slice()) else {
            continue;
        };
        for violation in failure.violations {
            if violation.r#type != "MISSING" {
                continue;
            }
            let Some((prefix, size)) = violation.subject.rsplit_once('/') else {
                continue;
            };
            let Some((prefix, hash)) = prefix.rsplit_once('/') else {
                continue;
            };
            if prefix != "blobs" && !prefix.ends_with("/blobs") {
                continue;
            }
            let Ok(size) = size.parse::<u64>() else {
                continue;
            };
            if size > i64::MAX as u64 {
                continue;
            }
            let Ok(digest) = Digest::from_hex_and_size(hash, size) else {
                continue;
            };
            if !digest.is_empty() && seen.insert(digest) {
                digests.push(digest);
                if digests.len() == MAX_REPORTED_MISSING_BLOBS {
                    return digests;
                }
            }
        }
    }
    digests
}

fn bounded_precondition(bytes: &[u8]) -> bool {
    use prost::encoding::{DecodeContext, decode_key, skip_field};
    if bytes.len() > MAX_PRECONDITION_BYTES {
        return false;
    }
    let mut input = bytes;
    let mut violations = 0;
    while !input.is_empty() {
        let Ok((tag, wire)) = decode_key(&mut input) else {
            return false;
        };
        if tag == 1 {
            violations += 1;
            if violations > MAX_REPORTED_MISSING_BLOBS {
                return false;
            }
        }
        if skip_field(wire, tag, &mut input, DecodeContext::default()).is_err() {
            return false;
        }
    }
    true
}

/// Errors returned when converting between [`gang_types::Digest`] and [`ProtoDigest`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DigestConversionError {
    /// The protobuf digest has a negative `size_bytes`.
    #[error("protobuf digest has negative size_bytes: {size_bytes}")]
    NegativeSize {
        /// The rejected `size_bytes` value.
        size_bytes: i64,
    },
    /// The `gang_types::Digest` byte size exceeds `i64::MAX`.
    #[error("digest size {size} exceeds REAPI i64::MAX")]
    SizeOverflow {
        /// The overflowing `u64` byte size.
        size: u64,
    },
    /// The protobuf digest hash failed SHA-256 validation.
    #[error("invalid protobuf digest hash: {0}")]
    InvalidDigest(#[from] DigestError),
}

/// Converts a [`gang_types::Digest`] into an `REAPI v2` protobuf [`ProtoDigest`].
///
/// # Errors
///
/// Returns [`DigestConversionError::SizeOverflow`] if `digest.size()` exceeds `i64::MAX`.
pub fn to_proto(digest: &Digest) -> Result<ProtoDigest, DigestConversionError> {
    let size_bytes =
        i64::try_from(digest.size()).map_err(|_| DigestConversionError::SizeOverflow {
            size: digest.size(),
        })?;
    Ok(ProtoDigest {
        hash: digest.hex(),
        size_bytes,
    })
}

/// Converts an `REAPI v2` protobuf [`ProtoDigest`] into a validated [`gang_types::Digest`].
///
/// # Errors
///
/// Returns [`DigestConversionError::NegativeSize`] if `proto.size_bytes` is negative, or
/// [`DigestConversionError::InvalidDigest`] if `proto.hash` is not a 64-character hex SHA-256.
pub fn from_proto(proto: &ProtoDigest) -> Result<Digest, DigestConversionError> {
    let size =
        u64::try_from(proto.size_bytes).map_err(|_| DigestConversionError::NegativeSize {
            size_bytes: proto.size_bytes,
        })?;
    Ok(Digest::from_hex_and_size(&proto.hash, size)?)
}

impl TryFrom<&Digest> for ProtoDigest {
    type Error = DigestConversionError;

    fn try_from(digest: &Digest) -> Result<Self, Self::Error> {
        to_proto(digest)
    }
}

impl TryFrom<Digest> for ProtoDigest {
    type Error = DigestConversionError;

    fn try_from(digest: Digest) -> Result<Self, Self::Error> {
        to_proto(&digest)
    }
}

impl TryFrom<&ProtoDigest> for Digest {
    type Error = DigestConversionError;

    fn try_from(proto: &ProtoDigest) -> Result<Self, Self::Error> {
        from_proto(proto)
    }
}

impl TryFrom<ProtoDigest> for Digest {
    type Error = DigestConversionError;

    fn try_from(proto: ProtoDigest) -> Result<Self, Self::Error> {
        from_proto(&proto)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ABC_HEX: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    #[test]
    fn missing_reports_are_bounded_and_ignore_empty() {
        let mut digests = vec![Digest::EMPTY];
        digests.extend((0_u32..20_000).map(|i| Digest::from_bytes(&i.to_le_bytes())));
        let status = crate::missing_blobs_rpc_status(&digests, "é".repeat(1_000_000));
        assert!(status.encode_to_vec().len() < 256 * 1024);
        assert!(status.message.len() <= super::MAX_MISSING_MESSAGE_BYTES);
        let extracted = crate::extract_missing_blobs(&status);
        assert_eq!(extracted.len(), super::MAX_REPORTED_MISSING_BLOBS);
        assert_eq!(extracted[0], digests[1]);
        let empty = crate::missing_blobs_rpc_status(&[Digest::EMPTY], "missing");
        assert!(crate::extract_missing_blobs(&empty).is_empty());
        let failure = crate::google::rpc::PreconditionFailure {
            violations: vec![crate::google::rpc::precondition_failure::Violation {
                r#type: "MISSING".to_owned(),
                subject: format!("blobs/{}/0", Digest::EMPTY.hex()),
                description: String::new(),
            }],
        };
        let mut status = empty;
        status.details[0].value = failure.encode_to_vec();
        assert!(crate::extract_missing_blobs(&status).is_empty());
        let digest = Digest::from_bytes(b"valid");
        let failure = crate::google::rpc::PreconditionFailure {
            violations: vec![crate::google::rpc::precondition_failure::Violation {
                r#type: "MISSING".to_owned(),
                subject: format!(
                    "{}blobs/{}/{}",
                    "/".repeat(32_000),
                    digest.hex(),
                    digest.size()
                ),
                description: String::new(),
            }],
        };
        status.details[0].value = failure.encode_to_vec();
        assert_eq!(crate::extract_missing_blobs(&status), vec![digest]);
    }

    #[test]
    fn missing_extraction_rejects_dense_and_separator_heavy_details() {
        let mut status = crate::missing_blobs_rpc_status(&[], "missing");
        status.details[0].value = [0x0a, 0x00].repeat(super::MAX_REPORTED_MISSING_BLOBS + 1);
        assert!(crate::extract_missing_blobs(&status).is_empty());
        let failure = crate::google::rpc::PreconditionFailure {
            violations: vec![crate::google::rpc::precondition_failure::Violation {
                r#type: "MISSING".to_owned(),
                subject: "/".repeat(super::MAX_PRECONDITION_BYTES),
                description: String::new(),
            }],
        };
        status.details[0].value = failure.encode_to_vec();
        assert!(crate::extract_missing_blobs(&status).is_empty());
    }

    #[test]
    fn missing_blob_status_round_trips_and_deduplicates() {
        let first = Digest::from_bytes(b"first");
        let second = Digest::from_bytes(b"second");
        let status = crate::missing_blobs_rpc_status(&[first, second, first], "missing");
        assert_eq!(
            status.code,
            crate::google::rpc::Code::FailedPrecondition as i32
        );
        let failure =
            crate::google::rpc::PreconditionFailure::decode(status.details[0].value.as_slice())
                .expect("decode precondition");
        assert_eq!(failure.violations.len(), 2);
        assert_eq!(failure.violations[0].r#type, "MISSING");
        assert_eq!(
            failure.violations[0].subject,
            format!("blobs/{}/{}", first.hex(), first.size())
        );
        assert_eq!(crate::extract_missing_blobs(&status), vec![first, second]);
    }

    #[test]
    fn missing_blob_extraction_rejects_invalid_details() {
        let digest = Digest::from_bytes(b"valid");
        let mut status = crate::missing_blobs_rpc_status(&[digest], "missing");
        let mut failure =
            crate::google::rpc::PreconditionFailure::decode(status.details[0].value.as_slice())
                .expect("decode precondition");
        let valid = failure.violations[0].clone();
        failure.violations[0].subject = format!("instance/nested/{}", valid.subject);
        failure.violations.push(valid.clone());
        failure
            .violations
            .push(crate::google::rpc::precondition_failure::Violation {
                r#type: "OTHER".to_owned(),
                ..valid.clone()
            });
        failure
            .violations
            .push(crate::google::rpc::precondition_failure::Violation {
                subject: "blobs/not-a-hash/2".to_owned(),
                ..valid
            });
        status.details[0].value = failure.encode_to_vec();
        assert_eq!(crate::extract_missing_blobs(&status), vec![digest]);
        status.code = crate::google::rpc::Code::Internal as i32;
        assert!(crate::extract_missing_blobs(&status).is_empty());
        status.code = crate::google::rpc::Code::FailedPrecondition as i32;
        status.details[0].value = vec![0xff];
        assert!(crate::extract_missing_blobs(&status).is_empty());
    }

    #[test]
    fn round_trips_empty_and_non_empty_digests() {
        for digest in [
            Digest::EMPTY,
            Digest::from_bytes(b"abc"),
            Digest::from_bytes(b"gang-reapi-round-trip"),
        ] {
            let proto = ProtoDigest::try_from(digest).expect("to proto");
            assert_eq!(proto.hash, digest.hex());
            assert_eq!(
                u64::try_from(proto.size_bytes).expect("non-negative"),
                digest.size()
            );

            let back = Digest::try_from(&proto).expect("from proto by ref");
            assert_eq!(back, digest);
            let owned_back = Digest::try_from(proto).expect("from proto by value");
            assert_eq!(owned_back, digest);
        }
    }

    #[test]
    fn accepts_uppercase_hex_from_proto_and_normalizes_to_lowercase() {
        let proto = ProtoDigest {
            hash: ABC_HEX.to_ascii_uppercase(),
            size_bytes: 3,
        };
        let digest = Digest::try_from(&proto).expect("valid uppercase hex");
        assert_eq!(digest.hex(), ABC_HEX);
        assert_eq!(digest.size(), 3);
    }

    #[test]
    fn rejects_negative_size_and_malformed_hashes() {
        let negative = ProtoDigest {
            hash: ABC_HEX.to_owned(),
            size_bytes: -1,
        };
        assert_eq!(
            Digest::try_from(&negative),
            Err(DigestConversionError::NegativeSize { size_bytes: -1 })
        );

        let short_hash = ProtoDigest {
            hash: ABC_HEX[..63].to_owned(),
            size_bytes: 3,
        };
        assert_eq!(
            Digest::try_from(&short_hash),
            Err(DigestConversionError::InvalidDigest(
                DigestError::HashLength { found: 63 }
            ))
        );

        let bad_char = ProtoDigest {
            hash: format!("z{}", &ABC_HEX[1..]),
            size_bytes: 3,
        };
        assert_eq!(
            Digest::try_from(&bad_char),
            Err(DigestConversionError::InvalidDigest(
                DigestError::InvalidHashCharacter { character: 'z' }
            ))
        );
    }

    #[test]
    fn rejects_size_exceeding_i64_max() {
        let max_valid = Digest::new([0_u8; 32], u64::MAX);
        assert_eq!(
            ProtoDigest::try_from(&max_valid),
            Err(DigestConversionError::SizeOverflow { size: u64::MAX })
        );

        let boundary = Digest::new([0_u8; 32], i64::MAX as u64);
        let proto = ProtoDigest::try_from(boundary).expect("i64::MAX fits");
        assert_eq!(proto.size_bytes, i64::MAX);
    }
}
