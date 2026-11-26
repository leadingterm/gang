//! REAPI v2 `ByteStream` resource-name parser and formatter.
//!
//! Supports the four canonical resource-name shapes:
//! - `[instance/]blobs/{hash}/{size}`
//! - `[instance/]compressed-blobs/zstd/{hash}/{size}`
//! - `[instance/]uploads/{uuid}/blobs/{hash}/{size}`
//! - `[instance/]uploads/{uuid}/compressed-blobs/zstd/{hash}/{size}`

use std::fmt;
use std::str::FromStr;

use gang_spi::{PortError, Rejection};
use gang_types::{Digest, DigestError};

use crate::codec::Compression;

const RESERVED_KEYWORDS: &[&str] = &[
    "blobs",
    "uploads",
    "actions",
    "actionResults",
    "operations",
    "capabilities",
    "compressed-blobs",
];

/// Errors returned when parsing or constructing a REAPI `ByteStream` [`ResourceName`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResourceNameError {
    /// The resource name is empty.
    #[error("resource name cannot be empty")]
    Empty,
    /// The resource name contains an empty path segment (leading, trailing, or consecutive `/`).
    #[error("resource name contains an empty path segment")]
    EmptySegment,
    /// The resource name contains a `.` or `..` path segment.
    #[error("resource name contains a dot segment")]
    DotSegment,
    /// The instance name contains a REAPI-reserved path keyword.
    #[error("instance name contains reserved REAPI keyword `{segment}`")]
    ReservedInstanceSegment {
        /// The disallowed keyword segment.
        segment: String,
    },
    /// The upload path is missing its upload identifier segment.
    #[error("upload resource name is missing an upload identifier")]
    MissingUploadId,
    /// The `compressed-blobs` compressor name is unsupported.
    #[error("unsupported ByteStream compressor `{compressor}`")]
    UnsupportedCompressor {
        /// The rejected compressor name.
        compressor: String,
    },
    /// The resource digest failed validation.
    #[error("invalid digest in resource name: {0}")]
    InvalidDigest(#[from] DigestError),
    /// The resource name does not match any supported `ByteStream` pattern.
    #[error("unrecognized ByteStream resource name format")]
    UnrecognizedFormat,
}

impl From<ResourceNameError> for PortError {
    fn from(error: ResourceNameError) -> Self {
        Self::rejected(Rejection::MalformedRequest, error)
    }
}

/// A parsed REAPI v2 `ByteStream` resource name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ResourceName {
    /// A read resource name (`[instance/]blobs/{hash}/{size}` or
    /// `[instance/]compressed-blobs/zstd/{hash}/{size}`).
    Blob {
        /// Instance prefix (empty string when omitted).
        instance_name: String,
        /// Transport compression format.
        compression: Compression,
        /// Uncompressed blob digest.
        digest: Digest,
    },
    /// An upload resource name (`[instance/]uploads/{uuid}/blobs/{hash}/{size}` or
    /// `[instance/]uploads/{uuid}/compressed-blobs/zstd/{hash}/{size}`).
    Upload {
        /// Instance prefix (empty string when omitted).
        instance_name: String,
        /// Client-chosen upload session identifier.
        upload_id: String,
        /// Transport compression format.
        compression: Compression,
        /// Uncompressed blob digest.
        digest: Digest,
    },
}

impl ResourceName {
    /// Constructs a validated `Blob` resource name.
    ///
    /// # Errors
    ///
    /// Returns [`ResourceNameError`] if `instance_name` contains empty, dot, or reserved segments.
    pub fn blob(
        instance_name: impl Into<String>,
        compression: Compression,
        digest: Digest,
    ) -> Result<Self, ResourceNameError> {
        let instance_name = instance_name.into();
        validate_instance_name(&instance_name)?;
        Ok(Self::Blob {
            instance_name,
            compression,
            digest,
        })
    }

    /// Constructs a validated `Upload` resource name.
    ///
    /// # Errors
    ///
    /// Returns [`ResourceNameError`] if `instance_name` or `upload_id` is invalid.
    pub fn upload(
        instance_name: impl Into<String>,
        upload_id: impl Into<String>,
        compression: Compression,
        digest: Digest,
    ) -> Result<Self, ResourceNameError> {
        let instance_name = instance_name.into();
        let upload_id = upload_id.into();
        validate_instance_name(&instance_name)?;
        validate_upload_id(&upload_id)?;
        Ok(Self::Upload {
            instance_name,
            upload_id,
            compression,
            digest,
        })
    }

    /// Parses a REAPI v2 `ByteStream` resource name string.
    ///
    /// # Errors
    ///
    /// Returns [`ResourceNameError`] if the resource name is malformed or uses an unsupported
    /// compressor or digest.
    pub fn parse(input: &str) -> Result<Self, ResourceNameError> {
        if input.is_empty() {
            return Err(ResourceNameError::Empty);
        }

        let segments: Vec<&str> = input.split('/').collect();
        for &segment in &segments {
            if segment.is_empty() {
                return Err(ResourceNameError::EmptySegment);
            }
            if segment == "." || segment == ".." {
                return Err(ResourceNameError::DotSegment);
            }
        }

        let keyword_index = segments
            .iter()
            .position(|segment| RESERVED_KEYWORDS.contains(segment))
            .ok_or(ResourceNameError::UnrecognizedFormat)?;

        let instance_name = segments[..keyword_index].join("/");
        let tail = &segments[keyword_index..];

        match tail {
            ["blobs", hash, size] => Ok(Self::Blob {
                instance_name,
                compression: Compression::Identity,
                digest: parse_digest(hash, size)?,
            }),
            ["compressed-blobs", compressor, hash, size] => Ok(Self::Blob {
                instance_name,
                compression: parse_compressor(compressor)?,
                digest: parse_digest(hash, size)?,
            }),
            ["uploads", upload_id, "blobs", hash, size] => {
                validate_upload_id(upload_id)?;
                Ok(Self::Upload {
                    instance_name,
                    upload_id: (*upload_id).to_owned(),
                    compression: Compression::Identity,
                    digest: parse_digest(hash, size)?,
                })
            }
            [
                "uploads",
                upload_id,
                "compressed-blobs",
                compressor,
                hash,
                size,
            ] => {
                validate_upload_id(upload_id)?;
                Ok(Self::Upload {
                    instance_name,
                    upload_id: (*upload_id).to_owned(),
                    compression: parse_compressor(compressor)?,
                    digest: parse_digest(hash, size)?,
                })
            }
            ["uploads", "blobs" | "compressed-blobs", ..] => {
                Err(ResourceNameError::MissingUploadId)
            }
            [first, ..]
                if *first != "blobs" && *first != "compressed-blobs" && *first != "uploads" =>
            {
                Err(ResourceNameError::ReservedInstanceSegment {
                    segment: (*first).to_owned(),
                })
            }
            _ => Err(ResourceNameError::UnrecognizedFormat),
        }
    }

    /// Returns the instance name prefix (`""` when omitted).
    #[must_use]
    pub fn instance_name(&self) -> &str {
        match self {
            Self::Blob { instance_name, .. } | Self::Upload { instance_name, .. } => instance_name,
        }
    }

    /// Returns the transport compression format encoded in the resource name.
    #[must_use]
    pub const fn compression(&self) -> Compression {
        match self {
            Self::Blob { compression, .. } | Self::Upload { compression, .. } => *compression,
        }
    }

    /// Returns the uncompressed blob [`Digest`].
    #[must_use]
    pub const fn digest(&self) -> Digest {
        match self {
            Self::Blob { digest, .. } | Self::Upload { digest, .. } => *digest,
        }
    }

    /// Returns the upload session identifier for `Upload` names, or `None` for `Blob` names.
    #[must_use]
    pub fn upload_id(&self) -> Option<&str> {
        match self {
            Self::Blob { .. } => None,
            Self::Upload { upload_id, .. } => Some(upload_id),
        }
    }
}

impl FromStr for ResourceName {
    type Err = ResourceNameError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::parse(text)
    }
}

impl fmt::Display for ResourceName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let instance = self.instance_name();
        if !instance.is_empty() {
            write!(formatter, "{instance}/")?;
        }
        if let Some(upload_id) = self.upload_id() {
            write!(formatter, "uploads/{upload_id}/")?;
        }
        let digest = self.digest();
        match self.compression() {
            Compression::Identity => write!(formatter, "blobs/{}/{}", digest.hex(), digest.size()),
            Compression::Zstd => {
                write!(
                    formatter,
                    "compressed-blobs/zstd/{}/{}",
                    digest.hex(),
                    digest.size()
                )
            }
        }
    }
}

fn validate_instance_name(instance_name: &str) -> Result<(), ResourceNameError> {
    if instance_name.is_empty() {
        return Ok(());
    }
    for segment in instance_name.split('/') {
        if segment.is_empty() {
            return Err(ResourceNameError::EmptySegment);
        }
        if segment == "." || segment == ".." {
            return Err(ResourceNameError::DotSegment);
        }
        if RESERVED_KEYWORDS.contains(&segment) {
            return Err(ResourceNameError::ReservedInstanceSegment {
                segment: segment.to_owned(),
            });
        }
    }
    Ok(())
}

fn validate_upload_id(upload_id: &str) -> Result<(), ResourceNameError> {
    if upload_id.is_empty() {
        return Err(ResourceNameError::MissingUploadId);
    }
    if upload_id.contains('/') {
        return Err(ResourceNameError::UnrecognizedFormat);
    }
    if upload_id == "." || upload_id == ".." {
        return Err(ResourceNameError::DotSegment);
    }
    if RESERVED_KEYWORDS.contains(&upload_id) {
        return Err(ResourceNameError::MissingUploadId);
    }
    Ok(())
}

fn parse_compressor(name: &str) -> Result<Compression, ResourceNameError> {
    match name {
        "zstd" => Ok(Compression::Zstd),
        other => Err(ResourceNameError::UnsupportedCompressor {
            compressor: other.to_owned(),
        }),
    }
}

fn parse_digest(hash: &str, size_str: &str) -> Result<Digest, ResourceNameError> {
    if size_str.is_empty() || !size_str.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(DigestError::InvalidSize {
            input: size_str.to_owned(),
        }
        .into());
    }
    let size = size_str
        .parse::<u64>()
        .map_err(|_| DigestError::InvalidSize {
            input: size_str.to_owned(),
        })?;
    Ok(Digest::from_hex_and_size(hash, size)?)
}

#[cfg(test)]
mod tests {
    use gang_types::Digest;

    use crate::codec::Compression;

    use super::{ResourceName, ResourceNameError};

    #[test]
    fn parses_and_formats_all_valid_variants() {
        let digest = Digest::from_bytes(b"resource-name-payload");
        let hex = digest.hex();
        let size = digest.size();

        let cases = [
            (
                format!("blobs/{hex}/{size}"),
                ResourceName::Blob {
                    instance_name: String::new(),
                    compression: Compression::Identity,
                    digest,
                },
            ),
            (
                format!("prod/team/blobs/{hex}/{size}"),
                ResourceName::Blob {
                    instance_name: "prod/team".to_owned(),
                    compression: Compression::Identity,
                    digest,
                },
            ),
            (
                format!("compressed-blobs/zstd/{hex}/{size}"),
                ResourceName::Blob {
                    instance_name: String::new(),
                    compression: Compression::Zstd,
                    digest,
                },
            ),
            (
                format!("default/compressed-blobs/zstd/{hex}/{size}"),
                ResourceName::Blob {
                    instance_name: "default".to_owned(),
                    compression: Compression::Zstd,
                    digest,
                },
            ),
            (
                format!("uploads/550e8400-e29b-41d4-a716-446655440000/blobs/{hex}/{size}"),
                ResourceName::Upload {
                    instance_name: String::new(),
                    upload_id: "550e8400-e29b-41d4-a716-446655440000".to_owned(),
                    compression: Compression::Identity,
                    digest,
                },
            ),
            (
                format!("org/app/uploads/up-1/blobs/{hex}/{size}"),
                ResourceName::Upload {
                    instance_name: "org/app".to_owned(),
                    upload_id: "up-1".to_owned(),
                    compression: Compression::Identity,
                    digest,
                },
            ),
            (
                format!("uploads/up-2/compressed-blobs/zstd/{hex}/{size}"),
                ResourceName::Upload {
                    instance_name: String::new(),
                    upload_id: "up-2".to_owned(),
                    compression: Compression::Zstd,
                    digest,
                },
            ),
            (
                format!("inst/uploads/up-3/compressed-blobs/zstd/{hex}/{size}"),
                ResourceName::Upload {
                    instance_name: "inst".to_owned(),
                    upload_id: "up-3".to_owned(),
                    compression: Compression::Zstd,
                    digest,
                },
            ),
        ];

        for (raw, expected) in cases {
            let parsed: ResourceName = raw.parse().expect("valid resource name");
            assert_eq!(parsed, expected);
            assert_eq!(parsed.to_string(), raw);
        }
    }

    #[test]
    fn normalizes_uppercase_hex_on_display() {
        let digest = Digest::from_bytes(b"uppercase");
        let raw = format!(
            "blobs/{}/{}",
            digest.hex().to_ascii_uppercase(),
            digest.size()
        );
        let parsed = ResourceName::parse(&raw).expect("uppercase hex parses");
        assert_eq!(parsed.digest(), digest);
        assert_eq!(
            parsed.to_string(),
            format!("blobs/{}/{}", digest.hex(), digest.size())
        );
    }

    #[test]
    fn rejects_malformed_resource_names() {
        let digest = Digest::from_bytes(b"bad");
        let hex = digest.hex();
        let size = digest.size();

        assert_eq!(ResourceName::parse(""), Err(ResourceNameError::Empty));
        for empty_seg in [
            format!("/blobs/{hex}/{size}"),
            format!("inst//blobs/{hex}/{size}"),
            format!("blobs/{hex}/{size}/"),
        ] {
            assert_eq!(
                ResourceName::parse(&empty_seg),
                Err(ResourceNameError::EmptySegment)
            );
        }
        for dot_seg in [
            format!("../blobs/{hex}/{size}"),
            format!("inst/./blobs/{hex}/{size}"),
            format!("uploads/../blobs/{hex}/{size}"),
        ] {
            assert_eq!(
                ResourceName::parse(&dot_seg),
                Err(ResourceNameError::DotSegment)
            );
        }
        assert_eq!(
            ResourceName::parse(&format!("compressed-blobs/deflate/{hex}/{size}")),
            Err(ResourceNameError::UnsupportedCompressor {
                compressor: "deflate".to_owned()
            })
        );
        assert_eq!(
            ResourceName::parse(&format!("compressed-blobs/identity/{hex}/{size}")),
            Err(ResourceNameError::UnsupportedCompressor {
                compressor: "identity".to_owned()
            })
        );
        assert_eq!(
            ResourceName::parse(&format!("uploads/blobs/{hex}/{size}")),
            Err(ResourceNameError::MissingUploadId)
        );
        assert_eq!(
            ResourceName::parse(&format!("actions/x/blobs/{hex}/{size}")),
            Err(ResourceNameError::ReservedInstanceSegment {
                segment: "actions".to_owned()
            })
        );
        for unrecognized in [
            format!("blobs/{hex}"),
            format!("blobs/{hex}/{size}/extra"),
            format!("uploads/u1/blobs/{hex}/{size}/extra"),
            format!("blobs/blobs/{hex}/{size}"),
            "just/some/path".to_owned(),
        ] {
            assert_eq!(
                ResourceName::parse(&unrecognized),
                Err(ResourceNameError::UnrecognizedFormat),
                "input: {unrecognized}"
            );
        }
        assert!(matches!(
            ResourceName::parse(&format!("blobs/{hex}/-1")),
            Err(ResourceNameError::InvalidDigest(_))
        ));
        assert!(matches!(
            ResourceName::parse(&format!("blobs/{hex}/+1")),
            Err(ResourceNameError::InvalidDigest(_))
        ));
        assert!(matches!(
            ResourceName::parse(&format!("blobs/nothex/{size}")),
            Err(ResourceNameError::InvalidDigest(_))
        ));
    }

    #[test]
    fn constructors_validate_instance_and_upload_id() {
        let digest = Digest::EMPTY;
        assert!(matches!(
            ResourceName::blob("bad/blobs", Compression::Identity, digest),
            Err(ResourceNameError::ReservedInstanceSegment { .. })
        ));
        assert_eq!(
            ResourceName::upload("inst", "", Compression::Identity, digest),
            Err(ResourceNameError::MissingUploadId)
        );
        assert_eq!(
            ResourceName::upload("inst", "a/b", Compression::Identity, digest),
            Err(ResourceNameError::UnrecognizedFormat)
        );
    }
}
