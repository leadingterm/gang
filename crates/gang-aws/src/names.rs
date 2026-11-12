//! Names and identifiers from the AWS config. Each type checks its value when it is built and
//! when it is deserialized, so a value that parses is valid.

use std::path::{Path, PathBuf};

use gang_types::validated_string;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Shortest bucket name, in bytes.
const BUCKET_NAME_MIN_LEN: usize = 3;
/// Longest bucket name, in bytes.
const BUCKET_NAME_MAX_LEN: usize = 63;
/// Longest fleet tag key, in bytes.
const FLEET_TAG_KEY_MAX_LEN: usize = 128;
/// Prefix that AWS reserves for its own tag keys, in any letter case.
const RESERVED_TAG_PREFIX: &[u8] = b"aws:";

/// A config value that names something AWS cannot accept.
#[derive(Debug, PartialEq, Eq, Error)]
pub enum AwsNameError {
    /// The region is empty or has characters outside `a-z`, `0-9` and `-`.
    #[error("region must be non-empty and contain only a-z, 0-9 and '-'")]
    Region,
    /// The bucket name has the wrong length or characters.
    #[error("bucket must be 3 to 63 characters of a-z, 0-9, '.' and '-'")]
    Bucket,
    /// The key prefix is rooted, which S3 keys cannot be.
    #[error("key_prefix must not start with '/'")]
    KeyPrefix,
    /// The fleet tag key is outside the allowed set, or is reserved.
    #[error(
        "fleet_tag_key must be 1 to 128 characters of a-z, A-Z, 0-9 and '_ . : = + - @ ,', and \
         must not be '.', '..' or '_index' (reserved by the instance metadata service) or start \
         with 'aws:' in any case"
    )]
    FleetTagKey,
    /// The KMS key identifier is empty.
    #[error("key_id must not be empty")]
    KeyId,
    /// The scratch mount point is relative or is the filesystem root.
    #[error("scratch_mount_point must be an absolute path other than the filesystem root")]
    MountPoint,
}

fn check_region(value: &str) -> Result<(), AwsNameError> {
    let well_formed = !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
    if well_formed {
        Ok(())
    } else {
        Err(AwsNameError::Region)
    }
}

fn check_bucket(value: &str) -> Result<(), AwsNameError> {
    let length_ok = (BUCKET_NAME_MIN_LEN..=BUCKET_NAME_MAX_LEN).contains(&value.len());
    let well_formed = value.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'.' || byte == b'-'
    });
    if length_ok && well_formed {
        Ok(())
    } else {
        Err(AwsNameError::Bucket)
    }
}

fn check_key_prefix(value: &str) -> Result<(), AwsNameError> {
    if value.starts_with('/') {
        Err(AwsNameError::KeyPrefix)
    } else {
        Ok(())
    }
}

fn check_fleet_tag_key(value: &str) -> Result<(), AwsNameError> {
    let length_ok = (1..=FLEET_TAG_KEY_MAX_LEN).contains(&value.len());
    let well_formed = value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric()
            || matches!(byte, b'_' | b'.' | b':' | b'=' | b'+' | b'-' | b'@' | b',')
    });
    let reserved = matches!(value, "." | ".." | "_index")
        || value
            .as_bytes()
            .get(..RESERVED_TAG_PREFIX.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(RESERVED_TAG_PREFIX));
    if length_ok && well_formed && !reserved {
        Ok(())
    } else {
        Err(AwsNameError::FleetTagKey)
    }
}

fn check_key_id(value: &str) -> Result<(), AwsNameError> {
    if value.is_empty() {
        Err(AwsNameError::KeyId)
    } else {
        Ok(())
    }
}

validated_string!(
    /// An AWS region such as `eu-north-1`.
    pub Region,
    check_region,
    AwsNameError
);

validated_string!(
    /// An S3 bucket name.
    pub BucketName,
    check_bucket,
    AwsNameError
);

validated_string!(
    /// The key prefix under which the cache stores its objects. Empty means the bucket root.
    #[derive(Default)]
    pub KeyPrefix,
    check_key_prefix,
    AwsNameError
);

validated_string!(
    /// The instance tag key that names a node's fleet.
    pub FleetTagKey,
    check_fleet_tag_key,
    AwsNameError
);

validated_string!(
    /// A KMS key identifier, as AWS accepts it: a key ID, an ARN, or an alias.
    pub KeyId,
    check_key_id,
    AwsNameError
);

impl FleetTagKey {
    /// The fleet tag key used when the config does not set one.
    pub const DEFAULT: &str = "gang-fleet";
}

impl Default for FleetTagKey {
    fn default() -> Self {
        Self(Self::DEFAULT.into())
    }
}

/// An absolute scratch mount point other than the filesystem root.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "PathBuf", into = "PathBuf")]
pub struct MountPoint(PathBuf);

impl MountPoint {
    /// The mount point as a path.
    #[must_use]
    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

impl TryFrom<PathBuf> for MountPoint {
    type Error = AwsNameError;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        if path.is_absolute() && path.parent().is_some() {
            Ok(Self(path))
        } else {
            Err(AwsNameError::MountPoint)
        }
    }
}

impl From<MountPoint> for PathBuf {
    fn from(mount: MountPoint) -> Self {
        mount.0
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn region_accepts_well_formed_and_rejects_the_rest() -> Result<(), Box<dyn Error>> {
        assert_eq!(Region::try_new("us-east-1")?.as_str(), "us-east-1");
        assert_eq!(Region::try_new(""), Err(AwsNameError::Region));
        assert_eq!(Region::try_new("US"), Err(AwsNameError::Region));
        assert_eq!(Region::try_new("eu_west"), Err(AwsNameError::Region));
        Ok(())
    }

    #[test]
    fn bucket_enforces_length_and_characters() -> Result<(), Box<dyn Error>> {
        assert_eq!(BucketName::try_new("abc")?.as_str(), "abc");
        assert_eq!(BucketName::try_new("my-bucket.1")?.as_str(), "my-bucket.1");
        assert_eq!(
            BucketName::try_new("ab"),
            Err(AwsNameError::Bucket),
            "two characters is too short"
        );
        assert_eq!(
            BucketName::try_new("a".repeat(64)),
            Err(AwsNameError::Bucket),
            "sixty-four characters is too long"
        );
        assert_eq!(BucketName::try_new("MyBucket"), Err(AwsNameError::Bucket));
        Ok(())
    }

    #[test]
    fn key_prefix_must_not_be_rooted() -> Result<(), Box<dyn Error>> {
        assert_eq!(KeyPrefix::try_new("/x"), Err(AwsNameError::KeyPrefix));
        assert_eq!(KeyPrefix::try_new("a/")?.as_str(), "a/");
        assert_eq!(KeyPrefix::try_new("")?.as_str(), "");
        assert_eq!(KeyPrefix::default().as_str(), "");
        Ok(())
    }

    #[test]
    fn fleet_tag_key_rejects_reserved_and_malformed_keys() {
        for reserved in ["aws:foo", "AWS:foo", "Aws:x", ".", "..", "_index"] {
            assert_eq!(
                FleetTagKey::try_new(reserved),
                Err(AwsNameError::FleetTagKey),
                "{reserved} must be rejected"
            );
        }
        assert_eq!(
            FleetTagKey::try_new("a".repeat(129)),
            Err(AwsNameError::FleetTagKey)
        );
        assert_eq!(
            FleetTagKey::try_new("has space"),
            Err(AwsNameError::FleetTagKey)
        );
        assert_eq!(FleetTagKey::try_new(""), Err(AwsNameError::FleetTagKey));
    }

    #[test]
    fn fleet_tag_key_accepts_allowed_keys() -> Result<(), Box<dyn Error>> {
        assert_eq!(FleetTagKey::try_new("gang-fleet")?.as_str(), "gang-fleet");
        assert_eq!(FleetTagKey::try_new("a:b=c+d@e,f")?.as_str(), "a:b=c+d@e,f");
        assert_eq!(FleetTagKey::try_new("a".repeat(128))?.as_str().len(), 128);
        assert_eq!(FleetTagKey::try_new("awsome")?.as_str(), "awsome");
        Ok(())
    }

    #[test]
    fn fleet_tag_key_default_is_the_documented_key() {
        assert_eq!(FleetTagKey::DEFAULT, "gang-fleet");
        assert_eq!(FleetTagKey::default().as_str(), FleetTagKey::DEFAULT);
    }

    #[test]
    fn key_id_must_not_be_empty() -> Result<(), Box<dyn Error>> {
        assert_eq!(KeyId::try_new(""), Err(AwsNameError::KeyId));
        assert_eq!(KeyId::try_new("alias/gang")?.as_str(), "alias/gang");
        Ok(())
    }

    #[test]
    fn mount_point_must_be_absolute_and_not_root() -> Result<(), Box<dyn Error>> {
        assert_eq!(
            MountPoint::try_from(PathBuf::from("/")),
            Err(AwsNameError::MountPoint)
        );
        assert_eq!(
            MountPoint::try_from(PathBuf::from("rel/path")),
            Err(AwsNameError::MountPoint)
        );
        let mount = MountPoint::try_from(PathBuf::from("/mnt/scratch"))?;
        assert_eq!(mount.as_path(), Path::new("/mnt/scratch"));
        Ok(())
    }

    #[test]
    fn deserialize_rejects_invalid_values() {
        assert!(serde_json::from_str::<Region>("\"US\"").is_err());
        assert!(serde_json::from_str::<FleetTagKey>("\"aws:x\"").is_err());
        assert!(serde_json::from_str::<BucketName>("\"ab\"").is_err());
        assert!(serde_json::from_str::<KeyPrefix>("\"/x\"").is_err());
        assert!(serde_json::from_str::<KeyId>("\"\"").is_err());
        assert!(serde_json::from_str::<MountPoint>("\"/\"").is_err());
    }

    #[test]
    fn round_trips_through_serde() -> Result<(), Box<dyn Error>> {
        let region: Region = serde_json::from_str("\"eu-north-1\"")?;
        assert_eq!(serde_json::to_string(&region)?, "\"eu-north-1\"");
        let mount: MountPoint = serde_json::from_str("\"/mnt/scratch\"")?;
        assert_eq!(serde_json::to_string(&mount)?, "\"/mnt/scratch\"");
        Ok(())
    }
}
