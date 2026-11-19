//! Pure publication and retention decisions for public AMIs.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::errors::{DeployError, Expected, Result};
use crate::newtype::{lower_hex, text_newtype};
pub use crate::sha::ImageSha256;
use crate::{Timestamp, UnixTime};

/// The time after a successor's publication before its predecessor is deprecated, in seconds.
pub const DEPRECATE_AFTER: i64 = 184 * 24 * 60 * 60;
/// The time after deprecation before an image is retired, in seconds.
pub const DEREGISTER_AFTER: i64 = 92 * 24 * 60 * 60;

/// An image architecture, using Gang's names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Architecture {
    /// 64-bit Arm.
    Aarch64,
    /// 64-bit x86.
    X86_64,
}
impl Architecture {
    /// All architectures required for a converged release.
    pub const ALL: [Self; 2] = [Self::Aarch64, Self::X86_64];

    /// The architecture name understood by EC2.
    #[must_use]
    pub const fn aws_name(self) -> &'static str {
        match self {
            Self::Aarch64 => "arm64",
            Self::X86_64 => "x86_64",
        }
    }
}
impl fmt::Display for Architecture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Aarch64 => "aarch64",
            Self::X86_64 => "x86_64",
        })
    }
}
impl FromStr for Architecture {
    type Err = DeployError;
    fn from_str(text: &str) -> Result<Self> {
        match text {
            "aarch64" => Ok(Self::Aarch64),
            "x86_64" => Ok(Self::X86_64),
            _ => Err(invalid(text, Expected::Architecture)),
        }
    }
}
impl Serialize for Architecture {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}
fn invalid(text: &str, expected: Expected) -> DeployError {
    DeployError::Invalid {
        text: text.to_owned(),
        expected,
    }
}
fn check_tag(text: &str) -> Result<()> {
    if text.starts_with('v') && !text.chars().any(char::is_whitespace) {
        Ok(())
    } else {
        Err(invalid(text, Expected::ReleaseTag))
    }
}
fn check_id(text: &str) -> Result<()> {
    match text.strip_prefix("ami-") {
        Some(hex) if !hex.is_empty() => lower_hex(hex, hex.len(), Expected::ImageId)
            .map_err(|_| invalid(text, Expected::ImageId)),
        _ => Err(invalid(text, Expected::ImageId)),
    }
}
fn check_snapshot_id(text: &str) -> Result<()> {
    match text.strip_prefix("snap-") {
        Some(hex) if !hex.is_empty() => lower_hex(hex, hex.len(), Expected::SnapshotId)
            .map_err(|_| invalid(text, Expected::SnapshotId)),
        _ => Err(invalid(text, Expected::SnapshotId)),
    }
}
fn check_region(text: &str) -> Result<()> {
    if !text.is_empty()
        && text
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        Ok(())
    } else {
        Err(invalid(text, Expected::Region))
    }
}
text_newtype! {
    /// A release tag beginning with `v` and containing no whitespace.
    ReleaseTag, check_tag
}
text_newtype! {
    /// An EC2 AMI identifier.
    ImageId, check_id
}
text_newtype! {
    /// An EC2 snapshot identifier.
    SnapshotId, check_snapshot_id
}
text_newtype! {
    /// An EC2 region name.
    Region, check_region
}
impl Ord for Region {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}
impl PartialOrd for Region {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// An image name derived from its architecture and image digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmiName(String);
impl AmiName {
    /// Derives the stable image name.
    #[must_use]
    pub fn new(arch: Architecture, sha: &ImageSha256) -> Self {
        Self(format!("gang-{arch}-{}", &sha.as_str()[..16]))
    }
}
impl fmt::Display for AmiName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The state EC2 reports for an image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AmiState {
    /// The image can be used.
    Available,
    /// EC2 is still preparing the image.
    Pending,
    /// Any other EC2 state.
    Failed(
        /// The EC2 state.
        String,
    ),
}
impl<'de> Deserialize<'de> for AmiState {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Ok(match s.as_str() {
            "available" => Self::Available,
            "pending" => Self::Pending,
            _ => Self::Failed(s),
        })
    }
}
/// Deregistration protection reported by EC2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Protection {
    /// Protection is active.
    Enabled,
    /// Protection is disabled.
    #[default]
    Disabled,
    /// Protection was disabled and its cooldown has not elapsed.
    CoolingDown {
        /// The end of the cooldown.
        until: UnixTime,
    },
}
fn parse_time<E: serde::de::Error>(s: &str) -> std::result::Result<UnixTime, E> {
    OffsetDateTime::parse(s, &Rfc3339)
        .map(|t| UnixTime::from_seconds(t.unix_timestamp()))
        .map_err(E::custom)
}
fn time_field<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<UnixTime, D::Error> {
    parse_time(&String::deserialize(d)?)
}
fn optional_time<'de, D: Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<UnixTime>, D::Error> {
    Option::<String>::deserialize(d)?
        .map(|s| parse_time(&s))
        .transpose()
}
impl<'de> Deserialize<'de> for Protection {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        match s.as_str() {
            "enabled-with-cooldown" | "enabled-without-cooldown" => Ok(Self::Enabled),
            "disabled" => Ok(Self::Disabled),
            _ => match s.strip_prefix("disabled-until ") {
                Some(t) => Ok(Self::CoolingDown {
                    until: parse_time(t)?,
                }),
                None => Err(serde::de::Error::custom(
                    "invalid deregistration protection",
                )),
            },
        }
    }
}
#[derive(Debug, Clone, Deserialize)]
struct Tag {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "Value")]
    value: String,
}
#[derive(Debug, Clone, Deserialize)]
struct BlockDeviceMapping {
    #[serde(rename = "Ebs")]
    ebs: Option<Ebs>,
}
#[derive(Debug, Clone, Deserialize)]
struct Ebs {
    #[serde(rename = "SnapshotId")]
    snapshot_id: Option<SnapshotId>,
}
/// One image returned by `describe-images`.
#[derive(Debug, Clone, Deserialize)]
pub struct Ami {
    #[serde(rename = "ImageId")]
    id: ImageId,
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "State")]
    state: AmiState,
    #[serde(rename = "Public")]
    public: bool,
    #[serde(rename = "CreationDate", deserialize_with = "time_field")]
    creation: UnixTime,
    #[serde(
        rename = "DeprecationTime",
        default,
        deserialize_with = "optional_time"
    )]
    deprecation: Option<UnixTime>,
    #[serde(rename = "DeregistrationProtection", default)]
    protection: Protection,
    #[serde(rename = "Tags", default)]
    tags: Vec<Tag>,
    #[serde(default, rename = "BlockDeviceMappings")]
    mappings: Vec<BlockDeviceMapping>,
}
impl Ami {
    fn tag(&self, key: &str) -> Option<&str> {
        self.tags
            .iter()
            .find(|tag| tag.key == key)
            .map(|tag| tag.value.as_str())
    }
    fn identity(&self) -> Option<(Architecture, ReleaseTag, ImageSha256)> {
        let tag = self.tag("ec2:SharedTag/GangRelease")?.parse().ok()?;
        let sha: ImageSha256 = self.tag("ec2:SharedTag/ImageSha256")?.parse().ok()?;
        let arch = Architecture::ALL
            .into_iter()
            .find(|a| AmiName::new(*a, &sha).0 == self.name)?;
        Some((arch, tag, sha))
    }
    fn ready(&self) -> bool {
        self.state == AmiState::Available && self.public && self.protection == Protection::Enabled
    }
}
#[derive(Debug, Clone)]
struct ImageInventory {
    images: Vec<Ami>,
    public_other: usize,
    other_ids: BTreeSet<String>,
    other_snapshots: BTreeSet<String>,
}
impl<'de> Deserialize<'de> for ImageInventory {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Response {
            #[serde(rename = "Images")]
            images: Vec<serde_json::Value>,
        }
        #[derive(Deserialize)]
        struct OtherImage {
            #[serde(rename = "ImageId")]
            id: Option<ImageId>,
            #[serde(default, rename = "BlockDeviceMappings")]
            mappings: Vec<BlockDeviceMapping>,
        }
        let response = Response::deserialize(deserializer)?;
        let mut images = Vec::new();
        let mut public_other = 0;
        let mut other_ids = BTreeSet::new();
        let mut other_snapshots = BTreeSet::new();
        for value in response.images {
            let name = value
                .get("Name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if name.starts_with("gang-") {
                images.push(serde_json::from_value(value).map_err(serde::de::Error::custom)?);
            } else {
                if value.get("Public").and_then(serde_json::Value::as_bool) == Some(true) {
                    public_other += 1;
                }
                let other: OtherImage =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                other_ids.extend(other.id.map(|id| id.0));
                other_snapshots.extend(
                    other
                        .mappings
                        .into_iter()
                        .filter_map(|mapping| mapping.ebs?.snapshot_id.map(|id| id.0)),
                );
            }
        }
        Ok(Self {
            images,
            public_other,
            other_ids,
            other_snapshots,
        })
    }
}
fn quota<'de, D: Deserializer<'de>>(deserializer: D) -> std::result::Result<u32, D::Error> {
    let value = f64::deserialize(deserializer)?;
    if value.is_finite() && value >= 0.0 && value.fract() == 0.0 && value <= f64::from(u32::MAX) {
        return format!("{value:.0}")
            .parse()
            .map_err(serde::de::Error::custom);
    }
    Err(serde::de::Error::custom(invalid(
        &value.to_string(),
        Expected::Quota,
    )))
}
#[derive(Debug, Clone, Deserialize)]
struct Snapshot {
    #[serde(rename = "SnapshotId")]
    id: SnapshotId,
    #[serde(default, rename = "Tags")]
    tags: Vec<Tag>,
}
#[derive(Debug, Clone, Deserialize)]
struct Snapshots {
    #[serde(rename = "Snapshots")]
    snapshots: Vec<Snapshot>,
}
/// The observed quota and images in one region.
#[derive(Debug, Clone, Deserialize)]
pub struct RegionImages {
    #[serde(deserialize_with = "quota")]
    quota: u32,
    images: ImageInventory,
    snapshots: Snapshots,
}
/// Observations keyed by region, in deterministic order.
#[derive(Debug, Clone, Deserialize)]
#[serde(transparent)]
pub struct Regions(BTreeMap<Region, RegionImages>);
/// The release currently being published.
#[derive(Debug, Clone)]
pub struct Release {
    /// The release tag.
    pub tag: ReleaseTag,
    /// The digest of each architecture's image.
    pub images: BTreeMap<Architecture, ImageSha256>,
}
/// A publication decision and all of its inputs.
#[derive(Debug, Clone)]
pub struct Publication {
    home: Region,
    regions: Regions,
    release: Option<Release>,
    now: UnixTime,
}
/// One operation for the release glue to execute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// Import an image in the home region.
    Upload {
        /// Image architecture.
        arch: Architecture,
        /// Derived image name.
        name: AmiName,
        /// Image digest.
        sha256: ImageSha256,
        /// Release tag.
        tag: ReleaseTag,
        /// EC2 image description.
        description: String,
    },
    /// Copy a home image to another region.
    Copy {
        /// Destination region.
        region: Region,
        /// Source image ID.
        source: ImageId,
        /// Derived image name.
        name: AmiName,
        /// EC2 image description.
        description: String,
    },
    /// Wait for an image to become available.
    Wait {
        /// Image region.
        region: Region,
        /// Image ID.
        id: ImageId,
    },
    /// Enable deregistration protection.
    Protect {
        /// Image region.
        region: Region,
        /// Image ID.
        id: ImageId,
    },
    /// Make an image public.
    Publish {
        /// Image region.
        region: Region,
        /// Image ID.
        id: ImageId,
    },
    /// Schedule image deprecation.
    Deprecate {
        /// Image region.
        region: Region,
        /// Image ID.
        id: ImageId,
        /// Scheduled UTC time.
        at: Timestamp,
    },
    /// Disable deregistration protection.
    Unprotect {
        /// Image region.
        region: Region,
        /// Image ID.
        id: ImageId,
    },
    /// Record the retiring image on one of its snapshots.
    RetireSnapshot {
        /// Snapshot region.
        region: Region,
        /// Snapshot ID.
        id: SnapshotId,
        /// Retiring image ID.
        image: ImageId,
    },
    /// Deregister an image.
    Deregister {
        /// Image region.
        region: Region,
        /// Image ID.
        id: ImageId,
    },
    /// Delete an unreferenced snapshot of a retired image.
    DeleteSnapshot {
        /// Snapshot region.
        region: Region,
        /// Snapshot ID.
        id: SnapshotId,
    },
}
impl fmt::Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Upload {
                arch,
                name,
                sha256,
                tag,
                description,
            } => write!(f, "upload\t{arch}\t{name}\t{sha256}\t{tag}\t{description}"),
            Self::Copy {
                region,
                source,
                name,
                description,
            } => write!(f, "copy\t{region}\t{source}\t{name}\t{description}"),
            Self::Wait { region, id } => write!(f, "wait\t{region}\t{id}"),
            Self::Protect { region, id } => write!(f, "protect\t{region}\t{id}"),
            Self::Publish { region, id } => write!(f, "publish\t{region}\t{id}"),
            Self::Deprecate { region, id, at } => write!(f, "deprecate\t{region}\t{id}\t{at}"),
            Self::Unprotect { region, id } => write!(f, "unprotect\t{region}\t{id}"),
            Self::RetireSnapshot { region, id, image } => {
                write!(f, "retire-snapshot\t{region}\t{id}\t{image}")
            }
            Self::Deregister { region, id } => write!(f, "deregister\t{region}\t{id}"),
            Self::DeleteSnapshot { region, id } => write!(f, "delete-snapshot\t{region}\t{id}"),
        }
    }
}
/// The release's published image IDs, keyed by architecture.
#[derive(Debug, Serialize)]
#[serde(transparent)]
pub struct AmiManifest(BTreeMap<Architecture, ManifestImage>);
#[derive(Debug, Serialize)]
struct ManifestImage {
    sha256: ImageSha256,
    regions: BTreeMap<Region, ImageId>,
}
fn description(arch: Architecture, tag: &ReleaseTag, sha: &ImageSha256) -> String {
    format!("Gang {tag} gang-image-{arch} sha256:{sha}")
}
fn add_time(time: UnixTime, seconds: i64) -> Result<UnixTime> {
    time.as_secs()
        .checked_add(seconds)
        .map(UnixTime::from_seconds)
        .ok_or(DeployError::TimeRange {
            seconds: time.as_secs(),
        })
}

fn deprecation_time(target: UnixTime, now: UnixTime) -> Result<UnixTime> {
    let time = target.max(add_time(now, 60)?);
    let remainder = time.as_secs().rem_euclid(60);
    if remainder == 0 {
        Ok(time)
    } else {
        add_time(time, 60 - remainder)
    }
}
impl Publication {
    /// Creates a publication, rejecting an unobserved home region.
    ///
    /// # Errors
    /// Returns [`DeployError::HomeRegionMissing`] if the home region is absent.
    pub fn new(
        home: Region,
        regions: Regions,
        release: Option<Release>,
        now: UnixTime,
    ) -> Result<Self> {
        if !regions.0.contains_key(&home) {
            return Err(DeployError::HomeRegionMissing {
                region: home.to_string(),
            });
        }
        Ok(Self {
            home,
            regions,
            release,
            now,
        })
    }
    fn ordered_regions(&self) -> Vec<(&Region, &RegionImages)> {
        let mut regions: Vec<_> = self.regions.0.iter().collect();
        regions.sort_by_key(|(r, _)| (*r != &self.home, *r));
        regions
    }
    fn validate(&self) -> Result<()> {
        let mut identities: BTreeMap<(String, Architecture), String> = BTreeMap::new();
        if let Some(release) = &self.release {
            for (arch, sha) in &release.images {
                identities.insert((release.tag.to_string(), *arch), sha.to_string());
            }
        }
        for (region, data) in self.ordered_regions() {
            for image in &data.images.images {
                if let Some(release) = &self.release {
                    for (arch, sha) in &release.images {
                        if image.name == AmiName::new(*arch, sha).0 {
                            let found_tag = image.tag("ec2:SharedTag/GangRelease");
                            let found_sha = image.tag("ec2:SharedTag/ImageSha256");
                            if found_tag.is_some()
                                && found_sha.is_some()
                                && (found_tag != Some(release.tag.as_str())
                                    || found_sha != Some(sha.as_str()))
                            {
                                return Err(DeployError::ImageConflict {
                                    region: region.to_string(),
                                    name: image.name.clone(),
                                    expected: format!("{} {}", release.tag, sha),
                                    found: format!(
                                        "{} {}",
                                        found_tag.unwrap_or_default(),
                                        found_sha.unwrap_or_default()
                                    ),
                                });
                            }
                        }
                    }
                }
                let Some((arch, tag, sha)) = image.identity() else {
                    return Err(DeployError::ForeignImage {
                        region: region.to_string(),
                        name: image.name.clone(),
                    });
                };
                if let AmiState::Failed(state) = &image.state {
                    return Err(DeployError::FailedImage {
                        region: region.to_string(),
                        name: image.name.clone(),
                        state: state.clone(),
                    });
                }
                let key = (tag.to_string(), arch);
                match identities.get(&key) {
                    Some(expected) if expected != sha.as_str() => {
                        return Err(DeployError::ImageConflict {
                            region: region.to_string(),
                            name: image.name.clone(),
                            expected: expected.clone(),
                            found: sha.to_string(),
                        });
                    }
                    Some(_) => {}
                    None => {
                        identities.insert(key, sha.to_string());
                    }
                }
            }
        }
        Ok(())
    }
    fn ready_in_region(data: &RegionImages, tag: &str) -> bool {
        Architecture::ALL.iter().all(|arch| {
            data.images.images.iter().any(|image| {
                image
                    .identity()
                    .is_some_and(|(a, release, _)| a == *arch && release.as_str() == tag)
                    && image.ready()
            })
        })
    }

    fn converged(&self, tag: &str) -> bool {
        self.regions
            .0
            .values()
            .all(|data| Self::ready_in_region(data, tag))
    }

    fn ready_for_retirement(&self, predecessor: &str, successor: &str) -> bool {
        self.regions.0.iter().all(|(region, data)| {
            let holds_predecessor = data.images.images.iter().any(|image| {
                image
                    .identity()
                    .is_some_and(|(_, release, _)| release.as_str() == predecessor)
            });
            (region != &self.home && !holds_predecessor) || Self::ready_in_region(data, successor)
        })
    }

    fn successors(
        &self,
        eligible: impl Fn(&str, &str) -> bool,
    ) -> Result<BTreeMap<String, UnixTime>> {
        let Some(home) = self.regions.0.get(&self.home) else {
            return Err(DeployError::HomeRegionMissing {
                region: self.home.to_string(),
            });
        };
        let mut releases: BTreeMap<String, UnixTime> = BTreeMap::new();
        for image in &home.images.images {
            if let Some((_, tag, _)) = image.identity() {
                releases
                    .entry(tag.to_string())
                    .and_modify(|time| *time = (*time).min(image.creation))
                    .or_insert(image.creation);
            }
        }
        if let Some(release) = &self.release {
            releases
                .entry(release.tag.to_string())
                .or_insert(UnixTime::from_seconds(i64::MAX));
        }
        let mut ordered: Vec<_> = releases.into_iter().collect();
        ordered.sort_by(|a, b| (a.1, &a.0).cmp(&(b.1, &b.0)));
        let mut result = BTreeMap::new();
        for (tag, published) in &ordered {
            if let Some((_, successor_time)) = ordered
                .iter()
                .find(|(successor, time)| *time > *published && eligible(tag, successor))
            {
                result.insert(tag.clone(), add_time(*successor_time, DEPRECATE_AFTER)?);
            }
        }
        Ok(result)
    }
    fn retiring(&self, image: &Ami, successors: &BTreeMap<String, UnixTime>) -> Result<bool> {
        let Some((_, tag, _)) = image.identity() else {
            return Ok(false);
        };
        if !successors.contains_key(tag.as_str()) {
            return Ok(false);
        }
        match image.deprecation {
            Some(at) => Ok(self.now >= add_time(at, DEREGISTER_AFTER)?),
            None => Ok(false),
        }
    }
    fn preflight(&self, successors: &BTreeMap<String, UnixTime>) -> Result<()> {
        let Some(home) = self.regions.0.get(&self.home) else {
            return Err(DeployError::HomeRegionMissing {
                region: self.home.to_string(),
            });
        };
        for (region, data) in self.ordered_regions() {
            let public = data.images.public_other
                + data
                    .images
                    .images
                    .iter()
                    .filter(|image| image.public)
                    .count();
            let mut new = BTreeSet::new();
            for image in &data.images.images {
                if !image.public && !self.retiring(image, successors)? {
                    new.insert(image.name.clone());
                }
            }
            if region != &self.home {
                for image in &home.images.images {
                    if !self.retiring(image, successors)?
                        && !data
                            .images
                            .images
                            .iter()
                            .any(|present| present.name == image.name)
                    {
                        new.insert(image.name.clone());
                    }
                }
            }
            if let Some(release) = &self.release {
                for (arch, sha) in &release.images {
                    let name = AmiName::new(*arch, sha).0;
                    if !data.images.images.iter().any(|image| image.name == name) {
                        new.insert(name);
                    }
                }
            }
            let new = new.len();
            if public.saturating_add(new) > data.quota as usize {
                return Err(DeployError::QuotaExceeded {
                    region: region.to_string(),
                    public,
                    new,
                    quota: data.quota,
                });
            }
        }
        Ok(())
    }

    fn uploads(&self, home: &RegionImages, push: &mut impl FnMut(&Region, u8, String, Step)) {
        if let Some(release) = &self.release {
            for (arch, sha) in &release.images {
                let name = AmiName::new(*arch, sha);
                if !home.images.images.iter().any(|i| i.name == name.0) {
                    push(
                        &self.home,
                        0,
                        arch.to_string(),
                        Step::Upload {
                            arch: *arch,
                            name,
                            sha256: sha.clone(),
                            tag: release.tag.clone(),
                            description: description(*arch, &release.tag, sha),
                        },
                    );
                }
            }
        }
    }

    fn copies(
        &self,
        home: &RegionImages,
        region: &Region,
        data: &RegionImages,
        successors: &BTreeMap<String, UnixTime>,
        push: &mut impl FnMut(&Region, u8, String, Step),
    ) -> Result<()> {
        if region != &self.home {
            for image in &home.images.images {
                if image.state != AmiState::Available
                    || self.retiring(image, successors)?
                    || data.images.images.iter().any(|i| i.name == image.name)
                {
                    continue;
                }
                if let Some((arch, tag, sha)) = image.identity() {
                    push(
                        region,
                        1,
                        image.id.to_string(),
                        Step::Copy {
                            region: region.clone(),
                            source: image.id.clone(),
                            name: AmiName::new(arch, &sha),
                            description: description(arch, &tag, &sha),
                        },
                    );
                }
            }
        }
        Ok(())
    }

    fn retirement_step(
        &self,
        region: &Region,
        image: &Ami,
        push: &mut impl FnMut(&Region, u8, String, Step),
    ) {
        let id = image.id.clone();
        let key = id.to_string();
        match image.protection {
            Protection::Enabled => push(
                region,
                6,
                key,
                Step::Unprotect {
                    region: region.clone(),
                    id,
                },
            ),
            Protection::Disabled => {
                let remote_exists = region == &self.home
                    && self.regions.0.iter().any(|(other_region, data)| {
                        other_region != &self.home
                            && data
                                .images
                                .images
                                .iter()
                                .any(|other| other.name == image.name)
                    });
                if !remote_exists {
                    for snapshot in image
                        .mappings
                        .iter()
                        .filter_map(|mapping| mapping.ebs.as_ref()?.snapshot_id.as_ref())
                    {
                        push(
                            region,
                            7,
                            snapshot.to_string(),
                            Step::RetireSnapshot {
                                region: region.clone(),
                                id: snapshot.clone(),
                                image: image.id.clone(),
                            },
                        );
                    }
                    push(
                        region,
                        8,
                        key,
                        Step::Deregister {
                            region: region.clone(),
                            id,
                        },
                    );
                }
            }
            Protection::CoolingDown { .. } => {}
        }
    }

    fn image_steps(
        &self,
        region: &Region,
        image: &Ami,
        successors: &BTreeMap<String, UnixTime>,
        retirement_successors: &BTreeMap<String, UnixTime>,
        push: &mut impl FnMut(&Region, u8, String, Step),
    ) -> Result<()> {
        if image.identity().is_none() {
            return Ok(());
        }
        let id = image.id.clone();
        let key = id.to_string();
        if image.state == AmiState::Pending {
            push(
                region,
                2,
                key,
                Step::Wait {
                    region: region.clone(),
                    id,
                },
            );
            return Ok(());
        }
        if image.state != AmiState::Available {
            return Ok(());
        }
        if self.retiring(image, retirement_successors)? {
            self.retirement_step(region, image, push);
            return Ok(());
        }
        if image.protection == Protection::Disabled {
            push(
                region,
                3,
                key.clone(),
                Step::Protect {
                    region: region.clone(),
                    id: id.clone(),
                },
            );
        }
        if !image.public {
            push(
                region,
                4,
                key.clone(),
                Step::Publish {
                    region: region.clone(),
                    id: id.clone(),
                },
            );
        }
        if let Some((_, tag, _)) = image.identity()
            && let Some(target) = successors.get(tag.as_str())
        {
            let at = deprecation_time(*target, self.now)?;
            if image.deprecation.is_none_or(|stored| stored > at) {
                push(
                    region,
                    5,
                    key,
                    Step::Deprecate {
                        region: region.clone(),
                        id,
                        at: Timestamp::from_unix(at)?,
                    },
                );
            }
        }
        Ok(())
    }

    /// Computes operations for the current release in the observed regions only.
    ///
    /// Unlike [`Self::steps`], this considers only supplied architectures and
    /// does not backfill or change the lifecycle of older images or snapshots.
    ///
    /// # Errors
    /// Returns an error if no release was supplied, images are inconsistent,
    /// or an observed region lacks public image quota.
    pub fn release_steps(&self) -> Result<Vec<Step>> {
        let release = self.release.as_ref().ok_or(DeployError::NoRelease)?;
        self.validate()?;
        let names: Vec<_> = release
            .images
            .iter()
            .map(|(arch, sha)| (*arch, sha, AmiName::new(*arch, sha)))
            .collect();
        for (region, data) in self.ordered_regions() {
            let public = data.images.public_other
                + data
                    .images
                    .images
                    .iter()
                    .filter(|image| image.public)
                    .count();
            let new = names
                .iter()
                .filter(|(_, _, name)| {
                    !data
                        .images
                        .images
                        .iter()
                        .any(|image| image.name == name.0 && image.public)
                })
                .count();
            if public.saturating_add(new) > data.quota as usize {
                return Err(DeployError::QuotaExceeded {
                    region: region.to_string(),
                    public,
                    new,
                    quota: data.quota,
                });
            }
        }
        let Some(home) = self.regions.0.get(&self.home) else {
            return Err(DeployError::HomeRegionMissing {
                region: self.home.to_string(),
            });
        };
        let mut ordered: Vec<(bool, Region, u8, String, Step)> = Vec::new();
        let mut push = |region: &Region, rank, key: String, step| {
            ordered.push((region != &self.home, region.clone(), rank, key, step));
        };
        self.uploads(home, &mut push);
        let successors = BTreeMap::new();
        for (region, data) in self.ordered_regions() {
            for (arch, sha, name) in &names {
                if region != &self.home
                    && !data.images.images.iter().any(|image| image.name == name.0)
                    && let Some(image) =
                        home.images.images.iter().find(|image| {
                            image.name == name.0 && image.state == AmiState::Available
                        })
                {
                    push(
                        region,
                        1,
                        image.id.to_string(),
                        Step::Copy {
                            region: region.clone(),
                            source: image.id.clone(),
                            name: name.clone(),
                            description: description(*arch, &release.tag, sha),
                        },
                    );
                }
            }
            for image in &data.images.images {
                if names.iter().any(|(_, _, name)| image.name == name.0) {
                    self.image_steps(region, image, &successors, &successors, &mut push)?;
                }
            }
        }
        ordered.sort_by(|a, b| (&a.0, &a.1, &a.2, &a.3).cmp(&(&b.0, &b.1, &b.2, &b.3)));
        ordered.dedup_by(|a, b| a.4 == b.4);
        Ok(ordered.into_iter().map(|(_, _, _, _, step)| step).collect())
    }

    /// Computes the ordered, idempotent publication operations.
    ///
    /// # Errors
    /// Returns an error on inconsistent images, exceeded quota, or unrepresentable times.
    pub fn steps(&self) -> Result<Vec<Step>> {
        self.validate()?;
        let successors = self.successors(|_, tag| self.converged(tag))?;
        let retirement_successors = self.successors(|predecessor, successor| {
            self.ready_for_retirement(predecessor, successor)
        })?;
        self.preflight(&retirement_successors)?;
        let mut ordered: Vec<(bool, Region, u8, String, Step)> = Vec::new();
        let mut push = |region: &Region, rank, key: String, step| {
            ordered.push((region != &self.home, region.clone(), rank, key, step));
        };
        let Some(home) = self.regions.0.get(&self.home) else {
            return Err(DeployError::HomeRegionMissing {
                region: self.home.to_string(),
            });
        };
        self.uploads(home, &mut push);
        for (region, data) in self.ordered_regions() {
            self.copies(home, region, data, &retirement_successors, &mut push)?;
            for image in &data.images.images {
                self.image_steps(
                    region,
                    image,
                    &successors,
                    &retirement_successors,
                    &mut push,
                )?;
            }
            for snapshot in &data.snapshots.snapshots {
                let marker = snapshot
                    .tags
                    .iter()
                    .find(|tag| tag.key == "ec2:SharedTag/GangRetiredImage");
                let Some(image) = marker
                    .map(|tag| tag.value.as_str())
                    .filter(|id| check_id(id).is_ok())
                else {
                    continue;
                };
                if !data.images.other_ids.contains(image)
                    && !data
                        .images
                        .images
                        .iter()
                        .any(|ami| ami.id.as_str() == image)
                    && !data.images.other_snapshots.contains(snapshot.id.as_str())
                    && !data.images.images.iter().any(|ami| {
                        ami.mappings.iter().any(|mapping| {
                            mapping
                                .ebs
                                .as_ref()
                                .and_then(|ebs| ebs.snapshot_id.as_ref())
                                == Some(&snapshot.id)
                        })
                    })
                {
                    push(
                        region,
                        9,
                        snapshot.id.to_string(),
                        Step::DeleteSnapshot {
                            region: region.clone(),
                            id: snapshot.id.clone(),
                        },
                    );
                }
            }
        }
        ordered.sort_by(|a, b| (&a.0, &a.1, &a.2, &a.3).cmp(&(&b.0, &b.1, &b.2, &b.3)));
        ordered.dedup_by(|a, b| a.4 == b.4);
        Ok(ordered.into_iter().map(|(_, _, _, _, step)| step).collect())
    }
    /// Builds the manifest only after every release image is available, public and protected.
    ///
    /// # Errors
    /// Returns an error for invalid input, a missing release, or an unconverged image.
    pub fn manifest(&self) -> Result<AmiManifest> {
        self.validate()?;
        let release = self.release.as_ref().ok_or(DeployError::NoRelease)?;
        let mut manifest = BTreeMap::new();
        for (arch, sha) in &release.images {
            let name = AmiName::new(*arch, sha);
            let mut regions = BTreeMap::new();
            for (region, data) in self.ordered_regions() {
                let image = data.images.images.iter().find(|i| i.name == name.0);
                let reason = match image {
                    None => Some("missing"),
                    Some(i) if i.state != AmiState::Available => Some("not available"),
                    Some(i) if !i.public => Some("not public"),
                    Some(i) if i.protection != Protection::Enabled => Some("not protected"),
                    Some(_) => None,
                };
                if let Some(reason) = reason {
                    return Err(DeployError::NotConverged {
                        region: region.to_string(),
                        name: name.to_string(),
                        reason: reason.to_owned(),
                    });
                }
                if let Some(image) = image {
                    regions.insert(region.clone(), image.id.clone());
                }
            }
            manifest.insert(
                *arch,
                ManifestImage {
                    sha256: sha.clone(),
                    regions,
                },
            );
        }
        Ok(AmiManifest(manifest))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{Architecture, Publication, Regions, Release};
    use crate::{DeployError, UnixTime};
    use serde_json::{Value, json};

    const X: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const A: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const D: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    const OLD: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    fn image(id: &str, arch: &str, sha: &str, tag: &str, date: &str) -> Value {
        json!({"ImageId": id, "Name": format!("gang-{arch}-{}", &sha[..16]),
        "Architecture": if arch == "aarch64" { "arm64" } else { arch },
        "State": "available", "Public": true, "CreationDate": date,
        "DeregistrationProtection": "enabled-with-cooldown",
        "Tags": [{"Key":"ec2:SharedTag/GangRelease", "Value":tag},
                 {"Key":"ec2:SharedTag/ImageSha256", "Value":sha}]})
    }
    fn current() -> Vec<Value> {
        vec![
            image("ami-01", "x86_64", X, "v2", "2025-01-01T00:00:00.000Z"),
            image("ami-02", "aarch64", A, "v2", "2025-01-01T00:00:00Z"),
        ]
    }
    fn old() -> Value {
        image("ami-03", "x86_64", OLD, "v1", "2024-01-01T00:00:00Z")
    }
    fn publication(
        home: &[Value],
        other: &[Value],
        release: bool,
        now: i64,
        quota: u32,
    ) -> Publication {
        let regions: Regions = serde_json::from_value(json!({
            "home-1": {"quota":quota,"images":{"Images":home.to_vec()},"snapshots":{"Snapshots":[]}},
            "other-1": {"quota":quota,"images":{"Images":other.to_vec()},"snapshots":{"Snapshots":[]}}
        }))
        .unwrap();
        let release = release.then(|| Release {
            tag: "v2".parse().unwrap(),
            images: BTreeMap::from([
                (Architecture::X86_64, X.parse().unwrap()),
                (Architecture::Aarch64, A.parse().unwrap()),
            ]),
        });
        Publication::new(
            "home-1".parse().unwrap(),
            regions,
            release,
            UnixTime::from_seconds(now),
        )
        .unwrap()
    }
    fn lines(p: &Publication) -> Vec<String> {
        p.steps().unwrap().iter().map(ToString::to_string).collect()
    }

    fn release_lines(p: &Publication) -> Vec<String> {
        p.release_steps()
            .unwrap()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    #[test]
    fn ordered_regions_prioritizes_home_then_key_order() {
        let mut p = publication(&[], &[], true, 0, 10);
        let data = p.regions.0.get(&p.home).unwrap().clone();
        p.regions.0.clear();
        for region in ["z-1", "c-1", "a-1", "b-1"] {
            p.regions.0.insert(region.parse().unwrap(), data.clone());
        }
        p.home = "z-1".parse().unwrap();
        assert_eq!(
            p.ordered_regions()
                .into_iter()
                .map(|(region, _)| region.as_str())
                .collect::<Vec<_>>(),
            vec!["z-1", "a-1", "b-1", "c-1"]
        );
        p.regions.0.remove(&p.home);
        assert_eq!(
            p.ordered_regions()
                .into_iter()
                .map(|(region, _)| region.as_str())
                .collect::<Vec<_>>(),
            vec!["a-1", "b-1", "c-1"]
        );
    }

    #[test]
    fn release_steps_only_plans_supplied_architectures() {
        let mut p = publication(&current(), &[], true, 0, 10);
        p.release
            .as_mut()
            .unwrap()
            .images
            .remove(&Architecture::Aarch64);
        p.regions
            .0
            .get_mut(&"other-1".parse().unwrap())
            .unwrap()
            .quota = 1;
        assert_eq!(
            release_lines(&p),
            vec![format!(
                "copy\tother-1\tami-01\tgang-x86_64-aaaaaaaaaaaaaaaa\tGang v2 gang-image-x86_64 sha256:{X}"
            )]
        );

        p.release.as_mut().unwrap().images.clear();
        assert!(release_lines(&p).is_empty());
    }

    #[test]
    fn release_steps_uploads_and_copies_only_current_images() {
        assert_eq!(
            release_lines(&publication(&[], &[], true, 0, 10)),
            lines(&publication(&[], &[], true, 0, 10))
        );
        let mut home = current();
        home.push(old());
        let result = release_lines(&publication(&home, &[], true, 0, 10));
        assert_eq!(
            result,
            vec![
                format!(
                    "copy\tother-1\tami-01\tgang-x86_64-aaaaaaaaaaaaaaaa\tGang v2 gang-image-x86_64 sha256:{X}"
                ),
                format!(
                    "copy\tother-1\tami-02\tgang-aarch64-bbbbbbbbbbbbbbbb\tGang v2 gang-image-aarch64 sha256:{A}"
                ),
            ]
        );
        home[0]["State"] = json!("pending");
        let result = release_lines(&publication(&home, &[], true, 0, 10));
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], "wait\thome-1\tami-01");
        assert!(result[1].starts_with("copy\tother-1\tami-02\t"));
    }

    #[test]
    fn release_steps_waits_and_publishes_current_images() {
        let mut home = current();
        home[0]["Public"] = json!(false);
        home[0]["DeregistrationProtection"] = json!("disabled");
        let mut other = current();
        other[0]["State"] = json!("pending");
        other[1]["Public"] = json!(false);
        assert_eq!(
            release_lines(&publication(&home, &other, true, 0, 10)),
            vec![
                "protect\thome-1\tami-01",
                "publish\thome-1\tami-01",
                "wait\tother-1\tami-01",
                "publish\tother-1\tami-02",
            ]
        );
    }

    #[test]
    fn release_steps_ignores_legacy_lifecycle_and_snapshots() {
        let mut legacy = old();
        legacy["DeprecationTime"] = json!("2020-01-01T00:00:00Z");
        legacy["DeregistrationProtection"] = json!("disabled");
        legacy["Public"] = json!(false);
        let mut home = current();
        home.push(legacy);
        let mut p = publication(&home, &current(), true, 1_800_000_000, 2);
        p.regions.0.get_mut(&p.home).unwrap().snapshots =
            serde_json::from_value(json!({"Snapshots": [{
                "SnapshotId": "snap-01",
                "State": "completed",
                "Tags": [{"Key": "ec2:SharedTag/GangRetiredImage", "Value": "ami-04"}]
            }]}))
            .unwrap();
        assert!(release_lines(&p).is_empty());
    }

    #[test]
    fn release_steps_requires_release_and_preflights_observed_regions() {
        assert!(matches!(
            publication(&[], &[], false, 0, 10).release_steps(),
            Err(DeployError::NoRelease)
        ));
        let mut p = publication(&[], &[], true, 0, 10);
        p.regions
            .0
            .get_mut(&"other-1".parse().unwrap())
            .unwrap()
            .quota = 1;
        assert!(matches!(
            p.release_steps(),
            Err(DeployError::QuotaExceeded { region, public: 0, new: 2, quota: 1 }) if region == "other-1"
        ));
        p.regions.0.remove(&"other-1".parse().unwrap());
        assert_eq!(release_lines(&p).len(), 2);

        let foreign = json!({"Public": true, "State": "failed"});
        let mut private = current();
        private[0]["Public"] = json!(false);
        private.push(foreign);
        assert!(matches!(
            publication(&private, &[], true, 0, 2).release_steps(),
            Err(DeployError::QuotaExceeded { region, public: 2, new: 1, quota: 2 }) if region == "home-1"
        ));
    }

    #[test]
    fn release_steps_preserves_identity_validation() {
        let mut conflicting = current();
        conflicting[0]["Tags"][1]["Value"] = json!(OLD);
        assert!(matches!(
            publication(&conflicting, &[], true, 0, 10).release_steps(),
            Err(DeployError::ImageConflict { .. })
        ));
        let mut failed = old();
        failed["State"] = json!("failed");
        assert!(matches!(
            publication(&[failed], &[], true, 0, 10).release_steps(),
            Err(DeployError::FailedImage { .. })
        ));
    }

    #[test]
    fn fresh_uploads_only() {
        assert_eq!(
            lines(&publication(&[], &[], true, 0, 10)),
            vec![
                format!(
                    "upload\taarch64\tgang-aarch64-bbbbbbbbbbbbbbbb\t{A}\tv2\tGang v2 gang-image-aarch64 sha256:{A}"
                ),
                format!(
                    "upload\tx86_64\tgang-x86_64-aaaaaaaaaaaaaaaa\t{X}\tv2\tGang v2 gang-image-x86_64 sha256:{X}"
                )
            ]
        );
    }
    #[test]
    fn private_home_is_protected_published_and_copied() {
        let mut images = current();
        for i in &mut images {
            i["Public"] = json!(false);
            i["DeregistrationProtection"] = json!("disabled");
        }
        assert_eq!(
            lines(&publication(&images, &[], true, 0, 10)),
            vec![
                "protect\thome-1\tami-01",
                "protect\thome-1\tami-02",
                "publish\thome-1\tami-01",
                "publish\thome-1\tami-02",
                &format!(
                    "copy\tother-1\tami-01\tgang-x86_64-aaaaaaaaaaaaaaaa\tGang v2 gang-image-x86_64 sha256:{X}"
                ),
                &format!(
                    "copy\tother-1\tami-02\tgang-aarch64-bbbbbbbbbbbbbbbb\tGang v2 gang-image-aarch64 sha256:{A}"
                )
            ]
        );
    }
    #[test]
    fn pending_copies_wait() {
        let home = current();
        let mut other = current();
        other[0]["State"] = json!("pending");
        other[1]["State"] = json!("pending");
        assert_eq!(
            lines(&publication(&home, &other, true, 0, 10)),
            vec!["wait\tother-1\tami-01", "wait\tother-1\tami-02"]
        );
    }
    #[test]
    fn successor_deprecates_then_is_idempotent() {
        let mut home = current();
        home.push(old());
        let mut other = current();
        other.push(old());
        let p = publication(&home, &other, true, 0, 10);
        assert_eq!(
            lines(&p),
            vec![
                "deprecate\thome-1\tami-03\t2025-07-04T00:00:00Z",
                "deprecate\tother-1\tami-03\t2025-07-04T00:00:00Z"
            ]
        );
        home[2]["DeprecationTime"] = json!("2025-07-04T00:00:00Z");
        other[2]["DeprecationTime"] = json!("2025-07-04T00:00:00Z");
        assert!(lines(&publication(&home, &other, true, 0, 10)).is_empty());
    }
    #[test]
    fn snapshots_pending_retirement_survive_image_disappearance() {
        let images = current();
        assert!(lines(&publication(&images, &images, false, 0, 10)).is_empty());

        let regions: Regions = serde_json::from_value(json!({
            "home-1": {
                "quota": 10,
                "images": {"Images": images},
                "snapshots": {"Snapshots": [{
                    "SnapshotId": "snap-01",
                    "State": "completed",
                    "Tags": [{"Key": "ec2:SharedTag/GangRetiredImage", "Value": "ami-03"}]
                }]}
            },
            "other-1": {
                "quota": 10,
                "images": {"Images": current()},
                "snapshots": {"Snapshots": []}
            }
        }))
        .unwrap();
        let p = Publication::new(
            "home-1".parse().unwrap(),
            regions,
            None,
            UnixTime::from_seconds(0),
        )
        .unwrap();
        assert_eq!(lines(&p), vec!["delete-snapshot\thome-1\tsnap-01"]);
    }
    fn snapshot_publication(images: &[Value], snapshots: &[Value]) -> Publication {
        let regions: Regions = serde_json::from_value(json!({
            "home-1": {
                "quota": 20,
                "images": {"Images": images},
                "snapshots": {"Snapshots": snapshots}
            }
        }))
        .unwrap();
        Publication::new(
            "home-1".parse().unwrap(),
            regions,
            None,
            UnixTime::from_seconds(1_759_536_000),
        )
        .unwrap()
    }

    fn retired_snapshot() -> Value {
        json!({"SnapshotId": "snap-01", "Tags": [
            {"Key": "ec2:SharedTag/GangRetiredImage", "Value": "ami-03"}
        ]})
    }

    #[test]
    fn tagged_snapshot_waits_for_retired_image_to_disappear() {
        let mut images = current();
        images.push(old());
        let p = snapshot_publication(&images, &[retired_snapshot()]);
        assert!(
            !lines(&p)
                .iter()
                .any(|line| line.starts_with("delete-snapshot\t"))
        );
    }

    #[test]
    fn tagged_snapshot_waits_for_foreign_image_reference() {
        let mut images = current();
        images.push(json!({
            "ImageId": "ami-04",
            "Name": "foreign",
            "Public": false,
            "State": "disabled",
            "BlockDeviceMappings": [{"Ebs": {"SnapshotId": "snap-01"}}]
        }));
        let p = snapshot_publication(&images, &[retired_snapshot()]);
        assert!(lines(&p).is_empty());
    }

    #[test]
    fn tagged_snapshot_waits_for_foreign_retired_image_id() {
        let mut images = current();
        images.push(json!({"ImageId": "ami-03", "Name": "foreign", "Public": false}));
        assert!(lines(&snapshot_publication(&images, &[retired_snapshot()])).is_empty());
    }

    #[test]
    fn tagged_snapshot_waits_for_gang_image_reference() {
        let mut images = current();
        images[0]["BlockDeviceMappings"] = json!([{"Ebs": {"SnapshotId": "snap-01"}}]);
        let p = snapshot_publication(&images, &[retired_snapshot()]);
        assert!(lines(&p).is_empty());
    }

    #[test]
    fn untagged_and_malformed_snapshot_markers_are_ignored() {
        let snapshots = [
            json!({"SnapshotId": "snap-01"}),
            json!({"SnapshotId": "snap-02", "Tags": [
                {"Key": "ec2:SharedTag/GangRetiredImage", "Value": "not-an-ami"}
            ]}),
            json!({"SnapshotId": "snap-03", "Tags": [
                {"Key": "GangRetiredImage", "Value": "ami-03"}
            ]}),
        ];
        assert!(lines(&snapshot_publication(&current(), &snapshots)).is_empty());
    }

    #[test]
    fn retiring_image_marks_every_snapshot_before_deregistering() {
        let mut retired = old();
        retired["DeprecationTime"] = json!("2025-07-04T00:00:00Z");
        retired["DeregistrationProtection"] = json!("disabled");
        retired["BlockDeviceMappings"] = json!([
            {"Ebs": {"SnapshotId": "snap-02"}},
            {"VirtualName": "ephemeral0"},
            {"Ebs": {}},
            {"Ebs": {"SnapshotId": "snap-01"}}
        ]);
        let mut images = current();
        images.push(retired);
        assert_eq!(
            lines(&snapshot_publication(&images, &[])),
            vec![
                "retire-snapshot\thome-1\tsnap-01\tami-03",
                "retire-snapshot\thome-1\tsnap-02\tami-03",
                "deregister\thome-1\tami-03",
            ]
        );
    }

    #[test]
    fn snapshot_inventory_and_typed_identifiers_are_required() {
        assert!(
            serde_json::from_value::<Regions>(json!({
                "home-1": {"quota": 10, "images": {"Images": []}}
            }))
            .is_err()
        );
        for foreign in [
            json!({"ImageId": "invalid"}),
            json!({"BlockDeviceMappings": [{"Ebs": {"SnapshotId": "snap-XYZ"}}]}),
        ] {
            assert!(
                serde_json::from_value::<Regions>(json!({
                    "home-1": {"quota": 10, "images": {"Images": [foreign]},
                               "snapshots": {"Snapshots": []}}
                }))
                .is_err()
            );
        }
        for id in ["snap-", "snap-A", "snap-xyz", "ami-01"] {
            assert!(id.parse::<super::SnapshotId>().is_err(), "{id}");
        }
        assert!("snap-01abcdef".parse::<super::SnapshotId>().is_ok());
    }
    #[test]
    fn retirement_boundary_and_protection() {
        for (protection, expected) in [
            ("enabled-with-cooldown", Some("unprotect")),
            ("disabled-until 2025-10-05T00:00:00Z", None),
            ("disabled", Some("deregister")),
        ] {
            let mut o = old();
            o["DeprecationTime"] = json!("2025-07-04T00:00:00Z");
            o["DeregistrationProtection"] = json!(protection);
            let mut home = current();
            home.push(o.clone());
            let mut other = current();
            other.push(o);
            let actual = lines(&publication(&home, &other, true, 1_759_536_000, 10));
            let expected: Vec<_> = match protection {
                "enabled-with-cooldown" => vec![
                    "unprotect\thome-1\tami-03".to_string(),
                    "unprotect\tother-1\tami-03".to_string(),
                ],
                _ => expected
                    .into_iter()
                    .map(|op| format!("{op}\tother-1\tami-03"))
                    .collect(),
            };
            assert_eq!(actual, expected);
        }
    }
    #[test]
    fn latest_is_never_retired_and_unconverged_successor_does_not_supersede() {
        let mut home = current();
        home[0]["DeprecationTime"] = json!("2020-01-01T00:00:00Z");
        let mut other = home.clone();
        assert!(lines(&publication(&home, &other, true, 1_800_000_000, 10)).is_empty());
        home.push(old());
        other.push(old());
        other[1]["Public"] = json!(false);
        assert_eq!(
            lines(&publication(&home, &other, true, 1_800_000_000, 10)),
            vec!["publish\tother-1\tami-02"]
        );
    }
    #[test]
    fn quota_conflict_and_foreign_images_are_errors() {
        let err = publication(&[], &[], true, 0, 1).steps().unwrap_err();
        assert!(matches!(err, DeployError::QuotaExceeded { .. }));
        assert!(err.to_string().contains("home-1"));
        let mut conflicting = current();
        conflicting[0]["Tags"][1]["Value"] = json!(OLD);
        assert!(matches!(
            publication(&conflicting, &[], true, 0, 10).steps(),
            Err(DeployError::ImageConflict { .. })
        ));
        let mut foreign = current();
        foreign[0]["Tags"] = json!([]);
        assert!(matches!(
            publication(&foreign, &[], true, 0, 10).steps(),
            Err(DeployError::ForeignImage { .. })
        ));
    }
    #[test]
    fn backfill_copies_retained_not_retiring() {
        let mut oldest = image("ami-04", "aarch64", D, "v0", "2024-06-01T00:00:00Z");
        oldest["DeprecationTime"] = json!("2024-01-01T00:00:00Z");
        let mut home = current();
        home.push(old());
        home.push(oldest.clone());
        let mut other = current();
        other.push(oldest);
        let result = lines(&publication(&home, &other, false, 1_800_000_000, 10));
        assert!(
            result
                .iter()
                .any(|s| s.starts_with("copy\tother-1\tami-03\t"))
        );
        assert!(
            !result
                .iter()
                .any(|s| s.starts_with("copy\tother-1\tami-04\t"))
        );
    }
    #[test]
    fn manifest_is_exact_and_missing_region_is_reported() {
        let p = publication(&current(), &current(), true, 0, 10);
        assert_eq!(
            serde_json::to_string_pretty(&p.manifest().unwrap()).unwrap(),
            format!(
                "{{\n  \"aarch64\": {{\n    \"sha256\": \"{A}\",\n    \"regions\": {{\n      \"home-1\": \"ami-02\",\n      \"other-1\": \"ami-02\"\n    }}\n  }},\n  \"x86_64\": {{\n    \"sha256\": \"{X}\",\n    \"regions\": {{\n      \"home-1\": \"ami-01\",\n      \"other-1\": \"ami-01\"\n    }}\n  }}\n}}"
            )
        );
        let err = publication(&current(), &[], true, 0, 10)
            .manifest()
            .unwrap_err();
        assert!(matches!(err, DeployError::NotConverged { .. }));
        assert!(err.to_string().contains("other-1"));
    }

    fn third_release() -> Vec<Value> {
        vec![
            image(
                "ami-05",
                "x86_64",
                "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                "v3",
                "2026-01-01T00:00:00Z",
            ),
            image(
                "ami-06",
                "aarch64",
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                "v3",
                "2026-01-01T00:00:00Z",
            ),
        ]
    }

    #[test]
    fn abandoned_release_does_not_block_next_converged_successor() {
        let mut home = third_release();
        home.push(current()[0].clone());
        home.push(old());
        let mut other = third_release();
        other.push(old());
        let result = lines(&publication(&home, &other, false, 0, 20));
        assert!(result.contains(&"deprecate\thome-1\tami-03\t2026-07-04T00:00:00Z".to_string()));
        assert!(result.contains(&"deprecate\tother-1\tami-03\t2026-07-04T00:00:00Z".to_string()));
    }

    #[test]
    fn single_architecture_does_not_supersede() {
        let mut home = current();
        home.push(third_release()[0].clone());
        let other = home.clone();
        let result = lines(&publication(&home, &other, false, 0, 20));
        assert!(!result.iter().any(|line| line.starts_with("deprecate\t")));
    }

    #[test]
    fn home_creation_dates_order_backfilled_releases() {
        let mut home = current();
        home.push(old());
        let mut other = current();
        let mut copied = old();
        copied["CreationDate"] = json!("2027-01-01T00:00:00Z");
        other.push(copied);
        let result = lines(&publication(&home, &other, false, 0, 20));
        assert!(result.contains(&"deprecate\thome-1\tami-03\t2025-07-04T00:00:00Z".to_string()));
        assert!(result.contains(&"deprecate\tother-1\tami-03\t2025-07-04T00:00:00Z".to_string()));
    }

    #[test]
    fn home_retirement_waits_for_remote_copy() {
        let mut retiring = old();
        retiring["DeprecationTime"] = json!("2025-07-04T00:00:00Z");
        retiring["DeregistrationProtection"] = json!("disabled");
        let mut home = current();
        home.push(retiring.clone());
        let mut other = current();
        other.push(retiring);
        let result = lines(&publication(&home, &other, false, 1_759_536_000, 20));
        assert_eq!(result, vec!["deregister\tother-1\tami-03"]);
        let result = lines(&publication(&home, &current(), false, 1_759_536_000, 20));
        assert_eq!(result, vec!["deregister\thome-1\tami-03"]);
        home[2]["DeregistrationProtection"] = json!("enabled-with-cooldown");
        other[2]["DeregistrationProtection"] = json!("enabled-with-cooldown");
        let result = lines(&publication(&home, &other, false, 1_759_536_000, 20));
        assert_eq!(
            result,
            vec!["unprotect\thome-1\tami-03", "unprotect\tother-1\tami-03"]
        );
    }

    #[test]
    fn backfill_copies_are_in_quota_preflight() {
        let err = publication(&current(), &[], false, 0, 1)
            .steps()
            .unwrap_err();
        assert!(
            matches!(err, DeployError::QuotaExceeded { region, public: 2, new: 0, quota: 1 } if region == "home-1")
        );
        let regions: Regions = serde_json::from_value(json!({
            "home-1": {"quota": 10, "images": {"Images": current()}, "snapshots":{"Snapshots":[]}},
            "other-1": {"quota": 1, "images": {"Images": []}, "snapshots":{"Snapshots":[]}}
        }))
        .unwrap();
        let p = Publication::new(
            "home-1".parse().unwrap(),
            regions,
            None,
            UnixTime::from_seconds(0),
        )
        .unwrap();
        assert!(
            matches!(p.steps(), Err(DeployError::QuotaExceeded { region, public: 0, new: 2, quota: 1 }) if region == "other-1")
        );
    }

    #[test]
    fn existing_private_images_count_against_quota() {
        let mut home = current();
        home[0]["Public"] = json!(false);
        home[1]["Public"] = json!(false);
        assert!(
            matches!(publication(&home, &[], true, 0, 1).steps(), Err(DeployError::QuotaExceeded { region, public: 0, new: 2, quota: 1 }) if region == "home-1")
        );
    }

    #[test]
    fn public_foreign_images_count_against_quota() {
        let unrelated = json!({"Public": true, "State": "failed"});
        let err = publication(&[unrelated], &[], true, 0, 2)
            .steps()
            .unwrap_err();
        assert!(
            matches!(err, DeployError::QuotaExceeded { region, public: 1, new: 2, quota: 2 } if region == "home-1")
        );
    }

    #[test]
    fn failed_gang_image_is_an_error() {
        let mut failed = old();
        failed["State"] = json!("failed");
        assert!(
            matches!(publication(&[failed], &[], false, 0, 20).steps(), Err(DeployError::FailedImage { region, state, .. }) if region == "home-1" && state == "failed")
        );
    }

    #[test]
    fn same_release_and_architecture_with_different_digests_conflicts() {
        let mut other = current();
        other[0] = image("ami-04", "x86_64", OLD, "v2", "2025-01-01T00:00:00Z");
        assert!(matches!(
            publication(&current(), &other, false, 0, 20).steps(),
            Err(DeployError::ImageConflict { .. })
        ));
    }

    #[test]
    fn quota_accepts_integral_floats_and_rejects_invalid_numbers() {
        for quota in ["100", "100.0", "0.0", "4294967295.0"] {
            let json = format!(
                r#"{{"home-1":{{"quota":{quota},"images":{{"Images":[]}},"snapshots":{{"Snapshots":[]}}}}}}"#
            );
            assert!(serde_json::from_str::<Regions>(&json).is_ok(), "{quota}");
        }
        for quota in ["100.5", "-1", "4294967296.0", "1e400"] {
            let json = format!(
                r#"{{"home-1":{{"quota":{quota},"images":{{"Images":[]}},"snapshots":{{"Snapshots":[]}}}}}}"#
            );
            assert!(serde_json::from_str::<Regions>(&json).is_err(), "{quota}");
        }
    }

    #[test]
    fn deprecation_rounds_up_and_accepts_aws_feedback() {
        let mut home = current();
        for image in &mut home {
            image["CreationDate"] = json!("2025-01-01T00:00:30Z");
        }
        home.push(old());
        let mut other = home.clone();
        let expected = "2025-07-04T00:01:00Z";
        let result = lines(&publication(&home, &other, false, 0, 10));
        assert!(result.contains(&format!("deprecate\thome-1\tami-03\t{expected}")));
        home[2]["DeprecationTime"] = json!(expected);
        other[2]["DeprecationTime"] = json!(expected);
        assert!(lines(&publication(&home, &other, false, 60, 10)).is_empty());
    }

    #[test]
    fn past_deprecation_is_scheduled_in_future_and_then_converges() {
        let mut home = current();
        home.push(old());
        let mut other = home.clone();
        let now = 1_800_000_031;
        let rounded = (now + 60 + 59) / 60 * 60;
        let expected = crate::Timestamp::from_unix(UnixTime::from_seconds(rounded))
            .unwrap()
            .to_string();
        let result = lines(&publication(&home, &other, false, now, 10));
        assert!(result.contains(&format!("deprecate\thome-1\tami-03\t{expected}")));
        home[2]["DeprecationTime"] = json!(expected);
        other[2]["DeprecationTime"] = json!(expected);
        assert!(lines(&publication(&home, &other, false, now + 120, 10)).is_empty());
    }

    #[test]
    fn empty_new_region_does_not_resurrect_expired_release() {
        let mut expired = old();
        expired["DeprecationTime"] = json!("2025-07-04T00:00:00Z");
        let mut home = current();
        home.push(expired.clone());
        expired["DeregistrationProtection"] = json!("disabled");
        let mut other = current();
        other.push(expired);
        let regions: Regions = serde_json::from_value(json!({
            "home-1": {"quota": 10, "images": {"Images": home}, "snapshots":{"Snapshots":[]}},
            "other-1": {"quota": 10, "images": {"Images": other}, "snapshots":{"Snapshots":[]}},
            "new-1": {"quota": 2, "images": {"Images": []}, "snapshots":{"Snapshots":[]}}
        }))
        .unwrap();
        let p = Publication::new(
            "home-1".parse().unwrap(),
            regions,
            None,
            UnixTime::from_seconds(1_759_536_000),
        )
        .unwrap();
        let result = lines(&p);
        assert!(result.contains(&"unprotect\thome-1\tami-03".to_string()));
        assert!(result.contains(&"deregister\tother-1\tami-03".to_string()));
        assert_eq!(
            result
                .iter()
                .filter(|line| line.starts_with("copy\tnew-1\t"))
                .count(),
            2
        );
        assert!(
            !result
                .iter()
                .any(|line| line.starts_with("copy\tnew-1\tami-03\t")
                    || line.starts_with("protect\t")
                    || line.starts_with("publish\t"))
        );
    }

    #[test]
    fn retirement_waits_for_ready_successor_in_each_existing_region() {
        let mut expired = old();
        expired["DeprecationTime"] = json!("2024-07-01T00:00:00Z");
        let mut home = current();
        for image in &mut home {
            image["Public"] = json!(false);
        }
        home.push(expired.clone());
        let mut other = vec![expired];
        let assert_retained = |home: &[Value], other: &[Value]| {
            let result = lines(&publication(home, other, false, 1_800_000_000, 20));
            assert!(!result.iter().any(|line| line.starts_with("unprotect\t") || line.starts_with("deregister\t")));
        };
        assert_retained(&home, &other);
        home[0]["Public"] = json!(true);
        home[1]["Public"] = json!(true);
        assert_retained(&home, &other);
        other.extend(current());
        other[1]["DeregistrationProtection"] = json!("disabled");
        assert_retained(&home, &other);
        other[1]["DeregistrationProtection"] = json!("enabled-with-cooldown");
        let result = lines(&publication(&home, &other, false, 1_800_000_000, 20));
        assert!(result.contains(&"unprotect\thome-1\tami-03".to_string()));
        assert!(result.contains(&"unprotect\tother-1\tami-03".to_string()));
    }
}
