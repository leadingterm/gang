//! Verifying the stored plan object before apply: one version, the manifest's version and
//! metadata, fresh, unexpired, and the downloaded bytes match the manifest's digest.

use std::collections::BTreeMap;
use std::str::FromStr;

use serde::Deserialize;

use crate::errors::{DeployError, Document, Result};
use crate::manifest::Manifest;
use crate::sha::PlanSha256;
use crate::timestamp::{MaxAge, UnixTime, object_age};

/// The versions below a key prefix, as `aws s3api list-object-versions --output json` prints them.
#[derive(Debug, Clone, Deserialize)]
pub struct ObjectVersions {
    /// The object versions.
    #[serde(rename = "Versions", default)]
    versions: Option<Vec<ObjectVersion>>,
    /// The delete markers.
    #[serde(rename = "DeleteMarkers", default)]
    delete_markers: Option<Vec<ObjectVersion>>,
}

/// One version or delete marker of an object.
#[derive(Debug, Clone, Deserialize)]
struct ObjectVersion {
    /// The object key.
    #[serde(rename = "Key")]
    key: String,
    /// The version id.
    #[serde(rename = "VersionId")]
    version_id: String,
}

impl FromStr for ObjectVersions {
    type Err = DeployError;

    fn from_str(text: &str) -> Result<Self> {
        serde_json::from_str(text).map_err(|source| DeployError::Json {
            document: Document::ObjectVersions,
            source,
        })
    }
}

/// The head of an object version, as `aws s3api head-object --output json` prints it.
#[derive(Debug, Clone, Deserialize)]
pub struct ObjectHead {
    /// When the object was last modified, as an RFC 3339 time.
    #[serde(rename = "LastModified", default)]
    last_modified: Option<String>,
    /// The user metadata.
    #[serde(rename = "Metadata", default)]
    metadata: Option<BTreeMap<String, String>>,
}

impl FromStr for ObjectHead {
    type Err = DeployError;

    fn from_str(text: &str) -> Result<Self> {
        serde_json::from_str(text).map_err(|source| DeployError::Json {
            document: Document::ObjectHead,
            source,
        })
    }
}

/// The entries of `list` whose key is `key`.
fn entries_of<'a>(
    list: Option<&'a [ObjectVersion]>,
    key: &'a str,
) -> impl Iterator<Item = &'a ObjectVersion> {
    list.unwrap_or_default()
        .iter()
        .filter(move |entry| entry.key == key)
}

impl Manifest {
    /// Checks that the plan object has exactly one version, no delete marker, and that version is
    /// the one the manifest records. Versions of other keys sharing the prefix are ignored.
    ///
    /// # Errors
    ///
    /// The first of these checks that fails.
    pub fn check_versions(&self, versions: &ObjectVersions) -> Result<()> {
        let key = self.plan_key.as_str();
        let matching: Vec<&ObjectVersion> = entries_of(versions.versions.as_deref(), key).collect();
        let markers = entries_of(versions.delete_markers.as_deref(), key).count();
        let [version] = matching.as_slice() else {
            return Err(DeployError::ObjectVersions {
                key: self.plan_key.clone(),
                count: matching.len(),
            });
        };
        if markers != 0 {
            return Err(DeployError::ObjectDeleteMarker {
                key: self.plan_key.clone(),
            });
        }
        if version.version_id != self.plan_version.as_str() {
            return Err(DeployError::ObjectVersion);
        }
        Ok(())
    }

    /// Checks that the plan object carries the manifest's metadata, was modified no later than
    /// `now` and no earlier than `max_age` before it, and that the plan has not expired at `now`.
    ///
    /// # Errors
    ///
    /// The first of these checks that fails.
    pub fn check_object(&self, head: &ObjectHead, now: UnixTime, max_age: MaxAge) -> Result<()> {
        for &(name, ref expected) in self.facts.object_metadata().pairs() {
            let found = head
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get(name))
                .map_or("", String::as_str);
            if found != expected.as_str() {
                return Err(DeployError::ObjectMetadata {
                    name,
                    expected: expected.clone(),
                    found: found.to_owned(),
                });
            }
        }
        let last_modified = head
            .last_modified
            .as_deref()
            .ok_or(DeployError::ObjectLastModified)?;
        let age = object_age(last_modified, now)?;
        if age < 0 {
            return Err(DeployError::ObjectFuture);
        }
        if age > max_age.as_secs() {
            return Err(DeployError::ObjectAge { age, max_age });
        }
        if now >= self.facts.expires_at.unix() {
            return Err(DeployError::Expired {
                expires_at: self.facts.expires_at,
            });
        }
        Ok(())
    }

    /// Checks that the downloaded plan has the digest the manifest records.
    ///
    /// # Errors
    ///
    /// [`DeployError::Digest`] when the digests differ.
    pub fn check_digest(&self, found: &PlanSha256) -> Result<()> {
        if *found == self.facts.plan_sha256 {
            Ok(())
        } else {
            Err(DeployError::Digest {
                expected: self.facts.plan_sha256.clone(),
                found: found.clone(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{ObjectHead, ObjectVersions};
    use crate::manifest::tests::{BASE, DIGEST, HEAD, manifest};
    use crate::sha::PlanSha256;
    use crate::timestamp::UnixTime;

    /// 2026-01-01T12:00:00Z.
    const NOW: UnixTime = UnixTime::from_seconds(1_767_268_800);
    const DAY: &str = "86400";

    fn key() -> String {
        manifest().plan_key.to_string()
    }

    fn versions(json: &Value) -> ObjectVersions {
        json.to_string().parse().unwrap()
    }

    fn version(key: &str, id: &str) -> Value {
        json!({"Key": key, "VersionId": id})
    }

    fn versions_rejection(json: &Value) -> Option<String> {
        manifest()
            .check_versions(&versions(json))
            .err()
            .as_ref()
            .map(ToString::to_string)
    }

    /// The head-object JSON of the manifest's plan, modified at `last_modified`, with `change`
    /// applied to its metadata.
    fn head(
        last_modified: &str,
        change: impl FnOnce(&mut serde_json::Map<String, Value>),
    ) -> ObjectHead {
        let mut metadata = serde_json::Map::new();
        for (name, value) in manifest().facts.object_metadata().pairs() {
            metadata.insert((*name).to_owned(), Value::from(value.as_str()));
        }
        change(&mut metadata);
        json!({"LastModified": last_modified, "Metadata": metadata, "ContentLength": 10})
            .to_string()
            .parse()
            .unwrap()
    }

    fn object_rejection(head: &ObjectHead, now: UnixTime) -> Option<String> {
        manifest()
            .check_object(head, now, DAY.parse().expect("positive max age"))
            .err()
            .as_ref()
            .map(ToString::to_string)
    }

    #[test]
    fn check_versions_passes_with_one_matching_version() {
        let with_markers = json!({"Versions": [version(&key(), "ver-1")], "DeleteMarkers": []});
        assert_eq!(versions_rejection(&with_markers), None);
        assert_eq!(
            versions_rejection(&json!({"Versions": [version(&key(), "ver-1")]})),
            None
        );
    }

    #[test]
    fn check_versions_rejects_no_versions() {
        assert_eq!(
            versions_rejection(&json!({})),
            Some(format!(
                "plan object {} has 0 versions; exactly one is allowed",
                key()
            ))
        );
    }

    #[test]
    fn check_versions_rejects_two_versions() {
        let json = json!({"Versions": [version(&key(), "ver-1"), version(&key(), "ver-0")]});
        assert_eq!(
            versions_rejection(&json),
            Some(format!(
                "plan object {} has 2 versions; exactly one is allowed",
                key()
            ))
        );
    }

    #[test]
    fn check_versions_rejects_a_different_version_id() {
        assert_eq!(
            versions_rejection(&json!({"Versions": [version(&key(), "ver-2")]})),
            Some("plan object version differs from the manifest".to_owned())
        );
    }

    #[test]
    fn check_versions_rejects_a_delete_marker() {
        let json = json!({
            "Versions": [version(&key(), "ver-1")],
            "DeleteMarkers": [version(&key(), "dm-1")],
        });
        assert_eq!(
            versions_rejection(&json),
            Some(format!("plan object {} has a delete marker", key()))
        );
    }

    #[test]
    fn check_versions_ignores_versions_of_other_keys_sharing_the_prefix() {
        let other = format!("{}2", key());
        let json = json!({
            "Versions": [version(&key(), "ver-1"), version(&other, "ver-2")],
            "DeleteMarkers": [version(&other, "dm-2")],
        });
        assert_eq!(versions_rejection(&json), None);
    }

    #[test]
    fn check_object_passes_on_matching_metadata_and_a_fresh_object() {
        assert_eq!(
            object_rejection(&head("2026-01-01T11:00:00+00:00", |_| {}), NOW),
            None
        );
    }

    #[test]
    fn check_object_rejects_a_differing_head_sha() {
        let head = head("2026-01-01T11:00:00+00:00", |metadata| {
            metadata.insert("head-sha".to_owned(), Value::from(BASE));
        });
        assert_eq!(
            object_rejection(&head, NOW),
            Some(format!(
                "plan object metadata head-sha mismatch (expected {HEAD}, found {BASE})"
            ))
        );
    }

    #[test]
    fn check_object_rejects_a_differing_plan_sha256() {
        let other = "6".repeat(64);
        let head = head("2026-01-01T11:00:00+00:00", |metadata| {
            metadata.insert("plan-sha256".to_owned(), Value::from(other.as_str()));
        });
        assert_eq!(
            object_rejection(&head, NOW),
            Some(format!(
                "plan object metadata plan-sha256 mismatch (expected {DIGEST}, found {other})"
            ))
        );
    }

    #[test]
    fn check_object_rejects_missing_metadata() {
        let head = head("2026-01-01T11:00:00+00:00", |metadata| {
            metadata.remove("expires-at");
        });
        assert_eq!(
            object_rejection(&head, NOW),
            Some(
                "plan object metadata expires-at mismatch (expected 2026-01-02T00:00:00Z, found )"
                    .to_owned()
            )
        );
    }

    #[test]
    fn check_object_rejects_an_object_older_than_the_maximum_age() {
        assert_eq!(
            object_rejection(&head("2025-12-31T11:00:00+00:00", |_| {}), NOW),
            Some("plan object age 90000 exceeds 86400 seconds".to_owned())
        );
    }

    #[test]
    fn check_object_accepts_an_object_exactly_at_the_maximum_age() {
        assert_eq!(
            object_rejection(&head("2025-12-31T12:00:00+00:00", |_| {}), NOW),
            None
        );
    }

    #[test]
    fn check_object_rejects_an_object_modified_an_hour_in_the_future() {
        assert_eq!(
            object_rejection(&head("2026-01-01T13:00:00+00:00", |_| {}), NOW),
            Some("plan object was modified in the future".to_owned())
        );
    }

    #[test]
    fn check_object_rejects_an_object_without_a_usable_last_modified() {
        let head: ObjectHead = json!({"Metadata": {}}).to_string().parse().unwrap();
        let error = manifest()
            .check_object(&head, NOW, DAY.parse().expect("positive max age"))
            .unwrap_err();
        assert!(error.to_string().starts_with("plan object metadata"));
        let mut metadata = serde_json::Map::new();
        for (name, value) in manifest().facts.object_metadata().pairs() {
            metadata.insert((*name).to_owned(), Value::from(value.as_str()));
        }
        for last_modified in [Value::Null, Value::from("yesterday")] {
            let head: ObjectHead = json!({"LastModified": last_modified, "Metadata": metadata})
                .to_string()
                .parse()
                .unwrap();
            assert_eq!(
                object_rejection(&head, NOW),
                Some("plan object has no usable LastModified".to_owned())
            );
        }
    }

    #[test]
    fn check_object_rejects_a_plan_past_expires_at() {
        // 2026-01-02T00:00:01Z, one second after the manifest's expiry.
        let late = UnixTime::from_seconds(1_767_312_001);
        assert_eq!(
            object_rejection(&head("2026-01-01T11:00:00+00:00", |_| {}), late),
            Some("plan expired at 2026-01-02T00:00:00Z".to_owned())
        );
    }

    #[test]
    fn check_object_rejects_a_plan_at_expires_at() {
        let at_expiry = UnixTime::from_seconds(1_767_312_000);
        assert_eq!(
            object_rejection(&head("2026-01-01T11:00:00+00:00", |_| {}), at_expiry),
            Some("plan expired at 2026-01-02T00:00:00Z".to_owned())
        );
    }

    #[test]
    fn check_digest_passes_when_the_digest_matches() {
        let digest: PlanSha256 = DIGEST.parse().unwrap();
        assert!(manifest().check_digest(&digest).is_ok());
    }

    #[test]
    fn check_digest_rejects_a_differing_digest() {
        let found = PlanSha256::digest(b"plan-bytes");
        let error = manifest().check_digest(&found).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!("plan sha256 mismatch (expected {DIGEST}, found {found})")
        );
    }
}
