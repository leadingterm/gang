//! The plan manifest: what a reviewed plan is, where it is stored, and how the plan comment
//! carries it from the pull request to the merge.

use std::fmt;
use std::str::FromStr;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::errors::{DeployError, Document, Result};
use crate::ids::{PrNumber, Repository, RepositoryId, RunAttempt, RunId};
use crate::key::{PlanKey, PlanPrefix, PlanVersion};
use crate::sha::{PlanSha256, Sha};
use crate::timestamp::Timestamp;

/// The manifest schema version this crate writes and reads.
pub const MANIFEST_SCHEMA_VERSION: u64 = 1;
/// Start of the hidden comment line that carries the manifest.
pub const MANIFEST_LINE_PREFIX: &str = "<!-- gang-deploy-manifest:";
/// End of the hidden comment line that carries the manifest.
const MANIFEST_LINE_SUFFIX: &str = " -->";
/// Every key of a manifest, sorted.
const MANIFEST_KEYS: [&str; 16] = [
    "base_sha",
    "created_at",
    "expires_at",
    "has_changes",
    "head_sha",
    "merge_sha",
    "plan_key",
    "plan_sha256",
    "plan_version",
    "pr_number",
    "repository",
    "repository_id",
    "schema_version",
    "tree_sha",
    "workflow_run_attempt",
    "workflow_run_id",
];

/// What a reviewed plan is: the merge it was planned for, the run that planned it, its digest,
/// and how long it stays usable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanFacts {
    /// The repository, as `owner/name`.
    pub repository: Repository,
    /// The repository's numeric id.
    pub repository_id: RepositoryId,
    /// The pull request the plan was made for.
    pub pr_number: PrNumber,
    /// The head of the pull request.
    pub head_sha: Sha,
    /// The base the pull request merges into: the first parent of the planned merge.
    pub base_sha: Sha,
    /// The planned merge commit.
    pub merge_sha: Sha,
    /// The tree of the planned merge commit.
    pub tree_sha: Sha,
    /// The workflow run that made the plan.
    pub workflow_run_id: RunId,
    /// The attempt of that run.
    pub workflow_run_attempt: RunAttempt,
    /// The SHA-256 of the saved plan file.
    pub plan_sha256: PlanSha256,
    /// Whether applying the plan changes anything.
    pub has_changes: bool,
    /// When the plan was stored.
    pub created_at: Timestamp,
    /// When the plan stops being usable.
    pub expires_at: Timestamp,
}

impl PlanFacts {
    /// The object key of the plan below `prefix`.
    #[must_use]
    pub fn object_key(&self, prefix: &PlanPrefix) -> PlanKey {
        PlanKey::new(
            prefix,
            self.repository_id,
            self.pr_number,
            &self.head_sha,
            &self.tree_sha,
            self.workflow_run_id,
            self.workflow_run_attempt,
        )
    }

    /// The S3 user metadata the plan object carries.
    #[must_use]
    pub fn object_metadata(&self) -> ObjectMetadata {
        ObjectMetadata([
            ("repository-id", self.repository_id.to_string()),
            ("pr-number", self.pr_number.to_string()),
            ("head-sha", self.head_sha.to_string()),
            ("base-sha", self.base_sha.to_string()),
            ("merge-sha", self.merge_sha.to_string()),
            ("tree-sha", self.tree_sha.to_string()),
            ("workflow-run-id", self.workflow_run_id.to_string()),
            (
                "workflow-run-attempt",
                self.workflow_run_attempt.to_string(),
            ),
            ("plan-sha256", self.plan_sha256.to_string()),
            ("has-changes", self.has_changes.to_string()),
            ("expires-at", self.expires_at.to_string()),
        ])
    }
}

/// The S3 user metadata of a plan object, in the order the plan store writes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadata([(&'static str, String); 11]);

impl ObjectMetadata {
    /// The metadata names and values.
    #[must_use]
    pub fn pairs(&self) -> &[(&'static str, String)] {
        &self.0
    }
}

impl fmt::Display for ObjectMetadata {
    /// `name=value` pairs joined by `,`, as `aws s3api put-object --metadata` takes them.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, (name, value)) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str(",")?;
            }
            write!(f, "{name}={value}")?;
        }
        Ok(())
    }
}

/// A reviewed plan and where it is stored. It prints as compact JSON with sorted keys and
/// `schema_version` 1; it parses from that JSON, accepting exactly those keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    /// What the plan is.
    pub facts: PlanFacts,
    /// The object key of the stored plan.
    pub plan_key: PlanKey,
    /// The object version of the stored plan.
    pub plan_version: PlanVersion,
}

/// The JSON form of a manifest; serde writes the fields in this sorted order.
#[derive(Serialize)]
struct ManifestJson<'a> {
    base_sha: &'a Sha,
    created_at: Timestamp,
    expires_at: Timestamp,
    has_changes: bool,
    head_sha: &'a Sha,
    merge_sha: &'a Sha,
    plan_key: &'a PlanKey,
    plan_sha256: &'a PlanSha256,
    plan_version: &'a PlanVersion,
    pr_number: PrNumber,
    repository: &'a Repository,
    repository_id: RepositoryId,
    schema_version: u64,
    tree_sha: &'a Sha,
    workflow_run_attempt: RunAttempt,
    workflow_run_id: RunId,
}

impl Manifest {
    /// The hidden comment line that carries the manifest:
    /// `<!-- gang-deploy-manifest:<base64 of the JSON> -->`.
    #[must_use]
    pub fn comment_line(&self) -> String {
        format!(
            "{MANIFEST_LINE_PREFIX}{}{MANIFEST_LINE_SUFFIX}",
            STANDARD.encode(self.to_string())
        )
    }

    /// The manifest carried by the one manifest line of a comment `body`.
    ///
    /// # Errors
    ///
    /// When `body` has no or several manifest lines, or the line does not carry a manifest.
    pub fn from_comment(body: &str) -> Result<Self> {
        let lines: Vec<&str> = body
            .split('\n')
            .filter(|line| line.starts_with(MANIFEST_LINE_PREFIX))
            .collect();
        let [line] = lines.as_slice() else {
            return Err(DeployError::ManifestLines { found: lines.len() });
        };
        if !line.ends_with(MANIFEST_LINE_SUFFIX) {
            return Err(DeployError::ManifestLineEnd);
        }
        let payload = line
            .strip_prefix(MANIFEST_LINE_PREFIX)
            .and_then(|rest| rest.strip_suffix(MANIFEST_LINE_SUFFIX))
            .ok_or(DeployError::ManifestLineEnd)?;
        let base64_alphabet =
            |byte: u8| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'=');
        if payload.is_empty() || !payload.bytes().all(base64_alphabet) {
            return Err(DeployError::ManifestNotBase64);
        }
        let json = STANDARD
            .decode(payload)
            .map_err(DeployError::ManifestDecode)?;
        let value: Value =
            serde_json::from_slice(&json).map_err(|_| DeployError::ManifestNotObject)?;
        Self::from_value(value)
    }

    /// Reads a manifest from its JSON value: an object with schema version 1 and exactly the
    /// manifest keys, each with a well-formed value.
    fn from_value(value: Value) -> Result<Self> {
        let Value::Object(mut object) = value else {
            return Err(DeployError::ManifestNotObject);
        };
        if object.get("schema_version").and_then(Value::as_u64) != Some(MANIFEST_SCHEMA_VERSION) {
            return Err(DeployError::ManifestSchema);
        }
        if object.len() != MANIFEST_KEYS.len()
            || !MANIFEST_KEYS.iter().all(|key| object.contains_key(*key))
        {
            return Err(DeployError::ManifestKeys);
        }
        let map = &mut object;
        Ok(Self {
            facts: PlanFacts {
                repository: take(map, "repository")?,
                repository_id: take(map, "repository_id")?,
                pr_number: take(map, "pr_number")?,
                head_sha: take(map, "head_sha")?,
                base_sha: take(map, "base_sha")?,
                merge_sha: take(map, "merge_sha")?,
                tree_sha: take(map, "tree_sha")?,
                workflow_run_id: take(map, "workflow_run_id")?,
                workflow_run_attempt: take(map, "workflow_run_attempt")?,
                plan_sha256: take(map, "plan_sha256")?,
                has_changes: take(map, "has_changes")?,
                created_at: take(map, "created_at")?,
                expires_at: take(map, "expires_at")?,
            },
            plan_key: take(map, "plan_key")?,
            plan_version: take(map, "plan_version")?,
        })
    }
}

/// Takes the value of `key` out of a manifest object and reads it as `T`.
fn take<T: DeserializeOwned>(object: &mut Map<String, Value>, key: &'static str) -> Result<T> {
    let value = object.remove(key).unwrap_or(Value::Null);
    serde_json::from_value(value).map_err(|source| DeployError::ManifestValue { key, source })
}

impl fmt::Display for Manifest {
    /// The compact JSON form with sorted keys.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let facts = &self.facts;
        let json = ManifestJson {
            base_sha: &facts.base_sha,
            created_at: facts.created_at,
            expires_at: facts.expires_at,
            has_changes: facts.has_changes,
            head_sha: &facts.head_sha,
            merge_sha: &facts.merge_sha,
            plan_key: &self.plan_key,
            plan_sha256: &facts.plan_sha256,
            plan_version: &self.plan_version,
            pr_number: facts.pr_number,
            repository: &facts.repository,
            repository_id: facts.repository_id,
            schema_version: MANIFEST_SCHEMA_VERSION,
            tree_sha: &facts.tree_sha,
            workflow_run_attempt: facts.workflow_run_attempt,
            workflow_run_id: facts.workflow_run_id,
        };
        let text = serde_json::to_string(&json).map_err(|_| fmt::Error)?;
        f.write_str(&text)
    }
}

impl FromStr for Manifest {
    type Err = DeployError;

    fn from_str(text: &str) -> Result<Self> {
        let value = serde_json::from_str(text).map_err(|source| DeployError::Json {
            document: Document::Manifest,
            source,
        })?;
        Self::from_value(value)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use serde_json::{Value, json};

    use super::{MANIFEST_LINE_PREFIX, Manifest};

    pub(crate) const HEAD: &str = "1111111111111111111111111111111111111111";
    pub(crate) const BASE: &str = "2222222222222222222222222222222222222222";
    pub(crate) const MERGE: &str = "3333333333333333333333333333333333333333";
    pub(crate) const TREE: &str = "4444444444444444444444444444444444444444";
    pub(crate) const DIGEST: &str =
        "5555555555555555555555555555555555555555555555555555555555555555";

    /// The JSON of a valid manifest for pull request 7 of `example-org/gang`, run 99 attempt 2.
    pub(crate) fn manifest_json() -> Value {
        json!({
            "repository": "example-org/gang",
            "repository_id": 42,
            "pr_number": 7,
            "head_sha": HEAD,
            "base_sha": BASE,
            "merge_sha": MERGE,
            "tree_sha": TREE,
            "workflow_run_id": 99,
            "workflow_run_attempt": 2,
            "plan_key": format!("plans/42/7/{HEAD}/{TREE}/99-2.tfplan"),
            "plan_version": "ver-1",
            "plan_sha256": DIGEST,
            "has_changes": true,
            "created_at": "2026-01-01T00:00:00Z",
            "expires_at": "2026-01-02T00:00:00Z",
            "schema_version": 1,
        })
    }

    /// The valid manifest of [`manifest_json`].
    pub(crate) fn manifest() -> Manifest {
        manifest_json().to_string().parse().unwrap()
    }

    /// The JSON with `key` set to `value`.
    fn with(key: &str, value: Value) -> Value {
        let mut json = manifest_json();
        json[key] = value;
        json
    }

    /// A comment line carrying `payload` as the manifest.
    fn line_for(payload: &str) -> String {
        format!("{MANIFEST_LINE_PREFIX}{} -->", STANDARD.encode(payload))
    }

    fn rejection(json: &Value) -> String {
        json.to_string()
            .parse::<Manifest>()
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn manifest_keeps_numbers_as_json_numbers_and_has_changes_as_a_boolean() {
        let printed: Value = serde_json::from_str(&manifest().to_string()).unwrap();
        let fields: Vec<&Value> = [
            "repository_id",
            "pr_number",
            "workflow_run_id",
            "workflow_run_attempt",
            "has_changes",
            "schema_version",
        ]
        .iter()
        .map(|key| &printed[key])
        .collect();
        assert_eq!(
            fields,
            [
                &json!(42),
                &json!(7),
                &json!(99),
                &json!(2),
                &json!(true),
                &json!(1)
            ]
        );
    }

    /// What `jq -cnS ... '$ARGS.named'` printed for [`manifest_json`] in the shell plan store.
    fn jq_sorted_json() -> String {
        format!(
            "{{\"base_sha\":\"{BASE}\",\"created_at\":\"2026-01-01T00:00:00Z\",\
             \"expires_at\":\"2026-01-02T00:00:00Z\",\"has_changes\":true,\"head_sha\":\"{HEAD}\",\
             \"merge_sha\":\"{MERGE}\",\"plan_key\":\"plans/42/7/{HEAD}/{TREE}/99-2.tfplan\",\
             \"plan_sha256\":\"{DIGEST}\",\"plan_version\":\"ver-1\",\"pr_number\":7,\
             \"repository\":\"example-org/gang\",\"repository_id\":42,\"schema_version\":1,\
             \"tree_sha\":\"{TREE}\",\"workflow_run_attempt\":2,\"workflow_run_id\":99}}"
        )
    }

    #[test]
    fn manifest_prints_the_compact_sorted_json_the_shell_store_wrote() {
        assert_eq!(manifest().to_string(), jq_sorted_json());
        assert_eq!(jq_sorted_json().parse::<Manifest>().unwrap(), manifest());
    }

    #[test]
    fn comment_line_carries_the_base64_of_the_compact_sorted_json() {
        assert_eq!(manifest().comment_line(), line_for(&jq_sorted_json()));
    }

    #[test]
    fn manifest_rejects_a_missing_key() {
        let mut json = manifest_json();
        json.as_object_mut().unwrap().remove("plan_key");
        assert_eq!(
            rejection(&json),
            "manifest keys do not match the expected set"
        );
    }

    #[test]
    fn manifest_rejects_an_unknown_key() {
        assert_eq!(
            rejection(&with("extra", json!(1))),
            "manifest keys do not match the expected set"
        );
    }

    #[test]
    fn manifest_rejects_a_non_boolean_has_changes() {
        assert_eq!(
            rejection(&with("has_changes", json!("yes"))),
            "manifest has_changes has an invalid value"
        );
    }

    #[test]
    fn manifest_rejects_a_malformed_repository() {
        assert_eq!(
            rejection(&with("repository", json!("gang"))),
            "manifest repository has an invalid value"
        );
    }

    #[test]
    fn manifest_rejects_malformed_numbers_shas_digests_and_times() {
        for (key, value) in [
            ("repository_id", json!("42")),
            ("pr_number", json!(-7)),
            ("head_sha", json!("abc")),
            ("tree_sha", json!("A".repeat(40))),
            ("plan_sha256", json!(HEAD)),
            ("expires_at", json!("2026-01-02T00:00:00+00:00")),
            ("plan_key", json!("")),
            ("plan_version", json!("None")),
        ] {
            assert_eq!(
                rejection(&with(key, value)),
                format!("manifest {key} has an invalid value")
            );
        }
    }

    #[test]
    fn manifest_line_round_trips_through_from_comment() {
        let body = format!("Plan text\n{}\n", manifest().comment_line());
        assert_eq!(Manifest::from_comment(&body).unwrap(), manifest());
    }

    #[test]
    fn from_comment_finds_the_manifest_line_followed_by_text() {
        let body = format!("{}\nTrailing text\n", manifest().comment_line());
        assert_eq!(Manifest::from_comment(&body).unwrap(), manifest());
    }

    #[test]
    fn manifest_from_a_file_that_is_not_json_cannot_be_read() {
        let error = "{".parse::<Manifest>().unwrap_err();
        assert_eq!(error.to_string(), "cannot read manifest JSON");
    }

    #[test]
    fn from_comment_rejects_a_comment_without_a_manifest_line() {
        let error = Manifest::from_comment("just text\n").unwrap_err();
        assert_eq!(
            error.to_string(),
            "expected exactly one manifest line in comment, found 0"
        );
    }

    #[test]
    fn from_comment_rejects_two_manifest_lines() {
        let line = manifest().comment_line();
        let error = Manifest::from_comment(&format!("{line}\n{line}\n")).unwrap_err();
        assert_eq!(
            error.to_string(),
            "expected exactly one manifest line in comment, found 2"
        );
    }

    #[test]
    fn from_comment_rejects_an_unclosed_line() {
        let line = manifest().comment_line();
        let unclosed = line.strip_suffix(" -->").unwrap();
        let error = Manifest::from_comment(unclosed).unwrap_err();
        assert_eq!(error.to_string(), "manifest line does not end with -->");
    }

    #[test]
    fn from_comment_rejects_non_base64_characters() {
        let error =
            Manifest::from_comment(&format!("{MANIFEST_LINE_PREFIX} abc!def -->")).unwrap_err();
        assert_eq!(error.to_string(), "manifest payload is not base64");
    }

    #[test]
    fn from_comment_rejects_a_payload_that_does_not_decode() {
        let error = Manifest::from_comment(&format!("{MANIFEST_LINE_PREFIX}abc -->")).unwrap_err();
        assert_eq!(error.to_string(), "manifest payload does not decode");
    }

    #[test]
    fn from_comment_rejects_a_payload_that_is_not_an_object() {
        let error = Manifest::from_comment(&line_for("[1]")).unwrap_err();
        assert_eq!(error.to_string(), "manifest payload is not a JSON object");
    }

    #[test]
    fn from_comment_rejects_a_wrong_schema_version() {
        let json = with("schema_version", json!(2));
        let error = Manifest::from_comment(&line_for(&json.to_string())).unwrap_err();
        assert_eq!(error.to_string(), "manifest schema_version must be 1");
    }

    #[test]
    fn from_comment_rejects_a_payload_missing_a_key() {
        let mut json = manifest_json();
        json.as_object_mut().unwrap().remove("plan_key");
        let error = Manifest::from_comment(&line_for(&json.to_string())).unwrap_err();
        assert_eq!(
            error.to_string(),
            "manifest keys do not match the expected set"
        );
    }

    #[test]
    fn object_metadata_prints_the_user_metadata_string() {
        assert_eq!(
            manifest().facts.object_metadata().to_string(),
            format!(
                "repository-id=42,pr-number=7,head-sha={HEAD},base-sha={BASE},merge-sha={MERGE},\
                 tree-sha={TREE},workflow-run-id=99,workflow-run-attempt=2,plan-sha256={DIGEST},\
                 has-changes=true,expires-at=2026-01-02T00:00:00Z"
            )
        );
    }

    #[test]
    fn object_key_is_the_key_the_manifest_records() {
        let manifest = manifest();
        let prefix = "plans".parse().unwrap();
        assert_eq!(manifest.facts.object_key(&prefix), manifest.plan_key);
    }
}
