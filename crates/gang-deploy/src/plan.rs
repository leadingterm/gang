//! The parts of `tofu show -json` the deploy decisions read: the planned actions of every
//! resource and output.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use serde::Deserialize;
use serde_json::Value;

use crate::errors::{DeployError, Document, Result};

/// A saved plan as `tofu show -json` prints it. Members the decisions do not read are ignored.
#[derive(Debug, Clone, Deserialize)]
pub struct Plan {
    /// The planned change of every resource instance.
    #[serde(default)]
    resource_changes: Option<Vec<ResourceChange>>,
    /// The planned change of every root output.
    #[serde(default)]
    output_changes: Option<BTreeMap<String, OutputChange>>,
}

impl Plan {
    /// The planned change of every resource instance, in plan order.
    #[must_use]
    pub fn resource_changes(&self) -> &[ResourceChange] {
        self.resource_changes.as_deref().unwrap_or_default()
    }

    /// Reports whether applying the plan changes anything: a resource or an output whose actions
    /// are neither `["no-op"]` nor `["read"]`.
    #[must_use]
    pub fn has_changes(&self) -> bool {
        self.resource_changes()
            .iter()
            .any(|resource| resource.change.actions.is_change())
            || self
                .output_changes
                .iter()
                .flat_map(BTreeMap::values)
                .any(|output| output.actions.is_change())
    }

    /// The resource changes that change something, in plan order.
    pub fn changed_resources(&self) -> impl Iterator<Item = &ResourceChange> {
        self.resource_changes()
            .iter()
            .filter(|resource| resource.change.actions.is_change())
    }

    /// The number of resource changes of each kind, in the order of [`ActionKind::ALL`].
    #[must_use]
    pub fn action_counts(&self) -> [(ActionKind, usize); 4] {
        ActionKind::ALL.map(|kind| {
            let count = self
                .resource_changes()
                .iter()
                .filter(|resource| resource.change.actions.kind() == Some(kind))
                .count();
            (kind, count)
        })
    }
}

impl FromStr for Plan {
    type Err = DeployError;

    fn from_str(text: &str) -> Result<Self> {
        serde_json::from_str(text).map_err(|source| DeployError::Json {
            document: Document::Plan,
            source,
        })
    }
}

/// The planned change of one resource instance.
#[derive(Debug, Clone, Deserialize)]
pub struct ResourceChange {
    /// The resource instance address, such as `module.gang.aws_ssm_parameter.config`.
    pub address: String,
    /// The resource type, such as `aws_ssm_parameter`.
    #[serde(rename = "type")]
    pub resource_type: String,
    /// What the plan does to the instance.
    pub change: Change,
}

/// What a plan does to one resource instance.
#[derive(Debug, Clone, Deserialize)]
pub struct Change {
    /// The planned actions.
    pub actions: Actions,
    /// The attributes before the change; `null` for a create.
    #[serde(default)]
    pub before: Value,
    /// The attributes after the change, without the values known only after apply.
    #[serde(default)]
    pub after: Value,
    /// Which attributes are known only after apply.
    #[serde(default)]
    pub after_unknown: Value,
}

impl Change {
    /// The attribute `name` before the change; `null` when absent.
    #[must_use]
    pub fn before_attribute(&self, name: &str) -> &Value {
        self.before.get(name).unwrap_or(&Value::Null)
    }

    /// The attribute `name` after the change; `null` when absent or known only after apply.
    #[must_use]
    pub fn after_attribute(&self, name: &str) -> &Value {
        self.after.get(name).unwrap_or(&Value::Null)
    }

    /// Reports whether the attribute `name` is known only after apply.
    #[must_use]
    pub fn is_unknown_after(&self, name: &str) -> bool {
        self.after_unknown.get(name) == Some(&Value::Bool(true))
    }
}

/// The planned change of one root output.
#[derive(Debug, Clone, Deserialize)]
pub struct OutputChange {
    /// The planned actions.
    pub actions: Actions,
}

/// One planned action.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
#[serde(from = "String")]
pub enum Action {
    /// `no-op`: nothing changes.
    NoOp,
    /// `create`.
    Create,
    /// `read`: a data source is read during apply.
    Read,
    /// `update`: the instance is changed in place.
    Update,
    /// `delete`.
    Delete,
    /// Any other action, by name.
    Other(String),
}

impl From<String> for Action {
    fn from(name: String) -> Self {
        match name.as_str() {
            "no-op" => Self::NoOp,
            "create" => Self::Create,
            "read" => Self::Read,
            "update" => Self::Update,
            "delete" => Self::Delete,
            _ => Self::Other(name),
        }
    }
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NoOp => "no-op",
            Self::Create => "create",
            Self::Read => "read",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::Other(name) => name.as_str(),
        })
    }
}

/// The planned actions of one resource instance or output, in plan order.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct Actions(Vec<Action>);

impl Actions {
    /// The actions in plan order.
    #[must_use]
    pub fn as_slice(&self) -> &[Action] {
        &self.0
    }

    /// Reports whether `action` is one of the actions.
    #[must_use]
    pub fn contains(&self, action: &Action) -> bool {
        self.0.contains(action)
    }

    /// Reports whether the actions change something: they are neither `["no-op"]` nor `["read"]`.
    #[must_use]
    pub fn is_change(&self) -> bool {
        !matches!(self.0.as_slice(), [Action::NoOp | Action::Read])
    }

    /// The kind of change the actions make, for the action table of the plan comment.
    #[must_use]
    pub fn kind(&self) -> Option<ActionKind> {
        if self.contains(&Action::Create) && self.contains(&Action::Delete) {
            return Some(ActionKind::Replace);
        }
        match self.0.as_slice() {
            [Action::Create] => Some(ActionKind::Create),
            [Action::Update] => Some(ActionKind::Update),
            [Action::Delete] => Some(ActionKind::Delete),
            _ => None,
        }
    }

    /// The action names joined by `separator`.
    #[must_use]
    pub fn join(&self, separator: &str) -> String {
        self.0
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(separator)
    }
}

/// The kinds of change the action table of the plan comment counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActionKind {
    /// Exactly `["create"]`.
    Create,
    /// Exactly `["update"]`.
    Update,
    /// Exactly `["delete"]`.
    Delete,
    /// Both `create` and `delete`, in either order.
    Replace,
}

impl ActionKind {
    /// Every kind, in the order of the action table.
    pub const ALL: [Self; 4] = [Self::Create, Self::Update, Self::Delete, Self::Replace];
}

impl fmt::Display for ActionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::Replace => "replace",
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use serde_json::{Value, json};

    use super::{ActionKind, Plan};

    /// A resource change of `resource_type` at `address` with `actions` and nothing else.
    pub(crate) fn resource(resource_type: &str, address: &str, actions: &[&str]) -> Value {
        json!({"address": address, "type": resource_type, "change": {"actions": actions}})
    }

    /// A plan of `resources`.
    pub(crate) fn plan(resources: &[Value]) -> Plan {
        serde_json::from_value(json!({ "resource_changes": resources })).unwrap()
    }

    /// The checked-in `tofu show -json` fixture `name`: real output of `OpenTofu` 1.13.2 with the
    /// AWS provider 6.72.0 for a root with a `module.gang` deployment (its
    /// `aws_ssm_parameter.config` and `module.controller.aws_instance.this`) and the protected
    /// resources, planned with `-refresh=false` from a state of example values. `created` plans
    /// that root from no state, `no-op` plans it unchanged, and each other fixture makes the change
    /// its name says.
    pub(crate) fn fixture(name: &str) -> Plan {
        let text = match name {
            "blocked" => include_str!("../tests/fixtures/blocked.json"),
            "created" => include_str!("../tests/fixtures/created.json"),
            "no-op" => include_str!("../tests/fixtures/no-op.json"),
            "output-only" => include_str!("../tests/fixtures/output-only.json"),
            "parameter-value" => include_str!("../tests/fixtures/parameter-value.json"),
            "parameter-tags" => include_str!("../tests/fixtures/parameter-tags.json"),
            "parameter-unknown" => include_str!("../tests/fixtures/parameter-unknown.json"),
            "controller-replaced" => include_str!("../tests/fixtures/controller-replaced.json"),
            _ => panic!("no fixture {name}"),
        };
        text.parse().unwrap()
    }

    #[test]
    fn has_changes_is_false_for_no_op_and_read_only() {
        let plan = plan(&[
            resource("aws_instance", "aws_instance.a", &["no-op"]),
            resource("aws_region", "data.aws_region.current", &["read"]),
        ]);
        assert!(!plan.has_changes());
        assert!(!fixture("no-op").has_changes());
    }

    #[test]
    fn has_changes_is_true_for_an_update() {
        assert!(plan(&[resource("aws_instance", "aws_instance.a", &["update"])]).has_changes());
        assert!(fixture("parameter-tags").has_changes());
    }

    #[test]
    fn has_changes_is_true_for_an_output_only_change() {
        let plan = fixture("output-only");
        assert!(plan.changed_resources().next().is_none());
        assert!(plan.has_changes());
    }

    #[test]
    fn has_changes_is_false_for_an_empty_plan() {
        assert!(!"{}".parse::<Plan>().unwrap().has_changes());
    }

    #[test]
    fn a_plan_that_is_not_json_cannot_be_read() {
        let error = "not json".parse::<Plan>().unwrap_err();
        assert_eq!(error.to_string(), "cannot read plan JSON");
    }

    #[test]
    fn action_counts_count_creates_updates_deletes_and_replaces() {
        let plan = plan(&[
            resource("aws_kms_key", "aws_kms_key.a", &["create"]),
            resource("aws_kms_key", "aws_kms_key.b", &["create"]),
            resource("aws_ssm_parameter", "aws_ssm_parameter.c", &["update"]),
            resource("aws_instance", "aws_instance.d", &["delete"]),
            resource("aws_kms_key", "aws_kms_key.e", &["delete", "create"]),
            resource("aws_kms_key", "aws_kms_key.f", &["create", "delete"]),
            resource("aws_kms_key", "aws_kms_key.g", &["no-op"]),
        ]);
        assert_eq!(
            plan.action_counts(),
            [
                (ActionKind::Create, 2),
                (ActionKind::Update, 1),
                (ActionKind::Delete, 1),
                (ActionKind::Replace, 2),
            ]
        );
    }

    #[test]
    fn action_counts_of_a_real_plan_count_each_kind() {
        let plan = fixture("blocked");
        assert_eq!(
            plan.action_counts(),
            [
                (ActionKind::Create, 1),
                (ActionKind::Update, 0),
                (ActionKind::Delete, 5),
                (ActionKind::Replace, 1),
            ]
        );
    }
}
