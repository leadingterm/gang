//! The plan policy: which changes block a merge, and which changes need the controller to
//! reload its configuration after apply.

use std::fmt;

use crate::plan::{Action, Actions, Change, Plan, ResourceChange};

/// Resource types whose deletion or replacement blocks a merge: they hold state that cannot be
/// recreated (keys, data, addresses, federation).
pub const PROTECTED_TYPES: [&str; 5] = [
    "aws_kms_key",
    "aws_s3_bucket",
    "aws_ebs_volume",
    "aws_eip",
    "aws_iam_outbound_web_identity_federation",
];

/// Address of the configuration parameter below a deployment module, after `module.<name>.`.
const CONFIG_PARAMETER: &str = "aws_ssm_parameter.config";
/// Address of the controller instance below a deployment module, after `module.<name>.`.
const CONTROLLER_INSTANCE: &str = "module.controller.aws_instance.this";

/// A planned deletion or replacement of a protected resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtectedChange<'a> {
    /// The resource instance address.
    pub address: &'a str,
    /// The resource type.
    pub resource_type: &'a str,
    /// The planned actions.
    pub actions: &'a Actions,
}

impl fmt::Display for ProtectedChange<'_> {
    /// `<address>\t<type>\t<actions joined by ,>`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}\t{}\t{}",
            self.address,
            self.resource_type,
            self.actions.join(",")
        )
    }
}

impl Plan {
    /// The changes that delete or replace a resource of a [`PROTECTED_TYPES`] type, in plan order.
    #[must_use]
    pub fn protected_changes(&self) -> Vec<ProtectedChange<'_>> {
        self.resource_changes()
            .iter()
            .filter(|resource| {
                resource.change.actions.contains(&Action::Delete)
                    && PROTECTED_TYPES.contains(&resource.resource_type.as_str())
            })
            .map(|resource| ProtectedChange {
                address: &resource.address,
                resource_type: &resource.resource_type,
                actions: &resource.change.actions,
            })
            .collect()
    }

    /// Reports whether the plan must not merge: it deletes or replaces a protected resource.
    #[must_use]
    pub fn is_blocked(&self) -> bool {
        !self.protected_changes().is_empty()
    }

    /// Reports whether applying the plan needs the running controller to reload its
    /// configuration: a deployment's configuration parameter is created, or updated with a new or
    /// unknown value, and no deployment's controller instance is created. A created controller
    /// reads the new configuration when it boots.
    #[must_use]
    pub fn restart_needed(&self) -> bool {
        let resources = self.resource_changes();
        resources.iter().any(changes_configuration) && !resources.iter().any(creates_controller)
    }
}

/// Reports whether `address` is `module.<name>.<rest>` for a name without dots.
fn is_module_resource(address: &str, rest: &str) -> bool {
    address
        .strip_prefix("module.")
        .and_then(|address| address.split_once('.'))
        .is_some_and(|(name, tail)| !name.is_empty() && tail == rest)
}

/// Reports whether the parameter's value changes: it differs, or it is known only after apply.
fn value_changed(change: &Change) -> bool {
    change.before_attribute("value") != change.after_attribute("value")
        || change.is_unknown_after("value")
}

/// Reports whether `resource` creates a deployment's configuration parameter or changes its value.
fn changes_configuration(resource: &ResourceChange) -> bool {
    let actions = &resource.change.actions;
    resource.resource_type == "aws_ssm_parameter"
        && is_module_resource(&resource.address, CONFIG_PARAMETER)
        && (actions.contains(&Action::Create)
            || (actions.contains(&Action::Update) && value_changed(&resource.change)))
}

/// Reports whether `resource` creates a deployment's controller instance.
fn creates_controller(resource: &ResourceChange) -> bool {
    resource.resource_type == "aws_instance"
        && is_module_resource(&resource.address, CONTROLLER_INSTANCE)
        && resource.change.actions.contains(&Action::Create)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::PROTECTED_TYPES;
    use crate::plan::tests::{fixture, plan, resource};

    /// An update of the parameter at `address` from `before` to `after`.
    fn parameter_update(address: &str, before: &Value, after: &Value, unknown: &Value) -> Value {
        json!({
            "address": address,
            "type": "aws_ssm_parameter",
            "change": {
                "actions": ["update"],
                "before": before,
                "after": after,
                "after_unknown": unknown,
            },
        })
    }

    fn value_update(address: &str) -> Value {
        parameter_update(
            address,
            &json!({"value": "a"}),
            &json!({"value": "b"}),
            &json!({}),
        )
    }

    #[test]
    fn creating_an_aws_kms_key_passes() {
        assert!(!plan(&[resource("aws_kms_key", "aws_kms_key.state", &["create"])]).is_blocked());
        assert!(!fixture("created").is_blocked());
    }

    #[test]
    fn deleting_or_replacing_a_protected_resource_is_blocked() {
        for resource_type in PROTECTED_TYPES {
            for actions in [
                &["delete"][..],
                &["delete", "create"],
                &["create", "delete"],
            ] {
                let address = format!("{resource_type}.example");
                let plan = plan(&[resource(resource_type, &address, actions)]);
                let protected = plan.protected_changes();
                assert_eq!(protected.len(), 1, "{resource_type} {actions:?}");
                assert_eq!(
                    protected[0].to_string(),
                    format!("{address}\t{resource_type}\t{}", actions.join(","))
                );
            }
        }
    }

    #[test]
    fn deleting_an_aws_instance_is_allowed() {
        let plan = plan(&[resource("aws_instance", "aws_instance.worker", &["delete"])]);
        assert!(!plan.is_blocked());
        assert!(plan.has_changes());
    }

    #[test]
    fn a_no_op_plan_passes() {
        assert!(!plan(&[resource("aws_kms_key", "aws_kms_key.state", &["no-op"])]).is_blocked());
        assert!(!fixture("no-op").is_blocked());
    }

    #[test]
    fn a_real_plan_lists_each_protected_change_in_plan_order() {
        let plan = fixture("blocked");
        let protected: Vec<String> = plan
            .protected_changes()
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(protected, BLOCKED_PROTECTED_CHANGES);
        assert!(plan.is_blocked());
    }

    /// The protected changes of the `blocked` fixture, as the shell's `protected_changes` jq
    /// filter printed them for that file.
    const BLOCKED_PROTECTED_CHANGES: [&str; 5] = [
        "aws_ebs_volume.data[0]\taws_ebs_volume\tdelete",
        "aws_eip.gateway[0]\taws_eip\tdelete",
        "aws_iam_outbound_web_identity_federation.main[0]\taws_iam_outbound_web_identity_federation\tdelete",
        "aws_kms_key.state\taws_kms_key\tdelete,create",
        "aws_s3_bucket.state[0]\taws_s3_bucket\tdelete",
    ];

    #[test]
    fn parameter_tags_only_update_does_not_restart() {
        let tags = parameter_update(
            "module.gang.aws_ssm_parameter.config",
            &json!({"value": "a", "tags": {"v": "1"}}),
            &json!({"value": "a", "tags": {"v": "2"}}),
            &json!({}),
        );
        assert!(!plan(&[tags]).restart_needed());
        assert!(!fixture("parameter-tags").restart_needed());
    }

    #[test]
    fn parameter_value_update_restarts_the_controller() {
        assert!(plan(&[value_update("module.gang.aws_ssm_parameter.config")]).restart_needed());
        assert!(fixture("parameter-value").restart_needed());
    }

    #[test]
    fn parameter_value_unknown_after_apply_restarts_the_controller() {
        let unknown = parameter_update(
            "module.gang.aws_ssm_parameter.config",
            &json!({"value": "a"}),
            &json!({"value": null}),
            &json!({"value": true}),
        );
        assert!(plan(&[unknown]).restart_needed());
        assert!(fixture("parameter-unknown").restart_needed());
    }

    #[test]
    fn parameter_create_restarts_the_controller() {
        let create = resource(
            "aws_ssm_parameter",
            "module.gang.aws_ssm_parameter.config",
            &["create"],
        );
        assert!(plan(&[create]).restart_needed());
    }

    #[test]
    fn parameter_value_update_with_controller_create_does_not_restart() {
        let controller = resource(
            "aws_instance",
            "module.gang.module.controller.aws_instance.this",
            &["create"],
        );
        assert!(
            !plan(&[
                value_update("module.gang.aws_ssm_parameter.config"),
                controller
            ])
            .restart_needed()
        );
        assert!(!fixture("controller-replaced").restart_needed());
    }

    #[test]
    fn no_parameter_change_does_not_restart() {
        let plan = plan(&[
            resource(
                "aws_instance",
                "module.gang.module.controller.aws_instance.this",
                &["no-op"],
            ),
            resource("aws_kms_key", "aws_kms_key.state", &["no-op"]),
        ]);
        assert!(!plan.restart_needed());
        assert!(!fixture("no-op").restart_needed());
    }

    #[test]
    fn parameter_value_update_at_another_address_does_not_restart() {
        for address in [
            "module.gang.aws_ssm_parameter.other",
            "aws_ssm_parameter.config",
            "module.gang.module.inner.aws_ssm_parameter.config",
            "module..aws_ssm_parameter.config",
        ] {
            assert!(
                !plan(&[value_update(address)]).restart_needed(),
                "{address}"
            );
        }
    }

    #[test]
    fn parameter_value_update_with_another_instance_create_still_restarts() {
        let worker = resource(
            "aws_instance",
            "module.gang.aws_instance.worker",
            &["create"],
        );
        assert!(
            plan(&[value_update("module.gang.aws_ssm_parameter.config"), worker]).restart_needed()
        );
    }

    #[test]
    fn parameter_value_update_under_another_module_restarts() {
        assert!(plan(&[value_update("module.build.aws_ssm_parameter.config")]).restart_needed());
    }
}
