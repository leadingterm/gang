//! The plan policy: which changes block a merge, and which changes need the controller to
//! reload its configuration after apply.

use std::fmt;

use crate::errors::{DeployError, Result};
use crate::plan::{Action, Actions, Change, Plan, ResourceChange};

/// Resource types whose deletion or replacement blocks a merge: they hold state that cannot be
/// recreated (keys, data, addresses, federation).
pub const PROTECTED_TYPES: [&str; 7] = [
    "aws_kms_key",
    "aws_s3_bucket",
    "aws_dynamodb_table",
    "aws_db_instance",
    "aws_ebs_volume",
    "aws_eip",
    "aws_iam_outbound_web_identity_federation",
];

/// Address of the configuration parameter below a deployment module, after `module.<name>.`.
const CONFIG_PARAMETER: &str = "aws_ssm_parameter.config";
/// Address of the immutable configuration generation below a deployment module.
const CONFIG_GENERATION_PARAMETER: &str = "aws_ssm_parameter.config_generation";
/// Address of the controller instance below a deployment module, after `module.<name>.`.
const CONTROLLER_INSTANCE: &str = "module.controller.aws_instance.this[0]";

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
    /// The exact planned immutable configuration generation.
    ///
    /// # Errors
    ///
    /// Returns an error if the generation is missing, ambiguous, unknown, or has no non-empty
    /// string value.
    pub fn configuration(&self) -> Result<&str> {
        let mut generations = self.resource_changes().iter().filter(|resource| {
            resource.resource_type == "aws_ssm_parameter"
                && is_module_resource(&resource.address, CONFIG_GENERATION_PARAMETER)
        });
        let generation = generations
            .next()
            .ok_or(DeployError::ConfigurationMissing)?;
        if let Some(other) = generations.next() {
            return Err(DeployError::ConfigurationMultiple {
                first: generation.address.clone(),
                second: other.address.clone(),
            });
        }
        if generation.change.is_unknown_after("value") {
            return Err(DeployError::ConfigurationUnknown {
                address: generation.address.clone(),
            });
        }
        generation
            .change
            .after_attribute("value")
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| DeployError::ConfigurationInvalid {
                address: generation.address.clone(),
            })
    }

    /// The protected resource deletions or replacements, in plan order.
    #[must_use]
    pub fn protected_changes(&self) -> Vec<ProtectedChange<'_>> {
        self.resource_changes()
            .iter()
            .filter(|resource| {
                resource.change.actions.contains(&Action::Delete)
                    && (PROTECTED_TYPES.contains(&resource.resource_type.as_str())
                        || (resource.resource_type == "aws_ssm_parameter"
                            && (is_module_resource(&resource.address, CONFIG_PARAMETER)
                                || is_module_resource(
                                    &resource.address,
                                    CONFIG_GENERATION_PARAMETER,
                                ))))
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
    use crate::errors::DeployError;
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
    fn configuration_preserves_exact_bytes_for_no_op_generation() {
        let configuration = "server:\n  name: example\n\n";
        let generation = json!({
            "address": "module.gang.aws_ssm_parameter.config_generation",
            "type": "aws_ssm_parameter",
            "change": {"actions": ["no-op"], "after": {"value": configuration}},
        });
        let plan = plan(&[generation]);
        assert_eq!(plan.configuration().expect("configuration"), configuration);
    }

    #[test]
    fn configuration_rejects_unknown_value_even_when_after_contains_a_value() {
        let address = "module.gang.aws_ssm_parameter.config_generation";
        let generation = parameter_update(
            address,
            &json!({}),
            &json!({"value": "stale"}),
            &json!({"value": true}),
        );
        let plan = plan(&[generation]);
        let error = plan.configuration().expect_err("unknown value");
        assert!(
            matches!(&error, DeployError::ConfigurationUnknown { address: found } if found == address)
        );
        assert!(error.to_string().contains("after_unknown.value"));
    }

    #[test]
    fn configuration_rejects_missing_generation() {
        assert!(matches!(
            plan(&[]).configuration(),
            Err(DeployError::ConfigurationMissing)
        ));
    }

    #[test]
    fn configuration_rejects_multiple_generations() {
        let first = "module.first.aws_ssm_parameter.config_generation";
        let second = "module.second.aws_ssm_parameter.config_generation";
        let plan = plan(&[value_update(first), value_update(second)]);
        assert!(matches!(
            plan.configuration(),
            Err(DeployError::ConfigurationMultiple { first: a, second: b })
                if a == first && b == second
        ));
    }

    #[test]
    fn configuration_ignores_unrelated_and_nested_resources() {
        let address = "module.gang.aws_ssm_parameter.config_generation";
        let plan = plan(&[
            value_update("aws_ssm_parameter.config_generation"),
            value_update("module.gang.module.child.aws_ssm_parameter.config_generation"),
            value_update("module.gang.aws_ssm_parameter.config"),
            resource("other_type", address, &["no-op"]),
            value_update(address),
        ]);
        assert_eq!(plan.configuration().expect("configuration"), "b");
    }

    #[test]
    fn configuration_rejects_missing_empty_and_non_string_values() {
        let address = "module.gang.aws_ssm_parameter.config_generation";
        for after in [
            json!({}),
            json!({"value": ""}),
            json!({"value": 42}),
            json!({"value": null}),
        ] {
            let generation = parameter_update(address, &json!({}), &after, &json!({}));
            let plan = plan(&[generation]);
            let error = plan.configuration().expect_err("invalid value");
            assert!(
                matches!(&error, DeployError::ConfigurationInvalid { address: found } if found == address)
            );
            assert!(error.to_string().contains("after.value"));
        }
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
    fn deleting_or_replacing_deployment_configuration_is_blocked() {
        for name in ["config", "config_generation"] {
            let address = format!("module.gang.aws_ssm_parameter.{name}");
            for actions in [
                &["delete"][..],
                &["delete", "create"],
                &["create", "delete"],
            ] {
                let plan = plan(&[resource("aws_ssm_parameter", &address, actions)]);
                let protected = plan.protected_changes();
                assert_eq!(protected.len(), 1, "{address} {actions:?}");
                assert_eq!(
                    protected[0].to_string(),
                    format!("{address}\taws_ssm_parameter\t{}", actions.join(","))
                );
            }
            for actions in [
                &["no-op"][..],
                &["update"],
                &["create"],
                &["forget"],
                &["forget", "create"],
            ] {
                assert!(
                    !plan(&[resource("aws_ssm_parameter", &address, actions)]).is_blocked(),
                    "{address} {actions:?}"
                );
            }
        }
    }

    #[test]
    fn deleting_other_parameter_addresses_is_allowed() {
        for address in [
            "aws_ssm_parameter.config",
            "aws_ssm_parameter.config_generation",
            "module.gang.module.inner.aws_ssm_parameter.config",
            "module.gang.module.inner.aws_ssm_parameter.config_generation",
            "module.gang.aws_ssm_parameter.other",
            "module.gang.aws_ssm_parameter.config_extra",
            "module.gang.aws_ssm_parameter.config_generation[0]",
            "module..aws_ssm_parameter.config",
        ] {
            assert!(
                !plan(&[resource("aws_ssm_parameter", address, &["delete"])]).is_blocked(),
                "{address}"
            );
        }
        assert!(
            !plan(&[resource(
                "other_type",
                "module.gang.aws_ssm_parameter.config",
                &["delete"],
            )])
            .is_blocked()
        );
    }

    #[test]
    fn deleting_a_dynamodb_table_is_blocked() {
        let plan = plan(&[resource(
            "aws_dynamodb_table",
            "aws_dynamodb_table.state",
            &["delete"],
        )]);
        assert!(plan.is_blocked());
    }

    #[test]
    fn deleting_an_rds_instance_is_blocked() {
        let plan = plan(&[resource(
            "aws_db_instance",
            "aws_db_instance.postgres",
            &["delete"],
        )]);
        assert!(plan.is_blocked());
    }

    #[test]
    fn replacing_an_rds_parameter_group_is_allowed() {
        let plan = plan(&[resource(
            "aws_db_parameter_group",
            "aws_db_parameter_group.postgres",
            &["delete", "create"],
        )]);
        assert!(!plan.is_blocked());
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
        for actions in [
            &["create"][..],
            &["delete", "create"],
            &["create", "delete"],
        ] {
            for parameter in [
                value_update("module.gang.aws_ssm_parameter.config"),
                resource(
                    "aws_ssm_parameter",
                    "module.gang.aws_ssm_parameter.config",
                    &["create"],
                ),
            ] {
                let controller = resource(
                    "aws_instance",
                    "module.gang.module.controller.aws_instance.this[0]",
                    actions,
                );
                assert!(
                    !plan(&[parameter, controller]).restart_needed(),
                    "{actions:?}"
                );
            }
        }
        assert!(!fixture("controller-replaced").restart_needed());
    }

    #[test]
    fn generation_rollout_and_rollback_do_not_restart_the_controller() {
        for actions in [&["create"][..], &["forget", "create"]] {
            let generation = resource(
                "aws_ssm_parameter",
                "module.gang.aws_ssm_parameter.config_generation",
                actions,
            );
            let controller = resource(
                "aws_instance",
                "module.gang.module.controller.aws_instance.this[0]",
                &["delete", "create"],
            );
            let plan = plan(&[generation, controller]);
            assert!(!plan.is_blocked(), "{actions:?}");
            assert!(!plan.restart_needed(), "{actions:?}");
        }
    }

    #[test]
    fn no_parameter_change_does_not_restart() {
        let plan = plan(&[
            resource(
                "aws_instance",
                "module.gang.module.controller.aws_instance.this[0]",
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
