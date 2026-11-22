//! The plan policy: which changes block a merge.

use std::fmt;

use crate::errors::{DeployError, Result};
use crate::plan::{Action, Actions, Plan, ResourceChange};

/// Resource types whose deletion or replacement blocks a merge: they hold state or running
/// capacity. Only Auto Scaling groups support an exact-address exception.
pub const PROTECTED_TYPES: [&str; 8] = [
    "aws_kms_key",
    "aws_s3_bucket",
    "aws_dynamodb_table",
    "aws_db_instance",
    "aws_ebs_volume",
    "aws_eip",
    "aws_iam_outbound_web_identity_federation",
    "aws_autoscaling_group",
];

/// Address of the immutable configuration generation below a deployment module.
const CONFIG_GENERATION_PARAMETER: &str = "aws_ssm_parameter.config_generation";

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
    ///
    /// `allow_asg_destroy` exempts only Auto Scaling groups with an exactly matching address.
    /// An empty slice grants no exceptions.
    #[must_use]
    pub fn protected_changes(&self, allow_asg_destroy: &[String]) -> Vec<ProtectedChange<'_>> {
        self.resource_changes()
            .iter()
            .filter(|resource| {
                resource.change.actions.contains(&Action::Delete)
                    && (PROTECTED_TYPES.contains(&resource.resource_type.as_str())
                        || (resource.resource_type == "aws_ssm_parameter"
                            && is_module_resource(&resource.address, CONFIG_GENERATION_PARAMETER)))
                    && !resource.is_allowed_asg_destroy(allow_asg_destroy)
            })
            .map(|resource| ProtectedChange {
                address: &resource.address,
                resource_type: &resource.resource_type,
                actions: &resource.change.actions,
            })
            .collect()
    }

    /// Reports whether the plan must not merge after exact-address Auto Scaling group exceptions.
    #[must_use]
    pub fn is_blocked(&self, allow_asg_destroy: &[String]) -> bool {
        !self.protected_changes(allow_asg_destroy).is_empty()
    }
}

impl ResourceChange {
    /// Whether this destructive Auto Scaling group change has an exact-address exception.
    pub(crate) fn is_allowed_asg_destroy(&self, allow_asg_destroy: &[String]) -> bool {
        self.resource_type == "aws_autoscaling_group"
            && self.change.actions.contains(&Action::Delete)
            && !self.address.is_empty()
            && allow_asg_destroy.contains(&self.address)
    }
}

/// Reports whether `address` is `module.<name>.<rest>` for a name without dots.
fn is_module_resource(address: &str, rest: &str) -> bool {
    address
        .strip_prefix("module.")
        .and_then(|address| address.split_once('.'))
        .is_some_and(|(name, tail)| !name.is_empty() && tail == rest)
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
    fn bootstrap_configuration_requires_generation_but_not_consumers() {
        let configuration = "[server]\nname = \"example\"\n\n";
        let mut resources = vec![
            resource(
                "aws_ecr_repository",
                "module.gang.module.controller.aws_ecr_repository.controller[0]",
                &["create"],
            ),
            resource(
                "aws_lb_target_group",
                "module.gang.module.load_balancer[0].aws_lb_target_group.service[0]",
                &["create"],
            ),
        ];
        assert!(matches!(
            plan(&resources).configuration(),
            Err(DeployError::ConfigurationMissing)
        ));

        let mut generation = resource(
            "aws_ssm_parameter",
            "module.gang.aws_ssm_parameter.config_generation",
            &["create"],
        );
        generation["change"]["after"] = json!({"value": configuration});
        resources.push(generation);
        let plan = plan(&resources);
        assert!(!plan.is_blocked(&[]));
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
    fn configuration_rejects_omitted_after() {
        let generation = resource(
            "aws_ssm_parameter",
            "module.gang.aws_ssm_parameter.config_generation",
            &["no-op"],
        );
        let plan = plan(&[generation]);
        assert!(matches!(
            plan.configuration(),
            Err(DeployError::ConfigurationInvalid { .. })
        ));
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
        assert!(
            !plan(&[resource("aws_kms_key", "aws_kms_key.state", &["create"])]).is_blocked(&[])
        );
        assert!(!fixture("created").is_blocked(&[]));
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
                let protected = plan.protected_changes(&[]);
                assert_eq!(protected.len(), 1, "{resource_type} {actions:?}");
                assert_eq!(
                    protected[0].to_string(),
                    format!("{address}\t{resource_type}\t{}", actions.join(","))
                );
            }
        }
    }

    #[test]
    fn deleting_or_replacing_deployment_generation_is_blocked() {
        let address = "module.gang.aws_ssm_parameter.config_generation";
        for actions in [
            &["delete"][..],
            &["delete", "create"],
            &["create", "delete"],
        ] {
            let plan = plan(&[resource("aws_ssm_parameter", address, actions)]);
            let protected = plan.protected_changes(&[]);
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
                !plan(&[resource("aws_ssm_parameter", address, actions)]).is_blocked(&[]),
                "{address} {actions:?}"
            );
        }
    }

    #[test]
    fn deleting_other_parameter_addresses_is_allowed() {
        for address in [
            "module.gang.aws_ssm_parameter.config",
            "aws_ssm_parameter.config_generation",
            "module.gang.module.inner.aws_ssm_parameter.config_generation",
            "module.gang.aws_ssm_parameter.other",
            "module.gang.aws_ssm_parameter.config_generation_extra",
            "module.gang.aws_ssm_parameter.config_generation[0]",
            "module..aws_ssm_parameter.config_generation",
        ] {
            assert!(
                !plan(&[resource("aws_ssm_parameter", address, &["delete"])]).is_blocked(&[]),
                "{address}"
            );
        }
        assert!(
            !plan(&[resource(
                "other_type",
                "module.gang.aws_ssm_parameter.config_generation",
                &["delete"],
            )])
            .is_blocked(&[])
        );
    }

    #[test]
    fn deleting_a_dynamodb_table_is_blocked() {
        let plan = plan(&[resource(
            "aws_dynamodb_table",
            "aws_dynamodb_table.state",
            &["delete"],
        )]);
        assert!(plan.is_blocked(&[]));
    }

    #[test]
    fn deleting_an_rds_instance_is_blocked() {
        let plan = plan(&[resource(
            "aws_db_instance",
            "aws_db_instance.postgres",
            &["delete"],
        )]);
        assert!(plan.is_blocked(&[]));
    }

    #[test]
    fn replacing_an_rds_parameter_group_is_allowed() {
        let plan = plan(&[resource(
            "aws_db_parameter_group",
            "aws_db_parameter_group.postgres",
            &["delete", "create"],
        )]);
        assert!(!plan.is_blocked(&[]));
    }

    #[test]
    fn deleting_an_aws_instance_is_allowed() {
        let plan = plan(&[resource("aws_instance", "aws_instance.worker", &["delete"])]);
        assert!(!plan.is_blocked(&[]));
        assert!(plan.has_changes());
    }

    #[test]
    fn a_no_op_plan_passes() {
        assert!(!plan(&[resource("aws_kms_key", "aws_kms_key.state", &["no-op"])]).is_blocked(&[]));
        assert!(!fixture("no-op").is_blocked(&[]));
    }

    #[test]
    fn a_real_plan_lists_each_protected_change_in_plan_order() {
        let plan = fixture("blocked");
        let protected: Vec<String> = plan
            .protected_changes(&[])
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(protected, BLOCKED_PROTECTED_CHANGES);
        assert!(plan.is_blocked(&[]));
    }

    #[test]
    fn every_asg_address_and_destructive_order_requires_an_exact_exception() {
        for address in [
            "module.gang.module.controller.aws_autoscaling_group.this[0]",
            "module.gang.module.worker.aws_autoscaling_group.fleet[\"arm spot\"]",
            "module.other.module.nested.aws_autoscaling_group.other",
            "aws_autoscaling_group.unrelated",
        ] {
            for actions in [
                &["delete"][..],
                &["delete", "create"],
                &["create", "delete"],
            ] {
                let plan = plan(&[resource("aws_autoscaling_group", address, actions)]);
                assert!(plan.is_blocked(&[]), "{address} {actions:?}");
                assert!(!plan.is_blocked(&[address.to_owned()]));
                for wrong in ["", "*", "module.gang", "aws_autoscaling_group.*"] {
                    assert!(plan.is_blocked(&[wrong.to_owned()]), "{wrong}");
                }
                assert!(plan.is_blocked(&[format!("{address}_other")]));
                assert!(plan.is_blocked(&[address[..address.len() - 1].to_owned()]));
            }
            for actions in [&["create"][..], &["update"], &["no-op"]] {
                assert!(
                    !plan(&[resource("aws_autoscaling_group", address, actions)]).is_blocked(&[])
                );
            }
        }
    }

    #[test]
    fn asg_exceptions_are_individual_and_never_exempt_other_protected_types() {
        let first = "aws_autoscaling_group.first";
        let second = "aws_autoscaling_group.second";
        let subject = plan(&[
            resource("aws_autoscaling_group", first, &["delete"]),
            resource("aws_autoscaling_group", second, &["create", "delete"]),
        ]);
        let allowed = [first.to_owned()];
        let blocked = subject.protected_changes(&allowed);
        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].address, second);
        assert!(subject.is_blocked(&allowed));
        assert!(!subject.is_blocked(&[first.to_owned(), second.to_owned()]));
        for resource_type in PROTECTED_TYPES {
            if resource_type != "aws_autoscaling_group" {
                let plan = plan(&[resource(resource_type, first, &["delete"])]);
                assert!(plan.is_blocked(&allowed), "{resource_type}");
            }
        }
        let generation = "module.gang.aws_ssm_parameter.config_generation";
        assert!(
            plan(&[resource("aws_ssm_parameter", generation, &["delete"])])
                .is_blocked(&[generation.to_owned()])
        );
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
}
