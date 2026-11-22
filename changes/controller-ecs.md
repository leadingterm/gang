### Changed

- Shared-database controllers run as ECS EC2 services with digest-pinned images, cooperative leader handoff and bounded SIGTERM drain. SQLite remains a standalone instance without HA.

### Removed

- The controller module's `group` shape, controller ASG lifecycle hooks and host lifecycle configuration are removed. The child module replaces `target_group_arns` with `target_groups`; controller ASG outputs are replaced by ECS service outputs. The deployment's `controller_target_group_arns` remains available.

### Upgrade notes

- Set `controller_image_digest` to the release image digest and migrate existing shared-database ASG deployments in explicit stages: preserve the old controller and routing while creating the service and IP targets, hand off and drain, then remove the old resources. A one-step apply is not a safe migration. See the deployment contract in the specification.
