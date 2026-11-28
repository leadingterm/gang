### Changed

- Shared-database controllers run as host-networked ECS EC2 services with digest-pinned images, existing instance target groups, managed N+1 host capacity and bounded SIGTERM drain. SQLite remains a standalone instance without HA.

### Removed

- The controller module's `group` shape, controller ASG lifecycle hooks and host lifecycle configuration are removed. The child module replaces `target_group_arns` with `target_groups`; controller ASG outputs are replaced by ECS service outputs. The deployment's `controller_target_group_arns` remains available.

### Upgrade notes

- Set `controller_image_digest` to the release image digest and migrate existing shared-database ASG deployments in explicit stages: verify an unhealthy ECS standby is prepared with a loopback PROXY v2 LOCAL-framed `/prepared` probe, dispatch a nonblocking stop of the old controller, verify the promoted task and drain, then remove the old resources. A legacy incumbent is not asked to yield automatically. If the reviewed task is not the healthy leader within 60 seconds of stop dispatch, the private caller must quiesce the ECS service, kill and restart the old unit, verify a fresh healthy old-instance leader, and fail the stage retaining the old resources without cleanup. This failure-only recovery can lose and retry old held operations; normal handover preserves them. Listeners do not switch. Local E2E exercises runtime handoff with local fixtures, not the real AWS adapter or private migration workflow. A one-step apply is not a safe migration; see the deployment contract in the specification.
