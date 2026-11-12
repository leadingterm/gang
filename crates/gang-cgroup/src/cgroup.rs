//! The public `[limits.cgroup]` plugin.

use gang_clock::SystemClock;
use gang_spi::{ActionLimits, ActionLimitsPort, ActionUsage, Plugin, PluginContext, PortError};

use crate::config::CgroupConfig;
use crate::controller::{CgroupScope, Controller};
use crate::fs::Kernel;

/// The `[limits.cgroup]` plugin. Runs every action in its own cgroup v2 scope
/// (memory, CPU, pids) under the worker's delegated subtree. Linux only.
#[derive(Debug)]
pub struct Cgroup {
    controller: Controller<Kernel, SystemClock>,
}

impl Plugin for Cgroup {
    type Config = CgroupConfig;

    /// Builds the controller over the real kernel cgroup filesystem. Nothing is created or moved
    /// until the worker prepares. The `gang-join` link sits beside `config_path`, where the sandbox
    /// file is.
    fn build(config: Self::Config, context: &PluginContext<'_>) -> Result<Self, PortError> {
        let root = config.cgroup_root.as_path().to_path_buf();
        let join = context.config_path.with_file_name(gang_spi::JOIN_LINK_NAME);
        Ok(Self {
            controller: Controller::new(Kernel::new(root), SystemClock::new(), config, join),
        })
    }
}

/// `enter` and `leave` run in the `gang exec` process, which stays in the worker's leaf cgroup:
/// the sandbox's processes join their scope through the launcher. `prepare` runs in the worker
/// itself, before it takes any action.
impl ActionLimitsPort for Cgroup {
    type Scope = CgroupScope;

    async fn prepare(&self) -> Result<(), PortError> {
        Ok(self.controller.prepare().await?)
    }

    async fn enter(&self, limits: &ActionLimits) -> Result<CgroupScope, PortError> {
        Ok(self.controller.enter(limits)?)
    }

    async fn leave(&self, scope: CgroupScope) -> Result<ActionUsage, PortError> {
        Ok(self.controller.leave(scope).await?)
    }

    async fn reap_abandoned(&self, creator: u32) -> Result<(), PortError> {
        Ok(self.controller.reap_abandoned(creator).await?)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use gang_spi::{ActionLimits, ActionLimitsPort, Plugin, PluginContext, PortError};
    use gang_types::AbsolutePathBuf;

    use super::Cgroup;
    use crate::config::CgroupConfig;

    /// The plugin context every test builds the cgroup plugin with.
    fn context() -> PluginContext<'static> {
        PluginContext {
            config_path: Path::new("gang.toml"),
        }
    }

    #[tokio::test]
    async fn the_plugin_surfaces_prepare_and_enter_failures_as_backend_errors() {
        let root = tempfile::tempdir().expect("create the cgroup root");
        let config = CgroupConfig {
            cgroup_root: AbsolutePathBuf::try_new(root.path().to_path_buf())
                .expect("the temporary directory is absolute"),
            ..Default::default()
        };
        let cgroup = Cgroup::build(config, &context()).expect("the plugin builds");

        let prepared = ActionLimitsPort::prepare(&cgroup).await;
        assert!(matches!(prepared, Err(PortError::Backend(_))));

        let entered = ActionLimitsPort::enter(
            &cgroup,
            &ActionLimits {
                cpu_count: None,
                memory: None,
            },
        )
        .await;
        assert!(matches!(entered, Err(PortError::Backend(_))));
    }
}
