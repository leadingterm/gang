//! The composed plugin set: the one place the binary names its concrete plugin crates.

use std::path::Path;

use gang_authn::NoneConfig;
use gang_config::AuthenticationConfig;
use gang_types::SchedulerSettings;

gang_spi::compose! {
    provisioner: Provisioner { aws: gang_aws::Aws, local: gang_local::Local },
    cache: Cache { s3: gang_aws::S3Cache, filesystem: gang_local::FilesystemCache },
    signer: Signer { kms: gang_aws::KmsSigner, file: gang_local::FileSigner },
    scheduler: Scheduler { nativelink: gang_nativelink::NativeLink },
    database: Database { sqlite: gang_sqlite::Sqlite },
    sandbox: Sandbox { noop: gang_noop::Noop, bwrap: gang_bwrap::Bwrap },
    identity_provider: IdentityProvider { oidc: gang_oidc::Provider },
    client_authentication: ClientAuthentication { token: gang_authn::Token, none: gang_authn::Anonymous },
    worker_authentication: WorkerAuthentication { mtls: gang_authn::Mtls, none: gang_authn::Anonymous },
    limits: Limits { cgroup: gang_cgroup::Cgroup },
}

/// Returns the local provisioner's state directory, or `None` for another provisioner.
#[must_use]
pub(crate) fn local_state_dir(provisioner: &ProvisionerConfig) -> Option<&Path> {
    match provisioner {
        ProvisionerConfig::Local(local) => Some(&local.state_dir),
        ProvisionerConfig::Aws(_) => None,
    }
}

/// Configures anonymous authentication for both clients and workers.
#[must_use]
pub(crate) fn anonymous_authentication()
-> AuthenticationConfig<ClientAuthenticationConfig, WorkerAuthenticationConfig> {
    AuthenticationConfig {
        clients: ClientAuthenticationConfig::None(NoneConfig::default()),
        workers: WorkerAuthenticationConfig::None(NoneConfig::default()),
    }
}

/// The dotted configuration key of the scheduler's worker endpoint, `scheduler.<plugin>.<field>`;
/// the plugin's section alone when the plugin has no worker endpoint.
pub(crate) fn worker_endpoint_key(scheduler: &SchedulerConfig) -> String {
    let section = format!("{}.{}", Scheduler::DOMAIN, scheduler.key());
    match SchedulerSettings::worker_endpoint_key(scheduler) {
        Some(field) => format!("{section}.{field}"),
        None => section,
    }
}
