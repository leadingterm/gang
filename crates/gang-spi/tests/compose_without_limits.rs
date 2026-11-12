//! `compose!` with every domain except `limits`: the composed configuration has no limits section,
//! and the `Limits` type still implements `ActionLimitsPort` (as an uninhabited enum).

use std::error::Error;

use gang_config::GangConfig;

#[cfg(test)]
#[macro_use]
mod support;

use support::{
    Alpha, AuthOpen, AuthTls, Beta, Boxed, CacheCloud, CacheDisk, Direct, Memory, SignerFile,
    SignerKms, Stub, Token,
};

gang_spi::compose! {
    provisioner: Provisioner { alpha: Alpha, beta: Beta },
    cache: Cache { disk: CacheDisk, cloud: CacheCloud },
    signer: Signer { kms: SignerKms, file: SignerFile },
    scheduler: Scheduler { stub: Stub },
    database: Database { memory: Memory },
    sandbox: Sandbox { direct: Direct, boxed: Boxed },
    identity_provider: IdentityProvider { token: Token },
    client_authentication: ClientAuthentication { token: AuthTls, none: AuthOpen },
    worker_authentication: WorkerAuthentication { mtls: AuthTls, none: AuthOpen },
}

const CONFIG: &str = r#"
poll_interval = "1s"
client_action_timeout = "1m"
default_isa = "x86-64"
listen = "127.0.0.1:9443"
worker_listen = "127.0.0.1:9090"
endpoint = "http://127.0.0.1:9443"

[server]
transport = "plaintext"

[authentication.clients.none]
number = 12

[authentication.workers.none]
number = 13

[[fleets]]
name = "workers"
isa = "x86-64"
sandbox = "boxed"
min_instances = 0
max_instances = 4
vcpus_per_instance = 4
memory_per_instance = "8GiB"
disk_per_instance = "100GiB"
target_actions_per_instance = 4
idle_timeout = "15s"
max_drain_wait = "1m"
boot_grace = "30s"

[provisioner.beta]
number = 2

[cache.disk]
number = 10

[signer.kms]
number = 11

[scheduler.stub]
number = 3

[database.memory]
number = 4

[sandbox.direct]
number = 5

[sandbox.boxed]
number = 6

[identity.providers.corp.token]
number = 7
"#;

fn parse(text: &str) -> Result<GangConfig<Plugins>, Box<dyn Error>> {
    Ok(text.parse()?)
}

fn assert_limits_port<T: gang_spi::ActionLimitsPort>() {}

#[test]
fn config_without_limits_parses_and_has_no_limits_section() -> Result<(), Box<dyn Error>> {
    let config = parse(CONFIG)?;
    assert!(config.limits.is_none());
    Ok(())
}

#[test]
fn config_with_a_limits_section_is_rejected() {
    let text = format!("{CONFIG}\n[limits.fake]\nnumber = 8\n");
    assert!(parse(&text).is_err());
}

#[test]
fn limits_type_still_implements_the_port() {
    assert_limits_port::<Limits>();
}
