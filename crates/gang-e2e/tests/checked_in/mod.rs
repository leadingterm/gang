//! The checked-in-configs scenario against simulated configurations: compliant ones pass, and each
//! flaw fails exactly its check.

use super::{assert_compliant, failures};
use gang_e2e::{Check, Ran, Shipped, ShippedConfig, Start, checked_in_configs, names};

behaviour! {
    /// The behaviour of the simulated checked-in configurations.
    struct Behaviour {
        /// Which configurations are listed: every checked-in configuration or only the AWS one.
        listing: Listing {
            /// Every checked-in configuration is listed.
            Found,
            /// Only the AWS configuration is listed.
            OnlyAws,
        },
        /// How `gang.local.toml` starts.
        local_start: LocalStart {
            /// It announces itself.
            Announces,
            /// It exits at once with a configuration error.
            Exits,
        },
        /// How `gang.local.plaintext.toml` starts.
        plaintext_start: PlaintextStart {
            /// It announces itself, its plaintext line last.
            Announces,
            /// It prints its listening lines and no startup line.
            Silent,
        },
        /// How `gang.aws.toml` gets through configuration loading.
        aws_loading: AwsLoading {
            /// It passes validation and stops at its first AWS call.
            PastValidation,
            /// It is rejected by configuration validation.
            Invalid,
        },
    }
}

/// The checked-in configurations of the simulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SimulatedShipped {
    /// The behaviour the simulation follows.
    behaviour: Behaviour,
}

impl SimulatedShipped {
    /// A simulation of the checked-in configurations that behaves as `behaviour` says.
    const fn new(behaviour: Behaviour) -> Self {
        Self { behaviour }
    }
}

/// The startup lines of a local server whose listeners are bound.
fn listening() -> Vec<String> {
    vec![
        "listening: public 127.0.0.1:41000".to_owned(),
        "listening: worker 127.0.0.1:41001".to_owned(),
    ]
}

/// The lines of `gang.local.toml` through its startup line.
fn local_lines() -> Vec<String> {
    let mut lines = listening();
    lines.push(format!("ca-pin: sha256:{}", "ab".repeat(32)));
    lines
}

/// The lines of `gang.local.plaintext.toml` through its startup line.
fn plaintext_lines() -> Vec<String> {
    let mut lines = listening();
    lines.push("plaintext: http://127.0.0.1:41000/".to_owned());
    lines
}

/// The failure of a server whose configuration `field` is not known.
fn invalid_config(field: &str) -> Ran {
    Ran {
        code: Some(1),
        stdout: String::new(),
        stderr: format!(
            "  fail  gang server: invalid configuration /sim/gang.toml: unknown field `{field}`\n"
        ),
    }
}

/// The failure of an AWS configuration that passes validation and then has no KMS credentials.
fn kms_failure() -> Ran {
    Ran {
        code: Some(1),
        stdout: String::new(),
        stderr: "  fail  gang server: the kms signer failed: no credentials in the environment\n"
            .to_owned(),
    }
}

/// The checked-in configurations, sorted by name, as the simulation lists them.
fn shipped_configs() -> [ShippedConfig; 3] {
    [
        ShippedConfig {
            name: "gang.aws.toml".to_owned(),
            needs_aws: true,
        },
        ShippedConfig {
            name: "gang.local.plaintext.toml".to_owned(),
            needs_aws: false,
        },
        ShippedConfig {
            name: "gang.local.toml".to_owned(),
            needs_aws: false,
        },
    ]
}

impl Shipped for SimulatedShipped {
    fn configs(&self) -> gang_e2e::Result<Vec<ShippedConfig>> {
        let all = shipped_configs();
        Ok(match self.behaviour.listing {
            Listing::Found => all.into_iter().collect(),
            Listing::OnlyAws => all.into_iter().filter(|config| config.needs_aws).collect(),
        })
    }

    fn start(&self, config: &ShippedConfig) -> gang_e2e::Result<Start> {
        match config.name.as_str() {
            "gang.local.toml" => Ok(match self.behaviour.local_start {
                LocalStart::Announces => Start::Announced(local_lines()),
                LocalStart::Exits => Start::Exited(invalid_config("listen")),
            }),
            "gang.local.plaintext.toml" => Ok(match self.behaviour.plaintext_start {
                PlaintextStart::Announces => Start::Announced(plaintext_lines()),
                PlaintextStart::Silent => Start::Announced(listening()),
            }),
            "gang.aws.toml" => Ok(Start::Exited(match self.behaviour.aws_loading {
                AwsLoading::PastValidation => kms_failure(),
                AwsLoading::Invalid => invalid_config("region"),
            })),
            _ => Err(gang_e2e::E2eError::system("unknown config")),
        }
    }
}

/// The verdicts of the checked-in-configs scenario against simulated configurations that behave
/// as `behaviour` says.
fn checked_in_checks(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    checked_in_configs(&SimulatedShipped::new(behaviour))
}

/// The check that `gang.local.toml` starts `gang server`.
fn local_starts() -> String {
    names::checked_in_configs::starts("gang.local.toml")
}

/// The check that `gang.local.plaintext.toml` starts `gang server`.
fn plaintext_starts() -> String {
    names::checked_in_configs::starts("gang.local.plaintext.toml")
}

/// The check that `gang.aws.toml` passes configuration loading.
fn aws_loads() -> String {
    names::checked_in_configs::loads("gang.aws.toml")
}

#[test]
fn checked_in_configs_are_compliant() -> gang_e2e::Result<()> {
    let checks = checked_in_checks(Behaviour::COMPLIANT)?;
    assert_compliant(&checks);
    assert_eq!(checks.len(), 4);
    Ok(())
}

flaw!(
    listing_flaw: checked_in_checks(listing = Listing::OnlyAws)
    => only [names::checked_in_configs::FOUND]
);
flaw!(
    local_start_flaw: checked_in_checks(local_start = LocalStart::Exits)
    => only [local_starts()]
);
flaw!(
    plaintext_start_flaw: checked_in_checks(plaintext_start = PlaintextStart::Silent)
    => only [plaintext_starts()]
);
flaw!(
    aws_loading_flaw: checked_in_checks(aws_loading = AwsLoading::Invalid)
    => only [aws_loads()]
);
