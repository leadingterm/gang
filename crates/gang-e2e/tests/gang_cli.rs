//! The argv, program, and environment each `gang` command builder runs with, and the CA pin read
//! from a server log.

use std::error::Error;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;

use clap::Parser;
use gang_e2e::GangCli;
use gang_main::cli::{Cli, Command as GangCommand};
use gang_types::{CaPin, Identity, ProviderName, Url};

fn cli() -> GangCli {
    GangCli::new(
        PathBuf::from("gang"),
        PathBuf::from("gang.toml"),
        PathBuf::from("home"),
    )
}

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn server_url() -> TestResult<Url> {
    Ok(Url::parse("https://gang.example:8443")?)
}

fn pin(hex: &str) -> TestResult<CaPin> {
    Ok(format!("sha256:{hex}").parse()?)
}

fn environment(command: &Command, key: &str) -> Option<PathBuf> {
    command
        .get_envs()
        .find(|(name, _)| *name == OsStr::new(key))
        .and_then(|(_, value)| value.map(PathBuf::from))
}

const FIRST_PIN: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

#[test]
fn bazelrc_runs_in_the_workspace() -> TestResult {
    let server = server_url()?;
    let command = cli().bazelrc(&server, Some(&pin(FIRST_PIN)?), Path::new("workspace"));

    assert_eq!(command.get_current_dir(), Some(Path::new("workspace")));
    Ok(())
}

#[test]
fn every_command_keeps_its_credentials_under_home() -> TestResult {
    let server = server_url()?;
    let commands = [
        cli().server(),
        cli().login_workload(&server, &pin(FIRST_PIN)?, Path::new("token.txt")),
        cli().bazelrc(&server, None, Path::new("workspace")),
        cli().get(),
        cli().revoke_principal("e2e:developer"),
        cli().restore_principal("e2e:developer"),
    ];

    for command in &commands {
        assert_eq!(environment(command, "HOME"), Some(PathBuf::from("home")));
        assert_eq!(
            environment(command, "XDG_CONFIG_HOME"),
            Some(PathBuf::from("home"))
        );
        assert_eq!(
            command
                .get_envs()
                .find(|(key, _)| *key == "RUNNER_TEMP")
                .map(|(_, value)| value),
            Some(None)
        );
    }
    Ok(())
}

/// The arguments `command` runs `gang` with, as text.
fn argv(command: &Command) -> Vec<String> {
    command
        .get_args()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect()
}

/// Parses the arguments of `command` the way the `gang` binary does, and returns its subcommand.
fn parsed(command: &Command) -> TestResult<GangCommand> {
    let cli = Cli::try_parse_from(std::iter::once("gang".to_owned()).chain(argv(command)))?;
    Ok(cli.command)
}

fn provider(name: &str) -> TestResult<ProviderName> {
    Ok(ProviderName::try_new(name)?)
}

fn identity(text: &str) -> TestResult<Identity> {
    Ok(text.parse()?)
}

#[test]
fn server_argv_is_what_gang_parses() -> TestResult {
    match parsed(&cli().server())? {
        GangCommand::Server(args) => assert_eq!(args.config, PathBuf::from("gang.toml")),
        other => panic!("expected `gang server`, got {other:?}"),
    }
    Ok(())
}

#[test]
fn login_workload_argv_is_what_gang_parses() -> TestResult {
    let server = server_url()?;
    let ca_pin = pin(FIRST_PIN)?;
    let command = cli().login_workload(&server, &ca_pin, Path::new("token.txt"));
    match parsed(&command)? {
        GangCommand::Login(args) => {
            assert_eq!(args.server, server);
            assert_eq!(args.ca_pin, Some(ca_pin));
            assert_eq!(args.provider, Some(provider("e2e")?));
            assert!(args.workload);
            assert_eq!(args.proof.token, Some(PathBuf::from("token.txt")));
            assert!(args.proof.token_command.is_none());
        }
        other => panic!("expected `gang login`, got {other:?}"),
    }
    Ok(())
}

#[test]
fn bazelrc_argv_is_what_gang_parses() -> TestResult {
    let server = server_url()?;
    let ca_pin = pin(FIRST_PIN)?;
    let command = cli().bazelrc(&server, Some(&ca_pin), Path::new("workspace"));
    match parsed(&command)? {
        GangCommand::Bazelrc(args) => {
            assert_eq!(args.server, server);
            assert_eq!(args.ca_pin, Some(ca_pin));
            assert_eq!(args.helper, "gang");
        }
        other => panic!("expected `gang bazelrc`, got {other:?}"),
    }

    let without_pin = cli().bazelrc(&server, None, Path::new("workspace"));
    match parsed(&without_pin)? {
        GangCommand::Bazelrc(args) => assert!(args.ca_pin.is_none()),
        other => panic!("expected `gang bazelrc`, got {other:?}"),
    }
    Ok(())
}

#[test]
fn revoke_principal_argv_is_what_gang_parses() -> TestResult {
    match parsed(&cli().revoke_principal("e2e:developer"))? {
        GangCommand::Revoke(args) => {
            assert_eq!(args.principal, Some(identity("e2e:developer")?));
            assert!(args.serial.is_none());
        }
        other => panic!("expected `gang revoke`, got {other:?}"),
    }
    Ok(())
}

#[test]
fn restore_principal_argv_is_what_gang_parses() -> TestResult {
    match parsed(&cli().restore_principal("e2e:developer"))? {
        GangCommand::Restore(args) => assert_eq!(args.principal, identity("e2e:developer")?),
        other => panic!("expected `gang restore`, got {other:?}"),
    }
    Ok(())
}

#[test]
fn get_argv_is_what_gang_parses() -> TestResult {
    match parsed(&cli().get())? {
        GangCommand::Get => {}
        other => panic!("expected `gang get`, got {other:?}"),
    }
    Ok(())
}
