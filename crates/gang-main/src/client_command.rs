//! The client commands: `gang login`, `gang revoke`, and `gang restore`, run against the client
//! directory of the current user or CI runner.

use std::process::ExitCode;
use std::time::Duration;

use gang_client::{Client, ClientHome, InteractiveLogin, LocalBrowser, LoggedIn, WorkloadLogin};
use gang_clock::{Clock, SystemClock, Timestamp};
use gang_types::CertSerial;
use gang_wire::{Certificate, RevokePrincipalResponse, name};

use crate::cli::{LoginArgs, LoginMode, RestoreArgs, RevokeArgs, RevokeTarget};
use crate::errors::Result;
use crate::login_providers::BuiltInProviders;
use crate::output::{Line, Output, Record};
use crate::runtime;
use crate::terminal_console::TerminalConsole;

/// Builds the client from the environment and its configured options; its console writes through
/// `out`.
pub(crate) fn client(
    out: &Output,
) -> Result<Client<BuiltInProviders, TerminalConsole, SystemClock>> {
    let home = ClientHome::resolve(|key| std::env::var_os(key))?;
    let options = home.options()?;
    let providers = BuiltInProviders::new(home.session_file());
    let console = TerminalConsole::new(&options, out.clone());
    Ok(Client::new(
        home,
        options,
        providers,
        console,
        SystemClock::new(),
    ))
}

/// Logs in and stores the client session.
///
/// The client, and with it the console and its wait line, is dropped before the outcome is
/// printed so the outcome replaces the wait line.
pub(crate) fn login(args: LoginArgs, out: &Output) -> Result<ExitCode> {
    let logged = match args.mode()? {
        LoginMode::Interactive {
            server,
            ca_pin,
            provider,
        } => {
            let client = client(out)?;
            runtime::block_on(client.login(&InteractiveLogin {
                server: &server,
                provider: provider.as_ref(),
                ca_pin: ca_pin.as_ref(),
                browser: LocalBrowser::detect(|name| std::env::var_os(name)),
            }))?
        }
        LoginMode::Workload {
            server,
            ca_pin,
            provider,
            proof,
        } => {
            let client = client(out)?;
            runtime::block_on(client.login_workload(WorkloadLogin {
                server: &server,
                provider: &provider,
                source: proof,
                ca_pin: ca_pin.as_ref(),
            }))?
        }
    };
    present_login(out, &logged, SystemClock::new().wall())?;
    Ok(ExitCode::SUCCESS)
}

/// Revokes one certificate, or every certificate of a principal and the principal itself.
pub(crate) fn revoke(args: RevokeArgs, out: &Output) -> Result<ExitCode> {
    let target = args.target()?;
    let client = client(out)?;
    match target {
        RevokeTarget::Certificate(serial) => {
            let certificate = runtime::block_on(client.revoke_certificate(&serial))?;
            present_revoked_certificate(out, &certificate)?;
        }
        RevokeTarget::Principal(identity) => {
            let response = runtime::block_on(client.revoke_principal(&identity))?;
            present_revoked_principal(out, &response)?;
        }
    }
    Ok(ExitCode::SUCCESS)
}

/// Restores a principal that was revoked, so it may receive tokens and certificates again.
pub(crate) fn restore(args: &RestoreArgs, out: &Output) -> Result<ExitCode> {
    let client = client(out)?;
    let principal = runtime::block_on(client.restore_principal(&args.principal))?;
    present_restored(out, &principal)?;
    Ok(ExitCode::SUCCESS)
}

/// Prints the outcome of a login: the provider and the validity left at `now`.
fn present_login(out: &Output, logged: &LoggedIn, now: Timestamp) -> Result<()> {
    out.result(&logged_in_record(logged, now))
}

/// The record `gang login` prints after a successful login.
fn logged_in_record(logged: &LoggedIn, now: Timestamp) -> Record {
    Record::ok(
        Line::new()
            .plain(format!("logged in with {} ", logged.provider))
            .muted(format!(
                "· token valid for {}",
                remaining_validity(logged.expires_at, now)
            )),
    )
}

/// Formats the time left from `now` until `expires_at` in whole seconds, in the configuration
/// duration format. Sub-second remainders are dropped so the output never shows milliseconds.
fn remaining_validity(expires_at: Timestamp, now: Timestamp) -> String {
    let remaining = expires_at.saturating_duration_since(now);
    gang_types::duration::display(Duration::from_secs(remaining.as_secs())).to_string()
}

/// Prints the revocation of one certificate.
fn present_revoked_certificate(out: &Output, certificate: &Certificate) -> Result<()> {
    out.result(&certificate_record(certificate)?)
}

/// Prints the revocation of a principal: one record per revoked certificate, then one for the
/// principal.
fn present_revoked_principal(out: &Output, response: &RevokePrincipalResponse) -> Result<()> {
    for certificate in &response.certificates {
        out.result(&certificate_record(certificate)?)?;
    }
    out.result(&Record::ok(format!(
        "revoked principal {}:{}",
        response.principal.provider, response.principal.principal
    )))
}

/// The record `gang revoke` prints for one revoked certificate.
fn certificate_record(certificate: &Certificate) -> Result<Record> {
    let serial: CertSerial = name::parse::<name::Certificates>(&certificate.name)?;
    Ok(Record::ok(format!("revoked certificate {serial}")))
}

/// Prints the principal `gang restore` restored.
fn present_restored(out: &Output, principal: &gang_wire::Principal) -> Result<()> {
    out.result(&Record::ok(format!(
        "restored principal {}:{}",
        principal.provider, principal.principal
    )))
}

#[cfg(test)]
mod tests {
    use gang_types::{Principal, ProviderName};

    use super::*;

    fn provider() -> ProviderName {
        ProviderName::try_new("example-idp").expect("valid provider")
    }

    fn wire_principal(id: &str) -> gang_wire::Principal {
        gang_wire::Principal {
            name: name::of::<name::Principals>(
                &format!("example-idp:{id}")
                    .parse::<gang_types::Identity>()
                    .expect("valid identity"),
            ),
            provider: provider(),
            principal: Principal::try_new(id).expect("valid principal"),
            create_time: Timestamp::from_unix_millis(1_000),
            last_seen_time: Timestamp::from_unix_millis(2_000),
            revoke_time: Some(Timestamp::from_unix_millis(4_000)),
        }
    }

    fn certificate(hex: &str) -> Certificate {
        Certificate {
            name: name::of::<name::Certificates>(
                &hex.parse::<CertSerial>().expect("valid certificate serial"),
            ),
            principal: name::of::<name::Principals>(
                &"example-idp:user-1"
                    .parse::<gang_types::Identity>()
                    .expect("valid identity"),
            ),
            create_time: Timestamp::from_unix_millis(1_000),
            expire_time: Timestamp::from_unix_millis(9_000),
            revoke_time: Some(Timestamp::from_unix_millis(4_000)),
            pem_certificate: None,
            ca_certificate: None,
        }
    }

    fn logged_in(valid_for_millis: u64, now: u64) -> (LoggedIn, Timestamp) {
        (
            LoggedIn {
                provider: provider(),
                expires_at: Timestamp::from_unix_millis(now + valid_for_millis),
            },
            Timestamp::from_unix_millis(now),
        )
    }

    #[test]
    fn login_names_the_provider_and_remaining_validity() {
        let (out, stdout, stderr) = Output::captured();
        let (logged, now) = logged_in(7 * 86_400_000, 1_000);
        present_login(&out, &logged, now).expect("login is printed");
        assert_eq!(
            stdout.text(),
            "    ok  logged in with example-idp · token valid for 7d\n"
        );
        assert_eq!(stderr.text(), "");
    }

    #[test]
    fn login_validity_drops_the_sub_second_remainder() {
        let (out, stdout, _) = Output::captured();
        let (logged, now) = logged_in(7 * 86_400_000 - 775, 1_000);
        present_login(&out, &logged, now).expect("login is printed");
        let text = stdout.text();
        assert_eq!(
            text,
            "    ok  logged in with example-idp · token valid for 604799s\n"
        );
        assert!(!text.contains("ms"), "{text}");
    }

    #[test]
    fn expired_token_states_no_remaining_validity() {
        let (out, stdout, _) = Output::captured();
        let logged = LoggedIn {
            provider: provider(),
            expires_at: Timestamp::from_unix_millis(1_000),
        };
        present_login(&out, &logged, Timestamp::from_unix_millis(5_000)).expect("login is printed");
        assert_eq!(
            stdout.text(),
            "    ok  logged in with example-idp · token valid for 0s\n"
        );
    }

    #[test]
    fn certificate_revocation_prints_the_serial() {
        let (out, stdout, stderr) = Output::captured();
        present_revoked_certificate(&out, &certificate("ff")).expect("a certificate name");
        assert_eq!(stdout.text(), "    ok  revoked certificate ff\n");
        assert_eq!(stderr.text(), "");
    }

    #[test]
    fn certificate_revocation_rejects_a_name_outside_the_collection() {
        let (out, stdout, _) = Output::captured();
        let mut revoked = certificate("ff");
        revoked.name = "principals/00ff".to_owned();
        assert!(present_revoked_certificate(&out, &revoked).is_err());
        assert_eq!(stdout.text(), "");
    }

    #[test]
    fn principal_revocation_prints_the_certificates_then_the_principal() {
        let (out, stdout, _) = Output::captured();
        let response = RevokePrincipalResponse {
            principal: wire_principal("user-1"),
            certificates: vec![certificate("ff"), certificate("0a0b")],
        };
        present_revoked_principal(&out, &response).expect("certificate names");
        assert_eq!(
            stdout.text(),
            [
                "    ok  revoked certificate ff\n",
                "    ok  revoked certificate 0a0b\n",
                "    ok  revoked principal example-idp:user-1\n",
            ]
            .concat()
        );
    }

    #[test]
    fn principal_revocation_without_certificates_prints_only_the_principal() {
        let (out, stdout, _) = Output::captured();
        let response = RevokePrincipalResponse {
            principal: wire_principal("user-1"),
            certificates: Vec::new(),
        };
        present_revoked_principal(&out, &response).expect("no certificate names");
        assert_eq!(
            stdout.text(),
            "    ok  revoked principal example-idp:user-1\n"
        );
    }

    #[test]
    fn restore_prints_the_restored_principal() {
        let (out, stdout, _) = Output::captured();
        present_restored(&out, &wire_principal("user-1")).expect("restore is printed");
        assert_eq!(
            stdout.text(),
            "    ok  restored principal example-idp:user-1\n"
        );
    }
}
