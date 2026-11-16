//! The sign-in step shared by interactive and unattended setup.

use gang_client::{
    ClientError, ClientHome, InteractiveLogin, LocalBrowser, SessionState, same_server,
    stored_login,
};
use gang_types::{CaPin, ProviderName, Url};
use gang_wire::{Authentication, Server};
use serde_json::Value;

use crate::client_command::client_with_home;
use crate::errors::Result;
use crate::output::{Output, Record};
use crate::runtime;

/// The server and optional login choices selected for setup.
pub(crate) struct Target {
    /// Server to authenticate against.
    pub server: Url,
    /// Optional server CA pin.
    pub ca_pin: Option<CaPin>,
    /// Optional identity provider.
    pub provider: Option<ProviderName>,
}

/// The outcome of the sign-in step.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SignIn {
    /// The server does not require a credential.
    NotNeeded,
    /// An existing login supplied a usable token.
    Already,
    /// An interactive login obtained a token.
    SignedIn,
    /// A login is required but interaction was not permitted.
    Needed,
}

/// Performs the sign-in step for the current client home.
pub(crate) fn sign_in(
    out: &Output,
    target: &Target,
    info: &Server<Value>,
    interactive: bool,
) -> Result<SignIn> {
    if matches!(info.authentication, Authentication::None(_)) {
        return Ok(SignIn::NotNeeded);
    }
    let home = ClientHome::resolve(|key| std::env::var_os(key))?;
    sign_in_with_home(out, target, info, interactive, home)
}

/// Performs the sign-in step using a resolved home, without changing process environment.
fn sign_in_with_home(
    out: &Output,
    target: &Target,
    info: &Server<Value>,
    interactive: bool,
    home: ClientHome,
) -> Result<SignIn> {
    match &info.authentication {
        Authentication::None(_) => return Ok(SignIn::NotNeeded),
        Authentication::Unrecognized(_) => {
            return Err(ClientError::UnsupportedAuthentication.into());
        }
        Authentication::Token(_) => {}
    }
    let stored = stored_login(&home)?;
    let client = client_with_home(out, home)?;
    let matching = stored.as_ref().is_some_and(|login| {
        same_server(login.server.as_str(), &target.server)
            && target.ca_pin.is_none_or(|pin| login.ca_pin == Some(pin))
    });
    if matching
        && (target.provider.is_none()
            || client.stored_provider()?.as_ref() == target.provider.as_ref())
        && let SessionState::Active { principal } = runtime::block_on(client.session_state())?
    {
        let provider = client.stored_provider()?.ok_or(ClientError::NotLoggedIn)?;
        present(out, principal.as_deref(), &provider)?;
        return Ok(SignIn::Already);
    }
    if !interactive {
        return Ok(SignIn::Needed);
    }
    let browser = LocalBrowser::detect(|name| std::env::var_os(name));
    out.note(match browser {
        LocalBrowser::Available => "browser: a browser opens on this machine",
        LocalBrowser::Unavailable => {
            "device code: no browser on this machine (SSH session or no display)"
        }
    });
    let logged = runtime::block_on(client.login(&InteractiveLogin {
        server: &target.server,
        ca_pin: target.ca_pin.as_ref(),
        provider: target.provider.as_ref(),
        browser,
    }))?;
    let principal = runtime::block_on(client.whoami())?;
    drop(client);
    present(out, principal.as_deref(), &logged.provider)?;
    Ok(SignIn::SignedIn)
}

/// Reports the identified principal, or the provider when the session is anonymous.
fn present(out: &Output, principal: Option<&str>, provider: &ProviderName) -> Result<()> {
    let message = match principal {
        Some(principal) => format!("signed in as {principal}"),
        None => format!("signed in with {provider}"),
    };
    out.result(&Record::ok(message).subject("sign-in"))
}

#[cfg(test)]
mod tests {
    use gang_client::{ClientHome, same_server};
    use gang_types::{ProviderName, Url};
    use gang_wire::Server;
    use serde_json::{Value, json};
    use tempfile::TempDir;

    use super::{SignIn, Target, sign_in_with_home};
    use crate::output::Output;

    /// Creates a server document for the chosen authentication member.
    fn info(authentication: &Value) -> Server<Value> {
        serde_json::from_value(json!({
            "name": "server",
            "remoteExecutor": "grpcs://example.test:443",
            "remoteCache": "grpcs://example.test:443",
            "defaultIsa": "x86-64",
            "authentication": authentication
        }))
        .expect("valid server document")
    }

    /// Creates a target for the test server.
    fn target() -> Target {
        Target {
            server: Url::parse("https://example.test").expect("valid url"),
            ca_pin: None,
            provider: None,
        }
    }

    /// Stores a reusable token for a server whose scheme fails before any network I/O.
    fn stored_session(home: &ClientHome) -> Target {
        let server = "ftp://example.test/";
        std::fs::write(
            home.session_file(),
            serde_json::to_vec(&json!({
                "server": server,
                "ca_pin": null,
                "provider": "google",
                "kind": {"kind": "device"}
            }))
            .expect("serialize session"),
        )
        .expect("write session");
        std::fs::write(
            home.token_file(),
            serde_json::to_vec(&json!({
                "token": "cached-token",
                "issued_at": 0,
                "expires_at": 4_102_444_800_000_u64
            }))
            .expect("serialize token"),
        )
        .expect("write token");
        Target {
            server: Url::parse(server).expect("valid url"),
            ca_pin: None,
            provider: None,
        }
    }

    #[test]
    fn differing_provider_requires_new_login() {
        let dir = TempDir::new().expect("temporary home");
        let home = ClientHome::try_new(dir.path()).expect("client home");
        let mut target = stored_session(&home);
        target.provider = Some(ProviderName::try_new("other").expect("valid provider"));
        let (out, _, _) = Output::captured();
        let result = sign_in_with_home(
            &out,
            &target,
            &info(&json!({"token": {"login": {}}})),
            false,
            home,
        );
        assert!(
            matches!(result, Ok(SignIn::Needed)),
            "mismatched provider must require a new login"
        );
    }

    #[test]
    fn differing_pin_requires_new_login() {
        let dir = TempDir::new().expect("temporary home");
        let home = ClientHome::try_new(dir.path()).expect("client home");
        let mut target = stored_session(&home);
        target.ca_pin = Some(
            format!("sha256:{}", "aa".repeat(32))
                .parse()
                .expect("valid pin"),
        );
        let (out, _, _) = Output::captured();
        let result = sign_in_with_home(
            &out,
            &target,
            &info(&json!({"token": {"login": {}}})),
            false,
            home,
        );
        assert!(
            matches!(result, Ok(SignIn::Needed)),
            "mismatched CA pin must require a new login"
        );
    }

    #[test]
    fn no_authentication_needs_no_login() {
        let dir = TempDir::new().expect("temporary home");
        let home = ClientHome::try_new(dir.path()).expect("client home");
        let (out, _, _) = Output::captured();
        assert_eq!(
            sign_in_with_home(&out, &target(), &info(&json!({"none": {}})), false, home)
                .expect("sign-in decision"),
            SignIn::NotNeeded
        );
    }

    #[test]
    fn noninteractive_setup_without_login_needs_sign_in() {
        let dir = TempDir::new().expect("temporary home");
        let home = ClientHome::try_new(dir.path()).expect("client home");
        let (out, _, _) = Output::captured();
        assert_eq!(
            sign_in_with_home(
                &out,
                &target(),
                &info(&json!({"token": {"login": {}}})),
                false,
                home,
            )
            .expect("sign-in decision"),
            SignIn::Needed
        );
    }

    #[test]
    fn server_comparison_ignores_trailing_slashes() {
        let a = Url::parse("https://example.test/path").expect("valid url");
        let b = Url::parse("https://example.test/path/").expect("valid url");
        let other = Url::parse("https://other.test/path").expect("valid url");
        assert!(same_server(a.as_str(), &b));
        assert!(!same_server(a.as_str(), &other));
    }
}
