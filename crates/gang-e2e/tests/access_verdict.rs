//! Unit tests of the identity scenario's verdicts over real-shaped `gang` output, HTTP answers and
//! the rendered configuration.

use gang_config::GangConfig;
use gang_e2e::{
    ActionTools, Answer, BROWSER_SUBJECT, ConfigTemplate, CookieValue, DASHBOARD_CLIENT_ID,
    E2eError, ExpectedCookie, IdentityKnobs, MaxAge, Placement, Ran, RefreshOutcome, RefreshRecord,
    RowStatus, SESSION_MAX_AGE, SIGN_IN_SUBJECT, Sandbox, SetCookie, StatusRow, Transport, Variant,
    authorization_request, consent_free, cookie_set, cross_site_renewal_refused, failure_reported,
    login_cookie_name, offline_requested, query_value, rechecked, redirected_to, renewal_cleared,
    renewal_cookie, renewal_kept, sealed_refused, shared_grant, status_rows,
};
use gang_testkit::OpaquePlugins;
use gang_types::Url;
use toml::{Table, Value};

/// Reads the local-mode configuration supplied in the test's runfiles.
fn base_config() -> gang_e2e::Result<String> {
    let path = std::env::var("GANG_E2E_BASE_CONFIG")
        .map_err(|error| E2eError::system(format!("reading GANG_E2E_BASE_CONFIG: {error}")))?;
    std::fs::read_to_string(&path)
        .map_err(E2eError::io(format!("reading the base config at {path}")))
}

/// The provider the identity scenario signs in with.
const PROVIDER: &str = "e2e";

/// The worker launcher argv the tests pass to `vary`.
fn launcher() -> Vec<String> {
    ["systemd-run", "--user", "--scope", "--"]
        .map(String::from)
        .to_vec()
}

/// Pinned action tools under the Nix store, as the bwrap sandbox requires.
fn tools() -> ActionTools {
    ActionTools {
        shell: std::path::PathBuf::from("/nix/store/aaa-bash/bin/bash"),
        path: "/nix/store/aaa-bash/bin:/nix/store/bbb-coreutils/bin".to_owned(),
        action_bin: std::path::PathBuf::from("/nix/store/ddd-action-tools/bin"),
        bwrap: Some(std::path::PathBuf::from(
            "/nix/store/ccc-bubblewrap/bin/bwrap",
        )),
        glibc_tool: None,
        glibc_loader: Vec::new(),
    }
}

/// An answer with `status` and `headers` (lower-case names), and an empty body.
fn answer(status: u16, headers: &[(&str, &str)]) -> Answer {
    Answer {
        status,
        headers: headers
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect(),
        body: Vec::new(),
    }
}

/// An answer of status 200 whose Set-Cookie headers are `cookies`, in order.
fn with_cookies(cookies: &[&str]) -> Answer {
    let headers: Vec<(&str, &str)> = cookies.iter().map(|value| ("set-cookie", *value)).collect();
    answer(200, &headers)
}

/// The command outcome with exit `code` and standard error `stderr`.
fn outcome(code: Option<i32>, stderr: &str) -> Ran {
    Ran {
        code,
        stdout: String::new(),
        stderr: stderr.to_owned(),
    }
}

/// The `gang revoke` failure the server's 403 produces, as `gang` prints it.
const REVOKE_STDERR: &str = "  fail  gang revoke: the server answered 403: missing permission \
    principals.revoke\n        ↳ detail\n   fix  ask an operator for a role that grants the \
    permission the action needs\n";

#[test]
fn failure_reported_needs_exit_one_and_both_lines() {
    const SUBJECT: &str = "gang revoke";
    const MESSAGE: &str = "the server answered 403: missing permission principals.revoke";
    const FIX: &str = "ask an operator for a role that grants the permission the action needs";

    assert!(
        failure_reported(&outcome(Some(1), REVOKE_STDERR), SUBJECT, MESSAGE, FIX).is_ok(),
        "exit 1 with both lines is a reported failure"
    );
    assert!(
        failure_reported(&outcome(Some(0), REVOKE_STDERR), SUBJECT, MESSAGE, FIX).is_err(),
        "exit 0 is not a reported failure"
    );
    let mut without_fix = String::new();
    for line in REVOKE_STDERR
        .lines()
        .filter(|line| !line.starts_with("   fix"))
    {
        without_fix.push_str(line);
        without_fix.push('\n');
    }
    assert!(
        failure_reported(&outcome(Some(1), &without_fix), SUBJECT, MESSAGE, FIX).is_err(),
        "a missing fix line is not a reported failure"
    );
    assert!(
        failure_reported(
            &outcome(Some(1), REVOKE_STDERR),
            "gang restore",
            MESSAGE,
            FIX
        )
        .is_err(),
        "a fail line with another subject is not a reported failure"
    );
    let mut trailing = String::new();
    for line in REVOKE_STDERR.lines() {
        trailing.push_str(line);
        trailing.push_str("   \n");
    }
    assert!(
        failure_reported(&outcome(Some(1), &trailing), SUBJECT, MESSAGE, FIX).is_ok(),
        "trailing spaces on the lines are ignored"
    );
    let coloured = format!("\x1b[31mfail\x1b[0m\t{SUBJECT}: {MESSAGE}\n\tfix\t{FIX}\n");
    assert!(failure_reported(&outcome(Some(1), &coloured), SUBJECT, MESSAGE, FIX).is_ok());
    let wrong_record = format!("fail  {SUBJECT}: {MESSAGE}\nwarn  another  warning\nfix  {FIX}\n");
    assert!(failure_reported(&outcome(Some(1), &wrong_record), SUBJECT, MESSAGE, FIX).is_err());
}

#[test]
fn set_cookie_parses_name_value_and_attributes() {
    const LOGIN: &str =
        "__Host-gang_login=abc; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=600";
    let cookie = SetCookie::parse(LOGIN).expect("the login cookie parses");
    assert_eq!(cookie.name, "__Host-gang_login");
    assert_eq!(cookie.value, "abc");
    assert_eq!(cookie.attribute("samesite"), Some(Some("Lax")));
    assert_eq!(cookie.attribute("httponly"), Some(None));
    assert_eq!(cookie.attribute("domain"), None);
    assert_eq!(SetCookie::parse("no-equals"), None);
}

#[test]
fn cookie_set_checks_every_attribute_of_the_expected_cookie() {
    const LOGIN: &str =
        "__Host-gang_login=abc; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=600";
    const SESSION: &str = "gang_session=x; HttpOnly; SameSite=Strict; Path=/; Max-Age=600";
    const CLEAR: &str =
        "__Host-gang_session=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0";
    let login = ExpectedCookie {
        name: "__Host-gang_login",
        value: CookieValue::Exactly("abc"),
        same_site: "Lax",
        max_age: MaxAge::Exactly(600),
    };
    assert!(
        cookie_set(&with_cookies(&[LOGIN]), &login).is_ok(),
        "the login cookie as the sign-in sets it is accepted"
    );
    let changed = [
        LOGIN.replace("SameSite=Lax", "SameSite=Strict"),
        LOGIN.replace("HttpOnly; ", ""),
        LOGIN.replace("Secure; ", ""),
        LOGIN.replace("Max-Age=600", "Max-Age=60"),
        LOGIN.replace("=abc;", "=abd;"),
        LOGIN.replace("Path=/;", "Path=/ui;"),
    ];
    for header in &changed {
        assert!(
            cookie_set(&with_cookies(&[header.as_str()]), &login).is_err(),
            "a single change is refused: {header}"
        );
    }
    assert!(
        cookie_set(&with_cookies(&[LOGIN, LOGIN]), &login).is_err(),
        "two Set-Cookie headers of the same name are refused"
    );
    assert!(
        cookie_set(&with_cookies(&[]), &login).is_err(),
        "no Set-Cookie is refused"
    );

    let session = ExpectedCookie {
        name: "gang_session",
        value: CookieValue::NonEmpty,
        same_site: "Strict",
        max_age: MaxAge::Positive,
    };
    assert!(
        cookie_set(&with_cookies(&[SESSION]), &session).is_ok(),
        "a plain session cookie is accepted"
    );
    assert!(
        cookie_set(
            &with_cookies(&[&SESSION.replace("HttpOnly; ", "HttpOnly; Secure; ")]),
            &session
        )
        .is_err(),
        "a non-__Host- cookie with Secure is refused"
    );

    let cleared = ExpectedCookie {
        name: "__Host-gang_session",
        value: CookieValue::Exactly(""),
        same_site: "Strict",
        max_age: MaxAge::Exactly(0),
    };
    assert!(
        cookie_set(&with_cookies(&[CLEAR]), &cleared).is_ok(),
        "a clearing cookie with an empty value and Max-Age=0 is accepted"
    );
    let not_clearing = [
        ExpectedCookie {
            value: CookieValue::NonEmpty,
            ..cleared
        },
        ExpectedCookie {
            max_age: MaxAge::Positive,
            ..cleared
        },
    ];
    for expected in &not_clearing {
        assert!(
            cookie_set(&with_cookies(&[CLEAR]), expected).is_err(),
            "a clearing cookie is refused against {expected:?}"
        );
    }
}

#[test]
fn authorization_request_accepts_the_pkce_request_and_names_each_defect() {
    const CLIENT: &str = "gang-e2e-dashboard";
    const CALLBACK: &str = "https://127.0.0.1:8443/ui/callback";
    const AUTHORIZE: &str = "http://127.0.0.1:9/authorize?response_type=code\
        &client_id=gang-e2e-dashboard\
        &redirect_uri=https%3A%2F%2F127.0.0.1%3A8443%2Fui%2Fcallback\
        &scope=openid+email&state=s1&nonce=n1\
        &code_challenge=E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM\
        &code_challenge_method=S256&login_hint=operator";

    assert!(
        authorization_request(AUTHORIZE, CLIENT, CALLBACK).is_ok(),
        "the PKCE authorization request is accepted"
    );
    let changed = [
        AUTHORIZE.replace("response_type=code", "response_type=token"),
        AUTHORIZE.replace("scope=openid+email", "scope=email"),
        AUTHORIZE.replace("code_challenge_method=S256", "code_challenge_method=plain"),
        AUTHORIZE.replace(
            "&code_challenge=E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
            "",
        ),
        AUTHORIZE.replace("nonce=n1", "nonce="),
        format!("{AUTHORIZE}&state=s2"),
        AUTHORIZE.replace("client_id=gang-e2e-dashboard", "client_id=other-client"),
        AUTHORIZE.replace(
            "redirect_uri=https%3A%2F%2F127.0.0.1%3A8443%2Fui%2Fcallback",
            "redirect_uri=https%3A%2F%2F127.0.0.1%3A8443%2Fui%2Fother",
        ),
    ];
    for location in &changed {
        assert!(
            authorization_request(location, CLIENT, CALLBACK).is_err(),
            "a single change is refused: {location}"
        );
    }
}

#[test]
fn redirected_to_needs_a_302_to_the_exact_location() {
    const TARGET: &str = "/ui/error?reason=failed";
    assert!(
        redirected_to(&answer(302, &[("location", TARGET)]), TARGET).is_ok(),
        "a 302 to the location is accepted"
    );
    assert!(
        redirected_to(&answer(303, &[("location", TARGET)]), TARGET).is_err(),
        "a 303 is refused"
    );
    assert!(
        redirected_to(&answer(302, &[("location", "/ui")]), TARGET).is_err(),
        "another location is refused"
    );
    assert!(
        redirected_to(&answer(302, &[]), TARGET).is_err(),
        "a 302 without a location is refused"
    );
}

#[test]
fn query_value_reads_the_first_value_of_a_key() {
    const AUTHORIZE: &str = "http://127.0.0.1:9/authorize?response_type=code&state=s1&nonce=n1";
    assert_eq!(query_value(AUTHORIZE, "state").as_deref(), Some("s1"));
    assert_eq!(query_value(AUTHORIZE, "missing"), None);
    assert_eq!(query_value("not a url", "state"), None);
}

#[test]
fn doctor_row_reads_the_row_its_fix_and_its_summary() {
    const OUTPUT: &str = "    ok  server             https://127.0.0.1:8443 answers
    ok  credential_helper  127.0.0.1=/opt/gang/bin/gang
  fail  credentials        the server refused the token of /opt/gang/bin/gang
        ↳ principal revoked
   fix  log in again with `gang login --server https://127.0.0.1:8443`; if it still refuses, \
ask the server's administrator whether your principal was revoked
    ok  clock              within 30s of the server
";

    let rows = status_rows(OUTPUT);
    let row = |check| rows.iter().find(|row| row.subject == check).cloned();
    assert_eq!(
        row("credentials"),
        Some(StatusRow {
            status: RowStatus::Fail,
            subject: "credentials",
            detail: "the server refused the token of /opt/gang/bin/gang",
            fix: Some(
                "log in again with `gang login --server https://127.0.0.1:8443`; if it still \
                 refuses, ask the server's administrator whether your principal was revoked"
            ),
        })
    );
    assert_eq!(
        row("credential_helper"),
        Some(StatusRow {
            status: RowStatus::Ok,
            subject: "credential_helper",
            detail: "127.0.0.1=/opt/gang/bin/gang",
            fix: None,
        })
    );
    assert_eq!(
        row("clock"),
        Some(StatusRow {
            status: RowStatus::Ok,
            subject: "clock",
            detail: "within 30s of the server",
            fix: None,
        })
    );
    assert_eq!(row("tls"), None);
}

#[test]
fn login_cookie_name_is_host_prefixed_only_over_https() -> gang_e2e::Result<()> {
    let secure = Url::parse("https://127.0.0.1:1/").map_err(E2eError::parse("secure URL"))?;
    let plain = Url::parse("http://127.0.0.1:1/").map_err(E2eError::parse("plain URL"))?;
    assert_eq!(login_cookie_name(&secure), "__Host-gang_login");
    assert_eq!(login_cookie_name(&plain), "gang_login");
    Ok(())
}

/// Parses `text` as the `gang.toml` of the plugin set below, which the tests judge against the
/// real `GangConfig`.
fn parsed(text: &str) -> gang_e2e::Result<GangConfig<OpaquePlugins>> {
    text.parse::<GangConfig<OpaquePlugins>>()
        .map_err(E2eError::parse("the rendered gang.toml"))
}

#[test]
fn rendered_identity_variant_sends_the_sign_in_login_hint() -> gang_e2e::Result<()> {
    let placement = Placement::create(None)?;
    let issuer = Url::parse("http://127.0.0.1:9").map_err(E2eError::parse("issuer URL"))?;
    let variant = Variant {
        identity: IdentityKnobs::IDENTITY,
        ..Variant::baseline(Sandbox::Bwrap)
    };
    let text = ConfigTemplate::parse(&base_config()?)?
        .place(&placement, Transport::Tls)?
        .vary(&variant, &launcher(), &tools())?
        .with_identity(&issuer, variant.identity)?
        .render()?;

    let config = parsed(&text)?;
    assert_eq!(config.server.rate_limit.client.burst.get(), 120);
    let login = config
        .dashboard
        .login
        .as_ref()
        .expect("the identity variant renders [dashboard.login]");
    assert_eq!(login.provider.as_str(), PROVIDER);
    assert_eq!(login.client_id, DASHBOARD_CLIENT_ID);

    let doc: Table = toml::from_str(&text).expect("the rendered config is TOML");
    let oidc = &doc["identity"]["providers"][PROVIDER]["oidc"];
    assert_eq!(
        oidc["login"]["authorization_params"]["login_hint"].as_str(),
        Some(SIGN_IN_SUBJECT)
    );
    assert_eq!(
        oidc["login"]["offline"]["authorization_params"],
        Value::Table(Table::from_iter([
            ("access_type".to_owned(), Value::from("offline")),
            ("prompt".to_owned(), Value::from("consent")),
        ]))
    );
    assert_eq!(
        oidc["login"]["scopes"],
        Value::Array(vec![Value::from("openid")])
    );
    assert_eq!(
        oidc["additional_audiences"],
        Value::Array(vec![Value::from(DASHBOARD_CLIENT_ID)])
    );
    assert_eq!(
        oidc["issuer_url"].as_str(),
        Some("http://127.0.0.1:9/"),
        "with_identity merges into the provider the variant rendered"
    );
    assert_eq!(
        doc["fleets"][0]["min_instances"].as_integer(),
        Some(1),
        "the identity variant keeps a standing worker"
    );

    let baseline = ConfigTemplate::parse(&base_config()?)?
        .place(&placement, Transport::Tls)?
        .vary(&Variant::baseline(Sandbox::Bwrap), &launcher(), &tools())?
        .with_identity(&issuer, IdentityKnobs::NONE)?
        .render()?;
    assert!(
        parsed(&baseline)?.dashboard.login.is_none(),
        "the baseline variant renders no dashboard.login"
    );
    Ok(())
}

/// Cross-site renewal cannot succeed or set cookies.
#[test]
fn cross_site_renewal_must_be_refused() {
    assert!(cross_site_renewal_refused(&answer(403, &[])).is_ok());
    let defect = answer(
        204,
        &[(
            "set-cookie",
            "__Host-gang_session=s; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=60",
        )],
    );
    assert!(
        cross_site_renewal_refused(&defect)
            .expect_err("must refuse")
            .contains("204")
    );
}

/// Renewal cookies require both `HttpOnly` and `SameSite=Strict`.
#[test]
fn renewal_cookie_needs_http_only_and_strict() {
    let name = "__Host-gang_renewal";
    let good = "__Host-gang_renewal=v; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=34560000";
    let no_http_only = "__Host-gang_renewal=v; Secure; SameSite=Strict; Path=/; Max-Age=34560000";
    let lax = "__Host-gang_renewal=v; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=34560000";
    assert!(renewal_cookie(&with_cookies(&[good]), name).is_ok());
    assert!(
        renewal_cookie(&with_cookies(&[no_http_only]), name)
            .expect_err("HttpOnly required")
            .contains("HttpOnly")
    );
    assert!(
        renewal_cookie(&with_cookies(&[lax]), name)
            .expect_err("Strict required")
            .contains("SameSite")
    );
}

/// A later sign-in does not request offline access or consent.
#[test]
fn sign_in_after_the_lifetime_must_not_ask_consent() {
    let url = "https://issuer.example/authorize?login_hint=browser";
    assert!(consent_free(url).is_ok());
    assert!(
        consent_free(&format!("{url}&prompt=consent"))
            .expect_err("consent requested")
            .contains("consent")
    );
    assert!(
        consent_free(&format!("{url}&access_type=offline"))
            .expect_err("offline requested")
            .contains("offline")
    );
    assert!(offline_requested(&format!("{url}&access_type=offline&prompt=consent")).is_ok());
    assert!(offline_requested(&format!("{url}&access_type=offline")).is_err());
}

/// A sealed renewal cookie is not a credential on other surfaces.
#[test]
fn sealed_cookie_is_no_credential() {
    let denied = answer(401, &[]);
    let accepted = answer(200, &[]);
    assert!(sealed_refused(&denied, &denied, &denied).is_ok());
    assert!(
        sealed_refused(&accepted, &denied, &denied)
            .expect_err("bearer accepted")
            .contains("bearer")
    );
    assert!(
        sealed_refused(&denied, &accepted, &denied)
            .expect_err("session accepted")
            .contains("session cookie")
    );
    assert!(
        sealed_refused(&denied, &denied, &accepted)
            .expect_err("proof accepted")
            .contains("session:authenticate")
    );
}

/// Expired renewal retains its cookie rather than clearing it.
#[test]
fn max_age_renewal_keeps_the_cookie() {
    let name = "__Host-gang_renewal";
    let denied = answer(401, &[]);
    let cleared = answer(
        401,
        &[(
            "set-cookie",
            "__Host-gang_renewal=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0",
        )],
    );
    assert!(renewal_kept(&denied, 401, name).is_ok());
    assert!(renewal_kept(&cleared, 401, name).is_err());
    assert!(renewal_cleared(&cleared, 401, name).is_ok());
    assert!(renewal_cleared(&denied, 401, name).is_err());
}

/// Concurrent renewals share a grant and re-check the held token.
#[test]
fn one_shared_grant_and_held_token_recheck() {
    let session = "__Host-gang_session";
    let renewal = "__Host-gang_renewal";
    let cookies = [
        (
            "set-cookie",
            "__Host-gang_session=s; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=60",
        ),
        (
            "set-cookie",
            "__Host-gang_renewal=v; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=34560000",
        ),
    ];
    let first = answer(204, &cookies);
    let second = answer(204, &cookies);
    let record = RefreshRecord {
        client_id: DASHBOARD_CLIENT_ID.to_owned(),
        refresh_token: "t1".to_owned(),
        outcome: RefreshOutcome::Rotated {
            issued: "t2".to_owned(),
        },
    };
    assert!(
        shared_grant(
            &first,
            &second,
            std::slice::from_ref(&record),
            session,
            renewal
        )
        .is_ok()
    );
    assert!(
        shared_grant(
            &first,
            &second,
            &[record.clone(), record.clone()],
            session,
            renewal
        )
        .expect_err("two grants")
        .contains("2 refresh grants")
    );
    let recheck = RefreshRecord {
        client_id: DASHBOARD_CLIENT_ID.to_owned(),
        refresh_token: "t2".to_owned(),
        outcome: RefreshOutcome::Granted,
    };
    assert!(rechecked(Some(&record), std::slice::from_ref(&recheck)).is_ok());
    let wrong = RefreshRecord {
        refresh_token: "t1".to_owned(),
        ..recheck
    };
    assert!(rechecked(Some(&record), &[wrong]).is_err());
}

/// The sessions variant renders short lifetimes and browser access.
#[test]
fn rendered_sessions_variant() -> gang_e2e::Result<()> {
    let placement = Placement::create(None)?;
    let issuer = Url::parse("http://127.0.0.1:9").map_err(E2eError::parse("issuer URL"))?;
    let variant = Variant {
        identity: IdentityKnobs::SESSIONS,
        ..Variant::baseline(Sandbox::Bwrap)
    };
    let text = ConfigTemplate::parse(&base_config()?)?
        .place(&placement, Transport::Tls)?
        .vary(&variant, &launcher(), &tools())?
        .with_identity(&issuer, variant.identity)?
        .render()?;
    let config = parsed(&text)?;
    assert_eq!(config.dashboard.session_max_age.get(), SESSION_MAX_AGE);
    let doc: Table = toml::from_str(&text).expect("the rendered config is TOML");
    let token = &doc["authentication"]["clients"]["token"];
    assert_eq!(token["token_ttl"].as_str(), Some("1m"));
    assert_eq!(token["clock_skew"].as_str(), Some("0s"));
    let login = &doc["identity"]["providers"][PROVIDER]["oidc"]["login"];
    assert_eq!(
        login["authorization_params"]["login_hint"].as_str(),
        Some(BROWSER_SUBJECT)
    );
    assert_eq!(
        login["offline"]["authorization_params"],
        Value::Table(Table::from_iter([
            ("access_type".to_owned(), Value::from("offline")),
            ("prompt".to_owned(), Value::from("consent")),
        ]))
    );
    let roles = doc["server"]["roles"].as_array().expect("server roles");
    assert!(roles.iter().any(|role| {
        role["principals"].as_array().is_some_and(|principals| {
            principals.iter().any(|principal| {
                principal["provider"].as_str() == Some(PROVIDER)
                    && principal["principal"].as_str() == Some(BROWSER_SUBJECT)
            })
        }) && role["permissions"] == Value::Array(vec![Value::from("state.read")])
    }));
    Ok(())
}

#[test]
fn device_prompt_reads_the_uri_and_the_code() {
    let lines = "To sign in from any device, open https://gang.example/ui/device\n\
        and enter the code BCDF-GHJK (valid until 14:32)";
    assert_eq!(
        gang_e2e::device_prompt(lines),
        Some(gang_e2e::DevicePrompt {
            verification_uri: "https://gang.example/ui/device".to_owned(),
            user_code: "BCDF-GHJK".to_owned(),
        })
    );
    assert!(gang_e2e::device_prompt(&lines.replace("BCDF-GHJK", "BCDA-GHJK")).is_none());
    assert!(gang_e2e::device_prompt("enter the code BCDF-GHJK").is_none());
}

#[test]
fn cross_site_device_approval_must_be_refused() {
    assert!(gang_e2e::cross_site_approval_refused(&answer(403, &[])).is_ok());
    let defect = answer(303, &[("location", "http://127.0.0.1:9/authorize?..")]);
    assert!(gang_e2e::cross_site_approval_refused(&defect).is_err_and(|e| e.contains("303")));
}

#[test]
fn second_redeem_must_not_be_served() -> Result<(), serde_json::Error> {
    let mut refused = answer(404, &[]);
    refused.body = serde_json::to_vec(&gang_wire::ErrorResponse {
        error: gang_wire::Status::new(
            gang_wire::ErrorReason::DeviceAuthorizationNotFound,
            "..",
            [],
        ),
    })?;
    assert!(gang_e2e::refused_with(&refused, 404, "DEVICE_AUTHORIZATION_NOT_FOUND").is_ok());
    let mut served = answer(200, &[]);
    served.body = br#"{"token":"t","lifetime":"600s","refreshToken":"r"}"#.to_vec();
    assert!(
        gang_e2e::refused_with(&served, 404, "DEVICE_AUTHORIZATION_NOT_FOUND")
            .is_err_and(|e| e.contains("200"))
    );
    Ok(())
}

#[test]
fn device_get_must_not_carry_the_secret() {
    let mut resource = answer(200, &[]);
    resource.body = br#"{"name":"deviceAuthorizations/BCDF-GHJK","clientAddress":"127.0.0.1","state":"PENDING"}"#.to_vec();
    assert!(gang_e2e::device_resource(&resource, "BCDF-GHJK", "127.0.0.1").is_ok());
    resource.body = br#"{"name":"deviceAuthorizations/BCDF-GHJK","clientAddress":"127.0.0.1","state":"PENDING","secret":"s"}"#.to_vec();
    assert!(
        gang_e2e::device_resource(&resource, "BCDF-GHJK", "127.0.0.1")
            .is_err_and(|e| e.contains("secret"))
    );
}

#[test]
fn rotated_refresh_token_is_written_first() {
    let t = std::time::UNIX_EPOCH;
    let later = t + std::time::Duration::from_nanos(1);
    let mut files = gang_e2e::StoredSession {
        refresh_token: Some("t2".to_owned()),
        refresh_written: Some(t),
        token_written: Some(later),
        ..gang_e2e::StoredSession::default()
    };
    assert!(gang_e2e::rotation_stored(&files, "t2").is_ok());
    files.refresh_written = Some(later);
    files.token_written = Some(t);
    assert!(gang_e2e::rotation_stored(&files, "t2").is_err_and(|e| e.contains("written before")));
    files.refresh_token = Some("t1".to_owned());
    assert!(gang_e2e::rotation_stored(&files, "t2").is_err_and(|e| e.contains("not the rotated")));
}

#[test]
fn device_only_refuses_a_browser_url() {
    assert!(gang_e2e::device_only("open https://gang.example/ui/device").is_ok());
    assert!(
        gang_e2e::device_only("open https://issuer.example/authorize?state=s")
            .is_err_and(|e| e.contains("/authorize"))
    );
}

#[test]
fn logged_in_with_needs_exit_zero_and_the_provider() {
    assert!(
        gang_e2e::logged_in_with(
            &outcome(Some(0), "ok  logged in with e2e · token valid for 599s"),
            "e2e"
        )
        .is_ok()
    );
    assert!(
        gang_e2e::logged_in_with(
            &outcome(Some(1), "ok  logged in with e2e · token valid for 599s"),
            "e2e"
        )
        .is_err()
    );
    assert!(gang_e2e::logged_in_with(&outcome(Some(0), "logged in with other"), "e2e").is_err());
}

#[test]
fn stored_nothing_needs_an_empty_home() {
    assert!(gang_e2e::stored_nothing(&gang_e2e::StoredSession::default()).is_ok());
    let files = gang_e2e::StoredSession {
        kind: Some("device".to_owned()),
        ..gang_e2e::StoredSession::default()
    };
    assert!(gang_e2e::stored_nothing(&files).is_err());
}
