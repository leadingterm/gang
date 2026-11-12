//! The identity scenario against a compliant and deliberately flawed in-memory deployment.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{assert_compliant, failures};
use gang_e2e::names::identity as names;
use gang_e2e::{
    Access, Administration, Aging, Login, MisdirectedLogin, Misdirection, Ran, TokenAnswer,
    Validity, WorkerIdentity, identity,
};
use gang_e2e::{Admission, Channel, ChannelEvent, Credentials, Reply, SessionCookie};
use gang_e2e::{Answer, Presented, Request};
use gang_types::CertSerial;
use gang_wire::{ErrorReason, ErrorResponse, Status};
use http::Method;
use parking_lot::Mutex;

/// The certificate serial of the simulated standing worker.
const WORKER_SERIAL: &str = "0a1b";

/// Cookie names used by the simulated HTTPS endpoint.
const COOKIE_NAMES: gang_wire::CookieNames = gang_wire::CookieNames::for_https(true);

behaviour! {
    /// The independently configurable flaws in the simulated deployment.
    struct Behaviour {
        /// Pin behavior. Pin validation behavior.
        pin: PinCheck {
            /// Enforce the pin.
            Enforced,
            /// Accept an incorrect pin.
            Ignored,
            /// Suggest an unrelated connectivity fix.
            ReachFix,
            /// Store credentials despite a pin failure.
            Leaks,
        },
        /// Scheme behavior. URL scheme validation behavior.
        scheme: SchemeCheck {
            /// Accept HTTP and HTTPS schemes.
            HttpOrHttps,
            /// Refuse plain HTTP.
            PlainRefused,
            /// Attempt to dial an FTP URL.
            FtpDialled,
        },
        /// Permission behavior. Authorization behavior.
        permission: PermissionCheck {
            /// Enforce administrative permissions.
            Enforced,
            /// Allow a member to revoke a principal.
            RevokeAllowed,
            /// Allow a member to restore a principal.
            RestoreAllowed,
            /// Allow a member to revoke a serial.
            SerialAllowed,
            /// Report a policy denial instead of a missing permission.
            ApiPolicyDenied,
        },
        /// Credential behavior.
        bearer: BearerCheck {
            /// Verify credential types and validity.
            Verified,
            /// Refuse freshly minted bearer tokens.
            MintedRefused,
            /// Accept expired bearer tokens.
            ExpiredAccepted,
            /// Accept session values as bearer tokens.
            SessionAsBearer,
            /// Accept bearer tokens as session cookies.
            BearerAsCookie,
            /// Refuse valid session cookies.
            CookieRefused,
        },
        /// Sign-in behavior. Browser flow behavior.
        sign_in: SignInFlow {
            /// Complete the browser flow securely.
            Compliant,
            /// Use an unprotected PKCE challenge.
            PlainChallenge,
            /// Retain the login cookie too long.
            LongLoginCookie,
            /// Accept mismatched login state.
            StateUnchecked,
            /// Set a lax session cookie.
            LaxSession,
            /// Refuse the newly signed-in session.
            SessionRefused,
            /// Redirect to an unsafe destination.
            OpenRedirect,
            /// Allow cross-site logout.
            CrossSiteLogout,
            /// Leave the session cookie intact on logout.
            LogoutKeepsCookie,
        },
        /// Proof command behavior. Proof execution behavior.
        proof: ProofCommand {
            /// Run the proof command once per token.
            PerToken,
            /// Skip the proof command at login.
            NotRunAtLogin,
            /// Run the proof command on every get.
            EveryGet,
            /// Reuse an expired proof token.
            Cached,
        },
        /// Renewal behavior.
        renewal: Renewal {
            /// Refresh stored logins correctly.
            Refreshed,
            /// Fail interactive login.
            LoginFails,
            /// Do not refresh an expired login.
            NoRefresh,
            /// Report the wrong error for a rejected refresh.
            WrongText,
            /// Skip the refresh retry after a 403.
            NoRetryOn403,
            /// Serve a revoked login.
            RevokedServed,
        },
        /// Principal administration behavior. Administrator behavior.
        administration: AdminRevocation {
            /// Perform principal revocations.
            Succeeds,
            /// Fail the administrator's principal revocation.
            Fails,
        },
        /// Doctor behavior.
        doctor: DoctorCheck {
            /// Report credential health accurately.
            Compliant,
            /// Report healthy credentials as failed.
            CredentialsFail,
            /// Miss a revoked principal.
            RevocationMissed,
            /// Exit successfully despite failed credentials.
            ExitZero,
        },
        /// Worker behavior. Worker listener behavior.
        worker: WorkerCheck {
            /// Enforce worker certificate revocation.
            Compliant,
            /// Admit an anonymous worker.
            AnonymousAdmitted,
            /// Refuse a valid worker certificate.
            CertificateRefused,
            /// Fail the administrator's serial revocation.
            SerialRevokeFails,
            /// Keep a revoked worker stream open.
            StreamKept,
            /// Admit a fresh handshake with a revoked certificate.
            NewHandshakeAdmitted,
            /// Omit the revocation reason from the worker log.
            LogSilent,
        },
        /// Restart behavior.
        restart: RestartCheck {
            /// Restart and retain revocations.
            Compliant,
            /// Leave the server process unchanged.
            NotRestarted,
            /// Forget revocations on restart.
            RevocationForgotten,
            /// Stop serving requests after restart.
            DownAfterRestart,
        },
    }
}

/// A client home and its renewal/proof state.
struct Home {
    /// Principal represented by this home.
    principal: String,
    /// Current token, if logged in.
    token: Option<String>,
    /// Simulated elapsed credential lifetime.
    aged: Option<Aging>,
    /// Number of proof command runs.
    runs: usize,
    /// Whether the provider rejects refresh.
    rejected: bool,
    /// Number of refresh grants.
    grants: usize,
}
/// Mutable state shared by requests and worker channels.
struct State {
    /// Revoked principals.
    revoked: BTreeSet<String>,
    /// Registered bearer tokens and their principals.
    tokens: BTreeMap<String, String>,
    /// Client homes.
    homes: BTreeMap<PathBuf, Home>,
    /// Pending browser states and their destinations.
    attempts: BTreeMap<String, String>,
    /// Whether the standing worker's serial is revoked.
    serial_revoked: bool,
    /// Simulated server process identifier.
    pid: u32,
    /// Whether a restart was attempted.
    restarted: bool,
    /// Counter for simulated identifiers.
    counter: usize,
}
/// An in-memory identity deployment.
struct SimulatedAccess {
    /// Configured behavior.
    behaviour: Behaviour,
    /// Shared mutable deployment state.
    state: Arc<Mutex<State>>,
}
impl SimulatedAccess {
    /// Creates isolated server state.
    fn new(behaviour: Behaviour) -> Self {
        Self {
            behaviour,
            state: Arc::new(Mutex::new(State {
                revoked: BTreeSet::new(),
                tokens: BTreeMap::new(),
                homes: BTreeMap::new(),
                attempts: BTreeMap::new(),
                serial_revoked: false,
                pid: 100,
                restarted: false,
                counter: 0,
            })),
        }
    }
    /// Mints and registers a unique simulated token.
    fn token(&self, principal: &str) -> String {
        let mut s = self.state.lock();
        s.counter += 1;
        let t = format!("tok{}", s.counter);
        s.tokens.insert(t.clone(), principal.into());
        t
    }
    /// Creates a simulated client home.
    fn home(&self, principal: &str, token: Option<String>, runs: usize) -> PathBuf {
        let mut s = self.state.lock();
        s.counter += 1;
        let path = PathBuf::from(format!("/sim/home/{}", s.counter));
        s.homes.insert(
            path.clone(),
            Home {
                principal: principal.into(),
                token,
                aged: None,
                runs,
                rejected: false,
                grants: 0,
            },
        );
        path
    }
    /// Constructs an HTTP answer.
    fn response(status: u16, headers: Vec<(String, String)>, body: Vec<u8>) -> Answer {
        Answer {
            status,
            headers,
            body,
        }
    }
    /// Serializes a wire error envelope.
    fn error(reason: ErrorReason, metadata: &[(&'static str, &str)]) -> Vec<u8> {
        serde_json::to_vec(&ErrorResponse {
            error: Status::new(
                reason,
                reason.message(),
                metadata.iter().map(|&(key, value)| (key, value.to_owned())),
            ),
        })
        .unwrap_or_default()
    }

    /// Completes a simulated browser authorization callback.
    fn callback(&self, r: &Request) -> Answer {
        let state_value = r
            .target
            .split("state=")
            .nth(1)
            .unwrap_or("")
            .split('&')
            .next()
            .unwrap_or("");
        let cookie = match &r.presented {
            Presented::Cookie(cookie) => cookie.value.as_str(),
            _ => "",
        };
        let mut state = self.state.lock();
        if !matches!(self.behaviour.sign_in, SignInFlow::StateUnchecked) && cookie != state_value {
            return Self::response(
                302,
                vec![
                    ("location".into(), "/ui/error?reason=failed".into()),
                    (
                        "set-cookie".into(),
                        format!(
                            "{}=; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=0",
                            COOKIE_NAMES.login
                        ),
                    ),
                ],
                vec![],
            );
        }
        let Some(next) = state.attempts.remove(state_value) else {
            return Self::response(
                302,
                vec![("location".into(), "/ui/error?reason=failed".into())],
                vec![],
            );
        };
        state.counter += 1;
        let signed = format!("signed-{}", state.counter);
        state.tokens.insert(signed.clone(), "e2e:operator".into());
        let same_site = if matches!(self.behaviour.sign_in, SignInFlow::LaxSession) {
            "Lax"
        } else {
            "Strict"
        };
        let headers = vec![
            ("location".into(), next),
            (
                "set-cookie".into(),
                format!(
                    "{}={signed}; HttpOnly; Secure; SameSite={same_site}; Path=/; Max-Age=600",
                    COOKIE_NAMES.session
                ),
            ),
            (
                "set-cookie".into(),
                format!(
                    "{}=; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=0",
                    COOKIE_NAMES.login
                ),
            ),
        ];
        Self::response(302, headers, vec![])
    }

    /// Routes a request after checking its credentials.
    fn route_request(
        &self,
        r: &Request,
        path: &str,
        principal: Option<&str>,
        invalid_bearer: bool,
    ) -> Answer {
        let anonymous = principal.is_none();
        let admin = principal == Some("e2e:administrator");
        let mut headers = vec![];
        let status = match (r.method.clone(), path) {
            (Method::GET, "/v1/principals") => {
                if anonymous {
                    401
                } else if admin {
                    200
                } else {
                    403
                }
            }
            (Method::GET, "/v1/session") => {
                if anonymous {
                    401
                } else {
                    200
                }
            }
            (Method::POST, p) if p.starts_with("/v1/principals/") => {
                if admin {
                    200
                } else {
                    403
                }
            }
            (Method::GET, "/ui/login") => self.login_route(r, &mut headers),
            (Method::POST, "/ui/logout") => self.logout_route(r, &mut headers),
            _ => 404,
        };
        let body = if status == 401 {
            Self::error(
                if invalid_bearer {
                    ErrorReason::CredentialsInvalid
                } else {
                    ErrorReason::CredentialsMissing
                },
                &[],
            )
        } else if status == 403 {
            if matches!(self.behaviour.permission, PermissionCheck::ApiPolicyDenied) {
                Self::error(ErrorReason::PolicyDenied, &[])
            } else if r.method == Method::POST {
                Self::error(
                    ErrorReason::MissingPermission,
                    &[("permission", "principals.revoke")],
                )
            } else {
                Self::error(
                    ErrorReason::MissingPermission,
                    &[("permission", "principals.read")],
                )
            }
        } else if status == 404 {
            Self::error(
                ErrorReason::RouteNotFound,
                &[
                    (
                        "method",
                        if r.method == Method::GET {
                            "GET"
                        } else {
                            "POST"
                        },
                    ),
                    ("path", path),
                ],
            )
        } else {
            vec![]
        };
        Self::response(status, headers, body)
    }

    /// Handles same-origin and cross-site logout.
    fn logout_route(&self, r: &Request, headers: &mut Vec<(String, String)>) -> u16 {
        let cross = r
            .headers
            .iter()
            .any(|(n, v)| *n == "sec-fetch-site" && v == "cross-site");
        let same = r
            .headers
            .iter()
            .any(|(n, v)| *n == "origin" && v == "https://127.0.0.1:8443")
            || r.headers
                .iter()
                .any(|(n, v)| *n == "sec-fetch-site" && v == "same-origin");
        if cross && !matches!(self.behaviour.sign_in, SignInFlow::CrossSiteLogout) {
            403
        } else if same || cross {
            if !matches!(self.behaviour.sign_in, SignInFlow::LogoutKeepsCookie) {
                headers.push((
                    "set-cookie".into(),
                    format!(
                        "{}=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0",
                        COOKIE_NAMES.session
                    ),
                ));
            }
            204
        } else {
            403
        }
    }

    /// Starts a simulated browser authorization transaction.
    fn login_route(&self, r: &Request, headers: &mut Vec<(String, String)>) -> u16 {
        let next = r
            .target
            .split("next=")
            .nth(1)
            .unwrap_or("/ui")
            .split('&')
            .next()
            .unwrap_or("/ui")
            .replace("%2F", "/")
            .replace("%3A", ":");
        let mut s = self.state.lock();
        s.counter += 1;
        let n = s.counter;
        let value = format!("st{n}");
        let next = if next.starts_with("/ui")
            || matches!(self.behaviour.sign_in, SignInFlow::OpenRedirect)
        {
            next
        } else {
            "/ui".into()
        };
        s.attempts.insert(value.clone(), next);
        let method = if matches!(self.behaviour.sign_in, SignInFlow::PlainChallenge) {
            "plain"
        } else {
            "S256"
        };
        headers.push((
            "location".into(),
            format!(
                concat!(
                    "http://127.0.0.1:9/authorize?response_type=code&client_id={}",
                    "&redirect_uri=https%3A%2F%2F127.0.0.1%3A8443%2Fui%2Fcallback",
                    "&scope=openid&state={}&nonce=nonce{}",
                    "&code_challenge=E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
                    "&code_challenge_method={}&login_hint=operator",
                ),
                gang_e2e::DASHBOARD_CLIENT_ID,
                value,
                n,
                method
            ),
        ));
        headers.push((
            "set-cookie".into(),
            format!(
                "{}={value}; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age={}",
                COOKIE_NAMES.login,
                if matches!(self.behaviour.sign_in, SignInFlow::LongLoginCookie) {
                    3600
                } else {
                    600
                }
            ),
        ));
        302
    }

    /// Reports failed interactive renewal, updating the attempted grant count.
    fn renewal_failure(&self, home: &mut Home, age: Aging, revoked: bool) -> Option<TokenAnswer> {
        if revoked && !matches!(self.behaviour.renewal, Renewal::RevokedServed) {
            if !matches!(self.behaviour.renewal, Renewal::NoRetryOn403) {
                home.grants += 1;
            }
            return Some(TokenAnswer {
                outcome: Ran {
                    code: Some(1),
                    stdout: String::new(),
                    stderr: "  fail  gang get: the server answered 403: principal revoked\n\
                             \x20  fix  ask an operator for a role that grants the permission \
                             the action needs\n"
                        .into(),
                },
                bearer: None,
            });
        }
        if matches!(age, Aging::TokenAndProof)
            && (home.rejected || matches!(self.behaviour.renewal, Renewal::NoRefresh))
        {
            let stderr = if matches!(
                self.behaviour.renewal,
                Renewal::WrongText | Renewal::NoRefresh
            ) {
                "  fail  gang get: the server answered 401: invalid credentials\n"
            } else {
                "  fail  gang get: the identity provider no longer accepts the stored login\n\
                 \x20       ↳ invalid proof\n   fix  run gang login again\n"
            };
            return Some(TokenAnswer {
                outcome: Ran {
                    code: Some(1),
                    stdout: String::new(),
                    stderr: stderr.into(),
                },
                bearer: None,
            });
        }
        None
    }
}
/// An established worker connection.
struct SimWorkerChannel {
    /// Shared revocation state.
    state: Arc<Mutex<State>>,
    /// Worker listener behavior.
    worker: WorkerCheck,
    /// Whether this channel was admitted without a valid, unrevoked certificate.
    bypassed_revocation: bool,
}
impl Channel for SimWorkerChannel {
    fn ping(&mut self) -> gang_e2e::Result<ChannelEvent> {
        Ok(
            if self.state.lock().serial_revoked
                && !self.bypassed_revocation
                && !matches!(self.worker, WorkerCheck::StreamKept)
            {
                ChannelEvent::Closed
            } else {
                ChannelEvent::PingAcknowledged
            },
        )
    }
    fn request(&mut self) -> gang_e2e::Result<Reply> {
        Ok(Reply::Served)
    }
}
impl Access for SimulatedAccess {
    type Channel = SimWorkerChannel;
    fn endpoint(&self) -> gang_e2e::Result<gang_types::Url> {
        gang_types::Url::parse("https://127.0.0.1:8443/")
            .map_err(gang_e2e::E2eError::issuer_from("endpoint"))
    }
    fn server_pid(&self) -> Option<u32> {
        Some(self.state.lock().pid)
    }
    fn administrator(&self) -> gang_e2e::Result<Credentials> {
        self.state
            .lock()
            .tokens
            .insert("admin-bearer".into(), "e2e:administrator".into());
        Ok(Credentials {
            principal: "e2e:administrator".into(),
            home: "/sim/admin".into(),
            token: "admin-bearer".into(),
            authority: None,
        })
    }
    fn workload(&self, subject: &str) -> gang_e2e::Result<Credentials> {
        let p = format!("e2e:{subject}");
        let t = format!("{subject}-bearer");
        self.state.lock().tokens.insert(t.clone(), p.clone());
        Ok(Credentials {
            principal: p,
            home: format!("/sim/{subject}").into(),
            token: t,
            authority: None,
        })
    }
    fn misdirected_login(&self, m: Misdirection) -> gang_e2e::Result<MisdirectedLogin> {
        let stored = if matches!(self.behaviour.pin, PinCheck::Leaks) {
            vec![PathBuf::from(".config/gang/session.json")]
        } else {
            vec![]
        };
        let (stderr, code, stored) = match m {
            Misdirection::WrongPin => {
                if matches!(self.behaviour.pin, PinCheck::Ignored) {
                    (
                        String::new(),
                        0,
                        vec![PathBuf::from(".config/gang/session.json")],
                    )
                } else {
                    let fix = if matches!(self.behaviour.pin, PinCheck::ReachFix) {
                        "check --server; for a farm on this machine, run gang local up"
                    } else {
                        "add --ca-pin sha256:<hex>; gang server prints it at startup"
                    };
                    (
                        format!(
                            "  fail  gang login: cannot reach the server https://127.0.0.1:8443\n\
                             \x20       ↳ invalid peer certificate\n   fix  {fix}\n"
                        ),
                        1,
                        stored,
                    )
                }
            }
            Misdirection::PlainScheme => {
                if matches!(self.behaviour.scheme, SchemeCheck::PlainRefused) {
                    (
                        "  fail  gang login: the server URL http://127.0.0.1:8443/ must use the \
                         http or https scheme\n   fix  use an http or https URL for --server\n"
                            .into(),
                        1,
                        vec![],
                    )
                } else {
                    (
                        "  fail  gang login: cannot reach the server http://127.0.0.1:8443\n\
                         \x20  fix  check --server; for a farm on this machine, run gang local up\n"
                            .into(),
                        1,
                        vec![],
                    )
                }
            }
            Misdirection::UnsupportedScheme => {
                if matches!(self.behaviour.scheme, SchemeCheck::FtpDialled) {
                    (
                        "  fail  gang login: cannot reach the server ftp://127.0.0.1:8443\n\
                         \x20  fix  check --server; for a farm on this machine, run gang local up\n"
                            .into(),
                        1,
                        vec![],
                    )
                } else {
                    (
                        "  fail  gang login: the server URL ftp://127.0.0.1:8443/ must use the \
                         http or https scheme\n   fix  use an http or https URL for --server\n"
                            .into(),
                        1,
                        vec![],
                    )
                }
            }
        };
        Ok(MisdirectedLogin {
            outcome: Ran {
                code: Some(code),
                stdout: String::new(),
                stderr,
            },
            stored,
        })
    }
    fn administer(&self, who: &Credentials, a: &Administration) -> gang_e2e::Result<Ran> {
        let permitted = who.principal == "e2e:administrator"
            || match a {
                Administration::RevokePrincipal(_) => {
                    matches!(self.behaviour.permission, PermissionCheck::RevokeAllowed)
                }
                Administration::RestorePrincipal(_) => {
                    matches!(self.behaviour.permission, PermissionCheck::RestoreAllowed)
                }
                Administration::RevokeSerial(_) => {
                    matches!(self.behaviour.permission, PermissionCheck::SerialAllowed)
                }
            };
        let administrator = who.principal == "e2e:administrator";
        if !permitted {
            let subject = match a {
                Administration::RestorePrincipal(_) => "gang restore",
                Administration::RevokePrincipal(_) | Administration::RevokeSerial(_) => {
                    "gang revoke"
                }
            };
            return Ok(Ran {
                code: Some(1),
                stdout: String::new(),
                stderr: format!(
                    "  fail  {subject}: the server answered 403: missing permission \
                     principals.revoke\n   fix  ask an operator for a role that grants the \
                     permission the action needs\n"
                ),
            });
        }
        let failed = administrator
            && ((matches!(self.behaviour.administration, AdminRevocation::Fails)
                && matches!(a, Administration::RevokePrincipal(_)))
                || (matches!(self.behaviour.worker, WorkerCheck::SerialRevokeFails)
                    && matches!(a, Administration::RevokeSerial(_))));
        if failed {
            return Ok(Ran {
                code: Some(1),
                stdout: String::new(),
                stderr: "  fail  gang revoke: the server answered 500: internal\n".into(),
            });
        }
        let mut s = self.state.lock();
        match a {
            Administration::RevokePrincipal(p) => {
                s.revoked.insert(p.clone());
            }
            Administration::RestorePrincipal(p) => {
                s.revoked.remove(p);
            }
            Administration::RevokeSerial(serial) if serial.as_str() == WORKER_SERIAL => {
                s.serial_revoked = true;
            }
            Administration::RevokeSerial(_) => {}
        }
        Ok(Ran {
            code: Some(0),
            stdout: String::new(),
            stderr: String::new(),
        })
    }
    fn request(&self, r: &Request) -> gang_e2e::Result<Answer> {
        let path = r.target.split('?').next().unwrap_or("");
        let invalid_bearer = matches!(&r.presented, Presented::Bearer(_));
        if matches!(&r.presented, Presented::Bearer(token) if token.starts_with("minted-"))
            && matches!(self.behaviour.bearer, BearerCheck::MintedRefused)
        {
            return Ok(Self::response(
                401,
                vec![],
                Self::error(ErrorReason::CredentialsInvalid, &[]),
            ));
        }
        if path == "/ui/callback" {
            return Ok(self.callback(r));
        }
        let s = self.state.lock();
        if s.restarted && matches!(self.behaviour.restart, RestartCheck::DownAfterRestart) {
            return Ok(Self::response(503, vec![], vec![]));
        }
        let mut principal = None;
        match &r.presented {
            Presented::Bearer(t) => {
                if let Some(p) = s.tokens.get(t) {
                    principal = Some(p.clone());
                } else if t.starts_with("expired-")
                    && matches!(self.behaviour.bearer, BearerCheck::ExpiredAccepted)
                {
                    principal = Some(t.trim_start_matches("expired-").into());
                } else if t.starts_with("session-")
                    && matches!(self.behaviour.bearer, BearerCheck::SessionAsBearer)
                {
                    principal = Some(t.trim_start_matches("session-").into());
                }
            }
            Presented::Cookie(c) => {
                if c.name == COOKIE_NAMES.session
                    && c.value.starts_with("session-")
                    && !matches!(self.behaviour.bearer, BearerCheck::CookieRefused)
                {
                    principal = Some(c.value.trim_start_matches("session-").into());
                } else if c.name == COOKIE_NAMES.session
                    && matches!(self.behaviour.bearer, BearerCheck::BearerAsCookie)
                {
                    principal = s.tokens.get(&c.value).cloned();
                } else if c.name == COOKIE_NAMES.session
                    && c.value.starts_with("signed-")
                    && !matches!(self.behaviour.sign_in, SignInFlow::SessionRefused)
                {
                    principal = Some("e2e:operator".into());
                }
            }
            Presented::Nothing => {}
        }
        let principal = principal.map(|p| {
            if p.starts_with("e2e:") {
                p
            } else {
                format!("e2e:{p}")
            }
        });
        if invalid_bearer && principal.is_none() {
            return Ok(Self::response(
                401,
                vec![],
                Self::error(ErrorReason::CredentialsInvalid, &[]),
            ));
        }
        if principal.as_ref().is_some_and(|p| {
            s.revoked.contains(p)
                && !(s.restarted
                    && matches!(self.behaviour.restart, RestartCheck::RevocationForgotten))
        }) {
            return Ok(Self::response(
                403,
                vec![],
                Self::error(ErrorReason::PrincipalRevoked, &[]),
            ));
        }
        drop(s);
        Ok(self.route_request(r, path, principal.as_deref(), invalid_bearer))
    }
    fn visit(&self, url: &gang_types::Url) -> gang_e2e::Result<Answer> {
        if url.path() == "/authorize" {
            let state = url
                .query_pairs()
                .find(|(k, _)| k == "state")
                .map(|(_, v)| v.into_owned())
                .unwrap_or_default();
            Ok(Self::response(
                302,
                vec![(
                    "location".into(),
                    format!("https://127.0.0.1:8443/ui/callback?code=c1&state={state}"),
                )],
                vec![],
            ))
        } else {
            Ok(Self::response(200, vec![], vec![]))
        }
    }
    fn mint_bearer(&self, p: &str, v: Validity) -> gang_e2e::Result<String> {
        let t = if v == Validity::Expired {
            format!("expired-{p}")
        } else {
            format!("minted-{p}")
        };
        if v == Validity::Current {
            self.state.lock().tokens.insert(t.clone(), p.into());
        }
        Ok(t)
    }
    fn session_cookie(&self, w: &Credentials) -> gang_e2e::Result<SessionCookie> {
        Ok(SessionCookie {
            name: COOKIE_NAMES.session.into(),
            value: format!("session-{}", w.principal),
        })
    }
    fn command_login(&self) -> gang_e2e::Result<Login> {
        let t = self.token("e2e:prover");
        let p = self.home(
            "e2e:prover",
            Some(t),
            usize::from(!matches!(self.behaviour.proof, ProofCommand::NotRunAtLogin)),
        );
        Ok(Login {
            home: p,
            outcome: Ran {
                code: Some(0),
                stdout: String::new(),
                stderr: String::new(),
            },
        })
    }
    fn proof_runs(&self, p: &Path) -> gang_e2e::Result<usize> {
        Ok(self.state.lock().homes.get(p).map_or(0, |h| h.runs))
    }
    fn interactive_login(&self) -> gang_e2e::Result<Login> {
        let fail = matches!(self.behaviour.renewal, Renewal::LoginFails);
        let t = if fail {
            None
        } else {
            Some(self.token("e2e:operator"))
        };
        let p = self.home("e2e:operator", t, 0);
        Ok(Login {
            home: p,
            outcome: Ran {
                code: Some(i32::from(fail)),
                stdout: String::new(),
                stderr: if fail {
                    "  fail  gang login: identity provider failed\n".into()
                } else {
                    String::new()
                },
            },
        })
    }
    fn get(&self, p: &Path) -> gang_e2e::Result<TokenAnswer> {
        let mut s = self.state.lock();
        let revoked = s
            .homes
            .get(p)
            .is_some_and(|home| s.revoked.contains(&home.principal));
        let Some(h) = s.homes.get_mut(p) else {
            return Ok(TokenAnswer {
                outcome: Ran {
                    code: Some(1),
                    stdout: String::new(),
                    stderr: "  fail  gang get: not logged in\n".into(),
                },
                bearer: None,
            });
        };
        let Some(old) = h.token.clone() else {
            return Ok(TokenAnswer {
                outcome: Ran {
                    code: Some(1),
                    stdout: String::new(),
                    stderr: "  fail  gang get: not logged in\n".into(),
                },
                bearer: None,
            });
        };
        if h.aged.is_none() {
            if matches!(self.behaviour.proof, ProofCommand::EveryGet)
                && h.principal.ends_with("prover")
            {
                h.runs += 1;
            }
            return Ok(TokenAnswer {
                outcome: Ran {
                    code: Some(0),
                    stdout: String::new(),
                    stderr: String::new(),
                },
                bearer: Some(old),
            });
        }
        let age = h.aged.take().unwrap_or(Aging::Token);
        if h.principal.ends_with("prover") {
            if matches!(self.behaviour.proof, ProofCommand::Cached) {
                return Ok(TokenAnswer {
                    outcome: Ran {
                        code: Some(0),
                        stdout: String::new(),
                        stderr: String::new(),
                    },
                    bearer: Some(old),
                });
            }
            h.runs += 1;
            let t = format!("prover-t{}", h.runs);
            let p = h.principal.clone();
            h.token = Some(t.clone());
            s.tokens.insert(t.clone(), p);
            return Ok(TokenAnswer {
                outcome: Ran {
                    code: Some(0),
                    stdout: String::new(),
                    stderr: String::new(),
                },
                bearer: Some(t),
            });
        }
        let p = h.principal.clone();
        if let Some(failure) = self.renewal_failure(h, age, revoked) {
            return Ok(failure);
        }
        if matches!(age, Aging::TokenAndProof) {
            h.grants += 1;
        }
        let t = format!("op-{}", h.grants + 1);
        h.token = Some(t.clone());
        s.tokens.insert(t.clone(), p);
        Ok(TokenAnswer {
            outcome: Ran {
                code: Some(0),
                stdout: String::new(),
                stderr: String::new(),
            },
            bearer: Some(t),
        })
    }
    fn age(&self, p: &Path, a: Aging) -> gang_e2e::Result<()> {
        if let Some(h) = self.state.lock().homes.get_mut(p) {
            h.aged = Some(a);
        }
        Ok(())
    }
    fn refresh_grants(&self, p: &Path) -> gang_e2e::Result<usize> {
        Ok(self.state.lock().homes.get(p).map_or(0, |h| h.grants))
    }
    fn reject_refresh(&self, p: &Path) -> gang_e2e::Result<()> {
        if let Some(h) = self.state.lock().homes.get_mut(p) {
            h.rejected = true;
        }
        Ok(())
    }
    fn doctor(&self, w: &Credentials) -> gang_e2e::Result<Ran> {
        let revoked = self.state.lock().revoked.contains(&w.principal);
        let failed = revoked && !matches!(self.behaviour.doctor, DoctorCheck::RevocationMissed);
        let (text, code) = if failed {
            (
                "  fail  credentials        the server refused the token of /sim/gang\n\
                 \x20       ↳ principal revoked\n   fix  log in again with `gang login --server \
                 https://127.0.0.1:8443`; if it still refuses, ask the server's administrator \
                 whether your principal was revoked\n",
                i32::from(!matches!(self.behaviour.doctor, DoctorCheck::ExitZero)),
            )
        } else if matches!(self.behaviour.doctor, DoctorCheck::CredentialsFail) {
            (
                "  fail  credentials        cannot present the token to the server\n\
                 \x20  fix  check connectivity to the server\n",
                1,
            )
        } else {
            (
                "    ok  credentials        /sim/gang get returns a token\n",
                0,
            )
        };
        Ok(Ran {
            code: Some(code),
            stdout: text.into(),
            stderr: String::new(),
        })
    }
    fn standing_worker(&self) -> gang_e2e::Result<WorkerIdentity> {
        Ok(WorkerIdentity {
            serial: CertSerial::try_new(WORKER_SERIAL)
                .map_err(gang_e2e::E2eError::parse("the simulated worker serial"))?,
            directory: "/sim/instances/w1/scratch/identity/1".into(),
            instance: "/sim/instances/w1".into(),
        })
    }
    fn connect_worker(
        &self,
        w: Option<&WorkerIdentity>,
    ) -> gang_e2e::Result<Admission<Self::Channel>> {
        if w.is_none() && !matches!(self.behaviour.worker, WorkerCheck::AnonymousAdmitted) {
            return Ok(Admission::Refused);
        }
        if w.is_some() && matches!(self.behaviour.worker, WorkerCheck::CertificateRefused) {
            return Ok(Admission::Refused);
        }
        if w.is_some()
            && self.state.lock().serial_revoked
            && !matches!(self.behaviour.worker, WorkerCheck::NewHandshakeAdmitted)
        {
            return Ok(Admission::Refused);
        }
        Ok(Admission::Accepted(SimWorkerChannel {
            state: Arc::clone(&self.state),
            worker: self.behaviour.worker,
            bypassed_revocation: w.is_none()
                || (self.state.lock().serial_revoked
                    && matches!(self.behaviour.worker, WorkerCheck::NewHandshakeAdmitted)),
        }))
    }
    fn worker_exit_log(&self, _worker: &WorkerIdentity) -> gang_e2e::Result<Option<String>> {
        if !self.state.lock().serial_revoked
            || matches!(self.behaviour.worker, WorkerCheck::StreamKept)
        {
            return Ok(None);
        }
        Ok(Some(
            if matches!(self.behaviour.worker, WorkerCheck::LogSilent) {
                "worker stopped\n".to_owned()
            } else {
                "error: the scheduler reconnect failed: CertificateRevoked\n".to_owned()
            },
        ))
    }
    fn restart_server(&self) -> gang_e2e::Result<()> {
        let mut s = self.state.lock();
        if !matches!(self.behaviour.restart, RestartCheck::NotRestarted) {
            s.pid += 1;
        }
        s.restarted = true;
        Ok(())
    }
}

/// Runs all identity checks against the supplied behavior.
fn identity_checks(behaviour: Behaviour) -> gang_e2e::Result<Vec<gang_e2e::Check>> {
    identity(&SimulatedAccess::new(behaviour))
}

/// Confirms the unmodified simulation satisfies the identity scenario.
#[test]
fn identity_is_compliant() {
    assert_compliant(&identity_checks(Behaviour::COMPLIANT).expect("the identity scenario runs"));
}

flaw!(wrong_pin_stores_nothing_fails: identity_checks(pin = PinCheck::Leaks)
    => only [names::WRONG_PIN_STORES_NOTHING]);
flaw!(wrong_pin_refusal_fails: identity_checks(pin = PinCheck::Ignored)
    => only [names::WRONG_PIN_REFUSED, names::WRONG_PIN_STORES_NOTHING]);
flaw!(wrong_pin_fix_fails: identity_checks(pin = PinCheck::ReachFix)
    => only [names::WRONG_PIN_REFUSED]);
flaw!(plain_scheme_fails: identity_checks(scheme = SchemeCheck::PlainRefused)
    => only [names::PLAIN_SCHEME_ACCEPTED]);
flaw!(ftp_scheme_fails: identity_checks(scheme = SchemeCheck::FtpDialled)
    => only [names::UNSUPPORTED_SCHEME_REFUSED]);
flaw!(member_revoke_fails: identity_checks(permission = PermissionCheck::RevokeAllowed)
=> only [
    names::MEMBER_REVOKE_REFUSED,
    names::MINTED_BEARER_ACCEPTED,
    names::SESSION_COOKIE_ACCEPTED,
    names::RESTARTED_SERVER_SERVES,
]);
flaw!(member_restore_fails: identity_checks(permission = PermissionCheck::RestoreAllowed)
    => only [names::MEMBER_RESTORE_REFUSED]);
flaw!(member_serial_fails: identity_checks(permission = PermissionCheck::SerialAllowed)
    => only [names::MEMBER_SERIAL_REFUSED]);
flaw!(api_permission_fails: identity_checks(permission = PermissionCheck::ApiPolicyDenied)
    => only [names::MEMBER_API_REFUSED]);
flaw!(minted_bearer_fails: identity_checks(bearer = BearerCheck::MintedRefused)
=> only [
    names::MINTED_BEARER_ACCEPTED,
    names::REVOCATION_SURVIVES_RESTART,
    names::RESTARTED_SERVER_SERVES,
]);
flaw!(expired_bearer_fails: identity_checks(bearer = BearerCheck::ExpiredAccepted)
    => only [names::EXPIRED_BEARER_REFUSED]);
flaw!(session_as_bearer_fails: identity_checks(bearer = BearerCheck::SessionAsBearer)
    => only [names::SESSION_AS_BEARER_REFUSED]);
flaw!(bearer_as_cookie_fails: identity_checks(bearer = BearerCheck::BearerAsCookie)
    => only [names::BEARER_AS_COOKIE_REFUSED]);
flaw!(cookie_fails: identity_checks(bearer = BearerCheck::CookieRefused)
    => only [names::SESSION_COOKIE_ACCEPTED]);
flaw!(plain_challenge_fails: identity_checks(sign_in = SignInFlow::PlainChallenge)
    => only [names::SIGN_IN_REDIRECTS]);
flaw!(long_login_cookie_fails: identity_checks(sign_in = SignInFlow::LongLoginCookie)
    => only [names::LOGIN_COOKIE_SET]);
flaw!(unchecked_state_fails: identity_checks(sign_in = SignInFlow::StateUnchecked)
=> only [
    names::STATE_MISMATCH_REFUSED,
    names::SIGN_IN_COMPLETES,
    names::SIGNED_IN_SESSION_ACCEPTED,
]);
flaw!(lax_session_fails: identity_checks(sign_in = SignInFlow::LaxSession)
    => only [names::SIGN_IN_COMPLETES]);
flaw!(session_refused_fails: identity_checks(sign_in = SignInFlow::SessionRefused)
    => only [names::SIGNED_IN_SESSION_ACCEPTED]);
flaw!(open_redirect_fails: identity_checks(sign_in = SignInFlow::OpenRedirect)
    => only [names::UNSAFE_NEXT_DROPPED]);
flaw!(cross_site_logout_fails: identity_checks(sign_in = SignInFlow::CrossSiteLogout)
    => only [names::CROSS_SITE_LOGOUT_REFUSED]);
flaw!(logout_cookie_fails: identity_checks(sign_in = SignInFlow::LogoutKeepsCookie)
    => only [names::LOGOUT_CLEARS_SESSION]);
flaw!(command_not_run_fails: identity_checks(proof = ProofCommand::NotRunAtLogin)
    => only [names::COMMAND_LOGIN]);
flaw!(proof_every_get_fails: identity_checks(proof = ProofCommand::EveryGet)
    => only [names::FRESH_TOKEN_REUSED]);
flaw!(command_cached_fails: identity_checks(proof = ProofCommand::Cached)
    => only [names::COMMAND_RERUN]);
flaw!(interactive_login_fails: identity_checks(renewal = Renewal::LoginFails)
=> only [
    names::INTERACTIVE_LOGIN,
    names::EXPIRED_LOGIN_RENEWED,
    names::RENEWAL_REJECTED,
    names::REVOKED_LOGIN_REFUSED,
]);
flaw!(no_refresh_fails: identity_checks(renewal = Renewal::NoRefresh)
    => only [names::EXPIRED_LOGIN_RENEWED, names::RENEWAL_REJECTED]);
flaw!(wrong_renewal_text_fails: identity_checks(renewal = Renewal::WrongText)
    => only [names::RENEWAL_REJECTED]);
flaw!(no_retry_revoked_fails: identity_checks(renewal = Renewal::NoRetryOn403)
    => only [names::REVOKED_LOGIN_REFUSED]);
flaw!(revoked_served_fails: identity_checks(renewal = Renewal::RevokedServed)
    => only [names::REVOKED_LOGIN_REFUSED]);
flaw!(administrator_revoke_fails: identity_checks(administration = AdminRevocation::Fails)
=> only [
    names::OPERATOR_REVOKED,
    names::REVOKED_LOGIN_REFUSED,
    names::MEMBER_REVOKED,
    names::DOCTOR_CREDENTIALS_REFUSED,
    names::REVOCATION_SURVIVES_RESTART,
]);
flaw!(doctor_credentials_fails: identity_checks(doctor = DoctorCheck::CredentialsFail)
    => only [names::DOCTOR_CREDENTIALS_OK]);
flaw!(doctor_revocation_missed_fails: identity_checks(doctor = DoctorCheck::RevocationMissed)
    => only [names::DOCTOR_CREDENTIALS_REFUSED]);
flaw!(doctor_exit_zero_fails: identity_checks(doctor = DoctorCheck::ExitZero)
    => only [names::DOCTOR_CREDENTIALS_REFUSED]);
flaw!(worker_anonymous_admitted_fails: identity_checks(worker = WorkerCheck::AnonymousAdmitted)
    => only [names::WORKER_LISTENER_NEEDS_CERTIFICATE]);
flaw!(worker_certificate_refused_fails: identity_checks(worker = WorkerCheck::CertificateRefused)
    => only [names::WORKER_CERTIFICATE_ADMITTED, names::OPEN_WORKER_CONNECTION_CUT]);
flaw!(serial_revoke_fails: identity_checks(worker = WorkerCheck::SerialRevokeFails)
=> only [
    names::SERIAL_REVOKED,
    names::OPEN_WORKER_CONNECTION_CUT,
    names::REVOKED_CERTIFICATE_REFUSED,
    names::WORKER_STREAM_ENDS_REVOKED,
]);
flaw!(stream_kept_fails: identity_checks(worker = WorkerCheck::StreamKept)
    => only [names::OPEN_WORKER_CONNECTION_CUT, names::WORKER_STREAM_ENDS_REVOKED]);
flaw!(new_handshake_admitted_fails: identity_checks(worker = WorkerCheck::NewHandshakeAdmitted)
    => only [names::REVOKED_CERTIFICATE_REFUSED]);
flaw!(worker_log_silent_fails: identity_checks(worker = WorkerCheck::LogSilent)
    => only [names::WORKER_STREAM_ENDS_REVOKED]);
flaw!(server_not_restarted_fails: identity_checks(restart = RestartCheck::NotRestarted)
    => only [names::SERVER_RESTARTED]);
flaw!(revocation_forgotten_fails: identity_checks(restart = RestartCheck::RevocationForgotten)
    => only [names::REVOCATION_SURVIVES_RESTART]);
flaw!(server_down_after_restart_fails: identity_checks(restart = RestartCheck::DownAfterRestart)
    => only [names::REVOCATION_SURVIVES_RESTART, names::RESTARTED_SERVER_SERVES]);
