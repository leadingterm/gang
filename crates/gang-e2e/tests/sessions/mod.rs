//! The sessions scenario against a compliant and deliberately flawed in-memory deployment.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use gang_e2e::names::sessions as names;
use gang_e2e::{
    Answer, AuthorizationRecord, Browser, Check, EventStream, Presented, RefreshOutcome,
    RefreshRecord, Request, SessionCookie, Settings, SubjectPolicy, sessions,
};
use gang_types::Url;
use gang_wire::{Closed, ClosedReason, CookieNames, Frame};
use http::Method;
use parking_lot::Mutex;

use super::support::state_frame;
use super::{assert_compliant, assert_fails, failures};

/// Session lifetime in virtual seconds.
const TTL: u64 = 60;
/// Absolute renewal lifetime in virtual seconds.
const MAX_AGE: u64 = 180;
/// Refresh grant reuse window in virtual seconds.
const REUSE: u64 = 60;
/// Cookie names at the simulated HTTPS endpoint.
const COOKIES: CookieNames = CookieNames::for_https(true);
/// OAuth client used by the dashboard.
const CLIENT: &str = "gang-e2e-dashboard";

behaviour! {
    /// Independently injectable defects in the simulated deployment.
    struct Behaviour {
        /// Whether login requests offline access.
        offline: Offline {
            /// Request offline access when necessary.
            Compliant,
            /// Never request offline access.
            Never
        },
        /// Attributes of nonempty renewal cookies.
        cookie: RenewalCookie {
            /// Use strict, HTTP-only cookies.
            Strict,
            /// Use SameSite=Lax.
            Lax,
            /// Omit `HttpOnly`.
            NotHttpOnly
        },
        /// Cross-site renewal enforcement.
        cross_site: CrossSite {
            /// Refuse cross-site requests.
            Refused,
            /// Permit cross-site renewals.
            Renews
        },
        /// The status for a missing renewal cookie.
        bare: NoCookie {
            /// Answer unauthorized.
            Unauthorized,
            /// Answer forbidden.
            Forbidden
        },
        /// Whether sealed cookies can be used as credentials.
        sealed: Sealed {
            /// Refuse sealed cookies as credentials.
            Refused,
            /// Accept a sealed cookie as a bearer token.
            AcceptedAsBearer,
            /// Accept a sealed cookie as an authentication proof.
            AcceptedByAuthenticate
        },
        /// Refresh grant sharing.
        flights: Flights {
            /// Share a grant within the reuse window.
            Shared,
            /// Make a grant for each renewal.
            GrantPerRenewal
        },
        /// Provider traffic caused by renewal.
        renewal: RenewalSignal {
            /// Renew without authorization requests.
            Silent,
            /// Record an authorization for each renewal.
            Reauthorizes
        },
        /// Enforcement of the absolute lifetime.
        max_age: MaxAgeCheck {
            /// Refuse expired cookies without clearing them.
            Enforced,
            /// Ignore the deadline.
            Ignored,
            /// Clear cookies at the deadline.
            ClearsCookie
        },
        /// Sign-in with a held renewal token.
        relogin: Relogin {
            /// Recheck the held token without consent.
            WithoutConsent,
            /// Always request offline access.
            AlwaysOffline,
            /// Skip the held token recheck.
            NoRecheck
        },
        /// Renewal of revoked principals.
        revocation: RevokedRenewal {
            /// Refuse and clear the cookie.
            Refused,
            /// Renew despite revocation.
            Renews
        },
        /// Logout cookie clearing.
        logout: Logout {
            /// Clear both cookies.
            ClearsBoth,
            /// Keep the renewal cookie.
            KeepsRenewal
        },
        /// Renewal after provider suspension.
        suspension: Suspension {
            /// Clear the refused cookie.
            Clears,
            /// Keep the refused cookie.
            KeepsCookie
        }
    }
}

/// A refresh token and its rotation family.
#[derive(Clone)]
struct Token {
    /// Subject that owns the token.
    subject: String,
    /// Family identifier.
    family: u64,
    /// Whether rotation retired this token.
    retired: bool,
}

/// The contents of a sealed renewal cookie.
#[derive(Clone)]
struct Renewal {
    /// Principal that owns the session.
    principal: String,
    /// Held provider refresh token.
    token: String,
    /// Absolute deadline.
    expire: u64,
}

/// State retained between login and callback.
struct Attempt {
    /// Destination after sign-in.
    next: String,
    /// Previously held renewal, if any.
    renewal: Option<Renewal>,
}

/// Issuer and deployment state protected by one lock.
#[derive(Default)]
struct State {
    /// Virtual time.
    now: u64,
    /// Per-subject issuer policy.
    policies: HashMap<String, SubjectPolicy>,
    /// Authorization requests by subject.
    authorizations: HashMap<String, Vec<AuthorizationRecord>>,
    /// Refresh grants by subject.
    refreshes: HashMap<String, Vec<RefreshRecord>>,
    /// Issued refresh tokens.
    tokens: HashMap<String, Token>,
    /// Families revoked by reuse.
    revoked_families: HashSet<u64>,
    /// Issued authorization codes.
    codes: HashMap<String, (String, bool)>,
    /// Outstanding login attempts.
    attempts: HashMap<String, Attempt>,
    /// Sealed renewal cookie contents.
    sealed: HashMap<String, Renewal>,
    /// Session cookie contents.
    sessions: HashMap<String, (String, u64)>,
    /// Revoked principals.
    revoked: HashSet<String>,
    /// Previously completed refresh flights.
    flights: HashMap<String, (u64, Result<Option<String>, ()>)>,
    /// Counter for all fresh values and families.
    next: u64,
}

impl State {
    /// Produces a unique value of the given kind.
    fn fresh(&mut self, kind: &str) -> String {
        self.next += 1;
        format!("{kind}-{}", self.next)
    }

    /// Returns a subject's current policy.
    fn policy(&self, subject: &str) -> SubjectPolicy {
        self.policies.get(subject).copied().unwrap_or_default()
    }

    /// Issues a refresh token in a new family.
    fn issue(&mut self, subject: &str) -> String {
        let value = self.fresh("rt");
        self.tokens.insert(
            value.clone(),
            Token {
                subject: subject.to_owned(),
                family: self.next,
                retired: false,
            },
        );
        value
    }

    /// Performs and records a provider refresh grant.
    fn grant(&mut self, value: &str, client: &str) -> Result<Option<String>, ()> {
        let Some(token) = self.tokens.get(value).cloned() else {
            return Err(());
        };
        let policy = self.policy(&token.subject);
        let outcome = if client != CLIENT
            || policy == SubjectPolicy::Suspended
            || self.revoked_families.contains(&token.family)
        {
            RefreshOutcome::Refused
        } else if policy == SubjectPolicy::Rotating && token.retired {
            self.revoked_families.insert(token.family);
            RefreshOutcome::ReuseDetected
        } else if policy == SubjectPolicy::Rotating {
            if let Some(old) = self.tokens.get_mut(value) {
                old.retired = true;
            }
            let issued = self.fresh("rt");
            self.tokens.insert(
                issued.clone(),
                Token {
                    retired: false,
                    ..token.clone()
                },
            );
            RefreshOutcome::Rotated { issued }
        } else {
            RefreshOutcome::Granted
        };
        let result = match &outcome {
            RefreshOutcome::Granted => Ok(None),
            RefreshOutcome::Rotated { issued } => Ok(Some(issued.clone())),
            RefreshOutcome::Refused | RefreshOutcome::ReuseDetected => Err(()),
        };
        self.refreshes
            .entry(token.subject)
            .or_default()
            .push(RefreshRecord {
                client_id: client.to_owned(),
                refresh_token: value.to_owned(),
                outcome,
            });
        result
    }

    /// Shares a completed refresh within the reuse window when configured.
    fn flight(&mut self, value: &str, behaviour: Behaviour) -> Result<Option<String>, ()> {
        if behaviour.flights == Flights::Shared
            && let Some((start, result)) = self.flights.get(value)
            && self.now < start + REUSE
        {
            return result.clone();
        }
        let result = self.grant(value, CLIENT);
        if behaviour.flights == Flights::Shared {
            self.flights
                .insert(value.to_owned(), (self.now, result.clone()));
        }
        result
    }

    /// Creates a session and attaches its cookie.
    fn session(&mut self, answer: Answer, principal: &str) -> Answer {
        let value = self.fresh("session");
        self.sessions
            .insert(value.clone(), (principal.to_owned(), self.now + TTL));
        answer.with_header(
            "set-cookie",
            cookie(COOKIES.session, &value, "Strict", TTL, true),
        )
    }

    /// Seals a renewal and attaches its cookie.
    fn seal(&mut self, answer: Answer, renewal: Renewal, behaviour: Behaviour) -> Answer {
        let value = self.fresh("sealed");
        self.sealed.insert(value.clone(), renewal);
        let site = if behaviour.cookie == RenewalCookie::Lax {
            "Lax"
        } else {
            "Strict"
        };
        answer.with_header(
            "set-cookie",
            cookie(
                COOKIES.renewal,
                &value,
                site,
                34_560_000,
                behaviour.cookie != RenewalCookie::NotHttpOnly,
            ),
        )
    }

    /// Whether a session value currently authenticates.
    fn live(&self, value: &str) -> Option<(String, u64)> {
        self.sessions
            .get(value)
            .filter(|(principal, exp)| *exp > self.now && !self.revoked.contains(principal))
            .cloned()
    }
}

/// Builds a cookie header with the deployment's HTTPS attributes.
fn cookie(name: &str, value: &str, site: &str, age: u64, http_only: bool) -> String {
    let http_only = if http_only { "; HttpOnly" } else { "" };
    format!("{name}={value}{http_only}; Secure; SameSite={site}; Path=/; Max-Age={age}")
}

/// Adds a compliant clearing cookie.
fn clear(answer: Answer, name: &str, site: &str) -> Answer {
    answer.with_header("set-cookie", cookie(name, "", site, 0, true))
}

/// Parses a request's cookie header and explicitly presented cookie.
fn cookies(request: &Request) -> HashMap<String, String> {
    let mut values = HashMap::new();
    for (_, header) in request.headers.iter().filter(|(name, _)| *name == "cookie") {
        for pair in header.split(';') {
            if let Some((name, value)) = pair.trim().split_once('=') {
                values.insert(name.to_owned(), value.to_owned());
            }
        }
    }
    if let Presented::Cookie(cookie) = &request.presented {
        values.insert(cookie.name.clone(), cookie.value.clone());
    }
    values
}

/// Tests the simulated same-origin request policy.
fn cross_site(request: &Request) -> bool {
    request.headers.iter().any(|(name, value)| {
        (*name == "sec-fetch-site" && value == "cross-site")
            || (*name == "origin" && value != "https://gang.example")
    })
}

/// In-memory browser deployment and issuer.
struct SimulatedBrowser {
    /// Injected deployment defect.
    behaviour: Behaviour,
    /// Shared state, including the virtual clock.
    state: Arc<Mutex<State>>,
}

impl SimulatedBrowser {
    /// Creates an isolated deployment.
    fn new(behaviour: Behaviour) -> Self {
        Self {
            behaviour,
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    /// Starts a login, retaining any openable renewal cookie.
    fn login(
        &self,
        state: &mut State,
        values: &HashMap<String, String>,
        next: &str,
    ) -> gang_e2e::Result<Answer> {
        let renewal = values
            .get(COOKIES.renewal)
            .and_then(|v| state.sealed.get(v))
            .cloned();
        let offline = self.behaviour.offline != Offline::Never
            && (renewal.is_none() || self.behaviour.relogin == Relogin::AlwaysOffline);
        let id = state.fresh("state");
        state.attempts.insert(
            id.clone(),
            Attempt {
                next: next.to_owned(),
                renewal: if offline { None } else { renewal },
            },
        );
        let mut url = Url::parse("https://issuer.example/authorize")
            .map_err(gang_e2e::E2eError::system_from("parsing provider URL"))?;
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("response_type", "code")
                .append_pair("client_id", CLIENT)
                .append_pair("redirect_uri", "https://gang.example/ui/callback")
                .append_pair("scope", "openid")
                .append_pair("state", &id)
                .append_pair("nonce", "n")
                .append_pair(
                    "code_challenge",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                )
                .append_pair("code_challenge_method", "S256")
                .append_pair("login_hint", "browser");
            if offline {
                q.append_pair("access_type", "offline")
                    .append_pair("prompt", "consent");
            }
        }
        Ok(Answer::empty(302)
            .with_header("location", url.to_string())
            .with_header("set-cookie", cookie(COOKIES.login, &id, "Lax", 600, true)))
    }

    /// Completes a login callback, including a held-token recheck.
    fn callback(
        &self,
        state: &mut State,
        values: &HashMap<String, String>,
        query: &HashMap<String, String>,
    ) -> gang_e2e::Result<Answer> {
        let failure = |reason| {
            clear(
                Answer::empty(302).with_header("location", format!("/ui/error?reason={reason}")),
                COOKIES.login,
                "Lax",
            )
        };
        let Some(id) = query.get("state") else {
            return Ok(failure("failed"));
        };
        if values.get(COOKIES.login) != Some(id) {
            return Ok(failure("failed"));
        }
        let Some(attempt) = state.attempts.remove(id) else {
            return Ok(failure("failed"));
        };
        if query.contains_key("error") {
            return Ok(failure("denied"));
        }
        let Some((subject, offline)) = query.get("code").and_then(|code| state.codes.remove(code))
        else {
            return Ok(failure("failed"));
        };
        let principal = format!("e2e:{subject}");
        let fresh_token =
            if offline && state.policy(&subject) != SubjectPolicy::WithoutRefreshTokens {
                Some(state.issue(&subject))
            } else {
                None
            };
        let renewal = if let Some(held) = attempt.renewal {
            let grant = if self.behaviour.relogin == Relogin::NoRecheck {
                Ok(None)
            } else {
                state.flight(&held.token, self.behaviour)
            };
            if let Ok(rotated) = grant {
                if held.principal != principal {
                    return self.retry_login(state, &attempt.next);
                }
                Some(Renewal {
                    principal: principal.clone(),
                    token: rotated.unwrap_or(held.token),
                    expire: state.now + MAX_AGE,
                })
            } else {
                return self.retry_login(state, &attempt.next);
            }
        } else {
            fresh_token.map(|token| Renewal {
                principal: principal.clone(),
                token,
                expire: state.now + MAX_AGE,
            })
        };
        let answer = clear(
            Answer::empty(302).with_header("location", attempt.next),
            COOKIES.login,
            "Lax",
        );
        let answer = state.session(answer, &principal);
        Ok(if let Some(renewal) = renewal {
            state.seal(answer, renewal, self.behaviour)
        } else {
            clear(answer, COOKIES.renewal, "Strict")
        })
    }

    /// Clears the old login and renewal before starting a fresh login.
    fn retry_login(&self, state: &mut State, next: &str) -> gang_e2e::Result<Answer> {
        let mut answer = self.login(state, &HashMap::new(), next)?;
        answer.headers.insert(
            0,
            (
                "set-cookie".to_owned(),
                cookie(COOKIES.login, "", "Lax", 0, true),
            ),
        );
        Ok(clear(answer, COOKIES.renewal, "Strict"))
    }

    /// Handles a renewal request in protocol order.
    fn renew(
        &self,
        state: &mut State,
        request: &Request,
        values: &HashMap<String, String>,
    ) -> Answer {
        if self.behaviour.cross_site != CrossSite::Renews && cross_site(request) {
            return Answer::empty(403);
        }
        let Some(held) = values
            .get(COOKIES.renewal)
            .and_then(|v| state.sealed.get(v))
            .cloned()
        else {
            let status = if self.behaviour.bare == NoCookie::Forbidden {
                403
            } else {
                401
            };
            return clear(Answer::empty(status), COOKIES.renewal, "Strict");
        };
        if held.expire <= state.now && self.behaviour.max_age != MaxAgeCheck::Ignored {
            let answer = Answer::empty(401);
            return if self.behaviour.max_age == MaxAgeCheck::ClearsCookie {
                clear(answer, COOKIES.renewal, "Strict")
            } else {
                answer
            };
        }
        let Ok(rotated) = state.flight(&held.token, self.behaviour) else {
            let answer = Answer::empty(401);
            return if self.behaviour.suspension == Suspension::KeepsCookie {
                answer
            } else {
                clear(answer, COOKIES.renewal, "Strict")
            };
        };
        if state.revoked.contains(&held.principal)
            && self.behaviour.revocation != RevokedRenewal::Renews
        {
            return clear(Answer::empty(403), COOKIES.renewal, "Strict");
        }
        if self.behaviour.renewal == RenewalSignal::Reauthorizes {
            let subject = held
                .principal
                .strip_prefix("e2e:")
                .unwrap_or(&held.principal);
            state
                .authorizations
                .entry(subject.to_owned())
                .or_default()
                .push(AuthorizationRecord {
                    client_id: CLIENT.to_owned(),
                    offline: false,
                    consent: false,
                });
        }
        let answer = state.session(Answer::empty(204), &held.principal);
        if let Some(token) = rotated {
            state.seal(answer, Renewal { token, ..held }, self.behaviour)
        } else {
            answer
        }
    }
}

impl Browser for SimulatedBrowser {
    type Stream = SimStream;

    fn endpoint(&self) -> gang_e2e::Result<Url> {
        Url::parse("https://gang.example/")
            .map_err(gang_e2e::E2eError::system_from("parsing endpoint URL"))
    }

    fn request(&self, request: &Request) -> gang_e2e::Result<Answer> {
        let url = Url::parse(&format!("https://gang.example{}", request.target))
            .map_err(gang_e2e::E2eError::system_from("parsing request target"))?;
        let query: HashMap<String, String> = url
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        let values = cookies(request);
        let mut state = self.state.lock();
        let answer = match (request.method.clone(), url.path()) {
            (Method::GET, "/ui/login") => self.login(
                &mut state,
                &values,
                query.get("next").map_or("/ui", String::as_str),
            )?,
            (Method::GET, "/ui/callback") => self.callback(&mut state, &values, &query)?,
            (Method::POST, "/ui/renew") => self.renew(&mut state, request, &values),
            (Method::POST, "/ui/logout") => {
                if cross_site(request) {
                    Answer::empty(403)
                } else {
                    let answer = clear(Answer::empty(204), COOKIES.session, "Strict");
                    if self.behaviour.logout == Logout::ClearsBoth {
                        clear(answer, COOKIES.renewal, "Strict")
                    } else {
                        answer
                    }
                }
            }
            (Method::GET, "/v1/state") => {
                let accepted = match &request.presented {
                    Presented::Bearer(value) => {
                        self.behaviour.sealed == Sealed::AcceptedAsBearer
                            && state.sealed.contains_key(value)
                    }
                    _ => values
                        .get(COOKIES.session)
                        .and_then(|v| state.live(v))
                        .is_some(),
                };
                Answer::empty(if accepted { 200 } else { 401 })
            }
            (Method::POST, "/v1/session:authenticate") => {
                let proof = request
                    .body
                    .as_ref()
                    .and_then(|body| serde_json::from_slice::<serde_json::Value>(&body.bytes).ok())
                    .and_then(|v| v.get("proof").and_then(|p| p.as_str()).map(str::to_owned));
                let accepted = self.behaviour.sealed == Sealed::AcceptedByAuthenticate
                    && proof.as_ref().is_some_and(|v| state.sealed.contains_key(v));
                Answer::empty(if accepted { 200 } else { 401 })
            }
            _ => Answer::empty(404),
        };
        Ok(answer)
    }

    fn visit(&self, url: &Url) -> gang_e2e::Result<Answer> {
        let query: HashMap<String, String> = url
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        let subject = query.get("login_hint").cloned().unwrap_or_default();
        let offline = query.get("access_type").is_some_and(|v| v == "offline");
        let consent = query.get("prompt").is_some_and(|v| v == "consent");
        let mut state = self.state.lock();
        state
            .authorizations
            .entry(subject.clone())
            .or_default()
            .push(AuthorizationRecord {
                client_id: query.get("client_id").cloned().unwrap_or_default(),
                offline,
                consent,
            });
        let mut redirect = Url::parse(
            query
                .get("redirect_uri")
                .map_or("https://gang.example/ui/callback", String::as_str),
        )
        .map_err(gang_e2e::E2eError::system_from("parsing callback URL"))?;
        if state.policy(&subject) == SubjectPolicy::Suspended {
            redirect
                .query_pairs_mut()
                .append_pair("error", "access_denied");
        } else {
            let code = state.fresh("code");
            state.codes.insert(code.clone(), (subject, offline));
            redirect.query_pairs_mut().append_pair("code", &code);
        }
        redirect
            .query_pairs_mut()
            .append_pair("state", query.get("state").map_or("", String::as_str));
        Ok(Answer::empty(302).with_header("location", redirect.to_string()))
    }

    fn open_state_stream(&self, cookie: &SessionCookie) -> gang_e2e::Result<Self::Stream> {
        let session = self.state.lock().live(&cookie.value);
        let status = if session.is_some() && cookie.name == COOKIES.session {
            200
        } else {
            401
        };
        let (principal, exp) = session.unwrap_or_default();
        Ok(SimStream {
            state: Arc::clone(&self.state),
            principal,
            exp,
            status,
            opened: false,
            ended: false,
        })
    }

    fn revoke(&self, subject: &str) -> gang_e2e::Result<()> {
        self.state.lock().revoked.insert(format!("e2e:{subject}"));
        Ok(())
    }

    fn restore(&self, subject: &str) -> gang_e2e::Result<()> {
        self.state.lock().revoked.remove(&format!("e2e:{subject}"));
        Ok(())
    }

    fn authorizations(&self, subject: &str) -> Vec<AuthorizationRecord> {
        self.state
            .lock()
            .authorizations
            .get(subject)
            .cloned()
            .unwrap_or_default()
    }

    fn refreshes(&self, subject: &str) -> Vec<RefreshRecord> {
        self.state
            .lock()
            .refreshes
            .get(subject)
            .cloned()
            .unwrap_or_default()
    }

    fn set_policy(&self, subject: &str, policy: SubjectPolicy) {
        self.state
            .lock()
            .policies
            .insert(subject.to_owned(), policy);
    }
}

/// A state stream driven by the virtual clock.
struct SimStream {
    /// Shared deployment state.
    state: Arc<Mutex<State>>,
    /// Authenticated principal.
    principal: String,
    /// Session expiry.
    exp: u64,
    /// HTTP response status.
    status: u16,
    /// Whether the snapshot was emitted.
    opened: bool,
    /// Whether the closed frame was emitted.
    ended: bool,
}

impl EventStream for SimStream {
    fn status(&self) -> u16 {
        self.status
    }
    fn content_type(&self) -> Option<&str> {
        (self.status == 200).then_some("text/event-stream")
    }
    fn next_event(&mut self) -> gang_e2e::Result<Option<Frame>> {
        self.next_event_within(Duration::MAX)
    }
    fn next_event_within(&mut self, _budget: Duration) -> gang_e2e::Result<Option<Frame>> {
        if self.status != 200 || self.ended {
            return Ok(None);
        }
        if !self.opened {
            self.opened = true;
            return Ok(Some(Frame::Snapshot(state_frame(
                0,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                0,
            ))));
        }
        self.ended = true;
        let mut state = self.state.lock();
        let reason = if state.revoked.contains(&self.principal) {
            ClosedReason::Revoked
        } else {
            state.now = state.now.max(self.exp);
            ClosedReason::Expired
        };
        Ok(Some(Frame::Closed(Closed {
            reason,
            retry_delay: None,
        })))
    }
}

/// Runs the sessions scenario with the selected defect.
fn sessions_checks(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    sessions(
        &SimulatedBrowser::new(behaviour),
        Settings::default().history_timeout,
    )
}

/// The compliant deployment passes all checks.
#[test]
fn sessions_is_compliant() {
    assert_compliant(&sessions_checks(Behaviour::COMPLIANT).expect("the sessions scenario runs"));
}

flaw!(never_offline_fails: sessions_checks(offline = Offline::Never) => fails [names::OFFLINE_SIGN_IN, names::RENEWAL_COOKIE_SET]);
flaw!(lax_renewal_cookie_fails: sessions_checks(cookie = RenewalCookie::Lax) => only [names::RENEWAL_COOKIE_SET, names::CONCURRENT_RENEWALS_SHARE_ONE_GRANT, names::SIGN_IN_WITHOUT_CONSENT]);
flaw!(script_readable_renewal_cookie_fails: sessions_checks(cookie = RenewalCookie::NotHttpOnly) => only [names::RENEWAL_COOKIE_SET, names::CONCURRENT_RENEWALS_SHARE_ONE_GRANT, names::SIGN_IN_WITHOUT_CONSENT]);
flaw!(cross_site_renewal_fails: sessions_checks(cross_site = CrossSite::Renews) => fails [names::CROSS_SITE_RENEWAL_REFUSED]);
flaw!(forbidden_without_cookie_fails: sessions_checks(bare = NoCookie::Forbidden) => only [names::RENEWAL_NEEDS_COOKIE, names::LOGOUT_CLEARS_BOTH]);
flaw!(sealed_bearer_fails: sessions_checks(sealed = Sealed::AcceptedAsBearer) => only [names::SEALED_COOKIE_NOT_A_CREDENTIAL]);
flaw!(sealed_proof_fails: sessions_checks(sealed = Sealed::AcceptedByAuthenticate) => only [names::SEALED_COOKIE_NOT_A_CREDENTIAL]);
flaw!(grant_per_renewal_fails: sessions_checks(flights = Flights::GrantPerRenewal) => fails [names::CONCURRENT_RENEWALS_SHARE_ONE_GRANT]);
flaw!(reauthorizing_renewal_fails: sessions_checks(renewal = RenewalSignal::Reauthorizes) => only [names::RENEWAL_SILENT]);
flaw!(ignored_max_age_fails: sessions_checks(max_age = MaxAgeCheck::Ignored) => only [names::MAX_AGE_ENDS_RENEWAL]);
flaw!(cleared_at_max_age_fails: sessions_checks(max_age = MaxAgeCheck::ClearsCookie) => only [names::MAX_AGE_ENDS_RENEWAL, names::SIGN_IN_WITHOUT_CONSENT]);
flaw!(always_offline_fails: sessions_checks(relogin = Relogin::AlwaysOffline) => only [names::SIGN_IN_WITHOUT_CONSENT]);
flaw!(no_recheck_fails: sessions_checks(relogin = Relogin::NoRecheck) => only [names::SIGN_IN_WITHOUT_CONSENT]);
flaw!(revoked_renewal_fails: sessions_checks(revocation = RevokedRenewal::Renews) => only [names::REVOCATION_ENDS_RENEWAL]);
flaw!(logout_keeps_renewal_fails: sessions_checks(logout = Logout::KeepsRenewal) => only [names::LOGOUT_CLEARS_BOTH]);
flaw!(suspension_keeps_cookie_fails: sessions_checks(suspension = Suspension::KeepsCookie) => only [names::SUSPENSION_ENDS_RENEWAL]);
