//! The browser sign-in routes under `/ui`: they start a login with an interactive identity
//! provider, complete it from the provider's redirect, and end a session.

use std::net::IpAddr;
use std::ops::ControlFlow;
use std::sync::Arc;

use gang_clock::Clock;
use gang_spi::{
    ApiRequestBody, ApiResponseBody, AuthenticationPort, CredentialLifecyclePort, LoginGrant,
    LoginPort, LoginRequest, PortError, ProofVerifierPort, Rejection, TokenIssue, full_body,
};
use gang_types::{Principal, ProviderName, RefreshToken};
use gang_wire::{DEVICE_APPROVED_PATH, ERROR_PATH, UiRoute};
use http::HeaderMap;
use http::header::{ALLOW, HeaderValue, LOCATION, ORIGIN, RETRY_AFTER, SET_COOKIE};
use http::{Method, Request, Response, StatusCode};
use parking_lot::Mutex;
use tracing::{info, warn};

use crate::attempts::{Attempt, LoginAttempts, Purpose};
use crate::client_address::ClientAddress;
use crate::cookies::cookie_value;
use crate::core::Core;
use crate::deployment::Stack;
use crate::device_logins::{Approval, MountedDevice, UserCode};
use crate::rate_limit::RateLimiter;
use crate::renewal_flights::{RenewalFailure, RenewalFlights};
use crate::renewal_seal::{Renewal, RenewalSeal};
use crate::responses::empty;
use crate::settings::LoginSettings;

/// The page a sign-in lands on when no `next` path is safe to return to.
const UI_HOME: &str = "/ui";

/// The browser routes of the session: logout and, when offered, the sign-in.
pub(crate) struct SessionRoutes<K: Stack>(Arc<Shared<K>>);

/// The state every clone of a [`SessionRoutes`] shares.
struct Shared<K: Stack> {
    core: Arc<Core<K>>,
    login: Option<MountedLogin<K::Clock, K::Identity>>,
    /// The maximum accepted browser request body.
    max_request_size: usize,
}

/// The sign-in routes and the verifier of their interactive provider.
struct MountedLogin<C: Clock, V> {
    settings: LoginSettings,
    /// The verified interactive provider's verifier.
    verifier: Arc<V>,
    attempts: Mutex<LoginAttempts>,
    /// The shared limiter for sign-in and callback requests.
    limiter: Arc<RateLimiter<Arc<C>>>,
    /// The authenticated encryption for renewal cookies.
    seal: RenewalSeal,
    /// Shared refresh grants for concurrent and recently started renewals.
    flights: RenewalFlights,
    /// The device authorization service shared with the API, when enabled.
    device: Option<Arc<MountedDevice<C, V>>>,
}

impl<K: Stack> Clone for SessionRoutes<K> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<K: Stack> SessionRoutes<K> {
    /// Builds the routes. The sign-in routes are mounted only when `login` is offered, the client
    /// plugin issues sessions, and the login's provider is interactive.
    #[must_use]
    pub(crate) fn new(
        core: Arc<Core<K>>,
        login: Option<LoginSettings>,
        limiter: Arc<RateLimiter<Arc<K::Clock>>>,
        max_request_size: usize,
    ) -> Self {
        let login = login.and_then(|settings| {
            if !core.clients.capabilities().sessions {
                return None;
            }
            let provider = core.host.providers.get(&settings.provider)?;
            provider.login()?;
            let device = settings.device.as_ref().and_then(|device| {
                let uri = core.host.endpoint.url().join("/ui/device").ok()?;
                Some(Arc::new(MountedDevice::new(
                    device,
                    settings.provider.clone(),
                    settings.client.clone(),
                    Arc::clone(&provider.verifier),
                    uri,
                    Arc::clone(&core.host.clock),
                )))
            });
            Some(MountedLogin {
                attempts: Mutex::new(LoginAttempts::new(settings.max_pending, settings.ttl.get())),
                seal: RenewalSeal::new(&settings.client.secret, core.host.endpoint.origin()),
                flights: RenewalFlights::new(settings.max_pending, settings.renewal_reuse.get()),
                verifier: Arc::clone(&provider.verifier),
                device,
                limiter,
                settings,
            })
        });
        if login.is_some() {
            core.cookies.warn_insecure();
        }
        Self(Arc::new(Shared {
            core,
            login,
            max_request_size,
        }))
    }

    /// The identity provider the browser signs in with; `None` when the sign-in is not mounted.
    #[must_use]
    pub(crate) fn login_provider(&self) -> Option<&ProviderName> {
        self.0
            .login
            .as_ref()
            .map(|mounted| &mounted.settings.provider)
    }

    /// The device login shared with the API, when mounted.
    pub(crate) fn device(&self) -> Option<Arc<MountedDevice<K::Clock, K::Identity>>> {
        self.0.login.as_ref().and_then(|login| login.device.clone())
    }

    /// Answers a browser session request.
    pub(crate) async fn answer(
        &self,
        route: UiRoute,
        request: Request<ApiRequestBody>,
    ) -> Response<ApiResponseBody> {
        let (parts, body) = request.into_parts();
        let method = parts.method.clone();
        let client = parts
            .extensions
            .get::<ClientAddress>()
            .map(|address| address.0);
        let mut response = match route {
            UiRoute::Logout => {
                if method == Method::POST {
                    self.logout(&parts.headers)
                } else {
                    method_not_allowed("POST")
                }
            }
            UiRoute::Login => match &self.0.login {
                None => empty(StatusCode::NOT_FOUND),
                Some(mounted) if method == Method::GET => {
                    self.start(mounted, parts.uri.query(), &parts.headers, client)
                        .await
                }
                Some(_) => method_not_allowed("GET"),
            },
            UiRoute::Callback => match &self.0.login {
                None => empty(StatusCode::NOT_FOUND),
                Some(mounted) if method == Method::GET => {
                    self.callback(mounted, parts.uri.query(), &parts.headers, client)
                        .await
                }
                Some(_) => method_not_allowed("GET"),
            },
            UiRoute::Renew => match &self.0.login {
                None => empty(StatusCode::NOT_FOUND),
                Some(mounted) if method == Method::POST => {
                    self.renew(mounted, &parts.headers, client).await
                }
                Some(_) => method_not_allowed("POST"),
            },
            UiRoute::Device => {
                if method == Method::GET || method == Method::HEAD {
                    return gang_ui::respond(&method, parts.uri.path(), &parts.headers)
                        .map(full_body);
                }
                if method != Method::POST {
                    method_not_allowed("GET, HEAD, POST")
                } else if let Some(mounted) = &self.0.login
                    && mounted.device.is_some()
                {
                    self.start_device(mounted, &parts.headers, body, client)
                        .await
                } else {
                    empty(StatusCode::NOT_FOUND)
                }
            }
        };
        if method == Method::HEAD {
            *response.body_mut() = full_body(Vec::new());
        }
        crate::responses::no_store(response.headers_mut());
        response
    }

    /// Ends the session when the request comes from the dashboard; any other request is refused
    /// before a cookie is cleared.
    fn logout(&self, headers: &HeaderMap) -> Response<ApiResponseBody> {
        let core = &self.0.core;
        if !is_same_origin_request(headers, core.host.endpoint.origin()) {
            return empty(StatusCode::FORBIDDEN);
        }
        if let (Ok(session), Ok(renewal)) = (
            core.cookies.clear_session_header(),
            core.cookies.clear_renewal_header(),
        ) {
            let mut response = empty(StatusCode::NO_CONTENT);
            response.headers_mut().append(SET_COOKIE, session);
            response.headers_mut().append(SET_COOKIE, renewal);
            response
        } else {
            tracing::error!("session cookies cannot be cleared");
            empty(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }

    /// The header that clears the sign-in cookie, or the failure when it cannot be built.
    fn login_clearing(&self) -> Result<HeaderValue, LoginFailure> {
        self.0
            .core
            .cookies
            .clear_login_header()
            .map_err(|_| LoginFailure::failed("sign-in cookie cannot be cleared"))
    }

    async fn start(
        &self,
        mounted: &MountedLogin<K::Clock, K::Identity>,
        query: Option<&str>,
        headers: &HeaderMap,
        client: Option<IpAddr>,
    ) -> Response<ApiResponseBody> {
        if let Err(delay) = mounted.limiter.check(client) {
            return rate_limited(delay);
        }
        let requested = crate::query::query_param(query, "next");
        let next = safe_next(requested.as_deref());
        let renewal = cookie_value(headers, self.0.core.cookies.renewal)
            .and_then(|cookie| mounted.seal.open(cookie).ok())
            .filter(|renewal| renewal.provider == mounted.settings.provider);
        match self
            .begin(mounted, Purpose::Session { next, renewal })
            .await
        {
            Ok(response) => response,
            Err(failure) => error_redirect(failure, Vec::new()),
        }
    }

    /// Starts an offline sign-in to approve a pending device authorization.
    async fn start_device(
        &self,
        mounted: &MountedLogin<K::Clock, K::Identity>,
        headers: &HeaderMap,
        body: ApiRequestBody,
        client: Option<IpAddr>,
    ) -> Response<ApiResponseBody> {
        if !is_same_origin_request(headers, self.0.core.host.endpoint.origin()) {
            return empty(StatusCode::FORBIDDEN);
        }
        if let Err(delay) = mounted.limiter.check(client) {
            return rate_limited(delay);
        }
        let Ok(bytes) = crate::api::read_body(body, self.0.max_request_size).await else {
            return empty(StatusCode::BAD_REQUEST);
        };
        let code = form_urlencoded::parse(&bytes)
            .find(|(name, _)| name == "userCode")
            .and_then(|(_, value)| mounted.device.as_ref()?.logins.begin_approval(&value));
        let Some(code) = code else {
            let mut response = redirect(
                HeaderValue::from_static("/ui/device?error=unknown"),
                Vec::new(),
            );
            *response.status_mut() = StatusCode::SEE_OTHER;
            return response;
        };
        match self.begin(mounted, Purpose::Device { code }).await {
            Ok(mut response) => {
                *response.status_mut() = StatusCode::SEE_OTHER;
                response
            }
            Err(failure) => error_redirect(failure, Vec::new()),
        }
    }

    async fn begin(
        &self,
        mounted: &MountedLogin<K::Clock, K::Identity>,
        purpose: Purpose,
    ) -> Result<Response<ApiResponseBody>, LoginFailure> {
        let login = &mounted.settings;
        let core = &self.0.core;
        let offline = !matches!(
            &purpose,
            Purpose::Session {
                renewal: Some(_),
                ..
            }
        );
        let challenge = mounted
            .verifier
            .begin_login(&LoginRequest {
                redirect_uri: core.host.endpoint.callback(),
                client: Some(&login.client),
                offline,
            })
            .await
            .map_err(|error| LoginFailure::of(&error))?;
        let state = challenge.state;
        let location = HeaderValue::try_from(challenge.authorization_url.as_str())
            .map_err(|_| LoginFailure::failed("authorization url is not a header value"))?;
        let cookie = core
            .cookies
            .login_header(&state, login.ttl.get())
            .map_err(|_| LoginFailure::failed("sign-in cookie is not a header value"))?;
        mounted.attempts.lock().insert(
            state,
            Attempt {
                session: challenge.session,
                purpose,
            },
            core.host.clock.monotonic(),
        );
        Ok(redirect(location, vec![cookie]))
    }

    async fn callback(
        &self,
        mounted: &MountedLogin<K::Clock, K::Identity>,
        query: Option<&str>,
        headers: &HeaderMap,
        client: Option<IpAddr>,
    ) -> Response<ApiResponseBody> {
        if let Err(delay) = mounted.limiter.check(client) {
            return rate_limited(delay);
        }
        match self.finish(mounted, query, headers, client).await {
            Ok(response) => response,
            Err(failure) => error_redirect(failure, self.login_clearing().into_iter().collect()),
        }
    }

    async fn finish(
        &self,
        mounted: &MountedLogin<K::Clock, K::Identity>,
        query: Option<&str>,
        headers: &HeaderMap,
        client: Option<IpAddr>,
    ) -> Result<Response<ApiResponseBody>, LoginFailure> {
        let login = &mounted.settings;
        let core = &self.0.core;
        let mut callback_url = core.host.endpoint.callback().clone();
        callback_url.set_query(query);
        let state = crate::query::query_param(query, "state");
        let clear = self.login_clearing()?;
        let cookie = cookie_value(headers, core.cookies.login);
        let (Some(state), Some(cookie)) = (state.as_deref(), cookie) else {
            return Err(LoginFailure::failed("sign-in state is missing"));
        };
        if state != cookie {
            return Err(LoginFailure::failed(
                "sign-in state does not match the sign-in cookie",
            ));
        }
        let attempt = mounted
            .attempts
            .lock()
            .take(state, core.host.clock.monotonic())
            .ok_or_else(|| LoginFailure::failed("sign-in attempt is unknown or expired"))?;
        let Attempt { session, purpose } = attempt;
        let grant = mounted
            .verifier
            .complete_login(session, &callback_url)
            .await
            .map_err(|error| LoginFailure::of(&error))?;
        let (next, renewal) = match purpose {
            Purpose::Session { next, renewal } => (next, renewal),
            Purpose::Device { code } => {
                return self
                    .finish_device(mounted, code, grant, clear, client)
                    .await;
            }
        };
        let principal = mounted
            .verifier
            .verify(&grant.proof)
            .await
            .map_err(|error| LoginFailure::of(&error))?;
        let refresh_token = if let Some(renewal) = renewal {
            match self.recheck(mounted, renewal, &principal, &next).await? {
                ControlFlow::Continue(refresh_token) => Some(refresh_token),
                ControlFlow::Break(response) => return Ok(response),
            }
        } else {
            grant.refresh_token
        };
        let issued = core
            .clients
            .issue_session(
                &core.host,
                &TokenIssue {
                    provider: &login.provider,
                    proof: &grant.proof,
                },
            )
            .await
            .map_err(|error| LoginFailure::of(&error))?;
        let session_cookie = core
            .cookies
            .session_header(&issued.token, issued.expires_in)
            .map_err(|_| LoginFailure::failed("session cookie is not a header value"))?;
        let mut cookies = vec![session_cookie, clear];
        if let Some(refresh_token) = refresh_token {
            let renewal = Renewal {
                provider: login.provider.clone(),
                principal: principal.as_str().to_owned(),
                refresh_token,
                expire_time: core.host.clock.wall() + login.session_max_age.get(),
            };
            if let Some(cookie) = self.renewal_cookie(mounted, &renewal) {
                cookies.push(cookie);
            }
        } else {
            warn!(
                provider = %login.provider,
                "no refresh token; check [identity.providers.{}.oidc.login.offline]",
                login.provider
            );
            cookies.push(
                core.cookies
                    .clear_renewal_header()
                    .map_err(|_| LoginFailure::failed("renewal cookie cannot be cleared"))?,
            );
        }
        Ok(redirect(next, cookies))
    }

    /// Approves a device login without issuing a browser session.
    async fn finish_device(
        &self,
        mounted: &MountedLogin<K::Clock, K::Identity>,
        code: UserCode,
        grant: LoginGrant,
        clear: HeaderValue,
        client: Option<IpAddr>,
    ) -> Result<Response<ApiResponseBody>, LoginFailure> {
        if grant.refresh_token.is_none() {
            warn!(
                provider = %mounted.settings.provider,
                "no refresh token; check [identity.providers.{}.oidc.login.offline]",
                mounted.settings.provider
            );
            return Err(LoginFailure::failed("device login has no refresh token"));
        }
        let principal = mounted
            .verifier
            .verify(&grant.proof)
            .await
            .map_err(|error| LoginFailure::of(&error))?;
        let device = mounted
            .device
            .as_ref()
            .ok_or_else(|| LoginFailure::failed("device login is not mounted"))?;
        let principal = principal.as_str().to_owned();
        if !device.logins.approve(
            &code,
            Approval {
                code: code.clone(),
                grant,
                principal: principal.clone(),
            },
        ) {
            return Err(LoginFailure::failed("device login is no longer pending"));
        }
        info!(%code, %principal, approver = ?client, "device login approved");
        Ok(redirect(
            HeaderValue::from_static(DEVICE_APPROVED_PATH),
            vec![clear],
        ))
    }

    /// Rechecks a held renewal against the signed-in principal, restarting offline if invalid.
    async fn recheck(
        &self,
        mounted: &MountedLogin<K::Clock, K::Identity>,
        renewal: Renewal,
        principal: &Principal,
        next: &HeaderValue,
    ) -> Result<ControlFlow<Response<ApiResponseBody>, RefreshToken>, LoginFailure> {
        let core = &self.0.core;
        let refreshed = mounted
            .flights
            .renew(&renewal.refresh_token, core.host.clock.monotonic(), || {
                mounted
                    .verifier
                    .renew(&renewal.refresh_token, Some(&mounted.settings.client))
            })
            .await;
        let valid = if let Ok(refreshed) = &refreshed {
            match mounted.verifier.verify(&refreshed.proof).await {
                Ok(refreshed_principal) => {
                    principal == &refreshed_principal && principal.as_str() == renewal.principal
                }
                Err(_) => false,
            }
        } else {
            false
        };
        if !valid {
            let renewal_clear = core
                .cookies
                .clear_renewal_header()
                .map_err(|_| LoginFailure::failed("renewal cookie cannot be cleared"))?;
            let mut response = self
                .begin(
                    mounted,
                    Purpose::Session {
                        next: next.clone(),
                        renewal: None,
                    },
                )
                .await?;
            response.headers_mut().append(SET_COOKIE, renewal_clear);
            return Ok(ControlFlow::Break(response));
        }
        Ok(ControlFlow::Continue(
            refreshed
                .ok()
                .and_then(|grant| grant.refresh_token)
                .unwrap_or(renewal.refresh_token),
        ))
    }

    /// Seals a renewal and formats its cookie, logging failures without issuing the cookie.
    fn renewal_cookie(
        &self,
        mounted: &MountedLogin<K::Clock, K::Identity>,
        renewal: &Renewal,
    ) -> Option<HeaderValue> {
        let sealed = match mounted.seal.seal(renewal) {
            Ok(sealed) => sealed,
            Err(error) => {
                warn!(%error, "renewal cookie cannot be sealed");
                return None;
            }
        };
        match self.0.core.cookies.renewal_header(&sealed) {
            Ok(cookie) => Some(cookie),
            Err(error) => {
                warn!(%error, "renewal cookie cannot be represented");
                None
            }
        }
    }

    /// Renews a session after checking origin, rate limit and the sealed identity.
    async fn renew(
        &self,
        mounted: &MountedLogin<K::Clock, K::Identity>,
        headers: &HeaderMap,
        client: Option<IpAddr>,
    ) -> Response<ApiResponseBody> {
        let core = &self.0.core;
        let login = &mounted.settings;
        if !is_same_origin_request(headers, core.host.endpoint.origin()) {
            return empty(StatusCode::FORBIDDEN);
        }
        if let Err(delay) = mounted.limiter.check(client) {
            return rate_limited(delay);
        }
        let renewal = cookie_value(headers, core.cookies.renewal)
            .and_then(|value| mounted.seal.open(value).ok())
            .filter(|renewal| renewal.provider == login.provider);
        let Some(mut renewal) = renewal else {
            return self.renewal_refusal(StatusCode::UNAUTHORIZED, true);
        };
        if renewal.expire_time <= core.host.clock.wall() {
            return self.renewal_refusal(StatusCode::UNAUTHORIZED, false);
        }
        let grant = match mounted
            .flights
            .renew(&renewal.refresh_token, core.host.clock.monotonic(), || {
                mounted
                    .verifier
                    .renew(&renewal.refresh_token, Some(&login.client))
            })
            .await
        {
            Ok(grant) => grant,
            Err(RenewalFailure::Rejected) => {
                return self.renewal_refusal(StatusCode::UNAUTHORIZED, true);
            }
            Err(RenewalFailure::Unavailable) => {
                return self.renewal_refusal(StatusCode::SERVICE_UNAVAILABLE, false);
            }
        };
        let principal = match mounted.verifier.verify(&grant.proof).await {
            Ok(principal) => principal,
            Err(error) => {
                let (status, clear) = classify_renewal_error(&error);
                return self.renewal_refusal(status, clear);
            }
        };
        if principal.as_str() != renewal.principal {
            return self.renewal_refusal(StatusCode::UNAUTHORIZED, true);
        }
        let issued = match core
            .clients
            .issue_session(
                &core.host,
                &TokenIssue {
                    provider: &login.provider,
                    proof: &grant.proof,
                },
            )
            .await
        {
            Ok(issued) => issued,
            Err(error) => {
                let (status, clear) = classify_renewal_error(&error);
                return self.renewal_refusal(status, clear);
            }
        };
        let session = match core
            .cookies
            .session_header(&issued.token, issued.expires_in)
        {
            Ok(session) => session,
            Err(error) => {
                tracing::error!(%error, "session cookie cannot be represented");
                return empty(StatusCode::INTERNAL_SERVER_ERROR);
            }
        };
        let mut response = empty(StatusCode::NO_CONTENT);
        response.headers_mut().append(SET_COOKIE, session);
        if let Some(rotated) = grant.refresh_token {
            renewal.refresh_token = rotated;
            if let Some(cookie) = self.renewal_cookie(mounted, &renewal) {
                response.headers_mut().append(SET_COOKIE, cookie);
            }
        }
        response
    }

    /// Builds a renewal refusal, clearing its cookie only for permanent failures.
    fn renewal_refusal(&self, status: StatusCode, clear: bool) -> Response<ApiResponseBody> {
        let mut response = empty(status);
        if clear {
            match self.0.core.cookies.clear_renewal_header() {
                Ok(cookie) => {
                    response.headers_mut().append(SET_COOKIE, cookie);
                }
                Err(error) => {
                    tracing::error!(%error, "renewal cookie cannot be cleared");
                    return empty(StatusCode::INTERNAL_SERVER_ERROR);
                }
            }
        }
        response
    }
}

/// Classifies verification and session-issuance failures for renewal.
fn classify_renewal_error(error: &PortError) -> (StatusCode, bool) {
    match error {
        PortError::Rejected {
            reason: Rejection::PolicyDenied | Rejection::PrincipalRevoked,
            ..
        } => (StatusCode::FORBIDDEN, true),
        PortError::Rejected { .. } => (StatusCode::UNAUTHORIZED, true),
        _ => (StatusCode::SERVICE_UNAVAILABLE, false),
    }
}

/// An empty rate-limit refusal with the rounded retry delay.
fn rate_limited(delay: std::time::Duration) -> Response<ApiResponseBody> {
    let mut response = empty(StatusCode::TOO_MANY_REQUESTS);
    response.headers_mut().insert(
        RETRY_AFTER,
        HeaderValue::from(crate::api_error::retry_seconds(delay)),
    );
    response
}

/// Why a sign-in failed. Only the reason reaches the browser.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoginFailure {
    /// The identity provider's policy does not admit the caller, or its principal is revoked.
    Denied,
    /// The sign-in did not complete.
    Failed,
    /// A dependency of the sign-in is down.
    Unavailable,
}

impl LoginFailure {
    /// Classifies a port error from the sign-in and logs it.
    fn of(error: &PortError) -> Self {
        match error {
            PortError::Rejected {
                reason: Rejection::PolicyDenied | Rejection::PrincipalRevoked,
                ..
            } => {
                info!(error = %gang_types::error_chain(error), "browser sign-in denied");
                Self::Denied
            }
            PortError::Unavailable(_) => {
                warn!(error = %gang_types::error_chain(error), "browser sign-in unavailable");
                Self::Unavailable
            }
            _ => {
                warn!(error = %gang_types::error_chain(error), "browser sign-in failed");
                Self::Failed
            }
        }
    }

    /// A failure found without a port error, logged with `detail`.
    fn failed(detail: &'static str) -> Self {
        warn!(detail, "browser sign-in failed");
        Self::Failed
    }

    /// The reason the error page names.
    const fn reason(self) -> &'static str {
        match self {
            Self::Denied => "denied",
            Self::Failed => "failed",
            Self::Unavailable => "unavailable",
        }
    }
}

/// The safe dashboard path to redirect to, or `/ui` when `next` is unsafe.
fn safe_next(next: Option<&str>) -> HeaderValue {
    next.filter(|value| is_safe_next(value))
        .and_then(|value| HeaderValue::from_str(value).ok())
        .unwrap_or(HeaderValue::from_static(UI_HOME))
}

/// Whether `value` is a path under `/ui` that is safe to redirect to.
fn is_safe_next(value: &str) -> bool {
    let Some(rest) = value.strip_prefix(UI_HOME) else {
        return false;
    };
    let path = value.split_once('?').map_or(value, |(path, _)| path);
    matches!(rest.chars().next(), None | Some('/' | '?'))
        && !value.contains("//")
        && !value.contains('\\')
        && value.bytes().all(|byte| (b'!'..=b'~').contains(&byte))
        && !path.split('/').any(is_dot_dot)
}

/// Whether a logout request comes from the dashboard: its `Sec-Fetch-Site` is `same-origin`, or
/// its `Origin` is exactly `endpoint_origin`.
fn is_same_origin_request(headers: &HeaderMap, endpoint_origin: &str) -> bool {
    let same_site = headers
        .get("sec-fetch-site")
        .is_some_and(|site| site == "same-origin");
    same_site
        || headers
            .get(ORIGIN)
            .is_some_and(|origin| origin == endpoint_origin)
}

/// Whether a path segment is `..`, with its dots possibly percent-encoded as `%2e` or `%2E`.
fn is_dot_dot(segment: &str) -> bool {
    segment == ".."
        || segment.eq_ignore_ascii_case(".%2e")
        || segment.eq_ignore_ascii_case("%2e.")
        || segment.eq_ignore_ascii_case("%2e%2e")
}

/// A `405` naming the methods the path accepts.
fn method_not_allowed(allow: &'static str) -> Response<ApiResponseBody> {
    let mut response = empty(StatusCode::METHOD_NOT_ALLOWED);
    response
        .headers_mut()
        .insert(ALLOW, HeaderValue::from_static(allow));
    response
}

/// A `302` to `location` that sets `set_cookies`.
fn redirect(location: HeaderValue, set_cookies: Vec<HeaderValue>) -> Response<ApiResponseBody> {
    let mut response = empty(StatusCode::FOUND);
    response.headers_mut().insert(LOCATION, location);
    for cookie in set_cookies {
        response.headers_mut().append(SET_COOKIE, cookie);
    }
    response
}

/// A `302` to the error page, naming only the reason.
fn error_redirect(
    failure: LoginFailure,
    set_cookies: Vec<HeaderValue>,
) -> Response<ApiResponseBody> {
    let location = HeaderValue::from_str(&format!("{ERROR_PATH}?reason={}", failure.reason()))
        .unwrap_or_else(|_| HeaderValue::from_static(ERROR_PATH));
    redirect(location, set_cookies)
}

#[cfg(test)]
mod tests {
    use super::safe_next;

    #[test]
    fn safe_next_keeps_ui_paths_and_rejects_others() {
        for value in [
            "/ui",
            "/ui/",
            "/ui/workers/w1",
            "/ui?tab=1",
            "/ui/a?b=c",
            "/ui/a.b",
            "/ui/%2ea",
        ] {
            assert_eq!(safe_next(Some(value)), value);
        }
        for value in [
            "",
            "ui",
            "/",
            "/v1/state",
            "//evil.example",
            "/ui//x",
            "https://evil.example",
            "/ui\\x",
            "/uix",
            "/ui/../v1",
            "/ui/%2e%2e/v1",
            "/ui/.%2E/v1",
            "/ui/%2E./v1",
            "/ui/%2E%2e",
            "/ui\r\nSet-Cookie: x=y",
            "/ui\0",
        ] {
            assert_eq!(safe_next(Some(value)), "/ui", "{value:?}");
        }
        assert_eq!(safe_next(None), "/ui");
    }
}
