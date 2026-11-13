//! The browser sign-in over the fakes: the public gate and the session routes, driven the way a
//! browser drives them.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use crate::core::Core;
use crate::gate::Gate;
use crate::host::Host;
use crate::login::SessionRoutes;
use crate::principals::{PrincipalSightings, RevokedPrincipals};
use crate::rate_limit::RateLimiter;
use crate::security::Security;
use crate::{DeviceSettings, LoginSettings, Stack};
use gang_clock::TestClock;
use gang_spi::{ApiRequestBody, ApiResponseBody, LoginClient, Peer, PortError, RequestGate};
use gang_testkit::{FakeRepository, TestSigner};
use gang_types::{
    ClientSecret, DeviceCodeTtl, DevicePollInterval, NonZeroDuration, ProviderName, SessionMaxAge,
};
use gang_wire::UiRoute;
use http::header::{COOKIE, HeaderName, HeaderValue};
use http::{Request, Response, StatusCode};

use crate::test_support::start_clock;
use crate::test_support_api::{ApiReply, api_request, read};
use crate::test_support_deployment::{
    PlainDeployment, TlsDeployment, anonymous, endpoint, mtls, providers, token,
};
use crate::test_support_verifier::FakeVerifier;

/// How a [`Harness`] is built.
#[derive(Debug, Clone, Copy)]
pub struct HarnessOptions {
    /// The public URL the server is reached on.
    pub endpoint: &'static str,
    /// Whether the server offers the browser sign-in.
    pub login: bool,
    /// Whether the server offers device login (ADR-37).
    pub device: bool,
    /// How long a sign-in cookie and a pending sign-in last.
    pub ttl: NonZeroDuration,
    /// How many sign-ins may be pending at once.
    pub max_pending: usize,
    /// The absolute lifetime of a browser session.
    pub session_max_age: SessionMaxAge,
    /// How long a renewal grant is shared.
    pub renewal_reuse: NonZeroDuration,
}

impl Default for HarnessOptions {
    fn default() -> Self {
        Self {
            endpoint: "https://gang.example.test",
            login: true,
            device: false,
            ttl: NonZeroDuration::from_mins(10),
            max_pending: 8,
            session_max_age: SessionMaxAge::from_units(43_200),
            renewal_reuse: NonZeroDuration::from_mins(1),
        }
    }
}

/// A server's browser routes over the fakes, behind the public gate.
pub struct Harness<K: Stack> {
    /// The browser routes under `/ui`.
    pub routes: SessionRoutes<K>,
    /// The public gate that authenticates requests before the routes see them.
    pub gate: Gate<K, K::Clients>,
    /// The core the gate and the routes share.
    pub core: Arc<Core<K>>,
    /// The clock the core and the routes read.
    pub clock: Arc<TestClock>,
    /// The verifier of the interactive provider `example-idp`.
    pub verifier: FakeVerifier,
}

impl Harness<TlsDeployment> {
    /// A harness whose client authentication is the token plugin over `example-idp`.
    #[must_use]
    pub fn token(options: HarnessOptions) -> Self {
        Self::build(token(), mtls(), &options)
    }
}

impl Harness<PlainDeployment> {
    /// A harness whose client authentication is the anonymous plugin.
    #[must_use]
    pub fn anonymous(options: HarnessOptions) -> Self {
        Self::build(anonymous(), anonymous(), &options)
    }
}

impl<K> Harness<K>
where
    K: Stack<
            Signer = TestSigner,
            Clock = TestClock,
            Repository = FakeRepository,
            Identity = FakeVerifier,
        >,
{
    /// Builds the harness over the plugins `clients` and `workers` with the fakes every test
    /// shares.
    fn build(clients: K::Clients, workers: K::Workers, options: &HarnessOptions) -> Self {
        let clock = start_clock();
        let (providers, verifier) = providers();
        let core = Core::new(
            Host {
                repository: FakeRepository::default(),
                signer: Arc::new(TestSigner::authority().expect("the test signer builds")),
                security: Security::Plaintext,
                providers,
                clock: clock.clone(),
                endpoint: endpoint(options.endpoint),
                revoked_principals: RevokedPrincipals::new(),
                sightings: PrincipalSightings::new(
                    Duration::from_mins(1),
                    NonZeroUsize::new(10).expect("ten is non-zero"),
                ),
            },
            clients,
            workers,
        );
        let login = options.login.then(|| LoginSettings {
            provider: ProviderName::try_new("example-idp").expect("test provider name is valid"),
            client: LoginClient {
                id: "web-client".to_owned(),
                secret: ClientSecret::new("dashboard-secret-value"),
            },
            ttl: options.ttl,
            max_pending: NonZeroUsize::new(options.max_pending)
                .expect("test max_pending is non-zero"),
            session_max_age: options.session_max_age,
            renewal_reuse: options.renewal_reuse,
            device: options.device.then(|| DeviceSettings {
                ttl: DeviceCodeTtl::from_units(600),
                poll_interval: DevicePollInterval::from_units(5),
                max_pending: NonZeroUsize::new(1024).expect("1024 is non-zero"),
            }),
        });
        let limiter = Arc::new(RateLimiter::new(
            &gang_config::RateLimitConfig::default(),
            Arc::clone(&clock),
        ));
        Self {
            routes: SessionRoutes::new(Arc::clone(&core), login, limiter, 64 * 1024),
            gate: Gate::public(
                Arc::clone(&core),
                Arc::new(crate::client_address::TrustedProxies::default()),
            ),
            core,
            clock,
            verifier,
        }
    }

    /// Sends `request` through the public gate, then through the browser routes.
    ///
    /// # Errors
    ///
    /// Returns the error the gate refuses the request with.
    pub async fn send(
        &self,
        request: Request<ApiRequestBody>,
    ) -> Result<Response<ApiResponseBody>, PortError> {
        let (mut parts, body) = request.into_parts();
        self.gate.admit(&Peer::default(), &mut parts).await?;
        let request = Request::from_parts(parts, body);
        let route = UiRoute::of(request.uri().path()).expect("a server ui path");
        Ok(self.routes.answer(route, request).await)
    }

    /// Signs a browser in as `proof`: starts the sign-in at `/ui/login`, then answers the
    /// provider's callback with `proof` as the authorization code, carrying the sign-in cookie.
    /// Returns the value of the session cookie the callback sets.
    ///
    /// # Panics
    ///
    /// Panics when the sign-in does not redirect to the provider, or the callback does not set a
    /// session cookie.
    pub async fn signed_in_cookie(&self, proof: &str) -> String {
        let started = self
            .exchange(request("GET", "/ui/login?next=/ui", &[]))
            .await;
        assert_eq!(
            started.status,
            StatusCode::FOUND,
            "the sign-in redirects to the provider"
        );
        let state = started
            .set_cookie_value(self.core.cookies.login)
            .expect("the sign-in sets the login cookie");
        let uri = format!("/ui/callback?state={state}&code={proof}");
        let login_cookie = [(self.core.cookies.login, state.as_str())];
        let callback = self.exchange(request("GET", &uri, &login_cookie)).await;
        assert_eq!(callback.status, StatusCode::FOUND, "the callback redirects");
        callback
            .set_cookie_value(self.core.cookies.session)
            .expect("the callback sets the session cookie")
    }

    /// Sends `request` and reads the reply.
    async fn exchange(&self, request: Request<ApiRequestBody>) -> ApiReply {
        let response = self
            .send(request)
            .await
            .expect("the public gate admits the browser request");
        read(response).await
    }
}

/// A request with `method` for `uri`, carrying `cookies` as one `Cookie` header when any are
/// given.
///
/// # Panics
///
/// Panics when `method` or `uri` is not a valid request line, or a cookie is not visible ASCII.
#[must_use]
pub fn request(method: &str, uri: &str, cookies: &[(&str, &str)]) -> Request<ApiRequestBody> {
    build_request(method, uri, cookies, None)
}

/// Like [`request`], with one extra header `name: value`.
///
/// # Panics
///
/// Panics when `method`, `uri`, `name` or a value is not valid for a request.
#[must_use]
pub fn request_with_header(
    method: &str,
    uri: &str,
    cookies: &[(&str, &str)],
    name: &str,
    value: &str,
) -> Request<ApiRequestBody> {
    build_request(method, uri, cookies, Some((name, value)))
}

/// Builds the request of [`request`] and [`request_with_header`].
fn build_request(
    method: &str,
    uri: &str,
    cookies: &[(&str, &str)],
    header: Option<(&str, &str)>,
) -> Request<ApiRequestBody> {
    let mut request = api_request(method, uri, None, None);
    let headers = request.headers_mut();
    if !cookies.is_empty() {
        let pairs: Vec<String> = cookies
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect();
        let cookie =
            HeaderValue::from_str(&pairs.join("; ")).expect("test cookies are visible ascii");
        headers.insert(COOKIE, cookie);
    }
    if let Some((name, value)) = header {
        let name = HeaderName::try_from(name).expect("test header name is valid");
        let value = HeaderValue::from_str(value).expect("test header value is valid");
        headers.append(name, value);
    }
    request
}
