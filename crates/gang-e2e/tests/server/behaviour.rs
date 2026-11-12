//! How the simulated public listener behaves, and the state and fixed data it shares.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use gang_e2e::{CERTIFICATE_TTL, Credentials, REISSUE_AT_PERCENT, SessionCookie};

behaviour! {
    /// A matrix of compliant and intentionally flawed simulation behaviors.
    pub(crate) struct Behaviour {
        /// Behavior for unauthenticated challenges.
        /// Variations in the listener's unauthenticated challenge.
        challenge: Challenge {
            /// 401 answers carry the expected Bearer challenge.
            Bearer,
            /// 401 answers carry no `WWW-Authenticate`.
            Silent,
        },
        /// Behavior for authenticated unknown routes.
        unknown_route: UnknownRoute {
            /// An authenticated unknown route answers 404.
            NotFound,
            /// An authenticated unknown route answers an empty 405.
            MethodNotAllowed,
        },
        /// Behavior for authentication and route-check order.
        route_gate: RouteGate {
            /// Credentials are checked before route existence.
            CredentialsFirst,
            /// An unknown route without credentials answers 404, not 401.
            RouteFirst,
        },
        /// Behavior for oversized page sizes.
        page_size: PageSize {
            /// Page sizes above the maximum are clamped.
            Clamped,
            /// A `pageSize` over 1000 answers 400 `MALFORMED_REQUEST`.
            Refused,
        },
        /// Behavior for page walking.
        walk: Walk {
            /// Page walking returns every item in order.
            Complete,
            /// The first page's token skips one item.
            SkipsOne,
        },
        /// Behavior for last-page tokens.
        last_page: LastPage {
            /// The last page omits its next-page token.
            Omitted,
            /// The last page carries a token to an empty page.
            Trailing,
        },
        /// Behavior for tampered page tokens.
        tampered_token: TamperedToken {
            /// A tampered token is refused.
            Refused,
            /// A token valid for no filter lists from the start.
            Restarts,
        },
        /// Behavior for page-token filter binding.
        token_filter: TokenFilter {
            /// Page tokens are bound to their filter.
            Bound,
            /// A token minted for another filter is accepted.
            Ignored,
        },
        /// Behavior for refusal envelopes.
        envelope: Envelope {
            /// Refusals carry a Google RPC error envelope.
            Google,
            /// Refusals carry `{"message":"refused"}` instead of the envelope.
            Bare,
        },
        /// Behavior for dashboard security headers.
        ui_headers: UiHeaders {
            /// The dashboard carries all required security headers.
            Secured,
            /// /ui/ answers omit `content-security-policy`.
            NoCsp,
        },
        /// Behavior for asset encoding.
        asset_encoding: AssetEncoding {
            /// Assets are served brotli-encoded when accepted.
            Brotli,
            /// Assets are never brotli-encoded.
            Identity,
        },
        /// Behavior for asset caching.
        asset_caching: AssetCaching {
            /// Hashed assets are immutable-cacheable.
            Immutable,
            /// Assets carry `cache-control: no-store`.
            NoStore,
        },
        /// Behavior for `ETag` revalidation.
        etag: Etag {
            /// Matching `ETags` cause revalidation responses.
            Revalidates,
            /// `if-none-match` is ignored.
            Ignored,
        },
        /// Behavior for cookie-only POST authentication.
        cookie_post: CookiePost {
            /// A cookie-only POST is refused.
            Refused,
            /// A POST presenting only the cookie is admitted.
            Admitted,
        },
        /// Behavior for cross-origin logout.
        logout: Logout {
            /// Logout only clears the cookie for same-origin requests.
            SameOriginOnly,
            /// A cross-origin logout clears the cookie.
            AnyOrigin,
        },
        /// Behavior for streams during shutdown.
        shutdown: ShutdownStreams {
            /// `SIGTERM` closes an open stream with a closed event.
            Closes,
            /// An open stream ends without a closed event on `SIGTERM`.
            Drops,
        },
        /// Behavior for the process exit status.
        /// Variations in the process exit status after shutdown.
        exit: ShutdownExit {
            /// The server exits 0 after `SIGTERM`.
            Zero,
            /// The server exits 1 after `SIGTERM`.
            Failure,
        },
        /// Behavior for request body-size enforcement.
        body_limit: BodyLimit {
            /// Oversized request bodies are refused.
            Enforced,
            /// An oversized body is not refused as too large.
            Ignored,
        },
        /// Behavior for rate limiting.
        rate_limit: RateLimit {
            /// Open requests are limited per caller.
            PerCaller,
            /// Nothing is rate limited.
            Off,
        },
        /// Behavior for the request burst size.
        burst: Burst {
            /// The configured caller burst is honoured.
            Honoured,
            /// A caller is refused one request before its burst ends.
            Short,
        },
        /// Behavior for rate-limit retry headers.
        retry_after: RetryAfter {
            /// 429 answers carry a whole-second `Retry-After`.
            WholeSeconds,
            /// 429 answers carry no `Retry-After`.
            Missing,
        },
        /// Behavior for rate-limited UI responses.
        ui_refusal: UiRefusal {
            /// A refused sign-in has an empty body.
            Empty,
            /// A refused sign-in carries the JSON envelope.
            Enveloped,
        },
        /// Behavior for admission of the first viewer.
        first_viewer: FirstViewer {
            /// The first viewer receives a snapshot.
            Admitted,
            /// The first viewer is turned away as if the cap were reached.
            Refused,
        },
        /// Behavior for viewer-cap enforcement.
        viewer_cap: ViewerCap {
            /// Viewers above the configured cap are refused.
            Enforced,
            /// A viewer over the cap gets a snapshot.
            Unbounded,
        },
        /// Behavior for SSE retry-line units.
        retry_line: RetryLine {
            /// The SSE retry line is in milliseconds.
            Millis,
            /// The `retry:` line is in whole seconds.
            Seconds,
        },
        /// Behavior for certificate reissue.
        reissue: Reissue {
            /// The certificate is reissued at its configured threshold.
            Rotates,
            /// The leaf is never reissued.
            Never,
        },
        /// Behavior for the first certificate chain.
        /// Variations in the pin chain of the first certificate.
        first_chain: FirstChain {
            /// the first leaf verifies against the configured pin.
            Pinned,
            /// the first leaf does not verify against the pin.
            Foreign,
        },
        /// Behavior for the reissued certificate chain.
        /// Variations in the pin chain of a reissued certificate.
        reissued_chain: ReissuedChain {
            /// The reissued leaf verifies against the configured pin.
            Pinned,
            /// A reissued leaf does not verify against the pin.
            Foreign,
        },
        /// Behavior for certificate lifetime.
        lifetime: LeafLifetime {
            /// Leaves are valid for the configured TTL.
            Ttl,
            /// Leaves are valid for seven days.
            Week,
        },
        /// Behavior for established connections during reissue.
        established: Established {
            /// An established connection survives certificate reissue.
            Kept,
            /// A connection opened before a reissue is closed by it.
            Dropped,
        },
    }
}

/// Mutable simulated listener state shared across requests and streams.
#[derive(Debug)]
pub(crate) struct SimState {
    /// Whether the server process is running.
    pub(crate) running: bool,
    /// Whether the administrator has been revoked.
    pub(crate) revoked: bool,
    /// Simulated wall clock.
    pub(crate) now: SystemTime,
    /// Time when the first certificate leaf was issued.
    pub(crate) started: SystemTime,
    /// Number of open viewer slots.
    pub(crate) viewers: usize,
    /// Number of open requests per caller.
    pub(crate) spent: HashMap<String, u32>,
}

impl SimState {
    /// Creates the initial simulated listener state.
    pub(crate) fn new() -> Self {
        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        Self {
            running: true,
            revoked: false,
            now: start,
            started: start,
            viewers: 0,
            spent: HashMap::new(),
        }
    }
}

/// Administrator principal used by the simulation.
pub(crate) const ADMIN_PRINCIPAL: &str = "e2e:administrator";
/// Administrator bearer token used by the simulation.
pub(crate) const ADMIN_TOKEN: &str = "administrator-token";
/// Administrator session value used by the simulation.
pub(crate) const ADMIN_SESSION: &str = "administrator-session";
/// Session-cookie name used by the simulation.
pub(crate) const COOKIE_NAME: &str = gang_wire::CookieNames::for_https(false).session;
/// Origin used by the simulated dashboard.
pub(crate) const ORIGIN: &str = "https://simulated.example";
/// `ETag` used by the simulated hashed asset.
pub(crate) const ETAG: &str = "\"0a1b2c3d\"";
/// Hashed stylesheet path served by the simulation.
pub(crate) const STYLESHEET: &str = "/ui/assets/main-0a1b2c3d.css";
/// Hashed script path served by the simulation.
pub(crate) const SCRIPT: &str = "/ui/assets/main-4e5f6a7b.js";

/// Returns the simulation's administrator credentials.
#[must_use]
pub(crate) fn administrator() -> Credentials {
    Credentials {
        principal: ADMIN_PRINCIPAL.to_owned(),
        home: PathBuf::from("/simulated/administrator"),
        token: ADMIN_TOKEN.to_owned(),
        authority: None,
    }
}

/// Returns the simulation's administrator session cookie.
#[must_use]
pub(crate) fn admin_cookie() -> SessionCookie {
    SessionCookie {
        name: COOKIE_NAME.to_owned(),
        value: ADMIN_SESSION.to_owned(),
    }
}

/// Returns the certificate rotation interval.
#[must_use]
pub(crate) fn reissue_interval() -> Duration {
    CERTIFICATE_TTL * REISSUE_AT_PERCENT / 100
}
