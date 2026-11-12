//! The server scenarios against a simulated public listener: a compliant listener passes every
//! check, and each simulated flaw fails the checks it breaks.

/// How the simulated listener behaves.
mod behaviour;
/// The simulated routes.
mod listener;
/// The simulated stream, certificates and held connection.
mod stream;

use std::time::Duration;

use behaviour::{
    AssetCaching, AssetEncoding, Behaviour, BodyLimit, Burst, Challenge, CookiePost, Envelope,
    Established, Etag, FirstChain, FirstViewer, LastPage, LeafLifetime, Logout, PageSize,
    RateLimit, Reissue, ReissuedChain, RetryAfter, RetryLine, RouteGate, ShutdownExit,
    ShutdownStreams, TamperedToken, TokenFilter, UiHeaders, UiRefusal, UnknownRoute, ViewerCap,
    Walk,
};
use gang_e2e::{Check, api, certificate_rotation, event_paging, names, request_limits, viewer_cap};
use listener::SimServer;

use super::{assert_compliant, failures};

/// How long a simulated wait may take; the simulation never waits.
const TIMEOUT: Duration = Duration::from_secs(5);

/// Runs the API checks against a simulated listener.
fn api_checks(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    api(&SimServer::new(behaviour), TIMEOUT)
}

/// Runs the request-limit checks against a simulated listener.
fn limit_checks(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    request_limits(&SimServer::new(behaviour))
}

/// Runs the viewer-cap checks against a simulated listener.
fn viewer_checks(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    viewer_cap(&SimServer::new(behaviour), TIMEOUT)
}

/// Runs the certificate-rotation checks against a simulated listener.
fn rotation_checks(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    certificate_rotation(&SimServer::new(behaviour))
}

/// Runs the event-paging checks against a simulated listener.
fn paging_checks(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    event_paging(&SimServer::new(behaviour))
}

#[test]
fn the_api_is_compliant() -> gang_e2e::Result<()> {
    assert_compliant(&api_checks(Behaviour::COMPLIANT)?);
    Ok(())
}

#[test]
fn the_request_limits_are_compliant() -> gang_e2e::Result<()> {
    assert_compliant(&limit_checks(Behaviour::COMPLIANT)?);
    Ok(())
}

#[test]
fn the_viewer_cap_is_compliant() -> gang_e2e::Result<()> {
    assert_compliant(&viewer_checks(Behaviour::COMPLIANT)?);
    Ok(())
}

#[test]
fn the_certificate_rotation_is_compliant() -> gang_e2e::Result<()> {
    assert_compliant(&rotation_checks(Behaviour::COMPLIANT)?);
    Ok(())
}

#[test]
fn the_event_paging_is_compliant() -> gang_e2e::Result<()> {
    assert_compliant(&paging_checks(Behaviour::COMPLIANT)?);
    Ok(())
}

#[test]
fn the_api_emits_every_check_in_order() -> gang_e2e::Result<()> {
    let actual: Vec<String> = api_checks(Behaviour::COMPLIANT)?
        .iter()
        .map(|check| check.name.to_string())
        .collect();
    assert_eq!(
        actual,
        [
            names::api::UNAUTHENTICATED_CHALLENGED,
            names::api::UNKNOWN_ROUTE_NOT_FOUND,
            names::api::UNKNOWN_ROUTE_NEEDS_CREDENTIALS,
            names::api::PAGE_SIZE_CLAMPED,
            names::api::PAGE_WALK_COMPLETE,
            names::api::LAST_PAGE_OMITS_TOKEN,
            names::api::TAMPERED_TOKEN_REFUSED,
            names::api::FOREIGN_FILTER_TOKEN_REFUSED,
            names::api::UI_SECURITY_HEADERS,
            names::api::UI_ASSET_BROTLI,
            names::api::UI_ASSET_IMMUTABLE,
            names::api::UI_ASSET_REVALIDATES,
            names::api::COOKIE_POST_REFUSED,
            names::api::COOKIE_POST_REVOKES_NOTHING,
            names::api::CROSS_ORIGIN_LOGOUT_REFUSED,
            names::api::ENVELOPE_ON_EVERY_REFUSAL,
            names::api::SHUTDOWN_CLOSES_STREAM,
            names::api::SHUTDOWN_EXITS_ZERO,
        ]
    );
    Ok(())
}

flaw!(
    an_unchallenged_401_fails_the_challenge: api_checks(challenge = Challenge::Silent)
    => only [names::api::UNAUTHENTICATED_CHALLENGED]
);
flaw!(
    a_405_for_an_unknown_route_fails: api_checks(unknown_route = UnknownRoute::MethodNotAllowed)
    => only [names::api::UNKNOWN_ROUTE_NOT_FOUND, names::api::ENVELOPE_ON_EVERY_REFUSAL]
);
flaw!(
    routing_before_authentication_fails: api_checks(route_gate = RouteGate::RouteFirst)
    => only [names::api::UNKNOWN_ROUTE_NEEDS_CREDENTIALS]
);
flaw!(
    refusing_a_large_page_size_fails: api_checks(page_size = PageSize::Refused)
    => only [
        names::api::PAGE_SIZE_CLAMPED,
        names::api::PAGE_WALK_COMPLETE,
        names::api::LAST_PAGE_OMITS_TOKEN
    ]
);
flaw!(
    a_token_that_skips_an_item_fails_the_walk: api_checks(walk = Walk::SkipsOne)
    => only [names::api::PAGE_WALK_COMPLETE]
);
flaw!(
    a_token_on_the_last_page_fails: api_checks(last_page = LastPage::Trailing)
    => only [names::api::PAGE_SIZE_CLAMPED, names::api::LAST_PAGE_OMITS_TOKEN]
);
flaw!(
    restarting_on_a_tampered_token_fails: api_checks(tampered_token = TamperedToken::Restarts)
    => only [names::api::TAMPERED_TOKEN_REFUSED]
);
flaw!(
    accepting_a_foreign_filter_token_fails: api_checks(token_filter = TokenFilter::Ignored)
    => only [names::api::FOREIGN_FILTER_TOKEN_REFUSED]
);
flaw!(
    a_bare_refusal_body_fails_the_envelope: api_checks(envelope = Envelope::Bare)
    => only [
        names::api::UNAUTHENTICATED_CHALLENGED,
        names::api::UNKNOWN_ROUTE_NOT_FOUND,
        names::api::UNKNOWN_ROUTE_NEEDS_CREDENTIALS,
        names::api::TAMPERED_TOKEN_REFUSED,
        names::api::FOREIGN_FILTER_TOKEN_REFUSED,
        names::api::COOKIE_POST_REFUSED,
        names::api::ENVELOPE_ON_EVERY_REFUSAL
    ]
);
flaw!(
    a_dashboard_without_csp_fails: api_checks(ui_headers = UiHeaders::NoCsp)
    => only [names::api::UI_SECURITY_HEADERS]
);
flaw!(
    an_uncompressed_asset_fails: api_checks(asset_encoding = AssetEncoding::Identity)
    => only [names::api::UI_ASSET_BROTLI]
);
flaw!(
    an_uncached_asset_fails: api_checks(asset_caching = AssetCaching::NoStore)
    => only [names::api::UI_ASSET_IMMUTABLE]
);
flaw!(
    ignoring_if_none_match_fails: api_checks(etag = Etag::Ignored)
    => only [names::api::UI_ASSET_REVALIDATES]
);
flaw!(
    a_cookie_admitted_to_post_fails: api_checks(cookie_post = CookiePost::Admitted)
    => only [names::api::COOKIE_POST_REFUSED, names::api::COOKIE_POST_REVOKES_NOTHING]
);
flaw!(
    a_cross_origin_logout_that_clears_fails: api_checks(logout = Logout::AnyOrigin)
    => only [names::api::CROSS_ORIGIN_LOGOUT_REFUSED]
);
flaw!(
    a_stream_dropped_on_shutdown_fails: api_checks(shutdown = ShutdownStreams::Drops)
    => only [names::api::SHUTDOWN_CLOSES_STREAM]
);
flaw!(
    a_failing_exit_after_sigterm_fails: api_checks(exit = ShutdownExit::Failure)
    => only [names::api::SHUTDOWN_EXITS_ZERO]
);
flaw!(
    an_unbounded_body_fails: limit_checks(body_limit = BodyLimit::Ignored)
    => only [names::request_limits::REQUEST_TOO_LARGE]
);
flaw!(
    no_rate_limit_fails: limit_checks(rate_limit = RateLimit::Off)
    => only [names::request_limits::API_RATE_LIMITED, names::request_limits::UI_RATE_LIMITED]
);
flaw!(
    a_short_burst_fails: limit_checks(burst = Burst::Short)
    => only [names::request_limits::BURST_ADMITTED]
);
flaw!(
    a_429_without_retry_after_fails: limit_checks(retry_after = RetryAfter::Missing)
    => only [names::request_limits::API_RATE_LIMITED, names::request_limits::UI_RATE_LIMITED]
);
flaw!(
    an_enveloped_sign_in_refusal_fails: limit_checks(ui_refusal = UiRefusal::Enveloped)
    => only [names::request_limits::UI_RATE_LIMITED]
);
flaw!(
    a_refused_first_viewer_fails: viewer_checks(first_viewer = FirstViewer::Refused)
    => only [names::viewer_cap::FIRST_VIEWER_ADMITTED]
);
flaw!(
    an_unbounded_viewer_count_fails: viewer_checks(viewer_cap = ViewerCap::Unbounded)
    => only [names::viewer_cap::VIEWER_CAP_CLOSED, names::viewer_cap::RETRY_LINE_MATCHES]
);
flaw!(
    a_retry_line_in_seconds_fails: viewer_checks(retry_line = RetryLine::Seconds)
    => only [names::viewer_cap::RETRY_LINE_MATCHES]
);
flaw!(
    a_first_leaf_off_the_pin_fails: rotation_checks(first_chain = FirstChain::Foreign)
    => only [
        names::certificate_rotation::FIRST_LEAF_PINNED,
        names::certificate_rotation::LEAF_LIFETIME,
        names::certificate_rotation::SECOND_LEAF_PINNED,
        names::certificate_rotation::LEAF_REISSUED,
        names::certificate_rotation::ESTABLISHED_KEPT
    ]
);
flaw!(
    a_leaf_never_reissued_fails: rotation_checks(reissue = Reissue::Never)
    => only [names::certificate_rotation::LEAF_REISSUED]
);
flaw!(
    a_reissued_leaf_off_the_pin_fails: rotation_checks(reissued_chain = ReissuedChain::Foreign)
    => only [
        names::certificate_rotation::SECOND_LEAF_PINNED,
        names::certificate_rotation::LEAF_REISSUED
    ]
);
flaw!(
    a_week_long_leaf_fails: rotation_checks(lifetime = LeafLifetime::Week)
    => only [names::certificate_rotation::LEAF_LIFETIME]
);
flaw!(
    a_reissue_that_drops_connections_fails: rotation_checks(established = Established::Dropped)
    => only [names::certificate_rotation::ESTABLISHED_KEPT]
);
flaw!(
    an_event_walk_that_skips_fails: paging_checks(walk = Walk::SkipsOne)
    => only [names::event_paging::WALK_COMPLETE]
);
flaw!(
    a_token_on_the_last_event_page_fails: paging_checks(last_page = LastPage::Trailing)
    => only [names::event_paging::LAST_PAGE_OMITS_TOKEN]
);
