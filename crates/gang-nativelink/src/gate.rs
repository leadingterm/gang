//! The request gate: the middleware that runs a listener's [`RequestGate`] over every request
//! before any service sees it, and the denial responses it produces.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use gang_spi::{
    Admitted, GateDecision, LeaderUnavailable, Peer, PortError, Rejection, RequestGate,
    RequestScope,
};
use http::{StatusCode, header};
use http_body_util::Empty;
use tonic::{Code, Status};

use crate::standing::{Standing, hold};

#[derive(Clone)]
pub(crate) struct GateState<G> {
    pub(crate) gate: G,
    pub(crate) scope: RequestScope,
    pub(crate) force_not_leading: bool,
}

/// Runs `gate` on a request and passes the request on when the gate admits it. The request's
/// [`Peer`] comes from the request extensions; a request without one has the default peer. When the
/// request carries its unproven connection's [`Standing`], it counts as in flight until its
/// response body is done. An authenticated admission is the one source of both what follows from
/// it: it proves the connection, after which its requests are no longer tracked, and it gives the
/// services behind the gate the caller's [`Authenticated`](gang_spi::Authenticated) identity in
/// the request extensions.
pub(crate) async fn admit<G: RequestGate>(
    State(state): State<GateState<G>>,
    request: Request,
    next: Next,
) -> Response {
    let peer = request
        .extensions()
        .get::<Peer>()
        .cloned()
        .unwrap_or_default();
    let grpc = is_grpc(&request);
    let standing = request.extensions().get::<Arc<Standing>>().cloned();
    let unproven = standing.as_ref().filter(|standing| !standing.is_proven());
    let busy = unproven.map(Standing::begin);
    let (mut parts, body) = request.into_parts();
    let decision = if state.force_not_leading {
        Ok(GateDecision::NotLeading)
    } else {
        state.gate.admit(state.scope, &peer, &mut parts).await
    };
    let response = match decision {
        Ok(GateDecision::Admit(admitted)) => {
            if unproven.is_some_and(|standing| !standing.admit()) {
                denial(&PortError::unavailable("connection is retiring"), grpc)
            } else {
                if let Admitted::Authenticated(authenticated) = admitted {
                    if let Some(standing) = unproven {
                        standing.prove();
                    }
                    parts.extensions.insert(authenticated);
                }
                let mut response = next.run(Request::from_parts(parts, body)).await;
                if response
                    .extensions_mut()
                    .remove::<LeaderUnavailable>()
                    .is_some()
                    && let Some(standing) = &standing
                {
                    standing.retire_leadership();
                }
                response
            }
        }
        Ok(GateDecision::NotLeading) => {
            if let Some(standing) = &standing {
                standing.retire_leadership();
            }
            denial(&PortError::unavailable("controller is not leading"), grpc)
        }
        Err(error) => {
            if matches!(&error, PortError::Unavailable(_))
                && let Some(standing) = unproven
            {
                standing.request_retry();
            }
            denial(&error, grpc)
        }
    };
    match busy {
        Some(busy) => hold(response, busy),
        None => response,
    }
}

/// Whether `request` is a gRPC request, by its `content-type`.
fn is_grpc(request: &Request) -> bool {
    request
        .headers()
        .get(header::CONTENT_TYPE)
        .is_some_and(|value| value.as_bytes().starts_with(b"application/grpc"))
}

/// The response for a request the gate denied. A gRPC request gets a trailers-only gRPC response
/// with the status in `grpc-status`; any other request gets the HTTP status and a short plain-text
/// body. The messages are generic and carry no detail from `error`.
pub(crate) fn denial(error: &PortError, grpc: bool) -> Response {
    let (code, status, message) = match error {
        PortError::Rejected {
            reason: Rejection::InvalidProof,
            ..
        } => (
            Code::Unauthenticated,
            StatusCode::UNAUTHORIZED,
            "unauthenticated",
        ),
        PortError::Rejected {
            reason: Rejection::PolicyDenied | Rejection::PrincipalRevoked,
            ..
        } => (
            Code::PermissionDenied,
            StatusCode::FORBIDDEN,
            "permission denied",
        ),
        PortError::Unavailable(_) => (
            Code::Unavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
        ),
        _ => (
            Code::Internal,
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal error",
        ),
    };
    let mut response = if grpc {
        let (parts, _) = Status::new(code, message)
            .into_http::<Empty<Bytes>>()
            .into_parts();
        Response::from_parts(parts, Body::empty())
    } else {
        (status, message).into_response()
    };
    if matches!(error, PortError::Unavailable(_)) {
        response.headers_mut().insert(
            gang_types::handover::REQUEST_NOT_STARTED_HEADER,
            header::HeaderValue::from_static("true"),
        );
    }
    response
}

#[cfg(test)]
mod tests {
    use axum::Extension;
    use axum::Router;
    use axum::body::Body;
    use axum::middleware::from_fn_with_state;
    use axum::routing::post;
    use gang_spi::{Authenticated, Peer};
    use gang_types::{CertSerial, Identity, Principal, ProviderName};
    use http::Request;
    use tower::ServiceExt;

    use super::*;

    fn caller() -> Authenticated {
        Authenticated {
            identity: Arc::new(Identity::new(
                ProviderName::try_new("test").expect("valid provider"),
                Principal::try_new("tester").expect("valid principal"),
            )),
            valid_until: None,
        }
    }

    /// Denies every request with the error `make` builds.
    #[derive(Clone)]
    struct Deny(fn() -> PortError);

    impl RequestGate for Deny {
        fn admit(
            &self,
            _scope: RequestScope,
            _peer: &Peer,
            _parts: &mut http::request::Parts,
        ) -> impl Future<Output = Result<GateDecision, PortError>> + Send {
            std::future::ready(Err((self.0)()))
        }
    }

    /// Admits every request.
    #[derive(Clone)]
    struct Allow;

    impl RequestGate for Allow {
        fn admit(
            &self,
            _scope: RequestScope,
            _peer: &Peer,
            _parts: &mut http::request::Parts,
        ) -> impl Future<Output = Result<GateDecision, PortError>> + Send {
            std::future::ready(Ok(GateDecision::Admit(Admitted::Authenticated(caller()))))
        }
    }

    fn invalid_proof() -> PortError {
        PortError::refused(Rejection::InvalidProof)
    }

    fn policy_denied() -> PortError {
        PortError::refused(Rejection::PolicyDenied)
    }

    fn principal_revoked() -> PortError {
        PortError::refused(Rejection::PrincipalRevoked)
    }

    fn unavailable() -> PortError {
        PortError::unavailable("the identity provider is down")
    }

    fn internal() -> PortError {
        PortError::backend("the ledger is corrupt")
    }

    fn state<G>(gate: G) -> GateState<G> {
        GateState {
            gate,
            scope: RequestScope::Shared,
            force_not_leading: false,
        }
    }

    /// A router with one POST route behind `gate`.
    fn gated<G: RequestGate>(gate: G) -> Router {
        Router::new()
            .route("/grpc.Service/Method", post(|| async { "ok" }))
            .layer(from_fn_with_state(state(gate), admit::<G>))
    }

    /// Sends a POST to the route, with `content_type` when given, and returns the response.
    async fn send<G: RequestGate>(gate: G, content_type: Option<&str>) -> Response {
        let mut request = Request::post("/grpc.Service/Method");
        if let Some(value) = content_type {
            request = request.header(header::CONTENT_TYPE, value);
        }
        let request = request.body(Body::empty()).expect("valid request");
        gated(gate)
            .oneshot(request)
            .await
            .expect("infallible router")
    }

    #[tokio::test]
    async fn denied_grpc_requests_get_the_grpc_status() {
        type Case = (fn() -> PortError, &'static str);
        let cases: [Case; 5] = [
            (invalid_proof, "16"),
            (policy_denied, "7"),
            (principal_revoked, "7"),
            (unavailable, "14"),
            (internal, "13"),
        ];
        for (make, status) in cases {
            let response = send(Deny(make), Some("application/grpc+proto")).await;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()["grpc-status"], status);
            assert_eq!(
                response
                    .headers()
                    .get(gang_types::handover::REQUEST_NOT_STARTED_HEADER),
                (status == "14").then_some(&header::HeaderValue::from_static("true"))
            );
        }
    }

    #[tokio::test]
    async fn denied_plain_requests_get_the_http_status() {
        let cases: [(fn() -> PortError, StatusCode); 5] = [
            (invalid_proof, StatusCode::UNAUTHORIZED),
            (policy_denied, StatusCode::FORBIDDEN),
            (principal_revoked, StatusCode::FORBIDDEN),
            (unavailable, StatusCode::SERVICE_UNAVAILABLE),
            (internal, StatusCode::INTERNAL_SERVER_ERROR),
        ];
        for (make, status) in cases {
            let response = send(Deny(make), Some("application/json")).await;
            assert_eq!(response.status(), status);
            assert!(response.headers().get("grpc-status").is_none());
            assert_eq!(
                response
                    .headers()
                    .get(gang_types::handover::REQUEST_NOT_STARTED_HEADER),
                (status == StatusCode::SERVICE_UNAVAILABLE)
                    .then_some(&header::HeaderValue::from_static("true"))
            );
        }
        let response = send(Deny(invalid_proof), None).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn unavailable_refusals_never_invoke_the_handler() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = Arc::clone(&calls);
        let router = Router::new()
            .route(
                "/grpc.Service/Method",
                post(move || {
                    let calls = Arc::clone(&handler_calls);
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        StatusCode::OK
                    }
                }),
            )
            .layer(from_fn_with_state(state(Deny(unavailable)), admit::<Deny>));
        for content_type in ["application/json", "application/grpc"] {
            let response = router
                .clone()
                .oneshot(
                    Request::post("/grpc.Service/Method")
                        .header(header::CONTENT_TYPE, content_type)
                        .body(Body::empty())
                        .expect("valid request"),
                )
                .await
                .expect("infallible router");
            assert_eq!(
                response.headers()[gang_types::handover::REQUEST_NOT_STARTED_HEADER],
                "true"
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn handler_unavailable_has_no_refusal_marker() {
        let standing = Arc::new(Standing::default());
        let router = Router::new()
            .route(
                "/grpc.Service/Method",
                post(|| async { StatusCode::SERVICE_UNAVAILABLE }),
            )
            .layer(from_fn_with_state(state(Allow), admit::<Allow>))
            .layer(Extension(Arc::clone(&standing)));
        let response = router
            .oneshot(
                Request::post("/grpc.Service/Method")
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("infallible router");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            !response
                .headers()
                .contains_key(gang_types::handover::REQUEST_NOT_STARTED_HEADER)
        );
        let retry = standing.retry_requested();
        tokio::pin!(retry);
        assert!(futures::poll!(&mut retry).is_pending());
    }

    #[tokio::test]
    async fn admitted_requests_reach_the_service() {
        let response = send(Allow, Some("application/grpc")).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 16)
            .await
            .expect("small body");
        assert_eq!(&body[..], b"ok");
    }

    #[derive(Clone)]
    struct Admit(Admitted);

    impl RequestGate for Admit {
        fn admit(
            &self,
            _scope: RequestScope,
            _peer: &Peer,
            _parts: &mut http::request::Parts,
        ) -> impl Future<Output = Result<GateDecision, PortError>> + Send {
            std::future::ready(Ok(GateDecision::Admit(self.0.clone())))
        }
    }

    #[tokio::test]
    async fn an_authenticated_admission_gives_the_service_its_caller() {
        let caller = caller();
        let expected = caller.clone();
        let router = Router::new()
            .route(
                "/grpc.Service/Method",
                post(move |request: Request<Body>| async move {
                    assert_eq!(request.extensions().get::<Authenticated>(), Some(&expected));
                    StatusCode::OK
                }),
            )
            .layer(from_fn_with_state(
                state(Admit(Admitted::Authenticated(caller))),
                admit::<Admit>,
            ));
        let request = Request::post("/grpc.Service/Method")
            .body(Body::empty())
            .expect("valid request");
        let response = router.oneshot(request).await.expect("infallible router");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn an_anonymous_admission_gives_the_service_no_caller() {
        let router = Router::new()
            .route(
                "/grpc.Service/Method",
                post(|request: Request<Body>| async move {
                    assert!(request.extensions().get::<Authenticated>().is_none());
                    StatusCode::OK
                }),
            )
            .layer(from_fn_with_state(
                state(Admit(Admitted::Anonymous)),
                admit::<Admit>,
            ));
        let request = Request::post("/grpc.Service/Method")
            .body(Body::empty())
            .expect("valid request");
        let response = router.oneshot(request).await.expect("infallible router");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn retrying_connection_denies_later_admissions_without_proving() {
        for admitted in [Admitted::Anonymous, Admitted::Authenticated(caller())] {
            let standing = Arc::new(Standing::default());
            standing.request_retry();
            let request = Request::post("/grpc.Service/Method")
                .body(Body::empty())
                .expect("valid request");
            let response = gated(Admit(admitted))
                .layer(Extension(Arc::clone(&standing)))
                .oneshot(request)
                .await
                .expect("infallible router");
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert!(!standing.is_proven());
            assert!(standing.is_busy());
            drop(response);
            assert!(!standing.is_busy());
        }
    }

    #[tokio::test]
    async fn unavailable_retires_only_a_connection_without_prior_admission() {
        for previously_admitted in [false, true] {
            let standing = Arc::new(Standing::default());
            if previously_admitted {
                let request = Request::post("/grpc.Service/Method")
                    .body(Body::empty())
                    .expect("valid request");
                let response = gated(Admit(Admitted::Anonymous))
                    .layer(Extension(Arc::clone(&standing)))
                    .oneshot(request)
                    .await
                    .expect("infallible router");
                assert_eq!(response.status(), StatusCode::OK);
                assert!(!standing.is_proven());
            }
            let request = Request::post("/grpc.Service/Method")
                .body(Body::empty())
                .expect("valid request");
            let response = gated(Deny(unavailable))
                .layer(Extension(Arc::clone(&standing)))
                .oneshot(request)
                .await
                .expect("infallible router");
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            let retry = standing.retry_requested();
            tokio::pin!(retry);
            assert_eq!(futures::poll!(&mut retry).is_ready(), !previously_admitted);
        }
    }

    #[tokio::test]
    async fn other_denials_do_not_request_retry() {
        for make in [invalid_proof as fn() -> PortError, internal] {
            let standing = Arc::new(Standing::default());
            let request = Request::post("/grpc.Service/Method")
                .body(Body::empty())
                .expect("valid request");
            let _response = gated(Deny(make))
                .layer(Extension(Arc::clone(&standing)))
                .oneshot(request)
                .await
                .expect("infallible router");
            let retry = standing.retry_requested();
            tokio::pin!(retry);
            assert!(futures::poll!(&mut retry).is_pending());
            assert!(standing.admit());
        }
    }

    #[tokio::test]
    async fn leadership_refusal_and_api_marker_retire_proven_and_anonymous_connections() {
        for proven in [false, true] {
            for api_marker in [false, true] {
                let standing = Arc::new(Standing::default());
                assert!(standing.admit());
                if proven {
                    standing.prove();
                }
                let router = if api_marker {
                    Router::new().route(
                        "/grpc.Service/Method",
                        post(|| async {
                            let mut response = StatusCode::SERVICE_UNAVAILABLE.into_response();
                            response.extensions_mut().insert(LeaderUnavailable);
                            response
                        }),
                    )
                } else {
                    Router::new().route(
                        "/grpc.Service/Method",
                        post(|| async { StatusCode::IM_A_TEAPOT }),
                    )
                };
                let router = router
                    .layer(from_fn_with_state(
                        GateState {
                            gate: Allow,
                            scope: RequestScope::Leader,
                            force_not_leading: !api_marker,
                        },
                        admit::<Allow>,
                    ))
                    .layer(Extension(Arc::clone(&standing)));
                let request = Request::post("/grpc.Service/Method")
                    .header(header::CONTENT_TYPE, "application/grpc")
                    .body(Body::empty())
                    .expect("valid request");
                let response = router.oneshot(request).await.expect("infallible router");
                if api_marker {
                    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
                    assert!(response.extensions().get::<LeaderUnavailable>().is_none());
                } else {
                    assert_eq!(response.headers()["grpc-status"], "14");
                    assert_eq!(
                        response.headers()[gang_types::handover::REQUEST_NOT_STARTED_HEADER],
                        "true"
                    );
                }
                let retry = standing.retry_requested();
                tokio::pin!(retry);
                assert!(futures::poll!(&mut retry).is_ready());
                assert!(standing.admit());
            }
        }
    }

    /// Admits only requests from a peer that presented a client certificate.
    #[derive(Clone)]
    struct RequireCertificate;

    impl RequestGate for RequireCertificate {
        fn admit(
            &self,
            _scope: RequestScope,
            peer: &Peer,
            _parts: &mut http::request::Parts,
        ) -> impl Future<Output = Result<GateDecision, PortError>> + Send {
            std::future::ready(match peer.certificate {
                Some(_) => Ok(GateDecision::Admit(Admitted::Authenticated(caller()))),
                None => Err(invalid_proof()),
            })
        }
    }

    /// Posts a gRPC request through the gate, layered as the serve loop does: the connection's
    /// peer is the outermost layer, so the gate sees it on every request.
    async fn post_from<G: RequestGate>(gate: G, peer: Option<Peer>) -> Response {
        let mut router = gated(gate);
        if let Some(peer) = peer {
            router = router.layer(Extension(peer));
        }
        let request = Request::post("/grpc.Service/Method")
            .header(header::CONTENT_TYPE, "application/grpc")
            .body(Body::empty())
            .expect("valid request");
        router.oneshot(request).await.expect("infallible router")
    }

    #[tokio::test]
    async fn anonymous_requests_are_refused_and_the_peer_reaches_the_gate() {
        let refused = post_from(RequireCertificate, None).await;
        assert_eq!(refused.headers()["grpc-status"], "16");
        let peer = Peer {
            certificate: Some(CertSerial::from_be_bytes(&[0x2a]).expect("serial")),
            ..Peer::default()
        };
        let admitted = post_from(RequireCertificate, Some(peer)).await;
        assert_eq!(admitted.status(), StatusCode::OK);
        assert!(admitted.headers().get("grpc-status").is_none());
    }
}
