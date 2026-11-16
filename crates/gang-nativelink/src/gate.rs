//! The request gate: the middleware that runs a listener's [`RequestGate`] over every request
//! before any service sees it, and the denial responses it produces.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use gang_spi::{Admitted, Peer, PortError, Rejection, RequestGate};
use http::{StatusCode, header};
use http_body_util::Empty;
use tonic::{Code, Status};

use crate::standing::{Standing, hold};

/// Runs `gate` on a request and passes the request on when the gate admits it. The request's
/// [`Peer`] comes from the request extensions; a request without one has the default peer. When the
/// request carries its unproven connection's [`Standing`], it counts as in flight until its
/// response body is done. An authenticated admission is the one source of both what follows from
/// it: it proves the connection, after which its requests are no longer tracked, and it gives the
/// services behind the gate the caller's [`Authenticated`](gang_spi::Authenticated) identity in
/// the request extensions.
pub(crate) async fn admit<G: RequestGate>(
    State(gate): State<G>,
    request: Request,
    next: Next,
) -> Response {
    let peer = request
        .extensions()
        .get::<Peer>()
        .cloned()
        .unwrap_or_default();
    let grpc = is_grpc(&request);
    let standing = request
        .extensions()
        .get::<Arc<Standing>>()
        .filter(|standing| !standing.is_proven())
        .cloned();
    let busy = standing.as_ref().map(Standing::begin);
    let (mut parts, body) = request.into_parts();
    let response = match gate.admit(&peer, &mut parts).await {
        Ok(admitted) => {
            if let Admitted::Authenticated(authenticated) = admitted {
                if let Some(standing) = &standing {
                    standing.prove();
                }
                parts.extensions.insert(authenticated);
            }
            next.run(Request::from_parts(parts, body)).await
        }
        Err(error) => denial(&error, grpc),
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
    if grpc {
        let (parts, _) = Status::new(code, message)
            .into_http::<Empty<Bytes>>()
            .into_parts();
        return Response::from_parts(parts, Body::empty());
    }
    (status, message).into_response()
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
            _peer: &Peer,
            _parts: &mut http::request::Parts,
        ) -> impl Future<Output = Result<Admitted, PortError>> + Send {
            std::future::ready(Err((self.0)()))
        }
    }

    /// Admits every request.
    #[derive(Clone)]
    struct Allow;

    impl RequestGate for Allow {
        fn admit(
            &self,
            _peer: &Peer,
            _parts: &mut http::request::Parts,
        ) -> impl Future<Output = Result<Admitted, PortError>> + Send {
            std::future::ready(Ok(Admitted::Authenticated(caller())))
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

    /// A router with one POST route behind `gate`.
    fn gated<G: RequestGate>(gate: G) -> Router {
        Router::new()
            .route("/grpc.Service/Method", post(|| async { "ok" }))
            .layer(from_fn_with_state(gate, admit::<G>))
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
        }
        let response = send(Deny(invalid_proof), None).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
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
            _peer: &Peer,
            _parts: &mut http::request::Parts,
        ) -> impl Future<Output = Result<Admitted, PortError>> + Send {
            std::future::ready(Ok(self.0.clone()))
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
                Admit(Admitted::Authenticated(caller)),
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
                Admit(Admitted::Anonymous),
                admit::<Admit>,
            ));
        let request = Request::post("/grpc.Service/Method")
            .body(Body::empty())
            .expect("valid request");
        let response = router.oneshot(request).await.expect("infallible router");
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// Admits only requests from a peer that presented a client certificate.
    #[derive(Clone)]
    struct RequireCertificate;

    impl RequestGate for RequireCertificate {
        fn admit(
            &self,
            peer: &Peer,
            _parts: &mut http::request::Parts,
        ) -> impl Future<Output = Result<Admitted, PortError>> + Send {
            std::future::ready(match peer.certificate {
                Some(_) => Ok(Admitted::Authenticated(caller())),
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
