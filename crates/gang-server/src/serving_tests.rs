//! The composed server: listener admission by side, controller hand-off, shutdown, endpoint
//! validation, the plaintext API surface, TLS admission on both listeners with bearer tokens on
//! the public listener, the AIP-193 envelope of every refusal, the caller's session, and live
//! revocation.

use std::future::pending;
use std::time::Duration;

use crate::gate::Caller;
use crate::{Deployment, Parts, Server, ServerError};
use gang_clock::{Clock, Timestamp};
use gang_spi::{
    ApiResponseBody, CertStatus, ControllerReady, IssuedCertRecord, Peer, PortError, Rejection,
    StateRepositoryPort,
};
use gang_testkit::{Controller, FakeScheduler, Received, Side};
use gang_tls::{TransportError, ca_pin_of_certificate};
use gang_types::{AccessToken, CertSerial, Identity, Permission, Principal, ProviderName};
use gang_wire::routes::{
    AUTHENTICATE_SESSION, CREATE_CERTIFICATE, GET_SERVER, GET_SESSION, GET_STATE, LIST_EVENTS,
    LIST_PRINCIPALS, REVOKE_CERTIFICATE, REVOKE_PRINCIPAL,
};
use gang_wire::{Authentication, ErrorReason, ErrorResponse, Session, name};
use http::StatusCode;
use serde_json::json;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::oneshot;

use crate::test_support::{
    SESSION_TTL, TlsDeployment, admits, api_request, authority, client_certificate, identity,
    mint_token, plain_parts, plain_settings, prepare_plain, read, request, request_parts,
    session_token, tls_client, tls_parts, tls_settings,
};

const PROVIDER: &str = "example-idp";
const WORKER_PROVIDER: &str = "example-workers";
const CI_PROVIDER: &str = "example-ci";
const ADMIN: &str = "admin";
const ADMIN_PROOF: &str = "admin-proof";
const ALICE: &str = "alice";
const ALICE_PROOF: &str = "alice-proof";
const WORKER: &str = "worker-1";
const WORKER_HOST: &str = "scheduler.example.test";

/// The session cookie name of the https deployment.
const SESSION_COOKIE: &str = "__Host-gang_session";

/// How far past its expiry a token is still accepted: the test token configuration's clock skew.
const CLOCK_SKEW: Duration = Duration::from_secs(30);

/// Prepares a TLS server with the test settings, failing the test when preparation fails.
async fn prepare_tls(parts: Parts<TlsDeployment>) -> Server<TlsDeployment> {
    match Server::prepare(parts, tls_settings()).await {
        Ok(server) => server,
        Err(error) => panic!("server prepares: {error}"),
    }
}

/// Runs `server` while `body` executes, then stops it and asserts that the run ended with Ok.
async fn run_until<D: Deployment>(server: Server<D>, body: impl Future<Output = ()>) {
    let (stop, stopped) = oneshot::channel::<()>();
    let body = async {
        body.await;
        let _ = stop.send(());
    };
    let (result, ()) = tokio::join!(
        server.run(
            async {
                let _ = stopped.await;
            },
            ControllerReady::channel().0,
        ),
        body
    );
    assert!(result.is_ok(), "run returns Ok after shutdown: {result:?}");
}

/// Waits for the next connection the controller receives.
async fn next_connection(received: &mut UnboundedReceiver<Received>) -> Received {
    received
        .recv()
        .await
        .expect("controller receives the connection")
}

/// Asserts that a client handshake was refused. The refusal may show up as a failed handshake
/// or, under TLS 1.3, as a connection the server closes before any byte is exchanged.
async fn assert_refused<S: AsyncRead + Unpin>(outcome: Result<S, TransportError>) {
    if let Ok(mut stream) = outcome {
        let mut byte = [0_u8; 1];
        let read = stream.read(&mut byte).await;
        assert!(
            !matches!(read, Ok(count) if count > 0),
            "refused client read {read:?}"
        );
    }
}

/// Asserts that a worker-listener handshake was refused and that the controller received nothing
/// from it.
async fn assert_worker_refused<S: AsyncRead + Unpin>(
    outcome: Result<S, TransportError>,
    received: &mut UnboundedReceiver<Received>,
) {
    assert_refused(outcome).await;
    // The server refuses the handshake before it hands anything to the controller, and the client
    // only observes the refusal after that decision, so an empty channel is not a race.
    assert!(
        matches!(
            received.try_recv(),
            Err(TryRecvError::Empty | TryRecvError::Disconnected)
        ),
        "the controller receives nothing from a refused worker connection"
    );
}

/// A record in the certificate ledger for `serial`, issued to `principal` under `provider`.
fn record(serial: &str, provider: &str, principal: &str, issued_at: Timestamp) -> IssuedCertRecord {
    IssuedCertRecord {
        serial: CertSerial::try_new(serial).expect("test serial is valid"),
        identity: Identity::new(
            ProviderName::try_new(provider).expect("test provider name is valid"),
            Principal::try_new(principal).expect("test principal is valid"),
        ),
        issued_at,
        expires_at: issued_at + Duration::from_hours(24),
        status: CertStatus::Active,
    }
}

/// Revokes through the Gang API, as the principal bearing `admin`, the resource at `path`,
/// asserting that it succeeds.
async fn revoke(controller: &Controller, admin: &AccessToken, path: &str) {
    let response = controller
        .call_api(api_request("POST", path, Some(admin), None))
        .await;
    let status = read(response).await.status;
    assert_eq!(status, StatusCode::OK, "the admin revokes");
}

/// The path that revokes `principal` under the test provider.
fn principal_revocation(principal: &str) -> String {
    REVOKE_PRINCIPAL.path(&identity(principal))
}

/// Asserts that `response` is the AIP-193 envelope for `reason`: its status and code, exactly one
/// `ErrorInfo` carrying the reason, and `WWW-Authenticate: Bearer` on exactly the 401s. Returns
/// the envelope.
async fn assert_error(
    response: http::Response<ApiResponseBody>,
    reason: ErrorReason,
) -> ErrorResponse {
    ErrorResponse {
        error: read(response).await.refused(reason),
    }
}

#[tokio::test]
async fn plain_listeners_feed_the_controller_by_side() {
    let (parts, dials, handles) = plain_parts();
    let mut received = handles.scheduler.connections();
    let server = prepare_plain(parts).await;
    Box::pin(run_until(server, async {
        let mut public_dial = dials.public.dial();
        let mut public = next_connection(&mut received).await;
        assert_eq!(public.side, Side::Public);
        public_dial
            .write_all(b"ping")
            .await
            .expect("the public dialer writes");
        let mut greeting = [0_u8; 4];
        public
            .connection
            .read_exact(&mut greeting)
            .await
            .expect("the controller reads what the dialer wrote");
        assert_eq!(&greeting, b"ping");
        let _worker = dials.worker.dial();
        let worker = next_connection(&mut received).await;
        assert_eq!(worker.side, Side::Worker);
    }))
    .await;
}

#[tokio::test]
async fn completing_shutdown_returns_ok() {
    let (parts, _dials, _handles) = plain_parts();
    let server = prepare_plain(parts).await;
    let (ready, shutdown) = oneshot::channel::<()>();
    let (result, ()) = tokio::join!(
        server.run(
            async {
                let _ = shutdown.await;
            },
            ControllerReady::channel().0,
        ),
        async {
            let _ = ready.send(());
        }
    );
    assert!(result.is_ok(), "run returns Ok after shutdown: {result:?}");
}

#[tokio::test]
async fn controller_failure_stops_run_with_an_error() {
    let (parts, _dials, handles) = plain_parts();
    handles.scheduler.fail_controller(true);
    let server = prepare_plain(parts).await;
    let result = server
        .run(pending::<()>(), ControllerReady::channel().0)
        .await;
    assert!(
        matches!(result, Err(ServerError::Controller(_))),
        "controller failure is reported"
    );
}

#[tokio::test]
async fn plaintext_mode_accepts_either_scheduler_endpoint_scheme() {
    for scheduler in [
        FakeScheduler::new().expect("the grpcs scheduler builds"),
        FakeScheduler::plain().expect("the grpc scheduler builds"),
    ] {
        let (mut parts, _dials, _handles) = plain_parts();
        parts.scheduler = scheduler;
        let prepared = Server::prepare(parts, plain_settings()).await;
        assert!(
            prepared.is_ok(),
            "plaintext transport accepts a grpc or grpcs worker endpoint"
        );
    }
}

#[tokio::test]
async fn tls_mode_refuses_a_grpc_scheduler_endpoint() {
    let (mut parts, _dials, _handles) = tls_parts();
    parts.scheduler = FakeScheduler::plain().expect("the grpc scheduler builds");
    match Server::prepare(parts, tls_settings()).await {
        Err(ServerError::EndpointScheme { .. }) => {}
        Err(error) => panic!("a grpc endpoint is refused with an endpoint scheme error: {error}"),
        Ok(_) => panic!("a grpc endpoint is refused under tls transport"),
    }
}

#[tokio::test]
async fn plaintext_api_serves_the_server_and_no_other_endpoint() {
    let (parts, _dials, handles) = plain_parts();
    let server = prepare_plain(parts).await;
    Box::pin(run_until(server, async {
        let controller = handles
            .scheduler
            .controller()
            .await
            .expect("controller is published");
        let response = controller
            .call_api(api_request("GET", GET_SERVER.template, None, None))
            .await;
        let reply = read(response).await;
        let status = reply.status;
        let _: serde_json::Value = reply.json();
        assert_eq!(status, StatusCode::OK, "the server is served");
        for route in [CREATE_CERTIFICATE, AUTHENTICATE_SESSION] {
            let method = route.method.as_str();
            let path = route.template;
            let response = controller
                .call_api(api_request(method, path, None, None))
                .await;
            let envelope = assert_error(response, ErrorReason::RouteNotFound).await;
            let info = envelope
                .error
                .error_info()
                .expect("the envelope has an info");
            assert_eq!(
                info.metadata.get("path").map(String::as_str),
                Some(path),
                "{method} {path} is not served"
            );
        }
    }))
    .await;
}

#[tokio::test]
async fn plaintext_server_has_no_certificate_authority_and_no_authentication() {
    let (parts, _dials, handles) = plain_parts();
    let server = prepare_plain(parts).await;
    Box::pin(run_until(server, async {
        let controller = handles
            .scheduler
            .controller()
            .await
            .expect("controller is published");
        let response = controller
            .call_api(api_request("GET", GET_SERVER.template, None, None))
            .await;
        let reply = read(response).await;
        let status = reply.status;
        let info = reply.json::<gang_wire::Server<serde_json::Value>>();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(info.name, name::SERVER);
        assert!(
            info.ca_certificate.is_none(),
            "a plaintext server has no CA certificate"
        );
        assert!(
            matches!(info.authentication, Authentication::None(_)),
            "a plaintext server has no authentication"
        );
    }))
    .await;
}

#[tokio::test]
async fn plaintext_server_has_no_ca_pin() {
    let (parts, _dials, _handles) = plain_parts();
    let server = prepare_plain(parts).await;
    assert!(
        server.ca_pin().is_none(),
        "plaintext server issues no certificates, so it has no CA pin"
    );
}

#[tokio::test]
async fn worker_listener_admits_a_worker_certificate_as_a_worker() {
    let (parts, dials, handles) = tls_parts();
    let ca = authority(&handles.clock).await;
    let (issued, key) = client_certificate(&ca, WORKER_PROVIDER, WORKER).await;
    handles.repository.insert(record(
        issued.serial.as_str(),
        WORKER_PROVIDER,
        WORKER,
        handles.clock.wall(),
    ));
    let mut received = handles.scheduler.connections();
    let server = prepare_tls(parts).await;
    Box::pin(run_until(server, async {
        let _stream = tls_client(
            &dials.worker,
            WORKER_HOST,
            ca.certificate(),
            Some((&issued.certificate, &key)),
            &handles.clock,
        )
        .await
        .expect("worker certificate handshakes");
        let worker = next_connection(&mut received).await;
        assert_eq!(worker.side, Side::Worker);
        assert_eq!(worker.peer.certificate, Some(issued.serial.clone()));
    }))
    .await;
}

#[tokio::test]
async fn worker_gate_refuses_certificates_issued_to_other_providers() {
    let (parts, _dials, handles) = tls_parts();
    let ca = authority(&handles.clock).await;
    let (developer, _developer_key) = client_certificate(&ca, PROVIDER, ADMIN).await;
    let (ci, _ci_key) = client_certificate(&ca, CI_PROVIDER, "ci-job").await;
    handles.repository.insert(record(
        developer.serial.as_str(),
        PROVIDER,
        ADMIN,
        handles.clock.wall(),
    ));
    handles.repository.insert(record(
        ci.serial.as_str(),
        CI_PROVIDER,
        "ci-job",
        handles.clock.wall(),
    ));
    let server = prepare_tls(parts).await;
    Box::pin(run_until(server, async {
        let controller = handles
            .scheduler
            .controller()
            .await
            .expect("controller is published");
        for serial in [developer.serial.clone(), ci.serial.clone()] {
            let peer = Peer {
                certificate: Some(serial),
                ..Peer::default()
            };
            let mut parts = request_parts(GET_SERVER.template, None);
            let refused = controller.admit_worker(&peer, &mut parts).await;
            assert!(
                matches!(
                    refused,
                    Err(PortError::Rejected {
                        reason: Rejection::PolicyDenied,
                        ..
                    })
                ),
                "a certificate of another provider is refused on the worker listener: {refused:?}"
            );
        }
    }))
    .await;
}

#[tokio::test]
async fn worker_gate_refuses_a_certificate_missing_from_the_ledger() {
    let (parts, _dials, handles) = tls_parts();
    let ca = authority(&handles.clock).await;
    let (unrecorded, _key) = client_certificate(&ca, WORKER_PROVIDER, WORKER).await;
    let server = prepare_tls(parts).await;
    Box::pin(run_until(server, async {
        let controller = handles
            .scheduler
            .controller()
            .await
            .expect("controller is published");
        let peer = Peer {
            certificate: Some(unrecorded.serial.clone()),
            ..Peer::default()
        };
        let mut parts = request_parts(GET_SERVER.template, None);
        let refused = controller.admit_worker(&peer, &mut parts).await;
        assert!(
            matches!(
                refused,
                Err(PortError::Rejected {
                    reason: Rejection::InvalidProof,
                    ..
                })
            ),
            "a certificate missing from the ledger is refused on the worker listener: {refused:?}"
        );
    }))
    .await;
}

#[tokio::test]
async fn revoking_a_serial_cuts_live_connections_and_refuses_new_ones() {
    let (parts, dials, handles) = tls_parts();
    let ca = authority(&handles.clock).await;
    handles.verifier.accept(ADMIN_PROOF, ADMIN);
    let (admin_cert, _) = client_certificate(&ca, PROVIDER, ADMIN).await;
    handles.repository.insert(record(
        admin_cert.serial.as_str(),
        PROVIDER,
        ADMIN,
        handles.clock.wall(),
    ));
    let (issued, key) = client_certificate(&ca, WORKER_PROVIDER, WORKER).await;
    handles.repository.insert(record(
        issued.serial.as_str(),
        WORKER_PROVIDER,
        WORKER,
        handles.clock.wall(),
    ));
    let mut received = handles.scheduler.connections();
    let server = prepare_tls(parts).await;
    Box::pin(run_until(server, async {
        let _live = tls_client(
            &dials.worker,
            WORKER_HOST,
            ca.certificate(),
            Some((&issued.certificate, &key)),
            &handles.clock,
        )
        .await
        .expect("worker certificate handshakes before revocation");
        let mut worker = next_connection(&mut received).await;

        let controller = handles
            .scheduler
            .controller()
            .await
            .expect("controller is published");
        let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
        revoke(
            &controller,
            &admin,
            &REVOKE_CERTIFICATE.path(&issued.serial),
        )
        .await;

        // The server cuts the connection when it revokes the serial: the read and write below
        // resolve only once that cut is observed.
        let mut byte = [0_u8; 1];
        let read = worker.connection.read(&mut byte).await;
        assert!(
            read.is_err(),
            "the revoked connection fails on read: {read:?}"
        );
        let write = worker.connection.write_all(b"x").await;
        assert!(
            write.is_err(),
            "the revoked connection fails on write: {write:?}"
        );

        assert_worker_refused(
            tls_client(
                &dials.worker,
                WORKER_HOST,
                ca.certificate(),
                Some((&issued.certificate, &key)),
                &handles.clock,
            )
            .await,
            &mut received,
        )
        .await;
    }))
    .await;
}

#[tokio::test]
async fn ca_pin_is_the_pin_of_the_authority_certificate() {
    let (parts, _dials, handles) = tls_parts();
    let ca = authority(&handles.clock).await;
    let server = prepare_tls(parts).await;
    let pin = ca_pin_of_certificate(ca.certificate()).expect("authority certificate has a pin");
    assert_eq!(server.ca_pin(), Some(pin));
}

#[tokio::test]
async fn public_listener_admits_a_minted_bearer_token() {
    let (parts, _dials, handles) = tls_parts();
    handles.verifier.accept(ALICE_PROOF, ALICE);
    let server = prepare_tls(parts).await;
    Box::pin(run_until(server, async {
        let controller = handles
            .scheduler
            .controller()
            .await
            .expect("controller is published");
        let token = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
        assert!(
            admits(&controller, &token).await,
            "a minted bearer token is admitted on the public listener"
        );
    }))
    .await;
}

#[tokio::test]
async fn public_listener_answers_an_invalid_bearer_token_with_the_envelope() {
    let (parts, _dials, handles) = tls_parts();
    let server = prepare_tls(parts).await;
    Box::pin(run_until(server, async {
        let controller = handles
            .scheduler
            .controller()
            .await
            .expect("controller is published");
        let bogus = AccessToken::new("not.a.valid.token");
        assert!(
            !admits(&controller, &bogus).await,
            "an invalid bearer token authenticates no caller"
        );
        for path in [
            GET_STATE.template,
            GET_SESSION.template,
            GET_SERVER.template,
        ] {
            let response = controller
                .call_api(api_request("GET", path, Some(&bogus), None))
                .await;
            assert_error(response, ErrorReason::CredentialsInvalid).await;
        }
    }))
    .await;
}

#[tokio::test]
async fn administrative_call_without_a_caller_is_unauthenticated() {
    let (parts, _dials, handles) = tls_parts();
    let server = prepare_tls(parts).await;
    Box::pin(run_until(server, async {
        let controller = handles
            .scheduler
            .controller()
            .await
            .expect("controller is published");
        for path in [
            LIST_EVENTS.template,
            GET_STATE.template,
            LIST_PRINCIPALS.template,
        ] {
            let response = controller
                .call_api(api_request("GET", path, None, None))
                .await;
            assert_error(response, ErrorReason::CredentialsMissing).await;
        }
    }))
    .await;
}

#[tokio::test]
async fn caller_without_the_permission_is_forbidden_naming_it() {
    let (parts, _dials, handles) = tls_parts();
    handles.verifier.accept(ALICE_PROOF, ALICE);
    let server = prepare_tls(parts).await;
    Box::pin(run_until(server, async {
        let controller = handles
            .scheduler
            .controller()
            .await
            .expect("controller is published");
        let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
        let response = controller
            .call_api(api_request("GET", LIST_EVENTS.template, Some(&alice), None))
            .await;
        let envelope = assert_error(response, ErrorReason::MissingPermission).await;
        let info = envelope
            .error
            .error_info()
            .expect("the envelope has an info");
        assert_eq!(
            info.metadata.get("permission").map(String::as_str),
            Some(Permission::EventsRead.as_str())
        );
    }))
    .await;
}

#[tokio::test]
async fn unknown_route_is_unauthenticated_for_anonymous_and_not_found_for_a_caller() {
    let (parts, _dials, handles) = tls_parts();
    handles.verifier.accept(ALICE_PROOF, ALICE);
    let server = prepare_tls(parts).await;
    Box::pin(run_until(server, async {
        let controller = handles
            .scheduler
            .controller()
            .await
            .expect("controller is published");
        let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
        for (method, path) in [
            ("GET", "/v1/nothing"),
            ("POST", GET_STATE.template),
            ("DELETE", GET_SESSION.template),
        ] {
            let anonymous = controller
                .call_api(api_request(method, path, None, None))
                .await;
            assert_error(anonymous, ErrorReason::CredentialsMissing).await;

            let known = controller
                .call_api(api_request(method, path, Some(&alice), None))
                .await;
            let envelope = assert_error(known, ErrorReason::RouteNotFound).await;
            let info = envelope
                .error
                .error_info()
                .expect("the envelope has an info");
            assert_eq!(
                info.metadata.get("method").map(String::as_str),
                Some(method)
            );
            assert_eq!(info.metadata.get("path").map(String::as_str), Some(path));
        }
    }))
    .await;
}

#[tokio::test]
async fn public_listener_refuses_a_revoked_principal() {
    let (parts, _dials, handles) = tls_parts();
    handles.verifier.accept(ALICE_PROOF, ALICE);
    handles.verifier.accept(ADMIN_PROOF, ADMIN);
    let server = prepare_tls(parts).await;
    Box::pin(run_until(server, async {
        let controller = handles
            .scheduler
            .controller()
            .await
            .expect("controller is published");
        let alice = mint_token(&controller, PROVIDER, ALICE_PROOF).await;
        let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
        assert!(
            admits(&controller, &alice).await,
            "alice is admitted before revocation"
        );

        revoke(&controller, &admin, &principal_revocation(ALICE)).await;

        assert!(
            !admits(&controller, &alice).await,
            "a revoked principal authenticates no caller"
        );
        let mut after = request_parts(GET_SERVER.template, Some(&alice));
        controller
            .admit_public(&Peer::default(), &mut after)
            .await
            .expect("the public gate admits open paths and records the refusal");
        let caller = Caller::of(&after.extensions);
        assert!(
            matches!(
                caller,
                Caller::Refused(refusal)
                    if matches!(**refusal, PortError::Rejected { reason: Rejection::PrincipalRevoked, .. })
            ),
            "the gate records the revocation: {caller:?}"
        );

        for path in [
            GET_SESSION.template,
            GET_STATE.template,
            GET_SERVER.template,
        ] {
            let response = controller
                .call_api(api_request("GET", path, Some(&alice), None))
                .await;
            assert_error(response, ErrorReason::PrincipalRevoked).await;
        }
    }))
    .await;
}

/// Sends `proof` from `provider` to `POST /v1/session:authenticate`.
async fn authenticate(
    controller: &Controller,
    provider: &str,
    proof: &str,
) -> http::Response<ApiResponseBody> {
    let body = json!({ "provider": provider, "proof": proof });
    let request = api_request("POST", AUTHENTICATE_SESSION.template, None, Some(body));
    controller.call_api(request).await
}

#[tokio::test]
async fn rejected_proof_is_refused_at_the_authenticate_endpoint() {
    let (parts, _dials, handles) = tls_parts();
    handles
        .verifier
        .reject("policy-proof", Rejection::PolicyDenied);
    handles
        .verifier
        .reject("forged-proof", Rejection::InvalidProof);
    let server = prepare_tls(parts).await;
    Box::pin(run_until(server, async {
        let controller = handles
            .scheduler
            .controller()
            .await
            .expect("controller is published");
        let denied = authenticate(&controller, PROVIDER, "policy-proof").await;
        assert_error(denied, ErrorReason::PolicyDenied).await;
        let forged = authenticate(&controller, PROVIDER, "forged-proof").await;
        assert_error(forged, ErrorReason::ProofRejected).await;
    }))
    .await;
}

#[tokio::test]
async fn unknown_provider_is_a_bad_request_at_the_authenticate_endpoint() {
    let (parts, _dials, handles) = tls_parts();
    let server = prepare_tls(parts).await;
    Box::pin(run_until(server, async {
        let controller = handles
            .scheduler
            .controller()
            .await
            .expect("controller is published");
        let response = authenticate(&controller, "nobody-knows", "any-proof").await;
        let envelope = assert_error(response, ErrorReason::UnknownProvider).await;
        let info = envelope
            .error
            .error_info()
            .expect("the envelope has an info");
        assert_eq!(
            info.metadata.get("provider").map(String::as_str),
            Some("nobody-knows")
        );
    }))
    .await;
}

#[tokio::test]
async fn unavailable_provider_is_reported_unavailable_at_the_authenticate_endpoint() {
    let (parts, _dials, handles) = tls_parts();
    handles.verifier.unavailable("flaky-proof");
    let server = prepare_tls(parts).await;
    Box::pin(run_until(server, async {
        let controller = handles
            .scheduler
            .controller()
            .await
            .expect("controller is published");
        let response = authenticate(&controller, PROVIDER, "flaky-proof").await;
        assert_error(response, ErrorReason::DependencyUnavailable).await;
    }))
    .await;
}

/// The `GET /v1/session` answer for a request carrying `cookie` as the session cookie.
async fn session_by_cookie(controller: &Controller, cookie: &AccessToken) -> (StatusCode, Session) {
    let request = request(
        "GET",
        GET_SESSION.template,
        &[(SESSION_COOKIE, cookie.expose())],
    );
    let reply = read(controller.call_api(request).await).await;
    (reply.status, reply.json::<Session>())
}

#[tokio::test]
async fn session_without_credentials_is_anonymous() {
    let (parts, _dials, handles) = tls_parts();
    let server = prepare_tls(parts).await;
    Box::pin(run_until(server, async {
        let controller = handles
            .scheduler
            .controller()
            .await
            .expect("controller is published");
        let response = controller
            .call_api(api_request("GET", GET_SESSION.template, None, None))
            .await;
        let reply = read(response).await;
        let status = reply.status;
        let session = reply.json::<Session>();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(session.name, name::SESSION);
        assert_eq!(session.principal, None);
        assert_eq!(session.permissions, Vec::new());
        assert_eq!(session.expire_time, None);
    }))
    .await;
}

#[tokio::test]
async fn session_cookie_of_an_administrator_reports_every_permission() {
    let (parts, _dials, handles) = tls_parts();
    let server = prepare_tls(parts).await;
    Box::pin(run_until(server, async {
        let controller = handles
            .scheduler
            .controller()
            .await
            .expect("controller is published");
        let cookie = session_token(&handles.clock, &identity(ADMIN)).await;
        let (status, session) = session_by_cookie(&controller, &cookie).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(session.name, name::SESSION);
        assert_eq!(
            session.principal,
            Some(name::of::<name::Principals>(&identity(ADMIN)))
        );
        assert_eq!(
            session.permissions,
            Permission::ALL.map(gang_wire::Permission::from)
        );
        assert_eq!(
            session.expire_time,
            Some(handles.clock.wall() + SESSION_TTL + CLOCK_SKEW)
        );
    }))
    .await;
}

#[tokio::test]
async fn session_cookie_of_a_principal_without_a_role_reports_no_permissions() {
    let (parts, _dials, handles) = tls_parts();
    let server = prepare_tls(parts).await;
    Box::pin(run_until(server, async {
        let controller = handles
            .scheduler
            .controller()
            .await
            .expect("controller is published");
        let cookie = session_token(&handles.clock, &identity(ALICE)).await;
        let (status, session) = session_by_cookie(&controller, &cookie).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            session.principal,
            Some(name::of::<name::Principals>(&identity(ALICE)))
        );
        assert_eq!(session.permissions, Vec::new());
        assert!(session.expire_time.is_some());
    }))
    .await;
}

#[tokio::test]
async fn session_cookie_of_a_revoked_principal_is_answered_with_the_envelope() {
    let (parts, _dials, handles) = tls_parts();
    handles.verifier.accept(ADMIN_PROOF, ADMIN);
    let server = prepare_tls(parts).await;
    Box::pin(run_until(server, async {
        let controller = handles
            .scheduler
            .controller()
            .await
            .expect("controller is published");
        let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
        let cookie = session_token(&handles.clock, &identity(ALICE)).await;
        let (status, _) = session_by_cookie(&controller, &cookie).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "alice's session is served before revocation"
        );

        revoke(&controller, &admin, &principal_revocation(ALICE)).await;

        let request = request(
            "GET",
            GET_SESSION.template,
            &[(SESSION_COOKIE, cookie.expose())],
        );
        let response = controller.call_api(request).await;
        assert_error(response, ErrorReason::PrincipalRevoked).await;
    }))
    .await;
}

#[tokio::test]
async fn worker_gate_admits_a_ledgered_certificate_and_refuses_an_unknown_one() {
    let (parts, _dials, handles) = tls_parts();
    let ca = authority(&handles.clock).await;
    let (issued, _key) = client_certificate(&ca, WORKER_PROVIDER, WORKER).await;
    let (unknown, _unknown_key) = client_certificate(&ca, WORKER_PROVIDER, "stranger").await;
    handles.repository.insert(record(
        issued.serial.as_str(),
        WORKER_PROVIDER,
        WORKER,
        handles.clock.wall(),
    ));
    let server = prepare_tls(parts).await;
    Box::pin(run_until(server, async {
        let controller = handles
            .scheduler
            .controller()
            .await
            .expect("controller is published");
        let known = Peer {
            certificate: Some(issued.serial.clone()),
            ..Peer::default()
        };
        let mut known_parts = request_parts(GET_SERVER.template, None);
        controller
            .admit_worker(&known, &mut known_parts)
            .await
            .expect("a worker certificate in the ledger is admitted");
        let stranger = Peer {
            certificate: Some(unknown.serial.clone()),
            ..Peer::default()
        };
        let mut stranger_parts = request_parts(GET_SERVER.template, None);
        let refused = controller
            .admit_worker(&stranger, &mut stranger_parts)
            .await;
        assert!(
            matches!(refused, Err(PortError::Rejected { .. })),
            "a certificate outside the ledger is refused: {refused:?}"
        );
    }))
    .await;
}

/// A principal revoked before startup is refused as PRINCIPAL_REVOKED on authenticate and on gated requests.
#[tokio::test]
async fn a_principal_revoked_before_the_start_is_refused_as_revoked_everywhere() {
    // The first server mints alice's token before any revocation is recorded. Its key is the
    // authority's public key, which is the same in every test build, so the token is valid on a
    // second server.
    let (parts, _dials, handles) = tls_parts();
    handles.verifier.accept(ALICE_PROOF, ALICE);
    let server = prepare_tls(parts).await;
    let mut alice_token = None;
    Box::pin(run_until(server, async {
        let controller = handles
            .scheduler
            .controller()
            .await
            .expect("controller is published");
        alice_token = Some(mint_token(&controller, PROVIDER, ALICE_PROOF).await);
    }))
    .await;
    let alice_token = alice_token.expect("the first server mints alice's token");

    // The second server starts over a repository where alice is already revoked.
    let (parts, _dials, handles) = tls_parts();
    handles.verifier.accept(ALICE_PROOF, ALICE);
    handles
        .repository
        .revoke_principal(&identity(ALICE), handles.clock.wall())
        .await
        .expect("the revocation is recorded before the start");
    let server = prepare_tls(parts).await;
    Box::pin(run_until(server, async {
        let controller = handles
            .scheduler
            .controller()
            .await
            .expect("controller is published");
        let refused = authenticate(&controller, PROVIDER, ALICE_PROOF).await;
        assert_error(refused, ErrorReason::PrincipalRevoked).await;
        let response = controller
            .call_api(api_request(
                "GET",
                GET_SESSION.template,
                Some(&alice_token),
                None,
            ))
            .await;
        assert_error(response, ErrorReason::PrincipalRevoked).await;
    }))
    .await;
}

#[tokio::test]
async fn a_worker_certificate_revoked_before_the_start_is_refused_after_it() {
    let (parts, dials, handles) = tls_parts();
    let ca = authority(&handles.clock).await;
    let (revoked, revoked_key) = client_certificate(&ca, WORKER_PROVIDER, WORKER).await;
    let (live, live_key) = client_certificate(&ca, WORKER_PROVIDER, "worker-2").await;
    handles.repository.insert(IssuedCertRecord {
        status: CertStatus::Revoked {
            at: handles.clock.wall(),
        },
        ..record(
            revoked.serial.as_str(),
            WORKER_PROVIDER,
            WORKER,
            handles.clock.wall(),
        )
    });
    handles.repository.insert(record(
        live.serial.as_str(),
        WORKER_PROVIDER,
        "worker-2",
        handles.clock.wall(),
    ));
    let mut received = handles.scheduler.connections();
    let server = prepare_tls(parts).await;
    Box::pin(run_until(server, async {
        // The unrevoked certificate of the same ledger handshakes, so the refusal below is not
        // a broken certificate authority.
        let _admitted = tls_client(
            &dials.worker,
            WORKER_HOST,
            ca.certificate(),
            Some((&live.certificate, &live_key)),
            &handles.clock,
        )
        .await
        .expect("an unrevoked worker certificate handshakes");
        next_connection(&mut received).await;

        assert_worker_refused(
            tls_client(
                &dials.worker,
                WORKER_HOST,
                ca.certificate(),
                Some((&revoked.certificate, &revoked_key)),
                &handles.clock,
            )
            .await,
            &mut received,
        )
        .await;
    }))
    .await;
}

#[tokio::test]
async fn prepare_fails_closed_when_the_revoked_principals_cannot_be_read() {
    let (parts, _dials, handles) = tls_parts();
    handles.repository.fail_principals(true);
    match Server::prepare(parts, tls_settings()).await {
        Err(ServerError::Revocations(_)) => {}
        Err(error) => panic!("preparation fails on the revocation read: {error}"),
        Ok(_) => panic!("preparation must not succeed without the revoked principals"),
    }
}

#[tokio::test]
async fn prepare_fails_closed_when_the_revoked_certificates_cannot_be_read() {
    let (parts, _dials, handles) = tls_parts();
    handles.repository.fail_revoked_reads(true);
    match Server::prepare(parts, tls_settings()).await {
        Err(ServerError::Revocations(_)) => {}
        Err(error) => panic!("preparation fails on the revocation read: {error}"),
        Ok(_) => panic!("preparation must not succeed without the revoked certificates"),
    }
}
