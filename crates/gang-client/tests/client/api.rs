//! `ServerApi`: the HTTP boundary to a Gang server, observed against the fake server.

use std::sync::Arc;
use std::time::Duration;

use gang_client::{ClientError, ClientOptions, ServerApi};
use gang_clock::{Clock, Sleep, TestClock, Tick, Timestamp};
use gang_mtls::KeyPair;
use gang_tls::ServerTrust;
use gang_types::{
    AccessToken, Bytes, CaPin, CertSerial, Fix, NonZeroBytes, Proof, ProviderName, Url,
};
use gang_wire::routes::{CREATE_CERTIFICATE, REVOKE_CERTIFICATE};
use gang_wire::{
    AuthenticateRequest, Authentication, CreateCertificateRequest, ErrorReason, ErrorResponse,
    Status,
};
use serde_json::json;

use super::support::*;

fn api(url: &Url, trust: ServerTrust<'_>, options: &ClientOptions) -> ServerApi {
    ServerApi::try_new(url, trust, options, Arc::new(TestClock::new(START)))
        .expect("the url is accepted")
}

fn options_with_limit(limit: u64) -> ClientOptions {
    ClientOptions {
        max_response_size: NonZeroBytes::new(Bytes::new(limit)).expect("the limit is not zero"),
        ..ClientOptions::default()
    }
}

fn serial() -> CertSerial {
    CertSerial::try_new("0c0c").expect("serial is valid in test")
}

#[tokio::test]
async fn server_over_plaintext_is_parsed() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    let api = api(
        &harness.server.url(),
        ServerTrust::System,
        &ClientOptions::default(),
    );

    let info = api.server(None).await.expect("server is served");

    assert_eq!(info, google());
    assert_eq!(
        info.ca_certificate
            .map(|certificate| certificate.as_str().to_owned()),
        Some(ca_certificate().as_str().to_owned())
    );
    assert!(matches!(info.authentication, Authentication::Token(_)));
}

#[tokio::test]
async fn server_over_tls_is_served_to_a_client_with_the_ca_pin() {
    let harness = Harness::start_tls(ConsoleMode::Silent, google()).await;
    let pin = ca_pin();
    let api = api(
        &harness.server.url(),
        ServerTrust::Pin(&pin),
        &ClientOptions::default(),
    );

    let info = api
        .server(None)
        .await
        .expect("server info is served over tls");

    assert_eq!(info, google());
    assert_eq!(
        info.ca_certificate
            .map(|certificate| certificate.as_str().to_owned()),
        Some(ca_certificate().as_str().to_owned())
    );
    assert!(matches!(info.authentication, Authentication::Token(_)));
}

#[tokio::test]
async fn server_with_a_wrong_ca_pin_is_refused_by_the_handshake() {
    let harness = Harness::start_tls(ConsoleMode::Silent, google()).await;
    let wrong_pin = CaPin::new([7; 32]);
    let api = api(
        &harness.server.url(),
        ServerTrust::Pin(&wrong_pin),
        &ClientOptions::default(),
    );

    let error = api
        .server(None)
        .await
        .expect_err("a wrong pin fails the handshake");

    assert!(matches!(error, ClientError::Transport { .. }));
    assert_eq!(
        error.fix().as_deref(),
        Some("add --ca-pin sha256:<hex>; gang server prints it at startup")
    );
}

#[tokio::test]
async fn server_with_no_authentication_parses() {
    let harness = Harness::start_plain(ConsoleMode::Silent, no_auth_info()).await;
    let api = api(
        &harness.server.url(),
        ServerTrust::System,
        &ClientOptions::default(),
    );

    let info = api.server(None).await.expect("server is served");

    assert!(matches!(info.authentication, Authentication::None(_)));
}

#[tokio::test]
async fn error_answer_with_an_api_error_body_carries_its_message() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    let body = serde_json::to_vec(&ErrorResponse::from(Status::new(
        ErrorReason::MalformedRequest,
        "the proof was refused",
        [],
    )))
    .expect("error envelope serializes");
    harness.server.force_response(403, body);
    let api = api(
        &harness.server.url(),
        ServerTrust::System,
        &ClientOptions::default(),
    );

    let error = api
        .server(None)
        .await
        .expect_err("the server refused the request");

    let ClientError::Server {
        status,
        message,
        retry_after,
    } = error
    else {
        panic!("expected a server error, got {error}");
    };
    assert_eq!(status, 403);
    assert_eq!(message, "the proof was refused");
    assert_eq!(retry_after, None);
}

#[tokio::test]
async fn error_answer_with_a_plain_text_body_carries_the_text() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    harness
        .server
        .force_response(503, b"maintenance window".to_vec());
    let budget = Duration::from_millis(1);
    let clock = deadline_clock(harness.clock.clone(), budget);
    let api = ServerApi::try_new(
        &harness.server.url(),
        ServerTrust::System,
        &ClientOptions {
            api_retry_budget: gang_types::NonZeroDuration::from_millis(1),
            ..ClientOptions::default()
        },
        clock.clone(),
    )
    .expect("client binds");
    let (result, ()) = tokio::join!(api.server(None), async {
        clock.sleeping.notified().await;
        clock.clock.advance(budget);
    });
    let error = result.expect_err("the server is unavailable");

    let ClientError::Server {
        status,
        message,
        retry_after,
    } = error
    else {
        panic!("expected a server error, got {error}");
    };
    assert_eq!(status, 503);
    assert_eq!(message, "maintenance window");
    assert_eq!(retry_after, None);
}

#[tokio::test]
async fn answer_above_the_size_limit_is_refused() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    let options = options_with_limit(16);
    let api = api(&harness.server.url(), ServerTrust::System, &options);

    let error = api
        .server(None)
        .await
        .expect_err("the body exceeds the limit");

    assert!(
        matches!(error, ClientError::BodyTooLarge { limit } if limit == options.max_response_size)
    );
}

#[tokio::test]
async fn successful_answer_that_is_not_a_document_is_a_body_error() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    harness
        .server
        .force_response(200, b"not a server information document".to_vec());
    let api = api(
        &harness.server.url(),
        ServerTrust::System,
        &ClientOptions::default(),
    );

    let error = api
        .server(None)
        .await
        .expect_err("the body is not a document");

    assert!(matches!(error, ClientError::Body(_)));
}

#[test]
fn ftp_base_is_an_unsupported_server_url() {
    let base = Url::parse("ftp://files.example/").expect("url parses");

    let Err(error) = ServerApi::try_new(
        &base,
        ServerTrust::System,
        &ClientOptions::default(),
        Arc::new(TestClock::new(START)),
    ) else {
        panic!("an ftp base must be refused");
    };

    assert!(matches!(error, ClientError::UnsupportedServerUrl { .. }));
}

#[tokio::test]
async fn authenticated_token_is_presented_as_the_bearer_on_later_requests() {
    let harness = Harness::start_plain(
        ConsoleMode::Silent,
        token_info(&[("google", json!({}))], false),
    )
    .await;
    harness.server.accept_proof("google", "proof-1");
    let url = harness.server.url();
    let minting = api(&url, ServerTrust::System, &ClientOptions::default());
    let minted = minting
        .authenticate(&AuthenticateRequest {
            provider: ProviderName::try_new("google").expect("provider name is valid"),
            credential: gang_wire::Credential::Proof(Proof::new("proof-1")),
        })
        .await
        .expect("the proof is accepted");
    let api = api(&url, ServerTrust::System, &ClientOptions::default());

    api.revoke_certificate(Some(&minted.token), &serial())
        .await
        .expect("the revocation is answered");

    assert_eq!(
        harness.server.calls().last(),
        Some(&(
            REVOKE_CERTIFICATE.path(&serial()),
            Some(minted.token.expose().to_owned())
        ))
    );
}

#[test]
fn plaintext_client_builds_without_tls_configuration() {
    let http = Url::parse("http://127.0.0.1:1").expect("url is valid");
    let built = ServerApi::try_new(
        &http,
        ServerTrust::System,
        &ClientOptions::default(),
        Arc::new(TestClock::new(START)),
    );
    assert!(
        built.is_ok(),
        "an http base builds without a TLS configuration"
    );
}

#[tokio::test]
async fn plaintext_requests_start_in_http2_without_an_upgrade() {
    let server = FakeServer::start_plain(no_auth_info(), Arc::new(TestClock::new(START))).await;
    let api = api(
        &server.url(),
        ServerTrust::System,
        &ClientOptions::default(),
    );

    let info = api
        .server(None)
        .await
        .expect("an HTTP/2-only server answers prior-knowledge requests");
    assert_eq!(info, no_auth_info());

    let error = api
        .revoke_certificate(None, &serial())
        .await
        .expect_err("revocation without a bearer is refused");
    assert!(matches!(error, ClientError::Server { status: 401, .. }));
}

/// Answers the server information request with a 429 carrying `headers` and `body`, and returns
/// the wait the client reports for it.
async fn rate_limit_wait(headers: &[(&str, &str)], body: Vec<u8>) -> Option<Duration> {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    harness
        .server
        .force_response_with_headers(429, headers, body);
    let api = api(
        &harness.server.url(),
        ServerTrust::System,
        &ClientOptions::default(),
    );

    let error = api
        .server(None)
        .await
        .expect_err("the server is rate limiting");

    let ClientError::RateLimited { retry_after } = error else {
        panic!("expected a rate limit, got {error}");
    };
    retry_after
}

/// The error envelope the server sends for `status`.
fn error_envelope(status: Status) -> Vec<u8> {
    serde_json::to_vec(&ErrorResponse::from(status)).expect("error envelope serializes")
}

/// A rate limit status that carries a `RetryInfo` detail asking for a three second wait.
fn rate_limit_with_retry_info() -> Status {
    Status::new(ErrorReason::RateLimited, "too many requests", [])
        .with_retry_delay(Duration::from_secs(3))
}

#[tokio::test]
async fn retry_after_header_is_the_wait_of_a_rate_limited_answer() {
    let body = error_envelope(Status::new(
        ErrorReason::RateLimited,
        "too many requests",
        [],
    ));

    let wait = rate_limit_wait(&[("retry-after", "7")], body).await;

    assert_eq!(wait, Some(Duration::from_secs(7)));
}

#[tokio::test]
async fn retry_info_of_the_envelope_is_the_wait_when_there_is_no_header() {
    let body = error_envelope(rate_limit_with_retry_info());

    let wait = rate_limit_wait(&[], body).await;

    assert_eq!(wait, Some(Duration::from_secs(3)));
}

#[tokio::test]
async fn retry_after_header_wins_over_the_envelope_retry_info() {
    let body = error_envelope(rate_limit_with_retry_info());

    let wait = rate_limit_wait(&[("retry-after", "7")], body).await;

    assert_eq!(wait, Some(Duration::from_secs(7)));
}

#[tokio::test]
async fn bare_rate_limit_answer_has_no_wait() {
    let wait = rate_limit_wait(&[], b"too many requests".to_vec()).await;

    assert_eq!(wait, None);
}

#[tokio::test]
async fn non_numeric_retry_after_falls_back_to_the_envelope_retry_info() {
    let body = error_envelope(rate_limit_with_retry_info());

    let wait = rate_limit_wait(&[("retry-after", "later")], body).await;

    assert_eq!(wait, Some(Duration::from_secs(3)));
}

#[tokio::test]
async fn non_numeric_retry_after_without_an_envelope_has_no_wait() {
    let wait = rate_limit_wait(&[("retry-after", "later")], b"too many requests".to_vec()).await;

    assert_eq!(wait, None);
}

/// An unavailable server's wait comes from the same places as a rate limit's.
#[tokio::test]
async fn retry_info_of_the_envelope_is_the_wait_of_an_unavailable_answer() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    let body = error_envelope(
        Status::new(
            ErrorReason::DependencyUnavailable,
            "database unavailable",
            [],
        )
        .with_retry_delay(Duration::from_secs(3)),
    );
    harness.server.force_response(503, body);
    let budget = Duration::from_secs(1);
    let clock = deadline_clock(harness.clock.clone(), budget);
    let api = ServerApi::try_new(
        &harness.server.url(),
        ServerTrust::System,
        &ClientOptions {
            api_retry_budget: gang_types::NonZeroDuration::from_secs(1),
            ..ClientOptions::default()
        },
        clock.clone(),
    )
    .expect("client binds");
    let (result, ()) = tokio::join!(api.server(None), async {
        clock.sleeping.notified().await;
        clock.clock.advance(budget);
    });
    let error = result.expect_err("the server is unavailable");

    assert!(
        matches!(
            error,
            ClientError::Server {
                status: 503,
                retry_after: Some(wait),
                ..
            } if wait == Duration::from_secs(3)
        ),
        "{error}"
    );
}

#[tokio::test]
async fn server_and_time_reads_the_date_header_over_plaintext() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    harness.clock.advance(Duration::from_mins(3));
    let api = api(
        &harness.server.url(),
        ServerTrust::System,
        &ClientOptions::default(),
    );

    let (server, time) = api.server_and_time(None).await.expect("server is served");

    assert_eq!(server, google());
    assert_eq!(time, Some(START + Duration::from_mins(3)));
}

#[tokio::test]
async fn server_and_time_reads_the_date_header_over_tls() {
    let harness = Harness::start_tls(ConsoleMode::Silent, google()).await;
    let pin = ca_pin();
    let api = api(
        &harness.server.url(),
        ServerTrust::Pin(&pin),
        &ClientOptions::default(),
    );

    let (server, time) = api.server_and_time(None).await.expect("server is served");

    assert_eq!(server, google());
    assert_eq!(time, Some(START));
}

#[tokio::test]
async fn create_certificate_posts_the_signing_request_and_returns_the_certificate() {
    let harness = Harness::start_plain(ConsoleMode::Silent, no_auth_info()).await;
    let api = api(
        &harness.server.url(),
        ServerTrust::System,
        &ClientOptions::default(),
    );
    let pem_csr = KeyPair::generate()
        .expect("generating a key succeeds")
        .certificate_request()
        .expect("the key yields a certificate request");
    let request = CreateCertificateRequest {
        provider: ProviderName::try_new("corp-ci").expect("provider name is valid in test"),
        proof: Proof::new("worker-proof"),
        pem_csr,
    };

    let issued = api
        .create_certificate(None, &request)
        .await
        .expect("the certificate is issued");
    let bearer = AccessToken::new("bearer-token".to_owned());
    api.create_certificate(Some(&bearer), &request)
        .await
        .expect("the certificate is issued with a bearer too");

    assert_eq!(issued.name, "certificates/0c0c");
    assert_eq!(issued.pem_certificate, Some(issued_certificate_pem()));
    let received = harness.server.certificate_requests();
    assert_eq!(received.len(), 2);
    for (method, path, _, body) in &received {
        assert_eq!(method.as_str(), "POST");
        assert_eq!(path.as_str(), CREATE_CERTIFICATE.template);
        assert_eq!(body.provider, request.provider);
        assert!(body.proof.matches(request.proof.expose()));
        assert_eq!(body.pem_csr, request.pem_csr);
    }
    assert_eq!(received[0].2, None);
    assert_eq!(received[1].2.as_deref(), Some("bearer-token"));
}

struct RetryClock {
    clock: Arc<TestClock>,
    delay: Duration,
    sleeping: tokio::sync::Notify,
}

impl Clock for RetryClock {
    fn wall(&self) -> Timestamp {
        self.clock.wall()
    }

    fn monotonic(&self) -> Tick {
        self.clock.monotonic()
    }

    fn sleep_until(&self, deadline: Tick) -> Sleep {
        if deadline.saturating_duration_since(self.clock.monotonic()) == self.delay {
            self.sleeping.notify_one();
        }
        self.clock.sleep_until(deadline)
    }
}

fn retry_clock(clock: Arc<TestClock>, delay: Duration) -> Arc<RetryClock> {
    Arc::new(RetryClock {
        clock,
        delay,
        sleeping: tokio::sync::Notify::new(),
    })
}

pub(super) struct DeadlineClock {
    clock: Arc<TestClock>,
    deadline: Tick,
    occurrences: std::sync::atomic::AtomicUsize,
    pub(super) sleeping: tokio::sync::Notify,
}

impl Clock for DeadlineClock {
    fn wall(&self) -> Timestamp {
        self.clock.wall()
    }

    fn monotonic(&self) -> Tick {
        self.clock.monotonic()
    }

    fn sleep_until(&self, deadline: Tick) -> Sleep {
        if deadline == self.deadline
            && self.occurrences.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1
        {
            self.sleeping.notify_one();
        }
        self.clock.sleep_until(deadline)
    }
}

pub(super) fn deadline_clock(clock: Arc<TestClock>, budget: Duration) -> Arc<DeadlineClock> {
    Arc::new(DeadlineClock {
        deadline: clock.monotonic() + budget,
        clock,
        occurrences: std::sync::atomic::AtomicUsize::new(0),
        sleeping: tokio::sync::Notify::new(),
    })
}

#[tokio::test]
async fn marked_post_retries_identical_body_and_bearer() {
    let harness = Harness::start_plain(ConsoleMode::Silent, no_auth_info()).await;
    harness
        .server
        .script_responses(&[(503, &[("gang-request-not-started", "true")], b"warming")]);
    let clock = retry_clock(harness.clock.clone(), Duration::from_millis(100));
    let api = ServerApi::try_new(
        &harness.server.url(),
        ServerTrust::System,
        &ClientOptions::default(),
        clock.clone(),
    )
    .expect("client binds");
    let request = CreateCertificateRequest {
        provider: ProviderName::try_new("corp-ci").expect("valid provider"),
        proof: Proof::new("worker-proof"),
        pem_csr: KeyPair::generate()
            .expect("key")
            .certificate_request()
            .expect("csr"),
    };
    let bearer = AccessToken::new("bearer-token");
    let (result, ()) = tokio::join!(api.create_certificate(Some(&bearer), &request), async {
        clock.sleeping.notified().await;
        clock.clock.advance(clock.delay);
    });
    assert!(result.is_ok());
    let requests = harness.server.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0], requests[1]);
    assert_eq!(requests[0].2.as_deref(), Some("bearer-token"));
}

#[tokio::test]
async fn default_budget_survives_planned_handover_for_marked_mutation() {
    let harness = Harness::start_plain(ConsoleMode::Silent, no_auth_info()).await;
    let refusal = (
        503,
        &[("gang-request-not-started", "true"), ("retry-after", "5")][..],
        &b"warming"[..],
    );
    harness.server.script_responses(&[refusal; 4]);
    let clock = retry_clock(harness.clock.clone(), Duration::from_secs(5));
    let options = ClientOptions::default();
    let bound = gang_types::handover::minimum_cli_last_attempt();
    assert!(options.api_retry_budget.get() > bound);
    let api = ServerApi::try_new(
        &harness.server.url(),
        ServerTrust::System,
        &options,
        clock.clone(),
    )
    .expect("client binds");
    let started = clock.monotonic();
    let (result, ()) = tokio::join!(api.create_device_authorization(), async {
        for _ in 0..4 {
            clock.sleeping.notified().await;
            clock.clock.advance(clock.delay);
        }
    });
    assert!(result.is_ok());
    assert!(clock.monotonic().saturating_duration_since(started) > bound);
    assert_eq!(harness.server.requests().len(), 5);
}

#[tokio::test]
async fn generic_get_refusal_retries() {
    let harness = Harness::start_plain(ConsoleMode::Silent, no_auth_info()).await;
    harness.server.script_responses(&[(503, &[], b"warming")]);
    let clock = retry_clock(harness.clock.clone(), Duration::from_millis(100));
    let api = ServerApi::try_new(
        &harness.server.url(),
        ServerTrust::System,
        &ClientOptions::default(),
        clock.clone(),
    )
    .expect("client binds");
    let (result, ()) = tokio::join!(api.server(None), async {
        clock.sleeping.notified().await;
        clock.clock.advance(clock.delay);
    });
    assert!(result.is_ok());
    assert_eq!(harness.server.requests().len(), 2);
}

#[tokio::test]
async fn unmarked_post_and_rate_limit_are_not_replayed() {
    for status in [503, 429] {
        let harness = Harness::start_plain(ConsoleMode::Silent, no_auth_info()).await;
        harness
            .server
            .script_responses(&[(status, &[], b"refused")]);
        let api = api(
            &harness.server.url(),
            ServerTrust::System,
            &ClientOptions::default(),
        );
        assert!(api.create_device_authorization().await.is_err());
        assert_eq!(harness.server.requests().len(), 1);
    }
}

#[tokio::test]
async fn retry_after_is_a_minimum_and_survives_budget_exhaustion() {
    let harness = Harness::start_plain(ConsoleMode::Silent, no_auth_info()).await;
    harness
        .server
        .script_responses(&[(503, &[("retry-after", "20")], b"warming")]);
    let clock = deadline_clock(harness.clock.clone(), Duration::from_secs(1));
    let api = ServerApi::try_new(
        &harness.server.url(),
        ServerTrust::System,
        &ClientOptions {
            api_retry_budget: gang_types::NonZeroDuration::from_secs(1),
            ..ClientOptions::default()
        },
        clock.clone(),
    )
    .expect("client binds");
    let (result, ()) = tokio::join!(api.server(None), async {
        clock.sleeping.notified().await;
        assert_eq!(harness.server.requests().len(), 1);
        clock.clock.advance(Duration::from_secs(1));
    });
    let error = result.expect_err("delay exceeds budget");
    assert!(
        matches!(error, ClientError::Server { status: 503, retry_after: Some(wait), .. } if wait == Duration::from_secs(20))
    );
    assert_eq!(harness.server.requests().len(), 1);
}

#[tokio::test]
async fn retry_after_seconds_and_http_date_delay_retry() {
    for value in ["2", "Mon, 01 Jan 2024 00:00:02 GMT"] {
        let harness = Harness::start_plain(ConsoleMode::Silent, no_auth_info()).await;
        harness
            .server
            .script_responses(&[(503, &[("retry-after", value)], b"warming")]);
        let clock = retry_clock(harness.clock.clone(), Duration::from_secs(2));
        let api = ServerApi::try_new(
            &harness.server.url(),
            ServerTrust::System,
            &ClientOptions::default(),
            clock.clone(),
        )
        .expect("client binds");
        let (result, ()) = tokio::join!(api.server(None), async {
            clock.sleeping.notified().await;
            assert_eq!(harness.server.requests().len(), 1);
            clock.clock.advance(clock.delay);
        });
        assert!(result.is_ok());
        assert_eq!(harness.server.requests().len(), 2);
    }
}

#[tokio::test]
async fn grpc_unavailable_requires_marker_for_post() {
    let harness = Harness::start_plain(ConsoleMode::Silent, no_auth_info()).await;
    harness
        .server
        .script_responses(&[(200, &[("grpc-status", "14")], b"")]);
    let api = api(
        &harness.server.url(),
        ServerTrust::System,
        &ClientOptions::default(),
    );
    let error = api
        .create_device_authorization()
        .await
        .expect_err("unmarked refusal");
    assert!(
        matches!(error, ClientError::Server { status: 503, ref message, .. } if message == "Service Unavailable")
    );
    assert_eq!(harness.server.requests().len(), 1);
}

#[tokio::test]
async fn cancelling_during_retry_delay_does_not_send_again() {
    let harness = Harness::start_plain(ConsoleMode::Silent, no_auth_info()).await;
    harness.server.script_responses(&[(503, &[], b"warming")]);
    let clock = retry_clock(harness.clock.clone(), Duration::from_millis(100));
    let api = ServerApi::try_new(
        &harness.server.url(),
        ServerTrust::System,
        &ClientOptions::default(),
        clock.clone(),
    )
    .expect("client binds");
    let mut request = Box::pin(api.server(None));
    tokio::select! {
        result = &mut request => panic!("unexpected completion: {result:?}"),
        () = clock.sleeping.notified() => {}
    }
    drop(request);
    clock.clock.advance(Duration::from_secs(1));
    assert_eq!(harness.server.requests().len(), 1);
}

#[tokio::test]
async fn unmarked_device_redemption_refusal_not_retried() {
    let harness = Harness::start_plain(ConsoleMode::Silent, no_auth_info()).await;
    harness
        .server
        .script_responses(&[(503, &[], b"issuer unavailable after consumption")]);
    let api = api(
        &harness.server.url(),
        ServerTrust::System,
        &ClientOptions::default(),
    );
    let secret = gang_types::DeviceSecret::new("device-secret");
    let error = api
        .redeem_device_authorization("code", &secret)
        .await
        .expect_err("issuer failed");
    assert!(matches!(error, ClientError::Server { status: 503, .. }));
    assert_eq!(harness.server.requests().len(), 1);
}

#[tokio::test]
async fn retry_header_takes_precedence_over_envelope() {
    let harness = Harness::start_plain(ConsoleMode::Silent, no_auth_info()).await;
    let body = error_envelope(
        Status::new(ErrorReason::DependencyUnavailable, "warming", [])
            .with_retry_delay(Duration::from_secs(7)),
    );
    harness
        .server
        .script_responses(&[(503, &[("retry-after", "2")], &body)]);
    let clock = retry_clock(harness.clock.clone(), Duration::from_secs(2));
    let api = ServerApi::try_new(
        &harness.server.url(),
        ServerTrust::System,
        &ClientOptions::default(),
        clock.clone(),
    )
    .expect("client binds");
    let (result, ()) = tokio::join!(api.server(None), async {
        clock.sleeping.notified().await;
        clock.clock.advance(clock.delay);
    });
    assert!(result.is_ok());
    assert_eq!(harness.server.requests().len(), 2);
}

#[tokio::test]
async fn advancing_past_budget_during_backoff_returns_last_refusal() {
    let harness = Harness::start_plain(ConsoleMode::Silent, no_auth_info()).await;
    harness.server.script_responses(&[(503, &[], b"warming")]);
    let clock = retry_clock(harness.clock.clone(), Duration::from_millis(100));
    let options = ClientOptions::default();
    let api = ServerApi::try_new(
        &harness.server.url(),
        ServerTrust::System,
        &options,
        clock.clone(),
    )
    .expect("client binds");
    let (result, ()) = tokio::join!(api.server(None), async {
        clock.sleeping.notified().await;
        clock.clock.advance(options.api_retry_budget.get());
    });
    assert!(matches!(
        result,
        Err(ClientError::Server { status: 503, .. })
    ));
    assert_eq!(harness.server.requests().len(), 1);
}

#[tokio::test]
async fn retries_share_one_overall_budget() {
    let harness = Harness::start_plain(ConsoleMode::Silent, no_auth_info()).await;
    harness.server.script_responses(&[
        (503, &[], b"warming"),
        (503, &[], b"warming"),
    ]);
    let clock = retry_clock(harness.clock.clone(), Duration::from_millis(100));
    let options = ClientOptions {
        api_retry_budget: gang_types::NonZeroDuration::from_secs(2),
        api_retry_max_backoff: gang_types::NonZeroDuration::from_millis(100),
        ..ClientOptions::default()
    };
    let api = ServerApi::try_new(
        &harness.server.url(),
        ServerTrust::System,
        &options,
        clock.clone(),
    )
    .expect("client binds");
    let (result, ()) = tokio::join!(api.server(None), async {
        clock.sleeping.notified().await;
        clock.clock.advance(Duration::from_millis(900));
        clock.sleeping.notified().await;
        clock.clock.advance(Duration::from_millis(1100));
    });
    assert!(matches!(result, Err(ClientError::Server { status: 503, .. })));
    assert_eq!(harness.server.requests().len(), 2);
}

#[tokio::test]
async fn overall_budget_cancels_an_inflight_attempt_without_retry() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("address");
    let url = Url::parse(&format!("http://{address}/")).expect("url");
    let clock = Arc::new(TestClock::new(START));
    let options = ClientOptions {
        http_timeout: gang_types::NonZeroDuration::from_secs(30),
        ..ClientOptions::default()
    };
    let api =
        ServerApi::try_new(&url, ServerTrust::System, &options, clock.clone()).expect("client");
    let (result, stream) = tokio::join!(api.server(None), async {
        let (stream, _) = listener.accept().await.expect("connection");
        clock.advance(options.api_retry_budget.get());
        stream
    });
    assert!(matches!(result, Err(ClientError::Transport { .. })));
    drop(stream);
}

#[tokio::test]
async fn ambiguous_mutation_transport_failure_is_not_retried() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let url = Url::parse(&format!(
        "http://{}/",
        listener.local_addr().expect("address")
    ))
    .expect("url");
    let clock = retry_clock(Arc::new(TestClock::new(START)), Duration::from_millis(100));
    let api = ServerApi::try_new(
        &url,
        ServerTrust::System,
        &ClientOptions::default(),
        clock.clone(),
    )
    .expect("client");
    let (result, ()) = tokio::join!(
        async {
            tokio::select! {
                result = api.create_device_authorization() => result,
                () = clock.sleeping.notified() => panic!("transport failure was retried"),
            }
        },
        async {
            let (stream, _) = listener.accept().await.expect("connection");
            drop(stream);
        }
    );
    assert!(matches!(result, Err(ClientError::Transport { .. })));
}
