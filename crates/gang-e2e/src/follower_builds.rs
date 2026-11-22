//! Passive controllers completely refuse new public requests while builds use the sole leader.

use std::sync::Arc;

use gang_clock::SystemClock;
use gang_tls::{HttpProtocol, HttpTransport, ServerTrust};
use nativelink_proto::build::bazel::remote::execution::v2::{
    GetActionResultRequest, RequestMetadata,
};

use crate::check::{Check, prefix_checks};
use crate::cluster::Cluster;
use crate::errors::{E2eError, Result};
use crate::h2_frames::H2Channel;
use crate::http::Http;
use crate::identity::Admission;
use crate::server::{Answer, Presented, Request, ServerApi};
use crate::system::Probe;
use crate::verdict;

const CONTROLLERS: usize = 3;

fn discovery_body(answer: &Answer) -> std::result::Result<serde_json::Value, String> {
    let mut body: serde_json::Value =
        serde_json::from_slice(&answer.body).map_err(|error| error.to_string())?;
    let certificate = body
        .get_mut("caCertificate")
        .ok_or("missing caCertificate")?;
    let pem = certificate
        .as_str()
        .ok_or("caCertificate is not a string")?;
    let pem =
        gang_types::CertificatePem::try_new(pem.to_owned()).map_err(|error| error.to_string())?;
    let pin = gang_tls::ca_pin_of_certificate(&pem).map_err(|error| error.to_string())?;
    // Controllers independently self-sign certificates; their serials and signatures
    // differ even when the pinned public key identifies the same CA.
    *certificate = serde_json::Value::String(pin.to_string());
    Ok(body)
}

fn leader_discovery_check(response: &Answer) -> Check {
    let body = discovery_body(response);
    Check::expect(
        "leader: discovery succeeds without a not-started marker",
        response.status == 200
            && body.is_ok()
            && response
                .header(gang_types::handover::REQUEST_NOT_STARTED_HEADER)
                .is_none(),
        || format!("leader {response:?}, normalized discovery: {body:?}"),
    )
}

fn refusal_check(follower: usize, path: &str, response: &Answer, grpc: bool) -> Check {
    let marker = response.header(gang_types::handover::REQUEST_NOT_STARTED_HEADER);
    let expected_status = if grpc { 200 } else { 503 };
    Check::expect(
        format!("follower {follower}: {path} is refused before starting"),
        response.status == expected_status
            && marker == Some("true")
            && response
                .headers
                .iter()
                .filter(|(name, _)| name == gang_types::handover::REQUEST_NOT_STARTED_HEADER)
                .count()
                == 1
            && (!grpc || response.header("grpc-status") == Some("14")),
        || {
            format!(
                "expected HTTP {expected_status}, not-started true{}; got {response:?}",
                if grpc { ", grpc-status 14" } else { "" }
            )
        },
    )
}

fn check_leader_only(
    client: &Http,
    endpoint: &gang_types::Url,
    token: &str,
    follower: usize,
    checks: &mut Vec<Check>,
) -> Result<()> {
    for path in ["/v1/server", "/v1/builds", "/v1/state"] {
        let request = Request::get(path).presenting(Presented::Bearer(token.to_owned()));
        let response = crate::login_across::exchange(client, endpoint, &request)?;
        checks.push(refusal_check(follower, path, &response, false));
    }
    for path in [
        "/build.bazel.remote.execution.v2.ActionCache/GetActionResult",
        "/build.bazel.remote.execution.v2.Execution/Execute",
        "/build.bazel.remote.execution.v2.Execution/WaitExecution",
        "/google.longrunning.Operations/GetOperation",
    ] {
        let request = Request::new(http::Method::POST, path)
            .body("application/grpc", vec![0; 5])
            .presenting(Presented::Bearer(token.to_owned()));
        let response = crate::login_across::exchange(client, endpoint, &request)?;
        checks.push(refusal_check(follower, path, &response, true));
    }
    Ok(())
}

/// Checks complete passive refusals on HTTP and gRPC routes, and cold and warm leader builds.
///
/// # Errors
/// Returns an error if the cluster, clients, or builds cannot be operated.
pub fn follower_builds(cluster: &Cluster<'_>) -> Result<Vec<Check>> {
    let stack = cluster.stack();
    let credentials = ServerApi::administrator_credentials(stack)?;
    let remote = stack.administrator_remote(&credentials)?;
    let pin = stack.ca_pin()?;
    let client = Http::try_new(
        HttpTransport::Tls {
            trust: ServerTrust::Pin(&pin),
        },
        HttpProtocol::Http2,
        stack.settings().command_timeout,
    )?;
    let config = Arc::new(
        gang_tls::client_config(
            ServerTrust::Pin(&pin),
            None,
            &[b"h2"],
            Arc::new(SystemClock::new()),
        )
        .map_err(E2eError::system_from("configuring follower request TLS"))?,
    );
    let leader = cluster.wait_for_leader()?;
    cluster.wait_for_single_leader(leader)?;
    cluster.wait_for_serving(&[leader])?;
    let shared_request =
        Request::get("/v1/server").presenting(Presented::Bearer(credentials.token.clone()));
    let leader_endpoint = cluster.direct_endpoint(leader);
    let leader_response =
        crate::login_across::exchange(&client, &leader_endpoint, &shared_request)?;
    let mut leader_channel = match H2Channel::connect(
        Arc::clone(&config),
        &leader_endpoint,
        Some(&credentials.token),
        stack.settings().handshake_timeout,
    )? {
        Admission::Accepted(channel) => channel,
        Admission::Refused => return Err(E2eError::system("leader refused TLS connection")),
    };
    let leader_grpc = leader_channel.get_action_result(
        &GetActionResultRequest::default(),
        &RequestMetadata::default(),
        stack.settings().command_timeout,
    )?;
    let mut checks = vec![
        leader_discovery_check(&leader_response),
        Check::expect(
            "leader: malformed ActionCache request reaches its handler",
            leader_grpc.is_some() && leader_grpc != Some(14),
            || format!("leader grpc-status {leader_grpc:?}"),
        ),
    ];
    for follower in (0..CONTROLLERS).filter(|&index| index != leader) {
        let endpoint = cluster.direct_endpoint(follower);
        check_leader_only(
            &client,
            &endpoint,
            &credentials.token,
            follower,
            &mut checks,
        )?;
    }
    let build = || {
        let (_sender, receiver) = tokio::sync::oneshot::channel();
        stack
            .bazel()
            .build_until(&remote, &[Probe::Pinned], None, receiver)
    };
    let cold = build()?;
    stack.bazel().clean(&remote)?;
    let warm = build()?;
    checks.extend(prefix_checks(
        vec![verdict::remote_execution(
            &cold,
            1,
            stack.settings().stderr_tail_lines,
        )],
        "leader cold build",
    ));
    checks.extend(prefix_checks(
        vec![verdict::cache_hit(
            &warm,
            1,
            stack.settings().stderr_tail_lines,
        )],
        "leader warm build",
    ));
    Ok(checks)
}

#[cfg(test)]
mod tests {
    use super::{leader_discovery_check, refusal_check};
    use crate::server::Answer;

    async fn certificate(seed: u8) -> gang_types::CertificatePem {
        gang_mtls::CertificateAuthority::establish(
            gang_testkit::TestSigner::new(seed).expect("valid signer seed"),
            gang_clock::TestClock::new(gang_clock::Timestamp::from_unix_millis(1_000_000)),
            gang_types::TrustDomain::try_new("example.test").expect("valid trust domain"),
            std::time::Duration::from_mins(1),
        )
        .await
        .expect("test authority establishes")
        .certificate()
        .clone()
    }

    fn discovery(pem: &str, field: &str) -> Answer {
        Answer::json(
            200,
            &serde_json::json!({
                "caCertificate": pem,
                "name": field,
            }),
        )
        .expect("discovery serializes")
    }

    #[tokio::test]
    async fn leader_discovery_requires_valid_certificate_and_unmarked_success() {
        let first = certificate(1).await;
        let leader = discovery(first.as_str(), "server");
        assert!(leader_discovery_check(&leader).is_passed());
        assert!(leader_discovery_check(&Answer::empty(503)).is_failure());
        assert!(leader_discovery_check(&discovery("not a certificate", "server")).is_failure());
        let missing = Answer::json(200, &serde_json::json!({"name": "server"}))
            .expect("discovery serializes");
        assert!(leader_discovery_check(&missing).is_failure());
        let marked = leader.with_header(gang_types::handover::REQUEST_NOT_STARTED_HEADER, "true");
        assert!(leader_discovery_check(&marked).is_failure());
    }

    #[test]
    fn refusal_requires_status_marker_and_grpc_status() {
        let marker = gang_types::handover::REQUEST_NOT_STARTED_HEADER;
        let http = Answer::empty(503).with_header(marker, "true");
        assert!(refusal_check(1, "/v1/state", &http, false).is_passed());
        let duplicate = http.clone().with_header(marker, "true");
        assert!(refusal_check(1, "/v1/server", &duplicate, false).is_failure());
        assert!(refusal_check(1, "/v1/state", &Answer::empty(503), false).is_failure());
        assert!(
            refusal_check(
                1,
                "/v1/state",
                &Answer::empty(200).with_header(marker, "true"),
                false
            )
            .is_failure()
        );
        let grpc = Answer::empty(200)
            .with_header(marker, "true")
            .with_header("grpc-status", "14");
        assert!(refusal_check(1, "/grpc", &grpc, true).is_passed());
        assert!(
            refusal_check(
                1,
                "/grpc",
                &Answer::empty(200).with_header(marker, "true"),
                true
            )
            .is_failure()
        );
        assert!(
            refusal_check(
                1,
                "/grpc",
                &Answer::empty(200)
                    .with_header(marker, "true")
                    .with_header("grpc-status", "13"),
                true
            )
            .is_failure()
        );
        assert!(
            refusal_check(
                1,
                "/grpc",
                &Answer::empty(200).with_header("grpc-status", "14"),
                true
            )
            .is_failure()
        );
        assert!(
            refusal_check(
                1,
                "/grpc",
                &Answer::empty(503)
                    .with_header(marker, "true")
                    .with_header("grpc-status", "14"),
                true
            )
            .is_failure()
        );
    }
}
