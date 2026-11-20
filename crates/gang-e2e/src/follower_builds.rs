//! Followers serve shared requests and refuse leader-only requests while builds use the leader.

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

fn shared_check(follower: usize, leader: &Answer, response: &Answer) -> Check {
    let marker = gang_types::handover::REQUEST_NOT_STARTED_HEADER;
    let leader_body = discovery_body(leader);
    let follower_body = discovery_body(response);
    Check::expect(
        format!("follower {follower}: shared HTTP response matches leader"),
        response.status == leader.status
            && matches!((&leader_body, &follower_body), (Ok(left), Ok(right)) if left == right)
            && response.header("content-type") == leader.header("content-type")
            && response.header(marker) != Some("true")
            && leader.header(marker) != Some("true"),
        || {
            format!(
                "leader {leader:?}, follower {response:?}, normalized bodies: {leader_body:?}, {follower_body:?}"
            )
        },
    )
}

fn refusal_check(follower: usize, path: &str, response: &Answer, grpc: bool) -> Check {
    let marker = response.header(gang_types::handover::REQUEST_NOT_STARTED_HEADER);
    let expected_status = if grpc { 200 } else { 503 };
    Check::expect(
        format!("follower {follower}: {path} is refused before starting"),
        response.status == expected_status
            && marker == Some("true")
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
    for path in ["/v1/builds", "/v1/state"] {
        let request = Request::get(path).presenting(Presented::Bearer(token.to_owned()));
        let response = crate::login_across::exchange(client, endpoint, &request)?;
        checks.push(refusal_check(follower, path, &response, false));
    }
    for path in [
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

/// Checks shared and leader-only follower routes and cold and warm leader builds.
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
    let mut checks = Vec::new();
    for follower in (0..CONTROLLERS).filter(|&index| index != leader) {
        let endpoint = cluster.direct_endpoint(follower);
        let response = crate::login_across::exchange(&client, &endpoint, &shared_request)?;
        checks.push(shared_check(follower, &leader_response, &response));
        let mut channel = match H2Channel::connect(
            Arc::clone(&config),
            &endpoint,
            Some(&credentials.token),
            stack.settings().handshake_timeout,
        )? {
            Admission::Accepted(channel) => channel,
            Admission::Refused => return Err(E2eError::system("follower refused TLS connection")),
        };
        let grpc = channel.get_action_result(
            &GetActionResultRequest::default(),
            &RequestMetadata::default(),
            stack.settings().command_timeout,
        )?;
        checks.push(Check::expect(
            format!("follower {follower}: malformed ActionCache baseline matches leader code {leader_grpc:?}"),
            leader_grpc.is_some() && grpc == leader_grpc,
            || format!("follower grpc-status {grpc:?}, leader {leader_grpc:?}"),
        ));
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
    use super::{refusal_check, shared_check};
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
    async fn shared_response_matches_only_contract_fields() {
        let first = certificate(1).await;
        let second = certificate(1).await;
        let other = certificate(2).await;
        assert_ne!(first.as_str(), second.as_str());
        let leader = discovery(first.as_str(), "server").with_header("date", "leader");
        let follower = discovery(second.as_str(), "server").with_header("date", "follower");
        assert!(shared_check(1, &leader, &follower).is_passed());
        assert!(shared_check(1, &leader, &discovery(other.as_str(), "server")).is_failure());
        assert!(shared_check(1, &leader, &discovery(second.as_str(), "other")).is_failure());
        let mut different = follower.clone();
        different.status = 503;
        assert!(shared_check(1, &leader, &different).is_failure());
        different = follower.clone();
        different.body = b"not json".to_vec();
        assert!(shared_check(1, &leader, &different).is_failure());
        assert!(shared_check(1, &different, &different).is_failure());
        assert!(shared_check(1, &leader, &discovery("not a certificate", "server")).is_failure());
        let missing = Answer::json(200, &serde_json::json!({"name": "server"}))
            .expect("discovery serializes");
        assert!(shared_check(1, &leader, &missing).is_failure());
        different = follower.clone();
        different.headers.retain(|(name, _)| name != "content-type");
        different = different.with_header("content-type", "text/plain");
        assert!(shared_check(1, &leader, &different).is_failure());
        different = follower
            .clone()
            .with_header(gang_types::handover::REQUEST_NOT_STARTED_HEADER, "true");
        assert!(shared_check(1, &leader, &different).is_failure());
        assert!(shared_check(1, &different, &different).is_failure());
    }

    #[test]
    fn refusal_requires_status_marker_and_grpc_status() {
        let marker = gang_types::handover::REQUEST_NOT_STARTED_HEADER;
        let http = Answer::empty(503).with_header(marker, "true");
        assert!(refusal_check(1, "/v1/state", &http, false).is_passed());
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
