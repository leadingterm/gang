//! Builds through follower-only public frontends and reads their accounting through each controller.

use std::sync::Arc;

use gang_clock::{SystemClock, Timestamp};
use gang_tls::{HttpProtocol, HttpTransport, ServerTrust};
use gang_wire::name;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Digest, GetActionResultRequest, RequestMetadata, digest_function,
};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::builds::{build_checks, listed_name, observe};
use crate::check::{Check, prefix_checks};
use crate::cluster::Cluster;
use crate::errors::{E2eError, Result};
use crate::h2_frames::H2Channel;
use crate::http::Http;
use crate::identity::Admission;
use crate::names::follower_builds::{
    CONTROLLED_FOLLOWER, CONTROLLED_LEADER, CONTROLLED_MISS, FOLLOWER_ONLY_SERVE,
    KILLED_BUILD_ABSENT, SURVIVOR_ONLY_SERVE, WARM_ZERO_USAGE,
};
use crate::server::{Presented, Request, ServerApi};
use crate::system::Probe;
use crate::verdict;

const CONTROLLERS: usize = 3;
const PROBES: &[Probe] = &[Probe::Pinned];
const A: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const B: &str = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const C: &str = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
const CONTROLLED: &str = "dddddddd-dddd-4ddd-8ddd-dddddddddddd";
const FIXTURE: &[u8] = b"gang-e2e follower observation controlled miss";
const ZERO_USAGE: &[&str] = &[
    "/usage/cache/misses",
    "/usage/cache/errors",
    "/usage/executions/executeCalls",
    "/usage/executions/completed",
    "/usage/executions/rejected",
    "/unrecorded",
];

fn zeros(value: &Value, pointers: &[&str]) -> bool {
    pointers
        .iter()
        .all(|pointer| value.pointer(pointer).and_then(Value::as_u64) == Some(0))
}

fn controlled_valid(value: &Value, principal: &str) -> bool {
    let time = |field: &str| {
        value
            .get(field)
            .and_then(Value::as_str)
            .and_then(|text| Timestamp::from_rfc3339(text).ok())
    };
    value.get("principal").and_then(Value::as_str) == Some(principal)
        && value.get("invocationId").and_then(Value::as_str) == Some(CONTROLLED)
        && time("startTime")
            .zip(time("lastActiveTime"))
            .is_some_and(|(start, end)| start <= end)
        && value.pointer("/usage/cache/misses").and_then(Value::as_u64) == Some(1)
        && zeros(
            value,
            &[
                "/usage/cache/hits",
                "/usage/cache/errors",
                "/usage/cache/uploadedResults",
                "/usage/executions/executeCalls",
                "/usage/executions/completed",
                "/usage/executions/rejected",
                "/unrecorded",
            ],
        )
}

fn controlled_read(
    read: &dyn Fn(&Request) -> Result<crate::server::Answer>,
    token: &str,
) -> Result<Option<(String, Value)>> {
    let list = read(
        &Request::get(format!(
            "/v1/builds?filter=invocationId%3D%22{CONTROLLED}%22"
        ))
        .presenting(Presented::Bearer(token.to_owned())),
    )?;
    if list.status != 200 {
        return Err(E2eError::system(format!(
            "controlled build list status {}: {}",
            list.status,
            list.text()
        )));
    }
    let _: Value = serde_json::from_slice(&list.body)
        .map_err(E2eError::system_from("parsing controlled build list"))?;
    let Some(name) = listed_name(&list) else {
        return Ok(None);
    };
    let detail =
        read(&Request::get(format!("/v1/{name}")).presenting(Presented::Bearer(token.to_owned())))?;
    if detail.status != 200 {
        return Err(E2eError::system(format!(
            "controlled build detail status {}: {}",
            detail.status,
            detail.text()
        )));
    }
    let value = serde_json::from_slice(&detail.body)
        .map_err(E2eError::system_from("parsing controlled build detail"))?;
    Ok(Some((name, value)))
}

fn controlled_observe(
    stack: &crate::stack::Stack<'_>,
    read: &dyn Fn(&Request) -> Result<crate::server::Answer>,
    token: &str,
) -> Result<(String, Value)> {
    let mut previous = None;
    let mut latest = None;
    let mut failure = None;
    let waited = stack.wait_live_state(
        stack.settings().history_timeout,
        |_| match controlled_read(read, token) {
            Ok(Some((name, value))) => {
                let usage = value
                    .get("usage")
                    .filter(|usage| usage.is_object())
                    .cloned();
                let ready = value.pointer("/usage/cache/misses").and_then(Value::as_u64) == Some(1);
                let stable = usage.is_some() && previous == usage;
                previous = usage;
                latest = Some((name, value));
                ready && stable
            }
            Ok(None) => {
                previous = None;
                false
            }
            Err(error) => {
                failure = Some(error);
                true
            }
        },
    );
    if let Some(error) = failure {
        return Err(error);
    }
    waited?;
    latest.ok_or_else(|| E2eError::system("no controlled build observation"))
}

fn controlled_checks(
    cluster: &Cluster<'_>,
    client: &Http,
    token: &str,
    principal: &str,
    pin: &gang_types::CaPin,
    controllers: (usize, usize),
) -> Vec<Check> {
    let stack = cluster.stack();
    let (leader, follower) = controllers;
    let mut checks = Vec::with_capacity(3);
    let controlled_rpc = (|| -> Result<Option<u32>> {
        let config = gang_tls::client_config(
            ServerTrust::Pin(pin),
            None,
            &[b"h2"],
            Arc::new(SystemClock::new()),
        )
        .map_err(E2eError::system_from("configuring controlled request TLS"))?;
        let endpoint = cluster.direct_endpoint(follower);
        let mut channel = match H2Channel::connect(
            Arc::new(config),
            &endpoint,
            Some(token),
            stack.settings().handshake_timeout,
        )? {
            Admission::Accepted(channel) => channel,
            Admission::Refused => {
                return Err(E2eError::system("follower refused controlled connection"));
            }
        };
        let request = GetActionResultRequest {
            instance_name: String::new(),
            action_digest: Some(Digest {
                hash: hex::encode(Sha256::digest(FIXTURE)),
                size_bytes: i64::try_from(FIXTURE.len())
                    .map_err(E2eError::system_from("converting controlled fixture size"))?,
            }),
            digest_function: digest_function::Value::Sha256 as i32,
            ..Default::default()
        };
        let metadata = RequestMetadata {
            tool_invocation_id: CONTROLLED.to_owned(),
            ..Default::default()
        };
        channel.get_action_result(&request, &metadata, stack.settings().command_timeout)
    })();
    checks.push(Check::expect(
        CONTROLLED_MISS,
        matches!(controlled_rpc, Ok(Some(5))),
        || format!("expected gRPC status 5, got {controlled_rpc:?}"),
    ));
    let leader_endpoint = cluster.direct_endpoint(leader);
    let leader_read =
        |request: &Request| crate::login_across::exchange(client, &leader_endpoint, request);
    let controlled = controlled_observe(stack, &leader_read, token);
    checks.push(Check::expect(
        CONTROLLED_LEADER,
        controlled
            .as_ref()
            .is_ok_and(|(_, value)| controlled_valid(value, principal)),
        || format!("unexpected controlled leader observation: {controlled:?}"),
    ));
    let follower_endpoint = cluster.direct_endpoint(follower);
    let follower_read =
        |request: &Request| crate::login_across::exchange(client, &follower_endpoint, request);
    let relayed = controlled_read(&follower_read, token);
    checks.push(Check::expect(
        CONTROLLED_FOLLOWER,
        controlled
            .as_ref()
            .ok()
            .zip(relayed.as_ref().ok().and_then(Option::as_ref))
            .is_some_and(|((leader_name, _), (follower_name, value))| {
                leader_name == follower_name && controlled_valid(value, principal)
            }),
        || format!("unexpected controlled follower observation: {relayed:?}"),
    ));
    checks
}

fn observed_checks(
    stack: &crate::stack::Stack<'_>,
    read: &dyn Fn(&Request) -> Result<crate::server::Answer>,
    token: &str,
    id: &str,
    cold: bool,
    principal: &str,
    prefix: &str,
) -> Result<Vec<Check>> {
    let observed = observe(
        stack,
        read,
        token,
        id,
        cold,
        stack.settings().history_timeout,
    )?;
    let mut result = build_checks(&observed, cold, principal);
    if !cold {
        let detail = observed.detail.as_ref();
        let value = detail
            .filter(|answer| answer.status == 200)
            .and_then(|answer| serde_json::from_slice::<Value>(&answer.body).ok());
        let zero = value.as_ref().is_some_and(|value| zeros(value, ZERO_USAGE));
        result[2] = Check::expect(WARM_ZERO_USAGE, zero, || {
            detail.map_or_else(
                || "no build detail".to_owned(),
                |answer| format!("status {}, JSON: {}", answer.status, answer.text()),
            )
        });
    }
    Ok(prefix_checks(result, prefix))
}

fn absent_build(client: &Http, endpoint: &gang_types::Url, token: &str, id: &str) -> Result<Check> {
    let answer = crate::login_across::exchange(
        client,
        endpoint,
        &Request::get(format!("/v1/builds?filter=invocationId%3D%22{id}%22"))
            .presenting(Presented::Bearer(token.to_owned())),
    )?;
    let empty = serde_json::from_slice::<Value>(&answer.body)
        .ok()
        .and_then(|value| {
            value
                .get("builds")
                .and_then(Value::as_array)
                .map(Vec::is_empty)
        })
        .unwrap_or(false);
    Ok(Check::expect(
        KILLED_BUILD_ABSENT,
        answer.status == 200 && empty,
        || {
            format!(
                "new leader returned status {} and body {}",
                answer.status,
                answer.text()
            )
        },
    ))
}

pub fn follower_builds(cluster: &Cluster<'_>) -> Result<Vec<Check>> {
    let stack = cluster.stack();
    let credentials = ServerApi::administrator_credentials(stack)?;
    let principal = name::of::<name::Principals>(&credentials.identity()?);
    let remote = stack.administrator_remote(&credentials)?;
    let pin = stack.ca_pin()?;
    let client = Http::try_new(
        HttpTransport::Tls {
            trust: ServerTrust::Pin(&pin),
        },
        HttpProtocol::Http2,
        stack.settings().command_timeout,
    )?;
    let build = |id: &str| {
        let driver = stack.bazel().with_flag(format!("--invocation_id={id}"));
        let (_sender, receiver) = tokio::sync::oneshot::channel();
        driver.build_until(&remote, PROBES, None, receiver)
    };
    let read = |controller: usize, id: &str, cold: bool, prefix: &str| {
        let endpoint = cluster.direct_endpoint(controller);
        observed_checks(
            stack,
            &|request| crate::login_across::exchange(&client, &endpoint, request),
            &credentials.token,
            id,
            cold,
            &principal,
            prefix,
        )
    };

    let leader = cluster.wait_for_leader()?;
    cluster.wait_for_single_leader(leader)?;
    let followers: Vec<_> = (0..CONTROLLERS).filter(|&i| i != leader).collect();
    cluster.unroute_public(leader);
    cluster.wait_for_serving(&followers)?;
    let mut checks = vec![Check::passed(FOLLOWER_ONLY_SERVE)];

    checks.extend(controlled_checks(
        cluster,
        &client,
        &credentials.token,
        &principal,
        &pin,
        (leader, followers[0]),
    ));

    let cold = build(A)?;
    stack.bazel().clean(&remote)?;
    let warm = build(B)?;
    checks.extend(prefix_checks(
        vec![verdict::remote_execution(
            &cold,
            1,
            stack.settings().stderr_tail_lines,
        )],
        "cold build",
    ));
    checks.extend(prefix_checks(
        vec![verdict::cache_hit(
            &warm,
            1,
            stack.settings().stderr_tail_lines,
        )],
        "warm build",
    ));
    checks.extend(read(leader, A, true, "leader direct, cold")?);
    checks.extend(read(leader, B, false, "leader direct, warm")?);
    checks.extend(read(followers[0], A, true, "follower relay, cold")?);
    checks.extend(read(followers[0], B, false, "follower relay, warm")?);

    stack.bazel().clean(&remote)?;
    stack.kill_bazel()?;
    cluster.kill(leader)?;
    let new_leader = cluster.wait_for_leader_except(Some(leader))?;
    cluster.wait_for_single_leader(new_leader)?;
    let survivor = (0..CONTROLLERS)
        .find(|&i| i != leader && i != new_leader)
        .ok_or_else(|| E2eError::system("no surviving follower"))?;
    cluster.unroute_public(new_leader);
    cluster.wait_for_serving(&[survivor])?;
    checks.push(Check::passed(SURVIVOR_ONLY_SERVE));

    let warm = build(C)?;
    checks.extend(prefix_checks(
        vec![verdict::cache_hit(
            &warm,
            1,
            stack.settings().stderr_tail_lines,
        )],
        "after transition",
    ));
    checks.extend(read(new_leader, C, false, "new leader direct, warm")?);
    checks.extend(read(survivor, C, false, "survivor relay, warm")?);

    checks.push(absent_build(
        &client,
        &cluster.direct_endpoint(new_leader),
        &credentials.token,
        A,
    )?);
    Ok(checks)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn controlled_accounting_requires_identity_times_and_every_counter() {
        let value = json!({
            "principal": "principals/example",
            "invocationId": CONTROLLED,
            "startTime": "2026-01-14T09:00:00Z",
            "lastActiveTime": "2026-01-14T09:00:01Z",
            "usage": {
                "cache": {"misses": 1, "hits": 0, "errors": 0, "uploadedResults": 0},
                "executions": {"executeCalls": 0, "completed": 0, "rejected": 0}
            },
            "unrecorded": 0
        });
        assert!(controlled_valid(&value, "principals/example"));
        assert!(!controlled_valid(&value, "principals/other"));
        for pointer in [
            "/usage/cache/misses",
            "/usage/cache/hits",
            "/usage/cache/errors",
            "/usage/cache/uploadedResults",
            "/usage/executions/executeCalls",
            "/usage/executions/completed",
            "/usage/executions/rejected",
            "/unrecorded",
        ] {
            let mut missing = value.clone();
            *missing
                .pointer_mut(pointer)
                .expect("fixture contains counter") = Value::Null;
            assert!(
                !controlled_valid(&missing, "principals/example"),
                "{pointer}"
            );
        }
        let mut reversed = value;
        reversed["lastActiveTime"] = json!("2026-01-14T08:59:59Z");
        assert!(!controlled_valid(&reversed, "principals/example"));
    }

    #[test]
    fn warm_zero_usage_requires_present_zero_counters() {
        let value = json!({
            "usage": {
                "cache": {"misses": 0, "errors": 0},
                "executions": {"executeCalls": 0, "completed": 0, "rejected": 0}
            },
            "unrecorded": 0
        });
        assert!(zeros(&value, ZERO_USAGE));
        for pointer in ZERO_USAGE {
            let mut changed = value.clone();
            *changed
                .pointer_mut(pointer)
                .expect("fixture contains counter") = json!(1);
            assert!(!zeros(&changed, ZERO_USAGE), "{pointer}");
            *changed
                .pointer_mut(pointer)
                .expect("fixture contains counter") = Value::Null;
            assert!(!zeros(&changed, ZERO_USAGE), "{pointer}");
        }
    }
}
