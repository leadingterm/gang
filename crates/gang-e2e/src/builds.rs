//! Two authenticated Bazel invocations, cold then warm, appear as distinct builds with their
//! principal, cache and execution usage, and ordered timestamps visible to the administrator.

use std::time::Duration;

use gang_clock::Timestamp;
use gang_wire::name;
use serde_json::Value;

use crate::bazel::Bazel;
use crate::check::Check;
use crate::errors::{E2eError, Result};
use crate::flags::Remote;
use crate::issuer::TestIssuer;
use crate::names;
use crate::secured::Secured;
use crate::server::{Answer, Presented, Request, ServerApi};
use crate::settings::DeploymentInputs;
use crate::system::{Probe, System, Variant};
use crate::verdict;

const COLD_ID: &str = "11111111-1111-4111-8111-111111111111";
const WARM_ID: &str = "22222222-2222-4222-8222-222222222222";
const PROBES: &[Probe] = &[Probe::Pinned];
const HTTP_OK: u16 = 200;

/// Runs the cold and warm builds and checks their administrator-visible accounting.
pub(crate) fn builds(
    invocation: &DeploymentInputs,
    variant: &Variant,
    issuer: &TestIssuer,
) -> Result<Vec<Check>> {
    let secured = Secured::create(invocation, variant, issuer)?;
    let stack = secured.stack();
    let credentials = stack.administrator_credentials()?;
    let expected_principal = name::of::<name::Principals>(&credentials.identity()?);
    let endpoint = stack.endpoint()?;
    let host = endpoint
        .host_str()
        .ok_or_else(|| E2eError::system(format!("{endpoint} has no host")))?;
    let remote = Remote::Gang {
        host: host.to_owned(),
        bazelrc: secured.render_bazelrc(&credentials.home)?,
        home: credentials.home.clone(),
        helper: stack.binary().to_path_buf(),
    };
    let driver = |id: &str| {
        let mut settings = invocation.settings.clone();
        settings.bazel_flags.push(format!("--invocation_id={id}"));
        Bazel::new(
            invocation.bazel.clone(),
            settings,
            invocation.action_tools.clone(),
            stack.placement().workspace(),
            stack.placement().bazel_root(),
        )
    };
    let cold_driver = driver(COLD_ID);
    let (_sender, receiver) = tokio::sync::oneshot::channel();
    let cold = cold_driver.build_until(&remote, PROBES, None, receiver)?;
    cold_driver.clean(&remote)?;
    let warm_driver = driver(WARM_ID);
    let (_sender, receiver) = tokio::sync::oneshot::channel();
    let warm = warm_driver.build_until(&remote, PROBES, None, receiver)?;
    let mut checks = vec![
        verdict::remote_execution(&cold, 1, stack.stderr_tail_lines()),
        verdict::cache_hit(&warm, 1, stack.stderr_tail_lines()),
    ];
    let cold = observe(
        stack,
        &|request: &Request| ServerApi::send(stack, request),
        &credentials.token,
        COLD_ID,
        true,
        invocation.settings.history_timeout,
    )?;
    let warm = observe(
        stack,
        &|request: &Request| ServerApi::send(stack, request),
        &credentials.token,
        WARM_ID,
        false,
        invocation.settings.history_timeout,
    )?;
    checks.extend(build_checks(&cold, true, &expected_principal));
    checks.extend(build_checks(&warm, false, &expected_principal));
    checks.push(Check::expect(
        names::builds::DISTINCT,
        listed_name(&cold.list)
            .zip(listed_name(&warm.list))
            .is_some_and(|(a, b)| a != b),
        || {
            format!(
                "expected distinct build names; cold list: {}; warm list: {}",
                cold.list.text(),
                warm.list.text()
            )
        },
    ));
    Ok(checks)
}

/// The last list and detail responses, including failures for diagnostic checks.
pub(crate) struct Observed {
    pub(crate) list: Answer,
    pub(crate) detail: Option<Answer>,
}

/// Observes build accounting on state-stream events until usage stabilizes or the deadline expires.
pub(crate) fn observe(
    stack: &crate::stack::Stack<'_>,
    read: &dyn Fn(&Request) -> Result<Answer>,
    token: &str,
    id: &str,
    cold: bool,
    timeout: Duration,
) -> Result<Observed> {
    let mut previous_usage = None;
    let mut latest = None;
    let mut failure = None;
    let waited = stack.wait_live_state(timeout, |_| {
        let result = (|| -> Result<Observed> {
            let list = read(
                &Request::get(format!("/v1/builds?filter=invocationId%3D%22{id}%22"))
                    .presenting(Presented::Bearer(token.to_owned())),
            )?;
            let name = listed_name(&list);
            let detail = match name.as_deref() {
                Some(name) => Some(read(
                    &Request::get(format!("/v1/{name}"))
                        .presenting(Presented::Bearer(token.to_owned())),
                )?),
                None => None,
            };
            Ok(Observed { list, detail })
        })();
        let observed = match result {
            Ok(observed) => observed,
            Err(error) => {
                failure = Some(error);
                return true;
            }
        };
        let checks = build_checks(&observed, cold, "");
        let ready = if cold {
            checks.get(1).is_some_and(Check::is_passed)
                && checks.get(2).is_some_and(Check::is_passed)
        } else {
            checks.get(1).is_some_and(Check::is_passed)
        };
        let stable = stable_usage(&mut previous_usage, &observed);
        latest = Some(observed);
        ready && stable
    });
    if let Some(error) = failure {
        return Err(error);
    }
    match waited {
        Ok(_) | Err(E2eError::Timeout { .. }) if latest.is_some() => {
            latest.ok_or_else(|| E2eError::system("no build observation"))
        }
        Err(error) => Err(error),
        Ok(_) => Err(E2eError::system("no build observation")),
    }
}

/// Whether this successful detail has the same usage as the previous successful detail.
fn stable_usage(previous: &mut Option<Value>, observed: &Observed) -> bool {
    let usage = observed
        .detail
        .as_ref()
        .filter(|answer| answer.status == HTTP_OK)
        .and_then(|answer| serde_json::from_slice::<Value>(&answer.body).ok())
        .and_then(|value| {
            value
                .get("usage")
                .filter(|usage| usage.is_object())
                .cloned()
        });
    let stable = usage.is_some() && *previous == usage;
    *previous = usage;
    stable
}

/// Extracts the unique listed build's name only from a successful response.
pub(crate) fn listed_name(answer: &Answer) -> Option<String> {
    if answer.status != HTTP_OK {
        return None;
    }
    let json: Value = serde_json::from_slice(&answer.body).ok()?;
    let builds = json.get("builds")?.as_array()?;
    if builds.len() != 1
        || json
            .get("nextPageToken")
            .and_then(Value::as_str)
            .is_some_and(|token| !token.is_empty())
    {
        return None;
    }
    builds
        .first()?
        .get("name")?
        .as_str()
        .filter(|name| name.starts_with("builds/"))
        .map(str::to_owned)
}

/// Checks one list response and its detail without treating malformed JSON as success.
pub(crate) fn build_checks(observed: &Observed, cold: bool, principal: &str) -> Vec<Check> {
    let (list_name, count_name, second_name, times_name, principal_name) = if cold {
        (
            names::builds::COLD_LIST,
            names::builds::COLD_MISSES,
            names::builds::COLD_COMPLETED,
            names::builds::COLD_TIMES,
            names::builds::COLD_PRINCIPAL,
        )
    } else {
        (
            names::builds::WARM_LIST,
            names::builds::WARM_HITS,
            names::builds::WARM_MISSES,
            names::builds::WARM_TIMES,
            names::builds::WARM_PRINCIPAL,
        )
    };
    let list_ok = listed_name(&observed.list).is_some();
    let detail = observed.detail.as_ref();
    let json = detail
        .filter(|answer| answer.status == HTTP_OK)
        .and_then(|answer| serde_json::from_slice::<Value>(&answer.body).ok());
    let number = |pointer: &str| {
        json.as_ref()
            .and_then(|value| value.pointer(pointer))
            .and_then(Value::as_u64)
    };
    let count_ok = if cold {
        number("/usage/cache/misses").is_some_and(|n| n >= 1)
    } else {
        number("/usage/cache/hits").is_some_and(|n| n >= 1)
    };
    let second_ok = if cold {
        number("/usage/executions/completed").is_some_and(|n| n >= 1)
    } else {
        number("/usage/cache/misses") == Some(0)
    };
    let time = |field: &str| {
        json.as_ref()
            .and_then(|value| value.get(field))
            .and_then(Value::as_str)
            .and_then(|text| Timestamp::from_rfc3339(text).ok())
    };
    let times_ok = time("startTime")
        .zip(time("lastActiveTime"))
        .is_some_and(|(start, end)| start <= end);
    let principal_ok = json
        .as_ref()
        .and_then(|value| value.get("principal"))
        .and_then(Value::as_str)
        == Some(principal);
    let detail_text = detail.map_or_else(
        || format!("no detail; list: {}", observed.list.text()),
        |answer| format!("status {}, JSON: {}", answer.status, answer.text()),
    );
    vec![
        Check::expect(list_name, list_ok, || {
            format!(
                "expected one listed build; status {}, JSON: {}",
                observed.list.status,
                observed.list.text()
            )
        }),
        Check::expect(count_name, count_ok, || {
            format!("unexpected cache usage; {detail_text}")
        }),
        Check::expect(second_name, second_ok, || {
            format!("unexpected usage; {detail_text}")
        }),
        Check::expect(times_name, times_ok, || {
            format!("expected startTime <= lastActiveTime; {detail_text}")
        }),
        Check::expect(principal_name, principal_ok, || {
            format!("expected principal {principal:?}; {detail_text}")
        }),
    ]
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn observed(detail: &Value) -> Observed {
        Observed {
            list: Answer::json(HTTP_OK, &json!({"builds": [{"name": "builds/abc"}]}))
                .expect("list serializes"),
            detail: Some(Answer::json(HTTP_OK, detail).expect("detail serializes")),
        }
    }

    fn detail() -> Value {
        json!({
            "principal": "principals/test:client",
            "startTime": "2026-01-01T00:00:00.000Z",
            "lastActiveTime": "2026-01-01T00:00:01.000Z",
            "usage": {
                "cache": {"hits": 1, "misses": 1},
                "executions": {"completed": 1}
            }
        })
    }

    #[test]
    fn cold_checks_require_miss_completion_order_and_principal() {
        let good = observed(&detail());
        assert!(
            build_checks(&good, true, "principals/test:client")
                .iter()
                .all(Check::is_passed)
        );
        let mut broken = detail();
        broken["usage"]["cache"]["misses"] = json!(0);
        broken["usage"]["executions"]["completed"] = json!(0);
        broken["lastActiveTime"] = json!("2025-01-01T00:00:00.000Z");
        broken["principal"] = json!("principals/test:other");
        let checks = build_checks(&observed(&broken), true, "principals/test:client");
        assert!(checks[0].is_passed());
        assert!(checks[1..].iter().all(Check::is_failure));
    }

    #[test]
    fn warm_checks_require_hit_and_no_miss() {
        let mut value = detail();
        value["usage"]["cache"]["misses"] = json!(0);
        assert!(
            build_checks(&observed(&value), false, "principals/test:client")
                .iter()
                .all(Check::is_passed)
        );
        value["usage"]["cache"]["hits"] = json!(0);
        value["usage"]["cache"]["misses"] = json!(1);
        let checks = build_checks(&observed(&value), false, "principals/test:client");
        assert!(checks[1].is_failure());
        assert!(checks[2].is_failure());
    }

    #[test]
    fn usage_must_remain_unchanged_after_a_trailing_miss() {
        let mut previous = None;
        let mut value = detail();
        value["usage"]["cache"]["misses"] = json!(0);
        assert!(!stable_usage(&mut previous, &observed(&value)));
        value["usage"]["cache"]["misses"] = json!(1);
        assert!(!stable_usage(&mut previous, &observed(&value)));
        assert!(stable_usage(&mut previous, &observed(&value)));
        assert!(!stable_usage(&mut previous, &observed(&json!({}))));
        assert!(!stable_usage(&mut previous, &observed(&json!({}))));
    }

    #[test]
    fn list_requires_one_build_and_no_next_page() {
        for value in [
            json!({"builds": []}),
            json!({"builds": [{"name": "builds/a"}, {"name": "builds/b"}]}),
            json!({"builds": [{"name": "builds/a"}], "nextPageToken": "more"}),
        ] {
            let answer = Answer::json(HTTP_OK, &value).expect("list serializes");
            assert!(listed_name(&answer).is_none());
        }
    }
}
