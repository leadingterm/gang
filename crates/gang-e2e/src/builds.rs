//! Two authenticated Bazel invocations, cold then warm, appear as distinct builds with their
//! principal, cache and execution usage, and ordered timestamps visible to the administrator.

use std::fs;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_config::Document;
use gang_main::plugins::{CacheConfig, Plugins};
use gang_wire::name;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::bazel::Bazel;
use crate::check::Check;
use crate::errors::{E2eError, Result};
use crate::flags::Remote;
use crate::issuer::TestIssuer;
use crate::names;
use crate::sample::{CACHE_INPUT_FILE, CACHE_INPUT_SIZE, CACHE_OUTPUT_PREFIX, cache_input};
use crate::secured::Secured;
use crate::server::{Answer, Presented, Request, ServerApi};
use crate::settings::DeploymentInputs;
use crate::system::{Probe, System, Variant};
use crate::verdict;

const COLD_ID: &str = "11111111-1111-4111-8111-111111111111";
const WARM_ID: &str = "22222222-2222-4222-8222-222222222222";
const PROBES: &[Probe] = &[Probe::CacheVisibility];
const MAX_BATCH_BYTES: usize = 65_536;
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
    let (cas_budget, ac_budget) = filesystem_budgets(stack)?;
    let before = store_baseline(stack, invocation.settings.history_timeout)?;
    let (input_digest, expected_digest) = stage_cache_input(stack)?;
    let output_size = CACHE_OUTPUT_PREFIX.len() + CACHE_INPUT_SIZE;
    let cold_driver = driver(COLD_ID);
    let (_sender, receiver) = tokio::sync::oneshot::channel();
    let cold = cold_driver.build_until(&remote, PROBES, None, receiver)?;
    cold_driver.clean(&remote)?;
    let warm_driver = driver(WARM_ID);
    let (_sender, receiver) = tokio::sync::oneshot::channel();
    let warm = warm_driver.build_until(&remote, PROBES, None, receiver)?;
    let report_path = stack
        .placement()
        .workspace()
        .join(Probe::CacheVisibility.report_path());
    let report = fs::read(&report_path)
        .map_err(E2eError::io(format!("reading {}", report_path.display())))?;
    let actual_digest: [u8; 32] = Sha256::digest(&report).into();
    let blob_ok = CACHE_INPUT_SIZE > MAX_BATCH_BYTES
        && expected_digest != input_digest
        && report.len() == output_size
        && actual_digest == expected_digest;
    let after = wait_for_growth(stack, invocation.settings.history_timeout, &before)?;
    let mut checks = vec![
        verdict::remote_execution(&cold, 1, stack.stderr_tail_lines()),
        verdict::cache_hit(&warm, 1, stack.stderr_tail_lines()),
    ];
    checks.push(Check::expect(names::builds::BLOB_THRESHOLD, blob_ok, || {
        format!("expected {output_size} bytes with digest {}; got {} bytes with digest {}; batch threshold {MAX_BATCH_BYTES}",
            hex::encode(expected_digest), report.len(), hex::encode(actual_digest))
    }));
    checks.extend(store_checks(&before, &after, cas_budget, ac_budget));
    let cold = observe_transfers(
        stack,
        &|request: &Request| ServerApi::send(stack, request),
        &credentials.token,
        COLD_ID,
        true,
        invocation.settings.history_timeout,
        output_size as u64,
    )?;
    let warm = observe_transfers(
        stack,
        &|request: &Request| ServerApi::send(stack, request),
        &credentials.token,
        WARM_ID,
        false,
        invocation.settings.history_timeout,
        output_size as u64,
    )?;
    checks.extend(build_checks(&cold, true, &expected_principal));
    checks.extend(build_checks(&warm, false, &expected_principal));
    checks.extend(transfer_checks(&cold, &warm, output_size as u64));
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

fn stage_cache_input(stack: &crate::stack::Stack<'_>) -> Result<([u8; 32], [u8; 32])> {
    let input = cache_input();
    let input_path = stack.placement().workspace().join(CACHE_INPUT_FILE);
    fs::write(&input_path, &input)
        .map_err(E2eError::io(format!("writing {}", input_path.display())))?;
    let input_digest = Sha256::digest(&input).into();
    let mut expected = Sha256::new();
    expected.update(CACHE_OUTPUT_PREFIX);
    expected.update(&input);
    Ok((input_digest, expected.finalize().into()))
}

fn store_baseline(stack: &crate::stack::Stack<'_>, timeout: Duration) -> Result<Value> {
    let state = stack.wait_live_state(timeout, |state| {
        serde_json::to_value(state)
            .ok()
            .is_some_and(|value| store(&value, "CAS").is_some() && store(&value, "AC").is_some())
    })?;
    serde_json::to_value(state).map_err(E2eError::system_from("encoding baseline state"))
}
fn filesystem_budgets(stack: &crate::stack::Stack<'_>) -> Result<(u64, u64)> {
    let config_path = stack.placement().config_file();
    let config = fs::read_to_string(&config_path)
        .map_err(E2eError::io(format!("reading {}", config_path.display())))?;
    let document = Document::<Plugins>::parse(&config).map_err(E2eError::parse("gang.toml"))?;
    let CacheConfig::Filesystem(cache) = document.cache else {
        return Err(E2eError::system(
            "build accounting requires a filesystem cache",
        ));
    };
    let capacity = gang_os::fs::space(cache.path.as_path())
        .map_err(E2eError::system_from("measuring the cache filesystem"))?
        .total;
    Ok((
        cache.content_store_percent.of_bytes(capacity).get(),
        cache.action_store_percent.of_bytes(capacity).get(),
    ))
}

fn wait_for_growth(
    stack: &crate::stack::Stack<'_>,
    timeout: Duration,
    before: &Value,
) -> Result<Value> {
    let mut after = None;
    let waited = stack.wait_live_state(timeout, |state| {
        let value = serde_json::to_value(state).ok();
        let grown = value
            .as_ref()
            .and_then(|value| store(value, "CAS"))
            .zip(store(before, "CAS"))
            .is_some_and(|(after, before)| after.0 > before.0);
        after = value;
        grown
    });
    match waited {
        Ok(_) | Err(E2eError::Timeout { .. }) if after.is_some() => {}
        Err(error) => return Err(error),
        Ok(_) => return Err(E2eError::system("no post-build state")),
    }
    after.ok_or_else(|| E2eError::system("no post-build state"))
}

/// The last list and detail responses, including failures for diagnostic checks.
pub(crate) struct Observed {
    pub(crate) list: Answer,
    pub(crate) detail: Option<Answer>,
}

fn observe_transfers(
    stack: &crate::stack::Stack<'_>,
    read: &dyn Fn(&Request) -> Result<Answer>,
    token: &str,
    id: &str,
    cold: bool,
    timeout: Duration,
    output_size: u64,
) -> Result<Observed> {
    observe_with_transfer(stack, read, token, id, cold, timeout, Some(output_size))
}

fn observe_with_transfer(
    stack: &crate::stack::Stack<'_>,
    read: &dyn Fn(&Request) -> Result<Answer>,
    token: &str,
    id: &str,
    cold: bool,
    timeout: Duration,
    output_size: Option<u64>,
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
        let traffic_ready = output_size.is_none_or(|size| transfer_ready(&observed, cold, size));
        latest = Some(observed);
        ready && stable && traffic_ready
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

fn cache_bytes(observed: &Observed, field: &str) -> Option<u64> {
    let detail = observed
        .detail
        .as_ref()
        .filter(|answer| answer.status == HTTP_OK)?;
    let json: Value = serde_json::from_slice(&detail.body).ok()?;
    json.pointer(&format!("/usage/cache/{field}"))?.as_u64()
}

fn transfer_ready(observed: &Observed, cold: bool, output_size: u64) -> bool {
    let downloaded = cache_bytes(observed, "downloadedBytes");
    if cold {
        cache_bytes(observed, "uploadedBytes").is_some_and(|value| value > 0)
            && downloaded.is_some_and(|value| value > 0)
    } else {
        downloaded.is_some_and(|value| value >= output_size)
    }
}

fn transfer_checks(cold: &Observed, warm: &Observed, output_size: u64) -> Vec<Check> {
    vec![
        Check::expect(
            names::builds::COLD_BYTES,
            transfer_ready(cold, true, output_size),
            || {
                format!(
                    "expected positive cold uploaded and downloaded bytes; detail: {:?}",
                    cold.detail.as_ref().map(Answer::text)
                )
            },
        ),
        Check::expect(
            names::builds::WARM_BYTES,
            transfer_ready(warm, false, output_size),
            || {
                format!(
                    "expected at least {output_size} warm downloaded bytes; detail: {:?}",
                    warm.detail.as_ref().map(Answer::text)
                )
            },
        ),
    ]
}

fn store(state: &Value, name: &str) -> Option<(u64, u64)> {
    let row = state
        .pointer("/cache/stores")?
        .as_array()?
        .iter()
        .find(|row| row.get("store").and_then(Value::as_str) == Some(name))?;
    Some((
        row.get("usedBytes")?.as_u64()?,
        row.get("budgetBytes")?.as_u64()?,
    ))
}

fn store_checks(before: &Value, after: &Value, cas_budget: u64, ac_budget: u64) -> Vec<Check> {
    let cas = store(after, "CAS");
    let ac = store(after, "AC");
    let baseline = store(before, "CAS");
    vec![
        Check::expect(
            names::builds::ENGINE_BUDGETS,
            cas.is_some_and(|row| row.1 == cas_budget) && ac.is_some_and(|row| row.1 == ac_budget),
            || {
                format!(
                    "expected CAS budget {cas_budget} and AC budget {ac_budget}; state: {after}"
                )
            },
        ),
        Check::expect(
            names::builds::CAS_GROWTH,
            cas.zip(baseline)
                .is_some_and(|(after, before)| after.0 > before.0),
            || format!("expected CAS used bytes to grow; baseline: {before}; post-build: {after}"),
        ),
    ]
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

    #[test]
    fn cache_input_is_deterministic_and_exceeds_batch_limit() {
        let input = cache_input();
        assert_eq!(input.len(), CACHE_INPUT_SIZE);
        assert!(input.len() > MAX_BATCH_BYTES);
        assert_ne!(&input[..32], &input[32..64]);
        assert_eq!(input, cache_input());
    }

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
    fn transfer_checks_reject_missing_cold_bytes_and_small_warm_cas() {
        let mut cold = detail();
        cold["usage"]["cache"]["uploadedBytes"] = json!(100);
        cold["usage"]["cache"]["downloadedBytes"] = json!(200);
        let mut warm = detail();
        warm["usage"]["cache"]["downloadedBytes"] = json!(4096);
        assert!(
            transfer_checks(&observed(&cold), &observed(&warm), 4096)
                .iter()
                .all(Check::is_passed)
        );
        cold["usage"]["cache"]["uploadedBytes"] = json!(0);
        warm["usage"]["cache"]["downloadedBytes"] = json!(4095);
        assert!(
            transfer_checks(&observed(&cold), &observed(&warm), 4096)
                .iter()
                .all(Check::is_failure)
        );
    }

    #[test]
    fn store_checks_reject_swapped_budgets_and_stale_cas() {
        let before = json!({"cache":{"stores":[{"store":"CAS","usedBytes":10,"budgetBytes":100},{"store":"AC","usedBytes":1,"budgetBytes":20}]}});
        let after = json!({"cache":{"stores":[{"store":"CAS","usedBytes":11,"budgetBytes":100},{"store":"AC","usedBytes":2,"budgetBytes":20}]}});
        assert!(
            store_checks(&before, &after, 100, 20)
                .iter()
                .all(Check::is_passed)
        );
        let stale = json!({"cache":{"stores":[{"store":"CAS","usedBytes":10,"budgetBytes":20},{"store":"AC","usedBytes":2,"budgetBytes":100}]}});
        assert!(
            store_checks(&before, &stale, 100, 20)
                .iter()
                .all(Check::is_failure)
        );
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
