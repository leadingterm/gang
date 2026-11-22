//! A published v0.3.8 reader restores its own history after candidate writes.
//!
//! The oracle is the reservation admitted by a fresh old controller, not a local
//! history decoder. Raw DynamoDB items independently prove rollback byte identity.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use gang_tls::{HttpProtocol, HttpRequest, HttpTransport, RequestBody};
use gang_types::Url;
use gang_wire::{Reservation, RunningAction, State};
use http::{HeaderName, HeaderValue, Method};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::bazel::Bazel;
use crate::check::Check;
use crate::cluster::Cluster;
use crate::errors::{E2eError, Result};
use crate::execution::Run;
use crate::held_build::{Hold, joined};
use crate::http::Http;
use crate::issuer::TestIssuer;
use crate::process;
use crate::settings::DeploymentInputs;
use crate::system::{Probe, System, Variant};
use crate::upgrade_fixture::ReleaseFixture;

const MNEMONIC: &str = "E2eHistoryMemory";
const RESOURCE_PARTITION: &str = "rec#resource-history";
const CHUNK_PARTITION: &str = "rec#history-chunk";
const HEADER_BYTES: usize = 56;
const TRAINING_MEMORY: u64 = 512 * 1024 * 1024;
type Records = BTreeMap<(String, String), Value>;

pub(crate) fn history_rollback(
    inputs: &DeploymentInputs,
    variant: &Variant,
    issuer: &TestIssuer,
) -> Result<Vec<Check>> {
    let binary = artifact("GANG_E2E_HISTORY_GANG")?;
    let fixture = ReleaseFixture::read(&artifact("GANG_E2E_HISTORY_FIXTURE")?)?.seeded()?;
    let candidate_path = &inputs
        .upgrade
        .as_ref()
        .ok_or_else(|| E2eError::system("history rollback candidate fixture missing"))?
        .candidate_fixture;
    let candidate = ReleaseFixture::read(candidate_path)?.seeded()?;
    require_release(&binary, inputs.settings.command_timeout)?;
    let cluster = Cluster::start_release(inputs, variant, issuer, &binary, &fixture)?;
    let result = (|| {
        let store = Store::new(&cluster)?;
        let cold = held_reservation(&cluster, Probe::HistoryCold, "E2eHistoryCold")?;
        let measured = train(&cluster, inputs)?;
        let learned = held_reservation(&cluster, Probe::HistoryMemory, MNEMONIC)?;
        let old_key = resource_id(b"gang-learned-history-key-v1\0", &learned.action.target);
        let new_key = resource_id(b"gang-resource-history-key-v2\0", &learned.action.target);
        require(
            learned.reservation.memory != cold.reservation.memory
                && learned.reservation.memory >= measured
                && learned.reservation.memory < TRAINING_MEMORY
                && learned.action.fleet == cold.action.fleet
                && learned.capacity == cold.capacity,
            format!(
                "history must change the cold memory reservation without a floor or fit clamp: cold={cold:?}, learned={learned:?}, measured resident bytes={measured}"
            ),
        )?;
        store.wait("v0.3.8 resource history flush", |records| {
            has_resource(records, &old_key, 1)
        })?;

        // No original controller or supervisor can compete, and no old worker can
        // contribute a sample during the candidate's two actual flushes.
        stop(&cluster, 0)?;
        let original = resource_baseline(store.snapshot()?)?;
        require(
            has_resource(&original, &old_key, 1) && !original.contains_key(&resource_key(&new_key)),
            "old binary must have persisted its authentic GH01/v1 head before rollout".to_owned(),
        )?;
        let candidate_index = start(&cluster, &inputs.gang, &candidate)?;
        train(&cluster, inputs)?;
        let first = store.wait("candidate v2 resource history flush", |records| {
            has_resource(records, &new_key, 2)
        })?;
        unchanged(&original, &first)?;
        let first_head = first.get(&resource_key(&new_key)).cloned();
        train(&cluster, inputs)?;
        let second = store.wait("second candidate v2 resource history flush", |records| {
            has_resource(records, &new_key, 2)
                && records.get(&resource_key(&new_key)) != first_head.as_ref()
        })?;
        unchanged(&original, &second)?;
        stop(&cluster, candidate_index)?;
        unchanged(&original, &store.snapshot()?)?;

        let restored_index = start(&cluster, &binary, &fixture)?;
        cluster.wait_for_single_leader(restored_index)?;
        // The original-target action must not complete between restoration and
        // the oracle. The old loader itself must retain the legacy originals.
        let restored_records = store.snapshot()?;
        unchanged(&original, &restored_records)?;
        let restored = held_reservation(&cluster, Probe::HistoryMemory, MNEMONIC)?;
        require(
            restored.reservation == learned.reservation
                && restored.reservation.memory != cold.reservation.memory
                && restored.action.target == learned.action.target
                && restored.action.mnemonic == learned.action.mnemonic
                && restored.action.digest != learned.action.digest
                && restored.action.fleet == learned.action.fleet
                && restored.capacity == learned.capacity,
            format!(
                "fresh published reader must admit the same learned resources for an uncached action: cold={cold:?}, before={learned:?}, after={restored:?}"
            ),
        )?;
        Ok(vec![
            Check::passed(format!(
                "published v0.3.8 GH01 keys, bodies and expiry unchanged across two candidate flushes ({} original rows)",
                original.len()
            )),
            Check::passed(format!(
                "fresh published v0.3.8 restores actual learned reservation {:?}, distinct from cold {:?}",
                restored.reservation, cold.reservation
            )),
        ])
    })();
    cluster.diagnose(result)
}

fn artifact(variable: &str) -> Result<PathBuf> {
    let path = std::env::var_os(variable)
        .map(PathBuf::from)
        .ok_or_else(|| E2eError::system(format!("missing pinned rollback artifact {variable}")))?;
    std::fs::canonicalize(&path)
        .map_err(E2eError::io(format!("resolving {variable}: {}", path.display())))
}

fn require_release(binary: &Path, timeout: Duration) -> Result<()> {
    let mut command = std::process::Command::new(binary);
    command.arg("--version");
    let result = process::run(command, None, timeout)?;
    require(
        result.succeeded() && result.stdout.split_whitespace().any(|part| part == "0.3.8"),
        format!("rollback requires the pinned published v0.3.8 binary: {}", result.combined()),
    )
}

fn require(ok: bool, detail: String) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(E2eError::system(detail))
    }
}

fn start(cluster: &Cluster<'_>, binary: &Path, fixture: &ReleaseFixture) -> Result<usize> {
    cluster.apply_release(fixture)?;
    let index = cluster.start_controller_release(binary, fixture)?;
    cluster.wait_for_controller_serve(index)?;
    cluster.wait_for_single_leader(index)?;
    cluster.wait_for_controller_health(index)?;
    cluster.wait_for_serve()?;
    Ok(index)
}

fn stop(cluster: &Cluster<'_>, index: usize) -> Result<()> {
    cluster.deregister_controller(index)?;
    cluster.kill(index)?;
    process::terminate_workers(&cluster.release_config_path(index)?)
}

#[derive(Debug)]
struct Admitted {
    action: RunningAction,
    reservation: Reservation,
    capacity: Reservation,
}

fn wait_state<T>(
    cluster: &Cluster<'_>,
    condition: &str,
    mut observe: impl FnMut(&State) -> Result<Option<T>>,
) -> Result<T> {
    let settings = cluster.stack().settings();
    let timeout = settings.history_timeout;
    let began = Instant::now();
    loop {
        let state = cluster.live_state()?;
        if let Some(result) = observe(&state)? {
            return Ok(result);
        }
        let remaining = timeout.saturating_sub(began.elapsed());
        if remaining.is_zero() {
            return Err(E2eError::timeout(format!("{condition}; last state={state:?}"), timeout));
        }
        std::thread::sleep(settings.upgrade_probe_interval.min(remaining));
    }
}

fn held_reservation(cluster: &Cluster<'_>, probe: Probe, mnemonic: &str) -> Result<Admitted> {
    let stack = cluster.stack();
    let credentials = stack.administrator()?.credentials(&stack.ca_pin()?)?;
    let remote = stack.administrator_remote(&credentials)?;
    let run = Run::fresh()?;
    let go = stack.tmp_path().join("history-unused-release");
    let observed = std::thread::scope(|scope| {
        let hold = Hold::new(&go, |_| stack.kill_bazel());
        let build = hold.build(scope, None, |_, stopped| {
            stack.bazel().build_until(&remote, &[probe], Some(&run), stopped)
        });
        let observed = wait_state(cluster, "floor-free held action reservation", |state| {
            let Some(action) = state.actions.as_ref().and_then(|actions| {
                actions.running.iter().find(|action| action.mnemonic == mnemonic)
            }) else {
                return Ok(None);
            };
            let Some(reservation) = action.reservation else {
                return Ok(None);
            };
            let Some(capacity) = state
                .workers
                .iter()
                .find(|worker| worker.name == action.worker)
                .and_then(|worker| worker.capacity)
            else {
                return Ok(None);
            };
            require(
                action.learned_reservation.is_none() && reservation.memory < capacity.memory,
                format!("held reservation is fitted or capacity-clamped: {action:?}, capacity={capacity:?}"),
            )?;
            Ok(Some(Admitted {
                action: action.clone(),
                reservation,
                capacity,
            }))
        });
        // Cancellation, never release: observing the reservation must not finish
        // this action and manufacture the history that the oracle is testing.
        hold.cancel("history reservation observed; do not complete or retrain");
        let outcome = joined(build);
        if outcome.as_ref().is_ok_and(crate::build_output::BuildOutcome::succeeded) {
            return Err(E2eError::system("history oracle unexpectedly completed"));
        }
        hold.disarm();
        observed
    })?;
    wait_state(
        cluster,
        "cancelled history probe to leave the scheduler",
        |state| {
            Ok(state
                .actions
                .as_ref()
                .is_none_or(|actions| {
                    actions.running.is_empty() && actions.queued.is_empty()
                })
                .then_some(()))
        },
    )?;
    Ok(observed)
}

fn train(cluster: &Cluster<'_>, inputs: &DeploymentInputs) -> Result<u64> {
    let stack = cluster.stack();
    let mut settings = stack.settings().clone();
    settings.bazel_flags.push("--define=e2e_history_train=1".to_owned());
    let driver = Bazel::new(
        inputs.bazel.clone(),
        settings,
        inputs.action_tools.clone(),
        stack.placement().workspace(),
        stack.placement().bazel_root(),
    );
    let credentials = stack.administrator()?.credentials(&stack.ca_pin()?)?;
    let remote = stack.administrator_remote(&credentials)?;
    let run = Run::fresh()?;
    let go = stack.tmp_path().join("history-unused-release");
    std::thread::scope(|scope| {
        let hold = Hold::new(&go, |_| driver.kill_servers());
        let build = hold.build(scope, None, |_, stopped| {
            driver.build_until(&remote, &[Probe::HistoryMemory], Some(&run), stopped)
        });
        let outcome = joined(build)?;
        require(
            outcome.succeeded() && outcome.processes.remote() == 1,
            format!("history training must execute once remotely: {outcome:?}"),
        )?;
        hold.disarm();
        let report = stack.report(Probe::HistoryMemory)?;
        let resident = report
            .get("resident_bytes")?
            .parse::<u64>()
            .map_err(E2eError::system_from("parsing native training RSS"))?;
        require(
            (192 * 1024 * 1024..TRAINING_MEMORY).contains(&resident),
            format!("training did not sustain its touched resident allocation: {report:?}"),
        )?;
        Ok(resident)
    })
}

fn resource_id(namespace: &[u8], target: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(namespace);
    hash.update([0]);
    for part in [target, MNEMONIC] {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part.as_bytes());
    }
    hex::encode(hash.finalize())
}

fn resource_key(id: &str) -> (String, String) {
    (RESOURCE_PARTITION.to_owned(), id.to_owned())
}

fn has_resource(records: &Records, id: &str, version: u8) -> bool {
    records
        .get(&resource_key(id))
        .and_then(|item| item.get("body")?.get("B")?.as_str())
        .and_then(|body| STANDARD.decode(body).ok())
        .is_some_and(|body| body.starts_with(b"GH01") && body.get(HEADER_BYTES) == Some(&version))
}

// Preserve only resource heads and the chunks those heads actually reference.
// Compatible source/duration profiles and unrelated orphan chunks may change.
// Reading the outer storage header locates bytes; it never restores a hint.
fn resource_baseline(mut records: Records) -> Result<Records> {
    let heads: Vec<_> = records
        .keys()
        .filter(|(partition, _)| partition == RESOURCE_PARTITION)
        .cloned()
        .collect();
    let mut baseline = Records::new();
    for key in heads {
        let head = records
            .remove(&key)
            .ok_or_else(|| E2eError::system("resource history head disappeared from snapshot"))?;
        let body = head
            .get("body")
            .and_then(|body| body.get("B"))
            .and_then(Value::as_str)
            .ok_or_else(|| E2eError::system("resource history head lacks stored body"))?;
        let body = STANDARD
            .decode(body)
            .map_err(E2eError::system_from("reading stored GH01 resource head"))?;
        let header = body
            .get(..HEADER_BYTES)
            .filter(|header| header.starts_with(b"GH01"))
            .ok_or_else(|| E2eError::system("resource history head lacks GH01 header"))?;
        let count = u32::from_be_bytes(
            header[52..56]
                .try_into()
                .map_err(E2eError::system_from("reading resource chunk count"))?,
        );
        require(
            usize::try_from(count).is_ok_and(|count| count <= records.len()),
            "resource history references more chunks than the stored snapshot".to_owned(),
        )?;
        let id = hex::decode(&key.1)
            .map_err(E2eError::system_from("reading resource head key"))?;
        require(id.len() == 32, "resource head key is not a SHA256 ID".to_owned())?;
        for index in 0..count {
            // This is the production GH01 chunk-addressing contract, not the
            // Native resource snapshot codec (the published binary reads that).
            let mut hash = Sha256::new();
            hash.update(b"gang-learned-history-chunk-v1\0");
            hash.update(b"resource-history");
            hash.update(&id);
            hash.update(&header[4..12]);
            hash.update(&header[20..52]);
            hash.update(index.to_be_bytes());
            let chunk_key = (CHUNK_PARTITION.to_owned(), hex::encode(hash.finalize()));
            let chunk = records.remove(&chunk_key).ok_or_else(|| {
                E2eError::system(format!("resource head references missing chunk {chunk_key:?}"))
            })?;
            baseline.insert(chunk_key, chunk);
        }
        baseline.insert(key, head);
    }
    Ok(baseline)
}

fn unchanged(original: &Records, current: &Records) -> Result<()> {
    for (key, item) in original {
        require(
            current.get(key) == Some(item),
            format!("candidate changed or deleted authentic rollback row {key:?}: before={item}, after={:?}", current.get(key)),
        )?;
    }
    Ok(())
}

struct Store {
    client: Http,
    endpoint: Url,
    timeout: Duration,
    interval: Duration,
}

impl Store {
    fn new(cluster: &Cluster<'_>) -> Result<Self> {
        let settings = cluster.stack().settings();
        Ok(Self {
            client: Http::try_new(
                HttpTransport::Plain,
                HttpProtocol::Negotiated,
                settings.upgrade_probe_timeout,
            )?,
            endpoint: cluster.history_database_endpoint()?,
            timeout: settings.history_timeout,
            interval: settings.upgrade_probe_interval,
        })
    }

    // Observe exact stored attributes, including version, expiry and chunk bytes.
    // No payload is decoded into a scheduler hint by this observer.
    fn snapshot(&self) -> Result<Records> {
        let mut records = Records::new();
        let began = Instant::now();
        for partition in [RESOURCE_PARTITION, CHUNK_PARTITION] {
            let mut after = None;
            loop {
                if began.elapsed() >= self.timeout {
                    return Err(E2eError::timeout(
                        "raw history snapshot pagination",
                        self.timeout,
                    ));
                }
                let mut request = json!({
                    "TableName": "gang-e2e", "ConsistentRead": true,
                    "KeyConditionExpression": "pk = :pk",
                    "ExpressionAttributeValues": {":pk": {"S": partition}},
                    "Limit": 100,
                });
                if let Some(after) = after.take() {
                    request["ExclusiveStartKey"] = after;
                }
                let body = serde_json::to_vec(&request)
                    .map_err(E2eError::system_from("encoding raw history query"))?;
                let response = self.client.send(HttpRequest {
                    method: Method::POST,
                    url: &self.endpoint,
                    bearer: None,
                    headers: &[(
                        HeaderName::from_static("x-amz-target"),
                        HeaderValue::from_static("DynamoDB_20120810.Query"),
                    )],
                    body: Some(RequestBody {
                        content_type: "application/x-amz-json-1.0",
                        bytes: Bytes::from(body),
                    }),
                })?;
                require(
                    response.status.is_success(),
                    format!("raw history Query answered {}", response.status),
                )?;
                let response: Value = serde_json::from_slice(&response.body)
                    .map_err(E2eError::system_from("decoding raw history query"))?;
                let items = response
                    .get("Items")
                    .and_then(Value::as_array)
                    .ok_or_else(|| E2eError::system("raw history Query lacks Items"))?;
                for item in items {
                    let sk = item
                        .get("sk")
                        .and_then(|value| value.get("S"))
                        .and_then(Value::as_str)
                        .ok_or_else(|| E2eError::system("raw history item lacks sort key"))?;
                    records.insert((partition.to_owned(), sk.to_owned()), item.clone());
                }
                after = response
                    .get("LastEvaluatedKey")
                    .filter(|key| key.as_object().is_some_and(|key| !key.is_empty()))
                    .cloned();
                if after.is_none() {
                    break;
                }
            }
        }
        Ok(records)
    }

    fn wait(&self, condition: &str, done: impl Fn(&Records) -> bool) -> Result<Records> {
        let began = Instant::now();
        loop {
            let records = self.snapshot()?;
            if done(&records) {
                return Ok(records);
            }
            let remaining = self.timeout.saturating_sub(began.elapsed());
            if remaining.is_zero() {
                return Err(E2eError::timeout(
                    format!("{condition}; stored keys={:?}", records.keys().collect::<Vec<_>>()),
                    self.timeout,
                ));
            }
            std::thread::sleep(self.interval.min(remaining));
        }
    }
}
