//! Behaviour of [`gang_worker::run`] against the fakes, on the virtual clock.

use std::fs;
use std::future;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_config::{SANDBOX_FILE_NAME, SandboxFile};
use gang_spi::{InstanceIdentity, PreemptionNotice, StoreAccess};
use gang_testkit::{SandboxKey, TestPlugins};
use gang_types::{
    Bytes, FleetName, InstanceId, Isa, NonZeroDuration, Percent, WorkerLifetime, property,
};
use gang_worker::{Layout, ServerCaError, WorkerError, WorkerSettings};
use tokio::task::JoinHandle;

use crate::support::{
    Behaviour, FakeEnroller, GIB, Harness, RecordedSpec, RecordedTls, ServerCaAnswer,
};

/// The single spec the scheduler recorded; fails the test when there is not exactly one.
fn only_spec(harness: &Harness) -> RecordedSpec {
    let mut specs = harness.scheduler.specs();
    assert_eq!(specs.len(), 1, "exactly one worker was started");
    specs.remove(0)
}

/// Spawns the worker with `settings`, enrolling only when `tls` is set, and lets it run to its
/// first wait at the current virtual time.
async fn spawn_run(
    harness: &Harness,
    settings: WorkerSettings<TestPlugins>,
    tls: bool,
) -> JoinHandle<Result<(), WorkerError>> {
    let run = harness.spawn(gang_worker::run(
        harness.parts(tls),
        settings,
        harness.build_limits(),
        future::pending(),
    ));
    harness.settle().await;
    run
}

#[tokio::test]
async fn plain_mode_records_the_identity_fleet_and_layout() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Finish);
    let result = gang_worker::run(
        harness.parts(false),
        harness.settings(),
        harness.build_limits(),
        future::pending(),
    )
    .await;
    result.expect("the scheduler finished");

    let spec = only_spec(&harness);
    let layout = Layout::new(&harness.volume);
    assert_eq!(spec.instance.as_str(), "i-0abc123");
    assert_eq!(spec.properties.get(property::FLEET), Some("example-fleet"));
    assert_eq!(spec.properties.get(property::ISA), Some("x86-64"));
    assert_eq!(spec.properties.get(property::SANDBOX), Some("noop"));
    assert_eq!(spec.properties.get(property::CPU_COUNT), Some("4"));
    assert_eq!(spec.properties.get(property::MEMORY_KB), Some("8388608"));
    assert_eq!(spec.slots.count().get(), 4);
    assert!(!spec.slots.is_single_use());
    assert_eq!(spec.work_directory, layout.work_directory());
    assert_eq!(spec.fast_cas_directory, layout.fast_cas_directory());
    assert_eq!(spec.fast_cas_budget.get(), 5 * GIB);
    assert_eq!(spec.tls, RecordedTls::Plain);
    assert!(matches!(spec.store_access, StoreAccess::Direct));
    assert!(layout.work_directory().is_dir());
    assert!(layout.fast_cas_directory().is_dir());
    assert_eq!(spec.entrypoint, layout.entrypoint());
    assert_eq!(
        fs::read_link(&spec.entrypoint).expect("entrypoint is a link"),
        harness.executable
    );
    assert_eq!(
        layout.sandbox_file(),
        layout.entrypoint().with_file_name(SANDBOX_FILE_NAME)
    );
    let sandbox: SandboxFile<TestPlugins> = fs::read_to_string(layout.sandbox_file())
        .expect("sandbox file is readable")
        .parse()
        .expect("sandbox file parses");
    assert_eq!(sandbox.work_directory.as_path(), layout.work_directory());
    assert_eq!(sandbox.limits.map(|limits| limits.max_pids), Some(512));
}

#[tokio::test]
async fn single_use_worker_takes_one_action_and_run_returns_ok() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::SingleUse);
    harness.scheduler.set_behaviour(Behaviour::Finish);
    let result = gang_worker::run(
        harness.parts(false),
        harness.settings(),
        harness.build_limits(),
        future::pending(),
    )
    .await;
    result.expect("a finished single-use worker ends the run");
    let spec = only_spec(&harness);
    assert_eq!(spec.slots.count().get(), 1);
    assert!(spec.slots.is_single_use());
}

#[tokio::test]
async fn limits_are_prepared_once_before_the_first_worker_starts() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Finish);
    let result = gang_worker::run(
        harness.parts(false),
        harness.settings(),
        harness.build_limits(),
        future::pending(),
    )
    .await;
    result.expect("the scheduler finished");
    assert_eq!(harness.limits.observed_at_prepare(), [0]);
}

#[tokio::test]
async fn tls_mode_enrolls_with_the_proof_and_passes_the_credential_paths() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Finish);
    let result = gang_worker::run(
        harness.parts(true),
        harness.settings(),
        harness.build_limits(),
        future::pending(),
    )
    .await;
    result.expect("the scheduler finished");

    assert_eq!(harness.enroller.proofs(), ["proof-1"]);
    let issued = FakeEnroller::issued();
    let spec = only_spec(&harness);
    let RecordedTls::Mutual {
        certificate,
        private_key,
        server_ca,
    } = spec.tls
    else {
        panic!("mutual TLS paths are passed");
    };
    assert_eq!(
        certificate,
        harness
            .volume
            .join("identity")
            .join("0")
            .join("certificate.pem")
    );
    assert_eq!(
        fs::read_to_string(&certificate).expect("certificate is readable"),
        issued.certificate.as_str()
    );
    assert_eq!(
        fs::read_to_string(&private_key).expect("key is readable"),
        issued.private_key.expose()
    );
    assert_eq!(
        fs::read_to_string(&server_ca).expect("CA is readable"),
        issued.ca_certificate.as_str()
    );
}

#[tokio::test]
async fn server_only_transport_passes_custom_ca_or_system_roots() {
    for custom_ca in [true, false] {
        let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
        harness.scheduler.set_behaviour(Behaviour::Finish);
        let ca = FakeEnroller::issued().ca_certificate;
        harness
            .server_ca
            .set_answer(ServerCaAnswer::Ca(custom_ca.then(|| ca.clone())));
        gang_worker::run(
            harness.server_only_parts(),
            harness.settings(),
            harness.build_limits(),
            future::pending(),
        )
        .await
        .expect("server-only worker finishes");
        match only_spec(&harness).tls {
            RecordedTls::ServerOnly(Some(path)) if custom_ca => {
                assert_eq!(path, harness.volume.join("identity/server-ca.pem"));
                assert_eq!(
                    fs::read_to_string(path).expect("CA is readable"),
                    ca.as_str()
                );
            }
            RecordedTls::ServerOnly(None) if !custom_ca => {}
            other => panic!("unexpected transport: {other:?}"),
        }
        assert_eq!(harness.enroller.calls(), 0);
        assert_eq!(harness.server_ca.calls(), 1);
    }
}

#[tokio::test]
async fn shutdown_stops_a_running_generation() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    let (send, receive) = tokio::sync::oneshot::channel();
    let run = harness.spawn(gang_worker::run(
        harness.parts(false),
        harness.settings(),
        harness.build_limits(),
        async {
            let _ = receive.await;
        },
    ));
    harness.settle().await;
    assert_eq!(harness.scheduler.worker_starts().get(), 1);
    send.send(()).expect("worker is listening");
    run.await
        .expect("worker task does not panic")
        .expect("shutdown succeeds");
    assert_eq!(harness.scheduler.worker_stops().get(), 1);
}

#[tokio::test]
async fn refresh_point_starts_a_new_generation_with_fresh_credentials() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    let run = spawn_run(&harness, harness.settings(), true).await;
    assert_eq!(harness.scheduler.specs().len(), 1);
    assert_eq!(harness.enroller.calls(), 1);

    // Credentials expire at 100 s and refresh at 75 percent of that: 75 s.
    let at = harness
        .run_until(|| harness.scheduler.worker_starts().get() >= 2)
        .await;
    assert_eq!(at, Duration::from_secs(75));
    assert_eq!(
        harness.scheduler.worker_stops().get(),
        1,
        "the old generation stopped before the next started"
    );
    assert_eq!(harness.scheduler.specs().len(), 2);
    assert_eq!(harness.scheduler.specs()[1].stops_before_start, 1);
    assert_eq!(harness.enroller.calls(), 2);
    assert_eq!(harness.limits.observed_at_prepare(), [0]);
    run.abort();
}

#[tokio::test]
async fn renewal_writes_the_next_generation_beside_the_running_one() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    let run = spawn_run(&harness, harness.settings(), true).await;
    assert_eq!(harness.scheduler.specs().len(), 1);

    // Credentials expire at 100 s and refresh at 75 percent of that: 75 s.
    let at = harness
        .run_until(|| harness.scheduler.worker_starts().get() >= 2)
        .await;
    assert_eq!(at, Duration::from_secs(75));
    let mut specs = harness.scheduler.specs();
    assert_eq!(specs.len(), 2);
    let second = specs.remove(1);
    let first = specs.remove(0);

    let RecordedTls::Mutual {
        certificate: first_cert,
        private_key: first_key,
        server_ca: first_ca,
    } = first.tls
    else {
        panic!("first generation uses mutual TLS");
    };
    let RecordedTls::Mutual {
        certificate: second_cert,
        private_key: second_key,
        server_ca: second_ca,
    } = second.tls
    else {
        panic!("second generation uses mutual TLS");
    };
    let first_directory = harness.volume.join("identity").join("0");
    let second_directory = harness.volume.join("identity").join("1");
    assert!(
        first_cert.starts_with(&first_directory),
        "generation 0 has its own directory"
    );
    assert!(
        second_cert.starts_with(&second_directory),
        "generation 1 has its own directory"
    );
    assert_ne!(
        second_cert, first_cert,
        "the next certificate has its own file"
    );
    assert_ne!(second_key, first_key, "the next key has its own file");
    assert_ne!(second_ca, first_ca, "the next CA has its own file");

    // The switch removes the first generation's files, so only the new generation is read.
    let second_issued = FakeEnroller::issued_on_call(2);
    assert_eq!(
        fs::read_to_string(&second_cert).expect("certificate is readable"),
        second_issued.certificate.as_str()
    );
    assert_eq!(
        fs::read_to_string(&second_key).expect("key is readable"),
        second_issued.private_key.expose()
    );
    assert_eq!(
        fs::read_to_string(&second_ca).expect("CA is readable"),
        second_issued.ca_certificate.as_str()
    );
    run.abort();
}

#[tokio::test]
async fn renewal_removes_the_previous_generation() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    let run = spawn_run(&harness, harness.settings(), true).await;
    let first_directory = harness.volume.join("identity").join("0");
    let second_directory = harness.volume.join("identity").join("1");
    assert!(first_directory.is_dir(), "the first generation is written");

    // Credentials expire at 100 s and refresh at 75 percent of that: 75 s.
    harness
        .run_until(|| harness.scheduler.worker_starts().get() >= 2)
        .await;
    assert_eq!(harness.scheduler.specs().len(), 2);
    assert!(
        !first_directory.exists(),
        "the previous generation is removed after the switch"
    );
    assert!(second_directory.is_dir(), "the new generation is kept");
    run.abort();
}

#[tokio::test]
async fn preemption_returns_ok_after_the_drain_margin() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    // Terminated at 60 s with a 10 s margin: the drain starts at 50 s.
    harness
        .provisioner
        .set_preemption_notice(Some(PreemptionNotice {
            terminate_at: Timestamp::from_unix_millis(60_000),
        }));
    let run = spawn_run(&harness, harness.settings(), false).await;
    let at = harness.run_until(|| run.is_finished()).await;
    assert_eq!(at, Duration::from_secs(50));
    let result = run.await.expect("the run task does not panic");
    result.expect("preemption ends the run with Ok");
}

#[tokio::test]
async fn preemption_margin_30s_stops_the_run_at_30_seconds() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    // Terminated at 60 s with a 30 s margin: the drain starts at 30 s.
    harness
        .provisioner
        .set_preemption_notice(Some(PreemptionNotice {
            terminate_at: Timestamp::from_unix_millis(60_000),
        }));
    let mut settings = harness.settings();
    settings.worker.preemption_margin = Duration::from_secs(30);
    let run = spawn_run(&harness, settings, false).await;
    let at = harness.run_until(|| run.is_finished()).await;
    assert_eq!(at, Duration::from_secs(30));
    let result = run.await.expect("the run task does not panic");
    result.expect("preemption ends the run with Ok");
}

#[tokio::test]
async fn a_preemption_notice_inside_the_margin_returns_immediately_and_starts_no_worker() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    // Terminated at 5 s with a 10 s margin: the drain has already begun.
    harness
        .provisioner
        .set_preemption_notice(Some(PreemptionNotice {
            terminate_at: Timestamp::from_unix_millis(5_000),
        }));
    let (result, at) = harness
        .drive(gang_worker::run(
            harness.parts(false),
            harness.settings(),
            harness.build_limits(),
            future::pending(),
        ))
        .await;
    result.expect("preemption ends the run with Ok");
    assert_eq!(at, Duration::ZERO, "the run ends before any time passes");
    assert_eq!(harness.scheduler.worker_starts().get(), 0);
    assert!(harness.scheduler.specs().is_empty());
}

#[tokio::test]
async fn shutdown_ends_the_run_with_ok() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    let result = gang_worker::run(
        harness.parts(false),
        harness.settings(),
        harness.build_limits(),
        future::ready(()),
    )
    .await;
    result.expect("shutdown ends the run with Ok");
}

#[tokio::test]
async fn parked_node_does_nothing_until_it_is_not_parked() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    harness.provisioner.set_parked(true);
    // The default `[worker] lifecycle_poll_interval` is 5 s.
    let run = spawn_run(&harness, harness.settings(), true).await;
    let polls = harness.provisioner.parked_calls();
    let at = harness.run_until(|| polls.get() >= 2).await;
    assert_eq!(at, Duration::from_secs(5));
    let at = harness.run_until(|| polls.get() >= 3).await;
    assert_eq!(at, Duration::from_secs(10));

    assert_eq!(harness.enroller.calls(), 0, "a parked node does not enroll");
    assert_eq!(
        harness.scheduler.worker_starts().get(),
        0,
        "a parked node runs no worker"
    );

    harness.provisioner.set_parked(false);
    let at = harness
        .run_until(|| harness.scheduler.worker_starts().get() >= 1)
        .await;
    assert_eq!(at, Duration::from_secs(15));
    assert_eq!(harness.enroller.calls(), 1, "the serving node enrolls once");
    assert_eq!(harness.scheduler.specs().len(), 1, "it registers once");
    run.abort();
}

#[tokio::test]
async fn node_that_is_not_parked_serves_without_delay() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    let run = spawn_run(&harness, harness.settings(), false).await;
    let at = harness
        .run_until(|| harness.scheduler.worker_starts().get() >= 1)
        .await;

    assert_eq!(harness.provisioner.parked_calls().get(), 1, "polled once");
    assert_eq!(at, Duration::ZERO, "no interval passes before serving");
    run.abort();
}

#[tokio::test]
async fn failed_parked_check_does_not_start_the_worker() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    // The node is not parked, but the first check fails: a failure is retried, not served.
    harness.provisioner.fail_parked_next(1);
    let run = spawn_run(&harness, harness.settings(), false).await;
    let polls = harness.provisioner.parked_calls();
    assert_eq!(polls.get(), 1);
    harness.advance(Duration::from_secs(4)).await;
    assert_eq!(
        harness.scheduler.worker_starts().get(),
        0,
        "a failed check does not start the worker"
    );
    assert_eq!(polls.get(), 1, "the next check waits a full interval");

    let at = harness.run_until(|| polls.get() >= 2).await;
    assert_eq!(at, Duration::from_secs(5));
    assert_eq!(harness.scheduler.worker_starts().get(), 1);
    run.abort();
}

#[tokio::test]
async fn shutdown_while_parked_ends_the_run_without_enrolling() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    harness.provisioner.set_parked(true);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let run = harness.spawn(gang_worker::run(
        harness.parts(true),
        harness.settings(),
        harness.build_limits(),
        async {
            let _ = stopped.await;
        },
    ));
    harness.provisioner.parked_calls().reached(1).await;
    stop.send(())
        .expect("the run is still waiting for shutdown");

    let result = run.await.expect("the run task does not panic");
    result.expect("shutdown ends the run with Ok");
    assert_eq!(harness.enroller.calls(), 0, "a parked node does not enroll");
    assert_eq!(harness.scheduler.worker_starts().get(), 0);
}

#[tokio::test]
async fn a_parked_server_only_node_fetches_no_server_ca_until_it_serves() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    harness.provisioner.set_parked(true);
    let run = harness.spawn(gang_worker::run(
        harness.server_only_parts(),
        harness.settings(),
        harness.build_limits(),
        future::pending(),
    ));
    harness.settle().await;
    let polls = harness.provisioner.parked_calls();
    let at = harness.run_until(|| polls.get() >= 3).await;
    assert_eq!(at, Duration::from_secs(10));
    assert_eq!(harness.server_ca.calls(), 0);
    assert_eq!(harness.scheduler.worker_starts().get(), 0);

    harness.provisioner.set_parked(false);
    let at = harness
        .run_until(|| harness.scheduler.worker_starts().get() >= 1)
        .await;
    assert_eq!(at, Duration::from_secs(15));
    assert_eq!(harness.server_ca.calls(), 1);
    run.abort();
}

#[tokio::test]
async fn shutdown_while_parked_ends_a_server_only_run_without_fetching_the_server_ca() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    harness.provisioner.set_parked(true);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let run = harness.spawn(gang_worker::run(
        harness.server_only_parts(),
        harness.settings(),
        harness.build_limits(),
        async {
            let _ = stopped.await;
        },
    ));
    harness.provisioner.parked_calls().reached(1).await;
    stop.send(())
        .expect("the run is still waiting for shutdown");
    run.await
        .expect("the run task does not panic")
        .expect("shutdown ends the run with Ok");
    assert_eq!(harness.server_ca.calls(), 0);
    assert_eq!(harness.scheduler.worker_starts().get(), 0);
}

#[tokio::test]
async fn a_preemption_notice_at_unparking_ends_a_server_only_run_without_fetching_the_server_ca() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    harness
        .provisioner
        .set_preemption_notice(Some(PreemptionNotice {
            terminate_at: Timestamp::from_unix_millis(5_000),
        }));
    let (result, at) = harness
        .drive(gang_worker::run(
            harness.server_only_parts(),
            harness.settings(),
            harness.build_limits(),
            future::pending(),
        ))
        .await;
    result.expect("preemption ends the run with Ok");
    assert_eq!(at, Duration::ZERO);
    assert_eq!(harness.server_ca.calls(), 0);
    assert_eq!(harness.scheduler.worker_starts().get(), 0);
}

#[tokio::test]
async fn shutdown_during_the_server_ca_fetch_ends_the_run() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    harness.server_ca.hold();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let run = harness.spawn(gang_worker::run(
        harness.server_only_parts(),
        harness.settings(),
        harness.build_limits(),
        async {
            let _ = stopped.await;
        },
    ));
    harness.server_ca.call_count().reached(1).await;
    stop.send(())
        .expect("the run is still waiting for shutdown");
    run.await
        .expect("the run task does not panic")
        .expect("shutdown ends the run with Ok");
    assert_eq!(harness.scheduler.worker_starts().get(), 0);
}

#[tokio::test]
async fn preemption_during_the_server_ca_fetch_ends_the_run() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    harness.server_ca.hold();
    let run = harness.spawn(gang_worker::run(
        harness.server_only_parts(),
        harness.settings(),
        harness.build_limits(),
        future::pending(),
    ));
    harness.settle().await;
    assert_eq!(harness.server_ca.calls(), 1);
    harness
        .provisioner
        .set_preemption_notice(Some(PreemptionNotice {
            terminate_at: Timestamp::from_unix_millis(60_000),
        }));
    let at = harness.run_until(|| run.is_finished()).await;
    assert_eq!(at, Duration::from_secs(50));
    run.await
        .expect("the run task does not panic")
        .expect("preemption ends the run with Ok");
    assert_eq!(harness.scheduler.worker_starts().get(), 0);
}

#[tokio::test]
async fn a_pinned_server_without_a_ca_fails_the_run_after_unparking() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    harness.server_ca.set_answer(ServerCaAnswer::Missing);
    harness.provisioner.set_parked(true);
    let run = harness.spawn(gang_worker::run(
        harness.server_only_parts(),
        harness.settings(),
        harness.build_limits(),
        future::pending(),
    ));
    let polls = harness.provisioner.parked_calls();
    harness.run_until(|| polls.get() >= 2).await;
    assert_eq!(harness.server_ca.calls(), 0);
    assert!(!run.is_finished());
    harness.provisioner.set_parked(false);
    harness.run_until(|| run.is_finished()).await;
    let result = run.await.expect("the run task does not panic");
    assert!(matches!(
        result,
        Err(WorkerError::ServerCa(ServerCaError::Missing))
    ));
    assert_eq!(harness.server_ca.calls(), 1);
    assert_eq!(harness.scheduler.worker_starts().get(), 0);
}

/// The instance identity of `fleet` running instruction set `isa`.
fn identity_of(fleet: &str, isa: Isa) -> InstanceIdentity {
    InstanceIdentity {
        instance: InstanceId::try_new("i-0abc123").expect("instance id is valid"),
        fleet: FleetName::try_new(fleet).expect("test fleet name is valid"),
        isa,
    }
}

#[tokio::test]
async fn unknown_fleet_has_no_fleet() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness
        .provisioner
        .set_identity(identity_of("example-arm", Isa::X86_64));
    let error = gang_worker::run(
        harness.parts(false),
        harness.settings(),
        harness.build_limits(),
        future::pending(),
    )
    .await
    .expect_err("no fleet is configured under that name");
    assert!(matches!(error, WorkerError::NoFleet(name) if name.to_string() == "example-arm"));
}

#[tokio::test]
async fn isa_mismatch_stops_before_anything_starts() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness
        .provisioner
        .set_identity(identity_of("example-fleet", Isa::Aarch64));
    let error = gang_worker::run(
        harness.parts(false),
        harness.settings(),
        harness.build_limits(),
        future::pending(),
    )
    .await
    .expect_err("the fleet is configured for x86-64");
    assert!(matches!(error, WorkerError::IsaMismatch { .. }));
    assert!(harness.limits.observed_at_prepare().is_empty());
    assert!(harness.scheduler.specs().is_empty());
}

/// An undersized volume is rejected before limits preparation, enrollment, or worker start.
#[tokio::test]
async fn a_scratch_volume_below_the_fleet_disk_stops_the_worker_before_it_enrolls() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.provisioner.set_capacity(Bytes::gib(2));
    let settings = harness.settings();
    let fleet = FleetName::try_new("example-fleet").expect("test fleet name is valid");
    let declared = settings
        .fleets
        .by_name(&fleet)
        .expect("the fleet is configured")
        .policy
        .disk_per_instance
        .get();
    let error = gang_worker::run(
        harness.parts(true),
        settings,
        harness.build_limits(),
        future::pending(),
    )
    .await
    .expect_err("the scratch volume is too small for the fleet");
    assert!(matches!(error, WorkerError::FleetDiskUnavailable {
            fleet: actual_fleet,
            declared: actual_declared,
            measured,
        } if actual_fleet == fleet && actual_declared == declared && measured < declared));
    assert_eq!(harness.enroller.calls(), 0);
    assert!(harness.limits.observed_at_prepare().is_empty());
    assert!(harness.scheduler.specs().is_empty());
}

#[tokio::test]
async fn sandbox_file_selects_the_identity_fleets_sandbox() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness
        .provisioner
        .set_identity(identity_of("example-boxed", Isa::X86_64));
    harness.scheduler.set_behaviour(Behaviour::Finish);
    let result = gang_worker::run(
        harness.parts(false),
        harness.settings(),
        harness.build_limits(),
        future::pending(),
    )
    .await;
    result.expect("the scheduler finished");

    let spec = only_spec(&harness);
    assert_eq!(spec.properties.get(property::FLEET), Some("example-boxed"));
    let layout = Layout::new(&harness.volume);
    let sandbox: SandboxFile<TestPlugins> = fs::read_to_string(layout.sandbox_file())
        .expect("sandbox file is readable")
        .parse()
        .expect("sandbox file parses");
    assert_eq!(sandbox.selected, SandboxKey::Boxed);
}

#[tokio::test]
async fn scheduler_failure_is_reported_as_a_worker_stop() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Fail);
    let error = gang_worker::run(
        harness.parts(false),
        harness.settings(),
        harness.build_limits(),
        future::pending(),
    )
    .await
    .expect_err("the scheduler failed");
    assert!(matches!(error, WorkerError::Scheduler(_)));
}

#[tokio::test]
async fn enroller_failure_is_reported_as_an_enroll_error() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.enroller.fail_forever();
    let error = gang_worker::run(
        harness.parts(true),
        harness.settings(),
        harness.build_limits(),
        future::pending(),
    )
    .await
    .expect_err("enrollment failed");
    assert!(matches!(error, WorkerError::Enroll(_)));
}

// The certificate expires at 100 s and is refreshed at 75 s; renewals are retried every 10 s.
#[tokio::test]
async fn failed_renewals_keep_the_current_worker_running() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    let run = spawn_run(&harness, harness.settings(), true).await;
    assert_eq!(harness.enroller.calls(), 1);
    harness.enroller.fail_next(2);

    let at = harness
        .run_until(|| harness.enroller.enrollments().get() >= 2)
        .await;
    assert_eq!(at, Duration::from_secs(75));
    assert_eq!(harness.enroller.calls(), 2);
    assert_eq!(harness.scheduler.specs().len(), 1);
    assert!(!run.is_finished());

    let at = harness
        .run_until(|| harness.enroller.enrollments().get() >= 3)
        .await;
    assert_eq!(at, Duration::from_secs(85));
    assert_eq!(harness.enroller.calls(), 3);
    assert_eq!(harness.scheduler.specs().len(), 1);

    let at = harness
        .run_until(|| harness.scheduler.worker_starts().get() >= 2)
        .await;
    assert_eq!(at, Duration::from_secs(95));
    assert_eq!(
        harness.enroller.calls(),
        4,
        "three renewals after the first"
    );
    let specs = harness.scheduler.specs();
    assert_eq!(specs.len(), 2);
    assert_ne!(
        specs[0].tls, specs[1].tls,
        "the next generation has its own identity files"
    );
    assert_eq!(harness.limits.observed_at_prepare(), [0]);
    run.abort();
}

#[tokio::test]
async fn a_rate_limited_renewal_honours_the_servers_retry_after() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    let run = spawn_run(&harness, harness.settings(), true).await;
    // The first enrollment is accepted; the next two are refused, each with a 7 s retry-after.
    // Renewals wait that 7 s rather than the 10 s retry interval: refused at 75 s and 82 s,
    // accepted at 89 s.
    harness
        .enroller
        .rate_limit_next(2, Some(Duration::from_secs(7)));

    let at = harness
        .run_until(|| harness.scheduler.worker_starts().get() >= 2)
        .await;
    assert_eq!(at, Duration::from_secs(89));
    assert_eq!(
        harness.enroller.calls(),
        4,
        "refused at 75 s and 82 s, accepted at 89 s"
    );
    run.abort();
}

#[tokio::test]
async fn a_rate_limited_renewal_is_fatal_once_the_certificate_expires() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    let run = spawn_run(&harness, harness.settings(), true).await;
    harness.enroller.rate_limit_forever(None);

    let at = harness.run_until(|| run.is_finished()).await;
    assert_eq!(at, Duration::from_secs(95));
    let error = run
        .await
        .expect("the run task does not panic")
        .expect_err("the next retry would exceed expiry");
    assert!(matches!(error, WorkerError::Enroll(_)));
    assert_eq!(
        harness.enroller.calls(),
        4,
        "attempts at 75, 85 and 95 s, after the first enrollment"
    );
}

#[tokio::test]
async fn renewal_failure_is_fatal_once_the_certificate_expires() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    let run = spawn_run(&harness, harness.settings(), true).await;
    harness.enroller.fail_forever();

    let at = harness
        .run_until(|| harness.enroller.enrollments().get() >= 4)
        .await;
    assert_eq!(at, Duration::from_secs(95));
    assert!(run.is_finished(), "the next retry would exceed expiry");
    assert_eq!(harness.scheduler.specs().len(), 1);

    let at = harness.run_until(|| run.is_finished()).await;
    assert_eq!(at, Duration::from_secs(95));
    let error = run
        .await
        .expect("the run task does not panic")
        .expect_err("the next retry would exceed expiry");
    assert!(matches!(error, WorkerError::Enroll(_)));
    assert_eq!(harness.enroller.calls(), 4);
}

#[tokio::test]
async fn failed_workload_credential_is_retried_like_a_failed_enrollment() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    let run = spawn_run(&harness, harness.settings(), true).await;
    harness.provisioner.fail_workload_credential_next(1);

    let at = harness
        .run_until(|| harness.provisioner.workload_credential_calls().get() >= 2)
        .await;
    assert_eq!(at, Duration::from_secs(75));
    assert_eq!(harness.scheduler.specs().len(), 1);
    assert_eq!(harness.enroller.calls(), 1, "no enrollment without a proof");
    assert!(!run.is_finished());

    let at = harness
        .run_until(|| harness.scheduler.worker_starts().get() >= 2)
        .await;
    assert_eq!(at, Duration::from_secs(85));
    assert_eq!(harness.scheduler.specs().len(), 2);
    assert_eq!(harness.enroller.calls(), 2);
    run.abort();
}

#[tokio::test]
async fn failed_workload_credential_is_fatal_once_the_certificate_expires() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    let run = spawn_run(&harness, harness.settings(), true).await;
    harness.provisioner.fail_workload_credential_forever();

    let at = harness
        .run_until(|| harness.provisioner.workload_credential_calls().get() >= 4)
        .await;
    assert_eq!(at, Duration::from_secs(95));
    assert!(run.is_finished());

    let at = harness.run_until(|| run.is_finished()).await;
    assert_eq!(at, Duration::from_secs(95));
    let error = run
        .await
        .expect("the run task does not panic")
        .expect_err("the next retry would exceed expiry");
    assert!(matches!(error, WorkerError::Port(_)));
    assert_eq!(harness.provisioner.workload_credential_calls().get(), 4);
    assert_eq!(harness.enroller.calls(), 1);
}

#[tokio::test]
async fn sandbox_file_names_the_canonical_program_path() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness
        .provisioner
        .set_identity(identity_of("example-boxed", Isa::X86_64));
    harness.scheduler.set_behaviour(Behaviour::Finish);
    let result = gang_worker::run(
        harness.parts(false),
        harness.settings(),
        harness.build_limits(),
        future::pending(),
    )
    .await;
    result.expect("the scheduler finished");

    let layout = Layout::new(&harness.volume);
    let sandbox: SandboxFile<TestPlugins> = fs::read_to_string(layout.sandbox_file())
        .expect("sandbox file is readable")
        .parse()
        .expect("sandbox file parses");
    let program = fs::canonicalize(harness.bin.join("fake-sandbox"))
        .expect("sandbox program can be canonicalized");
    assert!(program.is_absolute());
    let boxed = sandbox
        .sandbox
        .boxed
        .expect("the boxed section is rendered");
    assert_eq!(boxed.program, program);
}

#[tokio::test]
async fn certificate_refresh_percent_60_starts_the_next_generation_at_60_seconds() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    let mut settings = harness.settings();
    settings.worker.certificate_refresh_percent = Percent::from_percent(60);
    let run = spawn_run(&harness, settings, true).await;

    // Credentials expire at 100 s and refresh at 60 percent of that: 60 s.
    let at = harness
        .run_until(|| harness.scheduler.worker_starts().get() >= 2)
        .await;
    assert_eq!(at, Duration::from_mins(1));
    run.abort();
}

#[tokio::test]
async fn fast_cas_percent_20_gives_a_two_gib_budget() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Finish);
    let mut settings = harness.settings();
    settings.worker.fast_cas_percent = Percent::from_percent(20);
    let result = gang_worker::run(
        harness.parts(false),
        settings,
        harness.build_limits(),
        future::pending(),
    )
    .await;
    result.expect("the scheduler finished");

    // 20 percent of the 10 GiB scratch volume.
    assert_eq!(only_spec(&harness).fast_cas_budget.get(), 2 * GIB);
}

#[tokio::test]
async fn a_restart_with_a_filled_fast_cas_keeps_the_cold_start_budget() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Finish);
    let layout = Layout::new(&harness.volume);
    fs::create_dir_all(layout.fast_cas_directory()).expect("fast CAS directory can be created");
    let cached_file = layout.fast_cas_directory().join("cached");
    fs::write(&cached_file, vec![0_u8; 1024 * 1024]).expect("cached file can be written");
    let allocated = gang_os::fs::usage(&layout.fast_cas_directory())
        .expect("fast CAS usage can be measured")
        .get();
    // A real volume reports the free space left after the cache holds `allocated` bytes.
    harness
        .provisioner
        .set_capacity(Bytes::new(10 * GIB - allocated));
    let mut settings = harness.settings();
    settings.worker.fast_cas_percent = Percent::from_percent(50);
    let result = gang_worker::run(
        harness.parts(false),
        settings,
        harness.build_limits(),
        future::pending(),
    )
    .await;
    result.expect("the scheduler finished");

    // The cold-start budget: 50 percent of the 10 GiB scratch volume.
    assert_eq!(only_spec(&harness).fast_cas_budget.get(), 5 * GIB);
}

#[tokio::test]
async fn lifecycle_poll_interval_3s_polls_the_parked_node_every_three_seconds() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    harness.provisioner.set_parked(true);
    let mut settings = harness.settings();
    settings.worker.lifecycle_poll_interval = NonZeroDuration::from_secs(3);
    let run = spawn_run(&harness, settings, true).await;
    let polls = harness.provisioner.parked_calls();

    let at = harness.run_until(|| polls.get() >= 2).await;
    assert_eq!(at, Duration::from_secs(3));
    let at = harness.run_until(|| polls.get() >= 3).await;
    assert_eq!(at, Duration::from_secs(6));
    run.abort();
}
