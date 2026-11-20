//! Behaviour of [`gang_worker::run`] against the fakes, on the virtual clock.

use std::fs;
use std::future;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_config::{FleetMember, Fleets, SANDBOX_FILE_NAME, SandboxFile};
use gang_spi::{ActionMemoryPool, InstanceIdentity, PreemptionNotice, StoreAccess};
use gang_testkit::{SandboxKey, TestPlugins};
use gang_types::{
    Bytes, FleetName, InstanceId, Isa, NonZeroBytes, NonZeroDuration, Percent, WorkerLifetime,
    property,
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
        harness.build_sandbox(),
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
        harness.build_sandbox(),
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
    assert_eq!(
        spec.properties.get(property::ACTION_ENVIRONMENT),
        Some("host:test")
    );
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
async fn wrong_image_is_rejected_before_enrollment_and_registration() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    let mut settings = harness.settings();
    let fleet_name = FleetName::try_new("example-fleet").expect("valid fleet");
    let fleet = settings.fleets.by_name(&fleet_name).expect("fleet exists");
    let mut policy = fleet.policy.clone();
    policy.action_environment = "1111111111111111111111111111111111111111111111111111111111111111"
        .parse()
        .expect("valid E1 identity");
    settings.fleets = Fleets::try_new(vec![FleetMember {
        policy,
        sandbox: fleet.sandbox,
        options: fleet.options.clone(),
    }])
    .expect("valid fleet");
    let error = gang_worker::run(
        harness.parts(true),
        settings,
        harness.build_limits(),
        |_, _| {
            Ok(
                "2222222222222222222222222222222222222222222222222222222222222222"
                    .parse()
                    .expect("valid E2 identity"),
            )
        },
        future::pending(),
    )
    .await
    .expect_err("a different local environment cannot register");
    assert!(matches!(error, WorkerError::EnvironmentMismatch { .. }));
    assert_eq!(harness.enroller.enrollments().get(), 0);
    assert!(harness.scheduler.specs().is_empty());
}

#[tokio::test]
async fn selected_root_is_pinned_before_identity_and_serialization() {
    use std::os::unix::fs::symlink;

    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Finish);
    harness
        .provisioner
        .set_identity(identity_of("example-boxed", Isa::X86_64));
    let first = harness.volume.join("image-one");
    let second = harness.volume.join("image-two");
    let link = harness.volume.join("current-image");
    fs::create_dir(&first).expect("first image");
    fs::create_dir(&second).expect("second image");
    fs::write(first.join("identity"), "host:test").expect("first identity");
    fs::write(second.join("identity"), "host:other").expect("second identity");
    symlink(&first, &link).expect("image link");
    let expected = fs::canonicalize(&first).expect("canonical first image");
    let mut settings = harness.settings();
    settings.sandbox.boxed.as_mut().expect("boxed section").root =
        Some(gang_types::AbsolutePathBuf::try_new(&link).expect("absolute link"));

    gang_worker::run(
        harness.parts(false),
        settings,
        harness.build_limits(),
        |key, sandbox| {
            assert_eq!(key, SandboxKey::Boxed);
            let root = sandbox
                .boxed
                .as_ref()
                .expect("boxed section")
                .root
                .as_ref()
                .expect("root");
            assert_eq!(root.as_path(), expected);
            fs::remove_file(&link).expect("remove link");
            symlink(&second, &link).expect("switch image link");
            Ok(fs::read_to_string(root.join("identity"))
                .expect("pinned identity")
                .parse()
                .expect("valid identity"))
        },
        future::pending(),
    )
    .await
    .expect("worker registers");

    let sandbox: SandboxFile<TestPlugins> =
        fs::read_to_string(Layout::new(&harness.volume).sandbox_file())
            .expect("sandbox file")
            .parse()
            .expect("valid sandbox file");
    assert_eq!(
        sandbox
            .sandbox
            .boxed
            .expect("boxed section")
            .root
            .expect("root")
            .as_path(),
        expected
    );
    assert_eq!(
        only_spec(&harness)
            .properties
            .get(property::ACTION_ENVIRONMENT),
        Some("host:test")
    );
}

#[tokio::test]
async fn single_use_worker_takes_one_action_and_run_returns_ok() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::SingleUse);
    harness.scheduler.set_behaviour(Behaviour::Finish);
    let result = gang_worker::run(
        harness.parts(false),
        harness.settings(),
        harness.build_limits(),
        harness.build_sandbox(),
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
        harness.build_sandbox(),
        future::pending(),
    )
    .await;
    result.expect("the scheduler finished");
    assert_eq!(harness.limits.observed_at_prepare(), [0]);
    assert_eq!(
        harness.limits.prepared_reserve(),
        Some(NonZeroBytes::gib(2).get())
    );
    assert_eq!(only_spec(&harness).memory_pool, None);
}

#[tokio::test]
async fn memory_pool_and_actions_reach_the_scheduler() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Finish);
    let settings = harness.settings();
    let actions = settings.actions.clone();
    let pool = ActionMemoryPool {
        cgroup: PathBuf::from("/test/action-pool"),
        capacity: NonZeroBytes::gib(8),
    };
    let limits = harness.limits.clone().with_memory_pool(pool.clone());
    gang_worker::run(
        harness.parts(false),
        settings,
        move |_| Ok(limits),
        harness.build_sandbox(),
        future::pending(),
    )
    .await
    .expect("a pool equal to the declared memory is accepted");
    assert_eq!(
        harness.limits.prepared_reserve(),
        Some(NonZeroBytes::gib(2).get())
    );
    let spec = only_spec(&harness);
    assert_eq!(spec.memory_pool, Some(pool));
    assert!(std::sync::Arc::ptr_eq(&spec.actions, &actions));
}

#[tokio::test]
async fn insufficient_memory_pool_fails_before_enrollment() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    let limits = harness.limits.clone().with_memory_pool(ActionMemoryPool {
        cgroup: PathBuf::from("/test/action-pool"),
        capacity: NonZeroBytes::gib(7),
    });
    let error = gang_worker::run(
        harness.parts(true),
        harness.settings(),
        move |_| Ok(limits),
        harness.build_sandbox(),
        future::pending(),
    )
    .await
    .expect_err("a smaller action pool is rejected");
    match error {
        WorkerError::FleetMemoryUnavailable {
            fleet,
            declared,
            capacity,
            reserve,
        } => {
            assert_eq!(fleet.as_str(), "example-fleet");
            assert_eq!(declared, NonZeroBytes::gib(8).get());
            assert_eq!(capacity, NonZeroBytes::gib(7).get());
            assert_eq!(reserve, NonZeroBytes::gib(2).get());
        }
        other => panic!("unexpected worker error: {other:?}"),
    }
    assert_eq!(harness.enroller.calls(), 0);
    assert!(harness.scheduler.specs().is_empty());
}

#[tokio::test]
async fn no_limits_plugin_passes_no_pool() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Finish);
    let mut settings = harness.settings();
    settings.limits = None;
    gang_worker::run(
        harness.parts(false),
        settings,
        harness.build_limits(),
        harness.build_sandbox(),
        future::pending(),
    )
    .await
    .expect("a worker without limits starts");
    assert_eq!(harness.limits.prepared_reserve(), None);
    assert_eq!(only_spec(&harness).memory_pool, None);
}

#[tokio::test]
async fn tls_mode_enrolls_with_the_proof_and_passes_the_credential_paths() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Finish);
    let result = gang_worker::run(
        harness.parts(true),
        harness.settings(),
        harness.build_limits(),
        harness.build_sandbox(),
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
            harness.build_sandbox(),
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
        harness.build_sandbox(),
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
            harness.build_sandbox(),
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
        harness.build_sandbox(),
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
        harness.build_sandbox(),
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
        harness.build_sandbox(),
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
        harness.build_sandbox(),
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
            harness.build_sandbox(),
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
        harness.build_sandbox(),
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
        harness.build_sandbox(),
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
        harness.build_sandbox(),
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
        harness.build_sandbox(),
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
        harness.build_sandbox(),
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
        harness.build_sandbox(),
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
async fn stale_work_is_reclaimed_before_capacity_check() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Finish);
    harness
        .provisioner
        .set_capacity(Bytes::new(2 * GIB + 64 * 1024));
    harness.provisioner.set_capacity_accounting(true);

    let layout = Layout::new(&harness.volume);
    let stale = layout.work_directory().join("stale");
    fs::create_dir_all(layout.work_directory()).expect("work directory can be created");
    fs::write(&stale, vec![1_u8; 1024 * 1024]).expect("stale work can be written");
    assert!(
        gang_os::fs::usage(&stale)
            .expect("stale work usage can be measured")
            .get()
            > 0,
        "stale work must occupy disk blocks"
    );
    fs::create_dir_all(layout.fast_cas_directory()).expect("fast CAS directory can be created");
    let cached = layout.fast_cas_directory().join("cached");
    let sentinel = b"warm cache sentinel";
    fs::write(&cached, sentinel).expect("cached sentinel can be written");

    let mut settings = harness.settings();
    let fleet_name = FleetName::try_new("example-fleet").expect("test fleet name is valid");
    let fleet = settings.fleets.by_name(&fleet_name).expect("fleet exists");
    let mut policy = fleet.policy.clone();
    policy.disk_per_instance = NonZeroBytes::gib(1);
    settings.fleets = Fleets::try_new(vec![FleetMember {
        policy,
        sandbox: fleet.sandbox,
        options: fleet.options.clone(),
    }])
    .expect("test fleet is valid");

    gang_worker::run(
        harness.parts(true),
        settings,
        harness.build_limits(),
        harness.build_sandbox(),
        future::pending(),
    )
    .await
    .expect("reclaiming stale work leaves enough space for the fleet");

    assert!(!stale.exists());
    assert_eq!(
        fs::read(cached)
            .expect("cached sentinel is preserved")
            .as_slice(),
        sentinel
    );
    assert!(only_spec(&harness).fast_cas_budget.get() >= GIB);
    assert_eq!(harness.enroller.calls(), 1);
}

/// Restores owner access to its directories, outermost first, when dropped, so a failed test
/// leaves a tree its temporary directory can delete.
struct OwnerAccess(Vec<PathBuf>);

impl Drop for OwnerAccess {
    fn drop(&mut self) {
        for directory in &self.0 {
            // A directory that was removed has nothing to restore.
            let _ = fs::set_permissions(directory, fs::Permissions::from_mode(0o700));
        }
    }
}

/// A restarted worker reclaims stale work that an action left read-only or search-only, and
/// removes a link to an external read-only directory without following it.
#[tokio::test]
async fn read_only_stale_work_is_reclaimed_without_following_links() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Finish);
    let outside = tempfile::tempdir().expect("a temporary directory can be created");
    let external = outside.path().join("external");
    let layout = Layout::new(&harness.volume);
    let action = layout.work_directory().join("action");
    let read_only = action.join("read-only");
    let search_only = read_only.join("search-only");
    let _access = OwnerAccess(vec![
        external.clone(),
        read_only.clone(),
        search_only.clone(),
    ]);
    fs::create_dir(&external).expect("external directory can be created");
    fs::write(external.join("data"), b"external data").expect("external data can be written");
    fs::set_permissions(&external, fs::Permissions::from_mode(0o555))
        .expect("external directory can be made read-only");
    fs::create_dir_all(&search_only).expect("stale work can be created");
    fs::write(read_only.join("file"), b"stale").expect("stale work can be written");
    fs::write(search_only.join("file"), b"stale").expect("stale work can be written");
    std::os::unix::fs::symlink(&external, read_only.join("external"))
        .expect("a link to the external directory can be created");
    fs::set_permissions(&search_only, fs::Permissions::from_mode(0o100))
        .expect("work can be made search-only");
    fs::set_permissions(&read_only, fs::Permissions::from_mode(0o555))
        .expect("work can be made read-only");

    gang_worker::run(
        harness.parts(false),
        harness.settings(),
        harness.build_limits(),
        harness.build_sandbox(),
        future::pending(),
    )
    .await
    .expect("read-only stale work does not stop the worker");

    assert_eq!(
        fs::symlink_metadata(&action)
            .map_err(|error| error.kind())
            .err(),
        Some(std::io::ErrorKind::NotFound)
    );
    only_spec(&harness);
    let mode = fs::metadata(&external)
        .expect("external directory remains")
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o555);
    assert_eq!(
        fs::read(external.join("data")).expect("external data remains"),
        b"external data"
    );
    assert_eq!(
        fs::read_dir(&external)
            .expect("external directory can be listed")
            .count(),
        1
    );
}

/// Stale work an action left without owner read or search access is reclaimed on Linux. Apple
/// cannot pin such a directory, so the worker stops before starting, naming the directory and
/// leaving it and its contents untouched.
#[tokio::test]
async fn sealed_stale_work_is_reclaimed_where_pinnable() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Finish);
    let layout = Layout::new(&harness.volume);
    let action = layout.work_directory().join("action");
    let sealed = action.join("sealed");
    let _access = OwnerAccess(vec![sealed.clone()]);
    fs::create_dir_all(&sealed).expect("stale work can be created");
    fs::write(sealed.join("file"), b"stale").expect("stale work can be written");
    fs::set_permissions(&sealed, fs::Permissions::from_mode(0o000))
        .expect("work can be made inaccessible");

    let run = gang_worker::run(
        harness.parts(false),
        harness.settings(),
        harness.build_limits(),
        harness.build_sandbox(),
        future::pending(),
    )
    .await;

    if cfg!(target_os = "linux") {
        run.expect("sealed stale work does not stop the worker");
        assert_eq!(
            fs::symlink_metadata(&action)
                .map_err(|error| error.kind())
                .err(),
            Some(std::io::ErrorKind::NotFound)
        );
        only_spec(&harness);
    } else {
        let error = run.expect_err("a sealed directory cannot be pinned on Apple");
        assert!(matches!(&error, WorkerError::Io(io)
            if io.path == sealed && io.source.kind() == std::io::ErrorKind::PermissionDenied));
        assert!(harness.scheduler.specs().is_empty());
        let mode = fs::symlink_metadata(&sealed)
            .expect("sealed work remains")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o000);
        fs::set_permissions(&sealed, fs::Permissions::from_mode(0o700))
            .expect("sealed work can be opened for inspection");
        assert_eq!(
            fs::read(sealed.join("file")).expect("sealed work remains"),
            b"stale"
        );
    }
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
        harness.build_sandbox(),
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
        harness.build_sandbox(),
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
        harness.build_sandbox(),
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
        harness.build_sandbox(),
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
        harness.build_sandbox(),
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
        harness.build_sandbox(),
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
