//! Enrollment of the first credentials on a virtual clock: rate-limit refusals are retried until
//! the enrollment deadline, and every other refusal is fatal at once.

use std::future;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_spi::PreemptionNotice;
use gang_types::{Isa, NonZeroDuration, WorkerLifetime};
use gang_worker::WorkerError;
use tokio::sync::oneshot;

use crate::support::{Behaviour, Harness};

#[tokio::test]
async fn rate_limited_first_enrollment_waits_for_retry_after_then_succeeds() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    harness
        .enroller
        .rate_limit_next(2, Some(Duration::from_secs(7)));
    let run = harness.spawn(gang_worker::run(
        harness.parts(true),
        harness.settings(),
        harness.build_limits(),
        future::pending(),
    ));

    let started_at = harness
        .run_until(|| harness.scheduler.worker_starts().get() >= 1)
        .await;
    assert_eq!(
        started_at,
        Duration::from_secs(14),
        "two refusals of 7 s each were waited out before the worker ran"
    );
    assert_eq!(harness.enroller.calls(), 3);
    assert_eq!(harness.scheduler.specs().len(), 1);
    assert!(!run.is_finished());
    run.abort();
}

#[tokio::test]
async fn rate_limit_without_retry_after_waits_the_retry_interval() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    harness.enroller.rate_limit_next(1, None);
    let mut settings = harness.settings();
    settings.worker.enrollment_retry_interval = NonZeroDuration::from_secs(45);
    let run = harness.spawn(gang_worker::run(
        harness.parts(true),
        settings,
        harness.build_limits(),
        future::pending(),
    ));

    let started_at = harness
        .run_until(|| harness.scheduler.worker_starts().get() >= 1)
        .await;
    assert_eq!(
        started_at,
        Duration::from_secs(45),
        "the refusal was retried after enrollment_retry_interval"
    );
    assert_eq!(harness.enroller.calls(), 2);
    run.abort();
}

#[tokio::test]
async fn rate_limit_refusals_past_the_enrollment_deadline_are_fatal() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    harness
        .enroller
        .rate_limit_forever(Some(Duration::from_secs(10)));
    let mut settings = harness.settings();
    settings.worker.enrollment_deadline = NonZeroDuration::from_secs(30);
    let run = harness.spawn(gang_worker::run(
        harness.parts(true),
        settings,
        harness.build_limits(),
        future::pending(),
    ));

    let finished_at = harness.run_until(|| run.is_finished()).await;
    assert_eq!(
        finished_at,
        Duration::from_secs(30),
        "the attempt at the deadline is refused and fatal"
    );
    let error = run
        .await
        .expect("the run task does not panic")
        .expect_err("the enrollment deadline passed");
    assert!(matches!(error, WorkerError::Enroll(_)));
    assert_eq!(
        harness.enroller.calls(),
        4,
        "attempts at 0, 10, 20 and 30 s"
    );
    assert!(harness.scheduler.specs().is_empty());
}

#[tokio::test]
async fn a_retry_that_would_exceed_the_deadline_is_not_started() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    harness
        .enroller
        .rate_limit_forever(Some(Duration::from_secs(10)));
    let mut settings = harness.settings();
    settings.worker.enrollment_deadline = NonZeroDuration::from_secs(25);
    let run = harness.spawn(gang_worker::run(
        harness.parts(true),
        settings,
        harness.build_limits(),
        future::pending(),
    ));

    let finished_at = harness.run_until(|| run.is_finished()).await;
    assert_eq!(
        finished_at,
        Duration::from_secs(20),
        "the next retry would start beyond the deadline"
    );
    let error = run
        .await
        .expect("the run task does not panic")
        .expect_err("the enrollment deadline passed");
    assert!(matches!(error, WorkerError::Enroll(_)));
    assert_eq!(harness.enroller.calls(), 3, "attempts at 0, 10 and 20 s");
    assert!(harness.scheduler.specs().is_empty());
}

#[tokio::test]
async fn retry_after_longer_than_deadline_fails_immediately() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness
        .enroller
        .rate_limit_forever(Some(Duration::from_secs(31)));
    let mut settings = harness.settings();
    settings.worker.enrollment_deadline = NonZeroDuration::from_secs(30);
    let result = gang_worker::run(
        harness.parts(true),
        settings,
        harness.build_limits(),
        future::pending(),
    )
    .await;
    assert!(matches!(result, Err(WorkerError::Enroll(_))));
    assert_eq!(harness.elapsed(), Duration::ZERO);
    assert_eq!(harness.enroller.calls(), 1);
}

#[tokio::test]
async fn already_expired_certificate_is_rejected() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    let not_after = Timestamp::from_unix_millis(0);
    harness.enroller.set_not_after(not_after);
    let result = gang_worker::run(
        harness.parts(true),
        harness.settings(),
        harness.build_limits(),
        future::pending(),
    )
    .await;
    assert!(
        matches!(result, Err(WorkerError::ExpiredCertificate { not_after: actual }) if actual == not_after)
    );
    assert_eq!(harness.enroller.calls(), 1);
    assert!(harness.scheduler.specs().is_empty());
}

#[tokio::test]
async fn ordinary_enrollment_failure_is_fatal_after_one_attempt() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.enroller.fail_next(1);
    let (result, elapsed) = harness
        .drive(gang_worker::run(
            harness.parts(true),
            harness.settings(),
            harness.build_limits(),
            future::pending(),
        ))
        .await;
    let error = result.expect_err("an ordinary enrollment failure is fatal");
    assert!(matches!(error, WorkerError::Enroll(_)));
    assert_eq!(
        harness.enroller.calls(),
        1,
        "no retry of an ordinary refusal"
    );
    assert_eq!(elapsed, Duration::ZERO);
    assert!(harness.scheduler.specs().is_empty());
}

#[tokio::test]
async fn shutdown_during_a_rate_limited_first_enrollment_ends_the_run() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    harness
        .enroller
        .rate_limit_forever(Some(Duration::from_secs(10)));
    let (tx, rx) = oneshot::channel::<()>();
    let run = harness.spawn(gang_worker::run(
        harness.parts(true),
        harness.settings(),
        harness.build_limits(),
        async move {
            rx.await.ok();
        },
    ));

    harness.run_until(|| harness.enroller.calls() >= 2).await;
    tx.send(()).expect("the run is still waiting for shutdown");
    let at = harness.run_until(|| run.is_finished()).await;
    assert_eq!(
        at,
        Duration::from_secs(10),
        "the run ends at the second attempt, with no further retry wait"
    );
    assert!(
        at < harness.settings().worker.enrollment_deadline.get(),
        "the run ends long before the enrollment deadline"
    );
    let result = run.await.expect("the run task does not panic");
    result.expect("shutdown ends the run with Ok");
    assert_eq!(
        harness.enroller.calls(),
        2,
        "no attempt follows the shutdown"
    );
    assert_eq!(harness.scheduler.worker_starts().get(), 0);
}

#[tokio::test]
async fn preemption_during_a_rate_limited_first_enrollment_ends_the_run() {
    let harness = Harness::new(Isa::X86_64, WorkerLifetime::LongLived);
    harness.scheduler.set_behaviour(Behaviour::Pending);
    harness
        .enroller
        .rate_limit_forever(Some(Duration::from_secs(10)));
    // Terminated at 45 s with the default 10 s margin: the drain starts at 35 s.
    harness
        .provisioner
        .set_preemption_notice(Some(PreemptionNotice {
            terminate_at: Timestamp::from_unix_millis(45_000),
        }));
    let run = harness.spawn(gang_worker::run(
        harness.parts(true),
        harness.settings(),
        harness.build_limits(),
        future::pending(),
    ));

    let at = harness.run_until(|| run.is_finished()).await;
    assert_eq!(
        at,
        Duration::from_secs(35),
        "the drain starts at the notice's terminate_at minus the preemption margin"
    );
    assert!(
        at < harness.settings().worker.enrollment_deadline.get(),
        "the run ends long before the enrollment deadline"
    );
    let result = run.await.expect("the run task does not panic");
    result.expect("preemption ends the run with Ok");
    assert_eq!(
        harness.enroller.calls(),
        4,
        "attempts at 0, 10, 20 and 30 s"
    );
    assert_eq!(harness.scheduler.worker_starts().get(), 0);
    assert!(harness.scheduler.specs().is_empty());
}
