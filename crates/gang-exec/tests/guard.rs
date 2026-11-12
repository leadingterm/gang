//! The guardian waits for its creator's exit and reaps only its scopes.

use gang_exec::{ExecError, guard};
use gang_testkit::{CallLog, LimitsStep, RecordingLimits, ready_now};

#[tokio::test]
async fn reaps_once_after_the_creator_exits() {
    let log = CallLog::default();
    let limits = RecordingLimits::new(&log);
    let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
    let exited = async move { receiver.await.map_err(std::io::Error::other) };
    let future = guard(&limits, 42, exited);
    tokio::pin!(future);
    assert!(ready_now(future.as_mut()).is_none());
    assert!(limits.reaped().is_empty());
    assert!(sender.send(()).is_ok());
    assert!(future.await.is_ok());
    assert_eq!(limits.reaped(), [42]);
}

#[tokio::test]
async fn reports_reap_failure() {
    let log = CallLog::default();
    let limits = RecordingLimits::new(&log).failing(LimitsStep::Reap);
    let exited = std::future::ready(Ok(()));
    assert!(matches!(
        guard(&limits, 42, exited).await,
        Err(ExecError::Reap(_))
    ));
    assert_eq!(limits.reaped(), [42]);
}

#[tokio::test]
async fn a_failed_watch_reaps_nothing() {
    let log = CallLog::default();
    let limits = RecordingLimits::new(&log);
    let exited = std::future::ready(Err(std::io::Error::other("lost")));
    assert!(matches!(
        guard(&limits, 42, exited).await,
        Err(ExecError::Watch(_))
    ));
    assert!(limits.reaped().is_empty());
}
