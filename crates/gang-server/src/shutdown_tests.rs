//! Shutdown of the composed server: a controller that ends before shutdown was asked for is an
//! error, not a clean stop.

use std::future::pending;
use std::pin::pin;

use gang_spi::ControllerReady;
use tokio::sync::oneshot;

use crate::ServerError;

use crate::test_support::{plain_parts, plain_settings, prepare_plain};

#[tokio::test]
async fn a_controller_that_returns_ok_before_shutdown_is_an_error() {
    let (parts, _dials, handles) = plain_parts();
    handles.scheduler.finish_controller(true);
    let server = prepare_plain(parts).await;
    let result = server
        .run(pending::<()>(), ControllerReady::channel().0)
        .await;
    assert!(
        matches!(result, Err(ServerError::ControllerEnded)),
        "a controller that ends without shutdown is reported: {result:?}"
    );
}

/// The server signals readiness once its controller is serving.
#[tokio::test]
async fn run_signals_ready_once_the_controller_serves() {
    let (parts, _dials, _handles) = plain_parts();
    let server = prepare_plain(parts).await;
    let (stop, stopped) = oneshot::channel::<()>();
    let (ready, readiness) = ControllerReady::channel();
    let mut run = pin!(server.run(
        async {
            let _ = stopped.await;
        },
        ready,
    ));

    tokio::select! {
        biased;
        result = &mut run => panic!("the server ended before readiness: {result:?}"),
        result = readiness => assert!(result.is_ok(), "the controller signals readiness"),
    }
    stop.send(()).expect("the server is running");
    let result = run.await;
    assert!(result.is_ok(), "run returns Ok after shutdown: {result:?}");
}

/// A controller that fails before serving drops readiness unsent.
#[tokio::test]
async fn run_whose_controller_fails_never_signals_ready() {
    let (parts, _dials, handles) = plain_parts();
    handles.scheduler.fail_controller(true);
    let server = prepare_plain(parts).await;
    let (ready, readiness) = ControllerReady::channel();
    let result = server.run(pending::<()>(), ready).await;
    assert!(
        matches!(result, Err(ServerError::Controller(_))),
        "controller failure is reported: {result:?}"
    );
    assert!(readiness.await.is_err(), "readiness is dropped unsent");
}

/// The control loop and the dashboard stop before the controller drains its connections, so no
/// pass starts while it drains.
#[tokio::test]
async fn no_control_pass_starts_while_the_controller_drains() {
    let (parts, _dials, handles) = plain_parts();
    let poll_interval = plain_settings().poll_interval.get();
    let release = handles.scheduler.hold_drain();
    let snapshots = handles.scheduler.snapshots();
    let drains = handles.scheduler.drains();
    let server = prepare_plain(parts).await;
    let (stop, stopped) = oneshot::channel::<()>();
    let mut run = pin!(server.run(
        async {
            let _ = stopped.await;
        },
        ControllerReady::channel().0,
    ));

    tokio::select! {
        biased;
        result = &mut run => panic!("the server ended before its first pass: {result:?}"),
        () = snapshots.reached(1) => {}
    }
    stop.send(()).expect("the server is running");
    tokio::select! {
        biased;
        result = &mut run => panic!("the server ended before the controller drained: {result:?}"),
        () = drains.reached(1) => {}
    }
    let passes = snapshots.get();

    handles.clock.advance(poll_interval);
    // A control loop still alive would run its next pass on this poll.
    assert!(
        futures::poll!(run.as_mut()).is_pending(),
        "the controller still drains"
    );
    assert_eq!(
        snapshots.get(),
        passes,
        "no control pass starts while the controller drains"
    );

    release.send(()).expect("the controller waits on the hold");
    let result = run.await;
    assert!(result.is_ok(), "run returns Ok after shutdown: {result:?}");
}
