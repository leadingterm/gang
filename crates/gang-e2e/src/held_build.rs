use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::thread::{Scope, ScopedJoinHandle};

use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::{BuildOutcome, E2eError, Result};

/// The first reason to abort a build's launcher wins.
pub(crate) struct Cancel {
    sender: Mutex<Option<oneshot::Sender<String>>>,
}

impl Cancel {
    pub(crate) fn pair() -> (Arc<Self>, oneshot::Receiver<String>) {
        let (sender, receiver) = oneshot::channel();
        (
            Arc::new(Self {
                sender: Mutex::new(Some(sender)),
            }),
            receiver,
        )
    }

    pub(crate) fn fire(&self, reason: &str) {
        let sender = self.sender.lock().take();
        if let Some(sender) = sender {
            let _ = sender.send(reason.to_owned());
        }
    }
}

struct State<'a> {
    cancels: Vec<(Arc<Cancel>, Option<&'a str>)>,
    cancelled: bool,
    released: bool,
    armed: bool,
}

/// Owns the release and cancellation of all builds started through it.
pub(crate) struct Hold<'a, K: Fn(Option<&str>) -> Result<()>> {
    go: &'a Path,
    kill: K,
    state: Mutex<State<'a>>,
}

impl<'a, K: Fn(Option<&str>) -> Result<()>> Hold<'a, K> {
    pub(crate) fn new(go: &'a Path, kill: K) -> Self {
        Self {
            go,
            kill,
            state: Mutex::new(State {
                cancels: Vec::new(),
                cancelled: false,
                released: false,
                armed: true,
            }),
        }
    }

    pub(crate) fn build<'scope, 'env, F>(
        &self,
        scope: &'scope Scope<'scope, 'env>,
        root: Option<&'a str>,
        build: F,
    ) -> ScopedJoinHandle<'scope, Result<BuildOutcome>>
    where
        F: FnOnce(&Cancel, oneshot::Receiver<String>) -> Result<BuildOutcome> + Send + 'scope,
    {
        let (cancel, stop) = Cancel::pair();
        let cancelled = {
            let mut state = self.state.lock();
            if state.cancelled {
                true
            } else {
                state.cancels.push((Arc::clone(&cancel), root));
                false
            }
        };
        if cancelled {
            cancel.fire("scenario already cancelled");
        }
        scope.spawn(move || build(&cancel, stop))
    }

    pub(crate) fn release(&self) -> Result<()> {
        let result = fs::write(self.go, b"1").map_err(E2eError::io("releasing held probes"));
        if result.is_err() {
            self.cancel("release failed");
        } else {
            self.state.lock().released = true;
        }
        result
    }

    pub(crate) fn is_held(&self) -> Result<bool> {
        let state = self.state.lock();
        let released = match fs::metadata(self.go) {
            Ok(metadata) => metadata.len() != 0,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(E2eError::io("observing held probe release")(error)),
        };
        Ok(!state.released && !state.cancelled && !released)
    }

    /// Cancels owned builds and their Bazel servers.
    pub(crate) fn cancel(&self, reason: &str) {
        let cancels = {
            let mut state = self.state.lock();
            if state.cancelled {
                return;
            }
            state.cancelled = true;
            std::mem::take(&mut state.cancels)
        };
        for (cancel, _) in &cancels {
            cancel.fire(reason);
        }
        for (_, root) in cancels {
            if let Err(error) = (self.kill)(root) {
                eprintln!("failed to kill held build Bazel servers: {error}");
            }
        }
    }

    pub(crate) fn settle<T>(
        &self,
        observed: Result<T>,
        drain: impl FnOnce() -> Result<()>,
    ) -> Result<T> {
        let released = self.release();
        if observed.is_err() {
            self.cancel("observation failed");
        }
        let drained = if observed.is_ok() && released.is_ok() {
            let result = drain();
            if result.is_err() {
                self.cancel("drain failed");
            }
            result
        } else {
            Ok(())
        };
        let value = observed?;
        released?;
        drained?;
        Ok(value)
    }

    pub(crate) fn disarm(self) {
        self.state.lock().armed = false;
    }
}

impl<K: Fn(Option<&str>) -> Result<()>> Drop for Hold<'_, K> {
    fn drop(&mut self) {
        let armed = self.state.lock().armed;
        if armed {
            self.cancel("scenario left without settling");
        }
    }
}

pub(crate) fn joined(build: ScopedJoinHandle<'_, Result<BuildOutcome>>) -> Result<BuildOutcome> {
    build
        .join()
        .map_err(|_| E2eError::system("a held build thread panicked"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::thread;

    fn checkpoint_build<'a, 'scope, K: Fn(Option<&str>) -> Result<()>>(
        hold: &Hold<'a, K>,
        scope: &'scope Scope<'scope, '_>,
        root: Option<&'a str>,
        sampled: Arc<AtomicBool>,
    ) -> (
        mpsc::Sender<()>,
        ScopedJoinHandle<'scope, Result<BuildOutcome>>,
    ) {
        let (send, receive) = mpsc::channel();
        let build = hold.build(scope, root, move |_, mut stop| {
            receive.recv().expect("checkpoint sent");
            sampled.store(stop.try_recv().is_ok(), Ordering::SeqCst);
            Ok(BuildOutcome::new(Some(0), String::new()))
        });
        (send, build)
    }

    #[test]
    fn failed_release_cancels_every_build_before_returning() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let go = dir.path().join("go");
        symlink(dir.path().join("missing/go"), &go).expect("dangling symlink");
        let roots = Mutex::new(Vec::new());
        let first = Arc::new(AtomicBool::new(false));
        let second = Arc::new(AtomicBool::new(false));
        thread::scope(|scope| {
            let hold = Hold::new(&go, |root| {
                roots.lock().push(root.map(str::to_owned));
                Ok(())
            });
            let (a, build_a) = checkpoint_build(&hold, scope, None, Arc::clone(&first));
            let (b, build_b) = checkpoint_build(&hold, scope, Some("x"), Arc::clone(&second));
            assert!(matches!(hold.release(), Err(E2eError::Io { .. })));
            a.send(()).expect("checkpoint");
            b.send(()).expect("checkpoint");
            joined(build_a).expect("first build");
            joined(build_b).expect("second build");
            hold.disarm();
        });
        assert!(first.load(Ordering::SeqCst));
        assert!(second.load(Ordering::SeqCst));
        assert_eq!(*roots.lock(), vec![None, Some("x".to_owned())]);
    }

    #[test]
    fn failed_observation_cancels_and_retains_the_observation_error() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let go = dir.path().join("go");
        let sampled = Arc::new(AtomicBool::new(false));
        let drained = AtomicBool::new(false);
        thread::scope(|scope| {
            let hold = Hold::new(&go, |_| Ok(()));
            let (checkpoint, build) = checkpoint_build(&hold, scope, None, Arc::clone(&sampled));
            let result = hold.settle::<()>(Err(E2eError::system("observation")), || {
                drained.store(true, Ordering::SeqCst);
                Ok(())
            });
            assert!(matches!(result, Err(E2eError::System { .. })));
            assert!(go.exists());
            checkpoint.send(()).expect("checkpoint");
            joined(build).expect("build");
            hold.disarm();
        });
        assert!(sampled.load(Ordering::SeqCst));
        assert!(!drained.load(Ordering::SeqCst));
    }

    #[test]
    fn observation_error_precedes_release_error() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let go = dir.path().join("go");
        symlink(dir.path().join("missing/go"), &go).expect("dangling symlink");
        let sampled = Arc::new(AtomicBool::new(false));
        let drained = AtomicBool::new(false);
        thread::scope(|scope| {
            let hold = Hold::new(&go, |_| Ok(()));
            let (checkpoint, build) = checkpoint_build(&hold, scope, None, Arc::clone(&sampled));
            let result = hold.settle::<()>(Err(E2eError::system("observation")), || {
                drained.store(true, Ordering::SeqCst);
                Ok(())
            });
            assert!(matches!(result, Err(E2eError::System { .. })));
            checkpoint.send(()).expect("checkpoint");
            joined(build).expect("build");
            hold.disarm();
        });
        assert!(sampled.load(Ordering::SeqCst));
        assert!(!drained.load(Ordering::SeqCst));
    }

    #[test]
    fn failed_drain_cancels_before_the_join() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let go = dir.path().join("go");
        let sampled = Arc::new(AtomicBool::new(false));
        thread::scope(|scope| {
            let hold = Hold::new(&go, |_| Ok(()));
            let (checkpoint, build) = checkpoint_build(&hold, scope, None, Arc::clone(&sampled));
            assert!(matches!(
                hold.settle(Ok(()), || Err(E2eError::system("drain"))),
                Err(E2eError::System { .. })
            ));
            checkpoint.send(()).expect("checkpoint");
            joined(build).expect("build");
            hold.disarm();
        });
        assert!(sampled.load(Ordering::SeqCst));
    }

    #[test]
    fn successful_settle_does_not_cancel() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let go = dir.path().join("go");
        let sampled = Arc::new(AtomicBool::new(false));
        let drained = AtomicUsize::new(0);
        let killed = AtomicUsize::new(0);
        thread::scope(|scope| {
            let hold = Hold::new(&go, |_| {
                killed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            });
            let (checkpoint, build) = checkpoint_build(&hold, scope, None, Arc::clone(&sampled));
            assert_eq!(
                hold.settle(Ok(42), || {
                    drained.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .expect("settled"),
                42
            );
            checkpoint.send(()).expect("checkpoint");
            joined(build).expect("build");
            hold.disarm();
        });
        assert!(!sampled.load(Ordering::SeqCst));
        assert_eq!(drained.load(Ordering::SeqCst), 1);
        assert_eq!(killed.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn dropping_an_armed_hold_cancels_every_build() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let go = dir.path().join("go");
        let roots = Mutex::new(Vec::new());
        let first = Arc::new(AtomicBool::new(false));
        let second = Arc::new(AtomicBool::new(false));
        thread::scope(|scope| {
            let hold = Hold::new(&go, |root| {
                roots.lock().push(root.map(str::to_owned));
                Ok(())
            });
            let (a, build_a) = checkpoint_build(&hold, scope, None, Arc::clone(&first));
            let (b, build_b) = checkpoint_build(&hold, scope, Some("x"), Arc::clone(&second));
            drop(hold);
            a.send(()).expect("checkpoint");
            b.send(()).expect("checkpoint");
            joined(build_a).expect("first build");
            joined(build_b).expect("second build");
        });
        assert!(first.load(Ordering::SeqCst));
        assert!(second.load(Ordering::SeqCst));
        assert_eq!(*roots.lock(), vec![None, Some("x".to_owned())]);
        let disarmed = Arc::new(AtomicBool::new(false));
        thread::scope(|scope| {
            let hold = Hold::new(&go, |root| {
                roots.lock().push(root.map(str::to_owned));
                Ok(())
            });
            let (checkpoint, build) = checkpoint_build(&hold, scope, None, Arc::clone(&disarmed));
            hold.disarm();
            checkpoint.send(()).expect("checkpoint");
            joined(build).expect("build");
        });
        assert!(!disarmed.load(Ordering::SeqCst));
        assert_eq!(roots.lock().len(), 2);
    }

    #[test]
    fn cancel_wakes_a_build_registered_after_the_cancel() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let go = dir.path().join("go");
        let sampled = Arc::new(AtomicBool::new(false));
        thread::scope(|scope| {
            let hold = Hold::new(&go, |_| Ok(()));
            hold.cancel("first");
            let (checkpoint, build) = checkpoint_build(&hold, scope, None, Arc::clone(&sampled));
            checkpoint.send(()).expect("checkpoint");
            joined(build).expect("build");
            hold.disarm();
        });
        assert!(sampled.load(Ordering::SeqCst));
    }

    #[test]
    fn first_cancellation_reason_wins() {
        let (cancel, mut receiver) = Cancel::pair();
        cancel.fire("first");
        cancel.fire("second");
        assert_eq!(receiver.try_recv().expect("cancellation"), "first");
    }

    #[test]
    fn held_observation_rejects_release_even_if_the_release_file_is_removed() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let go = dir.path().join("go");
        let hold = Hold::new(&go, |_| Ok(()));
        assert!(hold.is_held().expect("held state"));
        hold.release().expect("release");
        fs::remove_file(&go).expect("remove release file");
        assert!(!hold.is_held().expect("released state"));
        hold.disarm();
    }

    #[test]
    fn held_observation_rejects_cancellation_and_external_release() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let go = dir.path().join("go");
        let hold = Hold::new(&go, |_| Ok(()));
        hold.cancel("observation interrupted");
        assert!(!hold.is_held().expect("cancelled state"));
        hold.disarm();
        let hold = Hold::new(&go, |_| Ok(()));
        fs::write(&go, b"1").expect("external release");
        assert!(!hold.is_held().expect("external release state"));
        hold.disarm();
    }
}
