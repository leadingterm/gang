//! The client lock: serializes the client operations that rewrite the identity files, across
//! processes. It is an exclusive `flock` on the lock file in the client directory.

use std::io;

use gang_os::{FileLock, PathIo};

use crate::ClientError;
use crate::home::ClientHome;

/// Holds the client lock. The lock is released when this value is dropped.
#[derive(Debug)]
pub(crate) struct ClientLock {
    _lock: FileLock,
}

impl ClientHome {
    /// Takes the client lock, creating the client directory and the lock file when absent.
    ///
    /// Serializes the client operations that rewrite the identity files, across processes:
    /// waits while another process holds the lock. The lock file is locked on a blocking
    /// thread, so the wait does not stall the async runtime.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Io`] when the directory or the lock file cannot be created or
    /// locked.
    pub(crate) async fn lock(&self) -> Result<ClientLock, ClientError> {
        self.create_dir()?;
        let path = self.lock_file();
        let blocking = path.clone();
        let lock = tokio::task::spawn_blocking(move || FileLock::exclusive_blocking(&blocking))
            .await
            .map_err(|join| PathIo::new("lock", &path, io::Error::other(join)))??;
        Ok(ClientLock { _lock: lock })
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, poll_fn};
    use std::pin::pin;
    use std::task::Poll;

    use tempfile::TempDir;

    use crate::test_support::{home_in, mode};

    #[tokio::test]
    async fn lock_creates_the_directory_and_the_lock_file() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);
        let lock = home.lock().await.expect("lock is taken");
        assert_eq!(mode(home.dir()), 0o700);
        assert_eq!(mode(&home.lock_file()), 0o600);
        drop(lock);
    }

    #[tokio::test]
    async fn lock_can_be_taken_again_after_the_first_is_dropped() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);
        let first = home.lock().await.expect("first lock is taken");
        drop(first);
        let second = home.lock().await.expect("second lock is taken");
        drop(second);
    }

    #[tokio::test]
    async fn second_lock_waits_while_the_first_is_held() {
        let tmp = TempDir::new().expect("temp dir");
        let home = home_in(&tmp);
        let first = home.lock().await.expect("first lock is taken");
        let mut second = pin!(home.lock());
        // The acquisition blocks on the file lock, so one poll must come back pending. The
        // blocking thread cannot finish while `first` is held, so this does not depend on timing.
        let polled = poll_fn(|cx| Poll::Ready(second.as_mut().poll(cx))).await;
        assert!(
            polled.is_pending(),
            "second lock must wait while the first is held"
        );
        drop(first);
        // Completion is driven by the blocking thread acquiring the released lock, not by a timer.
        second
            .await
            .expect("second lock is taken once the first is dropped");
    }
}
