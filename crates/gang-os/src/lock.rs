//! Exclusive advisory file locks.
//!
//! A lock is held for as long as its [`FileLock`] or a descriptor duplicated from it
//! (see [`FileLock::to_stdio`]) stays open. Child processes that inherit the descriptor keep
//! the lock too.

use crate::PathIo;
use crate::fs::PRIVATE_FILE;
use rustix::fs::{FlockOperation, flock};
use rustix::io::Errno;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::process::Stdio;

/// An exclusive advisory `flock` on a file, released on drop.
///
/// The lock belongs to the open file description, so every duplicate or inherited copy of the
/// descriptor shares it.
#[derive(Debug)]
#[must_use]
pub struct FileLock {
    file: File,
}

impl FileLock {
    /// Takes the exclusive lock on `path` without blocking. The file is created with mode 0600
    /// when it is missing. The parent directory is not created.
    ///
    /// Returns `Ok(None)` when another lock holder has the file.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be opened (action "open lock file") or the lock
    /// call fails for another reason (action "lock").
    pub fn try_exclusive(path: &Path) -> Result<Option<Self>, PathIo> {
        let file = open_lock_file(path)?;
        match flock(&file, FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => Ok(Some(Self { file })),
            Err(Errno::WOULDBLOCK) => Ok(None),
            Err(errno) => Err(PathIo::new("lock", path, errno.into())),
        }
    }

    /// Reports whether another descriptor holds an exclusive lock on `path`. A missing file
    /// is not held. The probe takes no lasting lock.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be opened for a reason other than being missing
    /// (action "open lock file") or the probe fails (action "lock").
    pub fn is_held(path: &Path) -> Result<bool, PathIo> {
        let file = match File::open(path) {
            Ok(file) => file,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(source) => return Err(PathIo::new("open lock file", path, source)),
        };
        match flock(&file, FlockOperation::NonBlockingLockShared) {
            Ok(()) => Ok(false),
            Err(Errno::WOULDBLOCK) => Ok(true),
            Err(errno) => Err(PathIo::new("lock", path, errno.into())),
        }
    }

    /// Takes the exclusive lock on `path`, blocking the calling thread until it is free. The
    /// file is created with mode 0600 when it is missing. The parent directory is not created.
    /// From async code, wrap the call in `tokio::task::spawn_blocking`.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be opened (action "open lock file") or the lock
    /// call fails (action "lock").
    pub fn exclusive_blocking(path: &Path) -> Result<Self, PathIo> {
        let file = open_lock_file(path)?;
        loop {
            match flock(&file, FlockOperation::LockExclusive) {
                Ok(()) => return Ok(Self { file }),
                Err(Errno::INTR) => {}
                Err(errno) => return Err(PathIo::new("lock", path, errno.into())),
            }
        }
    }

    /// Duplicates the descriptor as a [`Stdio`]. The lock stays held until both this lock and
    /// the returned value are dropped.
    ///
    /// # Errors
    ///
    /// Returns the error from duplicating the descriptor.
    pub fn to_stdio(&self) -> io::Result<Stdio> {
        Ok(Stdio::from(self.file.try_clone()?))
    }
}

impl From<FileLock> for Stdio {
    /// Hands the descriptor to a child process, so the lock lasts as long as the child does.
    fn from(lock: FileLock) -> Self {
        Self::from(lock.file)
    }
}

fn open_lock_file(path: &Path) -> Result<File, PathIo> {
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .mode(PRIVATE_FILE)
        .open(path)
        .map_err(|source| PathIo::new("open lock file", path, source))
}

#[cfg(test)]
mod tests {
    use super::FileLock;
    use crate::fs::PRIVATE_FILE;
    use gang_testkit::{ChildTest, TestResult, fork_gate};
    use std::io::{self, BufRead, BufReader, Read, Write};
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::process::Stdio;

    const CHILD_ENV: &str = "GANG_OS_LOCK_CHILD";
    const CHILD_PATH_ENV: &str = "GANG_OS_LOCK_PATH";
    const READY_MARKER: &str = "gang-os-lock-held";
    const CHILD_HOLDS: ChildTest = ChildTest::new("lock::tests::child_holds_until_stdin_closes");
    const CHILD_PARKS: ChildTest = ChildTest::new("lock::tests::child_parks");

    fn mode_of(path: &Path) -> io::Result<u32> {
        Ok(std::fs::metadata(path)?.permissions().mode() & 0o777)
    }

    #[test]
    fn try_exclusive_creates_private_file_and_excludes_second_holder() -> TestResult {
        let _guard = fork_gate::lock_user();
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("lock");
        let lock = FileLock::try_exclusive(&path)?.ok_or("first try_exclusive was busy")?;
        assert_eq!(mode_of(&path)?, PRIVATE_FILE);
        assert!(FileLock::try_exclusive(&path)?.is_none());
        drop(lock);
        assert!(FileLock::try_exclusive(&path)?.is_some());
        Ok(())
    }

    #[test]
    fn try_exclusive_in_missing_directory_is_not_found() -> TestResult {
        let _guard = fork_gate::lock_user();
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("missing").join("lock");
        let err = FileLock::try_exclusive(&path).expect_err("parent does not exist");
        assert!(err.is_not_found());
        assert_eq!(err.action, "open lock file");
        assert_eq!(err.path, path);
        Ok(())
    }

    #[test]
    fn is_held_tracks_the_lock() -> TestResult {
        let _guard = fork_gate::lock_user();
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("lock");
        assert!(!FileLock::is_held(&path)?);
        let lock = FileLock::try_exclusive(&path)?.ok_or("try_exclusive was busy")?;
        assert!(FileLock::is_held(&path)?);
        drop(lock);
        assert!(!FileLock::is_held(&path)?);
        Ok(())
    }

    #[test]
    fn exclusive_blocking_succeeds_when_free() -> TestResult {
        let _guard = fork_gate::lock_user();
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("lock");
        let lock = FileLock::exclusive_blocking(&path)?;
        assert!(FileLock::is_held(&path)?);
        drop(lock);
        Ok(())
    }

    #[test]
    fn exclusive_blocking_waits_until_the_child_releases() -> TestResult {
        let _spawn = fork_gate::spawner();
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("lock");
        let mut child = CHILD_HOLDS
            .command()?
            .env(CHILD_ENV, "hold")
            .env(CHILD_PATH_ENV, &path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let stdin = child.stdin.take().ok_or("child stdin is missing")?;
        let mut stdout = BufReader::new(child.stdout.take().ok_or("child stdout is missing")?);
        let mut line = String::new();
        loop {
            line.clear();
            if stdout.read_line(&mut line)? == 0 {
                return Err("child exited before it took the lock".into());
            }
            if line.contains(READY_MARKER) {
                break;
            }
        }
        assert!(FileLock::is_held(&path)?);
        assert!(FileLock::try_exclusive(&path)?.is_none());
        let (entering, entered) = std::sync::mpsc::channel();
        let waiter_path = path.clone();
        type WaiterResult = Result<FileLock, Box<dyn std::error::Error + Send + Sync>>;
        let waiter = std::thread::spawn(move || -> WaiterResult {
            // While the child holds the lock, a non-blocking attempt from this thread is busy.
            let busy = FileLock::try_exclusive(&waiter_path)?.is_none();
            entering
                .send(busy)
                .map_err(|_| "the test stopped listening")?;
            FileLock::exclusive_blocking(&waiter_path).map_err(Into::into)
        });
        assert!(
            entered.recv()?,
            "the lock was free before the child released it"
        );
        // Release only after the waiter has confirmed the lock is busy and is entering the wait.
        drop(stdin);
        let mut remaining_stdout = String::new();
        stdout.read_to_string(&mut remaining_stdout)?;
        assert!(child.wait()?.success());
        assert!(
            CHILD_HOLDS.completed_in(&remaining_stdout),
            "child did not complete; remaining stdout: {remaining_stdout}"
        );
        let _acquired = waiter
            .join()
            .map_err(|_| "waiter thread panicked")?
            .map_err(|e| -> Box<dyn std::error::Error> { e })?;
        drop(stdout);
        Ok(())
    }

    // Helper for `exclusive_blocking_waits_until_the_child_releases`: it runs only in the child
    // process. It takes the lock, announces it on stdout, and holds it until stdin reaches EOF.
    #[test]
    #[ignore = "child process helper of the lock tests"]
    fn child_holds_until_stdin_closes() -> TestResult {
        if std::env::var(CHILD_ENV).as_deref() != Ok("hold") {
            return Ok(());
        }
        let path = std::env::var_os(CHILD_PATH_ENV).ok_or("child lock path is missing")?;
        let _lock =
            FileLock::try_exclusive(Path::new(&path))?.ok_or("child could not take the lock")?;
        println!("{READY_MARKER}");
        io::stdout().flush()?;
        let mut rest = Vec::new();
        io::stdin().read_to_end(&mut rest)?;
        CHILD_HOLDS.complete();
        Ok(())
    }

    #[test]
    fn to_stdio_duplicate_keeps_lock_until_dropped() -> TestResult {
        let _guard = fork_gate::lock_user();
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("lock");
        let lock = FileLock::try_exclusive(&path)?.ok_or("try_exclusive was busy")?;
        let stdio = lock.to_stdio()?;
        drop(lock);
        assert!(FileLock::is_held(&path)?);
        drop(stdio);
        assert!(!FileLock::is_held(&path)?);
        Ok(())
    }

    // Helper for `lock_is_inherited_by_child`: it runs only in the child process and parks
    // until the parent kills it.
    #[test]
    #[ignore = "child process helper of the lock tests"]
    fn child_parks() -> TestResult {
        if std::env::var(CHILD_ENV).as_deref() != Ok("park") {
            return Ok(());
        }
        loop {
            std::thread::park();
        }
    }

    #[test]
    fn lock_is_inherited_by_child() -> TestResult {
        // The spawn must not overlap any lock user: a forked child shares the parent's
        // descriptions until exec.
        let _spawn = fork_gate::spawner();
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("lock");
        let lock = FileLock::try_exclusive(&path)?.ok_or("try_exclusive was busy")?;
        let mut child = CHILD_PARKS
            .command()?
            .env(CHILD_ENV, "park")
            .stdin(Stdio::from(lock))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        assert!(FileLock::is_held(&path)?);
        child.kill()?;
        child.wait()?;
        assert!(!FileLock::is_held(&path)?);
        Ok(())
    }
}
