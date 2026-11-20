//! Action-local access to the harness's shared probe inodes.

#[cfg(not(target_os = "linux"))]
use std::path::Path;

#[cfg(not(target_os = "linux"))]
use crate::{E2eError, Result};

#[cfg(target_os = "linux")]
mod linux {
    use std::collections::{HashMap, HashSet};
    use std::fs::{self, File};
    use std::io::{self, Write};
    use std::mem::MaybeUninit;
    use std::os::fd::{AsRawFd, OwnedFd};
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    use std::path::Path;
    use std::sync::Arc;
    use std::thread::{self, JoinHandle};

    use parking_lot::Mutex;
    use rustix::event::{PollFd, PollFlags, poll};
    use rustix::fs::inotify::{self, CreateFlags, ReadFlags, Reader, WatchFlags};
    use rustix::fs::{AtFlags, Mode, OFlags, linkat, open, openat};
    use rustix::io::Errno;

    use crate::{E2eError, Result};

    type Identity = (u64, u64);

    pub(crate) struct ActionIpc {
        stop: Option<io::PipeWriter>,
        thread: Option<JoinHandle<()>>,
        failure: Arc<Mutex<Option<String>>>,
    }

    impl ActionIpc {
        pub(crate) fn start(instances_root: &Path, shared_dir: &Path, salt: &str) -> Result<Self> {
            if !hex_id(salt) {
                return Err(E2eError::system("invalid action IPC salt"));
            }
            let directory = |path: &Path| {
                open(
                    path,
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                )
                .map_err(|error| {
                    E2eError::io(format!("opening action IPC directory {}", path.display()))(
                        error.into(),
                    )
                })
            };
            let root = directory(instances_root)?;
            let shared = directory(shared_dir)?;
            let inotify = inotify::init(CreateFlags::CLOEXEC | CreateFlags::NONBLOCK)
                .map_err(|error| E2eError::io("opening action IPC inotify")(error.into()))?;
            let mut watcher = Watcher {
                root,
                shared,
                inotify,
                salt: salt.to_owned(),
                watches: HashSet::new(),
                attempts: HashMap::new(),
            };
            // Every directory is watched before its entries are inspected.
            watcher.scan()?;
            let (stop_read, stop_write) =
                io::pipe().map_err(E2eError::io("creating action IPC shutdown pipe"))?;
            let failure = Arc::new(Mutex::new(None));
            let thread_failure = Arc::clone(&failure);
            let thread = thread::Builder::new()
                .name("action-ipc".into())
                .spawn(move || {
                    if let Err(error) = watcher.run(&stop_read) {
                        *thread_failure.lock() = Some(format!("{error:?}"));
                    }
                })
                .map_err(E2eError::io("starting action IPC watcher"))?;
            Ok(Self {
                stop: Some(stop_write),
                thread: Some(thread),
                failure,
            })
        }

        pub(crate) fn check(&self) -> Result<()> {
            if let Some(error) = self.failure.lock().as_ref() {
                return Err(E2eError::system(format!(
                    "action IPC watcher failed: {error}"
                )));
            }
            if self.thread.as_ref().is_some_and(JoinHandle::is_finished) {
                return Err(E2eError::system("action IPC watcher stopped unexpectedly"));
            }
            Ok(())
        }

        pub(crate) fn finish(mut self) -> Result<()> {
            self.stop_and_join()?;
            if let Some(error) = self.failure.lock().as_ref() {
                return Err(E2eError::system(format!(
                    "action IPC watcher failed: {error}"
                )));
            }
            Ok(())
        }

        fn stop_and_join(&mut self) -> Result<()> {
            // Closing the only writer wakes poll even if the watcher has already failed.
            self.stop.take();
            if let Some(thread) = self.thread.take() {
                thread
                    .join()
                    .map_err(|_| E2eError::system("action IPC watcher panicked"))?;
            }
            Ok(())
        }
    }

    impl Drop for ActionIpc {
        fn drop(&mut self) {
            let _ = self.stop_and_join();
        }
    }

    fn hex_id(value: &str) -> bool {
        value.len() == 16
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }

    fn marker<'a>(name: &'a str, salt: &str) -> Option<(&'a str, &'a str)> {
        let rest = name
            .strip_prefix(".gang-ipc-")?
            .strip_prefix(salt)?
            .strip_prefix('-')?;
        let (run, action) = rest.split_once('-')?;
        if !(run == "none" || hex_id(run))
            || action.is_empty()
            || !action
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
        {
            return None;
        }
        Some((run, action))
    }

    struct Watcher {
        root: OwnedFd,
        shared: OwnedFd,
        inotify: OwnedFd,
        salt: String,
        watches: HashSet<i32>,
        // The directory prevents inode reuse; the writer buffers the pre-reader acknowledgement.
        attempts: HashMap<Identity, (OwnedFd, File)>,
    }

    impl Watcher {
        fn run(&mut self, stop: &io::PipeReader) -> Result<()> {
            let mut buffer = [MaybeUninit::uninit(); 8192];
            loop {
                let mut fds = [
                    PollFd::new(&self.inotify, PollFlags::IN),
                    PollFd::new(stop, PollFlags::IN),
                ];
                match poll(&mut fds, None) {
                    Ok(_) => {}
                    Err(Errno::INTR) => continue,
                    Err(error) => {
                        return Err(E2eError::io("waiting for action IPC events")(error.into()));
                    }
                }
                if !fds[1].revents().is_empty() {
                    return Ok(());
                }
                if fds[0]
                    .revents()
                    .intersects(PollFlags::ERR | PollFlags::HUP | PollFlags::NVAL)
                {
                    return Err(E2eError::system("action IPC inotify stopped"));
                }
                let mut reader = Reader::new(&self.inotify, &mut buffer);
                let mut changed = false;
                loop {
                    match reader.next() {
                        Ok(event) => {
                            let flags = event.events();
                            if flags.contains(ReadFlags::QUEUE_OVERFLOW) {
                                return Err(E2eError::system("action IPC inotify queue overflow"));
                            }
                            if flags.intersects(
                                ReadFlags::CREATE
                                    | ReadFlags::DELETE
                                    | ReadFlags::MOVED_FROM
                                    | ReadFlags::MOVED_TO
                                    | ReadFlags::DELETE_SELF
                                    | ReadFlags::MOVE_SELF
                                    | ReadFlags::UNMOUNT,
                            ) {
                                changed = true;
                            }
                        }
                        Err(Errno::WOULDBLOCK) => break,
                        Err(Errno::INTR) => {}
                        Err(error) => {
                            return Err(E2eError::io("reading action IPC events")(error.into()));
                        }
                    }
                }
                if changed {
                    self.scan()?;
                }
            }
        }

        fn scan(&mut self) -> Result<()> {
            let root = self
                .root
                .try_clone()
                .map_err(E2eError::io("duplicating action IPC root"))?;
            let mut watches = HashSet::new();
            let mut attempts = HashSet::new();
            self.walk(&root, 0, &mut watches, &mut attempts)?;
            for wd in self.watches.difference(&watches) {
                match inotify::remove_watch(&self.inotify, *wd) {
                    Ok(()) | Err(Errno::INVAL) => {}
                    Err(error) => {
                        return Err(E2eError::io("removing action IPC watch")(error.into()));
                    }
                }
            }
            self.watches = watches;
            self.attempts.retain(|id, _| attempts.contains(id));
            Ok(())
        }

        fn walk(
            &mut self,
            dir: &OwnedFd,
            depth: usize,
            watches: &mut HashSet<i32>,
            attempts: &mut HashSet<Identity>,
        ) -> Result<()> {
            let fd_path = format!("/proc/self/fd/{}", dir.as_raw_fd());
            let flags = WatchFlags::CREATE
                | WatchFlags::DELETE
                | WatchFlags::MOVED_FROM
                | WatchFlags::MOVED_TO
                | WatchFlags::DELETE_SELF
                | WatchFlags::MOVE_SELF
                | WatchFlags::ONLYDIR;
            let wd = match inotify::add_watch(&self.inotify, fd_path.as_str(), flags) {
                Ok(wd) => wd,
                Err(Errno::NOENT) if gone(dir)? => return Ok(()),
                Err(error) => {
                    return Err(E2eError::io("watching action IPC directory")(error.into()));
                }
            };
            watches.insert(wd);
            let entries = match fs::read_dir(&fd_path) {
                Ok(entries) => entries,
                Err(error) if error.kind() == io::ErrorKind::NotFound && gone(dir)? => {
                    return Ok(());
                }
                Err(error) => return Err(E2eError::io("reading action IPC directory")(error)),
            };
            for entry in entries {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(_) if gone(dir)? => return Ok(()),
                    Err(error) => {
                        return Err(E2eError::io("reading action IPC directory entry")(error));
                    }
                };
                let name = entry.file_name();
                let Some(name_str) = name.to_str() else {
                    continue;
                };
                if (depth == 1 && name_str != "scratch")
                    || (depth == 2 && name_str != "work")
                    || (depth == 4 && name_str != "work")
                {
                    continue;
                }
                let parsed = if depth >= 5 {
                    marker(name_str, &self.salt).map(|(r, a)| (r.to_owned(), a.to_owned()))
                } else {
                    None
                };
                let child = match openat(
                    dir,
                    name.as_os_str(),
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                ) {
                    Ok(child) => child,
                    Err(Errno::NOENT | Errno::NOTDIR | Errno::LOOP) => continue,
                    Err(error) if gone(dir)? => {
                        let _ = error;
                        continue;
                    }
                    Err(error) => {
                        return Err(E2eError::io("opening action IPC child directory")(
                            error.into(),
                        ));
                    }
                };
                if let Some((run, action)) = parsed {
                    self.watch_marker(&child, &run, &action, watches, attempts)?;
                } else {
                    self.walk(&child, depth + 1, watches, attempts)?;
                }
            }
            Ok(())
        }

        fn watch_marker(
            &mut self,
            dir: &OwnedFd,
            run: &str,
            action: &str,
            watches: &mut HashSet<i32>,
            attempts: &mut HashSet<Identity>,
        ) -> Result<()> {
            let path = format!("/proc/self/fd/{}", dir.as_raw_fd());
            let wd = match inotify::add_watch(
                &self.inotify,
                path.as_str(),
                WatchFlags::CREATE
                    | WatchFlags::DELETE
                    | WatchFlags::MOVED_TO
                    | WatchFlags::MOVED_FROM
                    | WatchFlags::DELETE_SELF
                    | WatchFlags::MOVE_SELF
                    | WatchFlags::ONLYDIR,
            ) {
                Ok(wd) => wd,
                Err(Errno::NOENT) if gone(dir)? => return Ok(()),
                Err(error) => return Err(E2eError::io("watching action IPC marker")(error.into())),
            };
            watches.insert(wd);
            let ack = match openat(
                dir,
                "ack",
                OFlags::RDWR | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            ) {
                Ok(fd) => File::from(fd),
                Err(Errno::NOENT) => return Ok(()),
                Err(error) if gone(dir)? => {
                    let _ = error;
                    return Ok(());
                }
                Err(error) => {
                    return Err(E2eError::io("opening action IPC acknowledgement")(
                        error.into(),
                    ));
                }
            };
            let metadata = ack
                .metadata()
                .map_err(E2eError::io("inspecting action IPC acknowledgement"))?;
            if !metadata.file_type().is_fifo() {
                return Err(E2eError::system("action IPC acknowledgement is not a FIFO"));
            }
            let id = identity(dir)?;
            attempts.insert(id);
            if self.attempts.contains_key(&id) {
                return Ok(());
            }
            for name in ["started", "attempts", "go", "churn"] {
                if !self.link(dir, name, false)? {
                    return Ok(());
                }
            }
            if !self.link(dir, "journal", true)? {
                return Ok(());
            }
            let lock = format!("{run}-{action}.lock");
            if !self.link(dir, &lock, true)? {
                return Ok(());
            }
            let mut ack = ack;
            match ack.write_all(b"ready\n") {
                Ok(()) => {
                    let directory = dir
                        .try_clone()
                        .map_err(E2eError::io("retaining action IPC directory"))?;
                    self.attempts.insert(id, (directory, ack));
                    Ok(())
                }
                Err(_) if gone(dir)? => Ok(()),
                Err(error) => Err(E2eError::io("acknowledging action IPC")(error)),
            }
        }

        // Returns false only if the destination action has genuinely disappeared.
        fn link(&self, dir: &OwnedFd, name: &str, optional: bool) -> Result<bool> {
            match linkat(&self.shared, name, dir, name, AtFlags::empty()) {
                Ok(()) => Ok(true),
                Err(Errno::NOENT) if gone(dir)? => Ok(false),
                Err(Errno::NOENT) if optional => Ok(true),
                Err(Errno::EXIST) => {
                    let source = format!("/proc/self/fd/{}/{}", self.shared.as_raw_fd(), name);
                    let target = format!("/proc/self/fd/{}/{}", dir.as_raw_fd(), name);
                    let source = fs::symlink_metadata(source)
                        .map_err(E2eError::io("inspecting shared action IPC inode"))?;
                    let target = fs::symlink_metadata(target)
                        .map_err(E2eError::io("inspecting linked action IPC inode"))?;
                    if source.dev() == target.dev()
                        && source.ino() == target.ino()
                        && !source.file_type().is_symlink()
                    {
                        Ok(true)
                    } else {
                        Err(E2eError::system(
                            "action IPC link does not match its shared inode",
                        ))
                    }
                }
                Err(Errno::XDEV) => Err(E2eError::system(
                    "action IPC hard link crosses filesystems (EXDEV)",
                )),
                Err(error) if gone(dir)? => {
                    let _ = error;
                    Ok(false)
                }
                Err(error) => Err(E2eError::io(format!("linking action IPC {name}"))(
                    error.into(),
                )),
            }
        }
    }

    fn identity(dir: &OwnedFd) -> Result<Identity> {
        let stat = rustix::fs::fstat(dir)
            .map_err(|error| E2eError::io("inspecting action IPC directory")(error.into()))?;
        Ok((stat.st_dev, stat.st_ino))
    }

    fn gone(dir: &OwnedFd) -> Result<bool> {
        let stat = rustix::fs::fstat(dir).map_err(|error| {
            E2eError::io("inspecting action IPC directory lifetime")(error.into())
        })?;
        Ok(stat.st_nlink == 0)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn marker_requires_exact_salt_run_and_safe_action() {
            let salt = "0123456789abcdef";
            assert_eq!(
                marker(".gang-ipc-0123456789abcdef-none-controller-loss_0", salt),
                Some(("none", "controller-loss_0"))
            );
            assert_eq!(
                marker(".gang-ipc-0123456789abcdef-fedcba9876543210-build", salt),
                Some(("fedcba9876543210", "build"))
            );
            for name in [
                ".gang-ipc-0123456789abcdee-none-build",
                ".gang-ipc-0123456789abcdef-bad-build",
                ".gang-ipc-0123456789abcdef-none-",
                ".gang-ipc-0123456789abcdef-none-../evil",
            ] {
                assert!(marker(name, salt).is_none());
            }
        }

        #[test]
        fn shutdown_wakes_idle_watcher() -> Result<()> {
            let temp = tempfile::tempdir().map_err(E2eError::io("creating test directory"))?;
            let instances = temp.path().join("instances");
            let shared = temp.path().join("shared");
            fs::create_dir(&instances).map_err(E2eError::io("creating instances"))?;
            fs::create_dir(&shared).map_err(E2eError::io("creating shared"))?;
            let watcher = ActionIpc::start(&instances, &shared, "0123456789abcdef")?;
            watcher.check()?;
            watcher.finish()
        }

        #[test]
        fn initial_marker_links_shared_inodes_before_acknowledgement() -> Result<()> {
            use rustix::fs::{CWD, mkfifoat};
            use std::io::Read;

            let temp = tempfile::tempdir().map_err(E2eError::io("creating test directory"))?;
            let instances = temp.path().join("instances");
            let shared = temp.path().join("shared");
            let marker_dir = instances.join("local-1/scratch/work/action-id/work/cwd/.gang-ipc-0123456789abcdef-fedcba9876543210-controller-loss_0");
            fs::create_dir_all(&marker_dir).map_err(E2eError::io("creating action"))?;
            fs::create_dir(&shared).map_err(E2eError::io("creating shared directory"))?;
            for name in [
                "started",
                "attempts",
                "go",
                "churn",
                "journal",
                "fedcba9876543210-controller-loss_0.lock",
            ] {
                fs::write(shared.join(name), b"shared")
                    .map_err(E2eError::io("creating shared inode"))?;
            }
            mkfifoat(CWD, marker_dir.join("ack"), Mode::RUSR | Mode::WUSR)
                .map_err(|error| E2eError::io("creating acknowledgement FIFO")(error.into()))?;
            let watcher = ActionIpc::start(&instances, &shared, "0123456789abcdef")?;
            let mut ack = File::from(
                open(
                    marker_dir.join("ack"),
                    OFlags::RDONLY | OFlags::NONBLOCK,
                    Mode::empty(),
                )
                .map_err(|error| E2eError::io("opening acknowledgement FIFO")(error.into()))?,
            );
            let mut ready = [0; 6];
            ack.read_exact(&mut ready)
                .map_err(E2eError::io("reading acknowledgement"))?;
            assert_eq!(&ready, b"ready\n");
            for name in [
                "started",
                "attempts",
                "go",
                "churn",
                "journal",
                "fedcba9876543210-controller-loss_0.lock",
            ] {
                let source =
                    fs::metadata(shared.join(name)).map_err(E2eError::io("inspecting source"))?;
                let linked =
                    fs::metadata(marker_dir.join(name)).map_err(E2eError::io("inspecting link"))?;
                assert_eq!((source.dev(), source.ino()), (linked.dev(), linked.ino()));
            }
            watcher.finish()
        }

        #[test]
        fn observes_new_action_and_buffers_ack_before_reader() -> Result<()> {
            use rustix::event::Timespec;
            use rustix::fs::{CWD, mkfifoat};
            use std::io::Read;

            let temp = tempfile::tempdir().map_err(E2eError::io("creating test directory"))?;
            let instances = temp.path().join("instances");
            let shared = temp.path().join("shared");
            fs::create_dir(&instances).map_err(E2eError::io("creating instances"))?;
            fs::create_dir(&shared).map_err(E2eError::io("creating shared"))?;
            for name in ["started", "attempts", "go", "churn"] {
                fs::write(shared.join(name), b"").map_err(E2eError::io("creating shared inode"))?;
            }
            let watcher = ActionIpc::start(&instances, &shared, "0123456789abcdef")?;
            let marker_dir = instances.join(
                "local-1/scratch/work/action-id/work/cwd/.gang-ipc-0123456789abcdef-none-build",
            );
            fs::create_dir_all(&marker_dir).map_err(E2eError::io("creating action"))?;
            let ack_path = marker_dir.join("ack");
            mkfifoat(CWD, &ack_path, Mode::RUSR | Mode::WUSR)
                .map_err(|error| E2eError::io("creating acknowledgement FIFO")(error.into()))?;
            let mut ack = File::from(
                open(&ack_path, OFlags::RDWR | OFlags::NONBLOCK, Mode::empty())
                    .map_err(|error| E2eError::io("opening acknowledgement FIFO")(error.into()))?,
            );
            let mut fds = [PollFd::new(&ack, PollFlags::IN)];
            let timeout = Timespec {
                tv_sec: 5,
                tv_nsec: 0,
            };
            let count = poll(&mut fds, Some(&timeout))
                .map_err(|error| E2eError::io("waiting for acknowledgement")(error.into()))?;
            assert_eq!(count, 1);
            let mut ready = [0; 6];
            ack.read_exact(&mut ready)
                .map_err(E2eError::io("reading acknowledgement"))?;
            assert_eq!(&ready, b"ready\n");
            let source =
                fs::metadata(shared.join("attempts")).map_err(E2eError::io("inspecting source"))?;
            let linked = fs::metadata(marker_dir.join("attempts"))
                .map_err(E2eError::io("inspecting link"))?;
            assert_eq!((source.dev(), source.ino()), (linked.dev(), linked.ino()));
            fs::remove_dir_all(instances.join("local-1"))
                .map_err(E2eError::io("cancelling action"))?;
            watcher.finish()
        }
    }
}

#[cfg(target_os = "linux")]
pub(crate) use linux::ActionIpc;

#[cfg(not(target_os = "linux"))]
pub(crate) struct ActionIpc;

#[cfg(not(target_os = "linux"))]
impl ActionIpc {
    pub(crate) fn start(_instances_root: &Path, _shared_dir: &Path, _salt: &str) -> Result<Self> {
        Err(E2eError::system("action IPC watching is Linux-only"))
    }

    pub(crate) fn check(&self) -> Result<()> {
        Err(E2eError::system("action IPC watching is Linux-only"))
    }

    pub(crate) fn finish(self) -> Result<()> {
        Err(E2eError::system("action IPC watching is Linux-only"))
    }
}
