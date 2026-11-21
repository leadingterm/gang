#![expect(
    unsafe_code,
    reason = "signal masks, mkfifo, kqueue and physical memory queries require unsafe calls"
)]

use std::ffi::CStr;
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::OwnedFd;
use std::ptr;

/// Returns the operating system's reported physical memory in bytes.
///
/// # Errors
/// Returns an I/O error if the operating system query fails or its result is invalid.
#[cfg(target_os = "linux")]
pub fn physical_memory() -> io::Result<gang_types::Bytes> {
    let mut info = MaybeUninit::<libc::sysinfo>::uninit();
    // SAFETY: info points to writable storage for a sysinfo struct, initialized on success.
    if unsafe { libc::sysinfo(info.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful sysinfo call initialized the entire struct.
    let info = unsafe { info.assume_init() };
    let total = u64::try_from(u128::from(info.totalram))
        .ok()
        .and_then(|ram| ram.checked_mul(u64::from(info.mem_unit)))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "physical memory overflow"))?;
    Ok(gang_types::Bytes::new(total))
}

/// Returns the operating system's reported physical memory in bytes.
///
/// # Errors
/// Returns an I/O error if the operating system query fails or its result is invalid.
#[cfg(target_os = "macos")]
pub fn physical_memory() -> io::Result<gang_types::Bytes> {
    let mut total = 0_u64;
    let mut len = std::mem::size_of_val(&total);
    // SAFETY: the name is NUL-terminated, and total and len are valid writable buffers.
    if unsafe {
        libc::sysctlbyname(
            c"hw.memsize".as_ptr(),
            (&raw mut total).cast(),
            &raw mut len,
            ptr::null_mut(),
            0,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    if len != std::mem::size_of_val(&total) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid physical memory size",
        ));
    }
    Ok(gang_types::Bytes::new(total))
}

/// An asynchronously readable owned pipe. Dropping the reader closes its descriptor.
pub struct PipeReader {
    fd: tokio::io::unix::AsyncFd<OwnedFd>,
}

impl PipeReader {
    /// Configures and registers an owned pipe for asynchronous reads.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if configuring or registering the descriptor fails.
    pub fn new(fd: OwnedFd) -> io::Result<Self> {
        let flags = rustix::fs::fcntl_getfl(&fd)?;
        rustix::fs::fcntl_setfl(&fd, flags | rustix::fs::OFlags::NONBLOCK)?;
        // SAFETY: The owned descriptor remains open and unchanged until AsyncFd deregisters
        // on drop. It never escapes this type, and neither get_mut nor into_inner is used.
        let fd = unsafe { tokio::io::unix::AsyncFd::register(fd) }?;
        Ok(Self { fd })
    }

    /// Reads into `buffer` without blocking an executor thread.
    ///
    /// Returns the number of bytes read, or zero for an empty buffer or at EOF.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if waiting for readiness or reading the descriptor fails.
    pub async fn read(&self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        loop {
            let mut ready = match self.fd.readable().await {
                Ok(ready) => ready,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            };
            match ready
                .try_io(|fd| rustix::io::read(fd.get_ref(), &mut *buffer).map_err(io::Error::from))
            {
                Ok(Ok(count)) => return Ok(count),
                Ok(Err(error)) if error.kind() == io::ErrorKind::Interrupted => {}
                Ok(Err(error)) => return Err(error),
                Err(_) => {}
            }
        }
    }
}

pub(crate) fn thread_mask(
    how: libc::c_int,
    set: Option<&libc::sigset_t>,
) -> io::Result<libc::sigset_t> {
    let mut previous = MaybeUninit::<libc::sigset_t>::uninit();
    let set = set.map_or(ptr::null(), ptr::from_ref);
    // SAFETY: set is null or points to a live signal set, and previous is writable storage.
    let error = unsafe { libc::pthread_sigmask(how, set, previous.as_mut_ptr()) };
    if error != 0 {
        return Err(io::Error::from_raw_os_error(error));
    }
    // SAFETY: pthread_sigmask initialized the previous mask on success.
    Ok(unsafe { previous.assume_init() })
}

pub(crate) fn signal_set(signals: &[libc::c_int]) -> libc::sigset_t {
    let mut set = MaybeUninit::<libc::sigset_t>::uninit();
    // SAFETY: set points to writable storage for a signal set; sigemptyset initializes it.
    unsafe { libc::sigemptyset(set.as_mut_ptr()) };
    // SAFETY: sigemptyset initialized the signal set.
    let mut set = unsafe { set.assume_init() };
    for &signal in signals {
        // SAFETY: set is initialized and uniquely borrowed; signal is passed by value.
        unsafe { libc::sigaddset(&raw mut set, signal) };
    }
    set
}

pub(crate) fn mkfifo(path: &CStr, mode: rustix::fs::Mode) -> io::Result<()> {
    // SAFETY: path is a valid NUL-terminated C string and mode is passed by value.
    if unsafe { libc::mkfifo(path.as_ptr(), mode.bits()) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// An event queue watching one process for exit.
#[cfg(target_os = "macos")]
pub struct ProcessExitQueue(std::os::fd::OwnedFd);

#[cfg(target_os = "macos")]
impl ProcessExitQueue {
    /// Registers an exit filter before the process is signalled.
    ///
    /// # Errors
    ///
    /// Returns the kernel error if creating or registering the queue fails.
    pub fn new(pid: rustix::process::Pid) -> rustix::io::Result<Self> {
        use rustix::event::kqueue::{
            Event, EventFilter, EventFlags, ProcessEvents, kevent, kqueue,
        };

        let fd = kqueue()?;
        let change = Event::new(
            EventFilter::Proc {
                pid,
                flags: ProcessEvents::EXIT,
            },
            EventFlags::ADD,
            ptr::null_mut(),
        );
        // SAFETY: The process filter refers to a PID, not a file descriptor.
        unsafe {
            kevent(
                &fd,
                &[change],
                &mut [change; 0],
                Some(std::time::Duration::ZERO),
            )?
        };
        Ok(Self(fd))
    }

    /// Waits up to `timeout` for the registered process to exit.
    ///
    /// # Errors
    ///
    /// Returns the kernel error if waiting fails.
    pub fn wait(&self, timeout: std::time::Duration) -> rustix::io::Result<bool> {
        use rustix::event::kqueue::{Event, EventFlags, kevent_timespec};

        let deadline = std::time::Instant::now()
            .checked_add(timeout)
            .ok_or(rustix::io::Errno::INVAL)?;
        let mut events = [MaybeUninit::<Event>::uninit(); 1];
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let timeout = rustix::event::Timespec {
                tv_sec: i64::try_from(remaining.as_secs()).map_err(|_| rustix::io::Errno::INVAL)?,
                tv_nsec: remaining.subsec_nanos().into(),
            };
            // SAFETY: The queue contains only a process filter and no referenced file descriptors.
            let result = unsafe { kevent_timespec(&self.0, &[], &mut events, Some(&timeout)) };
            let events = match result {
                Err(rustix::io::Errno::INTR) => continue,
                Err(error) => return Err(error),
                Ok((events, _)) => events,
            };
            if let Some(event) = events.first() {
                if event.flags().contains(EventFlags::ERROR) {
                    return Err(rustix::io::Errno::from_raw_os_error(
                        i32::try_from(event.data()).unwrap_or(rustix::io::Errno::IO.raw_os_error()),
                    ));
                }
                return Ok(true);
            }
            return Ok(false);
        }
    }
}

#[cfg(test)]
pub(crate) fn contains_signal(set: &libc::sigset_t, signal: libc::c_int) -> bool {
    // SAFETY: set points to a live initialized signal set; signal is passed by value.
    unsafe { libc::sigismember(ptr::from_ref(set), signal) == 1 }
}
