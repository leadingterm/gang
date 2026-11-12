#![expect(
    unsafe_code,
    reason = "libc's sigprocmask and mkfifo have no safe wrapper in our dependencies (rustix exposes neither on every platform); signal masks are the only signal state that survives exec into the sandbox"
)]

use std::ffi::CStr;
use std::io;
use std::mem::MaybeUninit;
use std::ptr;

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

pub(crate) fn mkfifo(path: &CStr, mode: libc::mode_t) -> io::Result<()> {
    // SAFETY: path is a valid NUL-terminated C string and mode is passed by value.
    if unsafe { libc::mkfifo(path.as_ptr(), mode) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn contains_signal(set: &libc::sigset_t, signal: libc::c_int) -> bool {
    // SAFETY: set points to a live initialized signal set; signal is passed by value.
    unsafe { libc::sigismember(ptr::from_ref(set), signal) == 1 }
}
