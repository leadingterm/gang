//! Positive execution gate and byte-preserving environment transfer.

use std::ffi::OsString;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};

/// The parent's control endpoint. Dropping it aborts a waiting child.
pub struct ExecutionGateParent(OwnedFd);
/// The child's control endpoint.
pub struct ExecutionGateChild(OwnedFd);
/// The parent's asynchronously readable control endpoint.
pub struct AsyncExecutionGate(tokio::io::unix::AsyncFd<ExecutionGateParent>);
/// Descriptors pinned by the child at its ready point.
pub struct ExecutionReady {
    /// A pidfd for the ready process.
    pub process: OwnedFd,
    /// Its network namespace.
    pub network_namespace: OwnedFd,
}

macro_rules! fd_traits {
    ($type:ty) => {
        impl AsFd for $type {
            fn as_fd(&self) -> BorrowedFd<'_> {
                self.0.as_fd()
            }
        }
        impl AsRawFd for $type {
            fn as_raw_fd(&self) -> RawFd {
                self.0.as_raw_fd()
            }
        }
    };
}
fd_traits!(ExecutionGateParent);
fd_traits!(ExecutionGateChild);

#[cfg(not(target_os = "linux"))]
fn unsupported() -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, "execution gates require Linux")
}

/// Creates a close-on-exec sequenced-packet control pair.
///
/// # Errors
/// Returns an error when the kernel cannot create the pair or the platform is unsupported.
pub fn execution_gate_pair() -> io::Result<(ExecutionGateParent, ExecutionGateChild)> {
    #[cfg(target_os = "linux")]
    {
        use rustix::net::{AddressFamily, SocketFlags, SocketType, socketpair};
        let (a, b) = socketpair(
            AddressFamily::UNIX,
            SocketType::SEQPACKET,
            SocketFlags::CLOEXEC,
            None,
        )?;
        Ok((ExecutionGateParent(a), ExecutionGateChild(b)))
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(unsupported())
    }
}

impl ExecutionGateParent {
    /// Configures the gate for nonblocking reads and registers it with the current Tokio reactor.
    ///
    /// # Errors
    /// Returns an error if configuring or registering the descriptor fails.
    pub fn into_async(self) -> io::Result<AsyncExecutionGate> {
        self.set_nonblocking()?;
        Ok(AsyncExecutionGate(crate::sys::register_execution_gate(
            self,
        )?))
    }

    /// Enables nonblocking reads for integration with an event loop.
    ///
    /// # Errors
    /// Returns an error if the descriptor cannot be configured.
    pub fn set_nonblocking(&self) -> io::Result<()> {
        let flags = rustix::fs::fcntl_getfl(&self.0)?;
        rustix::fs::fcntl_setfl(&self.0, flags | rustix::fs::OFlags::NONBLOCK)?;
        Ok(())
    }
    /// Receives exactly the ready byte and the child's two pinned descriptors.
    ///
    /// # Errors
    /// Returns `WouldBlock` if not ready, or an error on a malformed or closed channel.
    pub fn try_ready(&self) -> io::Result<ExecutionReady> {
        #[cfg(target_os = "linux")]
        {
            let [process, network_namespace] = receive::<2>(&self.0, b'R')?;
            Ok(ExecutionReady {
                process,
                network_namespace,
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(unsupported())
        }
    }
    /// Positively releases the child with exactly one environment descriptor.
    ///
    /// # Errors
    /// Returns an error if the release packet cannot be sent.
    pub fn release(self, environment: BorrowedFd<'_>) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            send(&self.0, b'G', &[environment])
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = environment;
            Err(unsupported())
        }
    }
}

impl AsyncExecutionGate {
    /// Waits for the child's ready packet and its pinned descriptors.
    ///
    /// # Errors
    /// Returns an error if readiness fails, the peer closes, or the packet is invalid.
    pub async fn ready(&self) -> io::Result<ExecutionReady> {
        loop {
            let mut ready = match self.0.readable().await {
                Ok(ready) => ready,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            };
            match ready.try_io(|fd| fd.get_ref().try_ready()) {
                Ok(Ok(descriptors)) => return Ok(descriptors),
                Ok(Err(error)) if error.kind() == io::ErrorKind::Interrupted => {}
                Ok(Err(error)) => return Err(error),
                Err(_) => {}
            }
        }
    }

    /// Duplicates the gate descriptor with close-on-exec set.
    ///
    /// The duplicate keeps the child channel open independently of this endpoint.
    ///
    /// # Errors
    /// Returns an error if the descriptor cannot be duplicated.
    pub fn duplicate(&self) -> io::Result<OwnedFd> {
        Ok(rustix::io::fcntl_dupfd_cloexec(self.0.get_ref(), 0)?)
    }

    /// Positively releases the child with an environment descriptor.
    ///
    /// # Errors
    /// Returns an error if the release packet cannot be sent.
    pub fn release(self, environment: BorrowedFd<'_>) -> io::Result<()> {
        self.0.into_inner().release(environment)
    }
}

impl ExecutionGateChild {
    /// Duplicates an inherited descriptor and marks the original close-on-exec.
    ///
    /// # Errors
    /// Returns an error for an invalid descriptor or an unsupported platform.
    pub fn from_inherited_fd(fd: RawFd) -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            Ok(Self(crate::duplicate_inherited_fd(fd)?))
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = fd;
            Err(unsupported())
        }
    }
    /// Pins this process and its network namespace, then waits for positive release.
    ///
    /// # Errors
    /// Returns an error on any kernel or protocol failure.
    pub fn wait(self) -> io::Result<OwnedFd> {
        #[cfg(target_os = "linux")]
        {
            let process = rustix::process::pidfd_open(
                rustix::process::getpid(),
                rustix::process::PidfdFlags::empty(),
            )?;
            let network = std::fs::File::open("/proc/self/ns/net")?;
            send(&self.0, b'R', &[process.as_fd(), network.as_fd()])?;
            let [environment] = receive::<1>(&self.0, b'G')?;
            drop((self, process, network));
            Ok(environment)
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(unsupported())
        }
    }
}

/// Obtains the owning user namespace after verifying a network namespace descriptor.
///
/// # Errors
/// Returns an error if the descriptor is not a network namespace or the ioctl fails.
pub fn namespace_owner(namespace: BorrowedFd<'_>) -> io::Result<OwnedFd> {
    #[cfg(target_os = "linux")]
    {
        crate::sys::namespace_owner(namespace)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = namespace;
        Err(unsupported())
    }
}

#[cfg(target_os = "linux")]
fn send(socket: &OwnedFd, byte: u8, fds: &[BorrowedFd<'_>]) -> io::Result<()> {
    use rustix::net::{SendAncillaryBuffer, SendAncillaryMessage, SendFlags, sendmsg};
    let mut space = [std::mem::MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(2))];
    let mut control = SendAncillaryBuffer::new(&mut space);
    if !control.push(SendAncillaryMessage::ScmRights(fds)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "too many gate descriptors",
        ));
    }
    if sendmsg(
        socket,
        &[io::IoSlice::new(&[byte])],
        &mut control,
        SendFlags::NOSIGNAL,
    )? != 1
    {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "short gate packet",
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn receive<const N: usize>(socket: &OwnedFd, expected: u8) -> io::Result<[OwnedFd; N]> {
    use rustix::net::{RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, ReturnFlags, recvmsg};
    let mut data = [0u8; 1];
    let mut space = [std::mem::MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(2))];
    let mut control = RecvAncillaryBuffer::new(&mut space);
    let result = recvmsg(
        socket,
        &mut [io::IoSliceMut::new(&mut data)],
        &mut control,
        RecvFlags::CMSG_CLOEXEC,
    )?;
    let mut fds = Vec::new();
    let mut invalid = false;
    for message in control.drain() {
        match message {
            RecvAncillaryMessage::ScmRights(rights) => fds.extend(rights),
            _ => invalid = true,
        }
    }
    if invalid
        || result.bytes != 1
        || data[0] != expected
        || result
            .flags
            .intersects(ReturnFlags::TRUNC | ReturnFlags::CTRUNC)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid execution gate packet",
        ));
    }
    fds.try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "wrong execution gate descriptor count",
        )
    })
}

/// Writes an exact Unix environment using length-prefixed byte strings.
///
/// # Errors
/// Returns an error if the destination cannot be written.
pub fn write_execution_environment(
    output: &mut std::fs::File,
    environment: &[(OsString, OsString)],
) -> io::Result<()> {
    output.write_all(&(environment.len() as u64).to_le_bytes())?;
    for (key, value) in environment {
        for bytes in [key.as_bytes(), value.as_bytes()] {
            output.write_all(&(bytes.len() as u64).to_le_bytes())?;
            output.write_all(bytes)?;
        }
    }
    output.seek(SeekFrom::Start(0))?;
    Ok(())
}

/// Reads an environment encoded by [`write_execution_environment`].
///
/// # Errors
/// Returns an error for malformed, truncated, or unreadable input.
pub fn read_execution_environment(
    input: &mut std::fs::File,
) -> io::Result<Vec<(OsString, OsString)>> {
    fn invalid() -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, "invalid environment data")
    }
    fn number(input: &mut std::fs::File, remaining: &mut u64) -> io::Result<u64> {
        *remaining = remaining.checked_sub(8).ok_or_else(invalid)?;
        let mut bytes = [0; 8];
        input.read_exact(&mut bytes)?;
        Ok(u64::from_le_bytes(bytes))
    }
    fn string(input: &mut std::fs::File, remaining: &mut u64) -> io::Result<OsString> {
        let len = number(input, remaining)?;
        *remaining = remaining.checked_sub(len).ok_or_else(invalid)?;
        let len = usize::try_from(len).map_err(|_| invalid())?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(len).map_err(|_| invalid())?;
        bytes.resize(len, 0);
        input.read_exact(&mut bytes)?;
        Ok(OsString::from_vec(bytes))
    }
    let mut remaining = input
        .metadata()?
        .len()
        .checked_sub(input.stream_position()?)
        .ok_or_else(invalid)?;
    let count = number(input, &mut remaining)?;
    if count > remaining / 16 {
        return Err(invalid());
    }
    let count = usize::try_from(count).map_err(|_| invalid())?;
    let mut environment = Vec::new();
    environment
        .try_reserve_exact(count)
        .map_err(|_| invalid())?;
    for _ in 0..count {
        environment.push((
            string(input, &mut remaining)?,
            string(input, &mut remaining)?,
        ));
    }
    if remaining != 0 {
        return Err(invalid());
    }
    Ok(environment)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inherited_descriptor_is_duplicated_and_close_on_exec() -> io::Result<()> {
        use rustix::io::{FdFlags, fcntl_getfd, fcntl_setfd};
        let original = tempfile::tempfile()?;
        fcntl_setfd(&original, FdFlags::empty())?;
        let copy = crate::duplicate_inherited_fd(original.as_raw_fd())?;
        assert_ne!(copy.as_raw_fd(), original.as_raw_fd());
        assert!(fcntl_getfd(&original)?.contains(FdFlags::CLOEXEC));
        assert!(fcntl_getfd(&copy)?.contains(FdFlags::CLOEXEC));
        assert!(crate::duplicate_inherited_fd(-1).is_err());
        Ok(())
    }

    #[test]
    fn environment_roundtrips_nonunicode_and_empty() -> io::Result<()> {
        for expected in [
            vec![],
            vec![(
                OsString::from_vec(vec![b'K', 0xff]),
                OsString::from_vec(vec![0xfe, b'V']),
            )],
        ] {
            let mut file = tempfile::tempfile()?;
            write_execution_environment(&mut file, &expected)?;
            assert_eq!(read_execution_environment(&mut file)?, expected);
        }
        Ok(())
    }

    #[test]
    fn environment_rejects_unbounded_and_trailing_data() -> io::Result<()> {
        for bytes in [
            u64::MAX.to_le_bytes().to_vec(),
            [
                1u64.to_le_bytes(),
                u64::MAX.to_le_bytes(),
                0u64.to_le_bytes(),
            ]
            .concat(),
            [0u64.to_le_bytes().as_slice(), &[1]].concat(),
        ] {
            let mut file = tempfile::tempfile()?;
            file.write_all(&bytes)?;
            file.rewind()?;
            assert!(read_execution_environment(&mut file).is_err());
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn async_ready_reports_closed_child() -> io::Result<()> {
        let (parent, child) = execution_gate_pair()?;
        let parent = parent.into_async()?;
        drop(child);
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), parent.ready()).await;
        assert!(result.expect("ready timed out").is_err());
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn async_release_delivers_environment() -> io::Result<()> {
        let (parent, child) = execution_gate_pair()?;
        let parent = parent.into_async()?;
        let waiter = std::thread::spawn(move || child.wait());
        let ready = tokio::time::timeout(std::time::Duration::from_secs(5), parent.ready())
            .await
            .expect("ready timed out")?;
        let mut environment = tempfile::tempfile()?;
        environment.write_all(b"environment")?;
        environment.rewind()?;
        parent.release(environment.as_fd())?;
        let received = waiter.join().expect("gate thread panicked")?;
        let mut contents = String::new();
        std::fs::File::from(received).read_to_string(&mut contents)?;
        assert_eq!(contents, "environment");
        drop(ready);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn async_duplicate_keeps_child_channel_open() -> io::Result<()> {
        let (parent, child) = execution_gate_pair()?;
        let parent = parent.into_async()?;
        let duplicate = parent.duplicate()?;
        assert!(rustix::io::fcntl_getfd(&duplicate)?.contains(rustix::io::FdFlags::CLOEXEC));
        let flags = rustix::fs::fcntl_getfl(&child)?;
        rustix::fs::fcntl_setfl(&child, flags | rustix::fs::OFlags::NONBLOCK)?;
        drop(parent);
        assert_eq!(
            receive::<1>(&child.0, b'G').unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(duplicate);
        assert_eq!(
            receive::<1>(&child.0, b'G').unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn rejects_closed_wrong_byte_and_descriptor_count() -> io::Result<()> {
        let (parent, child) = execution_gate_pair()?;
        drop(child);
        assert!(parent.try_ready().is_err());
        let (parent, child) = execution_gate_pair()?;
        send(&child.0, b'X', &[child.as_fd(), child.as_fd()])?;
        assert!(parent.try_ready().is_err());
        let (parent, child) = execution_gate_pair()?;
        send(&child.0, b'R', &[child.as_fd()])?;
        assert!(parent.try_ready().is_err());
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn child_rejects_negative_release() -> io::Result<()> {
        let (parent, child) = execution_gate_pair()?;
        let waiter = std::thread::spawn(move || child.wait());
        let ready = parent.try_ready()?;
        send(&parent.0, b'X', &[ready.process.as_fd()])?;
        assert!(waiter.join().expect("gate thread panicked").is_err());
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn rejects_truncated_packet_and_ancillary_data() -> io::Result<()> {
        use rustix::net::{SendAncillaryBuffer, SendAncillaryMessage, SendFlags, sendmsg};
        let (parent, child) = execution_gate_pair()?;
        let mut space = [std::mem::MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(4))];
        let fds = [child.as_fd(), child.as_fd(), child.as_fd(), child.as_fd()];
        let mut ancillary = SendAncillaryBuffer::new(&mut space);
        assert!(ancillary.push(SendAncillaryMessage::ScmRights(&fds)));
        sendmsg(
            &child.0,
            &[io::IoSlice::new(b"RR")],
            &mut ancillary,
            SendFlags::NOSIGNAL,
        )?;
        assert!(parent.try_ready().is_err());
        Ok(())
    }
}
