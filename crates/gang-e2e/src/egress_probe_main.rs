//! Socket observations made from the actual remote action.

use std::io::{self, Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::time::Duration;

use gang_e2e::egress_wire::EgressTargets;

fn main() {
    if let Err(error) = run() {
        eprintln!("egress probe: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let path = args.next().ok_or("missing targets path")?;
    if args.next().is_some() {
        return Err("unexpected argument".into());
    }
    let targets: EgressTargets = serde_json::from_slice(&std::fs::read(path)?)?;
    println!("uid={}", rustix::process::getuid().as_raw());
    println!("gid={}", rustix::process::getgid().as_raw());
    println!("net={}", std::fs::read_link("/proc/self/ns/net")?.display());
    println!("fd_leak={}", descriptor_leak()?);
    println!("nft_mutation={}", nft_mutation(&targets));
    report(
        "loopback4",
        loopback(IpAddr::V4(Ipv4Addr::LOCALHOST), targets.timeout),
    );
    report(
        "loopback6",
        loopback(IpAddr::V6(Ipv6Addr::LOCALHOST), targets.timeout),
    );
    report(
        "public4",
        http(targets.public_ipv4, &targets.nonce, targets.timeout),
    );
    report(
        "public6",
        http(targets.public_ipv6, &targets.nonce, targets.timeout),
    );
    report(
        "udp4",
        datagram(targets.public_ipv4, &targets.nonce, targets.timeout),
    );
    report(
        "udp6",
        datagram(targets.public_ipv6, &targets.nonce, targets.timeout),
    );
    report_dns(&targets);
    report_blocked(&targets)
}

fn report_dns(targets: &EgressTargets) {
    let public_dns = (|| {
        let a = dns_required(targets, "egress.fixture.test", 1, false)?;
        let aaaa = dns_required(targets, "egress.fixture.test", 28, false)?;
        let tcp = dns_required(targets, "egress.fixture.test", 1, true)?;
        if a != targets.public_ipv4.ip() || aaaa != targets.public_ipv6.ip() || tcp != a {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected public DNS answer",
            ));
        }
        http(
            SocketAddr::new(a, targets.public_ipv4.port()),
            &targets.nonce,
            targets.timeout,
        )?;
        http(
            SocketAddr::new(aaaa, targets.public_ipv6.port()),
            &targets.nonce,
            targets.timeout,
        )
    })();
    report("dns_public", public_dns);
    let private_dns = (|| {
        let ip = dns_required(targets, "forbidden.fixture.test", 1, false)?;
        if dns(targets, "forbidden.fixture.test", 28, false)?.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected private AAAA answer",
            ));
        }
        if ip != IpAddr::V4(Ipv4Addr::new(10, 20, 0, 10)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected private DNS answer",
            ));
        }
        let port = targets
            .blocked
            .get("vpc4")
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing vpc4 target"))?
            .port();
        http(SocketAddr::new(ip, port), &targets.nonce, targets.timeout)
    })();
    report("dns_private", private_dns);
}

fn report_blocked(targets: &EgressTargets) -> Result<(), Box<dyn std::error::Error>> {
    for (label, address) in &targets.blocked {
        report(
            &format!("blocked_{label}_tcp"),
            http(*address, &targets.nonce, targets.timeout),
        );
        report(
            &format!("blocked_{label}_udp"),
            datagram(*address, &targets.nonce, targets.timeout),
        );
    }
    for (label, ip) in [
        ("alias4", IpAddr::V4(Ipv4Addr::new(10, 0, 2, 2))),
        ("dns_alias4", IpAddr::V4(Ipv4Addr::new(10, 0, 2, 3))),
        ("alias6", IpAddr::V6("fd00::2".parse()?)),
    ] {
        let port = targets
            .blocked
            .get("host4")
            .ok_or("missing host4 target")?
            .port();
        let address = SocketAddr::new(ip, port);
        report(
            &format!("blocked_{label}_tcp"),
            http(address, &targets.nonce, targets.timeout),
        );
        report(
            &format!("blocked_{label}_udp"),
            datagram(address, &targets.nonce, targets.timeout),
        );
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn nft_mutation(targets: &EgressTargets) -> &'static str {
    match netfilter_probe::run(targets.timeout) {
        Ok(()) => "permission_denied",
        Err(reason) => reason,
    }
}

#[cfg(not(target_os = "linux"))]
fn nft_mutation(_targets: &EgressTargets) -> &'static str {
    "unsupported"
}

#[cfg(target_os = "linux")]
mod netfilter_probe {
    use std::time::{Duration, Instant};

    use rustix::event::{PollFd, PollFlags, Timespec, poll};
    use rustix::net::{
        AddressFamily, RecvFlags, SendFlags, SocketFlags, SocketType, bind, netlink, recvfrom,
        sendto, socket_with,
    };

    const BATCH_BEGIN: u16 = 0x10;
    const BATCH_END: u16 = 0x11;
    const NEW_TABLE: u16 = 10 << 8;
    const NLMSG_ERROR: u16 = 2;
    const NLM_F_REQUEST: u16 = 0x01;
    const NLM_F_ACK: u16 = 0x04;
    const NLM_F_EXCL: u16 = 0x200;
    const NLM_F_CREATE: u16 = 0x400;
    const EPERM: i32 = 1;
    const BEGIN_SEQ: u32 = 1;
    const TABLE_SEQ: u32 = 2;
    const END_SEQ: u32 = 3;

    fn header(bytes: &mut Vec<u8>, len: u32, kind: u16, flags: u16, seq: u32) {
        bytes.extend_from_slice(&len.to_ne_bytes());
        bytes.extend_from_slice(&kind.to_ne_bytes());
        bytes.extend_from_slice(&flags.to_ne_bytes());
        bytes.extend_from_slice(&seq.to_ne_bytes());
        bytes.extend_from_slice(&0_u32.to_ne_bytes());
    }

    fn request() -> Vec<u8> {
        let mut bytes = Vec::with_capacity(84);
        // nfgenmsg.res_id is network byte order; the batch names the nftables subsystem.
        header(&mut bytes, 20, BATCH_BEGIN, NLM_F_REQUEST, BEGIN_SEQ);
        bytes.extend_from_slice(&[0, 0, 0, 10]);
        header(
            &mut bytes,
            44,
            NEW_TABLE,
            NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_EXCL,
            TABLE_SEQ,
        );
        bytes.extend_from_slice(&[1, 0, 0, 0]); // NFPROTO_INET, version 0
        bytes.extend_from_slice(&22_u16.to_ne_bytes());
        bytes.extend_from_slice(&1_u16.to_ne_bytes()); // NFTA_TABLE_NAME
        bytes.extend_from_slice(b"gang_egress_probe\0");
        bytes.extend_from_slice(&[0, 0]); // netlink attribute alignment
        header(&mut bytes, 20, BATCH_END, NLM_F_REQUEST, END_SEQ);
        bytes.extend_from_slice(&[0, 0, 0, 10]);
        bytes
    }

    // Returns None for unrelated messages; only an error for our batch or table is evidence.
    fn response(mut data: &[u8]) -> Result<Option<()>, &'static str> {
        while !data.is_empty() {
            if data.len() < 16 {
                return Err("invalid_response");
            }
            let len =
                u32::from_ne_bytes(data[0..4].try_into().map_err(|_| "invalid_response")?) as usize;
            if len < 16 || len > data.len() {
                return Err("invalid_response");
            }
            let kind = u16::from_ne_bytes([data[4], data[5]]);
            let seq = u32::from_ne_bytes(data[8..12].try_into().map_err(|_| "invalid_response")?);
            if kind == NLMSG_ERROR && (seq == BEGIN_SEQ || seq == TABLE_SEQ) {
                if len < 36 {
                    return Err("invalid_response");
                }
                let original_kind = u16::from_ne_bytes([data[24], data[25]]);
                let original_seq =
                    u32::from_ne_bytes(data[28..32].try_into().map_err(|_| "invalid_response")?);
                let expected_kind = if seq == BEGIN_SEQ {
                    BATCH_BEGIN
                } else {
                    NEW_TABLE
                };
                if original_seq != seq || original_kind != expected_kind {
                    return Err("invalid_response");
                }
                let error =
                    i32::from_ne_bytes(data[16..20].try_into().map_err(|_| "invalid_response")?);
                return if error == -EPERM {
                    Ok(Some(()))
                } else if error == 0 {
                    Err("unexpected_success")
                } else {
                    Err("unexpected_errno")
                };
            }
            let aligned = len.checked_add(3).ok_or("invalid_response")? & !3;
            if aligned > data.len() {
                return Err("invalid_response");
            }
            data = &data[aligned..];
        }
        Ok(None)
    }

    pub(super) fn run(timeout: Duration) -> Result<(), &'static str> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or("invalid_timeout")?;
        let socket = socket_with(
            AddressFamily::NETLINK,
            SocketType::RAW,
            SocketFlags::CLOEXEC | SocketFlags::NONBLOCK,
            Some(netlink::NETFILTER),
        )
        .map_err(|_| "socket_error")?;
        bind(&socket, &netlink::SocketAddrNetlink::new(0, 0)).map_err(|_| "bind_error")?;
        let request = request();
        let sent = sendto(
            &socket,
            &request,
            SendFlags::empty(),
            &netlink::SocketAddrNetlink::new(0, 0),
        )
        .map_err(|_| "send_error")?;
        if sent != request.len() {
            return Err("send_error");
        }
        let mut fds = [PollFd::new(&socket, PollFlags::IN)];
        let mut buf = [0_u8; 8192];
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err("timeout");
            }
            let wait = Timespec {
                tv_sec: i64::try_from(remaining.as_secs()).map_err(|_| "invalid_timeout")?,
                tv_nsec: remaining.subsec_nanos().into(),
            };
            match poll(&mut fds, Some(&wait)) {
                Err(rustix::io::Errno::INTR) => continue,
                Err(_) => return Err("poll_error"),
                Ok(0) => return Err("timeout"),
                Ok(_) if !fds[0].revents().contains(PollFlags::IN) => return Err("poll_error"),
                Ok(_) => {}
            }
            let (copied, actual, sender) = match recvfrom(&socket, &mut buf, RecvFlags::TRUNC) {
                Err(rustix::io::Errno::INTR | rustix::io::Errno::AGAIN) => continue,
                Err(_) => return Err("receive_error"),
                Ok(message) => message,
            };
            if actual != copied {
                return Err("invalid_response");
            }
            let sender = sender
                .and_then(|address| netlink::SocketAddrNetlink::try_from(address).ok())
                .ok_or("invalid_sender")?;
            if sender.pid() != 0 || sender.groups() != 0 {
                return Err("invalid_sender");
            }
            if response(&buf[..copied])?.is_some() {
                return Ok(());
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn error_message(seq: u32, kind: u16, error: i32) -> Vec<u8> {
            let mut bytes = Vec::new();
            header(&mut bytes, 36, NLMSG_ERROR, 0, seq);
            bytes.extend_from_slice(&error.to_ne_bytes());
            header(&mut bytes, 20, kind, 1, seq);
            bytes
        }

        #[test]
        fn request_encodes_nftables_transaction() {
            let bytes = request();
            let mut expected = Vec::new();
            expected.extend_from_slice(&20_u32.to_ne_bytes());
            expected.extend_from_slice(&BATCH_BEGIN.to_ne_bytes());
            expected.extend_from_slice(&1_u16.to_ne_bytes());
            expected.extend_from_slice(&1_u32.to_ne_bytes());
            expected.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 10]);
            expected.extend_from_slice(&44_u32.to_ne_bytes());
            expected.extend_from_slice(&0x0a00_u16.to_ne_bytes());
            expected.extend_from_slice(&0x605_u16.to_ne_bytes());
            expected.extend_from_slice(&2_u32.to_ne_bytes());
            expected.extend_from_slice(&[0, 0, 0, 0, 1, 0, 0, 0]);
            expected.extend_from_slice(&22_u16.to_ne_bytes());
            expected.extend_from_slice(&1_u16.to_ne_bytes());
            expected.extend_from_slice(b"gang_egress_probe\0\0\0");
            expected.extend_from_slice(&20_u32.to_ne_bytes());
            expected.extend_from_slice(&BATCH_END.to_ne_bytes());
            expected.extend_from_slice(&1_u16.to_ne_bytes());
            expected.extend_from_slice(&3_u32.to_ne_bytes());
            expected.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 10]);
            assert_eq!(bytes, expected);
        }

        #[test]
        fn accepts_only_matching_eperm() {
            let denied = error_message(TABLE_SEQ, NEW_TABLE, -1);
            assert_eq!(response(&denied), Ok(Some(())));
            assert_eq!(
                response(&error_message(BEGIN_SEQ, BATCH_BEGIN, -1)),
                Ok(Some(()))
            );
            assert_eq!(
                response(&error_message(TABLE_SEQ, NEW_TABLE, -13)),
                Err("unexpected_errno")
            );
            assert_eq!(
                response(&error_message(TABLE_SEQ, NEW_TABLE, 0)),
                Err("unexpected_success")
            );
            assert_eq!(response(&error_message(99, NEW_TABLE, -1)), Ok(None));
            assert_eq!(
                response(&error_message(TABLE_SEQ, BATCH_BEGIN, -1)),
                Err("invalid_response")
            );
            assert_eq!(response(&denied[..35]), Err("invalid_response"));
            let mut wrong_sequence = denied;
            wrong_sequence[28..32].copy_from_slice(&99_u32.to_ne_bytes());
            assert_eq!(response(&wrong_sequence), Err("invalid_response"));
        }
    }
}

fn report(key: &str, result: io::Result<()>) {
    let value = match result {
        Ok(()) => "ok",
        Err(error) => match error.kind() {
            io::ErrorKind::PermissionDenied => "permission_denied",
            io::ErrorKind::NetworkUnreachable => "network_unreachable",
            io::ErrorKind::HostUnreachable => "host_unreachable",
            io::ErrorKind::ConnectionRefused => "connection_refused",
            io::ErrorKind::ConnectionReset => "connection_reset",
            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock => "timeout",
            io::ErrorKind::AddrNotAvailable => "address_not_available",
            _ => "error",
        },
    };
    println!("{key}={value}");
}

fn descriptor_leak() -> io::Result<&'static str> {
    for entry in std::fs::read_dir("/proc/self/fd")? {
        let entry = entry?;
        let Ok(fd) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        if fd <= 2 {
            continue;
        }
        let Ok(link) = std::fs::read_link(entry.path()) else {
            continue;
        };
        if link.starts_with("/proc") && link.ends_with("fd") {
            continue;
        }
        return Ok("leaked");
    }
    Ok("ok")
}

fn loopback(ip: IpAddr, timeout: Duration) -> io::Result<()> {
    let listener = TcpListener::bind(SocketAddr::new(ip, 0))?;
    let address = listener.local_addr()?;
    let worker = std::thread::spawn(move || -> io::Result<()> {
        let (mut socket, _) = listener.accept()?;
        socket.set_read_timeout(Some(timeout))?;
        let mut byte = [0];
        socket.read_exact(&mut byte)?;
        socket.write_all(&byte)
    });
    let mut stream = TcpStream::connect_timeout(&address, timeout)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    stream.write_all(b"x")?;
    let mut response = [0];
    stream.read_exact(&mut response)?;
    worker
        .join()
        .map_err(|_| io::Error::other("loopback server panicked"))??;
    if response != *b"x" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "loopback mismatch",
        ));
    }
    Ok(())
}

fn http(address: SocketAddr, nonce: &str, timeout: Duration) -> io::Result<()> {
    let mut stream = TcpStream::connect_timeout(&address, timeout)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: egress.fixture.test\r\nConnection: close\r\n\r\n")?;
    let mut response = Vec::new();
    stream.take(8192).read_to_end(&mut response)?;
    let expected = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        nonce.len(),
        nonce
    );
    if response != expected.as_bytes() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "HTTP response mismatch",
        ));
    }
    Ok(())
}

fn datagram(address: SocketAddr, nonce: &str, timeout: Duration) -> io::Result<()> {
    let bind = if address.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = UdpSocket::bind(bind)?;
    socket.set_read_timeout(Some(timeout))?;
    socket.set_write_timeout(Some(timeout))?;
    socket.connect(address)?;
    socket.send(nonce.as_bytes())?;
    let mut buffer = [0_u8; 4096];
    let length = socket.recv(&mut buffer)?;
    if &buffer[..length] != nonce.as_bytes() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "datagram mismatch",
        ));
    }
    Ok(())
}

fn dns_required(targets: &EgressTargets, name: &str, kind: u16, tcp: bool) -> io::Result<IpAddr> {
    dns(targets, name, kind, tcp)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing DNS answer"))
}

fn dns(targets: &EgressTargets, name: &str, kind: u16, tcp: bool) -> io::Result<Option<IpAddr>> {
    let mut query = vec![0x47, 0x41, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0];
    for label in name.split('.') {
        let length = u8::try_from(label.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "DNS label too long"))?;
        query.push(length);
        query.extend_from_slice(label.as_bytes());
    }
    query.push(0);
    query.extend_from_slice(&kind.to_be_bytes());
    query.extend_from_slice(&[0, 1]);
    let address = SocketAddr::new(targets.dns, 53);
    let mut response = vec![0; 4096];
    let length = if tcp {
        let mut stream = TcpStream::connect_timeout(&address, targets.timeout)?;
        stream.set_read_timeout(Some(targets.timeout))?;
        stream.set_write_timeout(Some(targets.timeout))?;
        let length = u16::try_from(query.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "DNS query too long"))?;
        stream.write_all(&length.to_be_bytes())?;
        stream.write_all(&query)?;
        let mut length = [0; 2];
        stream.read_exact(&mut length)?;
        let length = usize::from(u16::from_be_bytes(length));
        if length > response.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "DNS response too long",
            ));
        }
        stream.read_exact(&mut response[..length])?;
        length
    } else {
        let socket = UdpSocket::bind(if address.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        })?;
        socket.set_read_timeout(Some(targets.timeout))?;
        socket.set_write_timeout(Some(targets.timeout))?;
        socket.connect(address)?;
        socket.send(&query)?;
        socket.recv(&mut response)?
    };
    response.truncate(length);
    let offset = query.len();
    if response.len() < offset
        || response[..2] != query[..2]
        || response[2] & 0x80 == 0
        || response[3] & 15 != 0
        || response[12..offset] != query[12..]
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid DNS response",
        ));
    }
    if response[6..8] == [0, 0] && response.len() == offset {
        return Ok(None);
    }
    if response.len() < offset + 12
        || response[6..8] != [0, 1]
        || response[offset..offset + 2] != [0xc0, 0x0c]
        || response[offset + 2..offset + 4] != kind.to_be_bytes()
        || response[offset + 4..offset + 6] != [0, 1]
        || usize::from(u16::from_be_bytes([
            response[offset + 10],
            response[offset + 11],
        ])) != response.len() - offset - 12
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid DNS answer",
        ));
    }
    let data = &response[offset + 12..];
    match (kind, data) {
        (1, [a, b, c, d]) => Ok(Some(Ipv4Addr::new(*a, *b, *c, *d).into())),
        (28, data) if data.len() == 16 => {
            let mut bytes = [0; 16];
            bytes.copy_from_slice(data);
            Ok(Some(Ipv6Addr::from(bytes).into()))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid DNS address",
        )),
    }
}
