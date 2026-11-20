//! Network fixture peer: bounded HTTP, datagram and DNS listeners.

use std::collections::BTreeMap;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use gang_e2e::egress_wire::{EgressTargets, PeerCommand, PeerMessage, inherited_unix_stream};

type Counters = BTreeMap<String, Arc<AtomicU64>>;

fn main() -> std::process::ExitCode {
    if let Some(status) = gang_e2e::egress_lifecycle::fault_helper_main() {
        return status;
    }
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("egress peer: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() != Some("--control-fd") {
        return Err("expected --control-fd".into());
    }
    let fd = args.next().ok_or("missing control fd")?.parse()?;
    if args.next().as_deref() != Some("--targets") {
        return Err("expected --targets".into());
    }
    let mut targets: EgressTargets = serde_json::from_str(&args.next().ok_or("missing targets")?)?;
    if args.next().is_some() {
        return Err("unexpected argument".into());
    }
    let mut control = inherited_unix_stream(fd)?;
    control.set_read_timeout(Some(targets.timeout))?;
    control.set_write_timeout(Some(targets.timeout))?;
    let mut reader = BufReader::new(control.try_clone()?);
    send(&mut control, &PeerMessage::Booted)?;
    let mut counters = Counters::new();
    let mut started = false;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            return Err("control stream closed".into());
        }
        if line.len() > 65536 {
            return Err("control message too large".into());
        }
        match serde_json::from_str::<PeerCommand>(&line)? {
            PeerCommand::Start if !started => {
                targets.public_ipv4 =
                    bind_pair("public4", targets.public_ipv4, &targets, &mut counters)?;
                targets.public_ipv6 =
                    bind_pair("public6", targets.public_ipv6, &targets, &mut counters)?;
                for (label, address) in targets.blocked.clone() {
                    if label == "host4" || label == "host_loopback" {
                        continue;
                    }
                    let actual = bind_pair(&label, address, &targets, &mut counters)?;
                    targets.blocked.insert(label, actual);
                }
                bind_dns(&targets, &mut counters)?;
                started = true;
                send(&mut control, &PeerMessage::Ready(Box::new(targets.clone())))?;
                // The owner bounds the worker lifetime and closes the stream on teardown.
                control.set_read_timeout(None)?;
            }
            PeerCommand::Reset if started => {
                for count in counters.values() {
                    count.store(0, Ordering::SeqCst);
                }
                send(&mut control, &PeerMessage::Counts(snapshot(&counters)))?;
            }
            PeerCommand::Counts if started => {
                send(&mut control, &PeerMessage::Counts(snapshot(&counters)))?;
            }
            PeerCommand::Stop => {
                send(&mut control, &PeerMessage::Stopped)?;
                return Ok(());
            }
            _ => return Err("invalid peer command for state".into()),
        }
    }
}

fn send(stream: &mut UnixStream, message: &PeerMessage) -> io::Result<()> {
    serde_json::to_writer(&mut *stream, message)?;
    stream.write_all(b"\n")
}

fn snapshot(counters: &Counters) -> BTreeMap<String, u64> {
    counters
        .iter()
        .map(|(key, value)| (key.clone(), value.load(Ordering::SeqCst)))
        .collect()
}

fn counter(counters: &mut Counters, label: &str, protocol: &str) -> Arc<AtomicU64> {
    let value = Arc::new(AtomicU64::new(0));
    counters.insert(format!("{label}.{protocol}"), Arc::clone(&value));
    value
}

fn bind_pair(
    label: &str,
    address: SocketAddr,
    targets: &EgressTargets,
    counters: &mut Counters,
) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(address)?;
    let actual = listener.local_addr()?;
    let udp = UdpSocket::bind(actual)?;
    let tcp_count = counter(counters, label, "tcp");
    let udp_count = counter(counters, label, "udp");
    let nonce = targets.nonce.clone();
    let timeout = targets.timeout;
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            tcp_count.fetch_add(1, Ordering::SeqCst);
            let _ = stream.set_read_timeout(Some(timeout));
            let _ = stream.set_write_timeout(Some(timeout));
            let mut request = [0_u8; 4096];
            if stream.read(&mut request).is_ok() {
                let _ = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    nonce.len(),
                    nonce
                );
            }
        }
    });
    std::thread::spawn(move || {
        let mut buffer = [0_u8; 4096];
        while let Ok((length, source)) = udp.recv_from(&mut buffer) {
            udp_count.fetch_add(1, Ordering::SeqCst);
            let _ = udp.send_to(&buffer[..length], source);
        }
    });
    Ok(actual)
}

fn bind_dns(targets: &EgressTargets, counters: &mut Counters) -> io::Result<()> {
    let address = SocketAddr::new(targets.dns, 53);
    let tcp = TcpListener::bind(address)?;
    let udp = UdpSocket::bind(address)?;
    let udp_count = counter(counters, "dns", "udp");
    let tcp_count = counter(counters, "dns", "tcp");
    let public4 = targets.public_ipv4.ip();
    let public6 = targets.public_ipv6.ip();
    let timeout = targets.timeout;
    std::thread::spawn(move || {
        let mut buffer = [0_u8; 4096];
        while let Ok((length, source)) = udp.recv_from(&mut buffer) {
            udp_count.fetch_add(1, Ordering::SeqCst);
            if let Some(reply) = dns_reply(&buffer[..length], public4, public6) {
                let _ = udp.send_to(&reply, source);
            }
        }
    });
    std::thread::spawn(move || {
        for stream in tcp.incoming() {
            let Ok(mut stream) = stream else { break };
            tcp_count.fetch_add(1, Ordering::SeqCst);
            let _ = dns_tcp(&mut stream, public4, public6, timeout);
        }
    });
    Ok(())
}

fn dns_tcp(
    stream: &mut TcpStream,
    public4: IpAddr,
    public6: IpAddr,
    timeout: Duration,
) -> io::Result<()> {
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    let mut length = [0_u8; 2];
    stream.read_exact(&mut length)?;
    let size = usize::from(u16::from_be_bytes(length));
    if size > 4096 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DNS request too large",
        ));
    }
    let mut request = vec![0; size];
    stream.read_exact(&mut request)?;
    let response = dns_reply(&request, public4, public6)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid DNS request"))?;
    let response_len = u16::try_from(response.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "DNS response too large"))?;
    stream.write_all(&response_len.to_be_bytes())?;
    stream.write_all(&response)
}

fn dns_reply(request: &[u8], public4: IpAddr, public6: IpAddr) -> Option<Vec<u8>> {
    if request.len() < 12 || request[2] & 0x80 != 0 || request[4..6] != [0, 1] {
        return None;
    }
    let mut pos = 12;
    let mut name = String::new();
    loop {
        let length = usize::from(*request.get(pos)?);
        pos += 1;
        if length == 0 {
            break;
        }
        if length > 63 || pos.checked_add(length)? > request.len() || name.len() + length > 253 {
            return None;
        }
        if !name.is_empty() {
            name.push('.');
        }
        name.push_str(std::str::from_utf8(request.get(pos..pos + length)?).ok()?);
        pos += length;
    }
    let question_end = pos.checked_add(4)?;
    let question = request.get(pos..question_end)?;
    let kind = u16::from_be_bytes([question[0], question[1]]);
    if question[2..4] != [0, 1] {
        return None;
    }
    let answer = match (name.to_ascii_lowercase().as_str(), kind) {
        ("egress.fixture.test", 1) => match public4 {
            IpAddr::V4(ip) => Some(ip.octets().to_vec()),
            IpAddr::V6(_) => None,
        },
        ("egress.fixture.test", 28) => match public6 {
            IpAddr::V6(ip) => Some(ip.octets().to_vec()),
            IpAddr::V4(_) => None,
        },
        ("forbidden.fixture.test", 1) => Some(vec![10, 20, 0, 10]),
        _ => None,
    };
    let mut reply = request.get(..question_end)?.to_vec();
    reply[2] = 0x81;
    reply[3] = 0x80;
    reply[6..8].copy_from_slice(&u16::from(answer.is_some()).to_be_bytes());
    reply[8..12].fill(0);
    if let Some(data) = answer {
        reply.extend_from_slice(&[0xc0, 0x0c]);
        reply.extend_from_slice(&kind.to_be_bytes());
        reply.extend_from_slice(&[0, 1, 0, 0, 0, 0]);
        reply.extend_from_slice(&u16::try_from(data.len()).ok()?.to_be_bytes());
        reply.extend_from_slice(&data);
    }
    Some(reply)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(name: &str, kind: u16) -> Vec<u8> {
        let mut bytes = vec![0x47, 0x41, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0];
        for label in name.split('.') {
            bytes.push(u8::try_from(label.len()).expect("fixture label fits"));
            bytes.extend_from_slice(label.as_bytes());
        }
        bytes.push(0);
        bytes.extend_from_slice(&kind.to_be_bytes());
        bytes.extend_from_slice(&[0, 1]);
        bytes
    }

    #[test]
    fn dns_answers_public_and_private_without_exposing_private_aaaa() {
        let public4 = IpAddr::V4(std::net::Ipv4Addr::new(93, 184, 216, 34));
        let public6 = IpAddr::V6("2001:4860:1234::34".parse().expect("fixture address"));
        for (name, kind, expected) in [
            ("egress.fixture.test", 1, vec![93, 184, 216, 34]),
            (
                "egress.fixture.test",
                28,
                match public6 {
                    IpAddr::V6(ip) => ip.octets().to_vec(),
                    IpAddr::V4(_) => Vec::new(),
                },
            ),
            ("forbidden.fixture.test", 1, vec![10, 20, 0, 10]),
        ] {
            let response = dns_reply(&query(name, kind), public4, public6).expect("valid query");
            assert_eq!(&response[6..8], &[0, 1]);
            assert!(response.ends_with(&expected));
        }
        let response =
            dns_reply(&query("forbidden.fixture.test", 28), public4, public6).expect("valid query");
        assert_eq!(&response[6..8], &[0, 0]);
        assert!(dns_reply(&[0; 11], public4, public6).is_none());
        let mut invalid = query("egress.fixture.test", 1);
        invalid[12] = 0xff;
        assert!(dns_reply(&invalid, public4, public6).is_none());
    }
}
