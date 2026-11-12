//! The state API over HTTP/2 with prior knowledge, without configuring TLS or installing a
//! process-wide crypto provider.

use std::error::Error;
use std::io;
use std::net::TcpListener;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

use bytes::Bytes;
use gang_e2e::{E2eError, EventStream, StateApi};
use gang_state::sse;
use gang_types::Url;
use gang_wire::{Frame, State};
use serde_json::json;

type TestResult = Result<(), Box<dyn Error>>;

/// One scripted answer of the stub server.
struct Reply {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
}

/// The state of an idle deployment at `seq`, in its wire form.
fn state(seq: u64) -> Result<State, serde_json::Error> {
    serde_json::from_value(json!({
        "name": "state",
        "seq": seq,
        "updateTime": "2026-01-14T09:19:30.000Z",
        "provisioner": "local",
        "pass": {"state": "OK", "interval": "2s", "applied": 0, "failed": 0},
        "queue": {"total": 0, "unplaceable": 0},
        "fleets": [],
        "workers": [],
        "instances": [],
        "events": [],
        "eventsLimit": 200,
        "completed": 0,
        "principalsEpoch": 0,
    }))
}

/// Serves one h2c connection: each request, in order, gets the next reply of `replies`. Returns
/// the `METHOD path` of every request once the client closes the connection.
async fn serve_connection(
    socket: tokio::net::TcpStream,
    replies: Vec<Reply>,
) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
    let mut connection = h2::server::handshake(socket).await?;
    let mut requests = Vec::new();
    let mut replies = replies.into_iter();
    while let Some(accepted) = connection.accept().await {
        let (request, mut respond) = accepted?;
        requests.push(format!("{} {}", request.method(), request.uri().path()));
        let Some(reply) = replies.next() else {
            break;
        };
        let response = http::Response::builder()
            .status(reply.status)
            .header("content-type", reply.content_type)
            .body(())?;
        let mut body = respond.send_response(response, false)?;
        body.send_data(Bytes::from(reply.body), true)?;
    }
    Ok(requests)
}

/// Starts an h2c server on an ephemeral loopback port that answers one connection with `replies`.
/// The handle yields the requests it saw once the client has closed the connection.
fn serve(replies: Vec<Reply>) -> io::Result<(Url, JoinHandle<io::Result<Vec<String>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let url =
        Url::parse(&format!("http://{}/", listener.local_addr()?)).map_err(io::Error::other)?;
    let handle = std::thread::spawn(move || -> io::Result<Vec<String>> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let listener = tokio::net::TcpListener::from_std(listener)?;
            let (socket, _) = listener.accept().await?;
            serve_connection(socket, replies)
                .await
                .map_err(io::Error::other)
        })
    });
    Ok((url, handle))
}

fn received(server: JoinHandle<io::Result<Vec<String>>>) -> Result<Vec<String>, Box<dyn Error>> {
    let requests = server.join().map_err(|_| "the stub server panicked")??;
    Ok(requests)
}

#[test]
fn a_status_is_read_over_h2c_without_a_crypto_provider() -> TestResult {
    let (url, server) = serve(vec![Reply {
        status: 401,
        content_type: "application/json",
        body: b"{}".to_vec(),
    }])?;

    let api = StateApi::anonymous(&url, None, Duration::from_secs(5))?;
    assert_eq!(api.status("/v1/state")?, 401);
    drop(api);

    assert_eq!(received(server)?, ["GET /v1/state"]);
    assert!(rustls::crypto::CryptoProvider::get_default().is_none());
    Ok(())
}

#[test]
fn the_state_is_read_over_h2c() -> TestResult {
    let expected = state(7)?;
    let (url, server) = serve(vec![Reply {
        status: 200,
        content_type: "application/json",
        body: serde_json::to_vec(&expected)?,
    }])?;

    let api = StateApi::anonymous(&url, None, Duration::from_secs(5))?;
    assert_eq!(api.state()?, expected);
    drop(api);

    assert_eq!(received(server)?, ["GET /v1/state"]);
    assert!(rustls::crypto::CryptoProvider::get_default().is_none());
    Ok(())
}

#[test]
fn a_refused_state_names_the_status_and_body() -> TestResult {
    let (url, server) = serve(vec![Reply {
        status: 503,
        content_type: "text/plain",
        body: b"state unavailable".to_vec(),
    }])?;

    let api = StateApi::anonymous(&url, None, Duration::from_secs(5))?;
    let error = api.state().err().ok_or("a 503 answer is not a state")?;
    drop(api);

    let message = gang_types::error_chain(&error).to_string();
    assert!(message.contains("503"), "{message}");
    assert!(message.contains("state unavailable"), "{message}");
    received(server)?;
    Ok(())
}

#[test]
fn the_stream_is_read_over_h2c() -> TestResult {
    let snapshot = Frame::Snapshot(state(3)?);
    let (url, server) = serve(vec![Reply {
        status: 200,
        content_type: "text/event-stream",
        body: sse::encode(&snapshot)?.to_vec(),
    }])?;

    let mut stream = StateApi::anonymous(&url, None, Duration::from_secs(5))?.open_stream()?;
    assert_eq!(stream.status(), 200);
    assert_eq!(stream.content_type(), Some("text/event-stream"));
    assert_eq!(stream.next_event()?, Some(snapshot));
    assert_eq!(stream.next_event()?, None);
    drop(stream);

    assert_eq!(received(server)?, ["GET /v1/state:watch"]);
    assert!(rustls::crypto::CryptoProvider::get_default().is_none());
    Ok(())
}

#[test]
fn silent_server_times_out_before_state_headers() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let url = Url::parse(&format!("http://{}/", listener.local_addr()?))?;
    let (release, wait) = mpsc::channel::<()>();
    let server = std::thread::spawn(move || -> io::Result<()> {
        let (_socket, _) = listener.accept()?;
        let _ = wait.recv();
        Ok(())
    });
    let timeout = Duration::from_millis(1);
    let api = StateApi::anonymous(&url, None, timeout)?;
    let error = api.state().err().ok_or("the silent server must time out")?;
    drop(release);
    server.join().map_err(|_| "server panicked")??;
    assert!(matches!(error, E2eError::Timeout { waited, .. } if waited == timeout));
    Ok(())
}
