//! Helpers shared by the black-box integration tests.

use std::error::Error;
use std::ffi::OsStr;
use std::net::SocketAddr;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::time::Duration;

use axum::http::header::CONTENT_TYPE;
use axum::{Router, routing::get};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use hyper_util::service::TowerToHyperService;
use tokio::net::TcpListener;
use tokio::runtime::{Builder, Runtime};

use crate::support::wait_within;

/// Owns the runtime serving server information until the test is finished.
pub struct FakeServer {
    /// Bound loopback address.
    address: SocketAddr,
    /// Keeps the server and its connection tasks alive.
    _runtime: Runtime,
}

impl FakeServer {
    /// Returns the server's base URL.
    pub fn url(&self) -> String {
        format!("http://{}", self.address)
    }
}

/// Serves the supplied JSON at `/v1/server` over HTTP/1.1 or HTTP/2.
pub fn serve_server_info(body: &'static str) -> Result<FakeServer, Box<dyn Error>> {
    let runtime = Builder::new_multi_thread().enable_all().build()?;
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let address = listener.local_addr()?;
    let listener = runtime.block_on(async { TcpListener::from_std(listener) })?;
    let app = Router::new().route(
        "/v1/server",
        get(move || async move { ([(CONTENT_TYPE, "application/json")], body) }),
    );
    runtime.spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let app = app.clone();
            tokio::spawn(async move {
                let _ = auto::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), TowerToHyperService::new(app))
                    .await;
            });
        }
    });
    Ok(FakeServer {
        address,
        _runtime: runtime,
    })
}

/// Runs a command within a deadline and captures both output streams.
pub fn output_within(command: &mut Command, limit: Duration) -> Result<Output, Box<dyn Error>> {
    let child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    wait_within(child, limit, true)
}

/// Builds a command with a clean environment and isolated home directories.
pub fn gang_command_for(program: &OsStr, home: &Path) -> Command {
    let mut command = Command::new(program);
    command
        .env_clear()
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", home)
        .stdin(Stdio::null());
    command
}
