//! Live revocation: refused handshakes, established connections closed without restart, and
//! revocation of unrelated serials.

use std::io;

use gang_testkit::park_then_signal;
use gang_types::CertSerial;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;

use crate::support::{Connection, TransportFixture, echo_once};

fn unrelated_serial() -> CertSerial {
    CertSerial::try_new("12".repeat(16)).expect("hex serial of 32 characters")
}

#[tokio::test(start_paused = true)]
async fn revoked_before_connect_is_refused() {
    let fixture = TransportFixture::new();
    let client = fixture.client(1, "alpha");
    fixture.revocations().revoke(client.serial.clone());

    let Connection {
        client: client_io,
        server,
    } = fixture.connect(&client).await;

    assert!(
        server.is_err(),
        "server must refuse a revoked client certificate"
    );
    let mut client_io = client_io.expect("TLS 1.3 client handshake completes before the rejection");
    let mut buf = [0_u8; 1];
    assert!(
        client_io.read(&mut buf).await.is_err(),
        "client read must fail after rejection"
    );
}

#[tokio::test(start_paused = true)]
async fn established_connection_is_closed_on_revoke() {
    let fixture = TransportFixture::new();
    let client = fixture.client(2, "alpha");
    let Connection {
        client: client_io,
        server,
    } = fixture.connect(&client).await;
    let mut client_io = client_io.expect("client handshake completes");
    let mut server = server.expect("server handshake completes");
    echo_once(&mut client_io, &mut server).await;

    fixture.revocations().revoke(client.serial.clone());

    let error = server
        .read(&mut [0_u8; 1])
        .await
        .expect_err("revoked connection must not read");
    assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted);
    server
        .write_all(b"late")
        .await
        .expect_err("revoked connection must not write");
    drop(server);

    let mut buf = [0_u8; 1];
    let read = client_io.read(&mut buf).await;
    assert!(
        matches!(read, Ok(0)) || read.is_err(),
        "client must observe end of stream after the server drops the connection"
    );
}

#[tokio::test(start_paused = true)]
async fn revocation_wakes_blocked_read() {
    let fixture = TransportFixture::new();
    let client = fixture.client(3, "alpha");
    let Connection {
        client: client_io,
        server,
    } = fixture.connect(&client).await;
    let client_io = client_io.expect("client handshake completes");
    let server = server.expect("server handshake completes");

    let (armed_tx, armed_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let mut server = server;
        let mut buf = [0_u8; 1];
        let result = park_then_signal(server.read(&mut buf), armed_tx)
            .await
            .map_err(|error| error.kind());
        (server, result)
    });
    armed_rx.await.expect("read task parks before revocation");
    fixture.revocations().revoke(client.serial.clone());

    let (server, result) = task.await.expect("read task completes");
    assert_eq!(result, Err(io::ErrorKind::ConnectionAborted));
    drop(server);
    drop(client_io);
}

#[tokio::test(start_paused = true)]
async fn revocation_aborts_a_pending_read_on_one_split_half() {
    let fixture = TransportFixture::new();
    let client = fixture.client(10, "alpha");
    let Connection {
        client: client_io,
        server,
    } = fixture.connect(&client).await;
    let mut client_io = client_io.expect("client handshake completes");
    let server = server.expect("server handshake completes");
    let (mut reader, mut writer) = tokio::io::split(server);

    let (armed_tx, armed_rx) = oneshot::channel();
    let read_task = tokio::spawn(async move {
        let mut buf = [0_u8; 1];
        park_then_signal(reader.read(&mut buf), armed_tx)
            .await
            .map_err(|error| error.kind())
    });
    armed_rx.await.expect("read task parks before revocation");
    fixture.revocations().revoke(client.serial.clone());

    let result = read_task.await.expect("read task completes");
    assert_eq!(result, Err(io::ErrorKind::ConnectionAborted));
    let error = writer
        .write_all(b"late")
        .await
        .expect_err("the write half must observe the revocation too");
    assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted);
    drop(writer);

    let mut buf = [0_u8; 1];
    let read = client_io.read(&mut buf).await;
    assert!(
        matches!(read, Ok(0)) || read.is_err(),
        "client must observe end of stream after the server drops the connection"
    );
}

#[tokio::test(start_paused = true)]
async fn revocation_leaves_other_clients_unaffected() {
    let fixture = TransportFixture::new();
    let revoked = fixture.client(4, "alpha");
    let other = fixture.client(5, "beta");

    let Connection {
        client: revoked_io,
        server: revoked_server,
    } = fixture.connect(&revoked).await;
    let revoked_io = revoked_io.expect("client handshake completes");
    let mut revoked_server = revoked_server.expect("server handshake completes");
    let Connection {
        client: other_io,
        server: other_server,
    } = fixture.connect(&other).await;
    let mut other_io = other_io.expect("client handshake completes");
    let mut other_server = other_server.expect("server handshake completes");

    fixture.revocations().revoke(revoked.serial.clone());

    echo_once(&mut other_io, &mut other_server).await;
    let error = revoked_server
        .read(&mut [0_u8; 1])
        .await
        .expect_err("revoked connection must not read");
    assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted);
    drop(revoked_io);

    let Connection {
        client: fresh_io,
        server: fresh_server,
    } = fixture.connect(&other).await;
    let mut fresh_io = fresh_io.expect("client handshake completes");
    let mut fresh_server = fresh_server.expect("server handshake completes");
    echo_once(&mut fresh_io, &mut fresh_server).await;
}

#[tokio::test(start_paused = true)]
async fn unrelated_revocation_changes_nothing() {
    let fixture = TransportFixture::new();
    let client = fixture.client(6, "alpha");
    let Connection {
        client: client_io,
        server,
    } = fixture.connect(&client).await;
    let mut client_io = client_io.expect("client handshake completes");
    let mut server = server.expect("server handshake completes");

    fixture.revocations().revoke(unrelated_serial());

    assert!(!fixture.revocations().is_revoked(&client.serial));
    echo_once(&mut client_io, &mut server).await;

    let Connection {
        client: fresh_io,
        server: fresh_server,
    } = fixture.connect(&client).await;
    let mut fresh_io = fresh_io.expect("client handshake completes");
    let mut fresh_server = fresh_server.expect("server handshake completes");
    echo_once(&mut fresh_io, &mut fresh_server).await;
    fresh_io.shutdown().await.expect("client shutdown succeeds");
}

#[tokio::test(start_paused = true)]
async fn serial_loaded_from_set_is_refused() {
    let (fixture, client) = TransportFixture::with_revoked_client(7, "alpha");
    assert!(fixture.revocations().is_revoked(&client.serial));

    let Connection {
        client: client_io,
        server,
    } = fixture.connect(&client).await;

    assert!(
        server.is_err(),
        "serial loaded through RevocationSet::from_serials must be refused"
    );
    let mut client_io = client_io.expect("TLS 1.3 client handshake completes before the rejection");
    assert!(client_io.read(&mut [0_u8; 1]).await.is_err());
}
