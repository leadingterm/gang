//! The loopback redirect listener of an interactive login.
//!
//! The identity provider redirects the browser to `http://127.0.0.1:<port>/callback`. This
//! module answers that request with a small page and returns the redirect URL to the login.
//! It speaks just enough HTTP/1.x for one `GET`: the request head is read up to the caller's cap,
//! the request line is parsed, and every answer closes the connection.

use std::time::Duration;

use gang_clock::Clock;
use gang_types::Url;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::errors::ClientError;

/// The address the listener binds. Only the loopback interface is ever reachable.
const LOOPBACK_HOST: &str = "127.0.0.1";
/// The path the identity provider redirects to.
const CALLBACK_PATH: &str = "/callback";
/// The read granularity while a request head is assembled, in bytes.
const READ_CHUNK: usize = 1024;
/// Ends an HTTP request head.
const HEAD_END: &[u8] = b"\r\n\r\n";
/// The status line of the answer to the callback.
const OK_STATUS: &str = "200 OK";
/// The status line of every other answer.
const NOT_FOUND_STATUS: &str = "404 Not Found";
/// The status line of a request head that does not end within the caller's cap.
const OVERSIZED_STATUS: &str = "431 Request Header Fields Too Large";
/// The page shown in the browser once the callback has been received.
const DONE_PAGE: &str = r#"<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Login complete</title></head>
<body><p>Login complete. You can close this tab and return to the terminal.</p></body>
</html>
"#;
/// The body of every answer other than the callback.
const NOT_FOUND_PAGE: &str = "Not Found\n";
/// The body of the answer to a request head that does not end within the caller's cap.
const OVERSIZED_PAGE: &str = "Request Header Fields Too Large\n";

/// The loopback listener of one interactive login.
pub(crate) struct LoopbackListener {
    listener: TcpListener,
    redirect_uri: Url,
}

impl LoopbackListener {
    /// Binds the loopback interface on a port the operating system chooses.
    pub(crate) async fn bind() -> Result<Self, ClientError> {
        let listener = TcpListener::bind((LOOPBACK_HOST, 0))
            .await
            .map_err(ClientError::Loopback)?;
        let port = listener.local_addr().map_err(ClientError::Loopback)?.port();
        let redirect_uri = Url::parse(&format!("http://{LOOPBACK_HOST}:{port}{CALLBACK_PATH}"))?;
        Ok(Self {
            listener,
            redirect_uri,
        })
    }

    /// The redirect URI to register with the identity provider.
    pub(crate) fn redirect_uri(&self) -> &Url {
        &self.redirect_uri
    }

    /// Serves connections one at a time until the browser delivers the callback, and returns
    /// the callback URL. A connection that times out, closes without a request, or is malformed
    /// is dropped. A request head that does not end within `max_head` bytes is answered `431`;
    /// any other request is answered `404` and waited past.
    pub(crate) async fn callback(
        self,
        clock: &impl Clock,
        read_timeout: Duration,
        max_head: usize,
    ) -> Result<Url, ClientError> {
        loop {
            let (mut stream, _) = self
                .listener
                .accept()
                .await
                .map_err(ClientError::Loopback)?;
            if let Some(url) = self.serve(clock, &mut stream, read_timeout, max_head).await {
                return Ok(url);
            }
        }
    }

    /// Handles one connection. Returns the callback URL when the connection carries the redirect.
    async fn serve(
        &self,
        clock: &impl Clock,
        stream: &mut TcpStream,
        read_timeout: Duration,
        max_head: usize,
    ) -> Option<Url> {
        let head = Clock::timeout(clock, read_timeout, read_head(stream, max_head))
            .await
            .ok()??;
        let head = match head {
            Head::Complete(head) => head,
            Head::Oversized => {
                respond(stream, OVERSIZED_STATUS, OVERSIZED_PAGE).await;
                let _ = Clock::timeout(clock, read_timeout, discard_rest(stream)).await;
                return None;
            }
        };
        match parse_request(&head, &self.redirect_uri) {
            Request::Callback(url) => {
                respond(stream, OK_STATUS, DONE_PAGE).await;
                Some(url)
            }
            Request::Other => {
                respond(stream, NOT_FOUND_STATUS, NOT_FOUND_PAGE).await;
                None
            }
            Request::Malformed => None,
        }
    }
}

/// What one request head asks for.
enum Request {
    /// `GET /callback?<query>`: the redirect URI with the query of the request.
    Callback(Url),
    /// A well-formed request that is not the callback.
    Other,
    /// Not an HTTP/1.x request head.
    Malformed,
}

/// What reading one request head produced.
enum Head {
    /// The head, through its terminator.
    Complete(Vec<u8>),
    /// No terminator within the cap.
    Oversized,
}

/// Reads a request head up to its terminator. `None` when the peer closes or fails first. A head
/// that does not end within `max_head` bytes is `Oversized`.
async fn read_head(stream: &mut TcpStream, max_head: usize) -> Option<Head> {
    let mut head = Vec::new();
    let mut chunk = [0_u8; READ_CHUNK];
    loop {
        let room = (max_head - head.len()).min(READ_CHUNK);
        if room == 0 {
            return Some(Head::Oversized);
        }
        let read = stream.read(&mut chunk[..room]).await.ok()?;
        if read == 0 {
            return None;
        }
        head.extend_from_slice(&chunk[..read]);
        if head
            .windows(HEAD_END.len())
            .any(|window| window == HEAD_END)
        {
            return Some(Head::Complete(head));
        }
    }
}

/// Reads and discards what the peer still sends, so that closing the connection does not reset
/// it before the answer is read. Ends when the peer closes or fails.
async fn discard_rest(stream: &mut TcpStream) {
    let mut chunk = [0_u8; READ_CHUNK];
    loop {
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

/// Classifies a request head against the callback path.
fn parse_request(head: &[u8], redirect_uri: &Url) -> Request {
    let Some(line) = request_line(head) else {
        return Request::Malformed;
    };
    let mut parts = line.split(' ');
    let (Some(method), Some(target), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Request::Malformed;
    };
    if !version.starts_with("HTTP/1.") {
        return Request::Malformed;
    }
    if method != "GET" {
        return Request::Other;
    }
    let Some((path, query)) = target.split_once('?') else {
        return Request::Other;
    };
    if path != CALLBACK_PATH || query.is_empty() {
        return Request::Other;
    }
    let mut url = redirect_uri.clone();
    url.set_query(Some(query));
    Request::Callback(url)
}

/// The first line of a request head, when it is valid UTF-8 and terminated by CRLF.
fn request_line(head: &[u8]) -> Option<&str> {
    let end = head.windows(2).position(|pair| pair == b"\r\n")?;
    std::str::from_utf8(&head[..end]).ok()
}

/// Writes one HTML answer and closes the connection. A peer that has already gone away is
/// not an error: the callback is still delivered.
async fn respond(stream: &mut TcpStream, status: &str, body: &str) {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len(),
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.flush().await;
    let _ = stream.shutdown().await;
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use gang_clock::TestClock;
    use gang_types::Url;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::task::JoinHandle;

    use super::LoopbackListener;
    use crate::errors::ClientError;
    use crate::options::ClientOptions;

    /// A head cap generous enough for every request the tests send whole: the default.
    fn default_max_head() -> usize {
        ClientOptions::default()
            .callback_max_head_size
            .get()
            .as_usize()
    }

    /// Binds a listener with the given head cap and starts serving its callback in the background.
    async fn listen_with(max_head: usize) -> (Url, JoinHandle<Result<Url, ClientError>>) {
        let listener = LoopbackListener::bind().await.expect("bind");
        let redirect = listener.redirect_uri().clone();
        (
            redirect,
            tokio::spawn(async move {
                listener
                    .callback(
                        &TestClock::default(),
                        ClientOptions::default().callback_read_timeout.get(),
                        max_head,
                    )
                    .await
            }),
        )
    }

    /// Binds a listener and starts serving its callback in the background.
    async fn listen() -> (Url, JoinHandle<Result<Url, ClientError>>) {
        listen_with(default_max_head()).await
    }

    /// Sends raw bytes to the listener and returns everything it answers before closing.
    async fn exchange(redirect: &Url, raw: &[u8]) -> String {
        let port = redirect.port().expect("loopback port");
        let mut stream = TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connect");
        stream.write_all(raw).await.expect("write request");
        // Closing the request side lets a head beyond the cap end at end of input, so no test
        // waits out the read timeout.
        stream.shutdown().await.expect("close the request side");
        let mut answer = String::new();
        stream
            .read_to_string(&mut answer)
            .await
            .expect("read answer");
        answer
    }

    /// Waits for the callback and returns the URL it delivered.
    async fn delivered(task: JoinHandle<Result<Url, ClientError>>) -> Url {
        task.await.expect("listener task").expect("callback")
    }

    const CALLBACK_REQUEST: &[u8] =
        b"GET /callback?code=abc&state=xyz HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n";

    #[tokio::test]
    async fn incomplete_request_times_out() {
        let listener = LoopbackListener::bind().await.expect("bind");
        let port = listener.redirect_uri().port().expect("loopback port");
        let _peer = TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connect");
        let (mut stream, _) = listener.listener.accept().await.expect("accept");
        let clock = TestClock::default();
        let timeout = Duration::from_secs(1);
        let (result, ()) = tokio::join!(
            listener.serve(&clock, &mut stream, timeout, default_max_head()),
            async {
                clock.advance(timeout);
            },
        );
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn oversized_request_discard_times_out() {
        let listener = LoopbackListener::bind().await.expect("bind");
        let port = listener.redirect_uri().port().expect("loopback port");
        let mut peer = TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connect");
        let (mut stream, _) = listener.listener.accept().await.expect("accept");
        peer.write_all(b"oversized request").await.expect("write");
        let clock = TestClock::default();
        let timeout = Duration::from_secs(1);
        let (result, ()) = tokio::join!(listener.serve(&clock, &mut stream, timeout, 4), async {
            let mut answer = [0; 128];
            let count = peer.read(&mut answer).await.expect("read answer");
            assert!(answer[..count].starts_with(b"HTTP/1.1 431"));
            clock.advance(timeout);
        },);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn callback_returns_the_redirect_with_its_query() {
        let (redirect, task) = listen().await;
        let answer = exchange(&redirect, CALLBACK_REQUEST).await;
        assert!(answer.starts_with("HTTP/1.1 200 OK"), "{answer}");
        let url = delivered(task).await;
        assert_eq!(url.path(), "/callback");
        assert_eq!(url.query(), Some("code=abc&state=xyz"));
    }

    #[tokio::test]
    async fn callback_answer_closes_the_tab() {
        let (redirect, task) = listen().await;
        let answer = exchange(&redirect, CALLBACK_REQUEST).await;
        assert!(answer.starts_with("HTTP/1.1 200 OK"), "{answer}");
        assert!(answer.contains("Content-Type: text/html; charset=utf-8"));
        assert!(answer.contains("Connection: close"));
        assert!(answer.to_lowercase().contains("close this tab"), "{answer}");
        delivered(task).await;
    }

    #[tokio::test]
    async fn favicon_request_is_answered_404_and_waited_past() {
        let (redirect, task) = listen().await;
        let answer = exchange(
            &redirect,
            b"GET /favicon.ico HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
        )
        .await;
        assert!(answer.starts_with("HTTP/1.1 404 Not Found"), "{answer}");
        exchange(&redirect, CALLBACK_REQUEST).await;
        assert_eq!(delivered(task).await.query(), Some("code=abc&state=xyz"));
    }

    #[tokio::test]
    async fn callback_without_query_is_answered_404_and_waited_past() {
        let (redirect, task) = listen().await;
        let answer = exchange(&redirect, b"GET /callback HTTP/1.1\r\n\r\n").await;
        assert!(answer.starts_with("HTTP/1.1 404 Not Found"), "{answer}");
        exchange(&redirect, CALLBACK_REQUEST).await;
        assert_eq!(delivered(task).await.query(), Some("code=abc&state=xyz"));
    }

    #[tokio::test]
    async fn post_to_callback_is_answered_404_and_waited_past() {
        let (redirect, task) = listen().await;
        let answer = exchange(
            &redirect,
            b"POST /callback?code=x HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\n\r\n",
        )
        .await;
        assert!(answer.starts_with("HTTP/1.1 404 Not Found"), "{answer}");
        exchange(&redirect, CALLBACK_REQUEST).await;
        assert_eq!(delivered(task).await.query(), Some("code=abc&state=xyz"));
    }

    #[tokio::test]
    async fn callback_with_empty_query_is_answered_404_and_waited_past() {
        let (redirect, task) = listen().await;
        let answer = exchange(
            &redirect,
            b"GET /callback? HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
        )
        .await;
        assert!(answer.starts_with("HTTP/1.1 404 Not Found"), "{answer}");
        exchange(&redirect, CALLBACK_REQUEST).await;
        assert_eq!(delivered(task).await.query(), Some("code=abc&state=xyz"));
    }

    #[tokio::test]
    async fn http2_request_line_is_dropped_without_a_callback() {
        let (redirect, task) = listen().await;
        let answer = exchange(
            &redirect,
            b"GET /callback?code=x HTTP/2.0\r\nHost: 127.0.0.1\r\n\r\n",
        )
        .await;
        assert!(answer.is_empty(), "{answer}");
        exchange(&redirect, CALLBACK_REQUEST).await;
        assert_eq!(delivered(task).await.query(), Some("code=abc&state=xyz"));
    }

    #[tokio::test]
    async fn connection_closed_without_a_request_is_skipped() {
        let (redirect, task) = listen().await;
        let port = redirect.port().expect("loopback port");
        {
            let _stream = TcpStream::connect(("127.0.0.1", port))
                .await
                .expect("connect");
        }
        exchange(&redirect, CALLBACK_REQUEST).await;
        assert_eq!(delivered(task).await.query(), Some("code=abc&state=xyz"));
    }

    #[tokio::test]
    async fn garbage_bytes_are_skipped() {
        let (redirect, task) = listen().await;
        let answer = exchange(&redirect, b"\x00\xff not http at all\r\n\r\n").await;
        assert!(answer.is_empty(), "{answer}");
        exchange(&redirect, CALLBACK_REQUEST).await;
        assert_eq!(delivered(task).await.query(), Some("code=abc&state=xyz"));
    }

    #[tokio::test]
    async fn head_beyond_the_limit_is_answered_431_and_waited_past() {
        let (redirect, task) = listen_with(256).await;
        let padded = format!(
            "GET /favicon.ico HTTP/1.1\r\nHost: 127.0.0.1\r\nX-Padding: {}\r\n\r\n",
            "a".repeat(512)
        );
        let answer = exchange(&redirect, padded.as_bytes()).await;
        assert!(
            answer.starts_with("HTTP/1.1 431 Request Header Fields Too Large"),
            "{answer}"
        );
        exchange(&redirect, CALLBACK_REQUEST).await;
        assert_eq!(delivered(task).await.query(), Some("code=abc&state=xyz"));
    }

    #[tokio::test]
    async fn head_within_the_limit_is_accepted() {
        let (redirect, task) = listen_with(CALLBACK_REQUEST.len()).await;
        let answer = exchange(&redirect, CALLBACK_REQUEST).await;
        assert!(answer.starts_with("HTTP/1.1 200 OK"), "{answer}");
        assert_eq!(delivered(task).await.query(), Some("code=abc&state=xyz"));
    }

    #[tokio::test]
    async fn redirect_uri_names_the_loopback_callback_on_a_real_port() {
        let listener = LoopbackListener::bind().await.expect("bind");
        let redirect = listener.redirect_uri();
        let port = redirect.port().expect("loopback port");
        assert_ne!(port, 0);
        assert_eq!(
            redirect.as_str(),
            format!("http://127.0.0.1:{port}/callback")
        );
    }
}
