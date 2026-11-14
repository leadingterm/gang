//! A headless Chromium driven over the `DevTools` protocol, for what only a real browser enforces, such as the Content-Security-Policy.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, ErrorKind};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use gang_types::Url;
use serde_json::{Value, json};
use tungstenite::{Message, WebSocket};

use crate::errors::{E2eError, Result};

/// `DevTools` announcement prefix on Chromium's standard error.
const DEVTOOLS_PREFIX: &str = "DevTools listening on ";
/// `DevTools` target creation method.
const CREATE_TARGET: &str = "Target.createTarget";
/// `DevTools` target attachment method.
const ATTACH_TARGET: &str = "Target.attachToTarget";
/// Blank page used before navigation.
const BLANK: &str = "about:blank";
/// `DevTools` keyboard event dispatch method.
const DISPATCH_KEY: &str = "Input.dispatchKeyEvent";
/// `DevTools` name of the Enter key.
const ENTER: &str = "Enter";
/// Windows virtual key code for Enter.
const ENTER_KEY_CODE: u8 = 13;

/// Where the page went after an interaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Navigation {
    /// The main frame committed this URL.
    Committed(Url),
    /// The browser refused a navigation under the page's Content-Security-Policy; its console message.
    Refused(String),
    /// Neither happened before the deadline.
    Stalled,
}

/// A Chromium process and its attached `DevTools` page.
pub struct Chromium {
    child: Child,
    socket: WebSocket<TcpStream>,
    session: String,
    next_id: u64,
    events: VecDeque<Value>,
    deadline: Instant,
    _profile: tempfile::TempDir,
}

impl Chromium {
    /// Starts `program` headless with a throwaway profile and attaches to a blank page; every later call fails once `deadline` passes.
    ///
    /// # Errors
    ///
    /// When Chromium or its `DevTools` session cannot be started.
    pub fn launch(program: &Path, deadline: Instant) -> Result<Self> {
        let profile = tempfile::tempdir().map_err(E2eError::io("creating the Chromium profile"))?;
        let mut child = Command::new(program)
            .args([
                "--headless",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-gpu",
                "--disable-extensions",
                "--remote-debugging-port=0",
            ])
            .arg(format!("--user-data-dir={}", profile.path().display()))
            // The server's certificate chains to the test CA, which this throwaway profile does not trust.
            .arg("--ignore-certificate-errors")
            .arg(BLANK)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(E2eError::io("starting Chromium"))?;
        let socket = match connect(&mut child, deadline) {
            Ok(socket) => socket,
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(error);
            }
        };
        let mut browser = Self {
            child,
            socket,
            session: String::new(),
            next_id: 1,
            events: VecDeque::new(),
            deadline,
            _profile: profile,
        };
        let target = browser.call(CREATE_TARGET, &json!({"url": BLANK}))?;
        let target_id = target
            .get("targetId")
            .and_then(Value::as_str)
            .ok_or_else(|| E2eError::system("devtools did not return a target id"))?;
        let attached = browser.call(
            ATTACH_TARGET,
            &json!({"targetId": target_id, "flatten": true}),
        )?;
        attached
            .get("sessionId")
            .and_then(Value::as_str)
            .ok_or_else(|| E2eError::system("devtools did not return a session id"))?
            .clone_into(&mut browser.session);
        browser.call("Page.enable", &json!({}))?;
        browser.call("Log.enable", &json!({}))?;
        browser.call("Runtime.enable", &json!({}))?;
        Ok(browser)
    }

    /// Reads one JSON message, or returns `None` at the deadline.
    fn read_message(&mut self) -> Result<Option<Value>> {
        loop {
            let remaining = self.deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }
            self.socket
                .get_mut()
                .set_read_timeout(Some(remaining))
                .map_err(E2eError::io("setting the DevTools timeout"))?;
            match self.socket.read() {
                Ok(Message::Text(text)) => {
                    return serde_json::from_str(&text)
                        .map(Some)
                        .map_err(E2eError::system_from("parsing a DevTools message"));
                }
                Ok(_) => {}
                Err(tungstenite::Error::Io(error))
                    if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) =>
                {
                    return Ok(None);
                }
                Err(error) => return Err(E2eError::system_from("reading DevTools")(error)),
            }
        }
    }

    /// Executes one `DevTools` command while retaining its asynchronous events.
    fn call(&mut self, method: &str, params: &Value) -> Result<Value> {
        if Instant::now() >= self.deadline {
            return Err(E2eError::system(format!("devtools {method} timed out")));
        }
        let id = self.next_id;
        self.next_id += 1;
        let mut command = json!({"id": id, "method": method, "params": params});
        if method != CREATE_TARGET && method != ATTACH_TARGET {
            command["sessionId"] = self.session.clone().into();
        }
        self.socket
            .send(Message::Text(command.to_string().into()))
            .map_err(E2eError::system_from("sending a DevTools command"))?;
        while let Some(message) = self.read_message()? {
            if message.get("method").is_some() {
                self.events.push_back(message);
                continue;
            }
            if message.get("id").and_then(Value::as_u64) == Some(id) {
                if let Some(error) = message.get("error") {
                    let text = error
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown error");
                    return Err(E2eError::system(format!("devtools {method}: {text}")));
                }
                return Ok(message.get("result").cloned().unwrap_or(Value::Null));
            }
        }
        Err(E2eError::system(format!("devtools {method} timed out")))
    }

    /// Retrieves the next queued or incoming `DevTools` event.
    fn next_event(&mut self) -> Result<Option<Value>> {
        if let Some(event) = self.events.pop_front() {
            return Ok(Some(event));
        }
        while let Some(message) = self.read_message()? {
            if message.get("method").is_some() {
                return Ok(Some(message));
            }
        }
        Ok(None)
    }

    /// Navigates the page to `url` and waits for its load event.
    ///
    /// # Errors
    ///
    /// When navigation fails or the page does not load.
    pub fn open(&mut self, url: &Url) -> Result<()> {
        let result = self.call("Page.navigate", &json!({"url": url.as_str()}))?;
        if let Some(error) = result
            .get("errorText")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        {
            return Err(E2eError::system(format!("page navigation: {error}")));
        }
        while let Some(event) = self.next_event()? {
            if event.get("method").and_then(Value::as_str) == Some("Page.loadEventFired") {
                return Ok(());
            }
        }
        Err(E2eError::system("the page did not load"))
    }

    /// Evaluates `expression` in the page, awaiting a returned promise, and returns its JSON value.
    ///
    /// # Errors
    ///
    /// When evaluation fails or the script throws.
    pub fn evaluate(&mut self, expression: &str) -> Result<Value> {
        let result = self.call(
            "Runtime.evaluate",
            &json!({"expression": expression, "awaitPromise": true, "returnByValue": true}),
        )?;
        if let Some(exception) = result.get("exceptionDetails") {
            let text = exception
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or("unknown exception");
            return Err(E2eError::system(format!("page script threw: {text}")));
        }
        Ok(result
            .pointer("/result/value")
            .cloned()
            .unwrap_or(Value::Null))
    }

    /// Types `text` into the focused element, as a paste or an input method commit does.
    ///
    /// # Errors
    ///
    /// When the input command fails.
    pub fn insert_text(&mut self, text: &str) -> Result<()> {
        self.call("Input.insertText", &json!({"text": text}))?;
        Ok(())
    }

    /// Presses and releases Enter in the focused element.
    ///
    /// # Errors
    ///
    /// When a key event fails.
    pub fn press_enter(&mut self) -> Result<()> {
        self.call(DISPATCH_KEY, &json!({"type":"keyDown", "key":ENTER, "code":ENTER, "windowsVirtualKeyCode":ENTER_KEY_CODE, "text":"\r"}))?;
        self.call(DISPATCH_KEY, &json!({"type":"keyUp", "key":ENTER, "code":ENTER, "windowsVirtualKeyCode":ENTER_KEY_CODE}))?;
        Ok(())
    }

    /// Waits for the main frame's next committed navigation or a Content-Security-Policy refusal, until the deadline.
    ///
    /// # Errors
    ///
    /// When `DevTools` fails or a committed URL is invalid.
    pub fn next_navigation(&mut self) -> Result<Navigation> {
        while let Some(event) = self.next_event()? {
            match event.get("method").and_then(Value::as_str) {
                Some("Page.frameNavigated") => {
                    if let Some(frame) = event.pointer("/params/frame")
                        && frame.get("parentId").is_none()
                        && let Some(url) = frame.get("url").and_then(Value::as_str)
                    {
                        return Url::parse(url)
                            .map(Navigation::Committed)
                            .map_err(E2eError::system_from("parsing the committed URL"));
                    }
                }
                Some("Log.entryAdded") => {
                    if let Some(text) = event.pointer("/params/entry/text").and_then(Value::as_str)
                        && text.contains("Content Security Policy")
                    {
                        return Ok(Navigation::Refused(text.to_owned()));
                    }
                }
                _ => {}
            }
        }
        Ok(Navigation::Stalled)
    }
}

/// Connects to the browser endpoint announced on standard error, keeping that pipe drained.
fn connect(child: &mut Child, deadline: Instant) -> Result<WebSocket<TcpStream>> {
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| E2eError::system("chromium stderr is unavailable"))?;
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let mut sender = Some(sender);
        let mut reader = BufReader::new(stderr);
        let mut buffer = Vec::new();
        loop {
            buffer.clear();
            match reader.read_until(b'\n', &mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
            let line = String::from_utf8_lossy(&buffer);
            if let Some(url) = line.strip_prefix(DEVTOOLS_PREFIX)
                && let Some(sender) = sender.take()
            {
                let _ = sender.send(url.trim_end_matches(['\r', '\n']).to_owned());
            }
        }
    });
    let remaining = deadline.saturating_duration_since(Instant::now());
    let ws = receiver
        .recv_timeout(remaining)
        .map_err(|_| E2eError::system("chromium did not announce its DevTools endpoint"))?;
    let url = Url::parse(&ws).map_err(E2eError::system_from("parsing the DevTools endpoint"))?;
    let host = url
        .host_str()
        .ok_or_else(|| E2eError::system("the DevTools endpoint has no host"))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| E2eError::system("the DevTools endpoint has no port"))?;
    let stream =
        TcpStream::connect((host, port)).map_err(E2eError::io("connecting to DevTools"))?;
    stream
        .set_read_timeout(Some(
            deadline
                .saturating_duration_since(Instant::now())
                .max(std::time::Duration::from_nanos(1)),
        ))
        .map_err(E2eError::io("setting the DevTools timeout"))?;
    let (socket, _) = tungstenite::client::client(ws.as_str(), stream).map_err(
        E2eError::system_from("connecting to the DevTools websocket"),
    )?;
    Ok(socket)
}

impl Drop for Chromium {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
