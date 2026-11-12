//! The terminal side of an interactive login: showing the authorization URL, starting the browser,
//! and reading a redirect URL the user pastes when the browser cannot reach this machine.

use std::io;
use std::process::{Command, Stdio};
use std::sync::OnceLock;

use gang_client::{ClientOptions, Console, DEFAULT_BROWSER_COMMAND};
use gang_types::Url;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::output::{Line, Output, Progress, Record};

/// The console of a process attached to a terminal: writes its instructions through [`Output`],
/// starts the browser, and reads pasted redirect URLs from stdin.
///
/// Stdin is not read unless an interactive login waits for a pasted URL, and the reader is a
/// detached standard thread so it never blocks process exit. Stdout is never written. The wait
/// line shown after the instructions lasts until the console is dropped.
pub(crate) struct TerminalConsole {
    browser_command: String,
    out: Output,
    paste: OnceLock<tokio::sync::Mutex<mpsc::UnboundedReceiver<Url>>>,
    waiting: Mutex<Option<Progress>>,
}

impl TerminalConsole {
    /// Builds the console. `options.browser_command` overrides the platform default.
    pub(crate) fn new(options: &ClientOptions, out: Output) -> Self {
        let browser_command = options
            .browser_command
            .clone()
            .unwrap_or_else(|| DEFAULT_BROWSER_COMMAND.to_owned());
        Self {
            browser_command,
            out,
            paste: OnceLock::new(),
            waiting: Mutex::new(None),
        }
    }

    /// Announces the URL, starting the browser with `spawn(command, url)`.
    fn announce_with(&self, url: &Url, spawn: impl FnOnce(&str, &str) -> io::Result<()>) {
        let browser_started = match spawn(&self.browser_command, url.as_str()) {
            Ok(()) => true,
            Err(error) => {
                self.out.step(
                    &Record::warn(format!("cannot start the browser {}", self.browser_command))
                        .cause(error.to_string())
                        .fix("open the address below yourself"),
                );
                self.out.blank();
                false
            }
        };
        // Keep framing visible in quiet mode only when the browser could not be started.
        let show = |line: Line| {
            if browser_started {
                self.out.note(line);
            } else {
                self.out.notice(line);
            }
        };
        show(Line::new().plain(if browser_started {
            "continue in the browser that just opened:"
        } else {
            "log in at:"
        }));
        self.out.notice(Line::new().plain(url.as_str()));
        show(
            Line::new()
                .muted("browser on another machine? paste the address it fails to load here"),
        );
        self.out.decoration(None, &[Line::new()]);
        let progress = self.out.progress("waiting for the browser");
        *self.waiting.lock() = Some(progress);
    }
}

impl Console for TerminalConsole {
    fn announce(&self, url: &Url) {
        self.announce_with(url, spawn_browser);
    }

    fn pasted_redirect(&self) -> impl Future<Output = Url> + Send {
        let receiver = self.paste.get_or_init(|| {
            let (sender, receiver) = mpsc::unbounded_channel();
            let out = self.out.clone();
            std::thread::spawn(move || read_pastes(&sender, &out));
            tokio::sync::Mutex::new(receiver)
        });
        async move {
            let mut receiver = receiver.lock().await;
            match receiver.recv().await {
                Some(url) => url,
                None => std::future::pending().await,
            }
        }
    }
}

/// Starts `command` with the URL as its only argument, detached from this process's streams.
/// A detached thread reaps the opener so it does not become a zombie while login waits.
fn spawn_browser(command: &str, url: &str) -> io::Result<()> {
    let mut child = Command::new(command)
        .arg(url)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    std::thread::Builder::new()
        .name("browser-opener".into())
        .spawn(move || child.wait())?;
    Ok(())
}

/// Reads stdin lines until end of input, forwarding every line that parses as a URL.
fn read_pastes(sender: &mpsc::UnboundedSender<Url>, out: &Output) {
    for line in io::stdin().lines() {
        let Ok(line) = line else {
            return;
        };
        let Some(url) = parse_paste(&line, out) else {
            continue;
        };
        if sender.send(url).is_err() {
            return;
        }
    }
}

/// Parses one pasted line; blank lines are ignored and a line that is not a URL is reported.
fn parse_paste(line: &str, out: &Output) -> Option<Url> {
    let text = line.trim();
    if text.is_empty() {
        return None;
    }
    match Url::parse(text) {
        Ok(url) => Some(url),
        Err(error) => {
            out.step(
                &Record::warn("not a URL")
                    .cause(error.to_string())
                    .fix("paste the address the browser failed to load"),
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::output::text_of as lines;
    use crate::palette::{Palette, strip_ansi};

    use super::*;

    const AUTH_URL: &str = "https://accounts.example.com/auth?state=abc&scope=openid";

    fn url() -> Url {
        Url::parse(AUTH_URL).expect("a valid URL")
    }

    fn console(
        options: &ClientOptions,
    ) -> (
        TerminalConsole,
        crate::output::Capture,
        crate::output::Capture,
    ) {
        let (out, stdout, stderr) = Output::captured();
        (TerminalConsole::new(options, out), stdout, stderr)
    }

    #[test]
    fn configured_browser_command_overrides_the_default() {
        let options = ClientOptions {
            browser_command: Some("firefox".to_owned()),
            ..ClientOptions::default()
        };
        assert_eq!(console(&options).0.browser_command, "firefox");
    }

    #[test]
    fn default_browser_command_applies_without_an_override() {
        let (console, _, _) = console(&ClientOptions::default());
        assert_eq!(console.browser_command, DEFAULT_BROWSER_COMMAND);
    }

    #[test]
    fn announce_with_a_started_browser_prints_the_waiting_screen() {
        let (console, stdout, stderr) = console(&ClientOptions::default());
        let mut started = None;
        console.announce_with(&url(), |command, address| {
            started = Some((command.to_owned(), address.to_owned()));
            Ok(())
        });
        assert_eq!(
            started,
            Some((DEFAULT_BROWSER_COMMAND.to_owned(), AUTH_URL.to_owned()))
        );
        assert_eq!(
            stderr.text(),
            lines(&[
                "        continue in the browser that just opened:",
                &format!("        {AUTH_URL}"),
                "        browser on another machine? paste the address it fails to load here",
            ])
        );
        assert_eq!(stdout.text(), "");
    }

    #[test]
    fn on_a_terminal_a_blank_row_separates_the_notes_from_the_wait_line() {
        let (out, _, stderr) = Output::captured_styled(Palette::Ansi, 80);
        let console = TerminalConsole::new(&ClientOptions::default(), out);
        console.announce_with(&url(), |_, _| Ok(()));
        assert_eq!(
            without_escapes(&stderr.text()),
            lines(&[
                "        continue in the browser that just opened:",
                &format!("        {AUTH_URL}"),
                "        browser on another machine? paste the address it fails to load here",
                "",
            ])
        );
    }

    #[test]
    fn on_a_terminal_a_failed_browser_screen_keeps_both_blank_rows() {
        let (out, _, stderr) = Output::captured_styled(Palette::Ansi, 80);
        let console = TerminalConsole::new(
            &ClientOptions {
                browser_command: Some("xdg-open".to_owned()),
                ..ClientOptions::default()
            },
            out,
        );
        console.announce_with(&url(), |_, _| Err(io::Error::from_raw_os_error(2)));
        assert_eq!(
            without_escapes(&stderr.text()),
            lines(&[
                "  warn  cannot start the browser xdg-open",
                "        ↳ No such file or directory (os error 2)",
                "   fix  open the address below yourself",
                "",
                "        log in at:",
                &format!("        {AUTH_URL}"),
                "        browser on another machine? paste the address it fails to load here",
                "",
            ])
        );
    }

    #[test]
    fn announce_with_a_failed_browser_warns_and_asks_to_open_the_address() {
        let (console, stdout, stderr) = console(&ClientOptions {
            browser_command: Some("xdg-open".to_owned()),
            ..ClientOptions::default()
        });
        console.announce_with(&url(), |_, _| Err(io::Error::from_raw_os_error(2)));
        assert_eq!(
            stderr.text(),
            lines(&[
                "  warn  cannot start the browser xdg-open",
                "        ↳ No such file or directory (os error 2)",
                "   fix  open the address below yourself",
                "",
                "        log in at:",
                &format!("        {AUTH_URL}"),
                "        browser on another machine? paste the address it fails to load here",
            ])
        );
        assert_eq!(stdout.text(), "");
    }

    #[test]
    fn quiet_still_shows_the_address_when_the_browser_cannot_start() {
        let (out, stdout, stderr) = Output::captured();
        out.set_quiet(true);
        let console = TerminalConsole::new(&ClientOptions::default(), out);
        console.announce_with(&url(), |_, _| Err(io::Error::from_raw_os_error(2)));
        assert_eq!(
            stderr.text(),
            lines(&[
                &format!("  warn  cannot start the browser {DEFAULT_BROWSER_COMMAND}"),
                "        ↳ No such file or directory (os error 2)",
                "   fix  open the address below yourself",
                "        log in at:",
                &format!("        {AUTH_URL}"),
                "        browser on another machine? paste the address it fails to load here",
            ])
        );
        assert_eq!(stdout.text(), "");
    }

    #[test]
    fn quiet_still_shows_the_address_when_the_browser_started() {
        let (out, stdout, stderr) = Output::captured();
        out.set_quiet(true);
        let console = TerminalConsole::new(&ClientOptions::default(), out);
        console.announce_with(&url(), |_, _| Ok(()));
        assert_eq!(stderr.text(), lines(&[&format!("        {AUTH_URL}")]));
        assert_eq!(stdout.text(), "");
    }

    #[test]
    fn a_pasted_url_is_accepted_with_surrounding_whitespace() {
        let (out, _, stderr) = Output::captured();
        assert_eq!(parse_paste(&format!("  {AUTH_URL}\n"), &out), Some(url()));
        assert_eq!(stderr.text(), "");
    }

    #[test]
    fn a_blank_pasted_line_is_ignored_silently() {
        let (out, _, stderr) = Output::captured();
        assert_eq!(parse_paste("   ", &out), None);
        assert_eq!(stderr.text(), "");
    }

    #[test]
    fn a_pasted_line_that_is_not_a_url_is_reported() {
        let (out, _, stderr) = Output::captured();
        assert_eq!(parse_paste("not a url", &out), None);
        assert_eq!(
            stderr.text(),
            lines(&[
                "  warn  not a URL",
                "        ↳ relative URL without a base",
                "   fix  paste the address the browser failed to load",
            ])
        );
    }

    /// `text` without ANSI CSI sequences and carriage returns.
    fn without_escapes(text: &str) -> String {
        strip_ansi(text).replace('\r', "")
    }
}
