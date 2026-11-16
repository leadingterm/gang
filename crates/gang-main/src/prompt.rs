//! Line-oriented prompts for the setup wizard.

use std::io::{self, BufRead, Read};
use std::sync::LazyLock;

use tokio::sync::{Mutex, mpsc};

use crate::errors::{MainError, Result};
use crate::output::{Line, Output};

/// Maximum number of bytes read for a single input line, including its newline.
const INPUT_LINE_LIMIT: usize = 4096;

/// Reads one bounded line, preserving the queue's newline-terminated representation.
fn capped_line(reader: &mut impl BufRead) -> io::Result<Option<String>> {
    let mut bytes = Vec::new();
    reader
        .take((INPUT_LINE_LIMIT + 1) as u64)
        .read_until(b'\n', &mut bytes)?;
    if bytes.len() > INPUT_LINE_LIMIT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "input line too long",
        ));
    }
    if bytes.is_empty() {
        return Ok(None);
    }
    let mut line = String::from_utf8(bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if line.ends_with('\n') {
        line.pop();
        if line.ends_with('\r') {
            line.pop();
        }
    }
    line.push('\n');
    Ok(Some(line))
}

/// The single process-wide queue of lines read from standard input.
static INPUT: LazyLock<Mutex<mpsc::Receiver<io::Result<String>>>> = LazyLock::new(|| {
    let (sender, receiver) = mpsc::channel(1);
    std::thread::spawn(move || {
        let mut input = io::stdin().lock();
        loop {
            let line = match capped_line(&mut input) {
                Ok(Some(line)) => Ok(line),
                Ok(None) => break,
                Err(error) => Err(error),
            };
            let failed = line.is_err();
            if sender.blocking_send(line).is_err() || failed {
                break;
            }
        }
    });
    Mutex::new(receiver)
});

/// Reads the next line asynchronously without stealing input after cancellation.
pub(crate) async fn input_line() -> Option<io::Result<String>> {
    INPUT.lock().await.recv().await
}

/// Buffered synchronous access to the same input queue used by login.
#[derive(Default)]
pub(crate) struct SharedInput {
    /// The current line.
    line: Vec<u8>,
    /// Number of bytes already consumed from the line.
    position: usize,
}

impl Read for SharedInput {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let available = self.fill_buf()?;
        let count = available.len().min(buf.len());
        buf[..count].copy_from_slice(&available[..count]);
        self.consume(count);
        Ok(count)
    }
}

impl BufRead for SharedInput {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.position == self.line.len() {
            let line = INPUT
                .blocking_lock()
                .blocking_recv()
                .transpose()?
                .unwrap_or_default();
            self.line = line.into_bytes();
            self.position = 0;
        }
        Ok(&self.line[self.position..])
    }

    fn consume(&mut self, amount: usize) {
        self.position = self.position.saturating_add(amount).min(self.line.len());
    }
}

/// One possible response to a choice prompt.
pub(crate) struct Choice {
    /// The case-insensitive response key.
    pub key: char,
    /// The displayed choice text.
    pub label: &'static str,
    /// Whether an empty answer selects this choice.
    pub default: bool,
}

/// Reads answers from a buffered input stream.
pub(crate) struct Prompt<R> {
    /// The source of answers.
    input: R,
}

impl<R: BufRead> Prompt<R> {
    /// Creates a prompt using the given input.
    pub(crate) fn new(input: R) -> Self {
        Self { input }
    }

    /// Reads an answer, substituting the default for an empty line.
    pub(crate) fn ask(&mut self, out: &Output, label: &str, default: &str) -> Result<String> {
        let text = if default.is_empty() {
            format!("{label}: ")
        } else {
            format!("{label} [{default}]: ")
        };
        out.prompt(&Line::from(text))?;
        let mut answer = String::new();
        if self
            .input
            .read_line(&mut answer)
            .map_err(MainError::Stdin)?
            == 0
        {
            return Err(MainError::PromptEof);
        }
        let answer = answer.trim();
        Ok(if answer.is_empty() { default } else { answer }.to_owned())
    }

    /// Repeats a choice until the answer matches one of the offered keys.
    pub(crate) fn choose(
        &mut self,
        out: &Output,
        question: &str,
        choices: &[Choice],
    ) -> Result<char> {
        let labels = choices
            .iter()
            .map(|choice| choice.label)
            .collect::<Vec<_>>()
            .join(" / ");
        let label = format!("{question} {labels}");
        loop {
            let answer = self.ask(out, &label, "")?;
            if answer.is_empty() {
                if let Some(choice) = choices.iter().find(|choice| choice.default) {
                    return Ok(choice.key);
                }
            } else if let Some(choice) = choices.iter().find(|choice| {
                answer.len() == 1 && answer.eq_ignore_ascii_case(&choice.key.to_string())
            }) {
                return Ok(choice.key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::{Choice, Prompt, capped_line};
    use crate::output::Output;

    /// Oversized input is rejected instead of buffering an unbounded line.
    #[test]
    fn input_line_is_bounded() {
        let mut input = Cursor::new(vec![b'a'; 5000]);
        let result = capped_line(&mut input);
        assert!(
            result.is_err(),
            "input line longer than 4096 bytes must fail"
        );
        let error = result.expect_err("oversized input");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "input line too long");
    }

    /// Defaults and invalid choices are handled without consuming later answers.
    #[test]
    fn answers_default_and_reprompt() {
        let (out, _, stderr) = Output::captured();
        let mut prompt = Prompt::new(Cursor::new(b"\ninvalid\nn\n"));
        assert_eq!(
            prompt.ask(&out, "server", "example").expect("answer"),
            "example"
        );
        let choices = [
            Choice {
                key: 'y',
                label: "[Y]es",
                default: true,
            },
            Choice {
                key: 'n',
                label: "[n]o",
                default: false,
            },
        ];
        assert_eq!(
            prompt.choose(&out, "apply?", &choices).expect("choice"),
            'n'
        );
        assert!(stderr.text().contains("server [example]: "));
        assert_eq!(stderr.text().matches("apply?").count(), 2);
    }

    /// End of input is not interpreted as consent.
    #[test]
    fn eof_never_accepts_default() {
        let (out, _, _) = Output::captured();
        let mut prompt = Prompt::new(Cursor::new(b""));
        assert!(prompt.ask(&out, "server", "example").is_err());
    }
}
