//! The live `wait` line: one line on a stderr terminal that shows what the command is waiting
//! for and for how long, redrawn once a second by a ticker thread.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::output::Shared;
use crate::palette::Role;
use crate::record::{COLUMN, Line, margin};
use crate::wrap::{char_columns, columns};

/// Time between redraws, and before the first one.
const TICK: Duration = Duration::from_secs(1);

/// The text of the wait line: `wait` in the margin, the label, then the whole seconds waited. The
/// line is at most `width - 1` columns wide, so it never reaches the last column, where a
/// terminal would wrap it and leave orphan rows behind when it is redrawn: a label that would
/// overflow is cut and ends with an ellipsis, and the seconds go too when even an empty label
/// leaves no room for them.
pub(crate) fn wait_line(label: &str, elapsed: Duration, width: Option<usize>) -> Line {
    let seconds = format!(" · {}s", elapsed.as_secs());
    let Some(width) = width else {
        return margin("wait", Role::Accent).plain(label).muted(seconds);
    };
    let limit = width.saturating_sub(1);
    let room = limit.saturating_sub(COLUMN + columns(&seconds));
    if room == 0 {
        // Too narrow for the seconds: the label alone, cut to what the margin leaves.
        let label = fit(label, limit.saturating_sub(COLUMN));
        return margin("wait", Role::Accent).plain(label);
    }
    margin("wait", Role::Accent)
        .plain(fit(label, room))
        .muted(seconds)
}

/// `label` cut to `room` columns, the last one an ellipsis when something was cut.
fn fit(label: &str, room: usize) -> String {
    if columns(label) <= room {
        return label.to_owned();
    }
    let mut kept = String::new();
    let mut used = 0;
    for c in label.chars() {
        let next = used + char_columns(c);
        if next >= room {
            break;
        }
        used = next;
        kept.push(c);
    }
    if room == 0 {
        kept
    } else {
        kept.push('…');
        kept
    }
}

/// A running wait line. Dropping it stops the ticker and clears the line.
pub(crate) struct Progress {
    /// When the wait began.
    start: Instant,
    /// The ticker; `None` for an inert progress that draws nothing.
    ticker: Option<Ticker>,
}

/// The thread that redraws the line and the means to stop it.
struct Ticker {
    /// The streams the line is drawn on.
    shared: Arc<Shared>,
    /// Set to stop the thread.
    stop: Arc<AtomicBool>,
    /// The thread, joined on drop.
    thread: JoinHandle<()>,
}

impl Progress {
    /// A progress that draws nothing but still measures the wait.
    pub(crate) fn inert() -> Self {
        Self {
            start: Instant::now(),
            ticker: None,
        }
    }

    /// Starts drawing `label` on stderr: the first draw after one second, then once a second. If
    /// the ticker thread cannot start, the progress is inert.
    pub(crate) fn start(shared: Arc<Shared>, label: &str) -> Self {
        let start = Instant::now();
        let stop = Arc::new(AtomicBool::new(false));
        let label = label.to_owned();
        let spawned = thread::Builder::new().name("progress".to_owned()).spawn({
            let shared = Arc::clone(&shared);
            let stop = Arc::clone(&stop);
            move || tick(&shared, &stop, &label, start)
        });
        Self {
            start,
            ticker: spawned.ok().map(|thread| Ticker {
                shared,
                stop,
                thread,
            }),
        }
    }

    /// Clears the line and returns how long the wait took.
    pub(crate) fn finish(self) -> Duration {
        self.start.elapsed()
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        if let Some(ticker) = self.ticker.take() {
            ticker.stop.store(true, Ordering::Release);
            ticker.thread.thread().unpark();
            // A ticker that panicked has nothing left to stop; the line is cleared either way.
            let _ = ticker.thread.join();
            ticker.shared.clear_wait();
        }
    }
}

/// Redraws the wait line each second until `stop` is set.
fn tick(shared: &Shared, stop: &AtomicBool, label: &str, start: Instant) {
    let mut ticks = 1u32;
    loop {
        let due = start + TICK * ticks;
        loop {
            if stop.load(Ordering::Acquire) {
                return;
            }
            let now = Instant::now();
            if now >= due {
                break;
            }
            thread::park_timeout((due - now).min(TICK));
        }
        shared.draw_wait(label, start.elapsed());
        ticks = ticks.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{COLUMN, fit, wait_line};
    use crate::palette::Palette;

    #[test]
    fn the_wait_line_has_the_word_in_the_margin_the_label_and_whole_seconds() {
        let line = wait_line(
            "waiting for the browser",
            Duration::from_millis(12_900),
            None,
        );
        assert_eq!(line.text(), "  wait  waiting for the browser · 12s");
    }

    #[test]
    fn the_wait_word_is_accent_and_the_time_is_muted() {
        let line = wait_line("x", Duration::from_secs(1), None);
        assert_eq!(
            line.paint(Some(Palette::Ansi)),
            "  \x1b[1m\x1b[32mwait\x1b[0m  x\x1b[2m · 1s\x1b[0m"
        );
    }

    #[test]
    fn a_label_that_would_wrap_is_cut_with_an_ellipsis() {
        let line = wait_line("waiting for the browser", Duration::from_secs(12), Some(30));
        assert_eq!(line.text(), "  wait  waiting for th… · 12s");
        assert!(line.width() < 30);
    }

    #[test]
    fn the_wait_line_never_reaches_the_last_column_at_any_width() {
        let labels = [
            "waiting for the browser".to_owned(),
            "waiting for the browser to hand back the login at https://gang.example.com/a/long/path"
                .to_owned(),
            "日本語日本語日本語日本語日本語日本語日本語日本語日本語日本語".to_owned(),
        ];
        for width in [3, 9, 12, 15, 16, 20, 40, 60, 80, 120] {
            for label in &labels {
                for seconds in [0, 9, 10, 3599] {
                    let line = wait_line(label, Duration::from_secs(seconds), Some(width));
                    assert!(
                        line.width() < width || width <= COLUMN,
                        "{width} columns: {:?}",
                        line.text()
                    );
                }
            }
        }
    }

    #[test]
    fn a_label_that_ends_one_column_short_of_the_width_is_kept_and_one_more_is_cut() {
        // `  wait  ` is 8 columns and ` · 5s` is 5: a 40-column line holds 26 label columns.
        let kept = "a".repeat(26);
        let line = wait_line(&kept, Duration::from_secs(5), Some(40));
        assert_eq!(line.width(), 39);
        assert!(line.text().contains(&kept));
        let line = wait_line(&format!("{kept}a"), Duration::from_secs(5), Some(40));
        assert_eq!(line.width(), 39);
        assert!(line.text().contains('…'));
    }

    #[test]
    fn a_width_too_narrow_for_the_seconds_drops_them() {
        let line = wait_line("waiting", Duration::from_secs(5), Some(12));
        assert_eq!(line.text(), "  wait  wa…");
        assert!(line.width() < 12);
    }

    #[test]
    fn a_label_that_fits_is_kept() {
        let line = wait_line("waiting", Duration::from_secs(3), Some(80));
        assert_eq!(line.text(), "  wait  waiting · 3s");
    }

    #[test]
    fn fit_keeps_whole_labels_and_cuts_long_ones() {
        assert_eq!(fit("abc", 3), "abc");
        assert_eq!(fit("abcd", 3), "ab…");
        assert_eq!(fit("abcd", 1), "…");
        assert_eq!(fit("abcd", 0), "");
    }
}
