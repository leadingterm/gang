//! Learned action duration averages with bounded `LRU` eviction and v1 JSON persistence.
//!
//! Produces v1 records and keys without a `serde_json` dependency.

use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::time::Duration;

use gang_clock::Timestamp;
use gang_config::HistoryWindow;
use gang_spi::{LearnedHistory, RecordKey, RecordKind};

use crate::history::history_id;
use crate::sketch::{LruMap, SAMPLER_GENERATION};

const FORMAT_VERSION: u32 = 1;

impl Key {
    fn record_key(&self) -> RecordKey {
        let id = match self {
            Self::Target { mnemonic, target } => history_id(0, &[mnemonic, target]),
            Self::Mnemonic(mnemonic) => history_id(1, &[mnemonic]),
        };
        RecordKey {
            kind: RecordKind::DurationHistory,
            id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Key {
    Target { mnemonic: String, target: String },
    Mnemonic(String),
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
enum KeyParts<'a> {
    Target { mnemonic: &'a str, target: &'a str },
    Mnemonic(&'a str),
}

trait KeyView {
    fn parts(&self) -> KeyParts<'_>;
}

impl KeyView for Key {
    fn parts(&self) -> KeyParts<'_> {
        match self {
            Self::Target { mnemonic, target } => KeyParts::Target { mnemonic, target },
            Self::Mnemonic(mnemonic) => KeyParts::Mnemonic(mnemonic),
        }
    }
}

impl KeyView for KeyParts<'_> {
    fn parts(&self) -> KeyParts<'_> {
        *self
    }
}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.parts().hash(state);
    }
}

impl Hash for dyn KeyView + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.parts().hash(state);
    }
}

impl PartialEq for dyn KeyView + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.parts() == other.parts()
    }
}

impl Eq for dyn KeyView + '_ {}

impl<'a> Borrow<dyn KeyView + 'a> for Key {
    fn borrow(&self) -> &(dyn KeyView + 'a) {
        self
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Half {
    sum_ms: u64,
    count: u32,
}

impl Half {
    fn record(&mut self, slot_ms: u64) {
        if let (Some(sum_ms), Some(count)) =
            (self.sum_ms.checked_add(slot_ms), self.count.checked_add(1))
        {
            self.sum_ms = sum_ms;
            self.count = count;
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Entry {
    period: u64,
    current: Half,
    previous: Half,
    last_seen_ms: u64,
}

impl Entry {
    fn new(period: u64) -> Self {
        Self {
            period,
            current: Half::default(),
            previous: Half::default(),
            last_seen_ms: 0,
        }
    }

    fn settle(&mut self, period: u64) {
        match period.saturating_sub(self.period) {
            0 => return,
            1 => self.previous = self.current,
            _ => self.previous = Half::default(),
        }
        self.current = Half::default();
        self.period = period;
    }

    fn expected(&self) -> Option<Duration> {
        let count = u64::from(self.current.count) + u64::from(self.previous.count);
        if count == 0 {
            return None;
        }
        let sum = u128::from(self.current.sum_ms) + u128::from(self.previous.sum_ms);
        let mean = u64::try_from(sum / u128::from(count)).unwrap_or(u64::MAX);
        Some(Duration::from_millis(mean))
    }
}

/// Bounded two-half-window mean execution duration estimator keyed by `(mnemonic, target)` and
/// `mnemonic` fallback.
#[derive(Debug, Clone)]
pub struct ActionDurations {
    half_window_ms: u64,
    entries: LruMap<Key, Entry>,
}

impl ActionDurations {
    /// Creates an estimator retaining up to `keys` entries over `window`.
    #[must_use]
    pub fn new(keys: NonZeroUsize, window: HistoryWindow) -> Self {
        Self {
            half_window_ms: window.get().as_secs() * 1_000 / 2,
            entries: LruMap::new(keys),
        }
    }

    /// Records a completed action duration `slot_ms` at `now`. Empty `mnemonic` is ignored.
    pub fn record(&mut self, target: &str, mnemonic: &str, slot_ms: u64, now: Timestamp) {
        if mnemonic.is_empty() {
            return;
        }
        let period = now.as_unix_millis() / self.half_window_ms;
        if !target.is_empty() {
            self.record_key(
                KeyParts::Target { mnemonic, target },
                slot_ms,
                period,
                now.as_unix_millis(),
            );
        }
        self.record_key(
            KeyParts::Mnemonic(mnemonic),
            slot_ms,
            period,
            now.as_unix_millis(),
        );
    }

    fn record_key(&mut self, parts: KeyParts<'_>, slot_ms: u64, period: u64, now_ms: u64) {
        let query: &dyn KeyView = &parts;
        if let Some(entry) = self.entries.get_mut(query) {
            entry.settle(period);
            entry.last_seen_ms = now_ms;
            entry.current.record(slot_ms);
            return;
        }
        let mut entry = Entry::new(period);
        entry.last_seen_ms = now_ms;
        entry.current.record(slot_ms);
        let owned = match parts {
            KeyParts::Target { mnemonic, target } => Key::Target {
                mnemonic: mnemonic.to_owned(),
                target: target.to_owned(),
            },
            KeyParts::Mnemonic(mnemonic) => Key::Mnemonic(mnemonic.to_owned()),
        };
        self.entries.put(owned, entry);
    }

    /// Returns the learned mean duration for `(target, mnemonic)`, falling back to `mnemonic`.
    ///
    /// Performs zero allocations on hit, fallback, or miss.
    pub fn expected(&mut self, target: &str, mnemonic: &str, now: Timestamp) -> Option<Duration> {
        if mnemonic.is_empty() {
            return None;
        }
        let period = now.as_unix_millis() / self.half_window_ms;
        if !target.is_empty()
            && let Some(duration) = self.expected_key(KeyParts::Target { mnemonic, target }, period)
        {
            return Some(duration);
        }
        self.expected_key(KeyParts::Mnemonic(mnemonic), period)
    }

    fn expected_key(&mut self, parts: KeyParts<'_>, period: u64) -> Option<Duration> {
        let query: &dyn KeyView = &parts;
        let entry = self.entries.get_mut(query)?;
        entry.settle(period);
        entry.expected()
    }

    /// Appends live v1 duration records into `into`, oldest (`LRU`) first.
    pub fn snapshot(&self, now: Timestamp, into: &mut Vec<LearnedHistory>) {
        let window_ms = self.half_window_ms * 2;
        for (key, entry) in self.entries.iter_rev() {
            let expires = entry.last_seen_ms.saturating_add(window_ms);
            let mut settled = entry.clone();
            settled.settle(now.as_unix_millis() / self.half_window_ms);
            if expires <= now.as_unix_millis() || settled.expected().is_none() {
                continue;
            }
            let body =
                encode_stored_duration(FORMAT_VERSION, SAMPLER_GENERATION, window_ms, key, entry);
            into.push(LearnedHistory {
                key: key.record_key(),
                body,
                expires_at: Timestamp::from_unix_millis(expires),
            });
        }
    }

    /// Replaces all entries with compatible live records sorted oldest-seen first.
    pub fn restore(&mut self, now: Timestamp, records: &[LearnedHistory]) {
        self.entries.clear();
        let window_ms = self.half_window_ms * 2;
        let mut entries = Vec::new();
        for record in records {
            if record.key.kind != RecordKind::DurationHistory || record.expires_at <= now {
                continue;
            }
            let Some(stored) = decode_stored_duration(&record.body) else {
                continue;
            };
            if stored.version != FORMAT_VERSION
                || stored.sampler_generation != SAMPLER_GENERATION
                || stored.window_ms != window_ms
                || stored.key.record_key() != record.key
                || stored.entry.last_seen_ms.saturating_add(window_ms)
                    != record.expires_at.as_unix_millis()
            {
                continue;
            }
            let mut settled = stored.entry.clone();
            settled.settle(now.as_unix_millis() / self.half_window_ms);
            if settled.expected().is_some() {
                entries.push(stored);
            }
        }
        entries.sort_by_key(|stored| stored.entry.last_seen_ms);
        for stored in entries {
            self.entries.put(stored.key, stored.entry);
        }
    }
}

struct StoredDuration {
    version: u32,
    sampler_generation: u32,
    window_ms: u64,
    key: Key,
    entry: Entry,
}

fn push_json_str(out: &mut Vec<u8>, text: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push(b'"');
    let bytes = text.as_bytes();
    let mut start = 0_usize;
    for (i, &b) in bytes.iter().enumerate() {
        let esc: &[u8] = match b {
            b'"' => br#"\""#,
            b'\\' => br"\\",
            0x08 => br"\b",
            0x0c => br"\f",
            b'\n' => br"\n",
            b'\r' => br"\r",
            b'\t' => br"\t",
            0x00..=0x1f => {
                if start < i {
                    out.extend_from_slice(&bytes[start..i]);
                }
                out.extend_from_slice(br"\u00");
                out.push(HEX[usize::from(b >> 4)]);
                out.push(HEX[usize::from(b & 0x0f)]);
                start = i + 1;
                continue;
            }
            _ => continue,
        };
        if start < i {
            out.extend_from_slice(&bytes[start..i]);
        }
        out.extend_from_slice(esc);
        start = i + 1;
    }
    if start < bytes.len() {
        out.extend_from_slice(&bytes[start..]);
    }
    out.push(b'"');
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    let mut buf = [0_u8; 20];
    let mut n = value;
    let mut i = buf.len();
    loop {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    out.extend_from_slice(&buf[i..]);
}

fn encode_stored_duration(
    version: u32,
    sampler_generation: u32,
    window_ms: u64,
    key: &Key,
    entry: &Entry,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(256);
    out.extend_from_slice(br#"{"version":"#);
    push_u64(&mut out, u64::from(version));
    out.extend_from_slice(br#","sampler_generation":"#);
    push_u64(&mut out, u64::from(sampler_generation));
    out.extend_from_slice(br#","window_ms":"#);
    push_u64(&mut out, window_ms);
    out.extend_from_slice(br#","key":{"#);
    match key {
        Key::Target { mnemonic, target } => {
            out.extend_from_slice(br#""Target":{"mnemonic":"#);
            push_json_str(&mut out, mnemonic);
            out.extend_from_slice(br#","target":"#);
            push_json_str(&mut out, target);
            out.push(b'}');
        }
        Key::Mnemonic(mnemonic) => {
            out.extend_from_slice(br#""Mnemonic":"#);
            push_json_str(&mut out, mnemonic);
        }
    }
    out.extend_from_slice(br#"},"entry":{"period":"#);
    push_u64(&mut out, entry.period);
    out.extend_from_slice(br#","current":{"sum_ms":"#);
    push_u64(&mut out, entry.current.sum_ms);
    out.extend_from_slice(br#","count":"#);
    push_u64(&mut out, u64::from(entry.current.count));
    out.extend_from_slice(br#"},"previous":{"sum_ms":"#);
    push_u64(&mut out, entry.previous.sum_ms);
    out.extend_from_slice(br#","count":"#);
    push_u64(&mut out, u64::from(entry.previous.count));
    out.extend_from_slice(br#"},"last_seen_ms":"#);
    push_u64(&mut out, entry.last_seen_ms);
    out.extend_from_slice(b"}}");
    out
}

fn decode_stored_duration(bytes: &[u8]) -> Option<StoredDuration> {
    let mut p = JsonParser { bytes, at: 0 };
    let mut version = None;
    let mut sampler_generation = None;
    let mut window_ms = None;
    let mut key = None;
    let mut entry = None;
    p.object(|field, p| match field.as_str() {
        "version" => set_once(&mut version, p.u32()?),
        "sampler_generation" => set_once(&mut sampler_generation, p.u32()?),
        "window_ms" => set_once(&mut window_ms, p.u64()?),
        "key" => set_once(&mut key, parse_key(p)?),
        "entry" => set_once(&mut entry, parse_entry(p)?),
        _ => p.skip_value(),
    })?;
    p.skip_ws();
    if p.at != bytes.len() {
        return None;
    }
    Some(StoredDuration {
        version: version?,
        sampler_generation: sampler_generation?,
        window_ms: window_ms?,
        key: key?,
        entry: entry?,
    })
}

fn parse_key(p: &mut JsonParser<'_>) -> Option<Key> {
    let mut out = None;
    p.object(|variant, p| {
        if out.is_some() {
            return None;
        }
        match variant.as_str() {
            "Target" => {
                let mut mnemonic = None;
                let mut target = None;
                p.object(|field, p| match field.as_str() {
                    "mnemonic" => set_once(&mut mnemonic, p.string()?),
                    "target" => set_once(&mut target, p.string()?),
                    _ => p.skip_value(),
                })?;
                out = Some(Key::Target {
                    mnemonic: mnemonic?,
                    target: target?,
                });
                Some(())
            }
            "Mnemonic" => {
                out = Some(Key::Mnemonic(p.string()?));
                Some(())
            }
            _ => None,
        }
    })?;
    out
}

fn parse_half(p: &mut JsonParser<'_>) -> Option<Half> {
    let mut sum_ms = None;
    let mut count = None;
    p.object(|field, p| match field.as_str() {
        "sum_ms" => set_once(&mut sum_ms, p.u64()?),
        "count" => set_once(&mut count, p.u32()?),
        _ => p.skip_value(),
    })?;
    Some(Half {
        sum_ms: sum_ms?,
        count: count?,
    })
}

fn parse_entry(p: &mut JsonParser<'_>) -> Option<Entry> {
    let mut period = None;
    let mut current = None;
    let mut previous = None;
    let mut last_seen_ms = None;
    p.object(|field, p| match field.as_str() {
        "period" => set_once(&mut period, p.u64()?),
        "current" => set_once(&mut current, parse_half(p)?),
        "previous" => set_once(&mut previous, parse_half(p)?),
        "last_seen_ms" => set_once(&mut last_seen_ms, p.u64()?),
        _ => p.skip_value(),
    })?;
    Some(Entry {
        period: period?,
        current: current?,
        previous: previous?,
        last_seen_ms: last_seen_ms?,
    })
}

fn set_once<T>(slot: &mut Option<T>, value: T) -> Option<()> {
    if slot.is_some() {
        return None;
    }
    *slot = Some(value);
    Some(())
}

struct JsonParser<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl JsonParser<'_> {
    fn skip_ws(&mut self) {
        while let Some(&b) = self.bytes.get(self.at) {
            if matches!(b, b' ' | b'\t' | b'\n' | b'\r') {
                self.at += 1;
            } else {
                break;
            }
        }
    }

    fn peek(&mut self) -> Option<u8> {
        self.skip_ws();
        self.bytes.get(self.at).copied()
    }

    fn expect_byte(&mut self, expected: u8) -> Option<()> {
        if self.peek()? != expected {
            return None;
        }
        self.at += 1;
        Some(())
    }

    fn object(&mut self, mut on_field: impl FnMut(String, &mut Self) -> Option<()>) -> Option<()> {
        self.expect_byte(b'{')?;
        if self.peek()? == b'}' {
            self.at += 1;
            return Some(());
        }
        loop {
            let key = self.string()?;
            self.expect_byte(b':')?;
            on_field(key, self)?;
            match self.peek()? {
                b',' => self.at += 1,
                b'}' => {
                    self.at += 1;
                    return Some(());
                }
                _ => return None,
            }
        }
    }

    fn u32(&mut self) -> Option<u32> {
        u32::try_from(self.u64()?).ok()
    }

    fn u64(&mut self) -> Option<u64> {
        self.skip_ws();
        let start = self.at;
        let first = *self.bytes.get(self.at)?;
        if !first.is_ascii_digit() {
            return None;
        }
        if first == b'0' {
            self.at += 1;
            if self.bytes.get(self.at).is_some_and(u8::is_ascii_digit) {
                return None;
            }
            return Some(0);
        }
        while self.bytes.get(self.at).is_some_and(u8::is_ascii_digit) {
            self.at += 1;
        }
        std::str::from_utf8(&self.bytes[start..self.at])
            .ok()?
            .parse::<u64>()
            .ok()
    }

    fn string(&mut self) -> Option<String> {
        self.expect_byte(b'"')?;
        let mut out = Vec::new();
        while let Some(&b) = self.bytes.get(self.at) {
            self.at += 1;
            match b {
                b'"' => return String::from_utf8(out).ok(),
                b'\\' => {
                    let esc = *self.bytes.get(self.at)?;
                    self.at += 1;
                    match esc {
                        b'"' => out.push(b'"'),
                        b'\\' => out.push(b'\\'),
                        b'/' => out.push(b'/'),
                        b'b' => out.push(0x08),
                        b'f' => out.push(0x0c),
                        b'n' => out.push(b'\n'),
                        b'r' => out.push(b'\r'),
                        b't' => out.push(b'\t'),
                        b'u' => {
                            let code = self.hex4()?;
                            let ch = if (0xd800..=0xdbff).contains(&code) {
                                if self.bytes.get(self.at..self.at + 2) != Some(br"\u") {
                                    return None;
                                }
                                self.at += 2;
                                let low = self.hex4()?;
                                if !(0xdc00..=0xdfff).contains(&low) {
                                    return None;
                                }
                                let combined = 0x10000
                                    + ((u32::from(code) - 0xd800) << 10)
                                    + (u32::from(low) - 0xdc00);
                                char::from_u32(combined)?
                            } else {
                                char::from_u32(u32::from(code))?
                            };
                            let mut utf8 = [0_u8; 4];
                            out.extend_from_slice(ch.encode_utf8(&mut utf8).as_bytes());
                        }
                        _ => return None,
                    }
                }
                0x00..=0x1f => return None,
                other => out.push(other),
            }
        }
        None
    }

    fn hex4(&mut self) -> Option<u16> {
        let slice = self.bytes.get(self.at..self.at + 4)?;
        self.at += 4;
        let text = std::str::from_utf8(slice).ok()?;
        u16::from_str_radix(text, 16).ok()
    }

    fn skip_value(&mut self) -> Option<()> {
        match self.peek()? {
            b'"' => {
                let _ = self.string()?;
                Some(())
            }
            b'{' => self.object(|_, p| p.skip_value()),
            b'[' => {
                self.at += 1;
                if self.peek()? == b']' {
                    self.at += 1;
                    return Some(());
                }
                loop {
                    self.skip_value()?;
                    match self.peek()? {
                        b',' => self.at += 1,
                        b']' => {
                            self.at += 1;
                            return Some(());
                        }
                        _ => return None,
                    }
                }
            }
            b't' => self.consume_literal(b"true"),
            b'f' => self.consume_literal(b"false"),
            b'n' => self.consume_literal(b"null"),
            b'-' | b'0'..=b'9' => self.skip_number(),
            _ => None,
        }
    }

    fn consume_literal(&mut self, literal: &[u8]) -> Option<()> {
        if self.bytes.get(self.at..self.at + literal.len()) == Some(literal) {
            self.at += literal.len();
            Some(())
        } else {
            None
        }
    }

    fn skip_number(&mut self) -> Option<()> {
        if self.bytes.get(self.at) == Some(&b'-') {
            self.at += 1;
        }
        let first = *self.bytes.get(self.at)?;
        if !first.is_ascii_digit() {
            return None;
        }
        if first == b'0' {
            self.at += 1;
        } else {
            while self.bytes.get(self.at).is_some_and(u8::is_ascii_digit) {
                self.at += 1;
            }
        }
        if self.bytes.get(self.at) == Some(&b'.') {
            self.at += 1;
            if !self.bytes.get(self.at).is_some_and(u8::is_ascii_digit) {
                return None;
            }
            while self.bytes.get(self.at).is_some_and(u8::is_ascii_digit) {
                self.at += 1;
            }
        }
        if matches!(self.bytes.get(self.at), Some(b'e' | b'E')) {
            self.at += 1;
            if matches!(self.bytes.get(self.at), Some(b'+' | b'-')) {
                self.at += 1;
            }
            if !self.bytes.get(self.at).is_some_and(u8::is_ascii_digit) {
                return None;
            }
            while self.bytes.get(self.at).is_some_and(u8::is_ascii_digit) {
                self.at += 1;
            }
        }
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    const HALF_DAY: u64 = 43_200_000;

    fn history(capacity: usize) -> ActionDurations {
        ActionDurations::new(
            NonZeroUsize::new(capacity).expect("nonzero capacity"),
            HistoryWindow::from_units(1),
        )
    }

    fn at(ms: u64) -> Timestamp {
        Timestamp::from_unix_millis(ms)
    }

    #[test]
    fn persisted_durations_keep_age_caps_and_compatible_configuration() {
        let mut source = history(2);
        source.record("", "old", 10, at(1));
        source.record("", "new", 20, at(2));
        let mut records = Vec::new();
        source.snapshot(at(3), &mut records);
        let mut later = Vec::new();
        source.snapshot(at(4), &mut later);
        assert_eq!(records, later);

        let mut restored = history(1);
        let mut reverse_order = records.clone();
        reverse_order.reverse();
        restored.restore(at(3), &reverse_order);
        assert_eq!(restored.expected("", "old", at(3)), None);
        assert_eq!(
            restored.expected("", "new", at(3)),
            Some(Duration::from_millis(20))
        );
        restored.restore(at(2 + 2 * HALF_DAY), &records);
        assert_eq!(restored.expected("", "new", at(2 + 2 * HALF_DAY)), None);

        let mut incompatible =
            ActionDurations::new(NonZeroUsize::MIN, HistoryWindow::from_units(2));
        incompatible.restore(at(3), &records);
        assert_eq!(incompatible.expected("", "new", at(3)), None);

        let mut corrupt = records.clone();
        let text = std::str::from_utf8(&corrupt[1].body)
            .expect("valid utf8")
            .replace("\"sampler_generation\":2", "\"sampler_generation\":999");
        corrupt[1].body = text.into_bytes();
        restored.restore(at(3), &corrupt);
        assert_eq!(restored.expected("", "new", at(3)), None);
    }

    #[test]
    fn json_encoding_matches_serde_json_v1_layout_and_escapes() {
        let mut h = history(4);
        let target = "//pkg:a\"b\\c\n\t\x01é";
        let mnemonic = "Rust/c\r";
        h.record(target, mnemonic, 42, at(1_000));
        let mut records = Vec::new();
        h.snapshot(at(2_000), &mut records);
        assert_eq!(records.len(), 2);

        let expected_mnemonic = concat!(
            r#"{"version":1,"sampler_generation":2,"window_ms":86400000,"#,
            r#""key":{"Mnemonic":"Rust/c\r"},"#,
            r#""entry":{"period":0,"current":{"sum_ms":42,"count":1},"#,
            r#""previous":{"sum_ms":0,"count":0},"last_seen_ms":1000}}"#,
        );
        let expected_target = concat!(
            r#"{"version":1,"sampler_generation":2,"window_ms":86400000,"#,
            r#""key":{"Target":{"mnemonic":"Rust/c\r","target":"//pkg:a\"b\\c\n\t\u0001é"}},"#,
            r#""entry":{"period":0,"current":{"sum_ms":42,"count":1},"#,
            r#""previous":{"sum_ms":0,"count":0},"last_seen_ms":1000}}"#,
        );
        assert_eq!(std::str::from_utf8(&records[0].body), Ok(expected_target));
        assert_eq!(std::str::from_utf8(&records[1].body), Ok(expected_mnemonic));

        // Decoder also accepts reordered fields, whitespace, and \uXXXX surrogate pairs.
        let mut reordered = records.clone();
        reordered[1].body = concat!(
            r#" { "entry" : { "last_seen_ms": 1000, "previous": {"count":0,"sum_ms":0}, "#,
            r#""current": {"count":1,"sum_ms":42}, "period":0 }, "#,
            r#""window_ms": 86400000, "key": {"Mnemonic": "Rust\/c\r"}, "#,
            r#""sampler_generation": 2, "version": 1 } "#,
        )
        .as_bytes()
        .to_vec();
        let mut restored = history(4);
        restored.restore(at(2_000), &reordered);
        assert_eq!(
            restored.expected(target, mnemonic, at(2_000)),
            Some(Duration::from_millis(42))
        );
    }

    #[test]
    fn target_then_mnemonic_fallback_and_empty_fields() {
        let mut h = history(8);
        h.record("a", "compile", 10, at(0));
        h.record("b", "compile", 30, at(0));
        h.record("ignored", "", 99, at(0));
        assert_eq!(
            h.expected("a", "compile", at(0)),
            Some(Duration::from_millis(10))
        );
        assert_eq!(
            h.expected("unknown", "compile", at(0)),
            Some(Duration::from_millis(20))
        );
        assert_eq!(
            h.expected("", "compile", at(0)),
            Some(Duration::from_millis(20))
        );
        assert_eq!(h.expected("ignored", "", at(0)), None);
        h.record("", "compile", 50, at(0));
        assert_eq!(
            h.expected("a", "compile", at(0)),
            Some(Duration::from_millis(10))
        );
        assert_eq!(
            h.expected("", "compile", at(0)),
            Some(Duration::from_millis(30))
        );
    }

    #[test]
    fn borrowed_keys_have_identical_hash_and_distinct_variants() {
        fn hash<T: Hash + ?Sized>(value: &T) -> u64 {
            let mut h = DefaultHasher::new();
            value.hash(&mut h);
            h.finish()
        }
        let owned = Key::Target {
            mnemonic: "x".into(),
            target: "x".into(),
        };
        let parts = KeyParts::Target {
            mnemonic: "x",
            target: "x",
        };
        let query: &dyn KeyView = &parts;
        let borrowed: &dyn KeyView = owned.borrow();
        assert!(borrowed == query);
        assert_eq!(hash(&owned), hash(query));
        let mnemonic = KeyParts::Mnemonic("x");
        let other: &dyn KeyView = &mnemonic;
        assert!(!(query == other));
        let owned_mnemonic = Key::Mnemonic("x".into());
        assert_eq!(hash(&owned_mnemonic), hash(other));
        let mut h = history(4);
        h.record("x", "x", 10, at(0));
        h.record("", "x", 30, at(0));
        assert_eq!(h.expected("x", "x", at(0)), Some(Duration::from_millis(10)));
        assert_eq!(h.expected("", "x", at(0)), Some(Duration::from_millis(20)));
    }

    #[test]
    fn rotations_are_epoch_aligned_and_expire_after_two_periods() {
        let mut h = history(4);
        h.record("a", "m", 10, at(HALF_DAY - 1));
        assert_eq!(
            h.expected("a", "m", at(HALF_DAY - 1)),
            Some(Duration::from_millis(10))
        );
        assert_eq!(
            h.expected("a", "m", at(HALF_DAY)),
            Some(Duration::from_millis(10))
        );
        h.record("a", "m", 30, at(HALF_DAY));
        assert_eq!(
            h.expected("a", "m", at(2 * HALF_DAY - 1)),
            Some(Duration::from_millis(20))
        );
        assert_eq!(
            h.expected("a", "m", at(2 * HALF_DAY)),
            Some(Duration::from_millis(30))
        );
        assert_eq!(h.expected("a", "m", at(3 * HALF_DAY)), None);
    }

    #[test]
    fn configured_window_controls_expiry() {
        let mut h = ActionDurations::new(
            NonZeroUsize::new(5).expect("nonzero capacity"),
            HistoryWindow::from_units(3),
        );
        h.record("a", "m", 17, at(0));
        assert_eq!(
            h.expected("a", "m", at(3 * HALF_DAY)),
            Some(Duration::from_millis(17))
        );
        assert_eq!(h.expected("a", "m", at(6 * HALF_DAY)), None);
    }

    #[test]
    fn multi_period_jump_and_backward_clock_do_not_resurrect_history() {
        let mut h = history(4);
        h.record("a", "m", 10, at(0));
        assert_eq!(h.expected("a", "m", at(3 * HALF_DAY)), None);
        assert_eq!(h.expected("a", "m", at(0)), None);
        h.record("a", "m", 30, at(HALF_DAY));
        assert_eq!(
            h.expected("a", "m", at(3 * HALF_DAY)),
            Some(Duration::from_millis(30))
        );
        assert_eq!(
            h.expected("a", "m", at(4 * HALF_DAY)),
            Some(Duration::from_millis(30))
        );
        assert_eq!(h.expected("a", "m", at(5 * HALF_DAY)), None);
    }

    #[test]
    fn capacity_one_and_active_mnemonic_recency() {
        let mut one = history(1);
        one.record("a", "m", 10, at(0));
        assert_eq!(
            one.expected("a", "m", at(0)),
            Some(Duration::from_millis(10))
        );
        one.record("b", "m", 30, at(0));
        assert_eq!(
            one.expected("a", "m", at(0)),
            Some(Duration::from_millis(30))
        );

        let mut h = history(3);
        h.record("a", "m", 10, at(0));
        h.record("b", "m", 20, at(0));
        h.record("c", "m", 60, at(0));
        assert_eq!(h.expected("a", "m", at(0)), Some(Duration::from_millis(30)));
        assert_eq!(h.expected("b", "m", at(0)), Some(Duration::from_millis(20)));
        assert_eq!(h.expected("c", "m", at(0)), Some(Duration::from_millis(60)));
    }

    #[test]
    fn overflow_skips_whole_sample_and_combined_mean_is_wide() {
        let mut half = Half {
            sum_ms: u64::MAX,
            count: 1,
        };
        half.record(1);
        assert_eq!((half.sum_ms, half.count), (u64::MAX, 1));
        half = Half {
            sum_ms: 0,
            count: u32::MAX,
        };
        half.record(1);
        assert_eq!((half.sum_ms, half.count), (0, u32::MAX));
        let mut entry = Entry::new(0);
        entry.current = Half {
            sum_ms: u64::MAX,
            count: 1,
        };
        entry.previous = Half {
            sum_ms: u64::MAX,
            count: 1,
        };
        assert_eq!(entry.expected(), Some(Duration::from_millis(u64::MAX)));
        entry.current = Half {
            sum_ms: u64::MAX,
            count: u32::MAX,
        };
        entry.previous = entry.current;
        assert_eq!(
            entry.expected(),
            Some(Duration::from_millis(u64::MAX / u64::from(u32::MAX)))
        );
    }
}
