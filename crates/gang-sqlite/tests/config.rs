//! Tests for `SqliteConfig` deserialization: defaults, busy timeout, synchronous and journal mode parsing, and rejection of invalid or unknown fields.

use std::path::PathBuf;
use std::time::Duration;

use gang_sqlite::{JournalMode, SqliteConfig, Synchronous};
use serde::Deserialize;
use serde::de::value::{Error, MapDeserializer};

fn from_pairs(pairs: &[(&'static str, &'static str)]) -> Result<SqliteConfig, Error> {
    SqliteConfig::deserialize(MapDeserializer::new(pairs.iter().copied()))
}

#[test]
fn path_only_configuration_takes_the_default_busy_timeout() {
    let config = from_pairs(&[("path", "gang.db")]).expect("path-only config deserializes");

    assert_eq!(config.path, PathBuf::from("gang.db"));
    assert_eq!(config.busy_timeout.get(), Duration::from_secs(5));
}

#[test]
fn explicit_busy_timeout_overrides_the_default() {
    let config = from_pairs(&[("path", "gang.db"), ("busy_timeout", "2s")])
        .expect("config with busy_timeout deserializes");

    assert_eq!(config.busy_timeout.get(), Duration::from_secs(2));
}

#[test]
fn zero_busy_timeout_is_rejected() {
    assert!(from_pairs(&[("path", "gang.db"), ("busy_timeout", "0s")]).is_err());
}

#[test]
fn unknown_fields_are_rejected() {
    assert!(from_pairs(&[("path", "gang.db"), ("bogus", "1")]).is_err());
}

#[test]
fn new_sets_the_path_and_the_defaults() {
    let config = SqliteConfig::new(PathBuf::from("gang.db"));

    assert_eq!(config.path, PathBuf::from("gang.db"));
    assert_eq!(config.busy_timeout.get(), Duration::from_secs(5));
    assert_eq!(config.synchronous, Synchronous::Normal);
}

#[test]
fn synchronous_defaults_to_normal() {
    assert_eq!(Synchronous::default(), Synchronous::Normal);

    let config = from_pairs(&[("path", "gang.db")]).expect("path-only config deserializes");
    assert_eq!(config.synchronous, Synchronous::Normal);
}

#[test]
fn full_synchronous_parses() {
    let config = from_pairs(&[("path", "gang.db"), ("synchronous", "full")])
        .expect("config with synchronous = full deserializes");

    assert_eq!(config.synchronous, Synchronous::Full);
}

#[test]
fn unknown_synchronous_is_rejected() {
    assert!(from_pairs(&[("path", "gang.db"), ("synchronous", "off")]).is_err());
}

#[test]
fn synchronous_maps_to_its_pragma_keyword() {
    assert_eq!(Synchronous::Normal.as_pragma(), "NORMAL");
    assert_eq!(Synchronous::Full.as_pragma(), "FULL");
}

#[test]
fn journal_mode_defaults_to_wal() {
    assert_eq!(JournalMode::default(), JournalMode::Wal);

    let config = from_pairs(&[("path", "gang.db")]).expect("path-only config deserializes");
    assert_eq!(config.journal_mode, JournalMode::Wal);
}

#[test]
fn delete_journal_mode_parses() {
    let config = from_pairs(&[("path", "gang.db"), ("journal_mode", "delete")])
        .expect("config with journal_mode = delete deserializes");

    assert_eq!(config.journal_mode, JournalMode::Delete);
}

#[test]
fn unknown_journal_mode_is_rejected() {
    assert!(from_pairs(&[("path", "gang.db"), ("journal_mode", "memory")]).is_err());
}

#[test]
fn journal_mode_maps_to_its_pragma_keyword() {
    assert_eq!(JournalMode::Wal.as_pragma(), "WAL");
    assert_eq!(JournalMode::Delete.as_pragma(), "DELETE");
}
