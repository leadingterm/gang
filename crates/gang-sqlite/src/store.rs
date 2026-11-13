use std::thread;
use std::time::Duration;

use gang_clock::Poll;
use gang_spi::{Plugin, PluginContext, PortError};
use gang_types::NonZeroDuration;

use crate::migrations::migrate;
use crate::{SqliteConfig, SqliteError};

/// The `SQLite`-backed state repository. The database is opened on first use, so building the
/// plugin touches no disk.
pub struct Sqlite {
    config: SqliteConfig,
    connection: tokio::sync::OnceCell<tokio_rusqlite::Connection>,
}

impl Plugin for Sqlite {
    type Config = SqliteConfig;

    fn build(config: SqliteConfig, _context: &PluginContext<'_>) -> Result<Self, PortError> {
        if config.path.as_os_str().is_empty() {
            return Err(PortError::config("sqlite path must not be empty"));
        }
        Ok(Self {
            config,
            connection: tokio::sync::OnceCell::new(),
        })
    }
}

impl Sqlite {
    /// Runs `work` on the database thread with the open, migrated connection; the first call
    /// opens the database (a failed open is not cached).
    pub(crate) async fn run<T: Send + 'static>(
        &self,
        work: impl FnOnce(&mut rusqlite::Connection) -> Result<T, SqliteError> + Send + 'static,
    ) -> Result<T, SqliteError> {
        let connection = self.connection.get_or_try_init(|| self.open()).await?;
        Ok(connection.call(work).await?)
    }

    async fn open(&self) -> Result<tokio_rusqlite::Connection, SqliteError> {
        let path = &self.config.path;
        let connection = tokio_rusqlite::Connection::open(path)
            .await
            .map_err(|source| SqliteError::Open {
                path: path.clone(),
                source,
            })?;
        let settings = BringUp {
            busy_timeout: self.config.busy_timeout,
            pragmas: format!(
                "PRAGMA journal_mode = {}; PRAGMA synchronous = {}; PRAGMA foreign_keys = ON;",
                self.config.journal_mode.as_pragma(),
                self.config.synchronous.as_pragma(),
            ),
        };
        connection
            .call(move |db| bring_up_with_retry(db, &settings, &mut thread::sleep))
            .await?;
        Ok(connection)
    }
}

/// The settings applied to a connection once, before the store uses it.
struct BringUp {
    busy_timeout: NonZeroDuration,
    pragmas: String,
}

/// Applies the pragmas and migrates the schema. A failure leaves the database as it was.
fn bring_up(db: &mut rusqlite::Connection, settings: &BringUp) -> Result<(), SqliteError> {
    db.execute_batch(&settings.pragmas)?;
    migrate(db)
}

/// Brings the connection up, waiting at most `busy_timeout` in total while another connection
/// holds the database. The retry loop is the only wait: `SQLite`'s busy handler is off during the
/// bring-up, since it would wait up to `busy_timeout` inside every attempt, and is set to
/// `busy_timeout` for the store's statements once the bring-up succeeds. The busy handler alone
/// could not cover the bring-up: it is not consulted when a connection holding a shared lock
/// tries to upgrade (deadlock avoidance). Each pause between attempts goes through `pause`;
/// production sleeps the thread, which is safe because this runs on the database thread, never
/// on an async worker.
fn bring_up_with_retry(
    db: &mut rusqlite::Connection,
    settings: &BringUp,
    pause: &mut impl FnMut(Duration),
) -> Result<(), SqliteError> {
    db.busy_timeout(Duration::ZERO)?;
    retry_while_busy(settings, || bring_up(db, settings), pause)?;
    db.busy_timeout(settings.busy_timeout.get())?;
    Ok(())
}

/// Runs `attempt` until it succeeds or fails with something other than lock contention. Between
/// busy attempts it pauses through `pause` for a hundredth of `busy_timeout`, in whole
/// milliseconds and at least 1ms. The attempts cover `busy_timeout` as [`Poll`] counts them; when
/// they run out, the last busy error is returned.
fn retry_while_busy(
    settings: &BringUp,
    mut attempt: impl FnMut() -> Result<(), SqliteError>,
    pause: &mut impl FnMut(Duration),
) -> Result<(), SqliteError> {
    let interval = NonZeroDuration::try_new(Duration::from_millis(
        settings.busy_timeout.as_millis() / 100,
    ))
    .unwrap_or(NonZeroDuration::from_millis(1));
    let attempts = Poll::new(settings.busy_timeout.get(), interval).attempts();
    for _ in 1..attempts {
        match attempt() {
            Err(error) if is_busy(&error) => pause(interval.get()),
            result => return result,
        }
    }
    attempt()
}

/// Whether the error is a lock held by another connection.
fn is_busy(error: &SqliteError) -> bool {
    matches!(
        error,
        SqliteError::Query(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error { code, .. },
            _,
        )) if matches!(
            code,
            rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
        )
    )
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use gang_spi::PluginContext;
    use tempfile::TempDir;

    use super::*;
    use crate::Synchronous;

    #[test]
    fn only_lock_contention_is_busy() {
        let failure = sqlite_failure;
        assert!(is_busy(&failure(rusqlite::ffi::SQLITE_BUSY)));
        assert!(is_busy(&failure(rusqlite::ffi::SQLITE_LOCKED)));
        assert!(!is_busy(&failure(rusqlite::ffi::SQLITE_CONSTRAINT)));
        assert!(!is_busy(&SqliteError::Closed));
    }

    fn sqlite_failure(code: i32) -> SqliteError {
        SqliteError::Query(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(code),
            None,
        ))
    }

    fn bring_up_settings(busy_timeout: Duration) -> BringUp {
        BringUp {
            busy_timeout: NonZeroDuration::try_new(busy_timeout)
                .expect("the timeout is a positive whole number of milliseconds"),
            pragmas: String::new(),
        }
    }

    /// The `synchronous` level the store's own connection reports. The setting is
    /// per-connection, so only the store's connection can show it.
    async fn synchronous_level(synchronous: Synchronous) -> i64 {
        let dir = TempDir::new().expect("temp dir is created");
        let mut config = SqliteConfig::new(dir.path().join("gang.db"));
        config.synchronous = synchronous;
        let context = PluginContext {
            config_path: Path::new("gang.toml"),
        };
        let store = Sqlite::build(config, &context).expect("valid configuration builds");
        store
            .run(|db| {
                db.query_row("PRAGMA synchronous", [], |row| row.get::<_, i64>(0))
                    .map_err(SqliteError::from)
            })
            .await
            .expect("pragma runs")
    }

    #[tokio::test]
    async fn store_applies_the_configured_synchronous_level() {
        assert_eq!(synchronous_level(Synchronous::Normal).await, 1);
        assert_eq!(synchronous_level(Synchronous::Full).await, 2);
    }

    #[test]
    fn each_busy_attempt_is_followed_by_one_interval() {
        let settings = bring_up_settings(Duration::from_secs(1));
        let mut busy_left = 3;
        let mut calls = 0;
        let mut pauses = Vec::new();
        let result = retry_while_busy(
            &settings,
            || {
                calls += 1;
                if busy_left == 0 {
                    return Ok(());
                }
                busy_left -= 1;
                Err(sqlite_failure(rusqlite::ffi::SQLITE_BUSY))
            },
            &mut |pause: Duration| pauses.push(pause),
        );
        assert!(result.is_ok());
        assert_eq!(calls, 4);
        assert_eq!(pauses, vec![Duration::from_millis(10); 3]);
    }

    #[test]
    fn a_failure_other_than_contention_returns_at_once() {
        let settings = bring_up_settings(Duration::from_secs(1));
        let mut calls = 0;
        let mut pauses = Vec::new();
        let result = retry_while_busy(
            &settings,
            || {
                calls += 1;
                Err(sqlite_failure(rusqlite::ffi::SQLITE_CONSTRAINT))
            },
            &mut |pause: Duration| pauses.push(pause),
        );
        assert!(matches!(result, Err(SqliteError::Query(_))));
        assert_eq!(calls, 1);
        assert!(pauses.is_empty());
    }

    #[test]
    fn busy_forever_returns_the_last_error_after_every_attempt() {
        let settings = bring_up_settings(Duration::from_secs(1));
        let mut calls = 0;
        let mut pauses = Vec::new();
        let result = retry_while_busy(
            &settings,
            || {
                calls += 1;
                Err(SqliteError::Query(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
                    Some(format!("attempt {calls}")),
                )))
            },
            &mut |pause: Duration| pauses.push(pause),
        );
        let attempts = Poll::new(
            Duration::from_secs(1),
            NonZeroDuration::try_new(Duration::from_millis(10))
                .expect("the interval is a positive whole number of milliseconds"),
        )
        .attempts();
        assert_eq!(calls, attempts);
        assert_eq!(
            u32::try_from(pauses.len()).expect("pauses fit in u32"),
            attempts - 1
        );
        let Err(SqliteError::Query(rusqlite::Error::SqliteFailure(_, message))) = result else {
            panic!("a store that stays busy must return the busy error");
        };
        assert_eq!(message, Some(format!("attempt {attempts}")));
    }

    #[test]
    fn a_held_write_lock_bounds_the_bring_up_by_busy_timeout() {
        // An hour-long timeout: a busy handler left on during the bring-up would block a single
        // attempt for that hour, so the test cannot pass with a second wait mechanism.
        let busy_timeout = Duration::from_secs(3600);
        let settings = bring_up_settings(busy_timeout);
        let dir = TempDir::new().expect("temp dir is created");
        let path = dir.path().join("gang.db");
        let holder = rusqlite::Connection::open(&path).expect("database opens");
        holder
            .execute_batch("BEGIN IMMEDIATE")
            .expect("write lock is taken");
        let mut db = rusqlite::Connection::open(&path).expect("database opens");

        let mut waited = Duration::ZERO;
        let held = bring_up_with_retry(&mut db, &settings, &mut |pause: Duration| {
            waited += pause;
        });

        assert!(
            held.as_ref().is_err_and(is_busy),
            "a held write lock fails the bring-up as busy"
        );
        assert!(
            waited <= busy_timeout,
            "the bring-up waited {waited:?}, longer than its busy timeout"
        );

        holder
            .execute_batch("COMMIT")
            .expect("write lock is released");
        bring_up_with_retry(&mut db, &settings, &mut |_: Duration| {})
            .expect("the bring-up succeeds once the lock is free");
        let configured: i64 = db
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .expect("busy timeout reads");
        let expected =
            i64::try_from(busy_timeout.as_millis()).expect("an hour in milliseconds fits in i64");
        assert_eq!(configured, expected);
    }

    /// Two first opens of one database, interleaved through the pause: the first holds the write
    /// lock while its migration is under way, the second finds it busy, and on the second's pause
    /// the first finishes migrating. The second must then come up without running any migration
    /// itself (a second run would fail on the existing tables).
    #[test]
    fn an_opener_that_waits_for_a_first_migration_runs_none_of_its_own() {
        let settings = bring_up_settings(Duration::from_secs(1));
        let dir = TempDir::new().expect("temp dir is created");
        let path = dir.path().join("gang.db");
        let first = rusqlite::Connection::open(&path).expect("database opens");
        first
            .execute_batch("BEGIN IMMEDIATE")
            .expect("write lock is taken");
        let mut first = Some(first);
        let mut second = rusqlite::Connection::open(&path).expect("database opens");

        let mut pauses = 0;
        bring_up_with_retry(&mut second, &settings, &mut |_: Duration| {
            pauses += 1;
            if let Some(mut first) = first.take() {
                first
                    .execute_batch("COMMIT")
                    .expect("write lock is released");
                migrate(&mut first).expect("the first opener migrates");
            }
        })
        .expect("the second opener comes up after the first one's migration");

        assert_eq!(pauses, 1, "the second opener waits exactly once");
        let version: i64 = second
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("user version reads");
        let mut migrated =
            rusqlite::Connection::open_in_memory().expect("in-memory database opens");
        migrate(&mut migrated).expect("a fresh database migrates");
        let newest: i64 = migrated
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("user version reads");
        assert_eq!(version, newest);
    }

    #[tokio::test]
    async fn a_failed_open_is_not_cached() {
        let dir = TempDir::new().expect("temp dir is created");
        let missing = dir.path().join("missing");
        let config = SqliteConfig::new(missing.join("gang.db"));
        let context = PluginContext {
            config_path: Path::new("gang.toml"),
        };
        let store = Sqlite::build(config, &context).expect("valid configuration builds");

        let failed = store.run(|_| Ok(())).await;
        assert!(matches!(failed, Err(SqliteError::Open { .. })));

        fs::create_dir(&missing).expect("directory is created");
        store
            .run(|_| Ok(()))
            .await
            .expect("the store opens once the directory exists");
    }
}
