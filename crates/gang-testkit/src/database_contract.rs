//! The database contract: the cases of every database port, run unchanged against each backend.
//!
//! A backend's test calls `database_contract!` once with a closure that builds a fresh
//! [`Fixture`]. Each case becomes its own `#[tokio::test]` on its own fresh database, and checks
//! one promise of [`StateRepositoryPort`](gang_spi::StateRepositoryPort),
//! [`LeaderLeasePort`](gang_spi::LeaderLeasePort) or
//! [`ExpiringRecordPort`](gang_spi::ExpiringRecordPort), whose cases live in
//! [`repository_contract`](crate::repository_contract), [`lease_contract`](crate::lease_contract)
//! and [`expiring_contract`](crate::expiring_contract). Persistent backends can also run
//! [`schema_contract`](crate::schema_contract) with a fixture that stamps the stored version.

use std::any::Any;

/// A database under test, with whatever must outlive it: a temporary directory, a fake server.
pub struct Fixture<D> {
    // Declared first, so the database closes before what it lives on goes away.
    database: D,
    _keep: Box<dyn Any + Send>,
}

impl<D> Fixture<D> {
    /// A database that needs nothing kept alive.
    pub fn new(database: D) -> Self {
        Self::keeping(database, ())
    }

    /// A database that lives on `keep`, which is dropped after it.
    pub fn keeping(database: D, keep: impl Any + Send) -> Self {
        Self {
            database,
            _keep: Box::new(keep),
        }
    }

    /// The database.
    pub const fn database(&self) -> &D {
        &self.database
    }
}

/// Runs every database contract case against the database that `$fresh` builds, one
/// `#[tokio::test]` per case. `$fresh` is an expression evaluating to a closure that returns a
/// future of `Result<Fixture<D>, E>`, where `D` implements every database port and
/// `E: Into<Box<dyn std::error::Error>>`. Each test calls the closure once.
///
/// `database_contract!($fresh, exclusive)` leaves out the shared-lease cases, for a backend that
/// admits one controller and whose lease is that exclusivity; the backend tests its lease itself.
///
/// `database_contract!($fresh, schema: $supported, $stamped)` also runs schema compatibility
/// cases. `$supported` is the backend's schema version, and `$stamped` builds a fresh, migrated
/// fixture with the supplied stored version. The schema cases can likewise be combined with
/// `exclusive`: `database_contract!($fresh, exclusive, schema: $supported, $stamped)`.
#[macro_export]
macro_rules! database_contract {
    (@cases $fresh:expr, $module:ident: $($case:ident),+ $(,)?) => {
        $(
            #[tokio::test]
            async fn $case() -> $crate::TestResult {
                let fixture = ($fresh)()
                    .await
                    .map_err(Into::<Box<dyn std::error::Error>>::into)?;
                $crate::$module::$case(fixture.database()).await
            }
        )+
    };
    (@schema $supported:expr, $stamped:expr) => {
        #[tokio::test]
        async fn a_newer_compatible_schema_is_used() -> $crate::TestResult {
            $crate::schema_contract::a_newer_compatible_schema_is_used($supported, $stamped).await
        }

        #[tokio::test]
        async fn a_newer_incompatible_schema_is_refused() -> $crate::TestResult {
            $crate::schema_contract::a_newer_incompatible_schema_is_refused($supported, $stamped).await
        }
    };
    ($fresh:expr, schema: $supported:expr, $stamped:expr) => {
        $crate::database_contract!($fresh);
        $crate::database_contract!(@schema $supported, $stamped);
    };
    ($fresh:expr, exclusive, schema: $supported:expr, $stamped:expr) => {
        $crate::database_contract!($fresh, exclusive);
        $crate::database_contract!(@schema $supported, $stamped);
    };
    ($fresh:expr, exclusive) => {
        $crate::database_contract!(@cases $fresh, repository_contract:
            recorded_certificate_is_found_by_serial,
            duplicate_record_issued_is_a_backend_error_and_keeps_one_record,
            revoke_keeps_the_first_revocation_time,
            revoked_is_ordered_by_revocation_time_then_serial,
            certificates_of_lists_active_records_by_issue_time_then_serial,
            scaling_events_get_increasing_sequence_numbers,
            launch_failure_and_withdrawal_round_trip,
            scaling_window_returns_the_newest_events_and_reports_skipped,
            observe_principal_keeps_first_seen_and_raises_last_seen,
            revoke_principal_twice_keeps_the_first_revocation_time_and_restore_clears_it,
            principals_page_with_after_and_the_more_flag_and_only_revoked,
            revoked_principals_are_ordered_by_revocation_time,
            certificates_page_by_serial,
            revocation_epoch_advances_on_mutations,
            revocation_epoch_and_record_are_visible,
            concurrent_scaling_events_are_ordered,
        );
        $crate::database_contract!(@cases $fresh, expiring_contract:
            record_insert_and_expiry,
            record_insert_is_atomic,
            record_insert_over_expired,
            a_reinserted_record_refuses_the_previous_version,
            record_concurrent_insert,
            record_replace_current,
            record_replace_stale_or_expired,
            record_remove_current,
            record_concurrent_remove,
            record_remove_stale,
            record_kinds_are_separate,
            record_purge,
        );
    };
    ($fresh:expr) => {
        $crate::database_contract!($fresh, exclusive);
        $crate::database_contract!(@cases $fresh, lease_contract:
            lease_first_acquire,
            lease_live_then_stale,
            lease_equal_boundary,
            lease_holder_renews,
            lease_renew_after_expiry_and_taking,
            lease_invalid_renewals,
            lease_release,
            lease_invalid_release,
            lease_concurrent_acquire,
        );
    };
}
