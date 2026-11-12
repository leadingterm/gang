//! Process-wide `NativeLink` settings and orphan reaping.
//!
//! One controller and one worker at a time per process. A worker may run again after returning.
//! The controller's resource sketch snapshot task and the process-wide zombie reaper outlive
//! their initiating calls. `init` decides the digest hash function and open-file soft limit once.

use std::collections::BTreeSet;
use std::sync::Once;

use nativelink_config::stores::ConfigDigestHashFunction;
use nativelink_util::digest_hasher::{DigestHasherFunc, set_default_digest_hasher_func};
use nativelink_util::fs::set_open_file_limit;
use nativelink_util::store_trait::{
    DEFAULT_DIGEST_SIZE_HEALTH_CHECK_CFG, set_default_digest_size_health_check,
};

use crate::open_file_limit::OpenFileLimit;

/// Guards the process-wide setup so that a second embedding reuses the first one's settings.
static INIT: Once = Once::new();

/// Guards the process-wide orphan reaper.
static REAPER: Once = Once::new();

/// Applies the process-wide `NativeLink` settings: the SHA-256 digest hasher, the open file limit,
/// and the digest size used by store health checks.
///
/// Only the first call in a process has an effect: the settings live in process-wide cells that can
/// be set once. Later calls leave the first call's values in place. The first call also decides the
/// digest hash function and raises the open-file soft limit for the whole process.
pub(crate) fn init(max_open_files: OpenFileLimit) {
    INIT.call_once(|| {
        let _ = set_default_digest_hasher_func(DigestHasherFunc::from(
            ConfigDigestHashFunction::Sha256,
        ));
        set_open_file_limit(max_open_files.get());
        let _ = set_default_digest_size_health_check(DEFAULT_DIGEST_SIZE_HEALTH_CHECK_CFG);
    });
}

/// Starts `NativeLink`'s orphan reaper once per process. `new_local_worker` would start one per
/// worker, but Gang assembles the worker itself.
pub(crate) fn reap_orphans() {
    REAPER.call_once(|| {
        use nativelink_worker::reaper;

        if let Err(error) = reaper::become_subreaper() {
            tracing::warn!(
                error = %error,
                "cannot become the child subreaper; orphans of actions are not reaped here"
            );
        }
        drop(tokio::spawn(async {
            let mut seen_last = BTreeSet::default();
            loop {
                tokio::time::sleep(reaper::REAP_INTERVAL).await;
                let (reaped, seen) =
                    tokio::task::spawn_blocking(move || reaper::reap_orphaned_zombies(&seen_last))
                        .await
                        .unwrap_or_default();
                seen_last = seen;
                if reaped > 0 {
                    tracing::info!(reaped, "reaped zombies that actions left behind");
                }
            }
        }));
    });
}
