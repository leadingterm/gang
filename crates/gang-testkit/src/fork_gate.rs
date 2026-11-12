//! Test-only gate between lifetime-lock users and child spawns.
//!
//! A `flock` belongs to an open file description, and a child forked between `fork` and `exec` shares every description its parent holds. So a lock dropped by one test thread still reads as held while another thread is forking, which makes lock probes flaky. Tests that use locks take the gate shared and tests that spawn children take it exclusively, so no spawn overlaps a lock's lifetime.
//!
//! This module is test support: depend on the crate only as a dev-dependency. Production code never depends on it.

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Serializes lock users against spawners. `parking_lot` does not poison, so one failed test cannot cascade into the others.
static GATE: RwLock<()> = RwLock::new(());

/// Held by every test that holds, drops or probes a lifetime lock, so those run concurrently with each other.
pub fn lock_user() -> RwLockReadGuard<'static, ()> {
    GATE.read()
}

/// Held by every test that forks or execs a child, excluding all lock users while the child shares their descriptions.
pub fn spawner() -> RwLockWriteGuard<'static, ()> {
    GATE.write()
}

#[cfg(test)]
mod tests {
    use super::{GATE, lock_user, spawner};

    // One test covers both sides: the gate is a process-wide static, so separate tests would
    // race on each other's guards. Every assertion below holds regardless of what other test
    // threads are doing, because each one either holds the gate itself or only asserts exclusion.
    #[test]
    fn gate_excludes_spawners_while_lock_users_hold_it() {
        {
            let _user = lock_user();
            assert!(GATE.try_write().is_none());
        }
        {
            let _spawner = spawner();
            assert!(GATE.try_read().is_none());
            assert!(GATE.try_write().is_none());
        }
    }
}
