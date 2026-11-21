//! Platform property keys that actions set to select a fleet, and that workers register.

/// The fleet's name. Matches exactly.
pub const FLEET: &str = "fleet";
/// The fleet's instruction set. Matches exactly.
pub const ISA: &str = "ISA";
/// The fleet's sandbox. Matches exactly.
pub const SANDBOX: &str = "sandbox";
/// The number of virtual CPUs. Matches when the fleet has at least the requested count.
pub const CPU_COUNT: &str = "cpu_count";
/// CPU in millicores. Matches when the fleet has at least the requested amount.
pub const CPU_MILLICORES: &str = "cpu_millicores";
/// Number of millicores in one CPU core.
pub const MILLICORES_PER_CORE: u64 = 1000;
/// The memory in KiB. Matches when the fleet has at least the requested amount.
pub const MEMORY_KB: &str = "memory_kb";
/// Disk in KiB. Matches when the fleet has at least the requested amount.
pub const DISK_KB: &str = "disk_kb";
/// The action's requested network capability.
pub const NETWORK: &str = "network";
/// Persistent-worker marker; actions still execute one-shot.
pub const SUPPORTS_WORKERS: &str = "supports-workers";
/// An old client's cache label, ignored by the scheduler.
pub const ACTION_ENVIRONMENT: &str = "gang_action_environment";

/// Every reserved property key.
pub const RESERVED: [&str; 10] = [
    FLEET,
    ISA,
    SANDBOX,
    CPU_COUNT,
    CPU_MILLICORES,
    MEMORY_KB,
    DISK_KB,
    NETWORK,
    SUPPORTS_WORKERS,
    ACTION_ENVIRONMENT,
];

/// Explicitly harmless properties that impose nothing on matching.
pub const HARMLESS_PROPERTIES: [&str; 2] = [SUPPORTS_WORKERS, ACTION_ENVIRONMENT];

/// Returns whether `key` is one of the [`RESERVED`] keys.
///
/// It is `const` so that the `FleetOptions` derive checks option names at compile time.
#[must_use]
pub const fn is_reserved(key: &str) -> bool {
    contains(&RESERVED, key.as_bytes())
}

/// Whether `keys` holds `key`, comparing bytes, so that it runs in `const` contexts.
const fn contains(keys: &[&str], key: &[u8]) -> bool {
    match keys {
        [] => false,
        [first, rest @ ..] => same(first.as_bytes(), key) || contains(rest, key),
    }
}

const fn same(left: &[u8], right: &[u8]) -> bool {
    match (left, right) {
        ([], []) => true,
        ([l, left @ ..], [r, right @ ..]) => *l == *r && same(left, right),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_reserved_key_is_reserved() {
        for key in [
            "fleet",
            "ISA",
            "sandbox",
            "cpu_count",
            "cpu_millicores",
            "memory_kb",
            "disk_kb",
            "network",
            "supports-workers",
            "gang_action_environment",
        ] {
            assert!(is_reserved(key), "{key}");
        }
    }

    #[test]
    fn option_and_unknown_keys_are_not_reserved() {
        assert!(!is_reserved("capacity"));
        assert!(!is_reserved("isa"));
        assert!(!is_reserved("Fleet"));
    }

    #[test]
    fn every_harmless_key_is_reserved() {
        for key in HARMLESS_PROPERTIES {
            assert!(is_reserved(key), "{key}");
        }
    }

    /// `is_reserved` is evaluated at compile time, as the `FleetOptions` guard evaluates it.
    const _: () = assert!(is_reserved("fleet") && !is_reserved("capacity"));

    #[test]
    fn is_reserved_matches_whole_keys_only() {
        assert!(!is_reserved("fleets"));
        assert!(!is_reserved("flee"));
    }
}
