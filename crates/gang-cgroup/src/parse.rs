//! Pure parsers for the text the cgroup v2 interface files and `/proc/self/cgroup` contain.

use std::path::{Path, PathBuf};
use std::time::Duration;

use gang_types::Bytes;

use crate::errors::CgroupError;
use crate::fs::{CPU_STAT, EVENTS, MEMORY_PEAK};

/// Prefix of the cgroup v2 entry in `/proc/self/cgroup`; v1 and hybrid lines have a nonzero id.
const CGROUP_V2_PREFIX: &str = "0::";
/// Name of the `cpu.stat` key holding total CPU time in microseconds.
const USAGE_KEY: &str = "usage_usec";
/// `Malformed::what` label for `/proc/self/cgroup` content.
const MEMBERSHIP_WHAT: &str = "/proc/self/cgroup";
/// Key of the `cgroup.events` line that says whether the cgroup has processes.
const POPULATED_KEY: &str = "populated";
/// Separates the creator pid from the serial in a scope name.
const SCOPE_SEPARATOR: char = '-';

/// Returns the calling process's cgroup v2 path relative to the cgroup root.
///
/// `membership` is the content of `/proc/self/cgroup`. The root cgroup yields an empty path.
pub(crate) fn own_cgroup(membership: &str) -> Result<PathBuf, CgroupError> {
    membership
        .lines()
        .find_map(|line| line.strip_prefix(CGROUP_V2_PREFIX))
        .map(|path| PathBuf::from(path.strip_prefix('/').unwrap_or(path)))
        .ok_or_else(|| CgroupError::Malformed {
            what: MEMBERSHIP_WHAT,
            content: membership.to_owned(),
        })
}

/// Parses the single decimal byte count in a `memory.peak` file.
pub(crate) fn peak_memory(text: &str) -> Result<Bytes, CgroupError> {
    text.trim()
        .parse::<u64>()
        .map(Bytes::new)
        .map_err(|_| CgroupError::Malformed {
            what: MEMORY_PEAK,
            content: text.to_owned(),
        })
}

/// The value of the first line of a flat keyed file whose first field is `key`, such as
/// `usage_usec 4821337`.
fn flat_keyed<'a>(text: &'a str, key: &str) -> Option<&'a str> {
    text.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        match (fields.next(), fields.next()) {
            (Some(name), Some(value)) if name == key => Some(value),
            _ => None,
        }
    })
}

/// Parses the total CPU time from the `usage_usec` line of a `cpu.stat` file.
pub(crate) fn cpu_time(text: &str) -> Result<Duration, CgroupError> {
    flat_keyed(text, USAGE_KEY)
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_micros)
        .ok_or_else(|| CgroupError::Malformed {
            what: CPU_STAT,
            content: text.to_owned(),
        })
}

/// Reports whether a `cgroup.events` file says the cgroup still has processes.
pub(crate) fn populated(events: &str) -> Result<bool, CgroupError> {
    flat_keyed(events, POPULATED_KEY)
        .and_then(|value| match value {
            "1" => Some(true),
            "0" => Some(false),
            _ => None,
        })
        .ok_or_else(|| CgroupError::Malformed {
            what: EVENTS,
            content: events.to_owned(),
        })
}

/// The process that created a scope, identified by its pid and start time. A recycled pid has a
/// different start time, so it never matches a scope left by its predecessor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Creator {
    /// The creator's pid.
    pub(crate) pid: u32,
    /// The creator's start time, as [`process_start`] reports it.
    pub(crate) start: u64,
}

/// Builds the name of a scope created by `creator` with the given serial.
pub(crate) fn scope_name(creator: Creator, serial: u64) -> String {
    let Creator { pid, start } = creator;
    format!("{pid}{SCOPE_SEPARATOR}{start}{SCOPE_SEPARATOR}{serial}")
}

/// Returns the creator of a scope cgroup named `<pid>-<starttime>-<serial>`, or `None` for any
/// other name.
pub(crate) fn scope_creator(scope: &Path) -> Option<Creator> {
    let name = scope.file_name()?.to_str()?;
    let mut parts = name.split(SCOPE_SEPARATOR);
    let (Some(pid), Some(start), Some(serial), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return None;
    };
    if ![pid, start, serial].into_iter().all(is_decimal) {
        return None;
    }
    // The serial is not kept, but it must fit a `u64` like the other two parts do.
    serial.parse::<u64>().ok()?;
    Some(Creator {
        pid: pid.parse().ok()?,
        start: start.parse().ok()?,
    })
}

/// Parses the start time from `/proc/<pid>/stat`, treating a zombie as gone.
///
/// The command name sits in parentheses and may contain spaces and parentheses, so the fields
/// are counted after the last `)`. There the state is field 3, so the start time (field 22) is
/// the element at index 19.
pub(crate) fn process_start(stat: &str) -> Option<u64> {
    const STARTTIME_INDEX: usize = 19;
    let (_, fields) = stat.rsplit_once(')')?;
    let mut fields = fields.split_whitespace();
    if fields.next()? == "Z" {
        return None;
    }
    fields.nth(STARTTIME_INDEX - 1)?.parse().ok()
}

/// Whether `text` is a non-empty run of ASCII digits.
fn is_decimal(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use super::{
        Creator, cpu_time, flat_keyed, own_cgroup, peak_memory, populated, process_start,
        scope_creator, scope_name,
    };
    use crate::errors::CgroupError;

    const CPU_STAT_SAMPLE: &str = "usage_usec 4821337\n\
        user_usec 3010112\n\
        system_usec 1811225\n\
        nr_periods 0\n\
        nr_throttled 0\n\
        throttled_usec 0\n";

    #[test]
    fn own_cgroup_reads_v2_only_membership() {
        let path = own_cgroup("0::/user.slice/gang.slice/worker.scope\n").unwrap();
        assert_eq!(path, Path::new("user.slice/gang.slice/worker.scope"));
    }

    #[test]
    fn own_cgroup_ignores_v1_lines_in_hybrid_membership() {
        let membership = "12:cpu,cpuacct:/legacy\n\
            1:name=systemd:/legacy\n\
            0::/system.slice/gang-worker.service\n";
        let path = own_cgroup(membership).unwrap();
        assert_eq!(path, Path::new("system.slice/gang-worker.service"));
    }

    #[test]
    fn own_cgroup_root_yields_empty_path() {
        let path = own_cgroup("0::/\n").unwrap();
        assert_eq!(path, PathBuf::new());
    }

    #[test]
    fn own_cgroup_without_v2_line_is_malformed() {
        let membership = "12:cpu,cpuacct:/legacy\n1:name=systemd:/legacy\n";
        let error = own_cgroup(membership).unwrap_err();
        match error {
            CgroupError::Malformed { what, content } => {
                assert_eq!(what, "/proc/self/cgroup");
                assert_eq!(content, membership);
            }
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[test]
    fn own_cgroup_of_empty_membership_is_malformed() {
        let error = own_cgroup("").unwrap_err();
        assert!(matches!(
            error,
            CgroupError::Malformed {
                what: "/proc/self/cgroup",
                ..
            }
        ));
    }

    #[test]
    fn peak_memory_accepts_trailing_newline() {
        let bytes = peak_memory("1073741824\n").unwrap();
        assert_eq!(bytes.get(), 1_073_741_824);
    }

    #[test]
    fn peak_memory_rejects_garbage() {
        let error = peak_memory("12MiB\n").unwrap_err();
        assert!(matches!(
            error,
            CgroupError::Malformed {
                what: "memory.peak",
                ..
            }
        ));
    }

    #[test]
    fn peak_memory_rejects_empty_input() {
        let error = peak_memory("").unwrap_err();
        assert!(matches!(
            error,
            CgroupError::Malformed {
                what: "memory.peak",
                ..
            }
        ));
    }

    #[test]
    fn cpu_time_reads_usage_usec_from_realistic_sample() {
        let time = cpu_time(CPU_STAT_SAMPLE).unwrap();
        assert_eq!(time, Duration::from_micros(4_821_337));
    }

    #[test]
    fn cpu_time_without_usage_usec_is_malformed() {
        let error = cpu_time("user_usec 1\nsystem_usec 2\n").unwrap_err();
        assert!(matches!(
            error,
            CgroupError::Malformed {
                what: "cpu.stat",
                ..
            }
        ));
    }

    #[test]
    fn cpu_time_with_non_numeric_usage_is_malformed() {
        let error = cpu_time("usage_usec lots\n").unwrap_err();
        assert!(matches!(
            error,
            CgroupError::Malformed {
                what: "cpu.stat",
                ..
            }
        ));
    }

    #[test]
    fn populated_reads_one_and_zero() {
        assert!(populated("populated 1\nfrozen 0\n").unwrap());
        assert!(!populated("populated 0\nfrozen 0\n").unwrap());
    }

    #[test]
    fn populated_finds_key_among_other_lines() {
        assert!(populated("frozen 1\nkernel 3\npopulated 1\n").unwrap());
        assert!(!populated("frozen 1\npopulated 0\nkernel 3\n").unwrap());
    }

    #[test]
    fn populated_without_key_is_malformed() {
        let error = populated("frozen 0\n").unwrap_err();
        assert!(matches!(
            error,
            CgroupError::Malformed {
                what: "cgroup.events",
                ..
            }
        ));
    }

    #[test]
    fn populated_with_other_value_is_malformed() {
        let error = populated("populated yes\n").unwrap_err();
        assert!(matches!(
            error,
            CgroupError::Malformed {
                what: "cgroup.events",
                ..
            }
        ));
    }

    const CREATOR: Creator = Creator {
        pid: 1234,
        start: 987_654,
    };

    #[test]
    fn scope_name_joins_creator_and_serial() {
        assert_eq!(scope_name(CREATOR, 7), "1234-987654-7");
    }

    #[test]
    fn scope_name_round_trips_through_scope_creator() {
        let name = scope_name(CREATOR, 7);
        assert_eq!(scope_creator(Path::new(&name)), Some(CREATOR));
    }

    #[test]
    fn scope_creator_reads_creator_from_scope_name() {
        assert_eq!(scope_creator(Path::new("1234-987654-7")), Some(CREATOR));
    }

    #[test]
    fn scope_creator_reads_creator_from_full_path() {
        assert_eq!(
            scope_creator(Path::new("a/b/actions/1234-987654-0")),
            Some(CREATOR)
        );
    }

    #[test]
    fn scope_creator_rejects_the_old_two_part_name() {
        assert_eq!(scope_creator(Path::new("1234-7")), None);
    }

    #[test]
    fn scope_creator_rejects_other_names() {
        for name in [
            "worker",
            "12-3",
            "12x-3-1",
            "12--1",
            "-3-4-5",
            "+12-3-1",
            "12-3-4-5",
            "12-3-",
            "4294967296-1-1",
        ] {
            assert_eq!(scope_creator(Path::new(name)), None, "{name}");
        }
    }

    #[test]
    fn scope_creator_rejects_numbers_that_overflow() {
        let overflowing = [
            "4294967296-1-1",
            "1-99999999999999999999999-1",
            "1-1-18446744073709551616",
        ];
        for name in overflowing {
            assert_eq!(scope_creator(Path::new(name)), None, "{name}");
        }
    }

    #[test]
    fn scope_creator_rejects_empty_and_signed_parts() {
        for name in ["-1-2-3", "1--3", "1-2-", "+1-2-3"] {
            assert_eq!(scope_creator(Path::new(name)), None, "{name}");
        }
    }

    #[test]
    fn process_start_reads_field_22_after_a_comm_with_spaces_and_parens() {
        let stat = "123 (a b) c) S 1 123 123 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 987654 1000 100 18446744073709551615";
        assert_eq!(process_start(stat), Some(987_654));
    }

    #[test]
    fn process_start_treats_zombies_as_gone() {
        let stat = "123 (a b) c) Z 1 123 123 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 987654";
        assert_eq!(process_start(stat), None);
    }

    #[test]
    fn process_start_rejects_missing_state_or_start_time() {
        assert_eq!(process_start("123 (name)"), None);
        assert_eq!(process_start("123 (name) S 1"), None);
    }

    #[test]
    fn flat_keyed_finds_the_first_matching_key() {
        assert_eq!(flat_keyed("user 1\nusage 2\nusage 3\n", "usage"), Some("2"));
        assert_eq!(flat_keyed("user 1\n", "usage"), None);
    }
}
