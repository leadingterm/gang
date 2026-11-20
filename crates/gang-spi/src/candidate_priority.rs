//! Restart-stable host priorities, encoded so canonical text has the same total order.

use std::fmt;
use std::str::FromStr;

use gang_clock::Timestamp;
use gang_types::InstanceId;

use crate::{CandidateGeneration, CandidatePriorityError};

/// A host's durable handoff priority, independent of controller process identity.
///
/// Registered hosts sort before launched hosts. Within each class, a larger registration
/// generation or launch timestamp ranks higher; equal launch times are ordered by instance id.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CandidatePriority(String);

impl CandidatePriority {
    /// Encodes a persistent host's first database registration generation.
    #[must_use]
    pub fn registered(generation: CandidateGeneration) -> Self {
        Self(format!("0:{:020}", generation.get()))
    }

    /// Encodes immutable launch metadata, breaking timestamp ties by instance id.
    #[must_use]
    pub fn launched(at: Timestamp, instance: &InstanceId) -> Self {
        Self(format!("1:{:020}:{instance}", at.as_unix_millis()))
    }

    /// The canonical ordered storage key.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Borrows this already-validated priority without parsing or allocating.
    #[must_use]
    pub fn view(&self) -> CandidatePriorityRef<'_> {
        CandidatePriorityRef(self.as_str())
    }
}

impl fmt::Display for CandidatePriority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for CandidatePriority {
    type Err = CandidatePriorityError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        CandidatePriorityRef::try_from(value)?;
        Ok(Self(value.to_owned()))
    }
}

/// A checked, borrowed canonical priority key; parsing and comparison do not allocate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CandidatePriorityRef<'a>(&'a str);

impl<'a> CandidatePriorityRef<'a> {
    /// The canonical ordered storage key.
    #[must_use]
    pub const fn as_str(self) -> &'a str {
        self.0
    }
}

impl<'a> TryFrom<&'a str> for CandidatePriorityRef<'a> {
    type Error = CandidatePriorityError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        let digits = if let Some(digits) = value.strip_prefix("0:") {
            digits
        } else if let Some(launched) = value.strip_prefix("1:") {
            let (digits, instance) = launched.split_once(':').ok_or(CandidatePriorityError)?;
            if instance.is_empty() || instance.contains(':') {
                return Err(CandidatePriorityError);
            }
            digits
        } else {
            return Err(CandidatePriorityError);
        };
        if digits.len() != 20
            || !digits.bytes().all(|byte| byte.is_ascii_digit())
            || digits.parse::<u64>().is_err()
        {
            return Err(CandidatePriorityError);
        }
        Ok(Self(value))
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use gang_clock::Timestamp;
    use gang_types::InstanceId;

    use crate::{CandidateGeneration, CandidatePriorityError, ControllerId, LeaseCandidate};

    use super::{CandidatePriority, CandidatePriorityRef};

    #[test]
    fn registration_encoding_roundtrips_u64_boundaries() -> Result<(), Box<dyn Error>> {
        for (generation, encoded) in [
            (0, "0:00000000000000000000"),
            (1, "0:00000000000000000001"),
            (u64::MAX, "0:18446744073709551615"),
        ] {
            let priority = CandidatePriority::registered(CandidateGeneration::new(generation));
            assert_eq!(priority.as_str(), encoded);
            assert_eq!(priority.to_string(), encoded);
            assert_eq!(encoded.parse::<CandidatePriority>()?, priority);
            assert_eq!(CandidatePriorityRef::try_from(encoded)?, priority.view());
        }
        Ok(())
    }

    #[test]
    fn launch_encoding_roundtrips_u64_boundaries() -> Result<(), Box<dyn Error>> {
        let instance = InstanceId::try_new("i-abc_01.test")?;
        for (millis, encoded) in [
            (0, "1:00000000000000000000:i-abc_01.test"),
            (1, "1:00000000000000000001:i-abc_01.test"),
            (u64::MAX, "1:18446744073709551615:i-abc_01.test"),
        ] {
            let priority =
                CandidatePriority::launched(Timestamp::from_unix_millis(millis), &instance);
            assert_eq!(priority.as_str(), encoded);
            assert_eq!(priority.to_string(), encoded);
            assert_eq!(encoded.parse::<CandidatePriority>()?, priority);
            assert_eq!(CandidatePriorityRef::try_from(encoded)?, priority.view());
        }
        Ok(())
    }

    #[test]
    fn malformed_keys_return_typed_errors() {
        for encoded in [
            "",
            "0",
            "0:",
            "2:00000000000000000001",
            "00:00000000000000000001",
            "0:0000000000000000001",
            "0:000000000000000000001",
            "0:+0000000000000000001",
            "0:-0000000000000000001",
            "0:0000000000000000000a",
            "0:0000000000000000000١",
            "0:18446744073709551616",
            "0:00000000000000000001:extra",
            "1:00000000000000000001",
            "1:00000000000000000001:",
            "1::i-a",
            "1:00000000000000000001:i-a:extra",
            "1:00000000000000000001::",
            "1:0000000000000000001:i-a",
            "1:000000000000000000001:i-a",
            "1:+0000000000000000001:i-a",
            "1:18446744073709551616:i-a",
            "1:0000000000000000000a:i-a",
        ] {
            assert_eq!(
                CandidatePriorityRef::try_from(encoded),
                Err(CandidatePriorityError),
                "{encoded:?}"
            );
            assert_eq!(
                encoded.parse::<CandidatePriority>(),
                Err(CandidatePriorityError),
                "{encoded:?}"
            );
        }
    }

    #[test]
    fn registration_text_and_views_follow_numeric_order() {
        let generations = [0, 1, 9, 10, 99, 100, u64::MAX - 1, u64::MAX];
        for left in generations {
            for right in generations {
                let left_key = CandidatePriority::registered(CandidateGeneration::new(left));
                let right_key = CandidatePriority::registered(CandidateGeneration::new(right));
                assert_eq!(left_key.cmp(&right_key), left.cmp(&right));
                assert_eq!(left_key.view().cmp(&right_key.view()), left.cmp(&right));
                assert_eq!(left_key.as_str().cmp(right_key.as_str()), left.cmp(&right));
            }
        }
    }

    #[test]
    fn launch_text_and_views_follow_timestamp_then_instance_order() -> Result<(), Box<dyn Error>> {
        let identities = [
            (0, "i-z"),
            (1, "i-a"),
            (1, "i-aa"),
            (1, "i-b"),
            (9, "i-z"),
            (10, "i-a"),
            (u64::MAX - 1, "i-z"),
            (u64::MAX, "i-a"),
        ];
        for left in identities {
            for right in identities {
                let left_key = CandidatePriority::launched(
                    Timestamp::from_unix_millis(left.0),
                    &InstanceId::try_new(left.1)?,
                );
                let right_key = CandidatePriority::launched(
                    Timestamp::from_unix_millis(right.0),
                    &InstanceId::try_new(right.1)?,
                );
                let expected = left.cmp(&right);
                assert_eq!(left_key.cmp(&right_key), expected);
                assert_eq!(left_key.view().cmp(&right_key.view()), expected);
                assert_eq!(left_key.as_str().cmp(right_key.as_str()), expected);
            }
        }
        Ok(())
    }

    #[test]
    fn every_registered_priority_precedes_launched_priorities() -> Result<(), Box<dyn Error>> {
        let registered = CandidatePriority::registered(CandidateGeneration::new(u64::MAX));
        let launched =
            CandidatePriority::launched(Timestamp::UNIX_EPOCH, &InstanceId::try_new("i-a")?);
        assert!(registered < launched);
        assert!(registered.view() < launched.view());
        Ok(())
    }

    #[test]
    fn restarted_process_keeps_same_host_priority() -> Result<(), Box<dyn Error>> {
        let instance = InstanceId::try_new("i-old")?;
        for priority in [
            CandidatePriority::registered(CandidateGeneration::new(1)),
            CandidatePriority::launched(Timestamp::from_unix_millis(42), &instance),
        ] {
            let original = LeaseCandidate {
                holder: ControllerId::new("original"),
                priority: priority.clone(),
            };
            let restarted = LeaseCandidate {
                holder: ControllerId::new("restarted"),
                priority: priority.as_str().parse()?,
            };
            assert_ne!(original.holder, restarted.holder);
            assert_eq!(original.priority, restarted.priority);
            assert_eq!(original.priority.view(), restarted.priority.view());
        }
        Ok(())
    }

    #[test]
    fn checked_view_borrows_original_key() -> Result<(), Box<dyn Error>> {
        let priority = CandidatePriority::registered(CandidateGeneration::new(7));
        let view = CandidatePriorityRef::try_from(priority.as_str())?;
        assert_eq!(view.as_str().as_ptr(), priority.as_str().as_ptr());
        assert_eq!(view, priority.view());
        Ok(())
    }
}
