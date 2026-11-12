//! Permissions that gate Gang's HTTP API, and sets of them.

use std::fmt;

use serde::de;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The string that stands for every permission when a set is written in configuration.
const WILDCARD: &str = "*";

crate::string_enum! {
    /// A permission an API caller can hold through a role.
    pub enum Permission("permission") {
        /// Reads fleet state.
        StateRead = "state.read",
        /// Reads principals.
        PrincipalsRead = "principals.read",
        /// Reads the scaling event history.
        EventsRead = "events.read",
        /// Revokes principals and restores revoked principals.
        PrincipalsRevoke = "principals.revoke",
    }
}

impl Permission {
    /// Returns the bit this permission occupies in a [`PermissionSet`].
    const fn bit(self) -> u8 {
        1 << self as u8
    }
}

/// A set of [`Permission`]s, stored as one bit per permission.
///
/// In configuration a set is a sequence of permission names, and the name `*` stands for every
/// permission.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PermissionSet(u8);

impl PermissionSet {
    /// The set that holds no permissions.
    pub const EMPTY: Self = Self(0);

    /// Returns the set that holds every permission.
    #[must_use]
    pub const fn all() -> Self {
        let mut bits = 0;
        let mut index = 0;
        while index < Permission::ALL.len() {
            bits |= Permission::ALL[index].bit();
            index += 1;
        }
        Self(bits)
    }

    /// Returns whether the set holds `permission`.
    #[must_use]
    pub const fn contains(self, permission: Permission) -> bool {
        self.0 & permission.bit() != 0
    }

    /// Returns the set with `permission` added.
    #[must_use]
    pub const fn with(self, permission: Permission) -> Self {
        Self(self.0 | permission.bit())
    }

    /// Returns the permissions held by either set.
    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Returns whether the set holds no permissions.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Iterates over the held permissions in [`Permission::ALL`] order.
    pub fn iter(self) -> impl Iterator<Item = Permission> {
        Permission::ALL
            .into_iter()
            .filter(move |permission| self.contains(*permission))
    }
}

impl Default for PermissionSet {
    fn default() -> Self {
        Self::EMPTY
    }
}

impl FromIterator<Permission> for PermissionSet {
    fn from_iter<I: IntoIterator<Item = Permission>>(iter: I) -> Self {
        iter.into_iter().fold(Self::EMPTY, Self::with)
    }
}

impl fmt::Debug for PermissionSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set()
            .entries(self.iter().map(Permission::as_str))
            .finish()
    }
}

impl Serialize for PermissionSet {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.iter().map(Permission::as_str))
    }
}

impl<'de> Deserialize<'de> for PermissionSet {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let names = Vec::<String>::deserialize(deserializer)?;
        let mut set = Self::EMPTY;
        for name in &names {
            if name == WILDCARD {
                set = set.union(Self::all());
            } else {
                let permission = name
                    .parse::<Permission>()
                    .map_err(<D::Error as de::Error>::custom)?;
                set = set.with(permission);
            }
        }
        Ok(set)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_NAMES: [&str; 4] = [
        "state.read",
        "principals.read",
        "events.read",
        "principals.revoke",
    ];

    #[test]
    fn names_round_trip() {
        for permission in Permission::ALL {
            assert_eq!(permission.as_str().parse::<Permission>(), Ok(permission));
            assert_eq!(permission.to_string(), permission.as_str());
        }
    }

    #[test]
    fn all_lists_permissions_in_declaration_order() {
        let names: Vec<&str> = Permission::ALL
            .into_iter()
            .map(Permission::as_str)
            .collect();
        assert_eq!(names, ALL_NAMES);
    }

    #[test]
    fn set_algebra_follows_membership() {
        let empty = PermissionSet::EMPTY;
        assert!(empty.is_empty());
        assert_eq!(empty.iter().count(), 0);
        assert_eq!(PermissionSet::default(), empty);

        let read = empty.with(Permission::StateRead);
        assert!(!read.is_empty());
        assert!(read.contains(Permission::StateRead));
        assert!(!read.contains(Permission::EventsRead));

        let revoke = empty.with(Permission::PrincipalsRevoke);
        let both = read.union(revoke);
        assert!(both.contains(Permission::StateRead));
        assert!(both.contains(Permission::PrincipalsRevoke));
        assert_eq!(both.union(read), both);
        assert_eq!(read.with(Permission::StateRead), read);

        let all = PermissionSet::all();
        for permission in Permission::ALL {
            assert!(all.contains(permission));
        }
        assert_eq!(all.iter().collect::<Vec<_>>(), Permission::ALL);
    }

    #[test]
    fn iter_yields_held_permissions_in_all_order() {
        let set: PermissionSet = [Permission::EventsRead, Permission::StateRead]
            .into_iter()
            .collect();
        assert_eq!(
            set.iter().collect::<Vec<_>>(),
            [Permission::StateRead, Permission::EventsRead]
        );
    }

    #[test]
    fn star_expands_to_every_permission() {
        let set: PermissionSet = serde_json::from_str(r#"["*"]"#).expect("wildcard parses");
        assert_eq!(set, PermissionSet::all());
    }

    #[test]
    fn wildcard_is_not_a_permission_name() {
        let error = "*"
            .parse::<Permission>()
            .expect_err("wildcard is not a permission");
        assert_eq!(error.input(), "*");
    }

    #[test]
    fn permission_names_are_case_sensitive() {
        assert!(serde_json::from_str::<PermissionSet>(r#"["State.Read"]"#).is_err());
    }

    #[test]
    fn set_rejects_an_unknown_name() {
        assert!(serde_json::from_str::<PermissionSet>(r#"["state.read","bogus"]"#).is_err());
    }

    #[test]
    fn serde_round_trips_as_array_of_names() {
        let set: PermissionSet = [Permission::StateRead, Permission::EventsRead]
            .into_iter()
            .collect();
        let text = serde_json::to_string(&set).expect("set serializes");
        assert_eq!(text, r#"["state.read","events.read"]"#);
        assert_eq!(
            serde_json::from_str::<PermissionSet>(&text).expect("set parses"),
            set
        );
    }

    #[test]
    fn empty_array_deserializes_to_empty_set() {
        let set: PermissionSet = serde_json::from_str("[]").expect("empty array parses");
        assert_eq!(set, PermissionSet::EMPTY);
    }
}
