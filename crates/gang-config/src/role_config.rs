//! The `[[server.roles]]` entries: the permissions each role grants and the principals holding it.

use gang_types::{PermissionSet, Principal, ProviderName, RoleName};
use serde::{Deserialize, Serialize};

/// A role: a named set of permissions granted to the principals listed under it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleConfig {
    /// The role's name. Unique among the roles.
    pub name: RoleName,
    /// The permissions the role grants. `"*"` grants every permission. Must be non-empty.
    pub permissions: PermissionSet,
    /// The principals that hold the role. Must be non-empty.
    pub principals: Vec<RolePrincipal>,
}

/// A principal that holds a role, authenticated by an identity provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RolePrincipal {
    /// The identity provider that vouches for the principal. Must name a key of
    /// `[identity.providers]`, or the reserved `none` for anonymous callers.
    pub provider: ProviderName,
    /// The principal the provider authenticates.
    pub principal: Principal,
}

impl RoleConfig {
    /// The role of an unauthenticated server: every permission, held by the anonymous caller.
    #[must_use]
    pub fn anonymous_administrator() -> Self {
        Self {
            name: RoleName::ADMINISTRATOR,
            permissions: PermissionSet::all(),
            principals: vec![RolePrincipal {
                provider: ProviderName::ANONYMOUS,
                principal: Principal::ANONYMOUS,
            }],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_anonymous_administrator_holds_every_permission() {
        let role = RoleConfig::anonymous_administrator();

        assert_eq!(role.permissions, PermissionSet::all());
        assert_eq!(role.principals.len(), 1);
        assert!(role.principals[0].provider.is_anonymous());
    }
}
