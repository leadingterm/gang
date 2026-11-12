//! The authorizer: decides which permissions each identity holds, from the roles the server is
//! configured with.

use std::collections::HashMap;

use gang_config::RoleConfig;
use gang_types::{Identity, Permission, PermissionSet};
use gang_wire::Role;
use gang_wire::name::{self, Roles};

/// Builds the wire form of a configured role, expanding permissions in their canonical order.
fn wire_role(config: &RoleConfig) -> Role {
    Role {
        name: name::of::<Roles>(&config.name),
        permissions: config
            .permissions
            .iter()
            .map(gang_wire::Permission::from)
            .collect(),
        principals: config
            .principals
            .iter()
            .map(|binding| {
                let mut principal = String::from("principals/");
                name::push_segment(
                    &mut principal,
                    &format_args!("{}:{}", binding.provider, binding.principal),
                );
                principal
            })
            .collect(),
    }
}

/// Grants each identity the union of the permissions of the roles that bind it.
pub(crate) struct Authorizer {
    roles: Vec<Role>,
    grants: HashMap<Identity, PermissionSet>,
}

impl Authorizer {
    /// Builds the authorizer from `roles`. An identity holds the permissions of every role that
    /// binds it.
    #[must_use]
    pub(crate) fn new(roles: Vec<RoleConfig>) -> Self {
        let mut grants: HashMap<Identity, PermissionSet> = HashMap::new();
        let mut wire_roles = Vec::with_capacity(roles.len());
        for role in roles {
            wire_roles.push(wire_role(&role));
            for binding in role.principals {
                let identity = Identity::new(binding.provider, binding.principal);
                let granted = grants.entry(identity).or_default();
                *granted = granted.union(role.permissions);
            }
        }
        let mut roles = wire_roles;
        roles.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        Self { roles, grants }
    }

    /// Whether `identity` holds `permission`.
    #[must_use]
    pub(crate) fn allows(&self, identity: &Identity, permission: Permission) -> bool {
        self.permissions(identity).contains(permission)
    }

    /// The permissions `identity` holds: empty when no role binds it.
    #[must_use]
    pub(crate) fn permissions(&self, identity: &Identity) -> PermissionSet {
        self.grants.get(identity).copied().unwrap_or_default()
    }

    /// The configured roles in ascending resource-name order.
    #[must_use]
    pub(crate) fn roles(&self) -> &[Role] {
        &self.roles
    }
}

#[cfg(test)]
mod tests {
    use gang_config::{RoleConfig, RolePrincipal};
    use gang_types::{Identity, Permission, PermissionSet, Principal, ProviderName, RoleName};

    use super::Authorizer;

    fn identity(provider: &str, principal: &str) -> Identity {
        Identity::new(
            ProviderName::try_new(provider).expect("provider name is valid"),
            Principal::try_new(principal).expect("principal is valid"),
        )
    }

    fn role(name: &str, permissions: PermissionSet, bindings: &[(&str, &str)]) -> RoleConfig {
        RoleConfig {
            name: RoleName::try_new(name).expect("role name is valid"),
            permissions,
            principals: bindings
                .iter()
                .map(|(provider, principal)| RolePrincipal {
                    provider: ProviderName::try_new(*provider).expect("provider name is valid"),
                    principal: Principal::try_new(*principal).expect("principal is valid"),
                })
                .collect(),
        }
    }

    #[test]
    fn an_identity_holds_the_union_of_its_roles() {
        let authorizer = Authorizer::new(vec![
            role(
                "state-reader",
                PermissionSet::EMPTY.with(Permission::StateRead),
                &[("example-idp", "ada@example.com")],
            ),
            role(
                "scaling-reader",
                PermissionSet::EMPTY.with(Permission::EventsRead),
                &[("example-idp", "ada@example.com")],
            ),
        ]);
        let ada = identity("example-idp", "ada@example.com");
        let expected = PermissionSet::EMPTY
            .with(Permission::StateRead)
            .with(Permission::EventsRead);

        assert_eq!(authorizer.permissions(&ada), expected);
        assert!(authorizer.allows(&ada, Permission::StateRead));
        assert!(authorizer.allows(&ada, Permission::EventsRead));
        assert!(!authorizer.allows(&ada, Permission::PrincipalsRevoke));
    }

    #[test]
    fn a_role_with_every_permission_grants_each_of_them() {
        let authorizer = Authorizer::new(vec![role(
            "administrator",
            PermissionSet::all(),
            &[("example-idp", "admin")],
        )]);
        let admin = identity("example-idp", "admin");

        for permission in Permission::ALL {
            assert!(
                authorizer.allows(&admin, permission),
                "{permission} is granted"
            );
        }
    }

    #[test]
    fn an_unbound_identity_holds_nothing() {
        let authorizer = Authorizer::new(vec![role(
            "administrator",
            PermissionSet::all(),
            &[("example-idp", "admin")],
        )]);
        let stranger = identity("example-idp", "alice");

        assert!(authorizer.permissions(&stranger).is_empty());
        for permission in Permission::ALL {
            assert!(
                !authorizer.allows(&stranger, permission),
                "{permission} is denied"
            );
        }
    }

    #[test]
    fn the_same_principal_under_another_provider_holds_nothing() {
        let authorizer = Authorizer::new(vec![role(
            "administrator",
            PermissionSet::all(),
            &[("example-idp", "admin")],
        )]);

        assert!(
            authorizer
                .permissions(&identity("other-idp", "admin"))
                .is_empty()
        );
    }

    #[test]
    fn the_anonymous_identity_can_be_bound_by_a_role() {
        let authorizer = Authorizer::new(vec![role(
            "anonymous-reader",
            PermissionSet::EMPTY.with(Permission::EventsRead),
            &[("none", "anonymous")],
        )]);
        let anonymous = identity("none", "anonymous");

        assert!(authorizer.allows(&anonymous, Permission::EventsRead));
        assert!(!authorizer.allows(&anonymous, Permission::PrincipalsRevoke));
    }
}
