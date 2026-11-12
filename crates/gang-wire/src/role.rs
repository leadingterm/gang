//! The `roles/{role}` resource and its list answer.

use serde::{Deserialize, Serialize};

use crate::Permission;
use crate::page::PageToken;

/// A role and the permissions and principals it grants, named `roles/{role}`.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct Role {
    /// Resource name, `roles/{role}`.
    pub name: String,
    /// The permissions the role grants, with `*` expanded to the permission names.
    pub permissions: Vec<Permission>,
    /// References `principals/{id}` of the principals that hold the role.
    pub principals: Vec<String>,
}

/// One page of roles, the body of `GET /v1/roles`.
#[derive(gang_macros::Known, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(optional_fields)]
pub struct ListRolesResponse {
    /// The roles on this page.
    pub roles: Vec<Role>,
    /// Token that resumes the list; absent on the last page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<PageToken>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn role_round_trips_exactly() {
        let value = json!({
            "name": "roles/administrator",
            "permissions": ["state.read", "principals.read", "events.read", "principals.revoke"],
            "principals": ["principals/google%3Aops%40example.org"],
        });
        let role: Role = serde_json::from_value(value.clone()).expect("role parses");
        assert_eq!(
            role.permissions,
            gang_types::Permission::ALL
                .map(crate::Permission::from)
                .to_vec()
        );
        assert_eq!(serde_json::to_value(&role).expect("role serializes"), value);
    }

    #[test]
    fn role_ignores_unknown_fields_and_keeps_unknown_permissions() {
        let tolerated = json!({"name": "roles/x", "permissions": [], "principals": [], "f": 13});
        assert!(serde_json::from_value::<Role>(tolerated).is_ok());
        let unknown = json!({"name": "roles/x", "permissions": ["bogus"], "principals": []});
        let parsed: Role = serde_json::from_value(unknown.clone()).expect("role parses");
        assert_eq!(
            serde_json::to_value(&parsed).expect("role serializes"),
            unknown
        );
    }

    #[test]
    fn list_response_omits_an_absent_token() {
        let value = json!({"roles": []});
        let parsed: ListRolesResponse =
            serde_json::from_value(value.clone()).expect("response parses");
        assert_eq!(parsed.next_page_token, None);
        assert_eq!(
            serde_json::to_value(&parsed).expect("response serializes"),
            value
        );
    }
}
