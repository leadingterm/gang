//! The Gang API (`/v1/*`): routing, body handling and error rendering, driven through a running
//! server.

use crate::ServerSettings;
use gang_spi::{ApiRequestBody, CertStatus, IssuedCertRecord, PrincipalRecord};
use gang_testkit::{Controller, FakeRepository};
use gang_types::{
    AccessToken, CertSerial, Identity, Permission, PermissionSet, Principal, ProviderName,
};
use gang_wire::routes::{ItemRoute, RESTORE_PRINCIPAL, REVOKE_PRINCIPAL};
use gang_wire::{Detail, Status, name};
use http::Request;

use crate::test_support::{
    ApiReply as Answer, api_request, at, identity, read, role_for, tls_settings,
};

pub(crate) const PROVIDER: &str = "example-idp";
pub(crate) const ADMIN: &str = "admin";
pub(crate) const ADMIN_PROOF: &str = "admin-proof";
pub(crate) const ALICE: &str = "alice";
pub(crate) const ALICE_PROOF: &str = "alice-proof";
pub(crate) const BOB: &str = "bob";
pub(crate) const BOB_PROOF: &str = "bob-proof";
pub(crate) const CAROL: &str = "carol";
pub(crate) const WORKER_PROVIDER: &str = "example-workers";
const WORKER: &str = "worker-1";
pub(crate) const WORKER_PROOF: &str = "worker-proof";
pub(crate) const FAR_FUTURE_MILLIS: u64 = 4_102_444_800_000;

/// The request body limit of every test server: 64 KiB.
pub(crate) const BODY_LIMIT: usize = 64 * 1024;

/// The value of the `ErrorInfo` metadata `key` of `error`.
pub(crate) fn metadata<'e>(error: &'e Status, key: &str) -> &'e str {
    error
        .error_info()
        .and_then(|info| info.metadata.get(key))
        .map_or_else(
            || panic!("the error carries metadata {key}"),
            String::as_str,
        )
}

/// The fields a `BadRequest` detail of `error` names, in order.
pub(crate) fn violated_fields(error: &Status) -> Vec<&str> {
    error
        .details
        .iter()
        .filter_map(|detail| match detail {
            Detail::BadRequest(bad_request) => Some(bad_request),
            _ => None,
        })
        .flat_map(|bad_request| &bad_request.field_violations)
        .map(|violation| violation.field.as_str())
        .collect()
}

/// Answers `request` with the Gang API, through the controller's public gate.
pub(crate) async fn send(controller: &Controller, request: Request<ApiRequestBody>) -> Answer {
    read(controller.call_api(request).await).await
}

/// A `GET` of `path` as the holder of `token`, or as an anonymous caller.
pub(crate) async fn get(
    controller: &Controller,
    path: &str,
    token: Option<&AccessToken>,
) -> Answer {
    send(controller, api_request("GET", path, token, None)).await
}

/// A `POST` of `path` with an empty body as the holder of `token`.
pub(crate) async fn post(controller: &Controller, path: &str, token: &AccessToken) -> Answer {
    send(controller, api_request("POST", path, Some(token), None)).await
}

/// The resource name of `principal` under the test provider.
pub(crate) fn principal_name(principal: &str) -> String {
    name::of::<name::Principals>(&identity(principal))
}

/// The path of the principal route `route` for `principal` under the test provider.
pub(crate) fn principal_path(route: &ItemRoute<name::Principals>, principal: &str) -> String {
    route.path(&identity(principal))
}

/// The ledger record of `who`, if the repository holds one.
pub(crate) fn record_of(repository: &FakeRepository, who: &Identity) -> Option<PrincipalRecord> {
    repository
        .principals()
        .into_iter()
        .find(|record| record.identity == *who)
}

/// Revokes `principal` under the test provider as the holder of `admin`.
pub(crate) async fn revoke_principal(
    controller: &Controller,
    admin: &AccessToken,
    principal: &str,
) -> Answer {
    post(
        controller,
        &principal_path(&REVOKE_PRINCIPAL, principal),
        admin,
    )
    .await
}

/// Restores `principal` under the test provider as the holder of `admin`.
pub(crate) async fn restore_principal(
    controller: &Controller,
    admin: &AccessToken,
    principal: &str,
) -> Answer {
    post(
        controller,
        &principal_path(&RESTORE_PRINCIPAL, principal),
        admin,
    )
    .await
}

/// Settings whose roles are `administrator` (every permission, held by [`ADMIN`]) and `role`
/// (`permission` only, held by [`ALICE`]).
pub(crate) fn settings_with_alice(role: &str, permission: Permission) -> ServerSettings {
    ServerSettings {
        roles: vec![
            role_for("administrator", PermissionSet::all(), ADMIN),
            role_for(role, PermissionSet::EMPTY.with(permission), ALICE),
        ],
        ..tls_settings()
    }
}

/// The identity of the worker principal under the worker provider.
pub(crate) fn worker_identity() -> Identity {
    Identity::new(
        ProviderName::try_new(WORKER_PROVIDER).expect("worker provider name is valid"),
        Principal::try_new(WORKER).expect("worker principal is valid"),
    )
}

/// An active certificate with `serial` of the worker principal, valid from the Unix epoch until
/// far in the future.
pub(crate) fn worker_certificate(serial: &str) -> IssuedCertRecord {
    IssuedCertRecord {
        serial: CertSerial::try_new(serial).expect("fixture serial is valid"),
        identity: worker_identity(),
        issued_at: at(0),
        expires_at: at(FAR_FUTURE_MILLIS),
        status: CertStatus::Active,
    }
}

/// Principals whose identity strings hold characters that are special in a URL path or query, a
/// percent sign that is not an escape, and non-ASCII text.
pub(crate) const AWKWARD_PRINCIPALS: [&str; 5] = ["a:b@x%y&z é", "a%41", "aA", "sp ace", "ünï"];
