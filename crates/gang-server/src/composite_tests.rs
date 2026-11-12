//! Routing between the public API, browser session routes, and embedded dashboard assets.

use std::num::NonZeroUsize;

use gang_config::DashboardConfig;
use gang_spi::LoginClient;
use gang_types::{ClientSecret, NonZeroDuration, ProviderName};
use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue, IF_NONE_MATCH};

use crate::composite::is_ui_path;
use crate::{LoginSettings, ServerSettings};

use crate::test_support::{api_request, mint_token, read, serve, tls_parts, tls_settings};

/// Server settings with an interactive browser login and the requested dashboard state.
fn settings(enabled: bool) -> ServerSettings {
    ServerSettings {
        dashboard: DashboardConfig {
            enabled,
            ..DashboardConfig::default()
        },
        login: Some(LoginSettings {
            provider: ProviderName::try_new("example-idp").expect("provider name is valid"),
            client: LoginClient {
                id: "web-client".to_owned(),
                secret: ClientSecret::new("secret"),
            },
            ttl: NonZeroDuration::from_mins(10),
            max_pending: NonZeroUsize::new(8).expect("8 is nonzero"),
        }),
        ..tls_settings()
    }
}

#[tokio::test]
async fn enabled_dashboard_routes_sessions_and_assets() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        settings(true),
        handles.scheduler,
        |controller| async move {
            let index = gang_ui::lookup("index.html").expect("dashboard embeds index.html");
            for path in ["/ui", "/ui/x"] {
                let response = controller
                    .call_api(api_request("GET", path, None, None))
                    .await;
                assert_eq!(response.status(), StatusCode::OK);
                assert_eq!(response.headers()[CONTENT_TYPE], "text/html; charset=utf-8");
                let body = read(response).await.body;
                assert_eq!(&body[..], index.identity);
            }
            let response = controller
                .call_api(api_request("GET", "/ui/login", None, None))
                .await;
            assert_eq!(response.status(), StatusCode::FOUND);
            let token = mint_token(&controller, "example-idp", "admin-proof").await;
            let response = controller
                .call_api(api_request("GET", "/uix", Some(&token), None))
                .await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND);

            let mut request = api_request("GET", "/ui/", None, None);
            request
                .headers_mut()
                .insert(IF_NONE_MATCH, HeaderValue::from_static(index.etag));
            let reply = read(controller.call_api(request).await).await;
            assert_eq!(reply.status, StatusCode::NOT_MODIFIED);
            assert!(reply.body.is_empty());
        },
    )
    .await;
}

#[tokio::test]
async fn unmounted_login_routes_do_not_fall_through_to_dashboard() {
    let (parts, _dials, handles) = tls_parts();
    let mut settings = settings(true);
    settings.login = None;
    serve(
        parts,
        settings,
        handles.scheduler,
        |controller| async move {
            for path in ["/ui/login", "/ui/callback"] {
                let reply = read(
                    controller
                        .call_api(api_request("GET", path, None, None))
                        .await,
                )
                .await;
                assert_eq!(reply.status, StatusCode::NOT_FOUND, "{path}");
                assert!(reply.body.is_empty(), "{path}");
            }

            let index = gang_ui::lookup("index.html").expect("dashboard embeds index.html");
            for path in ["/ui/error", "/ui/workers"] {
                let reply = read(
                    controller
                        .call_api(api_request("GET", path, None, None))
                        .await,
                )
                .await;
                assert_eq!(reply.status, StatusCode::OK, "{path}");
                assert_eq!(&reply.body[..], index.identity, "{path}");
            }
        },
    )
    .await;
}

#[tokio::test]
async fn disabled_dashboard_routes_ui_paths_to_api() {
    let (parts, _dials, handles) = tls_parts();
    serve(
        parts,
        settings(false),
        handles.scheduler,
        |controller| async move {
            for path in ["/ui", "/ui/x", "/ui/login"] {
                let response = controller
                    .call_api(api_request("GET", path, None, None))
                    .await;
                assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
            }
        },
    )
    .await;
}

#[test]
fn the_prefix_boundary_is_a_path_segment() {
    for path in ["/ui", "/ui/", "/ui/x", "/ui/x/y"] {
        assert!(is_ui_path(path), "{path:?}");
    }
    for path in ["", "/", "/u", "/uix", "/ui-x", "/v1/ui", "/UI"] {
        assert!(!is_ui_path(path), "{path:?}");
    }
}
