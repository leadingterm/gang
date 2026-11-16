//! The public service sends a browser opening `/` to the dashboard, routes dashboard paths to
//! browser routes or embedded assets when enabled, and sends all other requests to the Gang API.

use gang_spi::{ApiRequestBody, ApiResponseBody, ApiService, full_body};
use gang_wire::UiRoute;
use http::header::LOCATION;
use http::{HeaderValue, Method, Request, Response, StatusCode};

use crate::api::Api;
use crate::deployment::Stack;
use crate::login::SessionRoutes;
use crate::responses::empty;

/// The server root. No API route is served there; a browser opening the server asks for it.
pub(crate) const ROOT_PATH: &str = "/";

/// Where a browser opening the server root is sent: the dashboard.
const DASHBOARD_HOME: &str = "/ui/";

/// The path prefix the dashboard is served under.
const UI_PREFIX: &str = "/ui";

/// The API and optional browser routes served on the public listener.
pub(crate) struct PublicService<K: Stack> {
    /// The Gang API.
    pub(crate) api: Api<K>,
    /// Browser routes, present when the dashboard is enabled.
    pub(crate) ui: Option<SessionRoutes<K>>,
}

impl<K: Stack> Clone for PublicService<K> {
    fn clone(&self) -> Self {
        Self {
            api: self.api.clone(),
            ui: self.ui.clone(),
        }
    }
}

impl<K: Stack> ApiService for PublicService<K> {
    async fn call(&self, request: Request<ApiRequestBody>) -> Response<ApiResponseBody> {
        if let Some(ui) = &self.ui {
            if request.uri().path() == ROOT_PATH
                && matches!(*request.method(), Method::GET | Method::HEAD)
            {
                return dashboard_redirect();
            }
            if is_ui_path(request.uri().path()) {
                return match UiRoute::of(request.uri().path()) {
                    Some(route) => ui.answer(route, request).await,
                    None => {
                        gang_ui::respond(request.method(), request.uri().path(), request.headers())
                            .map(full_body)
                    }
                };
            }
        }
        self.api.call(request).await
    }
}

/// A `302` to the dashboard, so a browser opening the server lands there instead of an error.
fn dashboard_redirect() -> Response<ApiResponseBody> {
    let mut response = empty(StatusCode::FOUND);
    response
        .headers_mut()
        .insert(LOCATION, HeaderValue::from_static(DASHBOARD_HOME));
    response
}

/// Whether `path` is `prefix` or below it: `/uix` is not under `/ui`.
#[must_use]
pub(crate) fn under(path: &str, prefix: &str) -> bool {
    path.strip_prefix(prefix)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
}

/// Whether `path` is `/ui` or below it. `/uix` is not.
#[must_use]
pub(crate) fn is_ui_path(path: &str) -> bool {
    under(path, UI_PREFIX)
}
