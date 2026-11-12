//! The public service routes dashboard paths to browser routes or embedded assets when the
//! dashboard is enabled, and all other requests to the Gang API.

use gang_spi::{ApiRequestBody, ApiResponseBody, ApiService, full_body};
use gang_wire::UiRoute;
use http::{Request, Response};

use crate::api::Api;
use crate::deployment::Stack;
use crate::login::SessionRoutes;

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
        if let Some(ui) = &self.ui
            && is_ui_path(request.uri().path())
        {
            return match UiRoute::of(request.uri().path()) {
                Some(route) => ui.answer(route, request).await,
                None => gang_ui::respond(request.method(), request.uri().path(), request.headers())
                    .map(full_body),
            };
        }
        self.api.call(request).await
    }
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
