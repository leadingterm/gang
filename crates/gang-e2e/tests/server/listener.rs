//! The simulated public listener: the routes the server scenarios request, answered as the
//! behaviour says.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use gang_e2e::{
    Answer, Credentials, E2eError, Exit, Handshake, MAX_REQUEST_SIZE, Presented, RATE_LIMIT_BURST,
    Request, ServerApi, SessionCookie,
};
use gang_ui::SECURITY_HEADERS;
use gang_wire::{
    ErrorReason, ErrorResponse, Status,
    page::{PAGE_SIZE_DEFAULT, PAGE_SIZE_MAX, PageToken},
};
use http::Method;
use parking_lot::Mutex;
use serde_json::{Value, json};

use super::behaviour::{
    ADMIN_PRINCIPAL, ADMIN_SESSION, ADMIN_TOKEN, AssetCaching, AssetEncoding, Behaviour, BodyLimit,
    Burst, Challenge, CookiePost, ETAG, Envelope, Etag, LastPage, Logout, ORIGIN, PageSize,
    RateLimit, RetryAfter, RouteGate, SCRIPT, STYLESHEET, ShutdownExit, SimState, TamperedToken,
    TokenFilter, UiHeaders, UiRefusal, UnknownRoute, Walk, admin_cookie, administrator,
};
use super::stream::{SimHeld, SimStream, leaf};

/// The simulated public listener used by the server scenarios.
pub(crate) struct SimServer {
    /// The independent behaviour switches for this simulation.
    behaviour: Behaviour,
    /// The mutable server state shared with streams and held channels.
    state: Arc<Mutex<SimState>>,
}

impl SimServer {
    /// Creates a running simulated server with empty per-caller rate-limit state.
    pub(crate) fn new(behaviour: Behaviour) -> Self {
        Self {
            behaviour,
            state: Arc::new(Mutex::new(SimState::new())),
        }
    }

    /// Builds the configured error envelope.
    fn envelope(
        &self,
        reason: ErrorReason,
        metadata: Vec<(&'static str, String)>,
    ) -> gang_e2e::Result<Answer> {
        let status = reason.http();
        let mut answer = if self.behaviour.envelope == Envelope::Google {
            Answer::json(
                status,
                &ErrorResponse {
                    error: Status::new(reason, reason.message(), metadata),
                },
            )?
        } else {
            Answer::json(status, &json!({"message": "refused"}))?
        };
        if status == 401 && self.behaviour.challenge == Challenge::Bearer {
            answer = answer.with_header("www-authenticate", "Bearer");
        }
        Ok(answer)
    }

    /// Lists a filtered, paginated resource.
    fn list(
        &self,
        items: &[Value],
        field: &str,
        query: &[(String, String)],
    ) -> gang_e2e::Result<Answer> {
        let filter = query
            .iter()
            .find(|(key, _)| key == "filter")
            .map_or("", |(_, value)| value.as_str());
        let items: Vec<Value> = if field == "principals" && filter == "revokeTime:*" {
            if self.state.lock().revoked {
                items
                    .iter()
                    .filter(|item| item["principal"] == ADMIN_PRINCIPAL)
                    .cloned()
                    .collect()
            } else {
                Vec::new()
            }
        } else if field == "principals" && !filter.is_empty() {
            return self.envelope(ErrorReason::MalformedRequest, Vec::new());
        } else {
            items.to_vec()
        };
        let size = match query.iter().find(|(key, _)| key == "pageSize") {
            None => PAGE_SIZE_DEFAULT,
            Some((_, value)) if value.is_empty() || value == "0" => PAGE_SIZE_DEFAULT,
            Some((_, value)) => match value.parse::<usize>() {
                Ok(size) => size,
                Err(_) => return self.envelope(ErrorReason::MalformedRequest, Vec::new()),
            },
        };
        if size > PAGE_SIZE_MAX && self.behaviour.page_size == PageSize::Refused {
            return self.envelope(ErrorReason::MalformedRequest, Vec::new());
        }
        let size = size.min(PAGE_SIZE_MAX);
        let token = query
            .iter()
            .find(|(key, _)| key == "pageToken")
            .map(|(_, value)| value);
        let start = match token {
            None => 0,
            Some(text) => {
                let token = PageToken::from(text.clone());
                if let Ok(cursor) = token.cursor(filter) {
                    if let Ok(cursor) = cursor.parse::<usize>() {
                        cursor
                    } else {
                        return self.envelope(ErrorReason::InvalidPageToken, Vec::new());
                    }
                } else {
                    let other = if filter.is_empty() {
                        "revokeTime:*"
                    } else {
                        ""
                    };
                    let foreign = token.cursor(other);
                    match (self.behaviour.token_filter, foreign) {
                        (TokenFilter::Ignored, Ok(cursor)) => match cursor.parse::<usize>() {
                            Ok(cursor) => cursor,
                            Err(_) => {
                                return self.envelope(ErrorReason::InvalidPageToken, Vec::new());
                            }
                        },
                        (TokenFilter::Bound, Err(_))
                            if self.behaviour.tampered_token == TamperedToken::Restarts =>
                        {
                            0
                        }
                        _ => return self.envelope(ErrorReason::InvalidPageToken, Vec::new()),
                    }
                }
            }
        };
        let end = start.saturating_add(size).min(items.len());
        let page = &items[start.min(items.len())..end];
        let more = start.saturating_add(size) < items.len();
        let token_cursor = if self.behaviour.walk == Walk::SkipsOne && start == 0 {
            start.saturating_add(size).saturating_add(1)
        } else {
            start.saturating_add(size)
        };
        let next = if more {
            Some(PageToken::new(filter, &token_cursor.to_string()))
        } else if self.behaviour.last_page == LastPage::Trailing && !page.is_empty() {
            Some(PageToken::new(filter, &items.len().to_string()))
        } else {
            None
        };
        let mut body = json!({field: page});
        if let Some(next) = next {
            body["nextPageToken"] = json!(next.as_str());
        }
        Answer::json(200, &body)
    }

    /// Applies the per-caller rate limit to gated requests.
    fn rate_limited(
        &self,
        state: &mut SimState,
        request: &Request,
        path: &str,
    ) -> gang_e2e::Result<Option<Answer>> {
        let caller = request
            .headers
            .iter()
            .find(|(key, _)| *key == "x-forwarded-for")
            .map_or("loopback", |(_, value)| value.as_str())
            .to_owned();
        let gated = (request.method == Method::GET && path == "/v1/server")
            || (request.method == Method::POST && path == "/v1/session:authenticate")
            || (request.method == Method::GET && path == "/ui/login");
        if gated && self.behaviour.rate_limit == RateLimit::PerCaller {
            let allowance = RATE_LIMIT_BURST - u32::from(self.behaviour.burst == Burst::Short);
            let spent = state.spent.entry(caller).or_default();
            if *spent >= allowance {
                let ui = path == "/ui/login";
                let mut answer = if ui && self.behaviour.ui_refusal == UiRefusal::Empty {
                    Answer::empty(429)
                } else {
                    self.envelope(ErrorReason::RateLimited, Vec::new())?
                };
                if self.behaviour.retry_after == RetryAfter::WholeSeconds {
                    answer = answer.with_header("retry-after", "3600");
                }
                return Ok(Some(answer));
            }
            *spent += 1;
        }
        Ok(None)
    }

    /// Answers simulated API routes.
    fn api_route(
        &self,
        request: &Request,
        path: &str,
        query: &[(String, String)],
        admin: bool,
    ) -> gang_e2e::Result<Answer> {
        if request.method == Method::GET && path == "/v1/server" {
            Answer::json(200, &json!({}))
        } else if request.method == Method::POST && path == "/v1/session:authenticate" {
            if request
                .body
                .as_ref()
                .is_some_and(|body| body.bytes.len() > MAX_REQUEST_SIZE)
                && self.behaviour.body_limit == BodyLimit::Enforced
            {
                self.envelope(ErrorReason::RequestTooLarge, Vec::new())
            } else {
                self.envelope(ErrorReason::ProofRejected, Vec::new())
            }
        } else if request.method == Method::GET
            && ["/v1/state", "/v1/principals", "/v1/events"].contains(&path)
        {
            if !admin {
                self.envelope(ErrorReason::CredentialsMissing, Vec::new())
            } else if path == "/v1/state" {
                Answer::json(200, &json!({}))
            } else if path == "/v1/principals" {
                let items: Vec<Value> = ["administrator", "developer", "observer"]
                    .iter()
                    .map(|principal| {
                        json!({
                            "name": format!("principals/e2e%3A{principal}"),
                            "provider": "e2e",
                            "principal": principal,
                        })
                    })
                    .collect();
                self.list(&items, "principals", query)
            } else {
                let items: Vec<Value> = [4, 3, 2, 1].iter().map(|n| json!({"seq": n})).collect();
                self.list(&items, "events", query)
            }
        } else if request.method == Method::POST
            && path.starts_with("/v1/principals/")
            && path.ends_with(":revoke")
        {
            if admin {
                let principal = path
                    .trim_start_matches("/v1/principals/")
                    .trim_end_matches(":revoke")
                    .replace("%3A", ":");
                if principal == ADMIN_PRINCIPAL {
                    self.state.lock().revoked = true;
                }
                Answer::json(200, &json!({}))
            } else {
                self.envelope(ErrorReason::CredentialsMissing, Vec::new())
            }
        } else if !admin && self.behaviour.route_gate == RouteGate::CredentialsFirst {
            self.envelope(ErrorReason::CredentialsMissing, Vec::new())
        } else if self.behaviour.unknown_route == UnknownRoute::NotFound {
            self.envelope(
                ErrorReason::RouteNotFound,
                vec![
                    ("method", request.method.to_string()),
                    ("path", path.to_owned()),
                ],
            )
        } else {
            Ok(Answer::empty(405))
        }
    }

    /// Answers simulated dashboard routes.
    fn ui_route(&self, request: &Request, path: &str) -> Answer {
        let header = |name: &str| {
            request
                .headers
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.as_str())
        };
        if request.method == Method::GET && path == "/ui/" {
            let mut answer = Answer::empty(200).with_header("content-type", "text/html");
            answer.body = format!(
                "<link rel=\"stylesheet\" href=\"{STYLESHEET}\">\
                 <script type=\"module\" src=\"{SCRIPT}\"></script>"
            )
            .into_bytes();
            for (name, value) in SECURITY_HEADERS {
                if self.behaviour.ui_headers != UiHeaders::NoCsp
                    || name != "content-security-policy"
                {
                    answer = answer.with_header(name, value);
                }
            }
            answer
        } else if request.method == Method::GET && (path == STYLESHEET || path == SCRIPT) {
            if header("if-none-match") == Some(ETAG) && self.behaviour.etag == Etag::Revalidates {
                Answer::empty(304).with_header("etag", ETAG)
            } else {
                let mut answer = Answer::empty(200).with_header("etag", ETAG).with_header(
                    "cache-control",
                    if self.behaviour.asset_caching == AssetCaching::Immutable {
                        "public, max-age=31536000, immutable"
                    } else {
                        "no-store"
                    },
                );
                answer.body = if header("accept-encoding").unwrap_or("").contains("br")
                    && self.behaviour.asset_encoding == AssetEncoding::Brotli
                {
                    b"compressed".to_vec()
                } else {
                    b"plain".to_vec()
                };
                if header("accept-encoding").unwrap_or("").contains("br")
                    && self.behaviour.asset_encoding == AssetEncoding::Brotli
                {
                    answer = answer.with_header("content-encoding", "br");
                }
                answer
            }
        } else if request.method == Method::POST && path == "/ui/logout" {
            let same_origin = header("origin") == Some(ORIGIN)
                || header("sec-fetch-site") == Some("same-origin")
                || self.behaviour.logout == Logout::AnyOrigin;
            if same_origin {
                Answer::empty(204).with_header(
                    "set-cookie",
                    format!("{}=; Max-Age=0", super::behaviour::COOKIE_NAME),
                )
            } else {
                Answer::empty(403)
            }
        } else {
            Answer::empty(404)
        }
    }
}

impl ServerApi for SimServer {
    type Stream = SimStream;
    type Held = SimHeld;

    fn administrator_credentials(&self) -> gang_e2e::Result<Credentials> {
        Ok(administrator())
    }

    fn cookie_of(&self, _: &Credentials) -> gang_e2e::Result<SessionCookie> {
        Ok(admin_cookie())
    }

    fn send(&self, request: &Request) -> gang_e2e::Result<Answer> {
        let mut state = self.state.lock();
        if !state.running {
            return Err(E2eError::system("the simulated server is not running"));
        }
        let mut split = request.target.splitn(2, '?');
        let path = split.next().unwrap_or_default();
        let query: Vec<(String, String)> = split
            .next()
            .unwrap_or_default()
            .split('&')
            .filter(|pair| !pair.is_empty())
            .map(|pair| {
                let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
                (key.to_owned(), value.replace("%3A", ":"))
            })
            .collect();
        if let Some(answer) = self.rate_limited(&mut state, request, path)? {
            return Ok(answer);
        }
        let admin = !state.revoked
            && match &request.presented {
                Presented::Bearer(token) => token == ADMIN_TOKEN,
                Presented::Cookie(cookie) if cookie.value == ADMIN_SESSION => {
                    request.method == Method::GET
                        || self.behaviour.cookie_post == CookiePost::Admitted
                }
                _ => false,
            };
        drop(state);
        if path.starts_with("/v1/") {
            self.api_route(request, path, &query, admin)
        } else {
            Ok(self.ui_route(request, path))
        }
    }

    fn watch(&self, cookie: &SessionCookie) -> gang_e2e::Result<Self::Stream> {
        SimStream::open(&self.state, self.behaviour, cookie)
    }

    fn leaf(&self) -> gang_e2e::Result<Handshake> {
        Ok(leaf(&self.state.lock(), self.behaviour))
    }

    fn hold(&self, _: &Credentials) -> gang_e2e::Result<Self::Held> {
        Ok(SimHeld::new(&self.state, self.behaviour))
    }

    fn wait_until(&self, at: SystemTime) {
        let mut state = self.state.lock();
        state.now = state.now.max(at);
    }

    fn terminate(&self, _: Duration) -> gang_e2e::Result<Exit> {
        let mut state = self.state.lock();
        if !state.running {
            return Err(E2eError::system("the simulated server is not running"));
        }
        state.running = false;
        Ok(if self.behaviour.exit == ShutdownExit::Failure {
            Exit::Code(1)
        } else {
            Exit::Code(0)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignored_filter_refuses_non_numeric_foreign_cursor() {
        let server = SimServer::new(Behaviour {
            token_filter: TokenFilter::Ignored,
            ..Behaviour::COMPLIANT
        });
        let token = PageToken::new("revokeTime:*", "not-a-number");
        let query = vec![("pageToken".to_owned(), token.as_str().to_owned())];

        let answer = server.list(&[], "events", &query).expect("list succeeds");
        let expected = server
            .envelope(ErrorReason::InvalidPageToken, Vec::new())
            .expect("error envelope serializes");
        assert_eq!(answer.status, expected.status);
        assert_eq!(answer.body, expected.body);
    }
}
