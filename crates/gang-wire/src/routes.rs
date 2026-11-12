//! The route table: the one owner of each HTTP route's method, path template, permission, body
//! and response types, cookie eligibility, and rate limiting.
//!
//! The server's matcher, the cookie paths, the client's path building and the API reference test
//! all derive from [`ROUTES`]. Nothing else states a method, path or permission for an endpoint.

use gang_types::Permission;

use crate::name;

/// Path that starts a browser sign-in.
pub const LOGIN_PATH: &str = "/ui/login";

/// Path that completes a browser sign-in.
pub const CALLBACK_PATH: &str = "/ui/callback";

/// Path that ends a browser session.
pub const LOGOUT_PATH: &str = "/ui/logout";

/// A `/ui` path the server answers itself, whether or not its feature is mounted. The
/// dashboard serves every other `/ui` path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UiRoute {
    /// Starts browser sign-in.
    Login,
    /// Completes browser sign-in.
    Callback,
    /// Ends a browser session.
    Logout,
}

impl UiRoute {
    /// Every server-owned dashboard path.
    pub const ALL: [Self; 3] = [Self::Login, Self::Callback, Self::Logout];

    /// The path owned by this route.
    #[must_use]
    pub const fn path(self) -> &'static str {
        match self {
            Self::Login => LOGIN_PATH,
            Self::Callback => CALLBACK_PATH,
            Self::Logout => LOGOUT_PATH,
        }
    }

    /// Finds the server-owned route for an exact path.
    #[must_use]
    pub fn of(path: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|route| route.path() == path)
    }
}

/// Dashboard client page that a failed sign-in lands on, not a server route.
pub const ERROR_PATH: &str = "/ui/error";

gang_types::string_enum! {
    /// HTTP method of a route.
    pub enum Method("method") {
        /// `GET`: a read with no side effects.
        Get = "GET",
        /// `POST`: a create or a custom method with side effects.
        Post = "POST",
    }
}

/// One entry of the route table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Route {
    /// Identifier of this route, unique within [`ROUTES`].
    pub id: RouteId,
    /// HTTP method the route answers.
    pub method: Method,
    /// Path template, such as `/v1/principals/{principal}:revoke`. At most one `{param}`
    /// placeholder, which spans exactly one path segment.
    pub template: &'static str,
    /// Permission the caller must hold; `None` if the route is open.
    pub permission: Option<Permission>,
    /// Wire type name of the JSON request body, if the route takes one.
    pub request: Option<&'static str>,
    /// Wire type name of the response; for a stream, the type of each frame.
    pub response: &'static str,
    /// Whether this `GET` may identify its caller by the session cookie.
    pub cookie: bool,
    /// Whether requests spend the caller's and the shared rate-limit buckets.
    pub rate_limited: bool,
}

impl Route {
    /// Matches a request's method and path against this route.
    ///
    /// Returns `None` if the method or path does not match. Otherwise returns
    /// `Some(None)` for a template without a placeholder, or `Some(Some(raw))` where `raw` is
    /// the still-encoded placeholder segment. The segment is non-empty and contains neither `/`
    /// nor `:`.
    #[must_use]
    pub fn matches<'p>(&self, method: Method, path: &'p str) -> Option<Option<&'p str>> {
        if method != self.method {
            return None;
        }
        let Some((prefix, suffix)) = split_template(self.template) else {
            return (path == self.template).then_some(None);
        };
        let raw = path.strip_prefix(prefix)?.strip_suffix(suffix)?;
        if raw.is_empty() || raw.contains(['/', ':']) {
            return None;
        }
        Some(Some(raw))
    }
}

/// A parameterized route whose identifier belongs to collection `C`.
#[derive(Debug, Clone, Copy)]
pub struct ItemRoute<C: name::Collection> {
    /// The route table entry.
    route: Route,
    collection: std::marker::PhantomData<C>,
}

impl<C: name::Collection> ItemRoute<C> {
    /// Builds a path by encoding the collection's identifier as one segment.
    #[must_use]
    pub fn path(&self, id: &C::Id) -> String {
        let mut path = String::with_capacity(self.route.template.len());
        let mut placeholder = false;
        for ch in self.route.template.chars() {
            match ch {
                '{' => {
                    placeholder = true;
                    name::push_segment(&mut path, id);
                }
                '}' => placeholder = false,
                _ if !placeholder => path.push(ch),
                _ => {}
            }
        }
        path
    }
}

impl<C: name::Collection> std::ops::Deref for ItemRoute<C> {
    type Target = Route;

    fn deref(&self) -> &Self::Target {
        &self.route
    }
}

/// Selects the type and value generated for a table row.
macro_rules! route_type {
    () => {
        Route
    };
    ($collection:ty) => {
        ItemRoute<$collection>
    };
}

/// Wraps a parameterized row in its collection type.
macro_rules! route_value {
    ($route:expr) => {
        $route
    };
    ($route:expr, $collection:ty) => {
        ItemRoute::<$collection> {
            route: $route,
            collection: std::marker::PhantomData,
        }
    };
}

/// Extracts the common route entry for the table.
macro_rules! route_entry {
    ($route:ident) => {
        $route
    };
    ($route:ident, $collection:ty) => {
        $route.route
    };
}

/// Splits a template around its placeholder into the text before and after `{...}`, or `None` if
/// the template has no placeholder.
#[must_use]
fn split_template(template: &str) -> Option<(&str, &str)> {
    let start = template.find('{')?;
    let end = template[start..].find('}')? + start;
    Some((&template[..start], &template[end + 1..]))
}

/// Declares the route table. Each row generates the route's [`RouteId`] variant, its [`Route`]
/// constant and its entry in [`ROUTES`], in row order, so a route cannot be declared without being
/// listed.
macro_rules! routes {
    ($( $(#[$meta:meta])* $name:ident = $variant:ident {
        method: $method:expr,
        template: $template:expr,
        $(collection: $collection:ty,)?
        permission: $permission:expr,
        request: $request:expr,
        response: $response:expr,
        cookie: $cookie:expr,
        rate_limited: $rate_limited:expr $(,)?
    }; )*) => {
        /// Identifies one route of the table.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum RouteId {
            $( $(#[$meta])* $variant, )*
        }

        $(
            $(#[$meta])*
            pub const $name: route_type!($($collection)?) = route_value!(Route {
                id: RouteId::$variant,
                method: $method,
                template: $template,
                permission: $permission,
                request: $request,
                response: $response,
                cookie: $cookie,
                rate_limited: $rate_limited,
            } $(, $collection)?);
        )*

        /// Every route of the API, in matching order.
        pub static ROUTES: [Route; [$(RouteId::$variant),*].len()] =
            [$(route_entry!($name $(, $collection)?)),*];
    };
}

routes! {
    /// `GET /v1/server`: server information. Open to any caller.
    GET_SERVER = GetServer {
        method: Method::Get,
        template: "/v1/server",
        permission: None,
        request: None,
        response: "Server",
        cookie: false,
        rate_limited: true,
    };

    /// `POST /v1/session:authenticate`: exchanges an identity-provider proof for a bearer
    /// credential. Open, mounted only if the client plugin offers tokens.
    AUTHENTICATE_SESSION = AuthenticateSession {
        method: Method::Post,
        template: "/v1/session:authenticate",
        permission: None,
        request: Some("AuthenticateRequest"),
        response: "AuthenticateResponse",
        cookie: false,
        rate_limited: true,
    };

    /// `POST /v1/certificates`: issues a worker certificate for a signing request. Open,
    /// mounted only if the worker plugin offers certificates.
    CREATE_CERTIFICATE = CreateCertificate {
        method: Method::Post,
        template: "/v1/certificates",
        permission: None,
        request: Some("CreateCertificateRequest"),
        response: "Certificate",
        cookie: false,
        rate_limited: false,
    };

    /// `POST /v1/certificates/{certificate}:revoke`: revokes one certificate. Requires
    /// `principals.revoke`.
    REVOKE_CERTIFICATE = RevokeCertificate {
        method: Method::Post,
        template: "/v1/certificates/{certificate}:revoke",
        collection: name::Certificates,
        permission: Some(Permission::PrincipalsRevoke),
        request: None,
        response: "Certificate",
        cookie: false,
        rate_limited: false,
    };

    /// `GET /v1/principals`: lists principals. Requires `principals.read`; may be identified
    /// by the session cookie.
    LIST_PRINCIPALS = ListPrincipals {
        method: Method::Get,
        template: "/v1/principals",
        permission: Some(Permission::PrincipalsRead),
        request: None,
        response: "ListPrincipalsResponse",
        cookie: true,
        rate_limited: false,
    };

    /// `POST /v1/principals/{principal}:revoke`: revokes a principal and its certificates. Requires
    /// `principals.revoke`.
    REVOKE_PRINCIPAL = RevokePrincipal {
        method: Method::Post,
        template: "/v1/principals/{principal}:revoke",
        collection: name::Principals,
        permission: Some(Permission::PrincipalsRevoke),
        request: None,
        response: "RevokePrincipalResponse",
        cookie: false,
        rate_limited: false,
    };

    /// `POST /v1/principals/{principal}:restore`: puts a revoked principal back into service.
    /// Requires `principals.revoke`.
    RESTORE_PRINCIPAL = RestorePrincipal {
        method: Method::Post,
        template: "/v1/principals/{principal}:restore",
        collection: name::Principals,
        permission: Some(Permission::PrincipalsRevoke),
        request: None,
        response: "Principal",
        cookie: false,
        rate_limited: false,
    };

    /// `GET /v1/roles`: lists roles. Requires `principals.read`; may be identified by the session
    /// cookie.
    LIST_ROLES = ListRoles {
        method: Method::Get,
        template: "/v1/roles",
        permission: Some(Permission::PrincipalsRead),
        request: None,
        response: "ListRolesResponse",
        cookie: true,
        rate_limited: false,
    };

    /// `GET /v1/events`: lists scaling events, newest first. Requires `events.read`; may be
    /// identified by the session cookie.
    LIST_EVENTS = ListEvents {
        method: Method::Get,
        template: "/v1/events",
        permission: Some(Permission::EventsRead),
        request: None,
        response: "ListEventsResponse",
        cookie: true,
        rate_limited: false,
    };

    /// `GET /v1/state`: the current state. Requires `state.read`; may be identified by the session
    /// cookie.
    GET_STATE = GetState {
        method: Method::Get,
        template: "/v1/state",
        permission: Some(Permission::StateRead),
        request: None,
        response: "State",
        cookie: true,
        rate_limited: false,
    };

    /// `GET /v1/state:watch`: a server-sent stream of state frames. Requires `state.read`; may be
    /// identified by the session cookie.
    WATCH_STATE = WatchState {
        method: Method::Get,
        template: "/v1/state:watch",
        permission: Some(Permission::StateRead),
        request: None,
        response: "Frame",
        cookie: true,
        rate_limited: false,
    };

    /// `GET /v1/session`: the caller's session. Open, anonymous callers allowed; may be
    /// identified by the session cookie.
    GET_SESSION = GetSession {
        method: Method::Get,
        template: "/v1/session",
        permission: None,
        request: None,
        response: "Session",
        cookie: true,
        rate_limited: false,
    };
}

/// Finds the first route in [`ROUTES`] that matches `method` and `path`, with the raw
/// placeholder segment if the route has one.
///
/// Returns `None` if no route matches; the server answers `404 ROUTE_NOT_FOUND`.
#[must_use]
pub fn lookup(method: Method, path: &str) -> Option<(&'static Route, Option<&str>)> {
    ROUTES
        .iter()
        .find_map(|route| route.matches(method, path).map(|raw| (route, raw)))
}

/// Templates of the `GET` routes that accept the session cookie, in [`ROUTES`] order.
pub fn cookie_paths() -> impl Iterator<Item = &'static str> {
    ROUTES
        .iter()
        .filter(|route| route.cookie && route.method == Method::Get)
        .map(|route| route.template)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ui_routes_match_only_server_owned_paths() {
        for route in UiRoute::ALL {
            assert_eq!(UiRoute::of(route.path()), Some(route));
        }
        for path in [ERROR_PATH, "/ui", "/ui/login/x"] {
            assert_eq!(UiRoute::of(path), None);
        }
    }

    #[test]
    fn templates_are_unique_per_method() {
        for (index, route) in ROUTES.iter().enumerate() {
            for other in &ROUTES[index + 1..] {
                assert!(
                    route.method != other.method || route.template != other.template,
                    "duplicate route {} {}",
                    route.method.as_str(),
                    route.template
                );
            }
        }
    }

    #[test]
    fn method_tokens_match_the_request_line() {
        assert_eq!(Method::Get.as_str(), "GET");
        assert_eq!(Method::Post.as_str(), "POST");
    }

    #[test]
    fn method_tokens_round_trip_through_parse() {
        for method in Method::ALL {
            assert_eq!(method.as_str().parse::<Method>().ok(), Some(method));
        }
    }

    #[test]
    fn parse_rejects_anything_but_the_exact_tokens() {
        assert_eq!("get".parse::<Method>().ok(), None);
        assert_eq!("PUT".parse::<Method>().ok(), None);
        assert_eq!("".parse::<Method>().ok(), None);
    }

    #[test]
    fn route_ids_are_distinct() {
        for (index, route) in ROUTES.iter().enumerate() {
            for other in &ROUTES[index + 1..] {
                assert_ne!(route.id, other.id, "duplicate route id {:?}", route.id);
            }
        }
    }

    #[test]
    fn route_constants_carry_their_ids() {
        assert_eq!(GET_SERVER.id, RouteId::GetServer);
        assert_eq!(GET_STATE.id, RouteId::GetState);
        assert_eq!(WATCH_STATE.id, RouteId::WatchState);
        assert_eq!(GET_SESSION.id, RouteId::GetSession);
    }

    #[test]
    fn state_lookup_yields_the_get_state_id() {
        let (route, raw) = lookup(Method::Get, "/v1/state").expect("state matches");
        assert_eq!(route.id, RouteId::GetState);
        assert_eq!(raw, None);
    }

    #[test]
    fn watch_is_not_confused_with_state() {
        let (route, raw) = lookup(Method::Get, "/v1/state:watch").expect("watch matches");
        assert_eq!(*route, WATCH_STATE);
        assert_eq!(raw, None);

        let (route, raw) = lookup(Method::Get, "/v1/state").expect("state matches");
        assert_eq!(*route, GET_STATE);
        assert_eq!(raw, None);
    }

    #[test]
    fn principal_verbs_yield_the_raw_id() {
        let (route, raw) = lookup(
            Method::Post,
            "/v1/principals/google%3Aops%40example.org:revoke",
        )
        .expect("revoke matches");
        assert_eq!(route.id, REVOKE_PRINCIPAL.id);
        assert_eq!(raw, Some("google%3Aops%40example.org"));

        let (route, raw) = lookup(
            Method::Post,
            "/v1/principals/google%3Aops%40example.org:restore",
        )
        .expect("restore matches");
        assert_eq!(route.id, RESTORE_PRINCIPAL.id);
        assert_eq!(raw, Some("google%3Aops%40example.org"));
    }

    #[test]
    fn certificate_verb_yields_the_raw_id() {
        let (route, raw) =
            lookup(Method::Post, "/v1/certificates/3fa9:revoke").expect("revoke matches");
        assert_eq!(route.id, REVOKE_CERTIFICATE.id);
        assert_eq!(raw, Some("3fa9"));
    }

    #[test]
    fn mismatched_method_or_path_does_not_match() {
        assert!(lookup(Method::Post, "/v1/state").is_none());
        assert!(lookup(Method::Get, "/v1/session:authenticate").is_none());
        assert!(lookup(Method::Get, "/v1/nope").is_none());
        assert!(lookup(Method::Get, "/v1/principals/x").is_none());
        assert!(lookup(Method::Post, "/v1/principals/:revoke").is_none());
        assert!(lookup(Method::Post, "/v1/principals/a/b:revoke").is_none());
        assert!(lookup(Method::Post, "/v1/principals/a:b:revoke").is_none());
    }

    #[test]
    fn path_encodes_the_id() {
        let identity = "google:ops@example.org".parse().expect("valid identity");
        assert_eq!(
            REVOKE_PRINCIPAL.path(&identity),
            "/v1/principals/google%3Aops%40example.org:revoke"
        );
    }

    #[test]
    fn placeholder_free_routes_match_their_own_path() {
        for route in ROUTES.iter().filter(|route| !route.template.contains('{')) {
            assert_eq!(
                route.matches(route.method, route.template),
                Some(None),
                "{} {}",
                route.method.as_str(),
                route.template
            );
        }
    }

    #[test]
    fn cookie_paths_are_the_cookie_gets() {
        let mut paths: Vec<&str> = cookie_paths().collect();
        paths.sort_unstable();
        let mut expected = [
            "/v1/session",
            "/v1/state",
            "/v1/state:watch",
            "/v1/principals",
            "/v1/roles",
            "/v1/events",
        ];
        expected.sort_unstable();
        assert_eq!(paths, expected);
    }

    #[test]
    fn only_posts_carry_request_bodies() {
        for route in &ROUTES {
            if route.request.is_some() {
                assert_eq!(route.method, Method::Post, "{}", route.template);
            }
            if route.method == Method::Get {
                assert!(route.request.is_none(), "{}", route.template);
            }
        }
    }

    #[test]
    fn permissions_are_the_documented_names() {
        let table: Vec<(&str, Option<&str>)> = ROUTES
            .iter()
            .map(|route| (route.template, route.permission.map(Permission::as_str)))
            .collect();
        assert_eq!(
            table,
            [
                ("/v1/server", None),
                ("/v1/session:authenticate", None),
                ("/v1/certificates", None),
                (
                    "/v1/certificates/{certificate}:revoke",
                    Some("principals.revoke")
                ),
                ("/v1/principals", Some("principals.read")),
                (
                    "/v1/principals/{principal}:revoke",
                    Some("principals.revoke")
                ),
                (
                    "/v1/principals/{principal}:restore",
                    Some("principals.revoke")
                ),
                ("/v1/roles", Some("principals.read")),
                ("/v1/events", Some("events.read")),
                ("/v1/state", Some("state.read")),
                ("/v1/state:watch", Some("state.read")),
                ("/v1/session", None),
            ]
        );
    }

    #[test]
    fn browser_flow_paths_live_under_ui() {
        for path in [LOGIN_PATH, CALLBACK_PATH, LOGOUT_PATH, ERROR_PATH] {
            assert!(path.starts_with("/ui/"), "{path}");
            assert!(lookup(Method::Get, path).is_none(), "{path}");
        }
    }

    #[test]
    fn placeholders_only_appear_on_principal_and_certificate_routes() {
        for route in ROUTES.iter().filter(|route| route.template.contains('{')) {
            assert!(
                route.template.starts_with("/v1/principals/")
                    || route.template.starts_with("/v1/certificates/"),
                "{}",
                route.template
            );
        }
    }
}
