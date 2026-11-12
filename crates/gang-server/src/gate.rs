//! The authentication gate of one listener. It asks the listener's plugin who the caller is,
//! refuses revoked principals, records what it sees, and tells the services behind it who the
//! caller is. On the public listener's open paths it also records the client address that the
//! trusted proxies determine. Outside the open paths the gate stores no caller, because it admits
//! authenticated requests only.

use std::sync::Arc;

use gang_spi::{
    Authenticated, AuthenticationPort, Evidence, Peer, PortError, Rejection, RequestGate,
};
use gang_wire::routes::cookie_paths;
use http::header::AUTHORIZATION;
use http::request::Parts;
use http::{Extensions, Method};

use crate::client_address::{ClientAddress, TrustedProxies};
use crate::composite::{is_ui_path, under};
use crate::cookies::cookie_value;
use crate::core::Core;
use crate::deployment::Stack;

/// The caller of one request on an open path, stored in the request's extensions when the gate
/// has an outcome to report: who the request authenticated as, or why the gate refused its
/// credentials. A request that carried no credential the listener's plugin recognises, or whose
/// only credential was a session cookie the plugin refused, stores no `Caller`: a missing
/// `Caller` is an anonymous request. Outside the open paths the gate stores no `Caller`, because
/// it admits authenticated requests only. The Gang API decides what each outcome allows, and
/// renders a refusal itself.
#[derive(Debug, Clone)]
pub(crate) enum Caller {
    /// A request without a credential the listener recognises.
    Anonymous,
    /// A request authenticated as the caller.
    Authenticated(Authenticated),
    /// A request whose credentials the gate refused, shared because extensions are cloned.
    Refused(Arc<PortError>),
}

/// The caller of a request that has none recorded.
static ANONYMOUS: Caller = Caller::Anonymous;

impl Caller {
    /// The caller recorded in `extensions`; a missing caller is anonymous.
    #[must_use]
    pub(crate) fn of(extensions: &Extensions) -> &Self {
        extensions.get().unwrap_or(&ANONYMOUS)
    }

    /// The authenticated caller; `None` for an anonymous or refused one.
    #[must_use]
    pub(crate) fn as_authenticated(&self) -> Option<&Authenticated> {
        match self {
            Self::Authenticated(authenticated) => Some(authenticated),
            Self::Anonymous | Self::Refused(_) => None,
        }
    }
}

/// What only the public listener's gate has: it reads the session cookie, and resolves the client
/// address of each open-path request through the proxies whose `X-Forwarded-For` it believes.
#[derive(Clone)]
struct PublicSurface {
    proxies: Arc<TrustedProxies>,
}

/// The gate of one listener: the plugin that authenticates it over the shared core. The public
/// listener's gate, which has a `public` surface, also reads the session cookie and resolves the
/// client address of each open-path request for rate limiting.
pub(crate) struct Gate<K: Stack, P: AuthenticationPort> {
    core: Arc<Core<K>>,
    plugin: Arc<P>,
    /// The public listener's surface; `None` on the worker listener.
    public: Option<PublicSurface>,
}

impl<K: Stack, P: AuthenticationPort> Clone for Gate<K, P> {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
            plugin: Arc::clone(&self.plugin),
            public: self.public.clone(),
        }
    }
}

impl<K: Stack> Gate<K, K::Clients> {
    /// The gate of the public listener, authenticated by the clients' plugin. `proxies` names the
    /// proxies whose `X-Forwarded-For` header is believed.
    #[must_use]
    pub(crate) fn public(core: Arc<Core<K>>, proxies: Arc<TrustedProxies>) -> Self {
        Self {
            plugin: Arc::clone(&core.clients),
            core,
            public: Some(PublicSurface { proxies }),
        }
    }
}

impl<K: Stack> Gate<K, K::Workers> {
    /// The gate of the worker listener, authenticated by the workers' plugin.
    #[must_use]
    pub(crate) fn worker(core: Arc<Core<K>>) -> Self {
        Self {
            plugin: Arc::clone(&core.workers),
            core,
            public: None,
        }
    }
}

impl<K: Stack, P: AuthenticationPort> Gate<K, P> {
    /// The session cookie a request may authenticate with. Only the public listener reads it, and
    /// only for a `GET` or `HEAD` of a path that accepts it.
    fn session_cookie<'h>(&self, parts: &'h Parts) -> Option<&'h str> {
        self.public.as_ref()?;
        let readable = parts.method == Method::GET || parts.method == Method::HEAD;
        if !readable || !cookie_paths().any(|path| path == parts.uri.path()) {
            return None;
        }
        cookie_value(&parts.headers, self.core.cookies.session)
    }

    /// The authenticated caller of `parts` as the listener's plugin reads it.
    async fn authenticate_caller(
        &self,
        peer: &Peer,
        parts: &Parts,
        session: Option<&str>,
    ) -> Result<Option<Authenticated>, PortError> {
        let evidence = Evidence {
            peer,
            headers: &parts.headers,
            session,
        };
        self.plugin.authenticate(&self.core.host, &evidence).await
    }
}

/// Whether the session cookie is the only credential of `parts` on an open path. A refusal
/// of such a cookie admits the request as an anonymous caller.
fn is_session_only(parts: &Parts, session: Option<&str>) -> bool {
    session.is_some()
        && is_open_path(parts.uri.path())
        && !parts.headers.contains_key(AUTHORIZATION)
}

impl<K: Stack, P: AuthenticationPort + 'static> RequestGate for Gate<K, P> {
    /// Admits the request, or refuses it with the plugin's error. On the public listener's open
    /// paths nothing is refused here: a refusal or an authenticated caller is recorded in the
    /// request's [`Caller`] for the services behind the gate to decide, and an anonymous request
    /// stores nothing.
    async fn admit(&self, peer: &Peer, parts: &mut Parts) -> Result<(), PortError> {
        let open_public = self
            .public
            .as_ref()
            .filter(|_| is_open_path(parts.uri.path()));
        let open = open_public.is_some();
        if let Some(public) = open_public
            && let Some(client) = public.proxies.client(peer.address, &parts.headers)
        {
            parts.extensions.insert(ClientAddress(client));
        }
        let session = self.session_cookie(parts);
        let authenticated = match self.authenticate_caller(peer, parts, session).await {
            Ok(authenticated) => authenticated,
            Err(PortError::Rejected {
                reason: Rejection::InvalidProof,
                ..
            }) if is_session_only(parts, session) => None,
            Err(error) => return refuse(parts, open, error),
        };
        let Some(authenticated) = authenticated else {
            return if open {
                Ok(())
            } else {
                Err(PortError::refused(Rejection::InvalidProof))
            };
        };
        if self
            .core
            .host
            .revoked_principals
            .contains(&authenticated.identity)
        {
            return refuse(parts, open, PortError::refused(Rejection::PrincipalRevoked));
        }
        self.core.host.observe(&authenticated.identity).await;
        if open {
            parts
                .extensions
                .insert(Caller::Authenticated(authenticated));
        }
        Ok(())
    }
}

/// Records `error` as the refusal of an open path's request, or returns it as the gate's error.
fn refuse(parts: &mut Parts, open: bool, error: PortError) -> Result<(), PortError> {
    if open {
        parts.extensions.insert(Caller::Refused(Arc::new(error)));
        Ok(())
    } else {
        Err(error)
    }
}

/// Whether `path` is under the paths the services behind the gate decide for: `/v1` and `/ui`
/// themselves and anything below them.
pub(crate) fn is_open_path(path: &str) -> bool {
    under(path, "/v1") || is_ui_path(path)
}

#[cfg(test)]
mod tests {
    use super::is_open_path;

    #[test]
    fn open_paths_are_v1_and_ui_and_below() {
        assert!(is_open_path("/v1"));
        assert!(is_open_path("/v1/x"));
        assert!(is_open_path("/ui"));
        assert!(is_open_path("/ui/session"));
        assert!(is_open_path("/ui/"));
        assert!(!is_open_path("/v10"));
        assert!(!is_open_path("/v1x"));
        assert!(!is_open_path("/uix"));
        assert!(!is_open_path("/"));
        assert!(!is_open_path(
            "/google.devtools.remoteexecution.v2.Execution/Execute"
        ));
    }
}
