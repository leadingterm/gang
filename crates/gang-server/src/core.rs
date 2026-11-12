//! What every request-serving component shares: the host, both authentication plugins and the
//! session cookies of the public endpoint.

use std::sync::Arc;

use crate::cookies::SessionCookies;
use crate::deployment::Stack;
use crate::host::Host;

/// The host, the plugins and the cookies the gates, the Gang API and the browser routes serve with.
pub(crate) struct Core<K: Stack> {
    /// The host lent to the plugins, and the owner of the ledger, the signer and the clock.
    pub(crate) host: Host<K>,
    /// The plugin that authenticates clients of the public listener.
    pub(crate) clients: Arc<K::Clients>,
    /// The plugin that authenticates workers on the worker listener.
    pub(crate) workers: Arc<K::Workers>,
    /// The names and attributes of the session cookies of the public endpoint.
    pub(crate) cookies: SessionCookies,
}

impl<K: Stack> Core<K> {
    /// The core over `host` and the two plugins; the cookies are derived from the host's public
    /// endpoint.
    #[must_use]
    pub(crate) fn new(host: Host<K>, clients: K::Clients, workers: K::Workers) -> Arc<Self> {
        let cookies = SessionCookies::for_endpoint(host.endpoint.url());
        Arc::new(Self {
            host,
            clients: Arc::new(clients),
            workers: Arc::new(workers),
            cookies,
        })
    }
}
