//! The schemes of `gang server`'s listener URLs, and the URL of a bound listener.

use std::net::SocketAddr;

use gang_types::Url;

use crate::server_config::ServerTransport;

impl ServerTransport {
    /// The scheme of the public listener's URL: `https` under TLS, `http` under plaintext.
    #[must_use]
    pub const fn public_scheme(self) -> &'static str {
        match self {
            Self::Tls => "https",
            Self::Plaintext => "http",
        }
    }

    /// The scheme of the worker listener's URL: `grpcs` under TLS, `grpc` under plaintext.
    #[must_use]
    pub const fn worker_scheme(self) -> &'static str {
        match self {
            Self::Tls => "grpcs",
            Self::Plaintext => "grpc",
        }
    }

    /// The URL clients reach the public listener bound to `bound` at.
    ///
    /// # Errors
    ///
    /// [`ListenerUrlError`] when the public scheme does not form a valid URL with `bound`.
    pub fn public_url(self, bound: SocketAddr) -> Result<Url, ListenerUrlError> {
        listener_url(self.public_scheme(), bound)
    }

    /// The URL workers reach the worker listener bound to `bound` at.
    ///
    /// # Errors
    ///
    /// [`ListenerUrlError`] when the worker scheme does not form a valid URL with `bound`.
    pub fn worker_url(self, bound: SocketAddr) -> Result<Url, ListenerUrlError> {
        listener_url(self.worker_scheme(), bound)
    }
}

/// A listener URL that cannot be formed from its scheme and bound address.
#[derive(Debug, thiserror::Error)]
#[error("listener url `{0}` is not a valid url")]
pub struct ListenerUrlError(pub String);

/// The URL of a listener bound to `bound`, in the form configured endpoints take:
/// `scheme://ip:port/`. An IPv6 address is bracketed.
///
/// # Errors
///
/// [`ListenerUrlError`] when `scheme` is not a valid URL scheme.
fn listener_url(scheme: &'static str, bound: SocketAddr) -> Result<Url, ListenerUrlError> {
    let text = format!("{scheme}://{bound}/");
    Url::parse(&text).map_err(|_| ListenerUrlError(text))
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use crate::server_config::ServerTransport;

    #[test]
    fn transports_name_their_public_and_worker_schemes() {
        assert_eq!(ServerTransport::Tls.public_scheme(), "https");
        assert_eq!(ServerTransport::Tls.worker_scheme(), "grpcs");
        assert_eq!(ServerTransport::Plaintext.public_scheme(), "http");
        assert_eq!(ServerTransport::Plaintext.worker_scheme(), "grpc");
    }

    #[test]
    fn ipv4_listener_url_has_scheme_address_and_trailing_slash() {
        let bound: SocketAddr = "127.0.0.1:41234".parse().expect("address");

        assert_eq!(
            ServerTransport::Tls
                .public_url(bound)
                .expect("url")
                .as_str(),
            "https://127.0.0.1:41234/"
        );
        assert_eq!(
            ServerTransport::Tls
                .worker_url(bound)
                .expect("url")
                .as_str(),
            "grpcs://127.0.0.1:41234/"
        );
    }

    #[test]
    fn ipv6_listener_url_brackets_the_address() {
        let bound: SocketAddr = "[::1]:41235".parse().expect("address");

        let url = ServerTransport::Plaintext.worker_url(bound).expect("url");

        assert_eq!(url.as_str(), "grpc://[::1]:41235/");
        assert_eq!(url.port(), Some(41235));
        assert_eq!(url.host_str(), Some("[::1]"));
    }
}
