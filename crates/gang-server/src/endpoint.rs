//! The public and worker endpoint URLs, and everything derived from them: the origin, the sign-in
//! callback, the gRPC URL clients connect to, and the names the listener certificates present.

use std::fmt;
use std::net::IpAddr;

use gang_mtls::SubjectName;
use gang_types::Url;
use gang_wire::CALLBACK_PATH;

use crate::errors::ServerError;

/// The audience that reaches a server endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointRole {
    /// The endpoint reached by clients.
    Public,
    /// The endpoint reached by workers.
    Worker,
}

impl fmt::Display for EndpointRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Public => "public",
            Self::Worker => "worker",
        })
    }
}

/// The URL under which clients reach the server, with what the server derives from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicEndpoint {
    url: Url,
    origin: String,
    callback: Url,
    remote: Url,
}

impl PublicEndpoint {
    /// The public endpoint at `url`, which must use the `https` or `http` scheme.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::EndpointScheme`] when the scheme is neither `https` nor `http`.
    pub fn try_new(url: Url) -> Result<Self, ServerError> {
        let grpc = match url.scheme() {
            "https" => "grpcs",
            "http" => "grpc",
            _ => return Err(public_scheme(url)),
        };
        let rest = &url.as_str()[url.scheme().len()..];
        let (Ok(remote), Ok(callback)) = (
            Url::parse(&format!("{grpc}{rest}")),
            url.join(CALLBACK_PATH),
        ) else {
            return Err(public_scheme(url));
        };
        Ok(Self {
            origin: url.origin().ascii_serialization(),
            callback,
            remote,
            url,
        })
    }

    /// The URL itself.
    #[must_use]
    pub(crate) fn url(&self) -> &Url {
        &self.url
    }

    /// The origin of the URL, as a browser's `Origin` header serializes it.
    #[must_use]
    pub(crate) fn origin(&self) -> &str {
        &self.origin
    }

    /// The URL the identity provider redirects a sign-in to.
    #[must_use]
    pub(crate) fn callback(&self) -> &Url {
        &self.callback
    }

    /// The gRPC URL clients connect to: the same host and path under `grpcs` for `https` and
    /// `grpc` for `http`.
    #[must_use]
    pub(crate) fn remote(&self) -> &Url {
        &self.remote
    }

    /// The name the public listener's certificate presents.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::EndpointHost`] when the URL has no host.
    pub(crate) fn server_name(&self) -> Result<SubjectName, ServerError> {
        server_name(EndpointRole::Public, &self.url)
    }
}

/// The URL under which workers reach the scheduler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkerEndpoint(Url);

impl WorkerEndpoint {
    /// The worker endpoint at `url`: `grpcs` under TLS, and `grpc` or `grpcs` otherwise.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::EndpointScheme`] when the scheme is not the one the transport
    /// requires.
    pub(crate) fn try_new(url: Url, tls: bool) -> Result<Self, ServerError> {
        let expected: &'static [&'static str] = if tls { &["grpcs"] } else { &["grpc", "grpcs"] };
        if expected.contains(&url.scheme()) {
            return Ok(Self(url));
        }
        Err(ServerError::EndpointScheme {
            role: EndpointRole::Worker,
            url,
            expected,
        })
    }

    /// The URL itself.
    #[must_use]
    pub(crate) fn url(&self) -> &Url {
        &self.0
    }

    /// The name the worker listener's certificate presents.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::EndpointHost`] when the URL has no host.
    pub(crate) fn server_name(&self) -> Result<SubjectName, ServerError> {
        server_name(EndpointRole::Worker, &self.0)
    }
}

/// The scheme error of the public endpoint at `url`.
fn public_scheme(url: Url) -> ServerError {
    ServerError::EndpointScheme {
        role: EndpointRole::Public,
        url,
        expected: &["https", "http"],
    }
}

/// The name a certificate presents for the endpoint `role` at `url`: an IP address when the host
/// parses as one, otherwise a DNS name.
fn server_name(role: EndpointRole, url: &Url) -> Result<SubjectName, ServerError> {
    let host = url
        .host_str()
        .map(|host| host.trim_start_matches('[').trim_end_matches(']'))
        .filter(|host| !host.is_empty());
    let Some(host) = host else {
        return Err(ServerError::EndpointHost {
            role,
            url: url.clone(),
        });
    };
    Ok(match host.parse::<IpAddr>() {
        Ok(ip) => SubjectName::Ip(ip),
        Err(_) => SubjectName::Dns(host.to_owned()),
    })
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use gang_mtls::SubjectName;
    use gang_types::Url;

    use super::{EndpointRole, PublicEndpoint, WorkerEndpoint};
    use crate::errors::ServerError;

    fn url(text: &str) -> Url {
        Url::parse(text).expect("test url parses")
    }

    fn public(text: &str) -> PublicEndpoint {
        PublicEndpoint::try_new(url(text)).expect("test endpoint is accepted")
    }

    #[test]
    fn public_endpoint_accepts_https_and_http() {
        for text in ["https://gang.example.test", "http://gang.example.test:8080"] {
            assert_eq!(public(text).url(), &url(text), "{text}");
        }
    }

    #[test]
    fn public_endpoint_refuses_other_schemes() {
        for text in ["grpcs://gang.example.test", "ftp://gang.example.test"] {
            let result = PublicEndpoint::try_new(url(text));
            assert!(
                matches!(
                    result,
                    Err(ServerError::EndpointScheme {
                        role: EndpointRole::Public,
                        expected: &["https", "http"],
                        ..
                    })
                ),
                "{text}"
            );
        }
    }

    #[test]
    fn remote_is_grpcs_for_https_and_grpc_for_http_with_the_same_rest() {
        assert_eq!(
            public("https://gang.example.test:8443/base")
                .remote()
                .as_str(),
            "grpcs://gang.example.test:8443/base"
        );
        assert_eq!(
            public("http://gang.example.test:8080/").remote().as_str(),
            "grpc://gang.example.test:8080/"
        );
    }

    #[test]
    fn callback_is_the_callback_path_on_the_endpoint_host() {
        assert_eq!(
            public("https://gang.example.test:8443/base")
                .callback()
                .as_str(),
            "https://gang.example.test:8443/ui/callback"
        );
    }

    #[test]
    fn origin_is_the_ascii_serialization_without_a_default_port_or_path() {
        assert_eq!(
            public("https://gang.example.test:8443/base").origin(),
            "https://gang.example.test:8443"
        );
        assert_eq!(
            public("https://gang.example.test:443/").origin(),
            "https://gang.example.test"
        );
        assert_eq!(
            public("http://gang.example.test:8080").origin(),
            "http://gang.example.test:8080"
        );
    }

    #[test]
    fn server_name_is_an_ip_for_an_address_and_a_dns_name_otherwise() {
        let ip: IpAddr = "192.0.2.7".parse().expect("test address parses");
        let ipv6: IpAddr = "2001:db8::1".parse().expect("test address parses");
        assert_eq!(
            public("https://192.0.2.7:8443").server_name().ok(),
            Some(SubjectName::Ip(ip))
        );
        assert_eq!(
            public("https://[2001:db8::1]:8443").server_name().ok(),
            Some(SubjectName::Ip(ipv6))
        );
        assert_eq!(
            public("https://gang.example.test").server_name().ok(),
            Some(SubjectName::Dns("gang.example.test".to_owned()))
        );
    }

    #[test]
    fn server_name_of_a_url_without_a_host_is_refused() {
        let worker = WorkerEndpoint::try_new(url("grpcs:worker"), true)
            .expect("the scheme is the one tls requires");
        assert!(matches!(
            worker.server_name(),
            Err(ServerError::EndpointHost {
                role: EndpointRole::Worker,
                ..
            })
        ));
    }

    #[test]
    fn worker_endpoint_names_its_host() {
        let worker = WorkerEndpoint::try_new(url("grpcs://worker.example.test:8443"), true)
            .expect("grpcs is accepted under tls");
        assert_eq!(
            worker.server_name().ok(),
            Some(SubjectName::Dns("worker.example.test".to_owned()))
        );
        assert_eq!(worker.url(), &url("grpcs://worker.example.test:8443"));
    }

    #[test]
    fn tls_requires_a_grpcs_worker_endpoint() {
        for text in ["grpc://worker.example.test", "http://worker.example.test"] {
            let result = WorkerEndpoint::try_new(url(text), true);
            assert!(
                matches!(
                    result,
                    Err(ServerError::EndpointScheme {
                        role: EndpointRole::Worker,
                        expected: &["grpcs"],
                        ..
                    })
                ),
                "{text}"
            );
        }
        assert!(WorkerEndpoint::try_new(url("grpcs://worker.example.test"), true).is_ok());
    }

    #[test]
    fn plaintext_accepts_a_grpc_or_grpcs_worker_endpoint() {
        for text in ["grpc://worker.example.test", "grpcs://worker.example.test"] {
            assert!(WorkerEndpoint::try_new(url(text), false).is_ok(), "{text}");
        }
        let result = WorkerEndpoint::try_new(url("http://worker.example.test"), false);
        assert!(matches!(
            result,
            Err(ServerError::EndpointScheme {
                role: EndpointRole::Worker,
                expected: &["grpc", "grpcs"],
                ..
            })
        ));
    }

    #[test]
    fn tls_worker_error_names_its_only_accepted_scheme() {
        let error = WorkerEndpoint::try_new(url("grpc://worker.example.test"), true)
            .expect_err("grpc is not accepted under tls");
        assert_eq!(
            error.to_string(),
            "worker endpoint `grpc://worker.example.test` must use the grpcs scheme"
        );
    }

    #[test]
    fn plaintext_worker_rejects_https_with_both_accepted_schemes() {
        let error = WorkerEndpoint::try_new(url("https://worker.example.test"), false)
            .expect_err("https is not a worker scheme");
        assert!(matches!(
            &error,
            ServerError::EndpointScheme {
                role: EndpointRole::Worker,
                expected,
                ..
            } if *expected == ["grpc", "grpcs"]
        ));
        assert_eq!(
            error.to_string(),
            "worker endpoint `https://worker.example.test/` must use one of the schemes grpc, grpcs"
        );
    }
}
