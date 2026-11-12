//! Renders the configuration copy a local server reads: pinned under TLS, or plaintext with
//! `--insecure`.

use std::net::SocketAddr;

use gang_config::{Document, ServerTransport};
use gang_types::{CaPin, SchedulerSettings, Url, is_loopback_url};

use crate::errors::LocalCommandError;
use crate::plugins::{Plugins, SchedulerConfig, anonymous_authentication, worker_endpoint_key};

/// Returns the configuration copy of `document` with the server's CA pinned to `pin`, keeping
/// every other setting.
///
/// # Errors
///
/// [`LocalCommandError::Render`] when the configuration cannot be written as TOML.
pub(crate) fn with_ca_pin(
    document: Document<Plugins>,
    pin: CaPin,
) -> Result<String, LocalCommandError> {
    document
        .pinned(pin)
        .to_toml()
        .map_err(LocalCommandError::Render)
}

/// Returns the configuration copy of `document` for `gang local up --insecure`: a plaintext
/// server at `endpoint` with workers at `worker_endpoint`, no CA pin, unauthenticated clients and
/// workers, and one `administrator` role granting every permission to the anonymous
/// `none:anonymous` caller. Any existing roles are replaced. An absent endpoint stays absent, so
/// the server derives it from its bound address. Returns the rendered configuration and the
/// rewritten public and worker endpoints.
///
/// # Errors
///
/// Returns an error if an endpoint cannot be rewritten or the configuration cannot be rendered.
pub(crate) fn with_plaintext(
    mut document: Document<Plugins>,
    endpoint: Option<&Url>,
    worker_endpoint: Option<&Url>,
) -> Result<(String, Option<Url>, Option<Url>), LocalCommandError> {
    let transport = ServerTransport::Plaintext;
    let worker_endpoint = worker_endpoint
        .map(|url| with_scheme(url, transport.worker_scheme()))
        .transpose()?;
    document
        .scheduler
        .set_worker_endpoint(worker_endpoint.clone());
    let authentication = anonymous_authentication();
    let endpoint = endpoint
        .map(|url| with_scheme(url, transport.public_scheme()))
        .transpose()?;
    let rendered = document
        .insecure(endpoint.clone(), authentication)
        .to_toml()
        .map_err(LocalCommandError::Render)?;
    Ok((rendered, endpoint, worker_endpoint))
}

/// Refuses `gang local up --insecure` unless `listen`, `worker_listen`, and each configured endpoint
/// are loopback: an unauthenticated plaintext server must not be reachable from another host. An
/// omitted endpoint is derived from the bound loopback address, so it is accepted.
///
/// # Errors
///
/// [`LocalCommandError::InsecureNeedsLoopback`] naming the first address that is not loopback.
pub(crate) fn require_insecure_loopback(
    listen: SocketAddr,
    worker_listen: SocketAddr,
    endpoint: Option<&Url>,
    scheduler: &SchedulerConfig,
) -> Result<(), LocalCommandError> {
    if !listen.ip().is_loopback() {
        return Err(not_loopback("listen", &listen));
    }
    if !worker_listen.ip().is_loopback() {
        return Err(not_loopback("worker_listen", &worker_listen));
    }
    if let Some(url) = endpoint.filter(|url| !is_loopback_url(url)) {
        return Err(not_loopback("endpoint", url));
    }
    if let Some(url) = scheduler
        .worker_endpoint()
        .filter(|url| !is_loopback_url(url))
    {
        return Err(not_loopback(worker_endpoint_key(scheduler), url));
    }
    Ok(())
}

/// The refusal for an address that is not loopback: `what` names its configuration key.
fn not_loopback(what: impl Into<String>, value: &impl ToString) -> LocalCommandError {
    LocalCommandError::InsecureNeedsLoopback {
        what: what.into(),
        value: value.to_string(),
    }
}

/// `url` with its scheme replaced by `scheme`, preserving its effective port and path.
///
/// # Errors
///
/// Returns an error if the endpoint has no host or the rewritten URL cannot be parsed.
pub(crate) fn with_scheme(url: &Url, scheme: &'static str) -> Result<Url, LocalCommandError> {
    let host = url
        .host_str()
        .ok_or_else(|| LocalCommandError::EndpointWithoutHost {
            endpoint: url.to_string(),
        })?;
    let suffix = &url[url::Position::BeforePath..];
    let rewritten = match default_port(url) {
        Some(port) => format!("{scheme}://{host}:{port}{suffix}"),
        None => format!("{scheme}://{host}{suffix}"),
    };
    Url::parse(&rewritten).map_err(|source| LocalCommandError::EndpointScheme {
        endpoint: url.to_string(),
        scheme,
        source,
    })
}

/// gRPC schemes are non-special to the URL standard; grpcs defaults to 443 like https.
fn default_port(url: &Url) -> Option<u16> {
    url.port_or_known_default()
        .or_else(|| (url.scheme() == "grpcs").then_some(443))
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::path::Path;

    use gang_config::{CaPinSetting, GangConfig, Transport};
    use gang_types::PermissionSet;

    use super::*;
    use crate::config_file::tests::VALID_LOCAL_CONFIG;
    use crate::plugins::{ClientAuthenticationConfig, WorkerAuthenticationConfig, local_state_dir};

    /// A pin with a recognizable digest for the tests.
    fn pin() -> CaPin {
        CaPin::new([0xab; 32])
    }

    /// Parses a configuration copy into the document the rendering functions take.
    fn document(text: &str) -> Document<Plugins> {
        Document::<Plugins>::parse(text).expect("test configuration parses")
    }

    /// Parses and validates a rendered configuration.
    fn validated(text: &str) -> GangConfig<Plugins> {
        text.parse().expect("rendered configuration validates")
    }

    /// The state directory of the local provisioner.
    fn state_dir(config: &GangConfig<Plugins>) -> &Path {
        local_state_dir(&config.provisioner)
            .expect("the valid local configuration uses the local provisioner")
    }

    #[test]
    fn pinning_sets_the_pin_and_keeps_every_other_setting() {
        let before = validated(VALID_LOCAL_CONFIG);

        let output = with_ca_pin(document(VALID_LOCAL_CONFIG), pin()).expect("pinned");
        let after = validated(&output);

        assert_eq!(after.server.ca_pin, Some(CaPinSetting::Pinned(pin())));
        assert_eq!(after.server.transport, before.server.transport);
        assert_eq!(after.listen, before.listen);
        assert_eq!(after.fleets.len(), before.fleets.len());
        assert_eq!(state_dir(&after), state_dir(&before));
    }

    #[test]
    fn pinning_replaces_an_existing_pin() {
        let stale = CaPin::new([0xcd; 32]);
        let text =
            VALID_LOCAL_CONFIG.replace("[server]\n", &format!("[server]\nca_pin = \"{stale}\"\n"));
        assert_eq!(
            validated(&text).server.ca_pin,
            Some(CaPinSetting::Pinned(stale))
        );

        let output = with_ca_pin(document(&text), pin()).expect("pinned");

        assert_eq!(
            validated(&output).server.ca_pin,
            Some(CaPinSetting::Pinned(pin()))
        );
    }

    #[test]
    fn plaintext_sets_transport_endpoints_and_the_anonymous_administrator() {
        let endpoint: Url = "https://127.0.0.1:9443".parse().expect("url");
        let worker: Url = "grpcs://127.0.0.1:9090".parse().expect("url");

        let (output, _, _) =
            with_plaintext(document(VALID_LOCAL_CONFIG), Some(&endpoint), Some(&worker))
                .expect("rendered");
        let config = validated(&output);

        assert_eq!(config.server.transport, Transport::Plaintext);
        assert_eq!(config.server.ca_pin, None);
        assert_eq!(config.endpoint.as_ref().map(Url::scheme), Some("http"));
        assert_eq!(
            config.scheduler.worker_endpoint().map(Url::scheme),
            Some("grpc")
        );
        assert!(matches!(
            config.authentication.clients,
            ClientAuthenticationConfig::None(_)
        ));
        assert!(matches!(
            config.authentication.workers,
            WorkerAuthenticationConfig::None(_)
        ));
        let [role] = config.server.roles.as_slice() else {
            panic!("the plaintext server has exactly one role");
        };
        assert_eq!(role.name.as_str(), "administrator");
        assert_eq!(role.permissions, PermissionSet::all());
        let [principal] = role.principals.as_slice() else {
            panic!("the administrator role has exactly one principal");
        };
        assert!(principal.provider.is_anonymous());
        assert_eq!(principal.principal.as_str(), "anonymous");
    }

    #[test]
    fn plaintext_rewrite_keeps_an_https_default_port() {
        let endpoint = Url::parse("https://127.0.0.1:443/").expect("url");

        let (output, _, _) =
            with_plaintext(document(VALID_LOCAL_CONFIG), Some(&endpoint), None).expect("rendered");

        assert!(output.contains("http://127.0.0.1:443/"));
    }

    #[test]
    fn grpcs_rewrite_keeps_its_port() {
        let worker = Url::parse("grpcs://127.0.0.1:9444/").expect("url");

        let (output, _, _) =
            with_plaintext(document(VALID_LOCAL_CONFIG), None, Some(&worker)).expect("rendered");

        assert!(output.contains("grpc://127.0.0.1:9444/"));
    }

    #[test]
    fn grpcs_rewrite_keeps_the_default_tls_port() {
        let worker = Url::parse("grpcs://127.0.0.1/").expect("url");

        let (output, _, _) =
            with_plaintext(document(VALID_LOCAL_CONFIG), None, Some(&worker)).expect("rendered");

        assert!(output.contains("grpc://127.0.0.1:443/"));
    }

    #[test]
    fn plaintext_leaves_omitted_endpoints_for_the_server_to_derive() {
        let (output, _, _) =
            with_plaintext(document(VALID_LOCAL_CONFIG), None, None).expect("rendered");
        let config = validated(&output);

        assert_eq!(config.endpoint, None);
        assert_eq!(config.scheduler.worker_endpoint(), None);
    }

    #[test]
    fn plaintext_refuses_an_endpoint_that_cannot_be_rewritten() {
        let endpoint: Url = "mailto:operator".parse().expect("url");

        let error = with_plaintext(document(VALID_LOCAL_CONFIG), Some(&endpoint), None)
            .expect_err("an endpoint without a host cannot be rewritten");

        let LocalCommandError::EndpointWithoutHost { endpoint: named } = error else {
            panic!("the refusal names the endpoint");
        };
        assert_eq!(named, "mailto:operator");
    }

    #[test]
    fn plaintext_replaces_an_existing_roles_key() {
        let text = VALID_LOCAL_CONFIG.replace("[server]\n", "[server]\nroles = []\n");

        let (output, _, _) = with_plaintext(document(&text), None, None).expect("rendered");

        assert_eq!(validated(&output).server.roles.len(), 1);
    }

    #[test]
    fn rendering_the_plaintext_copy_again_changes_nothing() {
        let (once, _, _) =
            with_plaintext(document(VALID_LOCAL_CONFIG), None, None).expect("rendered");

        let (twice, _, _) = with_plaintext(document(&once), None, None).expect("rendered again");

        assert_eq!(once, twice);
    }

    /// The refusal a rejected address produces: its configuration key and its value.
    fn refusal(result: Result<(), LocalCommandError>) -> (String, String) {
        match result {
            Err(LocalCommandError::InsecureNeedsLoopback { what, value }) => (what, value),
            other => panic!("expected a loopback refusal, got {other:?}"),
        }
    }

    fn loopback(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    /// The scheduler section of the valid local configuration, with the given worker endpoint.
    fn scheduler(worker_endpoint: Option<&Url>) -> SchedulerConfig {
        let mut scheduler = validated(VALID_LOCAL_CONFIG).scheduler;
        scheduler.set_worker_endpoint(worker_endpoint.cloned());
        scheduler
    }

    #[test]
    fn insecure_accepts_loopback_listeners_and_endpoints() {
        let endpoint: Url = "https://localhost:9443".parse().expect("url");
        let worker: Url = "grpcs://[::1]:9090".parse().expect("url");
        let worker_listen: SocketAddr = "[::1]:9090".parse().expect("address");

        let result = require_insecure_loopback(
            loopback(9443),
            worker_listen,
            Some(&endpoint),
            &scheduler(Some(&worker)),
        );

        assert!(result.is_ok());
    }

    #[test]
    fn insecure_accepts_omitted_endpoints_for_port_zero_listeners() {
        let result = require_insecure_loopback(loopback(0), loopback(0), None, &scheduler(None));

        assert!(result.is_ok());
    }

    #[test]
    fn insecure_refuses_a_public_listen_address() {
        let listen: SocketAddr = "0.0.0.0:9443".parse().expect("address");

        let result = require_insecure_loopback(listen, loopback(9090), None, &scheduler(None));

        assert_eq!(
            refusal(result),
            ("listen".to_owned(), "0.0.0.0:9443".to_owned())
        );
    }

    #[test]
    fn insecure_refuses_a_public_worker_listen_address() {
        let worker_listen: SocketAddr = "10.0.0.5:9090".parse().expect("address");

        let result =
            require_insecure_loopback(loopback(9443), worker_listen, None, &scheduler(None));

        assert_eq!(
            refusal(result),
            ("worker_listen".to_owned(), "10.0.0.5:9090".to_owned())
        );
    }

    #[test]
    fn insecure_refuses_a_public_endpoint() {
        let endpoint: Url = "https://gang.example.com:9443".parse().expect("url");

        let result = require_insecure_loopback(
            loopback(9443),
            loopback(9090),
            Some(&endpoint),
            &scheduler(None),
        );

        assert_eq!(
            refusal(result),
            (
                "endpoint".to_owned(),
                "https://gang.example.com:9443/".to_owned()
            )
        );
    }

    #[test]
    fn insecure_refuses_a_public_worker_endpoint() {
        let worker: Url = "grpcs://192.168.1.2:9090".parse().expect("url");

        let result = require_insecure_loopback(
            loopback(9443),
            loopback(9090),
            None,
            &scheduler(Some(&worker)),
        );

        assert_eq!(
            refusal(result),
            (
                "scheduler.nativelink.worker_endpoint".to_owned(),
                "grpcs://192.168.1.2:9090".to_owned()
            )
        );
    }

    #[test]
    fn insecure_refuses_an_endpoint_without_a_host() {
        let endpoint: Url = "mailto:gang@localhost".parse().expect("url");

        let result = require_insecure_loopback(
            loopback(9443),
            loopback(9090),
            Some(&endpoint),
            &scheduler(None),
        );

        assert_eq!(refusal(result).0, "endpoint");
    }
}
