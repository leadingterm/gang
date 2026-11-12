//! The Bazel rc rendering: the settings the server answers with, the CA certificate it carries,
//! the write-free contract on the client home, and the refusals before and after the request.

use gang_client::{BazelrcRequest, ClientError};
use gang_types::Url;

use super::support::*;

const HELPER: &str = "/usr/local/bin/gang";

#[tokio::test]
async fn renders_the_executor_cache_and_helper_with_the_server_ca() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    let server = harness.server.url();

    let rendered = harness
        .client()
        .bazelrc(&BazelrcRequest {
            server: &server,
            ca_pin: None,
            helper: HELPER,
        })
        .await
        .expect("the bazelrc settings");

    let host = server.host_str().expect("the fake server has a host");
    assert_eq!(
        rendered.text,
        format!(
            "# Written by `gang bazelrc`. Do not edit.\n\
             build --remote_executor=grpcs://exec.example:443\n\
             build --remote_cache=grpcs://cache.example:443\n\
             build --credential_helper={host}={HELPER}\n\
             build --remote_default_exec_properties=ISA=x86-64\n\
             build --tls_certificate=gang-ca.pem\n"
        )
    );
    assert_eq!(rendered.ca_certificate, Some(ca_certificate()));
}

#[tokio::test]
async fn rendering_writes_no_session_to_the_client_home() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    let server = harness.server.url();
    let session = harness.home.session_file();
    assert!(!session.exists(), "no session before the call");

    let _rendered = harness
        .client()
        .bazelrc(&BazelrcRequest {
            server: &server,
            ca_pin: None,
            helper: HELPER,
        })
        .await
        .expect("the bazelrc settings");

    assert!(!session.exists(), "the call stores no session");
}

#[tokio::test]
async fn non_http_server_url_is_unsupported_without_a_request() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    // A failing answer would surface as a server error if the client reached the server at all.
    harness
        .server
        .force_response(500, b"must not be requested".to_vec());
    let server = Url::parse("ftp://127.0.0.1:21/").expect("valid url");

    let error = harness
        .client()
        .bazelrc(&BazelrcRequest {
            server: &server,
            ca_pin: None,
            helper: HELPER,
        })
        .await
        .expect_err("the scheme is not http or https");

    assert!(
        matches!(error, ClientError::UnsupportedServerUrl { ref url } if *url == server),
        "{error}"
    );
}

#[tokio::test]
async fn server_error_status_is_a_server_error() {
    let harness = Harness::start_plain(ConsoleMode::Silent, google()).await;
    harness
        .server
        .force_response(503, b"maintenance window".to_vec());
    let server = harness.server.url();

    let error = harness
        .client()
        .bazelrc(&BazelrcRequest {
            server: &server,
            ca_pin: None,
            helper: HELPER,
        })
        .await
        .expect_err("the server answers an error status");

    assert!(
        matches!(
            error,
            ClientError::Server { status: 503, ref message } if message == "maintenance window"
        ),
        "{error}"
    );
}
