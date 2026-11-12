//! Behaviour of Bazel build outcomes: the process summary, success, and action exit codes.

use gang_e2e::{BuildOutcome, ProcessSummary};

#[test]
fn exit_summary_includes_code_and_output() {
    let failed = BuildOutcome::new(Some(3), "build failed".to_owned());
    assert_eq!(
        failed.exit_summary(),
        "the build exited with Some(3): build failed"
    );

    let signalled = BuildOutcome::new(None, String::new());
    assert_eq!(signalled.exit_summary(), "the build exited with None: ");
}

#[test]
fn reads_the_plural_summary_line() {
    let summary =
        ProcessSummary::parse("INFO: 5 processes: 2 internal, 1 remote cache hit, 2 remote.\n");

    assert_eq!(summary.count("internal"), 2);
    assert_eq!(summary.remote_cache_hits(), 1);
    assert_eq!(summary.remote(), 2);
}

#[test]
fn reads_the_singular_summary_line() {
    let summary = ProcessSummary::parse("INFO: 1 process: 1 remote.\n");

    assert_eq!(summary.remote(), 1);
    assert_eq!(summary.remote_cache_hits(), 0);
    assert_eq!(summary.count("internal"), 0);
}

#[test]
fn reads_the_empty_summary_line() {
    let summary = ProcessSummary::parse("INFO: 0 processes.\n");

    assert_eq!(summary, ProcessSummary::default());
}

#[test]
fn reads_runner_names_with_spaces() {
    let summary = ProcessSummary::parse(
        "INFO: 4 processes: 1 disk cache hit, 3 remote cache hit, 2 remote.\n",
    );

    assert_eq!(summary.count("disk cache hit"), 1);
    assert_eq!(summary.remote_cache_hits(), 3);
    assert_eq!(summary.remote(), 2);
}

#[test]
fn the_last_summary_line_wins() {
    let summary =
        ProcessSummary::parse("INFO: 1 process: 1 remote.\nINFO: 3 processes: 3 remote.\n");
    assert_eq!(summary.remote(), 3);

    let empty = ProcessSummary::parse("INFO: 2 processes: 2 remote.\nINFO: 0 processes.\n");
    assert_eq!(empty.remote(), 0);
}

#[test]
fn text_without_a_summary_line_has_an_empty_summary() {
    let summary = ProcessSummary::parse("ERROR: the build broke\nsome output\n");

    assert_eq!(summary, ProcessSummary::default());
    assert_eq!(summary.remote(), 0);
    assert_eq!(summary.remote_cache_hits(), 0);
}

#[test]
fn a_line_without_the_info_prefix_is_not_a_summary() {
    let summary = ProcessSummary::parse("5 processes: 2 internal, 3 remote.\n");

    assert_eq!(summary, ProcessSummary::default());
}

#[test]
fn a_successful_exit_succeeded() {
    let outcome = BuildOutcome::new(Some(0), String::new());

    assert!(outcome.succeeded());
}

#[test]
fn a_failing_or_signalled_exit_did_not_succeed() {
    assert!(!BuildOutcome::new(Some(1), String::new()).succeeded());
    assert!(!BuildOutcome::new(None, String::new()).succeeded());
}

#[test]
fn the_outcome_reads_its_process_summary() {
    let outcome = BuildOutcome::new(Some(0), "INFO: 1 process: 1 remote.\n".to_owned());

    assert_eq!(outcome.processes.remote(), 1);
}

#[test]
fn action_exit_matches_the_reported_code() {
    let outcome = BuildOutcome::new(Some(1), "ERROR: action failed (Exit 3)\n".to_owned());

    assert!(outcome.action_exit(3));
}

#[test]
fn action_exit_does_not_match_a_longer_code() {
    let outcome = BuildOutcome::new(Some(1), "ERROR: action failed (Exit 30)\n".to_owned());

    assert!(!outcome.action_exit(3));
    assert!(outcome.action_exit(30));
}

#[test]
fn action_killed_matches_bazels_killed_rendering() {
    let outcome = BuildOutcome::new(
        Some(1),
        "ERROR: Probing //:memory_hog failed: (Killed): sh failed: error executing\n".to_owned(),
    );

    assert!(outcome.action_killed());
}

#[test]
fn action_killed_matches_exit_137() {
    let outcome = BuildOutcome::new(Some(1), "ERROR: action failed (Exit 137)\n".to_owned());

    assert!(outcome.action_killed());
}

#[test]
fn action_killed_ignores_other_exit_codes() {
    for stderr in [
        "ERROR: action failed (Exit 1)\n",
        "ERROR: action failed (Exit 1370)\n",
    ] {
        let outcome = BuildOutcome::new(Some(1), stderr.to_owned());

        assert!(!outcome.action_killed(), "{stderr}");
    }
}

#[test]
fn stderr_tail_keeps_the_last_lines_in_order() {
    let outcome = BuildOutcome::new(Some(1), "a\nb\nc\nd\ne\n".to_owned());

    assert_eq!(outcome.stderr_tail(2), "d\ne");
}

#[test]
fn stderr_tail_with_fewer_lines_than_asked_returns_all() {
    let outcome = BuildOutcome::new(Some(1), "a\nb\n".to_owned());

    assert_eq!(outcome.stderr_tail(5), "a\nb");
}

/// The outcome of a build that failed with `stderr` on its standard error.
fn failed_with(stderr: &str) -> BuildOutcome {
    BuildOutcome::new(Some(34), stderr.to_owned())
}

#[test]
fn an_unavailable_grpc_status_is_not_a_tls_failure() {
    let outcome =
        failed_with("ERROR: Remote cache error: UNAVAILABLE: io exception: Connection refused\n");

    assert!(!outcome.succeeded());
    assert!(!outcome.tls_failure());
}

#[test]
fn a_missing_target_is_not_a_tls_failure() {
    let outcome = failed_with(
        "ERROR: no such target '//app:missing': target 'missing' not declared in package 'app'\n",
    );

    assert!(!outcome.tls_failure());
}

#[test]
fn a_certificate_store_info_line_is_not_a_tls_failure() {
    let outcome = failed_with("INFO: certificate store loaded from /etc/ssl/cert.pem\n");

    assert!(!outcome.tls_failure());
}

// Captured output of a real build rejected for a revoked client certificate (Bazel 8.7.0,
// grpc-java). This sample is not synthetic.
#[test]
fn a_grpc_ssl_exception_from_a_real_rejected_build_is_a_tls_failure() {
    let outcome = BuildOutcome::new(
        Some(1),
        "INFO: Invocation ID: 2b638698-e0d1-41b7-8ea7-13ba5594cd64\n\
         INFO: Analyzed 3 targets (5 packages loaded, 9 targets configured).\n\
         ERROR: /tmp/gang-e2e-Ivjp5f/workspace/BUILD.bazel:31:6: Probing //:network_requested \
         failed: Failed to query remote execution capabilities: UNAVAILABLE: ssl exception\n\
         INFO: Elapsed time: 52.572s, Critical Path: 51.78s\n\
         INFO: 4 processes: 4 internal.\n\
         ERROR: Build did NOT complete successfully\n"
            .to_owned(),
    );

    assert!(!outcome.succeeded());
    assert!(outcome.tls_failure());
}

#[test]
fn a_connection_refused_to_localhost_is_not_a_tls_failure() {
    let outcome = failed_with(
        "ERROR: /work/BUILD.bazel:9:6: Probing //:pinned failed: Failed to query remote \
         execution capabilities: finishConnect(..) failed: Connection refused: \
         localhost/[0:0:0:0:0:0:0:1]:46635\n",
    );

    assert!(!outcome.tls_failure());
}

// The stderr fixtures below are SYNTHETIC samples, modelled on the Bazel, netty and BoringSSL
// messages a rejected TLS handshake produces. They are not captured output. Before Gate 2
// sign-off they MUST be replaced by stderr captured from a real rejected build.

#[test]
fn a_netty_bad_certificate_alert_is_a_tls_failure() {
    let outcome = failed_with(
        "ERROR: Remote cache error: io.netty.handler.codec.DecoderException: \
         javax.net.ssl.SSLHandshakeException: Received fatal alert: bad_certificate\n",
    );

    assert!(outcome.tls_failure());
}

#[test]
fn each_tls_alert_and_handshake_failure_is_a_tls_failure() {
    for alert in [
        "bad_certificate",
        "certificate_required",
        "certificate_revoked",
        "unknown_ca",
        "handshake_failure",
    ] {
        let outcome = failed_with(&format!(
            "ERROR: Remote execution failed: io.netty.handler.codec.DecoderException: \
             javax.net.ssl.SSLHandshakeException: Received fatal alert: {alert}\n"
        ));

        assert!(
            outcome.tls_failure(),
            "alert {alert} should be a TLS failure"
        );
    }
}

#[test]
fn an_ssl_handshake_exception_is_a_tls_failure() {
    let outcome = BuildOutcome::new(
        Some(34),
        "ERROR: Remote execution failed: javax.net.ssl.SSLHandshakeException: \
         PKIX path building failed\n"
            .to_owned(),
    );

    assert!(outcome.tls_failure());
}

#[test]
fn a_tlsv1_alert_certificate_required_cause_is_a_tls_failure() {
    let outcome = failed_with(
        "ERROR: Remote cache error: io.netty.handler.codec.DecoderException: \
         java.io.IOException: Remote execution failed\n\
         Caused by: java.io.IOException: \
         error:10000412:SSL routines:OPENSSL_internal:TLSV1_ALERT_CERTIFICATE_REQUIRED\n",
    );

    assert!(outcome.tls_failure());
}

#[test]
fn a_sslv3_alert_certificate_revoked_cause_is_a_tls_failure() {
    let outcome = failed_with(
        "ERROR: Remote cache error: io.netty.handler.codec.DecoderException: \
         java.io.IOException: Remote execution failed\n\
         Caused by: java.io.IOException: \
         error:10000412:SSL routines:OPENSSL_internal:SSLV3_ALERT_CERTIFICATE_REVOKED\n",
    );

    assert!(outcome.tls_failure());
}

#[test]
fn a_compile_error_is_not_a_tls_failure() {
    let outcome = BuildOutcome::new(
        Some(1),
        "ERROR: /work/app/BUILD.bazel:5:10: Compiling app/main.cc failed: (Exit 1): gcc failed\n\
         app/main.cc:3:5: error: 'answer' was not declared in this scope\n"
            .to_owned(),
    );

    assert!(!outcome.succeeded());
    assert!(!outcome.tls_failure());
}
