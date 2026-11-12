//! The `gang doctor` run: asks the server, Bazel, and the credential helper what they see, and
//! turns the answers into a [`Report`] through the pure checks of [`crate::doctor_checks`].

use gang_clock::Clock;
use gang_tls::ServerTrust;
use gang_types::Url;
use gang_wire::{Authentication, Server};
use serde_json::Value;

use crate::ClientOptions;
use crate::bazel_options::{BazelProbe, EffectiveOptions};
use crate::doctor_checks::{
    Endpoint, Gang, bazel_failed, bazel_reported, clock, credential_helper, credentials,
    credentials_not_needed, credentials_unsupported, helper_for, needs_bazel, needs_helper,
    needs_server, remote_endpoint, server_failed, server_reached, tls_certificate, tls_trusted,
};
use crate::doctor_report::{Check, Finding, Report, Status};
use crate::server_probe::ServerProbe;

/// A step of a [`Doctor::diagnose_with`] run.
#[derive(Debug, Clone, Copy)]
pub enum Event<'e> {
    /// The doctor starts a check that waits on something slow: the server, Bazel, or the
    /// credential helper.
    Started(Check),
    /// A check came out; its finding is final.
    Finished(&'e Finding),
}

/// The rows whose inputs are Bazel's effective options, in report order.
const BAZEL_ROWS: [Check; 4] = [
    Check::RemoteExecutor,
    Check::RemoteCache,
    Check::TlsCertificate,
    Check::CredentialHelper,
];

/// Diagnoses one setup against a Gang server: the server that answers, the Bazel options that
/// point at it, the credential helper, the trust the server was verified with, and the clock.
pub struct Doctor<'a, B, S, K> {
    bazel: &'a B,
    probe: &'a S,
    clock: &'a K,
    server: &'a Url,
    trust: ServerTrust<'a>,
    options: &'a ClientOptions,
}

impl<'a, B: BazelProbe, S: ServerProbe, K: Clock> Doctor<'a, B, S, K> {
    /// Creates a doctor for `server` that asks `bazel` and `probe`, reads time from `clock`, and
    /// reports the trust that verified the server.
    pub fn new(
        bazel: &'a B,
        probe: &'a S,
        clock: &'a K,
        server: &'a Url,
        trust: ServerTrust<'a>,
        options: &'a ClientOptions,
    ) -> Self {
        Self {
            bazel,
            probe,
            clock,
            server,
            trust,
            options,
        }
    }

    /// Runs every check and returns the report. A check whose input failed is reported as
    /// skipped, and the other checks still run.
    pub async fn diagnose(&self) -> Report {
        self.diagnose_with(&mut |_: Event<'_>| {}).await
    }

    /// Runs every check like [`Doctor::diagnose`], telling `observe` about each step as it
    /// happens: a check that is about to wait on something slow is announced with
    /// [`Event::Started`], and every finding is handed over with [`Event::Finished`] as soon as
    /// it is known. The findings arrive exactly once each, in the order of [`Report::findings`].
    pub async fn diagnose_with(&self, observe: &mut impl FnMut(Event<'_>)) -> Report {
        let mut findings = Vec::new();
        observe(Event::Started(Check::Server));
        let (info, clock) = self.server_rows(&mut findings, observe).await;
        observe(Event::Started(Check::Bazel));
        let options = self.bazel_rows(info.as_ref(), &mut findings, observe).await;
        let credentials = self
            .credential_row(info.as_ref(), options.as_ref(), observe)
            .await;
        // The clock reading is taken with the server's answer, but its row closes the report.
        for finding in [credentials, clock] {
            push(&mut findings, observe, finding);
        }
        Report::new(findings)
    }

    /// Reads the server and pushes its `server` and `tls` rows; returns its settings when it
    /// answered, and the `clock` row, which the caller reports last.
    async fn server_rows(
        &self,
        findings: &mut Vec<Finding>,
        observe: &mut impl FnMut(Event<'_>),
    ) -> (Option<Server<Value>>, Finding) {
        let sent_at = self.clock.wall();
        let read = self.probe.read().await;
        let received_at = self.clock.wall();
        match read {
            Ok(reading) => {
                let gang = Gang {
                    server: self.server,
                    info: &reading.info,
                };
                push(findings, observe, server_reached(&gang));
                push(findings, observe, tls_trusted(self.server, self.trust));
                let skew = clock(
                    reading.server_time,
                    sent_at,
                    received_at,
                    self.options.max_clock_skew,
                );
                (Some(reading.info), skew)
            }
            Err(fault) => {
                let (server, tls) = server_failed(self.server, &fault, self.trust);
                push(findings, observe, server);
                push(findings, observe, tls);
                (None, needs_server(Check::Clock))
            }
        }
    }

    /// Asks Bazel for its effective options and pushes the Bazel rows. Returns the options, or
    /// `None` when Bazel could not report them.
    async fn bazel_rows(
        &self,
        info: Option<&Server<Value>>,
        findings: &mut Vec<Finding>,
        observe: &mut impl FnMut(Event<'_>),
    ) -> Option<EffectiveOptions> {
        let options = match self.bazel.effective_options().await {
            Ok(options) => options,
            Err(error) => {
                push(findings, observe, bazel_failed(&error));
                for check in BAZEL_ROWS {
                    let skipped = match info {
                        Some(_) => needs_bazel(check),
                        None => needs_server(check),
                    };
                    push(findings, observe, skipped);
                }
                return None;
            }
        };
        push(findings, observe, bazel_reported());
        match info {
            Some(info) => {
                let gang = Gang {
                    server: self.server,
                    info,
                };
                let executor = remote_endpoint(&gang, &options, Endpoint::Executor, false);
                let executor_is_gang = executor.status == Status::Ok;
                let cache = remote_endpoint(&gang, &options, Endpoint::Cache, executor_is_gang);
                for finding in [
                    executor,
                    cache,
                    tls_certificate(&gang, &options),
                    credential_helper(&gang, &options),
                ] {
                    push(findings, observe, finding);
                }
            }
            None => {
                for check in BAZEL_ROWS {
                    push(findings, observe, needs_server(check));
                }
            }
        }
        Some(options)
    }

    /// Runs the credential helper that Bazel runs for the cache host and returns the
    /// credentials row. Announces [`Check::Credentials`] only when a helper really runs.
    async fn credential_row(
        &self,
        info: Option<&Server<Value>>,
        options: Option<&EffectiveOptions>,
        observe: &mut impl FnMut(Event<'_>),
    ) -> Finding {
        let Some(info) = info else {
            return needs_server(Check::Credentials);
        };
        match &info.authentication {
            Authentication::Token(_) => {}
            Authentication::None(_) => return credentials_not_needed(),
            Authentication::Unrecognized(_) => return credentials_unsupported(),
        }
        let Some(options) = options else {
            return needs_bazel(Check::Credentials);
        };
        let gang = Gang {
            server: self.server,
            info,
        };
        let Some(helper) = helper_for(&gang, options) else {
            return needs_helper();
        };
        observe(Event::Started(Check::Credentials));
        let outcome = self.probe.credential(helper, &info.remote_cache).await;
        credentials(&gang, helper, &outcome)
    }
}

/// Hands `finding` to `observe` and keeps it for the report.
fn push(findings: &mut Vec<Finding>, observe: &mut impl FnMut(Event<'_>), finding: Finding) {
    observe(Event::Finished(&finding));
    findings.push(finding);
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::path::PathBuf;
    use std::time::Duration;

    use gang_clock::{TestClock, Timestamp};
    use gang_types::CaPin;
    use parking_lot::Mutex;
    use serde_json::json;

    use super::*;
    use crate::bazel_options::{
        CREDENTIAL_HELPER, EffectiveOptions, OptionSource, REMOTE_CACHE, REMOTE_EXECUTOR, Setting,
    };
    use crate::errors::BazelError;
    use crate::server_probe::{CredentialFault, ServerFault, ServerReading};

    const SERVER: &str = "https://gang.example.com/";
    const EXECUTOR: &str = "grpcs://exec.example.com:443";
    const CACHE: &str = "grpcs://cache.example.com:443";
    const NOW_MILLIS: u64 = 1_800_000_000_000;

    /// Bazel that reports `options`, or fails to start when there are none.
    struct FakeBazel {
        options: Option<EffectiveOptions>,
    }

    impl BazelProbe for FakeBazel {
        async fn effective_options(&self) -> Result<EffectiveOptions, BazelError> {
            self.options.clone().ok_or_else(|| BazelError::Spawn {
                program: "bazel".to_owned(),
                source: io::Error::new(io::ErrorKind::NotFound, "no such file or directory"),
            })
        }
    }

    /// Server that answers `read` with the settings and time, or fails with the fault, and
    /// whose credential helper answers `credential`. Records the helpers it was asked to run.
    struct FakeServer {
        read: Result<(Server<Value>, Option<Timestamp>), ServerFault>,
        credential: Result<(), CredentialFault>,
        calls: Mutex<Vec<String>>,
    }

    impl FakeServer {
        /// The helpers the credential probe was asked to run, in call order.
        fn calls(&self) -> Vec<String> {
            self.calls.lock().clone()
        }
    }

    impl ServerProbe for FakeServer {
        async fn read(&self) -> Result<ServerReading, ServerFault> {
            match &self.read {
                Ok((info, server_time)) => Ok(ServerReading {
                    info: info.clone(),
                    server_time: *server_time,
                }),
                Err(fault) => Err(fault.clone()),
            }
        }

        async fn credential(&self, helper: &str, _uri: &Url) -> Result<(), CredentialFault> {
            self.calls.lock().push(helper.to_owned());
            self.credential.clone()
        }
    }

    fn now() -> Timestamp {
        Timestamp::from_unix_millis(NOW_MILLIS)
    }

    fn server() -> Url {
        Url::parse(SERVER).expect("the url parses")
    }

    fn token() -> Value {
        json!({"token": {"login": {}}})
    }

    fn info(authentication: &Value) -> Server<Value> {
        serde_json::from_value(json!({
            "name": "server",
            "remoteExecutor": EXECUTOR,
            "remoteCache": "grpcs://cache.example.com:443/",
            "defaultIsa": "x86-64",
            "authentication": authentication,
        }))
        .expect("the server parses")
    }

    fn setting(value: &str, source: OptionSource) -> Setting {
        Setting {
            value: value.to_owned(),
            source,
        }
    }

    /// Effective options that point Bazel at the server, with `helper` as the credential helper.
    fn options_with_helper(helper: &str) -> EffectiveOptions {
        EffectiveOptions::new(vec![
            (
                REMOTE_EXECUTOR.to_owned(),
                setting(EXECUTOR, OptionSource::Unknown),
            ),
            (
                REMOTE_CACHE.to_owned(),
                setting(CACHE, OptionSource::Unknown),
            ),
            (
                CREDENTIAL_HELPER.to_owned(),
                setting(helper, OptionSource::Unknown),
            ),
        ])
    }

    fn healthy_options() -> EffectiveOptions {
        options_with_helper("gang-helper")
    }

    fn healthy_server(server_time: Option<Timestamp>) -> FakeServer {
        FakeServer {
            read: Ok((info(&token()), server_time)),
            credential: Ok(()),
            calls: Mutex::default(),
        }
    }

    async fn run(
        bazel: &FakeBazel,
        probe: &FakeServer,
        clock: &TestClock,
        options: &ClientOptions,
    ) -> Report {
        diagnose_with_trust(bazel, probe, clock, ServerTrust::System, options).await
    }

    async fn diagnose_with_trust(
        bazel: &FakeBazel,
        probe: &FakeServer,
        clock: &TestClock,
        trust: ServerTrust<'_>,
        options: &ClientOptions,
    ) -> Report {
        let server = server();
        Doctor::new(bazel, probe, clock, &server, trust, options)
            .diagnose()
            .await
    }

    fn finding(report: &Report, check: Check) -> &Finding {
        report
            .findings()
            .iter()
            .find(|finding| finding.check == check)
            .expect("every check has a row")
    }

    fn statuses(report: &Report) -> Vec<(Check, Status)> {
        report
            .findings()
            .iter()
            .map(|finding| (finding.check, finding.status))
            .collect()
    }

    #[tokio::test]
    async fn healthy_setup_passes_every_check() {
        let clock = TestClock::new(now());
        let bazel = FakeBazel {
            options: Some(healthy_options()),
        };
        let probe = healthy_server(Some(now()));

        let report = run(&bazel, &probe, &clock, &ClientOptions::default()).await;

        assert_eq!(
            statuses(&report),
            vec![
                (Check::Server, Status::Ok),
                (Check::Tls, Status::Ok),
                (Check::Bazel, Status::Ok),
                (Check::RemoteExecutor, Status::Ok),
                (Check::RemoteCache, Status::Ok),
                (Check::TlsCertificate, Status::Ok),
                (Check::CredentialHelper, Status::Ok),
                (Check::Credentials, Status::Ok),
                (Check::Clock, Status::Ok),
            ]
        );
        assert!(report.is_healthy());
    }

    #[tokio::test]
    async fn tls_row_names_the_pin_when_the_server_was_verified_by_a_ca_pin() {
        let clock = TestClock::new(now());
        let bazel = FakeBazel {
            options: Some(healthy_options()),
        };
        let probe = healthy_server(Some(now()));
        let pin: CaPin = format!("sha256:{}", "cd".repeat(32))
            .parse()
            .expect("valid ca pin");

        let report = diagnose_with_trust(
            &bazel,
            &probe,
            &clock,
            ServerTrust::from(Some(&pin)),
            &ClientOptions::default(),
        )
        .await;

        let tls = finding(&report, Check::Tls);
        assert_eq!(tls.status, Status::Ok);
        assert_eq!(
            tls.summary,
            format!("trusted by the CA pinned as sha256:{}", "cd".repeat(32))
        );
    }

    #[tokio::test]
    async fn tls_row_names_the_system_roots_without_a_pin() {
        let clock = TestClock::new(now());
        let bazel = FakeBazel {
            options: Some(healthy_options()),
        };
        let probe = healthy_server(Some(now()));

        let report = diagnose_with_trust(
            &bazel,
            &probe,
            &clock,
            ServerTrust::from(None::<&CaPin>),
            &ClientOptions::default(),
        )
        .await;

        assert_eq!(
            finding(&report, Check::Tls).summary,
            "trusted by the system roots"
        );
    }

    #[tokio::test]
    async fn credential_probe_runs_the_helper_that_bazel_is_configured_with() {
        let clock = TestClock::new(now());
        let bazel = FakeBazel {
            options: Some(options_with_helper("/opt/gang/bin/helper")),
        };
        let probe = healthy_server(Some(now()));

        let report = run(&bazel, &probe, &clock, &ClientOptions::default()).await;

        assert_eq!(probe.calls(), vec!["/opt/gang/bin/helper".to_owned()]);
        assert_eq!(finding(&report, Check::Credentials).status, Status::Ok);
    }

    #[tokio::test]
    async fn home_rc_overriding_the_cache_fails_the_cache_row_and_names_the_rc_file() {
        let clock = TestClock::new(now());
        let options = EffectiveOptions::new(vec![
            (
                REMOTE_EXECUTOR.to_owned(),
                setting(EXECUTOR, OptionSource::Unknown),
            ),
            (
                REMOTE_CACHE.to_owned(),
                setting(
                    "grpc://ci.example.com:1",
                    OptionSource::RcFile {
                        path: PathBuf::from("/home/u/.bazelrc"),
                        section: "build:ci".to_owned(),
                    },
                ),
            ),
            (
                CREDENTIAL_HELPER.to_owned(),
                setting("gang-helper", OptionSource::Unknown),
            ),
        ]);
        let bazel = FakeBazel {
            options: Some(options),
        };
        let probe = healthy_server(Some(now()));

        let report = run(&bazel, &probe, &clock, &ClientOptions::default()).await;

        assert_eq!(finding(&report, Check::RemoteExecutor).status, Status::Ok);
        let cache = finding(&report, Check::RemoteCache);
        assert_eq!(cache.status, Status::Fail);
        assert!(
            cache.summary.contains("/home/u/.bazelrc"),
            "{}",
            cache.summary
        );
        assert!(
            cache.fix.as_deref().is_some_and(
                |fix| fix.contains("/home/u/.bazelrc") && fix.contains("missing from CAS")
            ),
            "{:?}",
            cache.fix
        );
        assert!(!report.is_healthy());
    }

    #[tokio::test]
    async fn unreachable_server_skips_the_rows_that_need_it() {
        let clock = TestClock::new(now());
        let bazel = FakeBazel {
            options: Some(healthy_options()),
        };
        let probe = FakeServer {
            read: Err(ServerFault::Unreachable("connection refused".to_owned())),
            credential: Ok(()),
            calls: Mutex::default(),
        };

        let report = run(&bazel, &probe, &clock, &ClientOptions::default()).await;

        assert_eq!(
            statuses(&report),
            vec![
                (Check::Server, Status::Fail),
                (Check::Tls, Status::Skip),
                (Check::Bazel, Status::Ok),
                (Check::RemoteExecutor, Status::Skip),
                (Check::RemoteCache, Status::Skip),
                (Check::TlsCertificate, Status::Skip),
                (Check::CredentialHelper, Status::Skip),
                (Check::Credentials, Status::Skip),
                (Check::Clock, Status::Skip),
            ]
        );
        assert!(probe.calls().is_empty());
        assert!(!report.is_healthy());
    }

    #[tokio::test]
    async fn bazel_that_cannot_start_fails_its_row_and_skips_the_bazel_dependent_rows() {
        let clock = TestClock::new(now());
        let bazel = FakeBazel { options: None };
        let probe = healthy_server(Some(now()));

        let report = run(&bazel, &probe, &clock, &ClientOptions::default()).await;

        assert_eq!(
            statuses(&report),
            vec![
                (Check::Server, Status::Ok),
                (Check::Tls, Status::Ok),
                (Check::Bazel, Status::Fail),
                (Check::RemoteExecutor, Status::Skip),
                (Check::RemoteCache, Status::Skip),
                (Check::TlsCertificate, Status::Skip),
                (Check::CredentialHelper, Status::Skip),
                (Check::Credentials, Status::Skip),
                (Check::Clock, Status::Ok),
            ]
        );
        assert!(probe.calls().is_empty());
        let bazel_row = finding(&report, Check::Bazel);
        assert_eq!(bazel_row.summary, "cannot read Bazel's effective flags");
        assert!(
            bazel_row
                .cause
                .as_deref()
                .is_some_and(|cause| cause.contains("no such file or directory")),
            "{:?}",
            bazel_row.cause
        );
        assert!(
            bazel_row
                .fix
                .as_deref()
                .is_some_and(|fix| fix.contains("install Bazel")),
            "{:?}",
            bazel_row.fix
        );
        assert!(!report.is_healthy());
    }

    /// One observed step of a run, reduced to what the tests compare.
    #[derive(Debug, PartialEq, Eq)]
    enum Seen {
        Started(Check),
        Finished(Check, Status),
    }

    /// Runs the doctor with a recording observer; returns the report and every event in order.
    async fn observed(
        bazel: &FakeBazel,
        probe: &FakeServer,
        clock: &TestClock,
    ) -> (Report, Vec<Seen>) {
        let server = server();
        let options = ClientOptions::default();
        let mut events = Vec::new();
        let report = Doctor::new(bazel, probe, clock, &server, ServerTrust::System, &options)
            .diagnose_with(&mut |event: Event<'_>| {
                events.push(match event {
                    Event::Started(check) => Seen::Started(check),
                    Event::Finished(finding) => Seen::Finished(finding.check, finding.status),
                });
            })
            .await;
        (report, events)
    }

    /// The findings of the events, which must be the report's findings in report order.
    fn finished(events: &[Seen]) -> Vec<(Check, Status)> {
        events
            .iter()
            .filter_map(|event| match event {
                Seen::Finished(check, status) => Some((*check, *status)),
                Seen::Started(_) => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn a_healthy_run_announces_the_slow_checks_and_emits_every_finding_once_in_order() {
        let clock = TestClock::new(now());
        let bazel = FakeBazel {
            options: Some(healthy_options()),
        };
        let probe = healthy_server(Some(now()));

        let (report, events) = observed(&bazel, &probe, &clock).await;

        assert_eq!(
            events,
            vec![
                Seen::Started(Check::Server),
                Seen::Finished(Check::Server, Status::Ok),
                Seen::Finished(Check::Tls, Status::Ok),
                Seen::Started(Check::Bazel),
                Seen::Finished(Check::Bazel, Status::Ok),
                Seen::Finished(Check::RemoteExecutor, Status::Ok),
                Seen::Finished(Check::RemoteCache, Status::Ok),
                Seen::Finished(Check::TlsCertificate, Status::Ok),
                Seen::Finished(Check::CredentialHelper, Status::Ok),
                Seen::Started(Check::Credentials),
                Seen::Finished(Check::Credentials, Status::Ok),
                Seen::Finished(Check::Clock, Status::Ok),
            ]
        );
        assert_eq!(finished(&events), statuses(&report));
    }

    #[tokio::test]
    async fn an_unreachable_server_emits_its_skips_and_never_starts_the_credential_probe() {
        let clock = TestClock::new(now());
        let bazel = FakeBazel {
            options: Some(healthy_options()),
        };
        let probe = FakeServer {
            read: Err(ServerFault::Unreachable("connection refused".to_owned())),
            credential: Ok(()),
            calls: Mutex::default(),
        };

        let (report, events) = observed(&bazel, &probe, &clock).await;

        assert_eq!(
            events,
            vec![
                Seen::Started(Check::Server),
                Seen::Finished(Check::Server, Status::Fail),
                Seen::Finished(Check::Tls, Status::Skip),
                Seen::Started(Check::Bazel),
                Seen::Finished(Check::Bazel, Status::Ok),
                Seen::Finished(Check::RemoteExecutor, Status::Skip),
                Seen::Finished(Check::RemoteCache, Status::Skip),
                Seen::Finished(Check::TlsCertificate, Status::Skip),
                Seen::Finished(Check::CredentialHelper, Status::Skip),
                Seen::Finished(Check::Credentials, Status::Skip),
                Seen::Finished(Check::Clock, Status::Skip),
            ]
        );
        assert_eq!(finished(&events), statuses(&report));
    }

    #[tokio::test]
    async fn a_failing_bazel_emits_its_row_and_skips_before_the_clock_and_starts_no_probe() {
        let clock = TestClock::new(now());
        let bazel = FakeBazel { options: None };
        let probe = healthy_server(Some(now()));

        let (report, events) = observed(&bazel, &probe, &clock).await;

        assert_eq!(
            events,
            vec![
                Seen::Started(Check::Server),
                Seen::Finished(Check::Server, Status::Ok),
                Seen::Finished(Check::Tls, Status::Ok),
                Seen::Started(Check::Bazel),
                Seen::Finished(Check::Bazel, Status::Fail),
                Seen::Finished(Check::RemoteExecutor, Status::Skip),
                Seen::Finished(Check::RemoteCache, Status::Skip),
                Seen::Finished(Check::TlsCertificate, Status::Skip),
                Seen::Finished(Check::CredentialHelper, Status::Skip),
                Seen::Finished(Check::Credentials, Status::Skip),
                Seen::Finished(Check::Clock, Status::Ok),
            ]
        );
        assert_eq!(finished(&events), statuses(&report));
        assert!(probe.calls().is_empty());
    }

    #[tokio::test]
    async fn failing_helper_fails_the_credentials_row_and_names_the_login() {
        let clock = TestClock::new(now());
        let bazel = FakeBazel {
            options: Some(healthy_options()),
        };
        let probe = FakeServer {
            read: Ok((info(&token()), Some(now()))),
            credential: Err(CredentialFault::Failed("exit status: 1".to_owned())),
            calls: Mutex::default(),
        };

        let report = run(&bazel, &probe, &clock, &ClientOptions::default()).await;

        let row = finding(&report, Check::Credentials);
        assert_eq!(row.status, Status::Fail);
        assert!(row.summary.contains("gang-helper"), "{}", row.summary);
        assert!(
            row.fix
                .as_deref()
                .is_some_and(|fix| fix.contains("gang login --server https://gang.example.com")),
            "{:?}",
            row.fix
        );
        assert!(!report.is_healthy());
    }

    #[tokio::test]
    async fn helper_that_cannot_run_fails_the_credentials_row_and_names_the_helper() {
        let clock = TestClock::new(now());
        let bazel = FakeBazel {
            options: Some(healthy_options()),
        };
        let probe = FakeServer {
            read: Ok((info(&token()), Some(now()))),
            credential: Err(CredentialFault::NotRunnable(
                "no such file or directory".to_owned(),
            )),
            calls: Mutex::default(),
        };

        let report = run(&bazel, &probe, &clock, &ClientOptions::default()).await;

        let row = finding(&report, Check::Credentials);
        assert_eq!(row.status, Status::Fail);
        assert!(row.summary.contains("gang-helper"), "{}", row.summary);
        assert!(
            row.cause
                .as_deref()
                .is_some_and(|cause| cause.contains("no such file or directory")),
            "{:?}",
            row.cause
        );
        assert!(
            row.fix
                .as_deref()
                .is_some_and(|fix| fix.contains("install it")),
            "{:?}",
            row.fix
        );
    }

    #[tokio::test]
    async fn no_covering_helper_skips_the_credentials_row_without_running_a_helper() {
        let clock = TestClock::new(now());
        let bazel = FakeBazel {
            options: Some(options_with_helper("other.example.com=gang-helper")),
        };
        let probe = healthy_server(Some(now()));

        let report = run(&bazel, &probe, &clock, &ClientOptions::default()).await;

        assert_eq!(
            finding(&report, Check::CredentialHelper).status,
            Status::Fail
        );
        assert_eq!(finding(&report, Check::Credentials).status, Status::Skip);
        assert!(probe.calls().is_empty());
    }

    #[tokio::test]
    async fn clock_skew_beyond_the_limit_warns_without_failing_the_report() {
        let clock = TestClock::new(now());
        let bazel = FakeBazel {
            options: Some(healthy_options()),
        };
        let probe = healthy_server(Some(now() + Duration::from_mins(2)));

        let report = run(&bazel, &probe, &clock, &ClientOptions::default()).await;

        let clock_row = finding(&report, Check::Clock);
        assert_eq!(clock_row.status, Status::Warn);
        assert!(
            clock_row.summary.contains("behind"),
            "{}",
            clock_row.summary
        );
        assert!(report.is_healthy());
    }

    #[tokio::test]
    async fn clock_skew_within_the_limit_is_ok() {
        let clock = TestClock::new(now());
        let bazel = FakeBazel {
            options: Some(healthy_options()),
        };
        let probe = healthy_server(Some(now() + Duration::from_secs(10)));

        let report = run(&bazel, &probe, &clock, &ClientOptions::default()).await;

        assert_eq!(finding(&report, Check::Clock).status, Status::Ok);
    }

    #[tokio::test]
    async fn server_without_login_never_runs_the_credential_helper() {
        let clock = TestClock::new(now());
        let bazel = FakeBazel {
            options: Some(EffectiveOptions::new(vec![
                (
                    REMOTE_EXECUTOR.to_owned(),
                    setting(EXECUTOR, OptionSource::Unknown),
                ),
                (
                    REMOTE_CACHE.to_owned(),
                    setting(CACHE, OptionSource::Unknown),
                ),
            ])),
        };
        // The helper would fail: the Ok row proves the helper was never run.
        let probe = FakeServer {
            read: Ok((info(&json!({"none": {}})), Some(now()))),
            credential: Err(CredentialFault::NotRunnable("would refuse".to_owned())),
            calls: Mutex::default(),
        };

        let report = run(&bazel, &probe, &clock, &ClientOptions::default()).await;

        assert_eq!(finding(&report, Check::Credentials).status, Status::Ok);
        assert_eq!(finding(&report, Check::CredentialHelper).status, Status::Ok);
        assert!(probe.calls().is_empty());
        assert!(report.is_healthy());
    }

    #[tokio::test]
    async fn server_with_an_unknown_authentication_scheme_fails_credentials_without_running_the_helper()
     {
        let clock = TestClock::new(now());
        let bazel = FakeBazel {
            options: Some(EffectiveOptions::new(vec![
                (
                    REMOTE_EXECUTOR.to_owned(),
                    setting(EXECUTOR, OptionSource::Unknown),
                ),
                (
                    REMOTE_CACHE.to_owned(),
                    setting(CACHE, OptionSource::Unknown),
                ),
            ])),
        };
        // The helper would succeed: the Fail row proves the helper was never run.
        let probe = FakeServer {
            read: Ok((info(&json!({"somethingNew": {}})), Some(now()))),
            credential: Ok(()),
            calls: Mutex::default(),
        };

        let report = run(&bazel, &probe, &clock, &ClientOptions::default()).await;

        assert_eq!(finding(&report, Check::Credentials).status, Status::Fail);
        assert!(probe.calls().is_empty());
    }

    #[tokio::test]
    async fn report_rows_follow_check_order() {
        let clock = TestClock::new(now());
        let bazel = FakeBazel {
            options: Some(healthy_options()),
        };
        let probe = FakeServer {
            read: Err(ServerFault::Unreachable("connection refused".to_owned())),
            credential: Ok(()),
            calls: Mutex::default(),
        };

        let report = run(&bazel, &probe, &clock, &ClientOptions::default()).await;

        assert_eq!(report.findings().len(), 9);
        assert!(
            report
                .findings()
                .windows(2)
                .all(|pair| pair[0].check <= pair[1].check),
            "{:?}",
            statuses(&report)
        );
    }
}
