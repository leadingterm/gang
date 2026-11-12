//! The cli scenario against a simulated `gang` command line: a compliant one passes every check, and each flaw fails exactly its check.

use super::{assert_compliant, failures};
use gang_e2e::{Call, Check, Colour, E2eError, Local, Outcome, Ran, Refused, Terminal, cli, names};
use gang_types::Url;
use parking_lot::Mutex;
use std::path::Path;

behaviour! {
    /// The behaviour of the simulated `gang` command line, one flaw per field.
    struct Behaviour {
        /// `gang revoke` without a target.
        /// Whether it is a usage error.
        revoke: Revoke {
            /// Exit 2, the usage error.
            Usage,
            /// Exit 1.
            Failure,
        },
        /// An unknown subcommand.
        /// Whether it is a usage error.
        unknown: Unknown {
            /// Exit 2, the usage error.
            Usage,
            /// Exit 1.
            Failure,
        },
        /// `gang doctor` without a server, not quiet.
        /// Whether it prints its failure record.
        doctor_record: DoctorRecord {
            /// The failure record.
            Recorded,
            /// A bare line that is not a record.
            Bare,
        },
        /// `gang doctor` without a server, quiet.
        /// Whether `--quiet` keeps its failure record.
        quiet_failure: QuietFailure {
            /// The failure record is printed.
            Kept,
            /// Nothing is printed.
            Dropped,
        },
        /// `gang server` with an unparsable configuration.
        /// Whether it is refused before announcing.
        unparsable: Unparsable {
            /// Refused without announcing.
            Refused,
            /// Refused, but the announcement is printed too.
            Announced,
        },
        /// `gang server` with a public listen address and no endpoint.
        /// Whether the error names the endpoint.
        public_listen: PublicListen {
            /// The endpoint is required.
            Refused,
            /// An unrelated bind error.
            OtherError,
        },
        /// `gang local down` with `--quiet`.
        /// Whether it drops the ok records.
        quiet_records: QuietRecords {
            /// The ok records are dropped.
            Dropped,
            /// The ok records are printed as if not quiet.
            Kept,
        },
        /// `gang local down` piped.
        /// Whether piped output carries colour.
        piped: Piped {
            /// No colour.
            Plain,
            /// Coloured as if forced.
            Coloured,
        },
        /// `gang local down` under `CLICOLOR_FORCE`.
        /// Whether `CLICOLOR_FORCE` colours the output.
        forced: Forced {
            /// Coloured.
            Coloured,
            /// No colour.
            Plain,
        },
        /// `gang local down` under `CLICOLOR_FORCE` and `NO_COLOR`.
        /// Whether `NO_COLOR` removes the colour `CLICOLOR_FORCE` adds.
        no_color: NoColor {
            /// The colour is removed.
            Wins,
            /// The colour stays.
            Ignored,
        },
        /// The fields of a pinned `gang local up`.
        /// Whether it reports the worker field.
        up_fields: UpFields {
            /// The worker field is printed.
            Reported,
            /// The worker field is omitted.
            NoWorker,
        },
        /// The pin the server log announces.
        /// Whether it is the one `gang local up` printed.
        announced_pin: AnnouncedPin {
            /// The same pin.
            Same,
            /// Another pin.
            Other,
        },
        /// A second pinned `gang local up`.
        /// Whether it is refused.
        second_up: SecondUp {
            /// Refused with the held lock.
            Refused,
            /// Succeeds like the first.
            Restarted,
        },
        /// `gang local down` of the pinned server.
        /// What it prints when it stops the server.
        down: Down {
            /// The stop record, then the terminated record.
            StoppedThenTerminated,
            /// Only the terminated record.
            TerminatedOnly,
        },
        /// A repeated `gang local down`.
        /// Whether it fails after the pinned server stopped.
        repeat_down: RepeatDown {
            /// Reports that nothing runs.
            NothingRunning,
            /// Fails with no server record.
            Failure,
        },
        /// The fields of an insecure `gang local up`.
        /// Whether it reports its `ui` field.
        insecure_fields: InsecureFields {
            /// The `ui` field.
            Ui,
            /// A `ca-pin` field instead.
            CaPin,
        },
        /// The bazelrc of an insecure server.
        /// Whether it is unpinned.
        bazelrc: Bazelrc {
            /// No CA certificate.
            Unpinned,
            /// Names the CA certificate too.
            Pinned,
        },
        /// `gang local down` of the insecure server.
        /// Whether stopping it terminates its worker.
        insecure_down: InsecureDown {
            /// One worker terminated.
            TerminatesWorker,
            /// Zero workers terminated.
            LeavesWorker,
        },
        /// `gang doctor` on an executor other than the server's.
        /// Whether it compares the executor.
        doctor_executor: DoctorExecutor {
            /// The executor is compared.
            Compared,
            /// The executor is ignored.
            Ignored,
        },
        /// `gang doctor` on the server's executor.
        /// Whether it passes a matching executor.
        doctor_match: DoctorMatch {
            /// Passes with a matching executor.
            Passes,
            /// Fails whatever the executor.
            Fails,
        },
        /// Whether the launched worker connects.
        connection: Connection {
            /// The worker connects.
            Connects,
            /// The worker never connects.
            Stays,
        },
        /// The worker endpoint the worker command reports.
        /// Whether the worker command reports the worker endpoint.
        worker_flags: WorkerFlags {
            /// The reported worker endpoint.
            Reported,
            /// A stale worker endpoint.
            Stale,
        },
        /// `gang worker` without any endpoint.
        /// Whether the error names the endpoint.
        worker_endpoint_missing: WorkerEndpointMissing {
            /// The endpoint is required.
            Refused,
            /// An unrelated error.
            OtherError,
        },
        /// `gang worker --endpoint` with a value that is not a URL.
        /// Whether the flag validates its value.
        endpoint_flag: EndpointFlag {
            /// Rejected with a usage error.
            Usage,
            /// Accepted.
            Accepted,
        },
        /// `gang worker --worker-endpoint` with a value that is not a URL.
        /// Whether the flag validates its value.
        worker_endpoint_flag: WorkerEndpointFlag {
            /// Rejected with a usage error.
            Usage,
            /// Accepted.
            Accepted,
        },
    }
}

/// The public TLS endpoint of the pinned server.
const PUBLIC_TLS: &str = "https://127.0.0.1:41000";
/// The worker TLS endpoint of the pinned server.
const WORKER_TLS: &str = "grpcs://127.0.0.1:41001";
/// The public plaintext endpoint of the insecure server.
const PUBLIC_PLAIN: &str = "http://127.0.0.1:41000";
/// The worker plaintext endpoint of the insecure server.
const WORKER_PLAIN: &str = "grpc://127.0.0.1:41001";
/// The executor the server reports.
const EXECUTOR: &str = "grpc://127.0.0.1:41000";
/// The CA pin the server announces.
const PIN: &str = concat!(
    "sha256:", "abababab", "abababab", "abababab", "abababab", "abababab", "abababab", "abababab",
    "abababab"
);
/// Another CA pin.
const PIN_OTHER: &str = concat!(
    "sha256:", "cdcdcdcd", "cdcdcdcd", "cdcdcdcd", "cdcdcdcd", "cdcdcdcd", "cdcdcdcd", "cdcdcdcd",
    "cdcdcdcd"
);
/// The server log of the scenario.
const LOG: &str = "/sim/state/server/server.log";
/// The log of the simulated worker that never connects.
const WORKER_LOG: &str = "fail  gang worker: the simulated worker exited";
/// An executor value that is not a URL.
const NOT_A_URL: &str = "not a url";

/// The local deployment the simulated server runs, and whether the pinned server was stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Running {
    /// The deployment running, if any.
    running: Option<Local>,
    /// Whether the pinned server was stopped at least once.
    stopped_pinned: bool,
}

/// The simulated `gang` command line, behaving as its [`Behaviour`] says.
struct SimulatedTerminal {
    /// How the command line behaves.
    behaviour: Behaviour,
    /// The deployment state shared by the commands.
    state: Mutex<Running>,
}

impl SimulatedTerminal {
    /// A command line that behaves as `behaviour` says, with nothing running.
    fn new(behaviour: Behaviour) -> Self {
        Self {
            behaviour,
            state: Mutex::new(Running {
                running: None,
                stopped_pinned: false,
            }),
        }
    }

    /// `gang revoke` without a target.
    fn revoke(&self) -> Ran {
        let code = match self.behaviour.revoke {
            Revoke::Usage => 2,
            Revoke::Failure => 1,
        };
        ran(
            code,
            "",
            "  fail  gang revoke: the following required arguments were not provided\n",
        )
    }

    /// An unknown subcommand.
    fn unknown(&self) -> Ran {
        let code = match self.behaviour.unknown {
            Unknown::Usage => 2,
            Unknown::Failure => 1,
        };
        ran(code, "", "  fail  gang: unrecognized subcommand 'bogus'\n")
    }

    /// `gang doctor` with no server and no stored login.
    fn doctor_without_server(&self, quiet: bool) -> Ran {
        const STANDARD: &str = "  fail  gang doctor: no server given and no stored login\n   fix  pass --server <URL>, or run gang login --server <URL>\n";
        let stderr = match (
            quiet,
            self.behaviour.quiet_failure,
            self.behaviour.doctor_record,
        ) {
            (true, QuietFailure::Dropped, _) => "",
            (false, _, DoctorRecord::Bare) => "error: no server\n",
            (true, QuietFailure::Kept, _) | (false, _, DoctorRecord::Recorded) => STANDARD,
        };
        ran(1, "", stderr)
    }

    /// `gang server` on a configuration it must refuse.
    fn server(&self, refused: Refused) -> Ran {
        match refused {
            Refused::Unparsable => {
                let stdout = match self.behaviour.unparsable {
                    Unparsable::Refused => "",
                    Unparsable::Announced => "listening: public 127.0.0.1:41000\n",
                };
                ran(
                    1,
                    stdout,
                    "  fail  gang server: invalid configuration /sim/unparsable.toml: missing field poll_interval\n",
                )
            }
            Refused::PublicListenWithoutEndpoint => {
                let stderr = match self.behaviour.public_listen {
                    PublicListen::Refused => {
                        "  fail  gang server: invalid configuration /sim/public-listen.toml: endpoint is required when listen `0.0.0.0:0` is not a loopback address\n"
                    }
                    PublicListen::OtherError => "  fail  gang server: cannot bind 0.0.0.0:0\n",
                };
                ran(1, "", stderr)
            }
        }
    }

    /// `gang local up` of `local`, while `state` says what runs.
    fn up(&self, state: &mut Running, local: Local) -> Ran {
        let restart = self.behaviour.second_up == SecondUp::Restarted
            && local == Local::Pinned
            && state.running == Some(Local::Pinned);
        if state.running.is_some() && !restart {
            return ran(
                1,
                "",
                "  fail  gang local up: /sim/state/server/server.lock is held by a running gang server or a gang local down in progress\n   fix  gang local down\n",
            );
        }
        state.running = Some(local);
        let started = record("started the gang server in 1.2s · pid 4242", false);
        match local {
            Local::Pinned => {
                let worker = match self.behaviour.up_fields {
                    UpFields::Reported => format!("worker  {WORKER_TLS}\n"),
                    UpFields::NoWorker => String::new(),
                };
                let stdout = format!(
                    "{started}\npublic  {PUBLIC_TLS}\n{worker}ca-pin  {PIN}\n   log  {LOG}\n"
                );
                ran(0, &stdout, "")
            }
            Local::Insecure => {
                let fields = match self.behaviour.insecure_fields {
                    InsecureFields::Ui => format!("    ui  {PUBLIC_PLAIN}/ui/\n"),
                    InsecureFields::CaPin => format!("ca-pin  {PIN}\n"),
                };
                let stdout = format!(
                    "{started}\npublic  {PUBLIC_PLAIN}\nworker  {WORKER_PLAIN}\n{fields}   log  {LOG}\n"
                );
                ran(
                    0,
                    &stdout,
                    "  warn  no TLS and no authentication: anyone on this machine can use the farm\n",
                )
            }
        }
    }

    /// `gang local down`, with `quiet` and `colour` as given.
    fn down(&self, state: &mut Running, quiet: bool, colour: Colour) -> Ran {
        let coloured = match colour {
            Colour::Plain => self.behaviour.piped == Piped::Coloured,
            Colour::Forced => self.behaviour.forced == Forced::Coloured,
            Colour::ForcedNoColor => self.behaviour.no_color == NoColor::Ignored,
        };
        let stdout = match state.running.take() {
            None => {
                if self.behaviour.repeat_down == RepeatDown::Failure && state.stopped_pinned {
                    return ran(1, "", "  fail  gang local down: no server record\n");
                }
                format!(
                    "{}{}",
                    record("no gang server was running", coloured),
                    record("terminated 0 workers", coloured)
                )
            }
            Some(local) => {
                if local == Local::Pinned {
                    state.stopped_pinned = true;
                }
                let terminated = match (local, self.behaviour.insecure_down) {
                    (Local::Insecure, InsecureDown::TerminatesWorker) => "1 worker",
                    _ => "0 workers",
                };
                let stopped =
                    if local == Local::Pinned && self.behaviour.down == Down::TerminatedOnly {
                        String::new()
                    } else {
                        record("stopped the gang server · pid 4242", coloured)
                    };
                format!(
                    "{stopped}{}",
                    record(&format!("terminated {terminated}"), coloured)
                )
            }
        };
        let print = !quiet || self.behaviour.quiet_records == QuietRecords::Kept;
        let shown = if print { stdout.as_str() } else { "" };
        ran(0, shown, "")
    }

    /// `gang bazelrc` of a server.
    fn bazelrc(&self) -> Ran {
        let mut stdout =
            format!("build --remote_executor={EXECUTOR}\nbuild --remote_cache={EXECUTOR}\n");
        if self.behaviour.bazelrc == Bazelrc::Pinned {
            stdout.push_str("build --tls_certificate=gang-ca.pem\n");
        }
        ran(0, &stdout, "")
    }

    /// `gang doctor` on `executor`, the value Bazel reports.
    fn doctor(&self, executor: &str) -> Ran {
        let passes = self.behaviour.doctor_match == DoctorMatch::Passes
            && (executor == EXECUTOR || self.behaviour.doctor_executor == DoctorExecutor::Ignored);
        if passes {
            ran(
                0,
                &format!(
                    "    ok  remote_executor  {executor}\n    ok  clock  within 0s of the server\n"
                ),
                "",
            )
        } else {
            ran(
                1,
                &format!(
                    "  fail  remote_executor  Bazel sets --remote_executor={executor}; Gang's is {EXECUTOR}\n"
                ),
                "",
            )
        }
    }

    /// `gang worker` with the given endpoint flags.
    fn worker(&self, endpoint: Option<&str>, worker_endpoint: Option<&str>) -> Ran {
        if endpoint == Some(NOT_A_URL) {
            return match self.behaviour.endpoint_flag {
                EndpointFlag::Usage => ran(
                    2,
                    "",
                    "  fail  gang worker: invalid value 'not a url' for '--endpoint <URL>'\n",
                ),
                EndpointFlag::Accepted => ran(1, "", ""),
            };
        }
        if worker_endpoint == Some(NOT_A_URL) {
            return match self.behaviour.worker_endpoint_flag {
                WorkerEndpointFlag::Usage => ran(
                    2,
                    "",
                    "  fail  gang worker: invalid value 'not a url' for '--worker-endpoint <URL>'\n",
                ),
                WorkerEndpointFlag::Accepted => ran(1, "", ""),
            };
        }
        let stderr = match self.behaviour.worker_endpoint_missing {
            WorkerEndpointMissing::Refused => {
                "  fail  gang worker: endpoint is required to enroll the worker when worker authentication is mtls\n"
            }
            WorkerEndpointMissing::OtherError => {
                "  fail  gang worker: cannot reach the scheduler\n"
            }
        };
        ran(1, "", stderr)
    }
}

impl Terminal for SimulatedTerminal {
    fn run(&self, call: &Call) -> gang_e2e::Result<Ran> {
        let mut state = self.state.lock();
        let outcome = match call {
            Call::RevokeWithoutTarget => self.revoke(),
            Call::UnknownSubcommand => self.unknown(),
            Call::DoctorWithoutServer { quiet } => self.doctor_without_server(*quiet),
            Call::Server(refused) => self.server(*refused),
            Call::Up(local) => self.up(&mut state, *local),
            Call::Down { quiet, colour, .. } => self.down(&mut state, *quiet, *colour),
            Call::Bazelrc { .. } => self.bazelrc(),
            Call::Doctor { executor, .. } => self.doctor(executor),
            Call::Worker {
                endpoint,
                worker_endpoint,
            } => self.worker(endpoint.as_deref(), worker_endpoint.as_deref()),
        };
        Ok(outcome)
    }

    fn server_log(&self, _path: &Path) -> gang_e2e::Result<String> {
        let pin = match self.behaviour.announced_pin {
            AnnouncedPin::Same => PIN,
            AnnouncedPin::Other => PIN_OTHER,
        };
        Ok(format!(
            "listening: public 127.0.0.1:41000\nlistening: worker 127.0.0.1:41001\nca-pin: {pin}\n"
        ))
    }

    fn worker_connects(&self, _public: &Url) -> gang_e2e::Result<bool> {
        let running = self.state.lock().running;
        Ok(running == Some(Local::Insecure) && self.behaviour.connection == Connection::Connects)
    }

    fn worker_log_tail(&self) -> gang_e2e::Result<String> {
        Ok(if self.behaviour.connection == Connection::Stays {
            WORKER_LOG.to_owned()
        } else {
            String::new()
        })
    }

    fn worker_commands(&self) -> gang_e2e::Result<Vec<Vec<String>>> {
        if self.state.lock().running != Some(Local::Insecure) {
            return Ok(Vec::new());
        }
        let worker_endpoint = match self.behaviour.worker_flags {
            WorkerFlags::Reported => "grpc://127.0.0.1:41001/",
            WorkerFlags::Stale => "grpc://127.0.0.1:9090/",
        };
        Ok(vec![vec![
            "/sim/gang".into(),
            "worker".into(),
            "--config".into(),
            "/sim/state/server/gang.toml".into(),
            "--endpoint".into(),
            "http://127.0.0.1:41000/".into(),
            "--worker-endpoint".into(),
            worker_endpoint.into(),
        ]])
    }
}

/// A `gang` outcome with the given exit code, standard output and standard error.
fn ran(code: i32, stdout: &str, stderr: &str) -> Ran {
    Ran {
        code: Some(code),
        stdout: stdout.to_owned(),
        stderr: stderr.to_owned(),
    }
}

/// The record line `gang` prints for a passing step, coloured when `coloured` is set.
fn record(text: &str, coloured: bool) -> String {
    if coloured {
        format!("    \x1b[32mok\x1b[0m  {text}\n")
    } else {
        format!("    ok  {text}\n")
    }
}

/// The verdicts of the cli scenario against a simulated `gang` command line that behaves as
/// `behaviour` says.
fn cli_checks(behaviour: Behaviour) -> gang_e2e::Result<Vec<Check>> {
    cli(&SimulatedTerminal::new(behaviour))
}

#[test]
fn cli_is_compliant() {
    let checks = cli_checks(Behaviour::COMPLIANT).expect("the cli scenario runs");
    assert_eq!(checks.len(), 25);
    assert_compliant(&checks);
}

flaw! { revoke_fails_its_check: cli_checks(revoke = Revoke::Failure) => only [names::cli::REVOKE_WITHOUT_TARGET] }
flaw! { unknown_fails_its_check: cli_checks(unknown = Unknown::Failure) => only [names::cli::UNKNOWN_SUBCOMMAND] }
flaw! { doctor_record_fails_its_check: cli_checks(doctor_record = DoctorRecord::Bare) => only [names::cli::DOCTOR_WITHOUT_SERVER] }
flaw! { quiet_failure_fails_its_check: cli_checks(quiet_failure = QuietFailure::Dropped) => only [names::cli::QUIET_KEEPS_FAILURE] }
flaw! { unparsable_fails_its_check: cli_checks(unparsable = Unparsable::Announced) => only [names::cli::UNPARSABLE_CONFIG] }
flaw! { public_listen_fails_its_check: cli_checks(public_listen = PublicListen::OtherError) => only [names::cli::PUBLIC_LISTEN_NEEDS_ENDPOINT] }
flaw! { quiet_records_fails_its_check: cli_checks(quiet_records = QuietRecords::Kept) => only [names::cli::QUIET_DROPS_OK] }
flaw! { piped_fails_its_check: cli_checks(piped = Piped::Coloured) => only [names::cli::PIPED_PLAIN] }
flaw! { forced_fails_its_check: cli_checks(forced = Forced::Plain) => only [names::cli::CLICOLOR_FORCE_COLOURS] }
flaw! { no_color_fails_its_check: cli_checks(no_color = NoColor::Ignored) => only [names::cli::NO_COLOR_WINS] }
flaw! { up_fields_fails_its_check: cli_checks(up_fields = UpFields::NoWorker) => only [names::cli::UP_REPORTS] }
flaw! { announced_pin_fails_its_check: cli_checks(announced_pin = AnnouncedPin::Other) => only [names::cli::UP_PIN_ANNOUNCED] }
flaw! { second_up_fails_its_check: cli_checks(second_up = SecondUp::Restarted) => only [names::cli::UP_TWICE_REFUSED] }
flaw! { down_fails_its_check: cli_checks(down = Down::TerminatedOnly) => only [names::cli::DOWN_STOPS] }
flaw! { repeat_down_fails_its_check: cli_checks(repeat_down = RepeatDown::Failure) => only [names::cli::DOWN_AGAIN] }
flaw! { insecure_fields_fails_its_check: cli_checks(insecure_fields = InsecureFields::CaPin) => only [names::cli::INSECURE_UP_REPORTS] }
flaw! { bazelrc_fails_its_check: cli_checks(bazelrc = Bazelrc::Pinned) => only [names::cli::INSECURE_BAZELRC_UNPINNED] }
flaw! { insecure_down_fails_its_check: cli_checks(insecure_down = InsecureDown::LeavesWorker) => only [names::cli::INSECURE_DOWN_TERMINATES] }
flaw! { doctor_executor_fails_its_check: cli_checks(doctor_executor = DoctorExecutor::Ignored) => only [names::cli::DOCTOR_MISMATCH_FAILS] }
flaw! { doctor_match_fails_its_check: cli_checks(doctor_match = DoctorMatch::Fails) => only [names::cli::DOCTOR_MATCH_PASSES] }
flaw! { connection_fails_its_check: cli_checks(connection = Connection::Stays) => only [names::cli::WORKER_CONNECTS] }
flaw! { worker_flags_fails_its_check: cli_checks(worker_flags = WorkerFlags::Stale) => only [names::cli::WORKER_FLAGS] }
flaw! { worker_endpoint_missing_fails_its_check: cli_checks(worker_endpoint_missing = WorkerEndpointMissing::OtherError) => only [names::cli::WORKER_NEEDS_ENDPOINT] }
flaw! { endpoint_flag_fails_its_check: cli_checks(endpoint_flag = EndpointFlag::Accepted) => only [names::cli::ENDPOINT_FLAG_VALIDATED] }
flaw! { worker_endpoint_flag_fails_its_check: cli_checks(worker_endpoint_flag = WorkerEndpointFlag::Accepted) => only [names::cli::WORKER_ENDPOINT_FLAG_VALIDATED] }

#[test]
fn a_worker_that_never_connects_quotes_the_worker_log() -> gang_e2e::Result<()> {
    let checks = cli_checks(Behaviour {
        connection: Connection::Stays,
        ..Behaviour::COMPLIANT
    })?;
    let check = checks
        .iter()
        .find(|check| check.name == names::cli::WORKER_CONNECTS)
        .ok_or_else(|| E2eError::system("the worker connection check is missing"))?;
    assert!(matches!(&check.outcome, Outcome::Failed(reason) if reason.contains(WORKER_LOG)));
    Ok(())
}
