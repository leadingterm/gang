//! The names of the checks, each stated once.
//!
//! A [`Check`](crate::Check) is named by the statement it verifies. The scenarios, the verdicts
//! and the tests that look a check up by name all take the sentence from here, so rewording one
//! is a change to one line. A name that depends on its scenario's input is built by a function.

/// Names of the checks of the isolation of an action from the host and from the worker.
pub mod isolation {
    /// The net namespace of an isolated action is not the host's.
    pub const NET_NAMESPACE: &str = "the net namespace differs from the host's";
    /// The mnt namespace of an isolated action is not the host's.
    pub const MNT_NAMESPACE: &str = "the mnt namespace differs from the host's";
    /// Both namespaces differ, where the noop sandbox isolates neither.
    pub const NET_AND_MNT_NAMESPACES: &str = "the net and mnt namespaces differ from the host's";
    /// The action's parent directories do not lead to the worker's CAS.
    pub const SIBLINGS_HIDDEN: &str =
        "the worker's CAS cannot be reached through the action's parent directories";
    /// The probe sees the action's parent directories, as the noop control requires.
    pub const SIBLINGS_LISTED: &str = "the probe lists the action's parent directories";
    /// The action's working directory is writable.
    pub const WORKING_DIRECTORY_WRITABLE: &str = "the working directory is writable";
    /// A working directory under the sandbox's tmp path stays visible.
    pub const WORKING_DIRECTORY_VISIBLE: &str =
        "a working directory under the sandbox tmp path stays visible";
    /// An action of the noop fleet runs in the host's own namespaces.
    pub const NOOP_FLEET_UNISOLATED: &str =
        "an action of the noop fleet runs in the host's net and mnt namespaces";
}

/// Names of the checks of network capability admission.
pub mod network {
    /// An ungranted request is rejected before the action reports output.
    pub const ADMISSION: &str = "an ungranted network request is rejected before execution";
}

/// Names of the checks of dynamically linked glibc programs in the sandbox.
pub mod glibc_toolchain {
    /// A dynamically linked glibc program, as a downloaded toolchain is, starts inside the sandbox.
    pub const TOOL_RUNS: &str = "a dynamically linked glibc program runs inside the sandbox";
}

/// Names of the checks of the tools a sandboxed action finds at standard paths.
pub mod fhs_tools {
    /// A script with `#!/usr/bin/env bash` runs with Bazel's strict action `PATH`.
    pub const SCRIPT_RUNS: &str =
        "a #!/usr/bin/env bash script runs with Bazel's strict action PATH";
}

/// Names of the checks of a build's outcome.
pub mod build {
    /// Every action of the build ran remotely.
    pub const REMOTE_EXECUTION: &str = "the actions run remotely";
    /// A rebuild after clean was served by the remote cache.
    pub const CACHE_HIT: &str = "a rebuild after clean is a full remote cache hit";

    /// An action that exits with `code` fails the build with that exit code.
    #[must_use]
    pub fn exit_status(code: i32) -> String {
        format!("an action exiting with {code} fails the build with that exit code")
    }
}
/// Names of the checks of per-build accounting.
pub mod builds {
    /// The cold invocation has exactly one listed build.
    pub const COLD_LIST: &str = "cold invocation lists exactly one build";
    /// The cold build has at least one cache miss.
    pub const COLD_MISSES: &str = "cold build records cache misses";
    /// The cold build has at least one completed execution.
    pub const COLD_COMPLETED: &str = "cold build records completed executions";
    /// The cold build's timestamps are ordered.
    pub const COLD_TIMES: &str = "cold build timestamps are ordered";
    /// The cold build belongs to its client principal.
    pub const COLD_PRINCIPAL: &str = "cold build names its client principal";
    /// The warm invocation has exactly one listed build.
    pub const WARM_LIST: &str = "warm invocation lists exactly one build";
    /// The warm build has at least one cache hit.
    pub const WARM_HITS: &str = "warm build records cache hits";
    /// The warm build has no cache misses.
    pub const WARM_MISSES: &str = "warm build records no cache misses";
    /// The warm build's timestamps are ordered.
    pub const WARM_TIMES: &str = "warm build timestamps are ordered";
    /// The warm build belongs to its client principal.
    pub const WARM_PRINCIPAL: &str = "warm build names its client principal";
    /// The two invocations have different build names.
    pub const DISTINCT: &str = "cold and warm builds have different names";
    /// The output is large enough for `ByteStream` and is the expected distinct blob.
    pub const BLOB_THRESHOLD: &str = "warm output is the expected ByteStream-sized blob";
    /// The engine reports the configured budgets for both filesystem stores.
    pub const ENGINE_BUDGETS: &str = "engine reports configured CAS and AC budgets";
    /// The content store grows after the cold build.
    pub const CAS_GROWTH: &str = "engine CAS usage grows after the build";
    /// The cold build attributes traffic in both directions.
    pub const COLD_BYTES: &str = "cold build attributes uploaded and downloaded bytes";
    /// The warm build attributes at least the downloaded output blob.
    pub const WARM_BYTES: &str = "warm build attributes downloaded output bytes";
}

/// Names of the checks of the scale-outs a build causes.
pub mod scaling {
    /// The first scale-out starts the fleet's first worker.
    pub const FIRST_SCALE_OUT_FROM_ZERO: &str = "the first scale-out raises the fleet from 0 to 1";
}

/// Names of the checks of work-aware scaling.
pub mod work_aware {
    /// The training build finishes successfully.
    pub const WARMUP_BUILD: &str = "warmup build succeeds";
    /// The readiness sample includes the held launch.
    pub const READINESS: &str = "readiness is measured";
    /// The trained fleet drains before the burst.
    pub const WARM_DRAIN: &str = "warm fleet drains to zero";
    /// A trained burst buys at most one worker.
    pub const BURST_SCALE_OUT: &str = "burst launches at most one worker";
    /// The burst finishes successfully.
    pub const BURST_BUILD: &str = "burst build succeeds";
    /// Both held actions execute while the third stays queued.
    pub const QUEUED: &str = "two held actions leave one queued";
    /// Short running work absorbs the queued action.
    pub const QUEUED_SCALE_OUT: &str = "queued short work does not scale out";
    /// The queued build finishes successfully.
    pub const QUEUED_BUILD: &str = "queued build succeeds";
    /// The held actions' build finishes successfully.
    pub const QUEUED_HELD_BUILD: &str = "held build succeeds";
    /// Persistent held work grows the fleet to three workers.
    pub const BACKLOG_WORKERS: &str = "held backlog reaches three workers";
    /// The backlog build finishes successfully.
    pub const BACKLOG_BUILD: &str = "backlog build succeeds";
    /// Completion evidence spans the measured classification horizon.
    pub const STREAM_REFILL: &str = "stream refills beyond readiness plus payback";
    /// Purchases advance one worker at a time.
    pub const STREAM_STEPS: &str = "stream scales out in bounded steps";
    /// Closed-loop refill stays within one worker's paid capacity.
    pub const STREAM_CEILING: &str = "stream stays within paid capacity";
    /// Successful purchases carry their decision inputs.
    pub const DECISION_DETAIL: &str = "scale-out audit includes decision detail";
    /// The streaming build succeeds after release.
    pub const STREAM_BUILD: &str = "sustained stream build succeeds";
    /// Distinct authenticated streams share one worker while refilling.
    pub const SOURCE_ISOLATION: &str = "distinct authenticated streams share one worker";
    /// Both independent principal builds finish.
    pub const SOURCES_BUILD: &str = "both source builds succeed";
    /// Light work occupies all three workers before scale-in.
    pub const LIGHT_LOAD_SPREAD: &str = "light load is distributed over three workers";
    /// Busy drains are audited with utilization evidence.
    pub const UTILIZATION_DRAINS: &str = "three workers drain to one for UTILIZATION";
    /// At most one physical instance is cordoned concurrently.
    pub const CORDON_BOUND: &str = "max_cordons is respected";
    /// Held and later returning work both complete.
    pub const UTILIZATION_BUILD: &str = "held and returning-wave builds succeed";
    /// The same physical worker remains after all actions finish.
    pub const SURVIVING_WORKER: &str = "builds finish with the surviving worker";
}

/// Names of the checks of the lifecycle scenario.
pub mod lifecycle {
    /// No worker runs before the first build.
    pub const STARTS_AT_ZERO: &str = "the fleet starts at zero workers";
    /// The cold build started exactly one worker.
    pub const COLD_BUILD_ONE_WORKER: &str = "a cold build leaves one worker";
    /// The idle fleet drains to zero workers.
    pub const IDLE_DRAIN: &str = "idle drain returns the fleet to zero workers";
    /// Every drained worker was terminated.
    pub const EVERY_DRAIN_TERMINATED: &str = "every drained worker is recorded as terminated";
    /// The ids of the drained workers lead to the terminated instances.
    pub const WORKER_IDS: &str = "real worker ids parse to the instance the provisioner terminates";
    /// A rebuild served by the cache starts no worker.
    pub const CACHE_HIT_NEEDS_NO_WORKER: &str = "the cache hit needs no worker";
    /// A rebuild served by the cache scales nothing out.
    pub const WARM_REBUILD_DID_NOT_SCALE_OUT: &str = "the warm rebuild did not scale out";
}

/// Names of the checks of the input-integrity scenario.
pub mod integrity {
    /// The build of the poisoning and the later action succeeded.
    pub const BUILD_SUCCEEDS: &str = "the build succeeds";
    /// One worker ran both actions.
    pub const ONE_WORKER: &str = "both actions ran on one worker";
    /// The poisoning action reached its modification.
    pub const POISON_ATTEMPTED: &str = "the poisoning action modified its input";
    /// A later action does not see the modification.
    pub const NO_ALTERATION: &str =
        "an action cannot alter a cached input that a later action sees";
    /// With input links kept, a later action does see the modification.
    pub const KEEP_CONTROL: &str =
        "control: with input_links = keep a modified input reaches the later action";
    /// A read-only bound input subtree rejects chmod and writes with `EROFS`.
    pub const BOUND_TREE_READ_ONLY: &str =
        "a read-only bound input tree rejects chmod and writes with EROFS";
}

/// Names of the checks of the input root scenario.
pub mod input_root {
    /// The build of the input root probe succeeded.
    pub const BUILD_SUCCEEDS: &str = "the input root probe builds";
    /// Bazel gave the action an input outside its working directory.
    pub const OUTSIDE_WORKING_DIRECTORY: &str =
        "the action's input lies outside its working directory";
    /// The action read that input.
    pub const SIBLING_READ: &str = "the action reads an input beside its working directory";
    /// The action read the same input through a relative symlink.
    pub const SYMLINK_FOLLOWED: &str = "the action follows a relative symlink to that input";
    /// With input links broken, that input has a private inode.
    pub const LINKS_BROKEN: &str = "that input's hard link is broken";
}

/// Names of the checks of the restart scenario.
pub mod restart {
    /// The scale-out is recorded before the server is killed.
    pub const SCALE_OUT_RECORDED: &str = "the scale-out is recorded before the restart";
    /// The build had not ended when the server was killed.
    pub const BUILD_STILL_RUNNING: &str = "the build was still running when the server was killed";
    /// The build ended successfully across the restart.
    pub const BUILD_SURVIVES: &str = "the build survives a server restart mid-scale-out";
    /// The server process is a new one.
    pub const SERVER_RESTARTED: &str = "the server was restarted";
    /// The restart did not scale out a second time.
    pub const SCALE_OUT_NOT_REPEATED: &str = "the restart did not repeat the scale-out";
    /// The fleet settles at one worker.
    pub const CONVERGED: &str = "the fleet converged to one worker";
}

/// Names of the checks of the revocation scenario.
pub mod revocation {
    /// The administrator's identity provider issued the token.
    pub const TOKEN_ISSUED: &str = "the developer token is issued";
    /// A request without a token is unauthenticated.
    pub const ANONYMOUS_REQUEST: &str = "a request without a token is unauthenticated";
    /// A build without a token is rejected by the server.
    pub const ANONYMOUS_BUILD: &str = "a build without a token is rejected for its authentication";
    /// A connection with the token is accepted.
    pub const CONNECTION_ACCEPTED: &str = "a connection with the token is accepted";
    /// The connection is open when the token is revoked.
    pub const CONNECTION_OPEN: &str = "the connection is open just before revocation";
    /// A request with the valid token is served.
    pub const SERVED_BEFORE: &str = "a request with the valid token is served before revocation";
    /// The open connection is denied at its next request.
    pub const OPEN_CONNECTION_DENIED: &str =
        "the open connection's next request is denied right after revocation";
    /// A new connection with the revoked token is denied.
    pub const NEW_CONNECTION_DENIED: &str = "a new connection with the revoked token is denied";
    /// A build with the revoked token is rejected by the server.
    pub const REVOKED_BUILD: &str =
        "a build with the revoked token is rejected for its authentication";
    /// The server process is the same after the revocation.
    pub const NO_RESTART: &str = "revocation needed no server restart";
    /// A new connection with the restored token is served.
    pub const RESTORED_CONNECTION: &str = "a new connection with the restored token is served";
    /// A build with the restored token succeeds.
    pub const RESTORED_BUILD: &str = "a build with the restored token succeeds";
}

/// Names of the checks of the state stream, shared by the scenarios that open it.
pub mod stream {
    /// The first event is the snapshot.
    pub const STARTS_WITH_SNAPSHOT: &str = "the first event of the state stream is a snapshot";
    /// Every delta applies to the state before it.
    pub const APPLIES: &str = "every delta of the state stream applies to the state before it";
    /// The scale-out carries its cause.
    pub const CAUSE_DEMAND: &str = "the stream carries a scale-out caused by demand";
    /// The start of a drain carries its cause.
    pub const CAUSE_IDLE_TIMEOUT: &str =
        "the stream carries the start of a drain caused by idle_timeout";
    /// A termination carries its cause.
    pub const CAUSE_DRAIN_FINISHED: &str =
        "the stream carries a termination caused by drain_finished";
}

/// Names of the checks of the plaintext scenario.
pub mod plaintext {
    /// No worker runs before the build.
    pub const STARTS_WITH_NO_WORKERS: &str = "the plaintext deployment starts with no workers";
    /// The state is empty before the build.
    pub const STATE_IDLE: &str = "the state reports no instances and no workers before the build";
    /// The state shows the scale-out.
    pub const STATE_SCALED_OUT: &str = "the state reports an instance and a worker after the build";
    /// The stream shows the drain.
    pub const DRAIN_STREAMED: &str = "the state stream shows the drained fleet and its termination";
    /// The state is empty once the workers drained.
    pub const STATE_DRAINED: &str =
        "the state returns to no instances and no workers after the drain";
    /// The count of completed actions rose.
    pub const COMPLETED_INCREASES: &str = "the completed count increases after the build";
    /// An anonymous caller is the administrator.
    pub const ADMIN_ADMITTED: &str = "an anonymous administrator is admitted to the admin routes";
}

/// Names of the checks of the setup scenario.
pub mod setup {
    /// The noninteractive run fails while the conflict remains.
    pub const YES_EXITS_WITH_CONFLICT: &str = "gang setup --yes exits 1 with the home conflict";
    /// The noninteractive run emits a failure record.
    pub const YES_REPORTS_FAILURE: &str = "gang setup --yes prints a fail record";
    /// Interactive setup succeeds after accepting the edit.
    pub const INTERACTIVE_SUCCEEDS: &str = "interactive gang setup exits 0";
    /// Interactive setup applies the expected home rc edit.
    pub const INTERACTIVE_EDITS_HOME_RC: &str =
        "interactive gang setup applies the home rc edit byte for byte";
    /// Interactive setup saves exactly one backup of the original home rc.
    pub const INTERACTIVE_BACKS_UP_HOME_RC: &str =
        "interactive gang setup saves one exact backup of the home rc";
    /// A subsequent noninteractive run succeeds.
    pub const YES_SUCCEEDS_AFTER_EDIT: &str = "gang setup --yes exits 0 after the edit";
    /// Setup reuses the previously stored workload login.
    pub const REUSES_SIGN_IN: &str = "gang setup reuses the workload sign-in";
    /// Setup writes the server's exact CA certificate.
    pub const WRITES_SERVER_CA: &str = "gang setup writes the server CA to .gang/ca.pem";
    /// The generated rc names the workspace CA certificate.
    pub const CONFIGURES_TLS_CERTIFICATE: &str = "gang setup configures the TLS certificate";
    /// The generated rc names the absolute gang credential helper.
    pub const CONFIGURES_CREDENTIAL_HELPER: &str =
        "gang setup configures the absolute credential helper";
    /// Setup identifies the conflicting home rc line.
    pub const REPORTS_THE_CONFLICTING_LINE: &str =
        "gang setup names the home rc line that overrides Gang's cache";
    /// Setup prints the minimal edit.
    pub const REPORTS_THE_MINIMAL_EDIT: &str =
        "gang setup prints the edit that moves the line into build:not-gang";
    /// Setup reports the credential header leak.
    pub const REPORTS_THE_HEADER_LEAK: &str =
        "gang setup reports the home rc header that would be sent to Gang";
    /// Setup writes the workspace files.
    pub const WRITES_THE_WORKSPACE_FILES: &str =
        "gang setup writes .gang/bazelrc and the try-import line";
    /// Setup does not modify the home rc.
    pub const LEAVES_THE_HOME_RC_UNCHANGED: &str =
        "gang setup --yes leaves the home rc byte for byte";
    /// The build runs remotely after the edit.
    pub const BUILDS_REMOTELY_AFTER_THE_EDIT: &str =
        "after the edit, a build through the workspace rc runs remotely";
}

/// Names of the checks of the dashboard scenario.
pub mod dashboard {
    /// The route refuses a request that carries no session cookie.
    #[must_use]
    pub fn cookieless_refused(path: &str) -> String {
        format!("a request for {path} without a session cookie answers 401")
    }

    /// The session cookie opens the stream.
    pub const STREAM_OPENED: &str =
        "a session cookie opens the state stream as a 200 text/event-stream";
    /// Revoking the principal closes the stream.
    pub const CLOSED_ON_REVOCATION: &str =
        "revoking the principal ends the open stream with a closed event: revoked";
}

/// Names of the checks of the sessions scenario (ADR-39).
pub mod sessions {
    /// The first sign-in asks for offline access.
    pub const OFFLINE_SIGN_IN: &str =
        "the first sign-in asks the provider for offline access with consent";
    /// The renewal cookie is set with its security attributes.
    pub const RENEWAL_COOKIE_SET: &str =
        "the offline sign-in sets an HttpOnly, SameSite=Strict renewal cookie for 400 days";
    /// A cross-site renewal is refused.
    pub const CROSS_SITE_RENEWAL_REFUSED: &str =
        "a cross-site renewal answers 403 and touches no cookie";
    /// A renewal without its cookie is refused.
    pub const RENEWAL_NEEDS_COOKIE: &str = "a renewal without a renewal cookie answers 401";
    /// The sealed cookie is no credential.
    pub const SEALED_COOKIE_NOT_A_CREDENTIAL: &str = "the sealed renewal cookie is refused as a bearer token, as a session cookie and as a session:authenticate proof";
    /// Concurrent renewals share one refresh grant.
    pub const CONCURRENT_RENEWALS_SHARE_ONE_GRANT: &str =
        "two concurrent renewals of a rotating refresh token both renew through one refresh grant";
    /// Renewal across expiries.
    pub const RENEWS_ACROSS_EXPIRY: &str =
        "for two token lifetimes, each expired stream is renewed and reopened";
    /// Renewal is silent at the provider.
    pub const RENEWAL_SILENT: &str = "renewals reach the provider only as web-client refresh grants, never as an authorization request";
    /// The absolute lifetime ends renewal.
    pub const MAX_AGE_ENDS_RENEWAL: &str =
        "past the absolute lifetime a renewal answers 401 and keeps the renewal cookie";
    /// The next sign-in asks no consent.
    pub const SIGN_IN_WITHOUT_CONSENT: &str = "the sign-in after the absolute lifetime asks no offline access and re-checks the held refresh token with one grant";
    /// Revocation ends renewal.
    pub const REVOCATION_ENDS_RENEWAL: &str = "revoking the principal closes the stream and a renewal answers 403 clearing the renewal cookie";
    /// Logout redirects and clears both cookies.
    pub const LOGOUT_CLEARS_BOTH: &str = "logout redirects with 303 to /ui/ and clears the session and renewal cookies, and a renewal afterwards answers 401";
    /// A suspended account cannot renew or sign in.
    pub const SUSPENSION_ENDS_RENEWAL: &str = "a suspended account's renewal answers 401 clearing the cookie, and its sign-in fails at the provider";
    /// Every check, in the order the scenario emits them.
    pub const ALL: [&str; 13] = [
        OFFLINE_SIGN_IN,
        RENEWAL_COOKIE_SET,
        CROSS_SITE_RENEWAL_REFUSED,
        RENEWAL_NEEDS_COOKIE,
        SEALED_COOKIE_NOT_A_CREDENTIAL,
        CONCURRENT_RENEWALS_SHARE_ONE_GRANT,
        RENEWS_ACROSS_EXPIRY,
        RENEWAL_SILENT,
        MAX_AGE_ENDS_RENEWAL,
        SIGN_IN_WITHOUT_CONSENT,
        REVOCATION_ENDS_RENEWAL,
        LOGOUT_CLEARS_BOTH,
        SUSPENSION_ENDS_RENEWAL,
    ];
}

/// Names of the checks of the per-action limits scenario.
pub mod limits {
    /// The platform enforces limits with cgroups.
    pub const CGROUPS_ENFORCED: &str = "per-action limits are enforced by cgroups";
    /// The held action has a scope.
    pub const HOLD_HAS_SCOPE: &str = "the hold action runs in its own scope";
    /// The hold action signals that its orphan runs before the hold timeout.
    pub const HOLD_SIGNALLED: &str = "the hold action signals that its orphan runs";
    /// `memory.max` is unbounded in each action scope.
    pub const MEMORY_MAX: &str = "the action scope has no reservation-derived memory.max";
    /// The kernel kills the whole action on an out-of-memory event.
    pub const OOM_GROUP: &str = "the kernel kills the whole action on an out-of-memory event";
    /// `cpu.max` is the count times the period.
    pub const CPU_MAX: &str = "cpu.max is the cpu count times the period";
    /// `pids.max` is the configured limit.
    pub const PIDS_MAX: &str = "pids.max is the configured limit";
    /// The orphan process lives while the action does.
    pub const ORPHAN_RUNS: &str = "the action's orphan process runs while the action does";
    /// The held action completes when released.
    pub const HOLD_COMPLETES: &str = "the held action completes once released";
    /// The orphan process dies with the action.
    pub const ORPHAN_KILLED: &str =
        "a process left behind by the action is killed when the action ends";
    /// The scope directory is removed with the action.
    pub const SCOPE_REMOVED: &str = "the action's scope is removed afterwards";
    /// A healthy spike above its reservation completes on the first attempt.
    pub const HARD_CEILING: &str = "a healthy memory spike completes on its first attempt";
}

/// Names of the checks of the soft memory scenario.
pub mod memory {
    /// The platform supports cgroup action scopes.
    pub const CGROUPS_ENFORCED: &str = "action scopes are available with cgroups";
    /// An action over its soft reservation is requeued once and completes.
    pub const SOFT_REQUEUE: &str =
        "an action over its soft memory reservation is killed once, requeued, and completes";
    /// The worker serves again after a kill for memory.
    pub const SERVING_AFTER_KILL: &str =
        "the worker keeps serving after an action was killed for memory";
}

/// Names of the checks of the disk scenario.
pub mod disk {
    /// The hog is killed at its reservation, where the sampler exists.
    pub const HOG_KILLED: &str = "the disk hog is killed at its disk reservation";
    /// An action past its disk reservation fails with the worker's message.
    pub const KILLED_WITH_MESSAGE: &str =
        "an action exceeding its disk reservation is killed and fails with the worker's message";
    /// The hog never reached the end of its script.
    pub const HOG_WROTE_NO_REPORT: &str = "the disk hog never reached its report";
    /// One scale-out served both actions.
    pub const ONE_WORKER: &str = "the hog and its neighbour were served by one worker";
    /// The neighbour finished intact.
    pub const NEIGHBOUR_FINISHED: &str =
        "the disk neighbour finished its writes and read them back";
    /// The worker serves again after the kill.
    pub const SERVING_AFTER_KILL: &str = "the worker keeps serving after the disk hog was killed";
}

/// Names of the checks of the fleets scenario. A step is named by the request its actions make.
pub mod fleets {
    /// No worker runs before the first build.
    pub const START_AT_ZERO: &str = "the fleets start at zero workers";
    /// A step whose actions set no exec property but the ISA.
    pub const UNCONSTRAINED: &str = "an unconstrained action";
    /// A step whose action names the large fleet.
    pub const PINNED: &str = "an action pinned to the large fleet";
    /// A step whose action asks for more memory than the first fleet has.
    pub const MEMORY: &str = "an action requesting more memory than the first fleet has";
    /// A step whose action asks for the noop sandbox.
    pub const NOOP_SANDBOX: &str = "an action requesting the noop sandbox";
    /// A cold pair whose CPU reservations require overflow capacity.
    pub const OVERFLOW: &str = "a resource-overflow cold pair of unconstrained actions";

    /// The scale-outs of `request` are exactly those of `fleets`, in order.
    #[must_use]
    pub fn placed(request: &str, fleets: &[&str]) -> String {
        format!("{request} scales out exactly {}", fleets.join(", then "))
    }

    /// The fleets drain to zero workers after `request`.
    #[must_use]
    pub fn drained(request: &str) -> String {
        format!("every fleet drains to zero workers after {request}")
    }

    /// Every worker drained after `request` is recorded as terminated.
    #[must_use]
    pub fn terminated(request: &str) -> String {
        format!("every drained worker is recorded as terminated after {request}")
    }

    /// The check `name` as judged for the step of `request`.
    #[must_use]
    pub fn labelled(request: &str, name: &str) -> String {
        format!("{request}: {name}")
    }
}

/// Names of the checks of the suite itself.
pub mod suite {
    /// The in-process issuer started.
    pub const ISSUER_STARTS: &str = "the in-process issuer starts";

    /// The scenario `scenario` ran to completion.
    #[must_use]
    pub fn scenario_completes(scenario: impl std::fmt::Display) -> String {
        format!("scenario {scenario} completes")
    }
}

/// Names of the checks of the identity scenario.
pub mod identity {
    /// B15: a login with a wrong CA pin fails with the trust fix.
    pub const WRONG_PIN_REFUSED: &str =
        "a login with a wrong --ca-pin cannot reach the server and names the --ca-pin fix";
    /// B15: a failed login leaves nothing in the client home.
    pub const WRONG_PIN_STORES_NOTHING: &str = "a login with a wrong --ca-pin stores nothing";
    /// B15: the client accepts an `http://` server URL.
    pub const PLAIN_SCHEME_ACCEPTED: &str =
        "an http:// server URL is dialled, not refused for its scheme";
    /// B15: the client refuses an `ftp://` server URL.
    pub const UNSUPPORTED_SCHEME_REFUSED: &str =
        "an ftp:// server URL is refused with the http-or-https fix";
    /// B14: a principal without a role cannot revoke a principal.
    pub const MEMBER_REVOKE_REFUSED: &str =
        "gang revoke --principal by a principal without the permission fails with 403";
    /// B14: a principal without a role cannot restore a principal.
    pub const MEMBER_RESTORE_REFUSED: &str =
        "gang restore --principal by a principal without the permission fails with 403";
    /// B14: a principal without a role cannot revoke a certificate.
    pub const MEMBER_SERIAL_REFUSED: &str =
        "gang revoke --serial by a principal without the permission fails with 403";
    /// B14: the API names the missing permission.
    pub const MEMBER_API_REFUSED: &str = "the API refuses a revocation without the permission as MISSING_PERMISSION principals.revoke";
    /// B16: the control: a current token minted with the server's key is accepted.
    pub const MINTED_BEARER_ACCEPTED: &str =
        "a current bearer token signed with the server's key is accepted";
    /// B16: an expired bearer token is refused.
    pub const EXPIRED_BEARER_REFUSED: &str =
        "an expired bearer token is refused as CREDENTIALS_INVALID";
    /// B19: the control: a session cookie is accepted on a cookie route.
    pub const SESSION_COOKIE_ACCEPTED: &str = "a session cookie is accepted on a cookie route";
    /// B19: a session token is no bearer token.
    pub const SESSION_AS_BEARER_REFUSED: &str =
        "a session token sent as a bearer token is refused as CREDENTIALS_INVALID";
    /// B19: a bearer token is no session cookie.
    pub const BEARER_AS_COOKIE_REFUSED: &str = "a bearer token sent as the session cookie is ignored, so the request is CREDENTIALS_MISSING";
    /// B18: the sign-in starts a PKCE authorization request.
    pub const SIGN_IN_REDIRECTS: &str =
        "the dashboard sign-in redirects to the provider with a PKCE S256 authorization request";
    /// B18: the sign-in binds its state to a login cookie.
    pub const LOGIN_COOKIE_SET: &str =
        "the sign-in sets an HttpOnly SameSite=Lax login cookie holding the state for 600 s";
    /// B18: a callback whose state is not the cookie's is refused.
    pub const STATE_MISMATCH_REFUSED: &str =
        "a callback whose state differs from the login cookie ends at /ui/error?reason=failed";
    /// B18: a valid callback signs the browser in.
    pub const SIGN_IN_COMPLETES: &str = "the callback sets an HttpOnly SameSite=Strict session cookie, clears the login cookie and returns to next";
    /// B18: the session the sign-in set is accepted.
    pub const SIGNED_IN_SESSION_ACCEPTED: &str = "the session cookie of the sign-in is accepted";
    /// B18: a `next` outside /ui is not followed.
    pub const UNSAFE_NEXT_DROPPED: &str = "a sign-in whose next is outside /ui returns to /ui";
    /// B18: a cross-site logout is refused.
    pub const CROSS_SITE_LOGOUT_REFUSED: &str =
        "a cross-site logout is refused with 403 and clears no cookie";
    /// B18: a same-origin logout redirects and clears the session.
    pub const LOGOUT_CLEARS_SESSION: &str = "a same-origin logout redirects with 303 to /ui/ and clears the session cookie with Max-Age=0";
    /// B20: the proof command runs for the login.
    pub const COMMAND_LOGIN: &str = "gang login --token-command runs the command once";
    /// B20: a fresh token is reused.
    pub const FRESH_TOKEN_REUSED: &str =
        "gang get reuses a fresh token without running the proof command";
    /// B20: a new token runs the command again.
    pub const COMMAND_RERUN: &str =
        "gang get runs the proof command again for a new token once the stored one expired";
    /// B21: the interactive login completes.
    pub const INTERACTIVE_LOGIN: &str =
        "the interactive gang login completes through the provider's redirect";
    /// B21: an expired login is renewed with the refresh token.
    pub const EXPIRED_LOGIN_RENEWED: &str =
        "gang get renews an expired login with one refresh grant and a new accepted token";
    /// B21: a refused renewal asks for a new login.
    pub const RENEWAL_REJECTED: &str =
        "gang get whose refresh token the provider rejects asks to run gang login again";
    /// B21: the administrator revokes the signed-in principal.
    pub const OPERATOR_REVOKED: &str = "the administrator revokes the signed-in principal";
    /// B21: a revoked principal's renewal is tried once, then the 403 is reported.
    pub const REVOKED_LOGIN_REFUSED: &str =
        "gang get of a revoked principal renews once, then reports the server's 403";
    /// B22: the doctor's credentials row passes before the revocation.
    pub const DOCTOR_CREDENTIALS_OK: &str = "gang doctor finds the credentials good";
    /// B22: the administrator revokes the member.
    pub const MEMBER_REVOKED: &str = "the administrator revokes the member";
    /// B22: the doctor's credentials row fails after the revocation.
    pub const DOCTOR_CREDENTIALS_REFUSED: &str =
        "gang doctor fails the credentials row of a revoked principal with its fix and exits 1";
    /// B17: the worker listener requires a client certificate.
    pub const WORKER_LISTENER_NEEDS_CERTIFICATE: &str =
        "the worker listener refuses a connection without a client certificate";
    /// B17: the worker listener admits the worker's certificate.
    pub const WORKER_CERTIFICATE_ADMITTED: &str =
        "the worker listener admits the running worker's certificate";
    /// B17: the administrator revokes the worker's certificate.
    pub const SERIAL_REVOKED: &str = "the administrator revokes the worker certificate by serial";
    /// B17: an open connection with the revoked certificate is closed.
    pub const OPEN_WORKER_CONNECTION_CUT: &str =
        "an open connection with the revoked certificate is closed";
    /// B17: a new connection with the revoked certificate is refused.
    pub const REVOKED_CERTIFICATE_REFUSED: &str =
        "a new connection with the revoked certificate is refused";
    /// B17: the worker logs the server refusing its revoked certificate after its stream ended.
    pub const WORKER_STREAM_ENDS_REVOKED: &str =
        "the worker's log shows the server refusing its revoked certificate after its stream ended";
    /// B13: the server restarted.
    pub const SERVER_RESTARTED: &str = "the server process restarted";
    /// B13: a revocation survives the restart.
    pub const REVOCATION_SURVIVES_RESTART: &str =
        "a principal revoked before the restart is refused after it as PRINCIPAL_REVOKED";
    /// B13: the restarted server serves.
    pub const RESTARTED_SERVER_SERVES: &str = "the restarted server serves an unrevoked principal";

    /// ADR-37: a headless login prompts for a device approval only.
    pub const DEVICE_LOGIN_PROMPTED: &str = "a headless gang login prints the verification URI and a code, and starts no browser sign-in";
    /// ADR-37: the approval page's resource shows the requester, never the secret.
    pub const DEVICE_SHOWS_CLIENT_ADDRESS: &str =
        "the device authorization shows the CLI's address and never its secret";
    /// ADR-37: approval answers the provider URL and goes through the callback to the approved page.
    pub const DEVICE_APPROVAL_REDIRECTS: &str = "a same-origin approval answers the provider URL and goes through the provider and the callback to /ui/device/approved without a session cookie";
    /// ADR-38/39: device approvals still ask for offline access.
    pub const DEVICE_APPROVAL_OFFLINE: &str =
        "the device approval asks the provider for offline access with consent";
    /// ADR-37: the login completes by itself.
    pub const DEVICE_LOGIN_COMPLETES: &str = "the device login ends by itself, logged in with the provider, storing a device session without an ID token";
    /// ADR-37: renewal through the web client.
    pub const DEVICE_LOGIN_RENEWS: &str = "an expired device session renews through one web-client refresh grant, storing the rotated refresh token before the token";
    /// ADR-37: revocation applies to device sessions.
    pub const DEVICE_LOGIN_REVOKED: &str = "a revoked approver's device session is refused with principal revoked, and works again once restored";
    /// ADR-37 simulated flaw: a provider that returns no refresh token.
    pub const DEVICE_WITHOUT_REFRESH_TOKEN_FAILS: &str = "an approval without a refresh token fails, the code stays pending, and the login exits 1 storing nothing, with a server warning";
    /// ADR-37 attacker: cross-site approval.
    pub const CROSS_SITE_DEVICE_APPROVAL_REFUSED: &str = "a cross-site device approval answers 403";
    /// ADR-37 attacker: wrong secret.
    pub const WRONG_SECRET_REDEEM_REFUSED: &str =
        "a redeem with a wrong secret answers 404 DEVICE_AUTHORIZATION_NOT_FOUND";
    /// ADR-37 attacker: revoked between approval and redeem.
    pub const REVOKED_BEFORE_REDEEM_REFUSED: &str = "revoking the approver between approval and redeem makes the redeem answer 403 PRINCIPAL_REVOKED";
    /// ADR-37 attacker: a second redeem.
    pub const SECOND_REDEEM_REFUSED: &str =
        "a second redeem answers 404 DEVICE_AUTHORIZATION_NOT_FOUND";
    /// ADR-37: a real browser approves under the dashboard's own headers.
    pub const DEVICE_APPROVED_IN_BROWSER: &str = "a headless browser types the code at /ui/device, approves, and reaches the provider and /ui/device/approved under the dashboard's Content-Security-Policy";
    /// The device-login checks, in the order the identity scenario emits them.
    pub const DEVICE: [&str; 13] = [
        DEVICE_LOGIN_PROMPTED,
        DEVICE_SHOWS_CLIENT_ADDRESS,
        DEVICE_APPROVAL_REDIRECTS,
        DEVICE_APPROVAL_OFFLINE,
        DEVICE_LOGIN_COMPLETES,
        DEVICE_LOGIN_RENEWS,
        DEVICE_LOGIN_REVOKED,
        DEVICE_WITHOUT_REFRESH_TOKEN_FAILS,
        CROSS_SITE_DEVICE_APPROVAL_REFUSED,
        WRONG_SECRET_REDEEM_REFUSED,
        REVOKED_BEFORE_REDEEM_REFUSED,
        SECOND_REDEEM_REFUSED,
        DEVICE_APPROVED_IN_BROWSER,
    ];
}

/// Names of the checks of the action timeout scenarios, under bwrap and under noop.
pub mod action_timeout {
    /// The timed-out action's background child starts.
    pub const CHILD_STARTED: &str = "the timed-out action's background child starts";
    /// An action that outlives `action_timeout` fails the build.
    pub const TIMEOUT_FAILS: &str = "an action that outlives action_timeout fails the build";
    /// The background child of a timed-out action is gone once the build ends.
    pub const CHILD_GONE: &str =
        "the background child of a timed-out action is gone once the build ends";
    /// A timed-out action receives SIGTERM and runs its trap.
    pub const TRAP_RAN: &str = "a timed-out action receives SIGTERM and runs its trap";
    /// Gang exec keeps running while the timed-out action runs its trap.
    pub const EXEC_WAITS: &str = "gang exec keeps running while the timed-out action runs its trap";
    /// A timed-out action that exits within `kill_grace` reaches its own exit.
    pub const TRAP_EXITS_CLEANLY: &str =
        "a timed-out action that exits within kill_grace reaches its own exit";
    /// The timed-out action's scope is removed afterwards.
    pub const SCOPE_REMOVED: &str = "the timed-out action's scope is removed afterwards";
    /// No per-action temporary directory is left behind.
    pub const TEMP_CLEAN: &str = "no per-action temporary directory is left behind";
    /// An action that ignores SIGTERM keeps running into `kill_grace`.
    pub const IGNORER_SURVIVES_GRACE: &str =
        "an action that ignores SIGTERM keeps running into kill_grace";
    /// An action that ignores SIGTERM is killed once `kill_grace` ends.
    pub const IGNORER_KILLED: &str =
        "an action that ignores SIGTERM is killed once kill_grace ends";
}

/// Names of the checks of the exit status of an action killed by a signal.
pub mod exit_signal {
    /// An action killed by SIGKILL fails with exit status 137.
    pub const SIGKILL: &str = "an action killed by SIGKILL fails with exit status 137";
    /// An action killed by SIGTERM fails with exit status 143.
    pub const SIGTERM: &str = "an action killed by SIGTERM fails with exit status 143";
}

/// Names of the checks of the bwrap sandbox contract scenario.
pub mod sandbox_contract {
    /// No variable of the gang worker's own environment reaches a bwrap action.
    pub const WORKER_ENV_HIDDEN: &str =
        "no variable of the gang worker's own environment reaches a bwrap action";
    /// /tmp in a bwrap action is a fresh, empty tmpfs.
    pub const TMP_FRESH: &str = "/tmp in a bwrap action is a fresh, empty tmpfs";
    /// The root, /usr and /etc mounts are read-only inside a bwrap action.
    pub const READONLY_BINDS: &str =
        "the root, /usr and /etc mounts are read-only inside a bwrap action";
    /// The working directory of a bwrap action is writable.
    pub const WORKDIR_WRITABLE: &str = "the working directory of a bwrap action is writable";
    /// The numeric action identity resolves uniquely to builder.
    pub const IDENTITY: &str = "the action identity resolves uniquely to builder";
    /// User and group lookup use files; host lookup uses files then DNS.
    pub const NSS: &str = "the action uses files for identity and files then DNS for hosts";
    /// No other host configuration is visible under /etc.
    pub const ETC_CONTENTS: &str =
        "the action sees only identity, resolver and public roots under /etc";
    /// Identity, resolver and public root files are read-only.
    pub const IDENTITY_ROOTS_READONLY: &str =
        "identity, resolver and public root files are read-only";
    /// Native TLS loads the public system roots.
    pub const ROOTS: &str = "native TLS loads public system roots";
    /// Native TLS respects explicit missing certificate overrides.
    pub const ROOTS_OVERRIDE: &str = "native TLS honors explicit certificate overrides";
    /// `PostgreSQL` initializes and starts under the action identity.
    pub const POSTGRES: &str = "PostgreSQL initializes and starts under the action identity";
    /// The held bwrap action runs in its own scope.
    pub const HOLD_HAS_SCOPE: &str = "the held bwrap action runs in its own scope";
    /// Bwrap runs inside the action's scope.
    pub const BWRAP_IN_SCOPE: &str = "bwrap runs inside the action's scope";
    /// Gang exec stays in the worker's leaf, outside the action's scope.
    pub const EXEC_OUTSIDE_SCOPE: &str =
        "gang exec stays in the worker's leaf, outside the action's scope";
    /// The held bwrap action completes once released.
    pub const HOLD_COMPLETES: &str = "the held bwrap action completes once released";
}

/// Names of the held memory action's cleanup checks.
pub mod oom {
    /// The held memory action completes.
    pub const KILLED: &str = "the held memory action completes";
    /// Gang exec removes the action scope.
    pub const SCOPE_REMOVED: &str = "gang exec removes the held memory action's scope";
    /// The next action succeeds.
    pub const SERVING: &str = "the next action succeeds after the held memory action";
}

/// Names of the checks of the stale scope scenario.
pub mod stale_scope {
    /// The held action's stray process starts in the action's scope.
    pub const STRAY_STARTED: &str = "the held action's stray process starts in the action's scope";
    /// A killed `gang exec` has its scope reaped by its guard.
    pub const GUARD_REAPS_SCOPE: &str = "a SIGKILLed gang exec's scope is reaped by its guard";
    /// A killed `gang exec` whose guard is gone leaves its scope behind.
    pub const SCOPE_OUTLIVES_EXEC: &str =
        "a SIGKILLed gang exec whose guard is gone leaves its action's scope behind";
    /// The next action of the same worker removes a stale scope and kills its stray.
    pub const SWEPT_BY_NEXT_ACTION: &str =
        "the next action of the same worker removes a stale scope and kills its stray";
    /// A dead worker's stale scope and stray are gone before its replacement serves; which
    /// component removes them is not part of the promise.
    pub const GONE_BEFORE_REPLACEMENT_SERVES: &str =
        "a dead worker's stale scope and stray are gone before its replacement serves";
}

/// Names of the checks of the memory escalation scenario.
pub mod escalation {
    /// An action still over its memory after `escalation_max_steps` escalations fails the build.
    pub const FAILS: &str =
        "an action still over its memory after escalation_max_steps escalations fails the build";
    /// The action runs `escalation_max_steps` + 1 times and is not requeued again.
    pub const ATTEMPTS: &str =
        "the action runs escalation_max_steps + 1 times and is not requeued again";
}

/// Names of the checks of the unplaceable disk scenario.
pub mod disk_guard {
    /// An unplaceable action fails the build.
    pub const REFUSED: &str = "an action whose disk no fleet accepts fails the build";
    /// The refusal names the rejected platform property.
    pub const NAMES_NO_FLEET: &str =
        "the build says no fleet accepts the action and names its disk_kb";
    /// The unplaceable action never runs.
    pub const NEVER_RAN: &str = "the action no fleet accepts never runs";
    /// The worker keeps serving.
    pub const SERVING: &str = "the worker keeps serving after an action no fleet accepts";
}

/// Names of the checks of the oversized fleet disk scenario.
pub mod oversized_fleet {
    /// The worker refuses its fleet and reports both sizes.
    pub const WORKER_REFUSES: &str = "a worker whose scratch volume cannot hold its fleet's disk_per_instance exits before enrolling and names both sizes";
    /// No worker of the oversized fleet enrolls.
    pub const NO_WORKER_ENROLLS: &str =
        "no worker of a fleet whose disk_per_instance its scratch cannot hold enrolls";
}

/// Names of the checks of the misfit cold start scenario.
pub mod cold_start {
    /// Gang server stops before listening when no fleet accepts the cold start.
    pub const REFUSED: &str =
        "gang server stops before listening when no fleet accepts the cold start";
    /// Gang server exits with status 1 when no fleet accepts the cold start.
    pub const EXIT_CODE: &str =
        "gang server exits with status 1 when no fleet accepts the cold start";
    /// Gang server names the cold start no fleet accepts.
    pub const MESSAGE: &str = "gang server names the cold start no fleet accepts";
}

/// Names of the checks of the spot preemption scenario.
pub mod preemption {
    /// The held action starts on a Local Mode instance.
    pub const HELD: &str = "the held action starts on a Local Mode instance";
    /// A preempted worker exits while its action still runs.
    pub const WORKER_EXITS: &str = "a preempted worker exits while its action still runs";
    /// The action of a preempted worker is requeued on another worker.
    pub const REQUEUED: &str = "the action of a preempted worker is requeued on another worker";
    /// A build whose worker was preempted succeeds.
    pub const BUILD_SUCCEEDS: &str = "a build whose worker was preempted succeeds";
}

/// Names of the checks of the worker certificate renewal scenario.
pub mod renewal {
    /// The worker writes a newer identity generation before its certificate expires.
    pub const RENEWED: &str =
        "the worker writes a newer identity generation before its certificate expires";
    /// The worker removes its previous identity generation once renewed.
    pub const PREVIOUS_REMOVED: &str =
        "the worker removes its previous identity generation once renewed";
    /// An action held across the renewal completes.
    pub const HELD_COMPLETES: &str = "an action held across the renewal completes";
    /// The renewed worker keeps serving.
    pub const SAME_WORKER: &str = "the renewed worker keeps serving";
}

/// Names of the checks of the orphan scenario.
pub mod orphan {
    /// An in-service instance whose worker is gone is listed with `pastBootGrace` before it is
    /// terminated.
    pub const FLAGGED_PAST_BOOT_GRACE: &str = "an in-service instance whose worker is gone is flagged pastBootGrace before its termination";

    /// An instance without a worker for its boot grace is terminated with cause `ORPHAN`.
    pub const TERMINATED_AS_ORPHAN: &str =
        "an instance without a worker for boot_grace is terminated with cause ORPHAN";

    /// An orphan is terminated exactly once.
    pub const TERMINATED_ONCE: &str = "an orphan is terminated exactly once";
}

/// Names of the checks of the scheduled-minimum scenario.
pub mod scheduled_minimum {
    /// A worker connects without a build.
    pub const WORKER_STANDS: &str = "a worker stands without a build";
    /// The fleet scales out for its minimum.
    pub const SCALED_OUT_FOR_MINIMUM: &str = "the fleet scales out for its scheduled minimum";
    /// The streamed policy reports the scheduled floor.
    pub const POLICY_MIN: &str = "the policy minimum reflects the scheduled minimum";
}

/// Names of the checks of the drain-cancel scenario.
pub mod drain_cancel {
    /// An action queued while the idle worker drains cancels the drain.
    pub const DRAIN_CANCELLED: &str =
        "an action queued during a drain cancels it: drain_started, then drain_cancelled";

    /// The worker whose drain was cancelled runs the queued action, without a scale-out.
    pub const RESUMED_WORKER_SERVES: &str =
        "the resumed worker runs the action that cancelled its drain, without a scale-out";

    /// An instance draining when the server restarts is terminated exactly once.
    pub const RESTART_TERMINATES_ONCE: &str =
        "an instance draining when the server restarts is terminated exactly once";

    /// A restarted server does not cancel a drain.
    pub const NO_CANCEL_AFTER_RESTART: &str =
        "a server restarted mid-drain records no drain_cancelled";

    /// Once the interrupted drain ended, no worker and no instance is left.
    pub const RESTART_LEAVES_NOTHING: &str =
        "after the restarted drain no worker, instance or worker process is left";
}

/// Names of the checks of the skipped-pass scenario.
pub mod skipped_pass {
    /// A pass whose provisioner supply read fails is skipped as `PROVISIONER_UNAVAILABLE`.
    pub const SKIPPED_FOR_PROVISIONER: &str =
        "a pass whose provisioner supply read fails is SKIPPED with PROVISIONER_UNAVAILABLE";

    /// Consecutive skipped passes count up by one.
    pub const SKIPS_COUNTED: &str = "consecutive skipped passes count up by one";

    /// No pass carries `lastOkTime` before a pass succeeded.
    pub const NO_LAST_OK_BEFORE_SUCCESS: &str =
        "a skipped pass carries no lastOkTime before any pass succeeded";

    /// A good pass carries no skip reason and no skip count.
    pub const OK_CLEARS_SKIP: &str = "a good pass carries no skipReason and no skipped count";

    /// The first skipped pass after a good one counts one.
    pub const SKIP_COUNT_RESETS: &str = "the first skipped pass after a good one counts one";

    /// A skipped pass after a good one carries `lastOkTime`.
    pub const LAST_OK_KEPT: &str = "a skipped pass after a good one carries lastOkTime";
}

/// Names of the checks of the cli scenario: the `gang` command line without a harness stack.
pub mod cli {
    /// B36: `gang revoke` with no target.
    pub const REVOKE_WITHOUT_TARGET: &str =
        "gang revoke without a target is a usage error (exit 2)";
    /// B36: an unknown subcommand.
    pub const UNKNOWN_SUBCOMMAND: &str = "an unknown subcommand is a usage error (exit 2)";
    /// B36: `gang doctor` with no server.
    pub const DOCTOR_WITHOUT_SERVER: &str =
        "gang doctor without a server fails (exit 1) with a gang doctor failure record";
    /// B36: an unparsable configuration.
    pub const UNPARSABLE_CONFIG: &str =
        "gang server refuses an unparsable configuration (exit 1) before announcing";
    /// B36: a public listen address without an endpoint.
    pub const PUBLIC_LISTEN_NEEDS_ENDPOINT: &str =
        "gang server refuses a public listen address without an endpoint before announcing";
    /// B39: `-q` and ok records.
    pub const QUIET_DROPS_OK: &str = "--quiet drops the ok records of gang local down";
    /// B39: `-q` and failure records.
    pub const QUIET_KEEPS_FAILURE: &str = "--quiet keeps the failure record of gang doctor";
    /// B39: piped output.
    pub const PIPED_PLAIN: &str = "piped output carries no ANSI escape";
    /// B39: `CLICOLOR_FORCE`.
    pub const CLICOLOR_FORCE_COLOURS: &str = "CLICOLOR_FORCE colours piped output";
    /// B39: `NO_COLOR` over `CLICOLOR_FORCE`.
    pub const NO_COLOR_WINS: &str = "NO_COLOR removes the colour CLICOLOR_FORCE adds";
    /// B35: the fields of a TLS `gang local up`.
    pub const UP_REPORTS: &str = "gang local up reports the public, worker, ca-pin and log fields";
    /// B35: the printed pin.
    pub const UP_PIN_ANNOUNCED: &str =
        "the ca-pin gang local up prints is the one gang server announced";
    /// B35: a second `gang local up`.
    pub const UP_TWICE_REFUSED: &str =
        "a second gang local up fails (exit 1) naming the held server.lock";
    /// B35: `gang local down`.
    pub const DOWN_STOPS: &str =
        "gang local down stops the server, then reports terminated 0 workers";
    /// B35: a repeated `gang local down`.
    pub const DOWN_AGAIN: &str =
        "a repeated gang local down reports no gang server was running, then terminated 0 workers";
    /// B35: the fields of `gang local up --insecure`.
    pub const INSECURE_UP_REPORTS: &str =
        "gang local up --insecure reports the ui field and no ca-pin";
    /// B35: the bazelrc of an insecure server.
    pub const INSECURE_BAZELRC_UNPINNED: &str =
        "the bazelrc of an --insecure server names its executor and no CA certificate";
    /// B35/B40: `gang local down` of the insecure server.
    pub const INSECURE_DOWN_TERMINATES: &str =
        "gang local down terminates the worker of the --insecure server";
    /// B38: a mismatched executor.
    pub const DOCTOR_MISMATCH_FAILS: &str =
        "gang doctor fails (exit 1) on a remote_executor that is not the server's";
    /// B38: the matching executor.
    pub const DOCTOR_MATCH_PASSES: &str =
        "gang doctor passes (exit 0) with the remote_executor the bazelrc names";
    /// B40: the launched worker connects.
    pub const WORKER_CONNECTS: &str =
        "a worker launched without configured endpoints connects through its endpoint flags";
    /// B40: the flags the server launches workers with.
    pub const WORKER_FLAGS: &str =
        "workers are launched with --endpoint and --worker-endpoint set to the reported endpoints";
    /// B40: mTLS without an endpoint.
    pub const WORKER_NEEDS_ENDPOINT: &str =
        "gang worker under mtls without an endpoint fails (exit 1) naming the missing endpoint";
    /// B40: `--endpoint` validation.
    pub const ENDPOINT_FLAG_VALIDATED: &str =
        "gang worker --endpoint rejects a value that is not a URL (exit 2)";
    /// B40: `--worker-endpoint` validation.
    pub const WORKER_ENDPOINT_FLAG_VALIDATED: &str =
        "gang worker --worker-endpoint rejects a value that is not a URL (exit 2)";
}

/// Names of the checks of the checked-in-configs scenario: every `configs/*.toml` runs.
pub mod checked_in_configs {
    /// Some checked-in configuration runs locally.
    pub const FOUND: &str = "the checked-in configurations include one that runs locally";

    /// The checked-in configuration `name` starts `gang server`, which announces itself.
    #[must_use]
    pub fn starts(name: &str) -> String {
        format!("the checked-in {name} starts gang server")
    }

    /// The checked-in AWS configuration `name` passes configuration loading.
    #[must_use]
    pub fn loads(name: &str) -> String {
        format!("the checked-in {name} passes configuration loading and stops only for want of AWS")
    }
}

/// Names of the checks of the api scenario.
pub mod api {
    /// B1: an unauthenticated request is challenged.
    pub const UNAUTHENTICATED_CHALLENGED: &str = "an unauthenticated GET /v1/principals answers 401 CREDENTIALS_MISSING with WWW-Authenticate: Bearer";
    /// B2: an authenticated request for an unknown route is not found.
    pub const UNKNOWN_ROUTE_NOT_FOUND: &str =
        "an authenticated DELETE /v1/state answers 404 ROUTE_NOT_FOUND naming the method and path";
    /// B2: an unauthenticated request for an unknown route is challenged first.
    pub const UNKNOWN_ROUTE_NEEDS_CREDENTIALS: &str =
        "an unauthenticated DELETE /v1/state answers 401 CREDENTIALS_MISSING";
    /// B3: a page size above the maximum is clamped.
    pub const PAGE_SIZE_CLAMPED: &str =
        "a pageSize above the maximum lists /v1/principals in one page instead of being refused";
    /// B3: walking the principals page by page lists each once.
    pub const PAGE_WALK_COMPLETE: &str = "walking /v1/principals one page at a time lists every principal of the full list once, in order";
    /// B3: the last page of the principals carries no token.
    pub const LAST_PAGE_OMITS_TOKEN: &str =
        "the last page of /v1/principals carries no nextPageToken";
    /// B3: a tampered page token is refused.
    pub const TAMPERED_TOKEN_REFUSED: &str = "a tampered page token answers 400 INVALID_PAGE_TOKEN";
    /// B3: a page token minted for another filter is refused.
    pub const FOREIGN_FILTER_TOKEN_REFUSED: &str =
        "a page token used with another filter answers 400 INVALID_PAGE_TOKEN";
    /// B5: every refusal is the error envelope.
    pub const ENVELOPE_ON_EVERY_REFUSAL: &str =
        "every non-2xx /v1 answer is a google.rpc error envelope with one gang ErrorInfo";
    /// B7: the dashboard page carries the security headers.
    pub const UI_SECURITY_HEADERS: &str =
        "GET /ui/ answers 200 with every dashboard security header";
    /// B7: a hashed asset is served brotli-encoded.
    pub const UI_ASSET_BROTLI: &str =
        "a hashed dashboard asset is served brotli-encoded to a client that accepts br";
    /// B7: a hashed asset is immutable.
    pub const UI_ASSET_IMMUTABLE: &str =
        "a hashed dashboard asset is cacheable for a year as immutable";
    /// B7: a hashed asset revalidates by its `ETag`.
    pub const UI_ASSET_REVALIDATES: &str =
        "a hashed dashboard asset answers an empty 304 to If-None-Match with its ETag";
    /// B10: a cookie-only revoke is refused.
    pub const COOKIE_POST_REFUSED: &str =
        "a revoke that presents only the session cookie answers 401 CREDENTIALS_MISSING";
    /// B10: a cookie-only revoke revokes nothing.
    pub const COOKIE_POST_REVOKES_NOTHING: &str =
        "the principal named by a cookie-only revoke is still admitted";
    /// B11: a cross-origin logout is refused.
    pub const CROSS_ORIGIN_LOGOUT_REFUSED: &str =
        "a cross-origin POST /ui/logout answers an empty 403 and clears no cookie";
    /// B9: shutdown closes an open stream.
    pub const SHUTDOWN_CLOSES_STREAM: &str =
        "SIGTERM ends an open state stream with a closed event: SHUTDOWN";
    /// B9: shutdown exits cleanly.
    pub const SHUTDOWN_EXITS_ZERO: &str = "the server exits 0 after SIGTERM";
}

/// Names of the checks of the request-limits scenario.
pub mod request_limits {
    /// B6: an oversized body is refused.
    pub const REQUEST_TOO_LARGE: &str =
        "a body over max_request_size answers 400 REQUEST_TOO_LARGE";
    /// B6: the burst is admitted.
    pub const BURST_ADMITTED: &str = "a caller's first burst of open requests is admitted";
    /// B6: the open API refuses the request after the burst.
    pub const API_RATE_LIMITED: &str = "the GET /v1/server after the burst answers 429 RATE_LIMITED with a whole-second Retry-After";
    /// B6: the sign-in refuses the request after the burst with an empty body.
    pub const UI_RATE_LIMITED: &str =
        "the GET /ui/login after the burst answers an empty 429 with a whole-second Retry-After";
}

/// Names of the checks of the viewer-cap scenario.
pub mod viewer_cap {
    /// B8: the first viewer is admitted.
    pub const FIRST_VIEWER_ADMITTED: &str = "the first viewer's state stream opens with a snapshot";
    /// B8: the viewer over the cap is turned away.
    pub const VIEWER_CAP_CLOSED: &str = "a viewer over max_viewers gets a closed event: VIEWER_CAP with the configured retryDelay first";
    /// B4: the SSE retry line matches the retry delay.
    pub const RETRY_LINE_MATCHES: &str =
        "the closed event's SSE retry: line is its retryDelay in milliseconds";
}

/// Names of the checks of the certificate-rotation scenario.
pub mod certificate_rotation {
    /// B12: the first leaf verifies against the pin.
    pub const FIRST_LEAF_PINNED: &str = "the listener's leaf verifies against the CA pin";
    /// B12: the leaf lives for the configured ttl.
    pub const LEAF_LIFETIME: &str = "the listener's leaf is valid for certificate_ttl";
    /// B12: the reissued leaf verifies against the pin.
    pub const SECOND_LEAF_PINNED: &str = "the reissued leaf verifies against the CA pin";
    /// B12: the leaf is reissued.
    pub const LEAF_REISSUED: &str =
        "a connection after reissue_at_percent of the ttl is shown a leaf with a new serial";
    /// B12: an established connection survives the reissue.
    pub const ESTABLISHED_KEPT: &str =
        "a connection established before the reissue still answers a ping";
}

/// Names of the event-paging checks that run after the lifecycle scenario.
pub mod event_paging {
    /// B3: walking the events page by page lists each once.
    pub const WALK_COMPLETE: &str =
        "walking /v1/events one page at a time lists every event of the full list once, in order";
    /// B3: the last page of the events carries no token.
    pub const LAST_PAGE_OMITS_TOKEN: &str = "the last page of /v1/events carries no nextPageToken";
}

/// Checks of controller loss during a build.
pub mod controller_loss {
    /// The held build succeeds.
    pub const BUILD: &str = "controller loss: build succeeds without failed actions";
    /// A leader becomes available.
    pub const TAKEOVER: &str = "controller loss: leader available within takeover bound";
    /// Retries stay within the number of executing actions.
    pub const REEXECUTIONS: &str = "controller loss: re-executions bounded by executing actions";
    /// No action overlaps across workers.
    pub const NO_OVERLAP: &str = "controller loss: no action overlaps on two workers";
    /// Work is both held and queued.
    pub const HELD: &str = "controller loss: actions execute while others are queued";
    /// The worker PID is not the action PID.
    pub const WORKER_PID: &str = "controller loss: worker pid differs from the action attempt";
    /// Two workers can execute concurrently.
    pub const TWO_WORKERS: &str = "controller loss: two workers can execute concurrently";
}

/// Checks of rolling controller replacement.
pub mod rolling_deploy {
    /// Work is both held and queued.
    pub const HELD: &str = "rolling deploy: actions execute while others are queued";
    /// The build succeeds.
    pub const BUILD: &str = "rolling deploy: build succeeds without failed actions";
    /// Each action starts exactly once.
    pub const ATTEMPTS: &str = "rolling deploy: every action executes exactly once";
    /// Both retired controllers exit.
    pub const RETIRED: &str = "rolling deploy: both retired controllers exit successfully";
    /// A leader is retired.
    pub const LEADER_RETIRED: &str = "rolling deploy: a leader is retired";
    /// A leader remains.
    pub const LEADER: &str = "rolling deploy: a leader exists after replacement";
}

/// Checks of login across controllers.
pub mod login_across {
    /// Device authorization crosses controllers.
    pub const DEVICE: &str = "login across: device login completes across controllers";
    /// A browser session survives leadership changing.
    pub const BROWSER: &str = "login across: browser session survives leader handoff";
    /// Device authorization cannot be redeemed twice.
    pub const REPLAY: &str = "login across: a redeemed device code cannot be redeemed again";
}

/// Checks of revocation across controllers.
pub mod cross_revocation {
    /// A credential is initially admitted.
    pub const INITIAL_LOGIN: &str = "cross-revocation: principal initially admitted on leader";
    /// The stream is initially open.
    pub const STREAM_OPEN: &str = "cross-revocation: principal stream initially open on leader";
    /// A new credential is refused.
    pub const LOGIN_REFUSED: &str = "cross-revocation: leader refuses a new credential";
    /// The leader stream closes.
    pub const STREAM_CLOSED: &str = "cross-revocation: leader closes the principal stream";
    /// The revoked worker is deregistered.
    pub const WORKER_CUT: &str = "cross-revocation: leader deregisters the revoked worker";
    /// Restoration permits a new credential.
    pub const RESTORED: &str = "cross-revocation: new leader admits the restored principal";
    /// An existing bearer is refused.
    pub const BEARER_REFUSED: &str =
        "cross-revocation: new leader refuses an existing bearer token";
    /// A second stream closes.
    pub const SECOND_STREAM_CLOSED: &str =
        "cross-revocation: leader closes the second principal stream";
}

/// Checks of leader failover.
pub mod leader_failover {
    /// Demand is queued with an existing worker.
    pub const DEMAND: &str = "leader failover: demand is queued while an old worker exists";
    /// Another controller takes over.
    pub const TAKEOVER: &str = "leader failover: another controller takes over";
    /// Bazel's executions reach the new leader while the old one is frozen.
    pub const ZOMBIE_REROUTED: &str = "leader failover: Bazel's executions reach the new leader while the former leader is frozen";
    /// Demand does not cause duplicate scale-outs.
    pub const SCALE_OUT: &str = "leader failover: demand does not cause duplicate scale-outs";
    /// Existing workers survive.
    pub const OLD_WORKERS: &str = "leader failover: existing instances are not terminated";
    /// The build succeeds.
    pub const BUILD: &str = "leader failover: build succeeds";
    /// A former leader adds no scaling events.
    pub const ZOMBIE_EVENTS: &str =
        "leader failover: resumed former leader adds no scaling or termination";
    /// A former leader creates no unaccounted instances.
    pub const ZOMBIE_RECORDS: &str =
        "leader failover: resumed former leader creates no unaccounted instances";
    /// A former leader exits after losing leadership.
    pub const ZOMBIE_EXIT: &str = "leader failover: former leader exits with lost leadership";
}

/// Checks of a shared database outage and partition.
pub mod database_outage {
    /// The build remains held.
    pub const HELD: &str = "database outage: existing build remains held";
    /// Device login is refused.
    pub const REFUSED: &str = "database outage: new device login is refused during the outage";
    /// Scaling pauses.
    pub const PAUSED: &str = "database outage: scaling pauses after lease validity lapses";
    /// Demand stays pending while readiness is lost.
    pub const DEMAND_PENDING: &str = "database outage: new demand remains pending until recovery";
    /// No scaling event occurs during the outage.
    pub const NO_EVENTS: &str = "database outage: no scaling event is recorded during the outage";
    /// No other controller becomes ready, and the original leader recovers.
    pub const LEADER: &str =
        "database outage: original leader becomes ready again without takeover";
    /// Login resumes.
    pub const LOGIN: &str = "database outage: new device login works after recovery";
    /// Retried demand is met.
    pub const RESUMED: &str = "database outage: retried demand is met after recovery";
    /// Recovery scales out exactly once.
    pub const ONE_SCALE_OUT: &str = "database outage: recovery records exactly one scale-out";
    /// Enrollment returns unavailable.
    pub const ENROLLMENT_REFUSED: &str =
        "database outage: worker certificate enrollment returns 503";
    /// Revocation returns unavailable.
    pub const REVOCATION_REFUSED: &str = "database outage: principal revocation returns 503";
    /// Both builds succeed.
    pub const BUILD: &str = "database outage: held build succeeds after recovery";
    /// The partition build stays held.
    pub const PARTITION_HELD: &str =
        "database partition: build remains active during the partition";
    /// The follower takes over.
    pub const PARTITION_TAKEOVER: &str = "database partition: the follower takes over";
    /// The old leader is fenced.
    pub const PARTITION_FENCED: &str =
        "database partition: the old leader makes no worker change after validity lapses";
    /// The old leader exits.
    pub const PARTITION_EXIT: &str =
        "database partition: the old leader exits after losing leadership";
    /// Exactly one leader remains.
    pub const PARTITION_SINGLE: &str = "database partition: exactly one leader remains";
    /// The partition build succeeds.
    pub const PARTITION_BUILD: &str = "database partition: the build succeeds";
}
