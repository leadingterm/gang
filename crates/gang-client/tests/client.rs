//! Single integration test crate for `gang-client`: every behavioural test lives in one crate so the
//! shared test fixtures are compiled once and their helpers are checked for use across all suites.

#[cfg(test)]
mod client {
    mod support;

    mod api;
    mod bazelrc;
    mod get;
    mod interactive;
    mod revoke;
    mod session_pin;
    mod token;
    mod workload;
}
