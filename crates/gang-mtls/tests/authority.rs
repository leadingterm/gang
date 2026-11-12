//! Single integration test crate for `gang-mtls`: every behavioural test lives in one crate so the
//! shared test fixtures are compiled once and their helpers are checked for use across all suites.

#[cfg(test)]
mod client_certificate;

#[cfg(test)]
mod csr;

#[cfg(test)]
mod interop;

#[cfg(test)]
mod key_pair;

#[cfg(test)]
mod server_certificate;

#[cfg(test)]
mod support;
