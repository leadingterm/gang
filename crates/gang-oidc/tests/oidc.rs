//! Single integration test crate for `gang-oidc`: every behavioural test lives in one crate so the
//! shared test issuer is compiled once and its helpers are checked for use across all suites.

#[cfg(test)]
mod config;

#[cfg(test)]
mod discovery;

#[cfg(test)]
mod keys;

#[cfg(test)]
mod login;

#[cfg(test)]
mod support;

#[cfg(test)]
mod verify;
