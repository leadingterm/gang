//! Single integration test crate for `gang-authn`: every behavioural test lives in one crate so
//! the shared fakes are compiled once and checked for use across all suites.

#[cfg(test)]
mod conformance;

#[cfg(test)]
mod mtls_plugin;

#[cfg(test)]
mod none_plugin;

#[cfg(test)]
mod support;

#[cfg(test)]
mod token;

#[cfg(test)]
mod token_plugin;
