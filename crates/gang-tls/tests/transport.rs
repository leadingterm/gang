//! Single integration test crate for `gang-tls`: every behavioural test lives in one crate so the
//! shared fixture is compiled once and its helpers are checked for use across all suites.

#[cfg(test)]
mod connect;

#[cfg(test)]
mod http;

#[cfg(test)]
mod incoming;

#[cfg(test)]
mod pin;

#[cfg(test)]
mod resumption;

#[cfg(test)]
mod revocation;

#[cfg(test)]
mod support;

#[cfg(test)]
mod timeout;

#[cfg(test)]
mod validity;
