//! Single integration test crate for `gang-worker`: every behavioural test lives in one crate so
//! the shared fakes in `support` are compiled once and checked for use across all suites.
#[cfg(test)]
mod enrollment;
#[cfg(test)]
mod layout;
#[cfg(test)]
mod programs;
#[cfg(test)]
mod run;
#[cfg(test)]
mod support;
