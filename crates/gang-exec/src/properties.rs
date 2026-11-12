//! The platform properties an action requests, read from the environment `gang exec` is started with.

use std::ffi::{OsStr, OsString};
use std::num::NonZeroU32;

use gang_spi::{ActionLimits, CPU_COUNT_ENV, MEMORY_KB_ENV, NETWORK_ENV, NetworkRequest};
use gang_types::{Bytes, NonZeroBytes};

use crate::{ExecError, PropertyError};

/// What an action requests, as `gang exec` reads it from its environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActionProperties {
    /// Whether the action asked for network access.
    pub network: NetworkRequest,
    /// The CPUs and memory the action reserves.
    pub limits: ActionLimits,
}

impl ActionProperties {
    /// Reads the properties from `environment`. An unset or empty variable means not requested.
    ///
    /// # Errors
    ///
    /// [`ExecError::InvalidProperty`] when a variable's value is not a valid request.
    pub fn from_environment(environment: &[(OsString, OsString)]) -> Result<Self, ExecError> {
        let network = property(environment, NETWORK_ENV, |text| {
            text.parse::<NetworkRequest>()
                .map_err(PropertyError::Network)
        })?
        .unwrap_or(NetworkRequest::NotRequested);

        let cpu_count = property(environment, CPU_COUNT_ENV, |text| {
            let count = text.parse::<u32>().map_err(PropertyError::Number)?;
            Ok(NonZeroU32::new(count))
        })?
        .flatten();

        // A zero KiB count is not a request, the same as an unset variable.
        let memory = property(environment, MEMORY_KB_ENV, |text| {
            let kib = text.parse::<u64>().map_err(PropertyError::Number)?;
            let bytes = kib
                .checked_mul(Bytes::kib(1).get())
                .ok_or(PropertyError::TooLarge)?;
            Ok(NonZeroBytes::new(Bytes::new(bytes)))
        })?
        .flatten();

        Ok(Self {
            network,
            limits: ActionLimits { cpu_count, memory },
        })
    }
}

/// Returns the value of the first entry named `name` whose value is not empty.
fn value<'a>(environment: &'a [(OsString, OsString)], name: &str) -> Option<&'a OsStr> {
    let name = OsStr::new(name);
    environment
        .iter()
        .find(|(key, entry)| key.as_os_str() == name && !entry.is_empty())
        .map(|(_, entry)| entry.as_os_str())
}

/// Reads the variable `name` as unicode and parses it with `parse`. `None` when it is unset or empty.
fn property<T>(
    environment: &[(OsString, OsString)],
    name: &'static str,
    parse: impl FnOnce(&str) -> Result<T, PropertyError>,
) -> Result<Option<T>, ExecError> {
    let Some(raw) = value(environment, name) else {
        return Ok(None);
    };
    raw.to_str()
        .ok_or(PropertyError::NotUnicode)
        .and_then(parse)
        .map(Some)
        .map_err(|source| ExecError::InvalidProperty { name, source })
}
