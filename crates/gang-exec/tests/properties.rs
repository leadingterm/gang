//! Behaviour of reading an action's requested properties from its environment.

use std::error::Error;
use std::ffi::OsString;
use std::num::NonZeroU32;
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;

use gang_exec::{ActionProperties, ExecError, PropertyError};
use gang_spi::{CPU_COUNT_ENV, MEMORY_KB_ENV, NETWORK_ENV, NetworkRequest};
use gang_types::NonZeroBytes;

type TestResult = Result<(), Box<dyn Error>>;

fn environment(pairs: &[(&str, &str)]) -> Vec<(OsString, OsString)> {
    pairs
        .iter()
        .map(|(name, value)| (OsString::from(name), OsString::from(value)))
        .collect()
}

fn properties(pairs: &[(&str, &str)]) -> Result<ActionProperties, ExecError> {
    ActionProperties::from_environment(&environment(pairs))
}

#[test]
fn empty_environment_requests_nothing() -> TestResult {
    let properties = properties(&[])?;

    assert_eq!(properties.network, NetworkRequest::NotRequested);
    assert_eq!(properties.limits.cpu_count, None);
    assert_eq!(properties.limits.memory, None);
    Ok(())
}

#[test]
fn empty_network_is_not_requested() -> TestResult {
    let properties = properties(&[(NETWORK_ENV, "")])?;

    assert_eq!(properties.network, NetworkRequest::NotRequested);
    Ok(())
}

#[test]
fn requested_network_is_requested() -> TestResult {
    let properties = properties(&[(NETWORK_ENV, "requested")])?;

    assert_eq!(properties.network, NetworkRequest::Requested);
    Ok(())
}

#[test]
fn not_requested_network_is_not_requested() -> TestResult {
    let properties = properties(&[(NETWORK_ENV, "not-requested")])?;

    assert_eq!(properties.network, NetworkRequest::NotRequested);
    Ok(())
}

#[test]
fn first_non_empty_network_value_wins() -> TestResult {
    let properties = properties(&[(NETWORK_ENV, ""), (NETWORK_ENV, "requested")])?;

    assert_eq!(properties.network, NetworkRequest::Requested);
    Ok(())
}

#[test]
fn garbage_network_is_rejected() {
    assert!(matches!(
        properties(&[(NETWORK_ENV, "maybe")]),
        Err(ExecError::InvalidProperty {
            name: NETWORK_ENV,
            source: PropertyError::Network(_),
        })
    ));
}

#[cfg(unix)]
#[test]
fn non_unicode_network_is_rejected() {
    let value = OsString::from_vec(vec![0xff, 0xfe]);
    let environment = [(OsString::from(NETWORK_ENV), value)];

    assert!(matches!(
        ActionProperties::from_environment(&environment),
        Err(ExecError::InvalidProperty {
            name: NETWORK_ENV,
            source: PropertyError::NotUnicode,
        })
    ));
}

#[test]
fn cpu_count_is_read() -> TestResult {
    let properties = properties(&[(CPU_COUNT_ENV, "4")])?;

    assert_eq!(properties.limits.cpu_count.map(NonZeroU32::get), Some(4));
    Ok(())
}

#[test]
fn cpu_count_zero_is_not_requested() -> TestResult {
    let properties = properties(&[(CPU_COUNT_ENV, "0")])?;

    assert_eq!(properties.limits.cpu_count, None);
    Ok(())
}

#[test]
fn invalid_cpu_count_is_rejected() {
    assert!(
        matches!(
            properties(&[(CPU_COUNT_ENV, "x")]),
            Err(ExecError::InvalidProperty {
                name: CPU_COUNT_ENV,
                source: PropertyError::Number(_),
            })
        ),
        "cpu count `x` must be rejected"
    );
}

#[test]
fn a_cpu_count_above_u32_is_an_invalid_property() {
    assert!(matches!(
        properties(&[(CPU_COUNT_ENV, "4294967296")]),
        Err(ExecError::InvalidProperty {
            name: CPU_COUNT_ENV,
            ..
        })
    ));
}

#[test]
fn a_negative_cpu_count_is_an_invalid_property() {
    assert!(matches!(
        properties(&[(CPU_COUNT_ENV, "-1")]),
        Err(ExecError::InvalidProperty {
            name: CPU_COUNT_ENV,
            ..
        })
    ));
}

#[test]
fn zero_cpu_count_and_zero_memory_mean_not_requested() -> TestResult {
    let properties = properties(&[(CPU_COUNT_ENV, "0"), (MEMORY_KB_ENV, "0")])?;

    assert_eq!(properties.limits.cpu_count, None);
    assert_eq!(properties.limits.memory, None);
    Ok(())
}

#[test]
fn memory_is_read_in_kib_as_bytes() -> TestResult {
    let properties = properties(&[(MEMORY_KB_ENV, "1024")])?;

    assert_eq!(properties.limits.memory, Some(NonZeroBytes::mib(1)));
    Ok(())
}

#[test]
fn memory_zero_is_not_requested() -> TestResult {
    let properties = properties(&[(MEMORY_KB_ENV, "0")])?;

    assert_eq!(properties.limits.memory, None);
    Ok(())
}

#[test]
fn memory_too_large_in_bytes_is_rejected() {
    assert!(matches!(
        properties(&[(MEMORY_KB_ENV, "18446744073709551615")]),
        Err(ExecError::InvalidProperty {
            name: MEMORY_KB_ENV,
            source: PropertyError::TooLarge,
        })
    ));
}

#[test]
fn unrelated_variables_are_ignored() -> TestResult {
    let properties = properties(&[
        ("GANG_OTHER", "requested"),
        ("PATH", "/usr/bin"),
        ("GANG_NETWORK_EXTRA", "garbage"),
    ])?;

    assert_eq!(properties.network, NetworkRequest::NotRequested);
    assert_eq!(properties.limits.cpu_count, None);
    assert_eq!(properties.limits.memory, None);
    Ok(())
}
