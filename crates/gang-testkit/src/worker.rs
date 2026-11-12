//! Deterministic worker identifiers for tests.

use gang_types::{InstanceId, NameError, WorkerId};

const TEST_UUID: &str = "6c5e4f3a-9b1d-4e2a-8f7c-000000000001";

/// Returns the worker id registered by a worker on `instance`, using a fixed test UUID.
///
/// # Errors
///
/// Returns [`NameError`] if the composed worker id is invalid.
pub fn worker_on(instance: &InstanceId) -> Result<WorkerId, NameError> {
    WorkerId::try_new(format!("{}{TEST_UUID}", instance.worker_name()))
}

/// Returns a deterministic worker id for an instance named by `instance`.
///
/// # Errors
///
/// Returns [`NameError`] when `instance` is not a valid instance id.
pub fn worker_id(instance: &str) -> Result<WorkerId, NameError> {
    worker_on(&InstanceId::try_new(instance)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_id_round_trips_to_its_instance() -> crate::TestResult {
        let instance = InstanceId::try_new("i-example_1")?;
        let worker = worker_on(&instance)?;
        assert_eq!(worker.instance().as_ref(), Some(&instance));
        assert_eq!(worker_id(instance.as_str())?, worker);
        assert!(worker_id("").is_err());
        assert!(worker_id("invalid name").is_err());
        Ok(())
    }
}
