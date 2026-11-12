//! Names shared by the controller and worker renderers and the snapshot bridge.

/// Name of the controller's content-addressable store entry.
pub(crate) const CONTENT_STORE: &str = "CAS_MAIN";

/// Name of the controller's action cache store entry.
pub(crate) const ACTION_STORE: &str = "AC_MAIN";

/// Name of the worker's `fast_slow` content store entry.
pub(crate) const WORKER_CONTENT_STORE: &str = "WORKER_FAST_SLOW";

/// Name of the worker's action cache store entry, used to upload action results.
pub(crate) const WORKER_ACTION_STORE: &str = "WORKER_AC";
