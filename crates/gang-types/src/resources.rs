//! Compute resources a worker requests or reports.

use serde::{Deserialize, Serialize};

use crate::Bytes;

/// Compute resources for a worker. The default requests nothing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    /// Number of virtual CPUs.
    pub vcpus: u32,
    /// Memory size.
    pub memory: Bytes,
    /// Disk the worker's action work directories may hold in total.
    pub disk: Bytes,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_known_fields() {
        let resources: Resources =
            toml::from_str("vcpus = 4\nmemory = \"29GiB\"\ndisk = \"100GiB\"\n").unwrap();
        assert_eq!(
            resources,
            Resources {
                vcpus: 4,
                memory: Bytes::new(29 << 30),
                disk: Bytes::new(100 << 30),
            }
        );
    }

    #[test]
    fn rejects_unknown_fields() {
        let text = "vcpus = 4\nmemory = \"29GiB\"\ngpus = 1\n";
        assert!(toml::from_str::<Resources>(text).is_err());
    }
}
