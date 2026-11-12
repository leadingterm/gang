//! Settings requested of a workload-token issuer.

use serde::{Deserialize, Serialize};

use crate::{Audience, TokenTtl};

/// What a provisioner asks its workload-token issuer for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WorkloadTokenSettings {
    /// The audience the token is issued for.
    pub audience: Audience,
    /// The lifetime of the token.
    pub ttl: TokenTtl,
}

/// The default settings: audience `gang` and a 15 minute lifetime.
impl Default for WorkloadTokenSettings {
    fn default() -> Self {
        Self {
            audience: Audience::DEFAULT,
            ttl: TokenTtl::from_units(900),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_settings_apply_during_deserialization() -> Result<(), serde_json::Error> {
        let settings: WorkloadTokenSettings = serde_json::from_str("{}")?;
        assert_eq!(settings, WorkloadTokenSettings::default());
        assert_eq!(settings.audience, Audience::DEFAULT);
        assert_eq!(settings.ttl, TokenTtl::from_units(900));
        assert!(serde_json::from_str::<WorkloadTokenSettings>(r#"{"ttl":"2h"}"#).is_err());
        assert!(serde_json::from_str::<WorkloadTokenSettings>(r#"{"unknown":true}"#).is_err());
        Ok(())
    }
}
