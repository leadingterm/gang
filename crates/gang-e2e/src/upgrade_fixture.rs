//! Deployment plans rendered by the version's own infrastructure.

use std::fs;
use std::path::Path;

use serde::Deserialize;

use crate::errors::{E2eError, Result};

#[derive(Debug, Deserialize)]
pub(crate) struct ReleaseFixture {
    pub(crate) gang_toml: String,
    pub(crate) local_config: String,
    pub(crate) public: FrontendPlan,
    pub(crate) workers: FrontendPlan,
    pub(crate) controller: ControllerPlan,
    pub(crate) configuration: ConfigurationPlan,
    pub(crate) seed: SeedConfiguration,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SeedConfiguration {
    pub(crate) gang_toml: String,
    pub(crate) configuration: ConfigurationPlan,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ConfigurationPlan {
    pub(crate) controller: ConfigurationAccess,
    pub(crate) workers: Vec<ConfigurationAccess>,
    pub(crate) generations_retained: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ConfigurationAccess {
    pub(crate) reference: String,
    pub(crate) readable_resources: Vec<String>,
    pub(crate) excluded_from_deny: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct FrontendPlan {
    pub(crate) port: u16,
    pub(crate) target_port: u16,
    pub(crate) proxy_protocol_v2: bool,
    pub(crate) preserve_client_ip: Option<bool>,
    pub(crate) health: HealthPlan,
}

#[derive(Debug, Deserialize)]
pub(crate) struct HealthPlan {
    pub(crate) path: String,
    pub(crate) interval_seconds: u64,
    pub(crate) timeout_seconds: u64,
    pub(crate) healthy_threshold: u32,
    pub(crate) unhealthy_threshold: u32,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ControllerPlan {
    pub(crate) instances: u32,
    pub(crate) health_check_type: String,
    pub(crate) min_healthy_percentage: u32,
    pub(crate) max_healthy_percentage: u32,
}

impl ReleaseFixture {
    pub(crate) fn seeded(mut self) -> Result<Self> {
        let config: toml::Value =
            toml::from_str(&self.seed.gang_toml).map_err(crate::template::template_error)?;
        if config
            .get("fleets")
            .and_then(toml::Value::as_array)
            .and_then(|fleets| fleets.first())
            .and_then(|fleet| fleet.get("max_instances"))
            .and_then(toml::Value::as_integer)
            != Some(1)
        {
            return Err(E2eError::system(
                "initial fixture must select seed fleet maximum one",
            ));
        }
        self.gang_toml.clone_from(&self.seed.gang_toml);
        self.configuration = self.seed.configuration.clone();
        Ok(self)
    }

    pub(crate) fn read(path: &Path) -> Result<Self> {
        let content =
            fs::read(path).map_err(E2eError::io(format!("reading {}", path.display())))?;
        serde_json::from_slice(&content)
            .map_err(E2eError::system_from(format!("parsing {}", path.display())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn fixture() -> Value {
        let frontend = json!({
            "port": 443,
            "target_port": 8443,
            "proxy_protocol_v2": true,
            "preserve_client_ip": false,
            "health": {
                "path": "/ready",
                "interval_seconds": 5,
                "timeout_seconds": 2,
                "healthy_threshold": 2,
                "unhealthy_threshold": 2
            }
        });
        let configuration = json!({
            "controller": {
                "reference": "/gang/gang.toml/generations/old:1",
                "readable_resources": [],
                "excluded_from_deny": []
            },
            "workers": [],
            "generations_retained": false
        });
        json!({
            "gang_toml": "rendered",
            "local_config": "local",
            "public": frontend,
            "workers": frontend,
            "configuration": configuration,
            "seed": { "gang_toml": "[[fleets]]\nmax_instances = 1", "configuration": configuration },
            "controller": {
                "instances": 1,
                "health_check_type": "EC2",
                "min_healthy_percentage": 100,
                "max_healthy_percentage": 200,
                "warmup_seconds": 20,
                "deregistration_delay_seconds": 300
            }
        })
    }

    #[test]
    fn deserializes_transport_and_ignores_unused_historical_refresh_metadata() {
        let mut value = fixture();
        value["workers"]["proxy_protocol_v2"] = json!(false);
        value["workers"]["preserve_client_ip"] = Value::Null;
        value["controller"]["warmup_seconds"] = json!(0);
        value["controller"]["deregistration_delay_seconds"] = json!(0);
        let fixture: ReleaseFixture = serde_json::from_value(value).expect("fixture");
        assert!(fixture.public.proxy_protocol_v2);
        assert_eq!(fixture.public.preserve_client_ip, Some(false));
        assert!(!fixture.workers.proxy_protocol_v2);
        assert_eq!(fixture.workers.preserve_client_ip, None);
    }

    #[test]
    fn requires_complete_seed_and_selects_its_own_configuration() {
        let mut value = fixture();
        value["seed"]["configuration"]["controller"]["reference"] = json!("/seed:1");
        let seeded = serde_json::from_value::<ReleaseFixture>(value)
            .expect("fixture")
            .seeded()
            .expect("seed snapshot");
        assert_eq!(seeded.gang_toml, "[[fleets]]\nmax_instances = 1");
        assert_eq!(seeded.configuration.controller.reference, "/seed:1");
        let mut missing = fixture();
        missing.as_object_mut().expect("fixture").remove("seed");
        assert!(serde_json::from_value::<ReleaseFixture>(missing).is_err());
        for field in ["gang_toml", "configuration"] {
            let mut missing = fixture();
            missing["seed"].as_object_mut().expect("seed").remove(field);
            assert!(serde_json::from_value::<ReleaseFixture>(missing).is_err());
            let mut malformed = fixture();
            malformed["seed"][field] = Value::Null;
            assert!(serde_json::from_value::<ReleaseFixture>(malformed).is_err());
        }
        let mut extra = fixture();
        extra["seed"]["local_config"] = json!("not part of the seed contract");
        assert!(serde_json::from_value::<ReleaseFixture>(extra).is_err());
        for field in ["controller", "workers", "generations_retained"] {
            let mut missing = fixture();
            missing["seed"]["configuration"]
                .as_object_mut()
                .expect("configuration")
                .remove(field);
            assert!(serde_json::from_value::<ReleaseFixture>(missing).is_err());
            let mut malformed = fixture();
            malformed["seed"]["configuration"][field] = Value::Null;
            assert!(serde_json::from_value::<ReleaseFixture>(malformed).is_err());
        }
        let mut oversized = fixture();
        oversized["seed"]["gang_toml"] = json!("[[fleets]]\nmax_instances = 2");
        assert!(
            serde_json::from_value::<ReleaseFixture>(oversized)
                .expect("fixture")
                .seeded()
                .is_err()
        );
    }

    #[test]
    fn rejects_missing_or_malformed_required_policy() {
        for (owner, field) in [
            ("public", "proxy_protocol_v2"),
            ("workers", "proxy_protocol_v2"),
            ("configuration", "controller"),
            ("configuration", "workers"),
            ("configuration", "generations_retained"),
        ] {
            let mut value = fixture();
            value[owner].as_object_mut().expect("object").remove(field);
            assert!(serde_json::from_value::<ReleaseFixture>(value).is_err());
            let mut value = fixture();
            value[owner][field] = Value::Null;
            assert!(serde_json::from_value::<ReleaseFixture>(value).is_err());
        }
    }

    #[test]
    fn requires_configuration_and_each_access_field() {
        let mut missing = fixture();
        missing
            .as_object_mut()
            .expect("fixture")
            .remove("configuration");
        assert!(serde_json::from_value::<ReleaseFixture>(missing).is_err());
        for field in ["reference", "readable_resources", "excluded_from_deny"] {
            let mut access = fixture()["configuration"]["controller"].clone();
            access.as_object_mut().expect("access").remove(field);
            assert!(serde_json::from_value::<ConfigurationAccess>(access).is_err());
            let mut access = fixture()["configuration"]["controller"].clone();
            access[field] = Value::Null;
            assert!(serde_json::from_value::<ConfigurationAccess>(access).is_err());
        }
    }
}
