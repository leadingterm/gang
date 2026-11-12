//! How a configuration pins the server's CA: a fixed pin, or the pin of the `[signer]` key.

use std::fmt;
use std::str::FromStr;

use gang_types::CaPin;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::errors::CaPinSettingError;

/// The only spelling of the keyword that selects the `[signer]` key's pin.
const SIGNER: &str = "signer";

/// How clients and workers configured by this file pin the server's CA (`[server] ca_pin`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaPinSetting {
    /// `"sha256:<64 lowercase hex>"`: this pin.
    Pinned(CaPin),
    /// `"signer"`: the pin of the `[signer]` key's public key, which the process derives at start.
    Signer,
}

impl fmt::Display for CaPinSetting {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pinned(pin) => write!(formatter, "{pin}"),
            Self::Signer => formatter.write_str(SIGNER),
        }
    }
}

impl FromStr for CaPinSetting {
    type Err = CaPinSettingError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text == SIGNER {
            return Ok(Self::Signer);
        }
        text.parse()
            .map(Self::Pinned)
            .map_err(|_| CaPinSettingError)
    }
}

impl Serialize for CaPinSetting {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for CaPinSetting {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use gang_types::CaPin;
    use serde::{Deserialize, Serialize};

    use super::CaPinSetting;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Holder {
        ca_pin: CaPinSetting,
    }

    fn pinned_text() -> String {
        format!("sha256:{}", "ab".repeat(32))
    }

    #[test]
    fn signer_round_trips_through_toml() -> Result<(), Box<dyn std::error::Error>> {
        let holder: Holder = toml::from_str("ca_pin = \"signer\"")?;
        assert_eq!(holder.ca_pin, CaPinSetting::Signer);
        assert_eq!(toml::to_string(&holder)?, "ca_pin = \"signer\"\n");
        Ok(())
    }

    #[test]
    fn pin_round_trips_through_toml() -> Result<(), Box<dyn std::error::Error>> {
        let text = format!("ca_pin = \"{}\"", pinned_text());
        let holder: Holder = toml::from_str(&text)?;
        assert_eq!(holder.ca_pin, CaPinSetting::Pinned(CaPin::new([0xab; 32])));
        assert_eq!(toml::to_string(&holder)?, format!("{text}\n"));
        Ok(())
    }

    #[test]
    fn rejects_a_capitalized_signer() {
        assert!("Signer".parse::<CaPinSetting>().is_err());
        assert!(toml::from_str::<Holder>("ca_pin = \"Signer\"").is_err());
    }

    #[test]
    fn rejects_a_short_pin() {
        assert!("sha256:abc".parse::<CaPinSetting>().is_err());
        assert!(toml::from_str::<Holder>("ca_pin = \"sha256:abc\"").is_err());
    }

    #[test]
    fn displays_the_keyword_or_the_pin() {
        assert_eq!(CaPinSetting::Signer.to_string(), "signer");
        assert_eq!(
            CaPinSetting::Pinned(CaPin::new([0xab; 32])).to_string(),
            pinned_text()
        );
    }
}
