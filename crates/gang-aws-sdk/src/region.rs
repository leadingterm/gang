use gang_types::validated_string;

/// An invalid AWS region.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("region must be non-empty and contain only a-z, 0-9 and '-'")]
pub struct RegionError;

fn check_region(value: &str) -> Result<(), RegionError> {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        Ok(())
    } else {
        Err(RegionError)
    }
}

validated_string!(
    /// An AWS region such as `eu-north-1`.
    pub Region,
    check_region,
    RegionError
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn region_accepts_well_formed_and_rejects_the_rest() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(Region::try_new("us-east-1")?.as_str(), "us-east-1");
        assert_eq!(Region::try_new(""), Err(RegionError));
        assert_eq!(Region::try_new("US"), Err(RegionError));
        assert_eq!(Region::try_new("eu_west"), Err(RegionError));
        Ok(())
    }

    #[test]
    fn region_serde_validation_and_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        assert!(serde_json::from_str::<Region>("\"US\"").is_err());
        let region: Region = serde_json::from_str("\"eu-north-1\"")?;
        assert_eq!(serde_json::to_string(&region)?, "\"eu-north-1\"");
        Ok(())
    }
}
