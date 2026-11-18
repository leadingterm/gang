use gang_aws_sdk::{ClientSettings, Region};
use gang_types::{Url, validated_string};
use serde::{Deserialize, Serialize};

const TABLE_NAME_MIN_LEN: usize = 3;
const TABLE_NAME_MAX_LEN: usize = 255;

/// The table name is outside `DynamoDB`'s permitted length or character set.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("table must be 3 to 255 characters of a-z, A-Z, 0-9, '_', '.' and '-'")]
pub struct TableNameError;

fn check_table_name(value: &str) -> Result<(), TableNameError> {
    if (TABLE_NAME_MIN_LEN..=TABLE_NAME_MAX_LEN).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
    {
        Ok(())
    } else {
        Err(TableNameError)
    }
}

validated_string!(
    /// The DynamoDB table containing Gang's shared state.
    pub TableName, check_table_name, TableNameError
);

/// Settings for the `[database.dynamodb]` plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DynamoDbConfig {
    /// The table containing shared state.
    pub table: TableName,
    /// AWS region containing the table.
    pub region: Region,
    /// Optional endpoint for a compatible `DynamoDB` service.
    pub endpoint: Option<Url>,
    /// AWS request timeouts and retry settings.
    #[serde(default)]
    pub client: ClientSettings,
}
