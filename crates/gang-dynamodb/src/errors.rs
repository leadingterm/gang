use gang_aws_sdk::CallError;

/// Why a `DynamoDB` operation failed.
#[derive(Debug, thiserror::Error, gang_spi::PortFailure)]
pub enum DynamoError {
    /// The service call failed.
    #[error(transparent)]
    #[failure(transparent)]
    Call(#[from] CallError),
    /// A stored attribute is missing or malformed.
    #[error("malformed dynamodb attribute {attribute}")]
    Malformed {
        /// The invalid attribute.
        attribute: &'static str,
    },
    /// The stored schema is incompatible with this binary.
    #[error("database schema cannot be used")]
    Schema(#[from] gang_spi::SchemaTooNew),
    /// No data migration is defined for the stored table shape.
    #[error("no migration from table shape {from}")]
    NoMigration {
        /// The stored table shape.
        from: u32,
    },
    /// The plugin configuration is unusable.
    #[error("invalid dynamodb configuration: {detail}")]
    #[failure(config)]
    Config {
        /// The reason the configuration cannot be used.
        detail: &'static str,
    },
}
