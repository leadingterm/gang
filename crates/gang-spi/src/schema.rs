//! Errors for unsupported persisted database schemas.

/// The stored schema differs from the schema supported by this binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("database schema {stored} is unsupported (this binary supports schema {supported})")]
pub struct SchemaMismatch {
    /// The version stored in the database.
    pub stored: u32,
    /// The schema version supported by this binary.
    pub supported: u32,
}
