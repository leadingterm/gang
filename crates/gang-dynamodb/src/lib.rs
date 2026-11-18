//! DynamoDB-backed shared state, leases, and expiring records.
//!
//! The table needs IAM actions `dynamodb:GetItem`, `dynamodb:PutItem`,
//! `dynamodb:UpdateItem`, `dynamodb:DeleteItem`, `dynamodb:Query`, and
//! `dynamodb:ConditionCheckItem`; transaction writes use those actions.
//!
//! ```
//! use gang_dynamodb::TableName;
//! assert!(TableName::try_new("gang-state").is_ok());
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod config;
#[cfg(test)]
mod contract_tests;
mod errors;
mod expiring;
mod items;
mod lease;
mod plugin;
mod repository;
mod schema;

pub use config::{DynamoDbConfig, TableName, TableNameError};
pub use errors::DynamoError;
pub use plugin::DynamoDb;
pub use schema::SCHEMA_VERSION;
