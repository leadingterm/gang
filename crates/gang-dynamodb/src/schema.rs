use aws_sdk_dynamodb::operation::put_item::PutItemError;
use gang_aws_sdk::call_error;
use gang_spi::{SchemaPlan, SchemaVersion};

use crate::items::{key, n, number};
use crate::{DynamoDb, DynamoError};

const META: &str = "meta";
const SCHEMA: &str = "schema";
/// Shape 1 is the first `DynamoDB` table layout.
pub const SCHEMA_VERSION: SchemaVersion = SchemaVersion {
    current: 1,
    oldest_compatible: 1,
};

fn schema_plan(
    binary: SchemaVersion,
    stored: Option<SchemaVersion>,
) -> Result<SchemaPlan, DynamoError> {
    match binary.plan(stored)? {
        SchemaPlan::Migrate { from } if from > 0 => Err(DynamoError::NoMigration { from }),
        plan => Ok(plan),
    }
}

impl DynamoDb {
    pub(crate) async fn ensure_schema(&self) -> Result<(), DynamoError> {
        self.schema
            .get_or_try_init(|| async { self.initialize_schema().await })
            .await?;
        Ok(())
    }

    async fn initialize_schema(&self) -> Result<(), DynamoError> {
        loop {
            let response = self
                .client()
                .await
                .get_item()
                .table_name(self.table())
                .set_key(Some(key(META, SCHEMA)))
                .consistent_read(true)
                .send()
                .await
                .map_err(|error| call_error("GetItem", error))?;
            let stored = response
                .item
                .as_ref()
                .map(|item| {
                    let current = u32::try_from(number(item, "shape")?)
                        .map_err(|_| DynamoError::Malformed { attribute: "shape" })?;
                    let oldest_compatible = if item.contains_key("oldest_compatible") {
                        u32::try_from(number(item, "oldest_compatible")?).map_err(|_| {
                            DynamoError::Malformed {
                                attribute: "oldest_compatible",
                            }
                        })?
                    } else {
                        current
                    };
                    let stored = SchemaVersion {
                        current,
                        oldest_compatible,
                    };
                    if !stored.is_valid() {
                        return Err(DynamoError::Malformed { attribute: "shape" });
                    }
                    Ok(stored)
                })
                .transpose()?;
            match schema_plan(SCHEMA_VERSION, stored)? {
                SchemaPlan::Use => return Ok(()),
                SchemaPlan::Migrate { from: 0 } => {
                    let mut item = key(META, SCHEMA);
                    item.insert("shape".into(), n(SCHEMA_VERSION.current));
                    item.insert(
                        "oldest_compatible".into(),
                        n(SCHEMA_VERSION.oldest_compatible),
                    );
                    match self
                        .client()
                        .await
                        .put_item()
                        .table_name(self.table())
                        .set_item(Some(item))
                        .condition_expression("attribute_not_exists(pk)")
                        .send()
                        .await
                    {
                        Ok(_) => return Ok(()),
                        Err(error)
                            if error.as_service_error().is_some_and(
                                PutItemError::is_conditional_check_failed_exception,
                            ) => {}
                        Err(error) => return Err(call_error("PutItem", error).into()),
                    }
                }
                SchemaPlan::Migrate { from } => return Err(DynamoError::NoMigration { from }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn older_shape_requires_a_defined_data_migration() {
        let binary = SchemaVersion {
            current: 2,
            oldest_compatible: 1,
        };
        let stored = SchemaVersion {
            current: 1,
            oldest_compatible: 1,
        };
        assert!(matches!(
            schema_plan(binary, Some(stored)),
            Err(DynamoError::NoMigration { from: 1 })
        ));
    }
}
