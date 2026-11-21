use aws_sdk_dynamodb::operation::put_item::PutItemError;
use gang_aws_sdk::call_error;
use gang_spi::SchemaMismatch;

use crate::items::{key, n, number};
use crate::{DynamoDb, DynamoError};

const META: &str = "meta";
const SCHEMA: &str = "schema";
/// The supported `DynamoDB` table shape.
pub const SCHEMA_VERSION: u32 = 1;

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
            if let Some(item) = response.item.as_ref() {
                let current = u32::try_from(number(item, "shape")?)
                    .map_err(|_| DynamoError::Malformed { attribute: "shape" })?;
                let oldest = if item.contains_key("oldest_compatible") {
                    Some(
                        u32::try_from(number(item, "oldest_compatible")?).map_err(|_| {
                            DynamoError::Malformed {
                                attribute: "oldest_compatible",
                            }
                        })?,
                    )
                } else {
                    None
                };
                if current == 0 || oldest.is_some_and(|oldest| oldest == 0 || oldest > current) {
                    return Err(DynamoError::Malformed { attribute: "shape" });
                }
                if current != SCHEMA_VERSION {
                    return Err(SchemaMismatch {
                        stored: current,
                        supported: SCHEMA_VERSION,
                    }
                    .into());
                }
                return Ok(());
            }
            let mut item = key(META, SCHEMA);
            item.insert("shape".into(), n(SCHEMA_VERSION));
            item.insert("oldest_compatible".into(), n(1));
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
                    if error
                        .as_service_error()
                        .is_some_and(PutItemError::is_conditional_check_failed_exception) => {}
                Err(error) => return Err(call_error("PutItem", error).into()),
            }
        }
    }
}
