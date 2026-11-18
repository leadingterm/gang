use std::sync::Arc;
use std::time::Duration;

use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::TransactWriteItem;
use gang_aws_sdk::{CallError, Retryability, call_error, load};
use gang_clock::{Clock, SystemClock};
use gang_spi::{Plugin, PluginContext, PortError};
use tokio::sync::OnceCell;

use crate::items::{Item, key, s};
use crate::{DynamoDbConfig, DynamoError};

/// DynamoDB-backed state repository, leader lease and expiring records.
pub struct DynamoDb {
    config: DynamoDbConfig,
    client: OnceCell<aws_sdk_dynamodb::Client>,
    pub(crate) schema: OnceCell<()>,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for DynamoDb {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DynamoDb")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

pub(crate) enum Transaction {
    Committed,
    ConditionFailed,
}

impl Plugin for DynamoDb {
    type Config = DynamoDbConfig;

    fn build(config: DynamoDbConfig, _context: &PluginContext<'_>) -> Result<Self, PortError> {
        if let Some(endpoint) = &config.endpoint
            && (!matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none())
        {
            return Err(DynamoError::Config {
                detail: "endpoint must be an HTTP or HTTPS URL with a host",
            }
            .into());
        }
        Ok(Self {
            config,
            client: OnceCell::new(),
            schema: OnceCell::new(),
            clock: Arc::new(SystemClock::new()),
        })
    }
}

impl DynamoDb {
    #[cfg(test)]
    pub(crate) fn with_client(
        config: DynamoDbConfig,
        client: aws_sdk_dynamodb::Client,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            config,
            client: OnceCell::new_with(Some(client)),
            schema: OnceCell::new(),
            clock,
        }
    }

    pub(crate) async fn client(&self) -> &aws_sdk_dynamodb::Client {
        self.client
            .get_or_init(|| async {
                let sdk = load(&self.config.region, &self.config.client).await;
                let mut builder = aws_sdk_dynamodb::config::Builder::from(&sdk);
                if let Some(endpoint) = &self.config.endpoint {
                    builder = builder.endpoint_url(endpoint.as_str());
                }
                aws_sdk_dynamodb::Client::from_conf(builder.build())
            })
            .await
    }

    pub(crate) fn table(&self) -> &str {
        self.config.table.as_str()
    }

    pub(crate) async fn get(&self, pk: &str, sk: &str) -> Result<Option<Item>, DynamoError> {
        self.ensure_schema().await?;
        let response = self
            .client()
            .await
            .get_item()
            .table_name(self.table())
            .set_key(Some(key(pk, sk)))
            .consistent_read(true)
            .send()
            .await
            .map_err(|error| call_error("GetItem", error))?;
        Ok(response.item)
    }

    pub(crate) async fn query_partition(&self, pk: &str) -> Result<Vec<Item>, DynamoError> {
        self.ensure_schema().await?;
        let mut items = Vec::new();
        let mut start = None;
        loop {
            let response = self
                .client()
                .await
                .query()
                .table_name(self.table())
                .key_condition_expression("#pk = :pk")
                .expression_attribute_names("#pk", "pk")
                .expression_attribute_values(":pk", s(pk))
                .consistent_read(true)
                .set_exclusive_start_key(start)
                .send()
                .await
                .map_err(|error| call_error("Query", error))?;
            items.extend(response.items.unwrap_or_default());
            start = response.last_evaluated_key.filter(|key| !key.is_empty());
            if start.is_none() {
                return Ok(items);
            }
        }
    }

    pub(crate) async fn transact(
        &self,
        items: Vec<TransactWriteItem>,
    ) -> Result<Transaction, DynamoError> {
        self.ensure_schema().await?;
        self.clock
            .timeout(
                self.config.client.request_timeout.get(),
                self.transact_attempts(items),
            )
            .await
            .map_err(|error| {
                CallError::failed(
                    "TransactWriteItems",
                    Retryability::Transient,
                    Box::new(error),
                )
            })?
    }

    async fn transact_attempts(
        &self,
        items: Vec<TransactWriteItem>,
    ) -> Result<Transaction, DynamoError> {
        let mut backoff = self.config.client.retry_initial_backoff.get();
        for attempt in 1..=self.config.client.max_attempts.get() {
            let result = self
                .client()
                .await
                .transact_write_items()
                .set_transact_items(Some(items.clone()))
                .send()
                .await;
            match result {
                Ok(_) => return Ok(Transaction::Committed),
                Err(error) => {
                    if let Some(TransactWriteItemsError::TransactionCanceledException(reason)) =
                        error.as_service_error()
                    {
                        let reasons = reason.cancellation_reasons();
                        let permanent = reasons.iter().any(|reason| {
                            !matches!(
                                reason.code(),
                                None | Some(
                                    "None"
                                        | "ConditionalCheckFailed"
                                        | "TransactionConflict"
                                        | "ThrottlingError"
                                        | "ProvisionedThroughputExceeded"
                                )
                            )
                        });
                        if !permanent
                            && reasons.iter().any(|reason| {
                                matches!(
                                    reason.code(),
                                    Some(
                                        "TransactionConflict"
                                            | "ThrottlingError"
                                            | "ProvisionedThroughputExceeded"
                                    )
                                )
                            })
                        {
                            if attempt == self.config.client.max_attempts.get() {
                                return Err(CallError::failed(
                                    "TransactWriteItems",
                                    Retryability::Transient,
                                    Box::new(error),
                                )
                                .into());
                            }
                            let mut random = [0_u8; 8];
                            getrandom::fill(&mut random).map_err(|source| {
                                CallError::failed(
                                    "TransactWriteItems",
                                    Retryability::Transient,
                                    Box::new(source),
                                )
                            })?;
                            let millis = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX);
                            let delay =
                                u64::from_ne_bytes(random) % millis.saturating_add(1).max(1);
                            self.clock.sleep(Duration::from_millis(delay)).await;
                            backoff = backoff
                                .saturating_mul(2)
                                .min(self.config.client.retry_max_backoff.get());
                            continue;
                        }
                        if !permanent
                            && reasons
                                .iter()
                                .any(|reason| reason.code() == Some("ConditionalCheckFailed"))
                        {
                            return Ok(Transaction::ConditionFailed);
                        }
                    }
                    return Err(call_error("TransactWriteItems", error).into());
                }
            }
        }
        Err(DynamoError::Config {
            detail: "transaction attempts must not be empty",
        })
    }
}
