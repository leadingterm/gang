//! Replayed `CloudWatch` HTTP for tests: canned RPC v2 CBOR responses served without a network,
//! and the recorded requests decoded back to JSON for assertions.

use aws_sdk_cloudwatch::config::{BehaviorVersion, Credentials, Region};
use aws_smithy_cbor::data::Type;
use aws_smithy_cbor::decode::DeserializeError;
use aws_smithy_cbor::{Decoder, Encoder};
use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
use aws_smithy_runtime_api::http::{HttpError, Request, Response, StatusCode};
use aws_smithy_types::DateTime;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::retry::RetryConfig;
use serde_json::{Map, Value, json};

/// One canned HTTP response.
pub(crate) type Reply = Response<SdkBody>;

/// One metric result in a `GetMetricData` response.
pub(crate) struct Series<'a> {
    pub(crate) id: &'a str,
    pub(crate) status: &'a str,
    /// Seconds since the Unix epoch of each datapoint.
    pub(crate) timestamps: &'a [i64],
    pub(crate) values: &'a [f64],
}

/// A complete result `id` with `timestamps` and `values`.
pub(crate) fn complete<'a>(id: &'a str, timestamps: &'a [i64], values: &'a [f64]) -> Series<'a> {
    Series {
        id,
        status: "Complete",
        timestamps,
        values,
    }
}

/// A response with complete bucket size and object count series, each given as timestamps and
/// values.
pub(crate) fn storage(
    bytes: (&[i64], &[f64]),
    objects: (&[i64], &[f64]),
) -> Result<Reply, HttpError> {
    metric_data(
        &[
            complete("bytes", bytes.0, bytes.1),
            complete("objects", objects.0, objects.1),
        ],
        None,
    )
}

/// A `GetMetricData` response with `results` and, when set, a continuation token.
pub(crate) fn metric_data(
    results: &[Series<'_>],
    next_token: Option<&str>,
) -> Result<Response<SdkBody>, HttpError> {
    let mut encoder = Encoder::new(Vec::new());
    encoder
        .map(1 + usize::from(next_token.is_some()))
        .str("MetricDataResults")
        .array(results.len());
    for series in results {
        encoder
            .map(4)
            .str("Id")
            .str(series.id)
            .str("StatusCode")
            .str(series.status)
            .str("Timestamps")
            .array(series.timestamps.len());
        for seconds in series.timestamps {
            encoder.timestamp(&DateTime::from_secs(*seconds));
        }
        encoder.str("Values").array(series.values.len());
        for value in series.values {
            encoder.double(*value);
        }
    }
    if let Some(token) = next_token {
        encoder.str("NextToken").str(token);
    }
    response(200, encoder.into_writer())
}

/// A response with `status` and an empty body, such as a server error.
pub(crate) fn failure(status: u16) -> Result<Response<SdkBody>, HttpError> {
    response(status, Vec::new())
}

fn response(status: u16, body: Vec<u8>) -> Result<Response<SdkBody>, HttpError> {
    let mut response = Response::new(StatusCode::try_from(status)?, SdkBody::from(body));
    response
        .headers_mut()
        .insert("smithy-protocol", "rpc-v2-cbor");
    response
        .headers_mut()
        .insert("content-type", "application/cbor");
    Ok(response)
}

/// A replay client that answers each request with the next of `responses`, then fails.
pub(crate) fn replay(responses: Vec<Response<SdkBody>>) -> StaticReplayClient {
    StaticReplayClient::new(
        responses
            .into_iter()
            .map(|response| ReplayEvent::new(Request::new(SdkBody::empty()), response))
            .collect(),
    )
}

/// A `CloudWatch` client in `us-east-1` that sends through `http` with static credentials and a
/// single attempt per call. It reads no environment, profile or network.
pub(crate) fn client(http: &StaticReplayClient) -> aws_sdk_cloudwatch::Client {
    let config = aws_sdk_cloudwatch::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("AKID", "SECRET", None, None, "replay"))
        .retry_config(RetryConfig::disabled())
        .http_client(http.clone())
        .build();
    aws_sdk_cloudwatch::Client::from_conf(config)
}

/// The bodies of the requests `http` received, decoded from CBOR to JSON. Timestamps become
/// whole seconds since the Unix epoch.
pub(crate) fn requests(http: &StaticReplayClient) -> Result<Vec<Value>, DeserializeError> {
    http.actual_requests()
        .map(|request| {
            let body = request.body().bytes().unwrap_or_default();
            decode(&mut Decoder::new(body))
        })
        .collect()
}

fn decode(decoder: &mut Decoder<'_>) -> Result<Value, DeserializeError> {
    Ok(match decoder.datatype()? {
        Type::Map | Type::MapIndef => {
            let len = decoder.map()?;
            let mut object = Map::new();
            while !at_end(decoder, len, object.len())? {
                let key = decoder.string()?;
                object.insert(key, decode(decoder)?);
            }
            Value::Object(object)
        }
        Type::Array | Type::ArrayIndef => {
            let len = decoder.list()?;
            let mut items = Vec::new();
            while !at_end(decoder, len, items.len())? {
                items.push(decode(decoder)?);
            }
            Value::Array(items)
        }
        Type::String | Type::StringIndef => Value::String(decoder.string()?),
        Type::Bool => Value::Bool(decoder.boolean()?),
        Type::Tag => json!(decoder.timestamp()?.secs()),
        Type::F16 | Type::F32 | Type::F64 => json!(decoder.double()?),
        _ => json!(decoder.long()?),
    })
}

/// Whether a container of `len` items (`None` when indefinite) ends after `read` items,
/// consuming the break of an indefinite one.
fn at_end(
    decoder: &mut Decoder<'_>,
    len: Option<u64>,
    read: usize,
) -> Result<bool, DeserializeError> {
    match len {
        Some(len) => Ok(u64::try_from(read).is_ok_and(|read| read >= len)),
        None if decoder.datatype()? == Type::Break => {
            decoder.skip()?;
            Ok(true)
        }
        None => Ok(false),
    }
}
