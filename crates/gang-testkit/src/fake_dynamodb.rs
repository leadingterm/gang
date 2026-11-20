//! A local `DynamoDB` JSON endpoint for database contract tests.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::post,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use parking_lot::Mutex;
use serde_json::{Map, Value, json};
use tokio::task::JoinHandle;
use url::Url;

/// Failure to start the local `DynamoDB` endpoint.
#[derive(Debug, thiserror::Error)]
pub enum FakeDynamoError {
    /// The listener could not be opened or inspected.
    #[error("failed to bind fake dynamodb listener")]
    Listen(#[from] std::io::Error),
    /// The local listener address could not be represented as a URL.
    #[error("failed to construct fake dynamodb endpoint")]
    Endpoint(#[from] url::ParseError),
}

/// An atomic in-memory `DynamoDB` endpoint. Dropping it stops the server.
pub struct FakeDynamoDb {
    endpoint: Url,
    state: Arc<Mutex<StateData>>,
    task: JoinHandle<()>,
}

impl FakeDynamoDb {
    /// Starts an empty endpoint on an ephemeral loopback port.
    ///
    /// # Errors
    /// Returns an error if the listener or endpoint URL cannot be created.
    pub async fn start() -> std::result::Result<Self, FakeDynamoError> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = Url::parse(&format!("http://{}", listener.local_addr()?))?;
        let state = Arc::new(Mutex::new(StateData::default()));
        let app = Router::new()
            .route("/", post(serve))
            .layer(DefaultBodyLimit::max(4 * 1024 * 1024))
            .with_state(Arc::clone(&state));
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self {
            endpoint,
            state,
            task,
        })
    }

    /// Returns the endpoint URL for SDK client configuration.
    #[must_use]
    pub fn endpoint(&self) -> Url {
        self.endpoint.clone()
    }

    /// Enables or disables HTTP 500 responses for all requests.
    pub fn set_unavailable(&self, unavailable: bool) {
        self.state.lock().unavailable = unavailable;
    }

    /// Returns the number of requests received, including outage responses.
    #[must_use]
    pub fn requests(&self) -> u64 {
        self.state.lock().requests
    }

    /// Clones one stored item without issuing a request or changing database state.
    ///
    /// Missing tables and keys return `None`, including while requests are unavailable.
    #[must_use]
    pub fn item(&self, table: &str, pk: &str, sk: &str) -> Option<Map<String, Value>> {
        self.state
            .lock()
            .db
            .tables
            .get(table)?
            .items
            .get(&(pk.to_owned(), sk.to_owned()))
            .cloned()
    }

    /// Returns the number of Query requests received.
    #[must_use]
    pub fn queries(&self) -> u64 {
        self.state.lock().queries
    }

    /// Cancels the next transaction with the supplied AWS cancellation reason codes.
    pub fn cancel_next_transaction(&self, reasons: &[&str]) {
        self.state.lock().cancellation = Some(reasons.iter().map(ToString::to_string).collect());
    }
    /// Cancels the next `count` transactions touching the specified item with a conflict.
    pub fn conflict_transactions_on(&self, pk: &str, sk: &str, count: usize) {
        self.state.lock().conflict = Some(((pk.to_owned(), sk.to_owned()), count));
    }

    /// Fails requests after the supplied total count, bounding tests of retry loops.
    pub fn fail_after_requests(&self, count: u64) {
        self.state.lock().request_limit = Some(count);
    }
}

impl Drop for FakeDynamoDb {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Default)]
struct StateData {
    db: Database,
    unavailable: bool,
    requests: u64,
    queries: u64,
    cancellation: Option<Vec<String>>,
    conflict: Option<(Key, usize)>,
    request_limit: Option<u64>,
}

async fn serve(
    State(state): State<Arc<Mutex<StateData>>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, Json<Value>) {
    let mut state = state.lock();
    state.requests = state.requests.saturating_add(1);
    if state
        .request_limit
        .is_some_and(|limit| state.requests > limit)
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                json!({"__type": format!("{ERROR_PREFIX}{VALIDATION_ERROR}"), "message": "fake request limit reached"}),
            ),
        );
    }
    if state.unavailable {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "__type": format!("{ERROR_PREFIX}{INTERNAL_ERROR}"),
                "message": "fake dynamodb unavailable"
            })),
        );
    }
    let operation = headers
        .get("x-amz-target")
        .and_then(|value| value.to_str().ok())
        .and_then(|target| target.strip_prefix(TARGET_PREFIX));
    if operation == Some(QUERY) {
        state.queries = state.queries.saturating_add(1);
    }
    if operation == Some(TRANSACT_WRITE_ITEMS)
        && let Some(reasons) = state.cancellation.take()
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "__type": format!("{ERROR_PREFIX}{TRANSACTION_ERROR}"),
                "message": "transaction cancelled",
                "CancellationReasons": reasons.into_iter().map(|code| json!({"Code": code})).collect::<Vec<_>>()
            })),
        );
    }
    if operation == Some(TRANSACT_WRITE_ITEMS)
        && let (Ok(body), Some((wanted, remaining))) = (
            serde_json::from_slice::<Value>(&body),
            state.conflict.as_mut(),
        )
        && *remaining > 0
        && let Some(actions) = body.get("TransactItems").and_then(Value::as_array)
        && let Some(conflicted) = actions.iter().position(|action| {
            action
                .as_object()
                .and_then(|object| object.iter().next())
                .is_some_and(|(kind, request)| {
                    let source = if kind == "Put" { "Item" } else { "Key" };
                    object(request, source)
                        .and_then(key)
                        .is_ok_and(|item| &item == wanted)
                })
        })
    {
        *remaining -= 1;
        let reasons: Vec<_> = (0..actions.len()).map(|index| {
            json!({"Code": if index == conflicted { "TransactionConflict" } else { "None" }})
        }).collect();
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "__type": format!("{ERROR_PREFIX}{TRANSACTION_ERROR}"),
                "message": "transaction cancelled",
                "CancellationReasons": reasons
            })),
        );
    }
    let parsed = serde_json::from_slice::<Value>(&body);
    let (status, response) = match (operation, parsed) {
        (Some(operation), Ok(body)) => state.db.handle(operation, &body),
        _ => (
            StatusCode::BAD_REQUEST,
            json!({
                "__type": format!("{ERROR_PREFIX}{VALIDATION_ERROR}"),
                "message": "invalid request"
            }),
        ),
    };
    (status, Json(response))
}

type Item = Map<String, Value>;
type Key = (String, String);
type Values = Map<String, Value>;
type Result<T> = std::result::Result<T, Error>;

const TARGET_PREFIX: &str = "DynamoDB_20120810.";
const ERROR_PREFIX: &str = "com.amazonaws.dynamodb.v20120810#";
const VALIDATION_ERROR: &str = "ValidationException";
const CONDITION_ERROR: &str = "ConditionalCheckFailedException";
const TRANSACTION_ERROR: &str = "TransactionCanceledException";
const INTERNAL_ERROR: &str = "InternalServerError";
const GET_ITEM: &str = "GetItem";
const PUT_ITEM: &str = "PutItem";
const UPDATE_ITEM: &str = "UpdateItem";
const DELETE_ITEM: &str = "DeleteItem";
const QUERY: &str = "Query";
const TRANSACT_WRITE_ITEMS: &str = "TransactWriteItems";

#[derive(Debug)]
enum Error {
    Validation,
    FilterKey(String),
    Conditional,
    Transaction(Vec<bool>),
}

impl Error {
    fn response(self) -> (StatusCode, Value) {
        let (kind, message) = match &self {
            Self::Validation => (VALIDATION_ERROR, "invalid request or expression"),
            Self::FilterKey(key) => (
                VALIDATION_ERROR,
                if key == "pk" {
                    "Filter Expression can only contain non-primary key attributes: Primary key attribute: pk"
                } else {
                    "Filter Expression can only contain non-primary key attributes: Primary key attribute: sk"
                },
            ),
            Self::Conditional => (CONDITION_ERROR, "condition failed"),
            Self::Transaction(_) => (TRANSACTION_ERROR, "transaction cancelled"),
        };
        let mut response = json!({"__type": format!("{ERROR_PREFIX}{kind}"), "message": message});
        if let Self::Transaction(reasons) = self {
            response["CancellationReasons"] = Value::Array(reasons.into_iter().map(|failed| {
                json!({"Code": if failed { "ConditionalCheckFailed" } else { "None" }})
            }).collect());
        }
        (StatusCode::BAD_REQUEST, response)
    }
}

#[derive(Clone, Debug, Default)]
struct Table {
    items: BTreeMap<Key, Item>,
}

#[derive(Clone, Debug, Default)]
struct Database {
    tables: BTreeMap<String, Table>,
}

impl Database {
    fn handle(&mut self, operation: &str, request: &Value) -> (StatusCode, Value) {
        match self.execute(operation, request) {
            Ok(response) => (StatusCode::OK, response),
            Err(error) => error.response(),
        }
    }

    fn execute(&mut self, operation: &str, request: &Value) -> Result<Value> {
        if operation == TRANSACT_WRITE_ITEMS {
            return self.transaction(request);
        }
        let name = string(request, "TableName")?;
        let table = self.tables.entry(name.to_owned()).or_default();
        match operation {
            GET_ITEM => get_item(table, request),
            PUT_ITEM => put_item(table, request),
            UPDATE_ITEM => update_item(table, request),
            DELETE_ITEM => delete_item(table, request),
            QUERY => query(table, request),
            _ => Err(Error::Validation),
        }
    }

    fn transaction(&mut self, request: &Value) -> Result<Value> {
        let actions = request
            .get("TransactItems")
            .and_then(Value::as_array)
            .filter(|items| !items.is_empty())
            .ok_or(Error::Validation)?;
        let mut staged = self.tables.clone();
        let mut reasons = Vec::with_capacity(actions.len());
        let mut seen = BTreeSet::new();
        for action in actions {
            match transaction_action(&mut staged, &mut seen, action) {
                Ok(()) => reasons.push(false),
                Err(Error::Conditional) => reasons.push(true),
                Err(error) => return Err(error),
            }
        }
        if reasons.iter().any(|failed| *failed) {
            return Err(Error::Transaction(reasons));
        }
        self.tables = staged;
        Ok(json!({}))
    }
}

fn transaction_action(
    tables: &mut BTreeMap<String, Table>,
    seen: &mut BTreeSet<(String, Key)>,
    action: &Value,
) -> Result<()> {
    let entry = action
        .as_object()
        .filter(|entry| entry.len() == 1)
        .ok_or(Error::Validation)?;
    let (kind, request) = entry.iter().next().ok_or(Error::Validation)?;
    let name = string(request, "TableName")?;
    let source = if kind == "Put" { "Item" } else { "Key" };
    let item_key = key(object(request, source)?)?;
    if !seen.insert((name.to_owned(), item_key.clone())) {
        return Err(Error::Validation);
    }
    let table = tables.entry(name.to_owned()).or_default();
    match kind.as_str() {
        "Put" => {
            put_item(table, request)?;
        }
        "Update" => {
            update_item(table, request)?;
        }
        "Delete" => {
            delete_item(table, request)?;
        }
        "ConditionCheck" => {
            let empty = Item::new();
            let item = table.items.get(&item_key).unwrap_or(&empty);
            check_condition(request, item, true)?;
        }
        _ => return Err(Error::Validation),
    }
    Ok(())
}

fn string<'a>(request: &'a Value, field: &str) -> Result<&'a str> {
    request
        .get(field)
        .and_then(Value::as_str)
        .ok_or(Error::Validation)
}

fn object<'a>(request: &'a Value, field: &str) -> Result<&'a Item> {
    request
        .get(field)
        .and_then(Value::as_object)
        .ok_or(Error::Validation)
}

fn key(item: &Item) -> Result<Key> {
    let part = |name: &str| {
        item.get(name)
            .and_then(|value| value.get("S"))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .ok_or(Error::Validation)
    };
    Ok((part("pk")?, part("sk")?))
}

fn key_item(key: &Key) -> Item {
    let mut item = Item::new();
    item.insert("pk".to_owned(), json!({"S": key.0}));
    item.insert("sk".to_owned(), json!({"S": key.1}));
    item
}

fn values(request: &Value) -> Result<Values> {
    let Some(value) = request.get("ExpressionAttributeValues") else {
        return Ok(Values::new());
    };
    let values = value.as_object().ok_or(Error::Validation)?;
    for value in values.values() {
        validate_value(value)?;
    }
    Ok(values.clone())
}

fn validate_value(value: &Value) -> Result<()> {
    let object = value
        .as_object()
        .filter(|object| object.len() == 1)
        .ok_or(Error::Validation)?;
    let (kind, value) = object.iter().next().ok_or(Error::Validation)?;
    match kind.as_str() {
        "S" if value.is_string() => Ok(()),
        "N" => {
            Decimal::parse(value.as_str().ok_or(Error::Validation)?)?;
            Ok(())
        }
        "B" => {
            STANDARD
                .decode(value.as_str().ok_or(Error::Validation)?)
                .map_err(|_| Error::Validation)?;
            Ok(())
        }
        "BOOL" if value.is_boolean() => Ok(()),
        _ => Err(Error::Validation),
    }
}

// DynamoDB numbers are decimal; comparisons and arithmetic must not round through floats.
#[derive(Clone, Copy)]
struct Decimal {
    coefficient: i128,
    scale: i32,
}

impl Decimal {
    fn parse(input: &str) -> Result<Self> {
        let (mantissa, exponent) = match input.find(['e', 'E']) {
            Some(index) => (
                &input[..index],
                input[index + 1..]
                    .parse::<i32>()
                    .map_err(|_| Error::Validation)?,
            ),
            None => (input, 0),
        };
        if !(-130..=125).contains(&exponent) {
            return Err(Error::Validation);
        }
        let negative = mantissa.starts_with('-');
        let unsigned = mantissa.strip_prefix(['-', '+']).unwrap_or(mantissa);
        let mut pieces = unsigned.split('.');
        let integer = pieces.next().ok_or(Error::Validation)?;
        let fraction = pieces.next().unwrap_or("");
        if pieces.next().is_some()
            || (integer.is_empty() && fraction.is_empty())
            || !integer
                .bytes()
                .chain(fraction.bytes())
                .all(|byte| byte.is_ascii_digit())
        {
            return Err(Error::Validation);
        }
        let mut coefficient = format!("{integer}{fraction}")
            .parse::<i128>()
            .map_err(|_| Error::Validation)?;
        if negative {
            coefficient = -coefficient;
        }
        let scale = i32::try_from(fraction.len())
            .map_err(|_| Error::Validation)?
            .checked_sub(exponent)
            .ok_or(Error::Validation)?;
        Ok(Self { coefficient, scale }.normal())
    }

    fn normal(mut self) -> Self {
        if self.coefficient == 0 {
            self.scale = 0;
            return self;
        }
        while self.coefficient % 10 == 0 {
            self.coefficient /= 10;
            self.scale -= 1;
        }
        self
    }

    fn add(self, other: Self) -> Result<Self> {
        if self.coefficient == 0 {
            return Ok(other);
        }
        if other.coefficient == 0 {
            return Ok(self);
        }
        let scale = self.scale.max(other.scale);
        let align = |number: Self| -> Result<i128> {
            let difference = scale.checked_sub(number.scale).ok_or(Error::Validation)?;
            let power = u32::try_from(difference).map_err(|_| Error::Validation)?;
            let factor = 10_i128.checked_pow(power).ok_or(Error::Validation)?;
            number
                .coefficient
                .checked_mul(factor)
                .ok_or(Error::Validation)
        };
        let coefficient = align(self)?
            .checked_add(align(other)?)
            .ok_or(Error::Validation)?;
        Ok(Self { coefficient, scale }.normal())
    }

    fn compare(self, other: Self) -> Ordering {
        let left = self.normal();
        let right = other.normal();
        if left.coefficient.signum() != right.coefficient.signum() {
            return left.coefficient.signum().cmp(&right.coefficient.signum());
        }
        if left.coefficient == 0 {
            return Ordering::Equal;
        }
        let left_digits = left.coefficient.unsigned_abs().to_string();
        let right_digits = right.coefficient.unsigned_abs().to_string();
        let left_exp = i64::try_from(left_digits.len()).unwrap_or(i64::MAX) - i64::from(left.scale);
        let right_exp =
            i64::try_from(right_digits.len()).unwrap_or(i64::MAX) - i64::from(right.scale);
        let width = left_digits.len().max(right_digits.len());
        let magnitude = left_exp.cmp(&right_exp).then_with(|| {
            left_digits
                .bytes()
                .chain(std::iter::repeat(b'0'))
                .take(width)
                .cmp(
                    right_digits
                        .bytes()
                        .chain(std::iter::repeat(b'0'))
                        .take(width),
                )
        });
        if left.coefficient < 0 {
            magnitude.reverse()
        } else {
            magnitude
        }
    }

    fn value(self) -> Value {
        let mut digits = self.coefficient.unsigned_abs().to_string();
        if self.scale <= 0 {
            digits.push_str(&"0".repeat(usize::try_from(-i64::from(self.scale)).unwrap_or(0)));
        } else {
            let scale = usize::try_from(self.scale).unwrap_or(0);
            if scale >= digits.len() {
                digits = format!("0.{}{digits}", "0".repeat(scale - digits.len()));
            } else {
                digits.insert(digits.len() - scale, '.');
            }
        }
        if self.coefficient < 0 {
            digits.insert(0, '-');
        }
        json!({"N": digits})
    }
}

fn number(value: &Value) -> Result<Decimal> {
    Decimal::parse(
        value
            .get("N")
            .and_then(Value::as_str)
            .ok_or(Error::Validation)?,
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Token {
    Word(String),
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Left,
    Right,
    Comma,
    Plus,
}

fn tokenize(input: &str) -> Result<Vec<Token>> {
    let mut chars = input.chars().peekable();
    let mut tokens = Vec::new();
    while let Some(ch) = chars.next() {
        if ch.is_whitespace() {
            continue;
        }
        let token = match ch {
            '=' => Token::Eq,
            '(' => Token::Left,
            ')' => Token::Right,
            ',' => Token::Comma,
            '+' => Token::Plus,
            '<' => match chars.peek() {
                Some('=') => {
                    chars.next();
                    Token::Le
                }
                Some('>') => {
                    chars.next();
                    Token::Ne
                }
                _ => Token::Lt,
            },
            '>' => {
                if chars.peek() == Some(&'=') {
                    chars.next();
                    Token::Ge
                } else {
                    Token::Gt
                }
            }
            ch if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '#' | ':') => {
                let mut word = String::from(ch);
                while chars
                    .peek()
                    .is_some_and(|next| next.is_ascii_alphanumeric() || *next == '_')
                {
                    if let Some(next) = chars.next() {
                        word.push(next);
                    }
                }
                Token::Word(word)
            }
            _ => return Err(Error::Validation),
        };
        tokens.push(token);
    }
    Ok(tokens)
}

#[derive(Clone)]
enum Operand {
    Attribute(String),
    Value(String),
}

#[derive(Clone, Copy)]
enum Comparison {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone)]
enum Expr {
    Exists(String),
    NotExists(String),
    Compare(Comparison, Operand, Operand),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Not(Box<Self>),
}

impl Expr {
    fn key_attribute(&self) -> Option<&str> {
        fn key(name: &str) -> Option<&str> {
            matches!(name, "pk" | "sk").then_some(name)
        }
        match self {
            Self::Exists(name) | Self::NotExists(name) => key(name),
            Self::Compare(_, left, right) => [left, right].into_iter().find_map(|operand| {
                if let Operand::Attribute(name) = operand {
                    key(name)
                } else {
                    None
                }
            }),
            Self::And(left, right) | Self::Or(left, right) => {
                left.key_attribute().or_else(|| right.key_attribute())
            }
            Self::Not(expr) => expr.key_attribute(),
        }
    }
}

struct Parser<'a> {
    tokens: Vec<Token>,
    position: usize,
    request: &'a Value,
}

impl<'a> Parser<'a> {
    fn new(input: &str, request: &'a Value) -> Result<Self> {
        Ok(Self {
            tokens: tokenize(input)?,
            position: 0,
            request,
        })
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.position)
    }

    fn take(&mut self) -> Option<Token> {
        let token = self.peek().cloned();
        if token.is_some() {
            self.position += 1;
        }
        token
    }

    fn consume(&mut self, token: &Token) -> bool {
        if self.peek() == Some(token) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn word(&mut self, expected: &str) -> bool {
        if matches!(self.peek(), Some(Token::Word(word)) if word.eq_ignore_ascii_case(expected)) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn require(&mut self, token: &Token) -> Result<()> {
        if self.consume(token) {
            Ok(())
        } else {
            Err(Error::Validation)
        }
    }

    fn finish(&self) -> Result<()> {
        if self.position == self.tokens.len() {
            Ok(())
        } else {
            Err(Error::Validation)
        }
    }

    fn path(&mut self) -> Result<String> {
        let Some(Token::Word(word)) = self.take() else {
            return Err(Error::Validation);
        };
        if word.starts_with(':') {
            return Err(Error::Validation);
        }
        if word.starts_with('#') {
            let name = self
                .request
                .get("ExpressionAttributeNames")
                .and_then(|names| names.get(&word))
                .and_then(Value::as_str)
                .filter(|name| !name.is_empty())
                .ok_or(Error::Validation)?;
            Ok(name.to_owned())
        } else {
            Ok(word)
        }
    }

    fn value(&mut self) -> Result<String> {
        let Some(Token::Word(word)) = self.take() else {
            return Err(Error::Validation);
        };
        if !word.starts_with(':') || word.len() == 1 {
            return Err(Error::Validation);
        }
        if self
            .request
            .get("ExpressionAttributeValues")
            .and_then(|values| values.get(&word))
            .is_none()
        {
            return Err(Error::Validation);
        }
        Ok(word)
    }

    fn operand(&mut self) -> Result<Operand> {
        if matches!(self.peek(), Some(Token::Word(word)) if word.starts_with(':')) {
            Ok(Operand::Value(self.value()?))
        } else {
            Ok(Operand::Attribute(self.path()?))
        }
    }

    fn comparison(&mut self) -> Result<Comparison> {
        match self.take() {
            Some(Token::Eq) => Ok(Comparison::Eq),
            Some(Token::Ne) => Ok(Comparison::Ne),
            Some(Token::Lt) => Ok(Comparison::Lt),
            Some(Token::Le) => Ok(Comparison::Le),
            Some(Token::Gt) => Ok(Comparison::Gt),
            Some(Token::Ge) => Ok(Comparison::Ge),
            _ => Err(Error::Validation),
        }
    }

    fn expr(&mut self) -> Result<Expr> {
        self.or()
    }

    fn or(&mut self) -> Result<Expr> {
        let mut expr = self.and()?;
        while self.word("OR") {
            expr = Expr::Or(Box::new(expr), Box::new(self.and()?));
        }
        Ok(expr)
    }

    fn and(&mut self) -> Result<Expr> {
        let mut expr = self.not()?;
        while self.word("AND") {
            expr = Expr::And(Box::new(expr), Box::new(self.not()?));
        }
        Ok(expr)
    }

    fn not(&mut self) -> Result<Expr> {
        if self.word("NOT") {
            Ok(Expr::Not(Box::new(self.not()?)))
        } else {
            self.atom()
        }
    }

    fn atom(&mut self) -> Result<Expr> {
        if self.consume(&Token::Left) {
            let expr = self.expr()?;
            self.require(&Token::Right)?;
            return Ok(expr);
        }
        for exists in [true, false] {
            let name = if exists {
                "attribute_exists"
            } else {
                "attribute_not_exists"
            };
            if self.word(name) {
                self.require(&Token::Left)?;
                let path = self.path()?;
                self.require(&Token::Right)?;
                return Ok(if exists {
                    Expr::Exists(path)
                } else {
                    Expr::NotExists(path)
                });
            }
        }
        let left = self.operand()?;
        let op = self.comparison()?;
        Ok(Expr::Compare(op, left, self.operand()?))
    }
}

fn parse_condition(expression: &str, request: &Value) -> Result<Expr> {
    let mut parser = Parser::new(expression, request)?;
    let expression = parser.expr()?;
    parser.finish()?;
    Ok(expression)
}

fn operand_value<'a>(operand: &Operand, item: &'a Item, values: &'a Values) -> Option<&'a Value> {
    match operand {
        Operand::Attribute(name) => item.get(name),
        Operand::Value(name) => values.get(name),
    }
}

fn compare(left: &Value, right: &Value) -> Option<Ordering> {
    if let (Some(left), Some(right)) = (left.get("N"), right.get("N")) {
        return Some(
            Decimal::parse(left.as_str()?)
                .ok()?
                .compare(Decimal::parse(right.as_str()?).ok()?),
        );
    }
    if let (Some(left), Some(right)) = (left.get("S"), right.get("S")) {
        return Some(left.as_str()?.as_bytes().cmp(right.as_str()?.as_bytes()));
    }
    if let (Some(left), Some(right)) = (left.get("B"), right.get("B")) {
        return Some(
            STANDARD
                .decode(left.as_str()?)
                .ok()?
                .cmp(&STANDARD.decode(right.as_str()?).ok()?),
        );
    }
    if let (Some(left), Some(right)) = (left.get("BOOL"), right.get("BOOL")) {
        return Some(left.as_bool()?.cmp(&right.as_bool()?));
    }
    None
}

fn evaluate(expr: &Expr, item: &Item, values: &Values) -> bool {
    match expr {
        Expr::Exists(path) => item.contains_key(path),
        Expr::NotExists(path) => !item.contains_key(path),
        Expr::And(left, right) => evaluate(left, item, values) && evaluate(right, item, values),
        Expr::Or(left, right) => evaluate(left, item, values) || evaluate(right, item, values),
        Expr::Not(inner) => !evaluate(inner, item, values),
        Expr::Compare(op, left, right) => {
            let ordering = operand_value(left, item, values)
                .zip(operand_value(right, item, values))
                .and_then(|(left, right)| compare(left, right));
            match op {
                Comparison::Eq => ordering == Some(Ordering::Equal),
                Comparison::Ne => matches!(ordering, Some(Ordering::Less | Ordering::Greater)),
                Comparison::Lt => ordering == Some(Ordering::Less),
                Comparison::Le => matches!(ordering, Some(Ordering::Less | Ordering::Equal)),
                Comparison::Gt => ordering == Some(Ordering::Greater),
                Comparison::Ge => matches!(ordering, Some(Ordering::Greater | Ordering::Equal)),
            }
        }
    }
}

fn condition(expression: &str, request: &Value, item: &Item) -> Result<bool> {
    let expression = parse_condition(expression, request)?;
    Ok(evaluate(&expression, item, &values(request)?))
}

fn check_condition(request: &Value, item: &Item, required: bool) -> Result<()> {
    let Some(expression) = request.get("ConditionExpression") else {
        return if required {
            Err(Error::Validation)
        } else {
            Ok(())
        };
    };
    if condition(expression.as_str().ok_or(Error::Validation)?, request, item)? {
        Ok(())
    } else {
        Err(Error::Conditional)
    }
}

struct KeyCondition {
    partition: String,
    sort: Option<SortCondition>,
}

enum SortCondition {
    Less(String),
    Greater(String),
    Between(String, String),
    Begins(String),
}

impl KeyCondition {
    fn parse(request: &Value) -> Result<Self> {
        let mut parser = Parser::new(string(request, "KeyConditionExpression")?, request)?;
        if parser.path()? != "pk" {
            return Err(Error::Validation);
        }
        parser.require(&Token::Eq)?;
        let partition = parser.value()?;
        let sort = if parser.word("AND") {
            Some(parse_sort_condition(&mut parser)?)
        } else {
            None
        };
        parser.finish()?;
        Ok(Self { partition, sort })
    }

    fn matches(&self, key: &Key, values: &Values) -> Result<bool> {
        if key.0 != string_value(values, &self.partition)? {
            return Ok(false);
        }
        match &self.sort {
            None => Ok(true),
            Some(SortCondition::Less(value)) => Ok(key.1.as_str() < string_value(values, value)?),
            Some(SortCondition::Greater(value)) => {
                Ok(key.1.as_str() > string_value(values, value)?)
            }
            Some(SortCondition::Between(low, high)) => Ok(key.1.as_str()
                >= string_value(values, low)?
                && key.1.as_str() <= string_value(values, high)?),
            Some(SortCondition::Begins(value)) => {
                Ok(key.1.starts_with(string_value(values, value)?))
            }
        }
    }
}

fn parse_sort_condition(parser: &mut Parser<'_>) -> Result<SortCondition> {
    if parser.word("begins_with") {
        parser.require(&Token::Left)?;
        if parser.path()? != "sk" {
            return Err(Error::Validation);
        }
        parser.require(&Token::Comma)?;
        let value = parser.value()?;
        parser.require(&Token::Right)?;
        return Ok(SortCondition::Begins(value));
    }
    if parser.path()? != "sk" {
        return Err(Error::Validation);
    }
    if parser.word("BETWEEN") {
        let low = parser.value()?;
        if !parser.word("AND") {
            return Err(Error::Validation);
        }
        return Ok(SortCondition::Between(low, parser.value()?));
    }
    match parser.take() {
        Some(Token::Lt) => Ok(SortCondition::Less(parser.value()?)),
        Some(Token::Gt) => Ok(SortCondition::Greater(parser.value()?)),
        _ => Err(Error::Validation),
    }
}

fn string_value<'a>(values: &'a Values, name: &str) -> Result<&'a str> {
    values
        .get(name)
        .and_then(|value| value.get("S"))
        .and_then(Value::as_str)
        .ok_or(Error::Validation)
}

#[derive(Clone)]
enum UpdateAction {
    Set(String, String),
    AddTo(String, String, String),
    Remove(String),
    Add(String, String),
}

fn parse_updates(request: &Value) -> Result<Vec<UpdateAction>> {
    let mut parser = Parser::new(string(request, "UpdateExpression")?, request)?;
    let mut actions = Vec::new();
    let mut clauses = BTreeSet::new();
    let mut paths = BTreeSet::new();
    while parser.peek().is_some() {
        let clause = if parser.word("SET") {
            "SET"
        } else if parser.word("REMOVE") {
            "REMOVE"
        } else if parser.word("ADD") {
            "ADD"
        } else {
            return Err(Error::Validation);
        };
        if !clauses.insert(clause) {
            return Err(Error::Validation);
        }
        loop {
            let path = parser.path()?;
            if path == "pk" || path == "sk" || !paths.insert(path.clone()) {
                return Err(Error::Validation);
            }
            actions.push(parse_update_action(&mut parser, clause, path)?);
            if !parser.consume(&Token::Comma) {
                break;
            }
        }
    }
    if actions.is_empty() {
        return Err(Error::Validation);
    }
    Ok(actions)
}

fn parse_update_action(
    parser: &mut Parser<'_>,
    clause: &str,
    path: String,
) -> Result<UpdateAction> {
    match clause {
        "REMOVE" => Ok(UpdateAction::Remove(path)),
        "ADD" => Ok(UpdateAction::Add(path, parser.value()?)),
        "SET" => {
            parser.require(&Token::Eq)?;
            match parser.operand()? {
                Operand::Value(value) => Ok(UpdateAction::Set(path, value)),
                Operand::Attribute(source) => {
                    parser.require(&Token::Plus)?;
                    Ok(UpdateAction::AddTo(path, source, parser.value()?))
                }
            }
        }
        _ => Err(Error::Validation),
    }
}

fn apply_updates(item: &mut Item, actions: &[UpdateAction], values: &Values) -> Result<()> {
    let original = item.clone();
    for action in actions {
        match action {
            UpdateAction::Set(path, value) => {
                item.insert(
                    path.clone(),
                    values.get(value).ok_or(Error::Validation)?.clone(),
                );
            }
            UpdateAction::Remove(path) => {
                item.remove(path);
            }
            UpdateAction::AddTo(path, source, value) => {
                let left = number(original.get(source).ok_or(Error::Validation)?)?;
                let right = number(values.get(value).ok_or(Error::Validation)?)?;
                item.insert(path.clone(), left.add(right)?.value());
            }
            UpdateAction::Add(path, value) => {
                let left = original
                    .get(path)
                    .map(number)
                    .transpose()?
                    .unwrap_or(Decimal {
                        coefficient: 0,
                        scale: 0,
                    });
                let right = number(values.get(value).ok_or(Error::Validation)?)?;
                item.insert(path.clone(), left.add(right)?.value());
            }
        }
    }
    Ok(())
}

fn get_item(table: &Table, request: &Value) -> Result<Value> {
    let key = key(object(request, "Key")?)?;
    Ok(match table.items.get(&key) {
        Some(item) => json!({"Item": item}),
        None => json!({}),
    })
}

fn put_item(table: &mut Table, request: &Value) -> Result<Value> {
    let item = object(request, "Item")?;
    let key = key(item)?;
    for value in item.values() {
        validate_value(value)?;
    }
    let old = table.items.get(&key);
    let empty = Item::new();
    check_condition(request, old.unwrap_or(&empty), false)?;
    let response = return_values(request, old, None)?;
    table.items.insert(key, item.clone());
    Ok(response)
}

fn update_item(table: &mut Table, request: &Value) -> Result<Value> {
    let key = key(object(request, "Key")?)?;
    let old = table.items.get(&key);
    let empty = Item::new();
    check_condition(request, old.unwrap_or(&empty), false)?;
    let mut new = old.cloned().unwrap_or_else(|| key_item(&key));
    apply_updates(&mut new, &parse_updates(request)?, &values(request)?)?;
    let response = return_values(request, old, Some(&new))?;
    table.items.insert(key, new);
    Ok(response)
}

fn delete_item(table: &mut Table, request: &Value) -> Result<Value> {
    let key = key(object(request, "Key")?)?;
    let old = table.items.get(&key);
    let empty = Item::new();
    check_condition(request, old.unwrap_or(&empty), false)?;
    let response = return_values(request, old, None)?;
    table.items.remove(&key);
    Ok(response)
}

fn return_values(request: &Value, old: Option<&Item>, new: Option<&Item>) -> Result<Value> {
    let mode = request
        .get("ReturnValues")
        .map(|value| value.as_str().ok_or(Error::Validation))
        .transpose()?
        .unwrap_or("NONE");
    let item = match mode {
        "NONE" => None,
        "ALL_OLD" => old,
        "ALL_NEW" if new.is_some() => new,
        _ => return Err(Error::Validation),
    };
    Ok(match item {
        Some(item) => json!({"Attributes": item}),
        None => json!({}),
    })
}

fn query(table: &Table, request: &Value) -> Result<Value> {
    let key_condition = KeyCondition::parse(request)?;
    let values = values(request)?;
    let filter = request
        .get("FilterExpression")
        .map(|value| parse_condition(value.as_str().ok_or(Error::Validation)?, request))
        .transpose()?;
    if let Some(key) = filter.as_ref().and_then(Expr::key_attribute) {
        return Err(Error::FilterKey(key.to_owned()));
    }
    let limit = request
        .get("Limit")
        .map(|value| {
            value
                .as_u64()
                .filter(|limit| *limit > 0)
                .ok_or(Error::Validation)
        })
        .transpose()?
        .unwrap_or(u64::MAX);
    let forward = request
        .get("ScanIndexForward")
        .map(|value| value.as_bool().ok_or(Error::Validation))
        .transpose()?
        .unwrap_or(true);
    let start = request
        .get("ExclusiveStartKey")
        .map(|value| value.as_object().ok_or(Error::Validation).and_then(key))
        .transpose()?;
    let rows: Box<dyn Iterator<Item = (&Key, &Item)> + '_> = if forward {
        Box::new(table.items.iter())
    } else {
        Box::new(table.items.iter().rev())
    };
    let mut items = Vec::new();
    let mut scanned = 0_u64;
    let mut last = None;
    for (key, item) in rows {
        if start
            .as_ref()
            .is_some_and(|start| if forward { key <= start } else { key >= start })
        {
            continue;
        }
        if !key_condition.matches(key, &values)? {
            continue;
        }
        scanned += 1;
        last = Some(key);
        if filter
            .as_ref()
            .is_none_or(|filter| evaluate(filter, item, &values))
        {
            items.push(item);
        }
        if scanned == limit {
            break;
        }
    }
    let mut response = json!({"Count": items.len(), "ScannedCount": scanned, "Items": items});
    if scanned == limit
        && let Some(key) = last
    {
        response["LastEvaluatedKey"] = Value::Object(key_item(key));
    }
    Ok(response)
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::TestResult;

    fn call(db: &mut Database, operation: &str, body: &Value) -> Value {
        let (status, response) = db.handle(operation, body);
        assert_eq!(status, StatusCode::OK, "{response}");
        response
    }

    fn put(db: &mut Database, sort_key: &str, number: &str) {
        call(
            db,
            "PutItem",
            &json!({
                "TableName": "t",
                "Item": {"pk": {"S": "p"}, "sk": {"S": sort_key}, "n": {"N": number}}
            }),
        );
    }

    #[tokio::test]
    async fn item_snapshot_is_independent_and_never_mutates_state() -> TestResult {
        let mut db = Database::default();
        put(&mut db, "one", "1");
        put(&mut db, "two", "2");
        let fake = FakeDynamoDb {
            endpoint: Url::parse("http://127.0.0.1:1")?,
            state: Arc::new(Mutex::new(StateData {
                db,
                unavailable: true,
                requests: 7,
                queries: 3,
                ..StateData::default()
            })),
            task: tokio::spawn(std::future::pending()),
        };
        assert!(fake.item("missing", "p", "one").is_none());
        assert!(fake.item("t", "missing", "one").is_none());
        assert!(fake.item("t", "p", "missing").is_none());
        let mut snapshot = fake.item("t", "p", "one").ok_or("missing snapshot")?;
        assert_eq!(snapshot["n"], json!({"N": "1"}));
        snapshot.insert("n".into(), json!({"N": "99"}));
        assert_eq!(
            fake.item("t", "p", "one").ok_or("missing item")?["n"],
            json!({"N": "1"})
        );
        assert_eq!(
            fake.item("t", "p", "two").ok_or("missing item")?["n"],
            json!({"N": "2"})
        );
        let state = fake.state.lock();
        assert!(state.unavailable);
        assert_eq!(state.requests, 7);
        assert_eq!(state.queries, 3);
        assert_eq!(state.db.tables.len(), 1);
        assert_eq!(state.db.tables["t"].items.len(), 2);
        Ok(())
    }

    #[test]
    fn conditions_handle_precedence_types_and_missing_values() -> TestResult {
        let request = json!({
            "ExpressionAttributeNames": {"#n": "n"},
            "ExpressionAttributeValues": {
                ":ten": {"N": "10"}, ":two": {"N": "2"},
                ":blob": {"B": "AAE="}, ":yes": {"BOOL": true}
            }
        });
        let item: Item = serde_json::from_value(json!({
            "n": {"N": "2.0"}, "blob": {"B": "AAE="}, "flag": {"BOOL": true}
        }))?;
        assert!(
            condition(
                "#n = :two AND NOT (#n >= :ten) OR attribute_exists(missing)",
                &request,
                &item
            )
            .is_ok_and(|v| v)
        );
        assert!(
            condition(
                "blob = :blob AND flag = :yes AND attribute_not_exists(missing)",
                &request,
                &item
            )
            .is_ok_and(|v| v)
        );
        assert!(condition("missing <> :two", &request, &item).is_ok_and(|v| !v));
        assert!(parse_condition("#n = :ten trailing", &request).is_err());
        assert!(parse_condition("(#n = :ten", &request).is_err());
        Ok(())
    }

    #[test]
    fn query_rejects_primary_keys_in_filters() {
        let mut db = Database::default();
        for attribute in ["pk", "sk"] {
            let request = json!({
                "TableName": "t",
                "KeyConditionExpression": "#pk = :p",
                "FilterExpression": "attribute_exists(#key)",
                "ExpressionAttributeNames": {"#pk": "pk", "#key": attribute},
                "ExpressionAttributeValues": {":p": {"S": "p"}}
            });
            let (status, response) = db.handle(QUERY, &request);
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(
                response["__type"],
                format!("{ERROR_PREFIX}{VALIDATION_ERROR}")
            );
            assert_eq!(
                response["message"],
                format!(
                    "Filter Expression can only contain non-primary key attributes: Primary key attribute: {attribute}"
                )
            );
        }
    }

    #[test]
    fn query_limits_evaluated_items_before_filtering() -> TestResult {
        let mut db = Database::default();
        put(&mut db, "a", "1");
        put(&mut db, "b", "2");
        put(&mut db, "c", "3");
        let mut query: Value = serde_json::from_str(
            r#"{
            "TableName": "t", "KeyConditionExpression": "pk = :p",
            "FilterExpression": "n > :one",
            "ExpressionAttributeValues": {":p": {"S": "p"}, ":one": {"N": "1"}},
            "Limit": 1
        }"#,
        )?;
        let first = call(&mut db, "Query", &query);
        assert_eq!(first["Count"], 0);
        assert_eq!(first["ScannedCount"], 1);
        assert_eq!(first["LastEvaluatedKey"]["sk"]["S"], "a");
        query["ExclusiveStartKey"] = first["LastEvaluatedKey"].clone();
        let second = call(&mut db, "Query", &query);
        assert_eq!(second["Items"][0]["sk"]["S"], "b");
        Ok(())
    }

    #[test]
    fn update_actions_and_key_ranges() -> TestResult {
        let mut db = Database::default();
        put(&mut db, "a", "1");
        put(&mut db, "b", "2");
        put(&mut db, "c", "3");
        let update: Value = serde_json::from_str(
            r#"{
            "TableName":"t", "Key":{"pk":{"S":"p"},"sk":{"S":"b"}},
            "UpdateExpression":"SET n = n + :one, data = :blob REMOVE missing ADD count :one",
            "ExpressionAttributeValues":{":one":{"N":"1"},":blob":{"B":"AAE="}},
            "ReturnValues":"ALL_NEW"
        }"#,
        )?;
        let updated = call(&mut db, "UpdateItem", &update);
        assert_eq!(updated["Attributes"]["n"]["N"], "3");
        assert_eq!(updated["Attributes"]["count"]["N"], "1");
        assert_eq!(updated["Attributes"]["data"]["B"], "AAE=");
        let query: Value = serde_json::from_str(
            r#"{
            "TableName":"t", "KeyConditionExpression":"pk = :p AND sk BETWEEN :a AND :c",
            "ExpressionAttributeValues":{":p":{"S":"p"},":a":{"S":"a"},":c":{"S":"c"}},
            "ScanIndexForward":false, "Limit":2
        }"#,
        )?;
        let result = call(&mut db, "Query", &query);
        assert_eq!(result["Items"][0]["sk"]["S"], "c");
        assert_eq!(result["Items"][1]["sk"]["S"], "b");
        assert_eq!(result["LastEvaluatedKey"]["sk"]["S"], "b");
        Ok(())
    }

    #[test]
    fn nested_handoff_condition_and_atomic_update() {
        let mut db = Database::default();
        put(&mut db, "leader", "1");
        let request = json!({
            "TableName": "t",
            "Key": {"pk": {"S": "p"}, "sk": {"S": "leader"}},
            "ConditionExpression": "(attribute_not_exists(priority) OR (n = :one AND n < :two)) AND NOT (n >= :two)",
            "UpdateExpression": "SET priority = :two REMOVE n ADD epoch :one",
            "ExpressionAttributeValues": {":one": {"N": "1"}, ":two": {"N": "2"}},
            "ReturnValues": "ALL_NEW"
        });
        let first = call(&mut db, UPDATE_ITEM, &request);
        assert_eq!(first["Attributes"]["priority"]["N"], "2");
        assert_eq!(first["Attributes"]["epoch"]["N"], "1");
        assert!(first["Attributes"].get("n").is_none());
        let (status, response) = db.handle(UPDATE_ITEM, &request);
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            response["__type"],
            format!("{ERROR_PREFIX}{CONDITION_ERROR}")
        );
        let current = call(
            &mut db,
            GET_ITEM,
            &json!({
                "TableName": "t",
                "Key": {"pk": {"S": "p"}, "sk": {"S": "leader"}}
            }),
        );
        assert_eq!(current["Item"]["epoch"]["N"], "1");
    }

    #[test]
    fn transaction_cancellation_rolls_back_all_writes() -> TestResult {
        let mut db = Database::default();
        put(&mut db, "a", "1");
        let request: Value = serde_json::from_str(
            r#"{"TransactItems": [
            {"Update": {
                "TableName": "t", "Key": {"pk": {"S": "p"}, "sk": {"S": "a"}},
                "UpdateExpression": "ADD n :one", "ExpressionAttributeValues": {":one": {"N": "1"}}
            }},
            {"ConditionCheck": {
                "TableName": "t", "Key": {"pk": {"S": "p"}, "sk": {"S": "missing"}},
                "ConditionExpression": "attribute_exists(pk)"
            }}
        ]}"#,
        )?;
        let (status, response) = db.handle("TransactWriteItems", &request);
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(response["CancellationReasons"][0]["Code"], "None");
        assert_eq!(
            response["CancellationReasons"][1]["Code"],
            "ConditionalCheckFailed"
        );
        let result = call(
            &mut db,
            "GetItem",
            &json!({
                "TableName": "t", "Key": {"pk": {"S": "p"}, "sk": {"S": "a"}}
            }),
        );
        assert_eq!(result["Item"]["n"]["N"], "1");
        Ok(())
    }

    #[tokio::test]
    async fn outage_switch_counts_and_recovers() -> TestResult {
        let fake = FakeDynamoDb::start().await?;
        fake.set_unavailable(true);
        let headers = HeaderMap::new();
        let (status, _) = serve(
            State(Arc::clone(&fake.state)),
            headers,
            axum::body::Bytes::new(),
        )
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        fake.set_unavailable(false);
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-target", "DynamoDB_20120810.GetItem".parse()?);
        let body = axum::body::Bytes::from_static(
            br#"{"TableName":"t","Key":{"pk":{"S":"p"},"sk":{"S":"a"}}}"#,
        );
        let (status, _) = serve(State(Arc::clone(&fake.state)), headers, body).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(fake.requests(), 2);
        Ok(())
    }
}
