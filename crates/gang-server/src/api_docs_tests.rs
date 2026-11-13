//! `docs/API.md`, the one reference of the HTTP API, checked against the wire types, the route
//! table, the error catalog, the stream decoder and the running API, so that no example, route or
//! error in the document can drift from the code.
//!
//! The document's grammar is the contract between it and these checks:
//!
//! - A Methods row is `` | `GET` | `/v1/principals` | `principals.read` | `` under the header
//!   `| Method | Path | Permission |`; the permission is a permission name or `open`.
//! - Every `json` fence is annotated `json request <Type>`, `json response <Type>` or
//!   `json error <REASON>`; an `sse` fence is a stream transcript of complete frames.
//! - The Errors table has the header `| reason | HTTP | status | metadata keys |`; the keys are
//!   backticked and comma separated, or `—` for none.

use std::collections::{BTreeMap, BTreeSet};

use gang_spi::ApiRequestBody;
use gang_state::sse;
use gang_testkit::Controller;
use gang_types::{AccessToken, Permission, PermissionSet};
use gang_wire::routes::{ROUTES, Route};
use gang_wire::{
    AuthenticateRequest, AuthenticateResponse, Certificate, CreateCertificateRequest,
    DeviceAuthorization, ErrorReason, ErrorResponse, Known, ListEventsResponse,
    ListPrincipalsResponse, ListRolesResponse, Principal, RedeemDeviceAuthorizationRequest,
    RevokePrincipalResponse, Server, Session, State,
};
use http::{Request, StatusCode};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::test_support::{
    api_request, mint_token, read, role_for, serve, tls_parts, tls_settings,
};
use crate::test_support_api::device_login_settings;

/// The document under test.
const DOCUMENT: &str = include_str!("../../../docs/API.md");

/// The name the document is reported under.
const FILE: &str = "docs/API.md";

/// The response type of the stream route, documented by an `sse` fence instead of a `json` one.
const STREAM_FRAME: &str = "Frame";

/// The identity provider every test server offers and the proof that signs in as the administrator.
const PROVIDER: &str = "example-idp";
const ADMIN_PROOF: &str = "admin-proof";

/// The methods tried on every documented path.
const METHODS: [&str; 5] = ["GET", "POST", "PUT", "PATCH", "DELETE"];

/// What an annotated fence holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind<'a> {
    /// `json request <Type>`: a request body.
    Request(&'a str),
    /// `json response <Type>`: a response body.
    Response(&'a str),
    /// `json error <REASON>`: an error body.
    Error(ErrorReason),
    /// `sse`: a stream transcript.
    Stream,
}

/// One fenced block of the document that the checks read.
#[derive(Debug)]
struct Block<'a> {
    /// The line of the opening fence.
    line: usize,
    kind: Kind<'a>,
    /// The lines between the fences, each ended by a newline.
    body: String,
}

/// One row of a table, split into trimmed cells.
#[derive(Debug)]
struct Row {
    /// The line of the row.
    line: usize,
    cells: Vec<String>,
}

/// A document split into the parts the checks read.
#[derive(Debug)]
struct Document<'a> {
    blocks: Vec<Block<'a>>,
    methods: Vec<Row>,
    errors: Vec<Row>,
}

impl<'a> Document<'a> {
    /// Splits `text` into fenced blocks and the rows of the Methods and Errors tables.
    ///
    /// Fails, naming the line, on a `json` fence without a valid annotation and on a fence that
    /// is never closed.
    fn parse(text: &'a str) -> Result<Self, String> {
        let mut blocks = Vec::new();
        let mut prose = Vec::new();
        let mut open: Option<(usize, Option<Kind<'a>>, String)> = None;
        for (index, text_line) in text.lines().enumerate() {
            let line = index + 1;
            match open.take() {
                Some((start, kind, body)) if text_line.trim_end() == "```" => {
                    if let Some(kind) = kind {
                        blocks.push(Block {
                            line: start,
                            kind,
                            body,
                        });
                    }
                }
                Some((start, kind, mut body)) => {
                    body.push_str(text_line);
                    body.push('\n');
                    open = Some((start, kind, body));
                }
                None => match text_line.strip_prefix("```") {
                    Some(info) => {
                        let kind =
                            kind_of(info).map_err(|reason| format!("{FILE}:{line}: {reason}"))?;
                        open = Some((line, kind, String::new()));
                    }
                    None => prose.push((line, text_line)),
                },
            }
        }
        if let Some((start, ..)) = open {
            return Err(format!("{FILE}:{start}: the fence is never closed"));
        }
        Ok(Self {
            blocks,
            methods: table(&prose, &["Method", "Path", "Permission"]),
            errors: table(&prose, &["reason", "HTTP", "status", "metadata keys"]),
        })
    }

    /// The document of this repository.
    fn load() -> Document<'static> {
        Document::parse(DOCUMENT).unwrap_or_else(|reason| panic!("{reason}"))
    }
}

/// What the info string of a fence announces: `None` for fences the checks do not read.
fn kind_of(info: &str) -> Result<Option<Kind<'_>>, String> {
    let mut words = info.split_whitespace();
    match words.next() {
        Some("json") => match (words.next(), words.next(), words.next()) {
            (Some("request"), Some(name), None) => Ok(Some(Kind::Request(name))),
            (Some("response"), Some(name), None) => Ok(Some(Kind::Response(name))),
            (Some("error"), Some(reason), None) => reason
                .parse::<ErrorReason>()
                .map(|reason| Some(Kind::Error(reason)))
                .map_err(|_| format!("`{reason}` is not an error reason")),
            _ => Err(
                "a json fence must be annotated `request <Type>`, `response <Type>` or \
                      `error <REASON>`"
                    .to_owned(),
            ),
        },
        Some("sse") if words.next().is_none() => Ok(Some(Kind::Stream)),
        _ => Ok(None),
    }
}

/// The cells of a table line, or `None` if `line` is not one.
fn cells(line: &str) -> Option<Vec<String>> {
    let inner = line.trim().strip_prefix('|')?.strip_suffix('|')?;
    Some(
        inner
            .split('|')
            .map(|cell| cell.trim().to_owned())
            .collect(),
    )
}

/// The rows of every table in `prose` whose header cells are `header`.
fn table(prose: &[(usize, &str)], header: &[&str]) -> Vec<Row> {
    let mut rows = Vec::new();
    for (position, (_, text_line)) in prose.iter().enumerate() {
        if cells(text_line).is_none_or(|found| found != header) {
            continue;
        }
        let body = prose
            .iter()
            .skip(position + 2)
            .map_while(|&(line, text_line)| cells(text_line).map(|cells| Row { line, cells }));
        rows.extend(body);
    }
    rows
}

/// The text of a cell without its code quotes.
fn unquote(cell: &str) -> &str {
    cell.trim_matches('`')
}

impl Block<'_> {
    /// The name of the wire type the block is registered under.
    fn type_name(&self) -> Option<&str> {
        match self.kind {
            Kind::Request(name) | Kind::Response(name) => Some(name),
            Kind::Error(_) => Some("ErrorResponse"),
            Kind::Stream => None,
        }
    }

    /// The JSON the block holds.
    fn json(&self) -> Result<Value, String> {
        serde_json::from_str(&self.body).map_err(|error| format!("is not JSON: {error}"))
    }

    /// `FILE:line` of the block.
    fn at(&self) -> String {
        format!("{FILE}:{}", self.line)
    }
}

/// Parses `value` as a `T` and writes it back out. A value holding an enum member or oneof member
/// this version does not know is refused: its text survives a round trip, so only the parsed
/// value shows it.
fn through<T>(value: &Value) -> Result<Value, String>
where
    T: Serialize + DeserializeOwned + Known,
{
    let typed: T = serde_json::from_value(value.clone())
        .map_err(|error| format!("does not parse as its type: {error}"))?;
    if !typed.is_known() {
        return Err("an enum value or member this version does not know".to_owned());
    }
    serde_json::to_value(&typed).map_err(|error| format!("does not serialize: {error}"))
}

/// The registry of wire types, by the names the route table and the annotations use: parses
/// `value` as the type called `name` and writes it back out.
fn round_trip(name: &str, value: &Value) -> Result<Value, String> {
    match name {
        "Server" => through::<Server<Value>>(value),
        "AuthenticateRequest" => through::<AuthenticateRequest>(value),
        "AuthenticateResponse" => through::<AuthenticateResponse>(value),
        "DeviceAuthorization" => through::<DeviceAuthorization>(value),
        "RedeemDeviceAuthorizationRequest" => through::<RedeemDeviceAuthorizationRequest>(value),
        "CreateCertificateRequest" => through::<CreateCertificateRequest>(value),
        "Certificate" => through::<Certificate>(value),
        "ListPrincipalsResponse" => through::<ListPrincipalsResponse>(value),
        "RevokePrincipalResponse" => through::<RevokePrincipalResponse>(value),
        "Principal" => through::<Principal>(value),
        "ListRolesResponse" => through::<ListRolesResponse>(value),
        "ListEventsResponse" => through::<ListEventsResponse>(value),
        "State" => through::<State>(value),
        "Session" => through::<Session>(value),
        "ErrorResponse" => through::<ErrorResponse>(value),
        other => Err(format!("type `{other}` is not registered")),
    }
}

/// Asserts that no check found a problem, listing every problem otherwise.
fn assert_clean(failures: &[String]) {
    assert!(failures.is_empty(), "\n{}\n", failures.join("\n"));
}

/// Every annotated example must parse into its wire type and write back out as itself.
fn check_round_trips(document: &Document<'_>) -> Vec<String> {
    let mut failures = Vec::new();
    for block in &document.blocks {
        let Some(name) = block.type_name() else {
            continue;
        };
        let outcome = block.json().and_then(|example| {
            let written = round_trip(name, &example)?;
            if written == example {
                Ok(())
            } else {
                Err(format!("does not round trip: it becomes {written}"))
            }
        });
        if let Err(reason) = outcome {
            failures.push(format!("{}: `{name}` example {reason}", block.at()));
        }
    }
    failures
}

/// The documented `(method, path, permission)` rows must be the route table, each route with its
/// examples.
fn check_routes(document: &Document<'_>) -> Vec<String> {
    let mut failures = Vec::new();
    let mut documented = BTreeSet::new();
    for row in &document.methods {
        let [method, path, permission] = row.cells.as_slice() else {
            failures.push(format!(
                "{FILE}:{}: a Methods row has method, path and permission cells",
                row.line
            ));
            continue;
        };
        let permission = Some(unquote(permission)).filter(|permission| *permission != "open");
        let key = (
            unquote(method).to_owned(),
            unquote(path).to_owned(),
            permission.map(str::to_owned),
        );
        if documented.contains(&key) {
            failures.push(format!(
                "{FILE}:{}: {} {} is documented twice",
                row.line, key.0, key.1
            ));
        }
        documented.insert(key);
    }
    let served: BTreeSet<_> = ROUTES
        .iter()
        .map(|route| {
            (
                route.method.as_str().to_owned(),
                route.template.to_owned(),
                route
                    .permission
                    .map(|permission| permission.as_str().to_owned()),
            )
        })
        .collect();
    for (method, path, permission) in documented.difference(&served) {
        failures.push(format!(
            "{FILE}: {method} {path} ({}) is documented but is not in the route table",
            permission.as_deref().unwrap_or("open")
        ));
    }
    for (method, path, permission) in served.difference(&documented) {
        failures.push(format!(
            "{FILE}: {method} {path} ({}) is in the route table but is not documented",
            permission.as_deref().unwrap_or("open")
        ));
    }
    for route in &ROUTES {
        failures.extend(missing_examples(document, route));
    }
    failures
}

/// The examples `route` lacks in `document`.
fn missing_examples(document: &Document<'_>, route: &Route) -> Vec<String> {
    let has = |wanted: Kind<'_>| document.blocks.iter().any(|block| block.kind == wanted);
    let name = format!("{} {}", route.method.as_str(), route.template);
    let mut missing = Vec::new();
    if let Some(request) = route.request
        && !has(Kind::Request(request))
    {
        missing.push(format!(
            "{FILE}: {name} has no `json request {request}` example"
        ));
    }
    let response_documented = if route.response == STREAM_FRAME {
        has(Kind::Stream)
    } else {
        has(Kind::Response(route.response))
    };
    if !response_documented {
        let fence = if route.response == STREAM_FRAME {
            "`sse` transcript".to_owned()
        } else {
            format!("`json response {}` example", route.response)
        };
        missing.push(format!("{FILE}: {name} has no {fence}"));
    }
    missing
}

/// One parsed row of the Errors table.
#[derive(Debug, PartialEq, Eq)]
struct ErrorRow {
    http: u16,
    status: String,
    keys: Vec<String>,
}

/// The Errors table must be the catalog, and every error example must be the catalogued error.
fn check_errors(document: &Document<'_>) -> Vec<String> {
    let mut failures = Vec::new();
    let mut documented = BTreeMap::new();
    for row in &document.errors {
        let [reason, http, status, keys] = row.cells.as_slice() else {
            failures.push(format!(
                "{FILE}:{}: an Errors row has reason, HTTP, status and metadata keys cells",
                row.line
            ));
            continue;
        };
        let Ok(http) = http.parse() else {
            failures.push(format!(
                "{FILE}:{}: `{http}` is not an HTTP status",
                row.line
            ));
            continue;
        };
        let keys = unquote_all(keys);
        let entry = ErrorRow {
            http,
            status: unquote(status).to_owned(),
            keys,
        };
        if documented
            .insert(unquote(reason).to_owned(), (row.line, entry))
            .is_some()
        {
            failures.push(format!(
                "{FILE}:{}: `{reason}` is documented twice",
                row.line
            ));
        }
    }
    for reason in ErrorReason::ALL {
        let expected = ErrorRow {
            http: reason.http(),
            status: reason.status().as_str().to_owned(),
            keys: reason
                .metadata_keys()
                .iter()
                .map(|key| (*key).to_owned())
                .collect(),
        };
        match documented.remove(reason.as_str()) {
            None => failures.push(format!("{FILE}: the Errors table lacks `{reason}`")),
            Some((_, found)) if found == expected => {}
            Some((line, found)) => failures.push(format!(
                "{FILE}:{line}: `{reason}` is documented as {found:?} but the catalog says \
                 {expected:?}"
            )),
        }
    }
    for (name, (line, _)) in documented {
        failures.push(format!(
            "{FILE}:{line}: `{name}` is not in the error catalog"
        ));
    }
    for block in &document.blocks {
        if let Kind::Error(reason) = block.kind {
            failures.extend(check_error_example(block, reason));
        }
    }
    failures
}

/// The backticked, comma-separated keys of a metadata keys cell; `—` and an empty cell hold none.
fn unquote_all(cell: &str) -> Vec<String> {
    cell.split(',')
        .map(|key| unquote(key.trim()))
        .filter(|key| !key.is_empty() && *key != "—")
        .map(str::to_owned)
        .collect()
}

/// The problems of an `error` example that is not the error `reason` is answered with.
fn check_error_example(block: &Block<'_>, reason: ErrorReason) -> Vec<String> {
    let parsed = block.json().and_then(|example| {
        serde_json::from_value::<ErrorResponse>(example).map_err(|error| error.to_string())
    });
    let response = match parsed {
        Ok(response) => response,
        Err(error) => {
            let at = block.at();
            return vec![format!(
                "{at}: `{reason}` example is not an error body: {error}"
            )];
        }
    };
    let status = response.error;
    let mut failures = Vec::new();
    let infos = status
        .details
        .iter()
        .filter(|detail| matches!(detail, gang_wire::Detail::ErrorInfo(_)))
        .count();
    if infos != 1 {
        failures.push(format!(
            "{}: the example carries {infos} ErrorInfo details, not 1",
            block.at()
        ));
    }
    if status.reason() != Some(reason) {
        failures.push(format!(
            "{}: the example's reason is not `{reason}`",
            block.at()
        ));
    }
    if status.code != reason.http() {
        failures.push(format!(
            "{}: the example's code is not {}",
            block.at(),
            reason.http()
        ));
    }
    if status.status.as_str() != reason.status().as_str() {
        failures.push(format!(
            "{}: the example's status is not `{}`",
            block.at(),
            reason.status().as_str()
        ));
    }
    let keys: Vec<&str> = status
        .error_info()
        .map(|info| info.metadata.keys().map(String::as_str).collect())
        .unwrap_or_default();
    let mut expected = reason.metadata_keys().to_vec();
    expected.sort_unstable();
    if keys != expected {
        failures.push(format!(
            "{}: the example's metadata keys are {keys:?}, not {expected:?}",
            block.at()
        ));
    }
    let permission = status
        .error_info()
        .and_then(|info| info.metadata.get("permission"));
    let expected = match permission {
        Some(permission) if reason == ErrorReason::MissingPermission => {
            format!("{} {permission}", reason.message())
        }
        _ => reason.message().to_owned(),
    };
    if status.message != expected {
        failures.push(format!(
            "{}: the example's message is {:?}, not {expected:?}",
            block.at(),
            status.message
        ));
    }
    failures
}

/// The text a stream transcript stands for: complete frames, each ended by a blank line.
fn transcript(body: &str) -> String {
    format!("{}\n\n", body.trim_end_matches('\n'))
}

/// Every transcript must decode into frames and encode back to the same bytes.
fn check_streams(document: &Document<'_>) -> Vec<String> {
    let mut failures = Vec::new();
    for block in document
        .blocks
        .iter()
        .filter(|block| block.kind == Kind::Stream)
    {
        let text = transcript(&block.body);
        let frames = match sse::Decoder::new().push(text.as_bytes()) {
            Ok(frames) if frames.is_empty() => {
                failures.push(format!("{}: the transcript holds no frames", block.at()));
                continue;
            }
            Ok(frames) => frames,
            Err(error) => {
                failures.push(format!(
                    "{}: the transcript does not decode: {error}",
                    block.at()
                ));
                continue;
            }
        };
        let mut encoded = Vec::new();
        for frame in &frames {
            match sse::encode(frame) {
                Ok(bytes) => encoded.extend_from_slice(&bytes),
                Err(error) => {
                    failures.push(format!("{}: a frame does not encode: {error}", block.at()));
                }
            }
        }
        if encoded != text.as_bytes() {
            failures.push(format!(
                "{}: the transcript is not what the server writes; it writes\n{}",
                block.at(),
                String::from_utf8_lossy(&encoded)
            ));
        }
    }
    failures
}

/// Whether `key` is `lowerCamelCase`.
fn is_lower_camel(key: &str) -> bool {
    key.starts_with(|first: char| first.is_ascii_lowercase())
        && key
            .chars()
            .all(|character| character.is_ascii_alphanumeric())
}

/// Collects the paths of the keys under `value` that are not `lowerCamelCase`. The plugin-owned
/// `token.login` values and the `properties` maps hold data keys and are not read.
fn misnamed_keys(value: &Value, path: &mut Vec<String>, found: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if key != "@type" && !is_lower_camel(key) {
                    found.push(format!("{}.{key}", path.join(".")));
                }
                let is_data = key == "properties"
                    || (key == "login" && path.last().is_some_and(|parent| parent == "token"));
                if !is_data {
                    path.push(key.clone());
                    misnamed_keys(child, path, found);
                    path.pop();
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                misnamed_keys(item, path, found);
            }
        }
        _ => {}
    }
}

/// Every key of every example, including the payloads of stream frames, is `lowerCamelCase`.
fn check_keys(document: &Document<'_>) -> Vec<String> {
    let mut failures = Vec::new();
    for block in &document.blocks {
        let examples = match block.kind {
            Kind::Stream => block
                .body
                .lines()
                .filter_map(|line| line.strip_prefix("data: "))
                .map(|data| serde_json::from_str(data).map_err(|error| error.to_string()))
                .collect(),
            _ => vec![block.json()],
        };
        for example in examples {
            match example {
                Ok(example) => {
                    let mut found = Vec::new();
                    misnamed_keys(&example, &mut Vec::new(), &mut found);
                    failures.extend(
                        found.into_iter().map(|key| {
                            format!("{}: key `{key}` is not lowerCamelCase", block.at())
                        }),
                    );
                }
                Err(error) => failures.push(format!("{}: is not JSON: {error}", block.at())),
            }
        }
    }
    failures
}

#[test]
fn examples_round_trip_through_the_wire_types() {
    assert_clean(&check_round_trips(&Document::load()));
}

#[test]
fn documented_routes_are_the_route_table() {
    assert_clean(&check_routes(&Document::load()));
}

#[test]
fn errors_are_the_catalog() {
    assert_clean(&check_errors(&Document::load()));
}

#[test]
fn stream_transcripts_are_what_the_server_writes() {
    assert_clean(&check_streams(&Document::load()));
}

#[test]
fn example_keys_are_lower_camel_case() {
    assert_clean(&check_keys(&Document::load()));
}

#[test]
fn a_json_fence_without_an_annotation_is_rejected() {
    let error = Document::parse("text\n```json\n{}\n```\n").expect_err("the fence is unannotated");
    assert!(error.starts_with("docs/API.md:2:"), "{error}");
}

#[test]
fn an_unregistered_type_fails_the_round_trip() {
    let document = Document::parse("```json response Unknown\n{}\n```\n").expect("document parses");
    let failures = check_round_trips(&document);
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert!(failures[0].contains("docs/API.md:1") && failures[0].contains("not registered"));
}

#[test]
fn a_request_with_an_unknown_field_fails_the_round_trip() {
    let document = Document::parse(
        "```json request AuthenticateRequest\n\
         {\"provider\":\"example-idp\",\"proof\":\"p\",\"extra\":1}\n```\n",
    )
    .expect("document parses");
    assert_eq!(check_round_trips(&document).len(), 1);
}

#[test]
fn keys_outside_lower_camel_case_are_found_except_in_data_maps() {
    let value = json!({
        "createTime": 1,
        "last_seen": 2,
        "properties": { "Some-Key": "x" },
        "authentication": { "token": { "login": { "example-idp": { "client_id": "x" } } } },
    });
    let mut found = Vec::new();
    misnamed_keys(&value, &mut Vec::new(), &mut found);
    assert_eq!(found, [".last_seen"]);
}

/// The principal that holds every permission except `missing`, and the proof that signs it in.
fn lacking(missing: Permission) -> (String, String) {
    let name = format!("lacking-{}", missing.as_str().replace('.', "-"));
    let proof = format!("{name}-proof");
    (name, proof)
}

/// What the server answered: its status, and its error body when the status is not a success.
struct Answer {
    status: StatusCode,
    error: Option<ErrorResponse>,
}

impl Answer {
    /// Whether the answer is `status` with `reason`.
    fn is(&self, status: StatusCode, reason: ErrorReason) -> bool {
        self.status == status
            && self.error.as_ref().and_then(|body| body.error.reason()) == Some(reason)
    }

    /// Whether the answer is the `404 ROUTE_NOT_FOUND` of an unknown route.
    fn is_route_not_found(&self) -> bool {
        self.is(StatusCode::NOT_FOUND, ErrorReason::RouteNotFound)
    }

    /// Whether the answer is the `403 MISSING_PERMISSION` that names `permission`.
    fn is_missing(&self, permission: Permission) -> bool {
        self.is(StatusCode::FORBIDDEN, ErrorReason::MissingPermission)
            && self
                .error
                .as_ref()
                .and_then(|body| body.error.error_info())
                .and_then(|info| info.metadata.get("permission"))
                .is_some_and(|named| named == permission.as_str())
    }
}

/// Sends `request` through the public gate and the API. The body of a success is not read, since
/// the stream never ends.
async fn answer(controller: &Controller, request: Request<ApiRequestBody>) -> Answer {
    let response = controller.call_api(request).await;
    let status = response.status();
    if status.is_success() {
        return Answer {
            status,
            error: None,
        };
    }
    let bytes = read(response).await.body;
    let error = serde_json::from_slice(&bytes).expect("a failure is an error body");
    Answer {
        status,
        error: Some(error),
    }
}

/// A request for `route` as the bearer of `token`, with an empty object for a body when the route
/// takes one, and the `method` it is sent with.
fn request_for(method: &str, route: &Route, token: &AccessToken) -> Request<ApiRequestBody> {
    let body = route.request.map(|_| json!({}));
    let path = match route.id {
        gang_wire::routes::RouteId::RevokeCertificate => gang_wire::routes::REVOKE_CERTIFICATE
            .path(&"aa".parse().expect("fixture serial is valid")),
        gang_wire::routes::RouteId::RevokePrincipal => gang_wire::routes::REVOKE_PRINCIPAL
            .path(&"test:principal".parse().expect("fixture identity is valid")),
        gang_wire::routes::RouteId::RestorePrincipal => gang_wire::routes::RESTORE_PRINCIPAL
            .path(&"test:principal".parse().expect("fixture identity is valid")),
        _ => route.template.to_owned(),
    };
    api_request(method, &path, Some(token), body)
}

/// The permissions some route requires, each once.
fn required_permissions() -> Vec<Permission> {
    let mut required = Vec::new();
    for permission in ROUTES.iter().filter_map(|route| route.permission) {
        if !required.contains(&permission) {
            required.push(permission);
        }
    }
    required
}

/// The problems of the server's answers about `route`.
async fn check_route(controller: &Controller, admin: &AccessToken, route: &Route) -> Vec<String> {
    let name = format!("{} {}", route.method.as_str(), route.template);
    let mut failures = Vec::new();
    let served = answer(controller, request_for(route.method.as_str(), route, admin)).await;
    if served.is_route_not_found() {
        failures.push(format!(
            "{name} is answered 404 ROUTE_NOT_FOUND to the administrator"
        ));
    }
    for method in METHODS {
        let documented = ROUTES
            .iter()
            .any(|known| known.template == route.template && known.method.as_str() == method);
        if documented {
            continue;
        }
        let wrong = answer(controller, request_for(method, route, admin)).await;
        if !wrong.is_route_not_found() {
            failures.push(format!(
                "{method} {} is not answered 404 ROUTE_NOT_FOUND",
                route.template
            ));
        }
    }
    if let Some(required) = route.permission {
        let (_, proof) = lacking(required);
        let token = mint_token(controller, PROVIDER, &proof).await;
        let denied = answer(
            controller,
            request_for(route.method.as_str(), route, &token),
        )
        .await;
        if !denied.is_missing(required) {
            failures.push(format!(
                "{name} is not answered 403 MISSING_PERMISSION naming `{required}` to a caller \
                 that lacks only it"
            ));
        }
    }
    failures
}

#[tokio::test]
async fn the_server_answers_the_documented_routes_and_only_those() {
    let (parts, _dials, handles) = tls_parts();
    let mut settings = tls_settings();
    settings.login = Some(device_login_settings());
    for missing in required_permissions() {
        let (name, proof) = lacking(missing);
        let permissions = Permission::ALL
            .into_iter()
            .filter(|permission| *permission != missing)
            .collect::<PermissionSet>();
        settings.roles.push(role_for(&name, permissions, &name));
        handles.verifier.accept(&proof, &name);
    }
    serve(
        parts,
        settings,
        handles.scheduler,
        |controller| async move {
            let admin = mint_token(&controller, PROVIDER, ADMIN_PROOF).await;
            let mut failures = Vec::new();
            for route in &ROUTES {
                failures.extend(check_route(&controller, &admin, route).await);
            }
            assert_clean(&failures);
        },
    )
    .await;
}
