//! The Gang API (`/v1/**`): serves the server and session resources, issues credentials through
//! the client and worker authentication plugins, revokes and restores principals and
//! certificates, lists principals, roles and scaling events, and serves the dashboard state with
//! its live stream. Each request arrives after its listener's gate, which records the caller;
//! this module decides from the route table, the mounted credentials and that caller alone
//! whether to serve a request, and renders every answer as JSON.

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use gang_clock::Clock;
use gang_spi::{
    ApiRequestBody, ApiResponseBody, ApiService, Authenticated, AuthenticationPort,
    CertificateIssue, CredentialLifecyclePort, ScalingEventSeq, StateRepositoryPort, TokenIssue,
    full_body,
};
use gang_types::{Identity, NonZeroDuration, ProviderName};
use gang_wire::name::{self, SESSION};
use gang_wire::page::{PageToken, page_size};
use gang_wire::routes::{self, Method as RouteMethod, RouteId};
use gang_wire::{
    AuthenticateRequest, AuthenticateResponse, Certificate, CreateCertificateRequest, ErrorReason,
    ErrorResponse, INTERNAL_ERROR_BODY, ListEventsResponse, ListPrincipalsResponse,
    ListRolesResponse, Principal, RevokePrincipalResponse, Role, Session, SessionLogin, Status,
};
use http::header::{CONTENT_TYPE, HeaderValue, RETRY_AFTER, WWW_AUTHENTICATE};
use http::{Method, Request, Response, StatusCode};
use http_body_util::BodyExt;
use serde::{
    Deserialize, Serialize,
    de::{DeserializeOwned, IgnoredAny},
};
use serde_json::Value;

use crate::api_error::{
    PortResultExt, gate_status, malformed_request, missing_permission, route_not_found,
};
use crate::authorizer::Authorizer;
use crate::client_address::ClientAddress;
use crate::core::Core;
use crate::deployment::Stack;
use crate::events::wire_event;
use crate::gate::{Caller, is_open_path};
use crate::hub::StateHub;
use crate::query::query_param;
use crate::rate_limit::RateLimiter;
use crate::resources::{wire_certificate, wire_principal};
use crate::revocation::Revocations;
use crate::settings::RequestLimits;
use crate::stream::{StreamParams, open_stream};

/// The only `filter` of a principal listing: the principals that are revoked.
const REVOKED_FILTER: &str = "revokeTime:*";

/// The dashboard the API serves: the hub that holds the latest state and fans it out to viewers,
/// and the interval between the heartbeats that keep a quiet stream alive.
pub(crate) struct ApiDashboard {
    /// The hub that holds the latest state and publishes each change to viewers.
    pub(crate) hub: StateHub,
    /// The interval between heartbeats while the state is quiet.
    pub(crate) heartbeat: NonZeroDuration,
}

/// The collaborators an [`Api`] serves requests with.
pub(crate) struct ApiParts<K: Stack> {
    /// The host, the plugins and the cookies the API serves with.
    pub(crate) core: Arc<Core<K>>,
    /// The revocation service the routes that change principals and certificates go through.
    pub(crate) revocations: Revocations<K>,
    /// The server resource served at `GET /v1/server`.
    pub(crate) info: gang_wire::Server<Value>,
    /// The authorizer that decides which permissions each caller holds.
    pub(crate) authorizer: Authorizer,
    /// The size and time limits every request is held to.
    pub(crate) limits: RequestLimits,
    /// The shared limiter for mounted open routes.
    pub(crate) limiter: Arc<RateLimiter<Arc<K::Clock>>>,
    /// The dashboard served at `GET /v1/state` and `GET /v1/state:watch`; `None` when the server
    /// does not mount it.
    pub(crate) dashboard: Option<ApiDashboard>,
    /// The identity provider the browser signs in with, which `GET /v1/session` reports; `None`
    /// when the browser sign-in is not mounted.
    pub(crate) login_provider: Option<ProviderName>,
}

/// The Gang API. Cloning it shares the same state.
pub(crate) struct Api<K: Stack>(Arc<ApiParts<K>>);

impl<K: Stack> Clone for Api<K> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<K: Stack> Api<K> {
    /// Builds the API from its collaborators.
    #[must_use]
    pub(crate) fn new(parts: ApiParts<K>) -> Self {
        Self(Arc::new(parts))
    }

    /// Decides what to do with a request, from the caller, the route table and the mounted
    /// credentials: the operation to serve with the still-encoded id the path names, or the
    /// refusal to answer with. The caller is weighed before anything about the request is
    /// revealed, so a refusal recorded by the gate comes first, an unknown route is `401` to an
    /// anonymous caller, and the permission is checked before any resource is looked up.
    fn decide<'a>(
        &'a self,
        method: &Method,
        path: &'a str,
        caller: &'a Caller,
        client: Option<IpAddr>,
    ) -> Result<Operation<'a>, Status> {
        if let Caller::Refused(refusal) = caller {
            return Err(gate_status(refusal));
        }
        // The API serves only its open paths; anything else names no route of the API.
        if !is_open_path(path) {
            return Err(route_not_found(method.as_str(), path));
        }
        let authenticated = caller.as_authenticated();
        let found = method
            .as_str()
            .parse::<RouteMethod>()
            .ok()
            .and_then(|method| routes::lookup(method, path));
        let Some((route, raw)) = found else {
            return Err(if authenticated.is_some() {
                route_not_found(method.as_str(), path)
            } else {
                ErrorReason::CredentialsMissing.into()
            });
        };
        if route.permission.is_some() && authenticated.is_none() {
            return Err(ErrorReason::CredentialsMissing.into());
        }
        let Some(operation) = self.mount(route.id, raw, authenticated) else {
            return Err(route_not_found(method.as_str(), path));
        };
        if route.rate_limited {
            self.0
                .limiter
                .check(client)
                .map_err(crate::api_error::rate_limited)?;
        }
        if let Some(permission) = route.permission {
            let granted = authenticated.is_some_and(|authenticated| {
                self.0
                    .authorizer
                    .allows(&authenticated.identity, permission)
            });
            if !granted {
                return Err(missing_permission(permission));
            }
        }
        Ok(operation)
    }

    /// The operation that serves route `id` when the credential it needs is mounted, `None`
    /// otherwise. A route with a permission is only mounted for an authenticated caller.
    fn mount<'a>(
        &'a self,
        id: RouteId,
        raw: Option<&'a str>,
        authenticated: Option<&'a Authenticated>,
    ) -> Option<Operation<'a>> {
        let shared = &*self.0;
        Some(match id {
            RouteId::GetServer => Operation::GetServer,
            RouteId::AuthenticateSession => {
                if !shared.core.clients.capabilities().tokens {
                    return None;
                }
                Operation::Authenticate
            }
            RouteId::CreateCertificate => {
                if !shared.core.workers.capabilities().certificates {
                    return None;
                }
                Operation::CreateCertificate
            }
            RouteId::RevokeCertificate => Operation::RevokeCertificate(raw?),
            RouteId::ListPrincipals => Operation::ListPrincipals,
            RouteId::RevokePrincipal => Operation::RevokePrincipal(raw?),
            RouteId::RestorePrincipal => Operation::RestorePrincipal(raw?),
            RouteId::ListRoles => Operation::ListRoles,
            RouteId::ListEvents => Operation::ListEvents,
            RouteId::GetState => Operation::GetState(shared.dashboard.as_ref()?),
            RouteId::WatchState => Operation::WatchState {
                dashboard: shared.dashboard.as_ref()?,
                caller: authenticated?,
            },
            RouteId::GetSession => Operation::GetSession,
        })
    }

    async fn serve(
        &self,
        operation: Operation<'_>,
        query: Option<&str>,
        body: ApiRequestBody,
        caller: &Caller,
    ) -> Response<ApiResponseBody> {
        match operation {
            Operation::GetServer => json(StatusCode::OK, &self.0.info),
            Operation::Authenticate => outcome(self.authenticate(body).await),
            Operation::CreateCertificate => outcome(self.create_certificate(body).await),
            Operation::RevokeCertificate(raw) => outcome(self.revoke_certificate(raw, body).await),
            Operation::ListPrincipals => outcome(self.list_principals(query).await),
            Operation::RevokePrincipal(raw) => outcome(self.revoke_principal(raw, body).await),
            Operation::RestorePrincipal(raw) => outcome(self.restore_principal(raw, body).await),
            Operation::ListRoles => outcome(self.list_roles(query)),
            Operation::ListEvents => outcome(self.list_events(query).await),
            Operation::GetState(dashboard) => state(dashboard),
            Operation::WatchState { dashboard, caller } => self.watch_state(dashboard, caller),
            Operation::GetSession => json(StatusCode::OK, &self.session(caller)),
        }
    }

    async fn authenticate(&self, body: ApiRequestBody) -> Result<AuthenticateResponse, Status> {
        let request: AuthenticateRequest = self.read_json(body).await?;
        let core = &self.0.core;
        let issued = core
            .clients
            .issue_token(
                &core.host,
                &TokenIssue {
                    provider: &request.provider,
                    proof: &request.proof,
                },
            )
            .await
            .or_status()?;
        Ok(AuthenticateResponse {
            token: issued.token,
            lifetime: issued.expires_in,
        })
    }

    async fn create_certificate(&self, body: ApiRequestBody) -> Result<Certificate, Status> {
        let request: CreateCertificateRequest = self.read_json(body).await?;
        let core = &self.0.core;
        let issued = core
            .workers
            .issue_certificate(
                &core.host,
                &CertificateIssue {
                    provider: &request.provider,
                    proof: &request.proof,
                    csr: &request.pem_csr,
                },
            )
            .await
            .or_status()?;
        Ok(wire_certificate(issued.record, Some(issued.certificate)))
    }

    async fn revoke_certificate(
        &self,
        raw: &str,
        body: ApiRequestBody,
    ) -> Result<Certificate, Status> {
        let serial = id_of::<name::Certificates>(raw, "certificate")?;
        self.read_empty(body).await?;
        self.0
            .revocations
            .revoke_certificate(&serial)
            .await
            .or_status()
    }

    async fn revoke_principal(
        &self,
        raw: &str,
        body: ApiRequestBody,
    ) -> Result<RevokePrincipalResponse, Status> {
        let identity = id_of::<name::Principals>(raw, "principal")?;
        self.read_empty(body).await?;
        self.0
            .revocations
            .revoke_principal(&identity)
            .await
            .or_status()
    }

    async fn restore_principal(
        &self,
        raw: &str,
        body: ApiRequestBody,
    ) -> Result<Principal, Status> {
        let identity = id_of::<name::Principals>(raw, "principal")?;
        self.read_empty(body).await?;
        self.0
            .revocations
            .restore_principal(&identity)
            .await
            .or_status()
    }

    /// Answers the page of principals that follows the request's page token, from the
    /// repository, in identity order.
    async fn list_principals(&self, query: Option<&str>) -> Result<ListPrincipalsResponse, Status> {
        let query = ListQuery::parse(query)?;
        let (filter, only_revoked) = match query.filter.as_deref() {
            None => ("", false),
            Some(REVOKED_FILTER) => (REVOKED_FILTER, true),
            Some(_) => {
                return Err(malformed_request([(
                    "filter",
                    "only `revokeTime:*` is supported",
                )]));
            }
        };
        let paging = query.paging(filter)?;
        let after = paging.cursor_as::<Identity>()?;
        let mut records = Vec::new();
        let more = self
            .0
            .core
            .host
            .repository
            .principals(after.as_ref(), paging.limit, only_revoked, &mut records)
            .await
            .or_status()?;
        let next_page_token = PageToken::after(
            filter,
            more,
            records
                .last()
                .map(|last| last.identity.to_string())
                .as_deref(),
        );
        Ok(ListPrincipalsResponse {
            principals: records.into_iter().map(wire_principal).collect(),
            next_page_token,
        })
    }

    /// Answers the page of roles that follows the request's page token, in name order.
    fn list_roles(&self, query: Option<&str>) -> Result<ListRolesResponse, Status> {
        let paging = ListQuery::parse(query)?.paging("")?;
        let (page, more) = role_page(self.0.authorizer.roles(), &paging)?;
        let next_page_token = PageToken::after("", more, page.last().map(role_id));
        Ok(ListRolesResponse {
            roles: page.to_vec(),
            next_page_token,
        })
    }

    /// Answers the page of scaling events that precedes the request's page token, newest first.
    async fn list_events(&self, query: Option<&str>) -> Result<ListEventsResponse, Status> {
        let paging = ListQuery::parse(query)?.paging("")?;
        let before = paging.cursor_as::<u64>()?.map(ScalingEventSeq::new);
        let mut recorded = Vec::new();
        let more = self
            .0
            .core
            .host
            .repository
            .scaling_events(
                ScalingEventSeq::default(),
                before,
                paging.limit,
                &mut recorded,
            )
            .await
            .or_status()?;
        // The repository answers oldest first; the oldest event of the page is where the next
        // page resumes.
        let next_page_token = PageToken::after(
            "",
            more,
            recorded
                .first()
                .map(|oldest| oldest.seq.get().to_string())
                .as_deref(),
        );
        recorded.reverse();
        Ok(ListEventsResponse {
            events: recorded.into_iter().map(wire_event).collect(),
            next_page_token,
        })
    }

    /// The caller's session. An anonymous caller has a session too: one without a principal.
    fn session(&self, caller: &Caller) -> Session {
        let authenticated = caller.as_authenticated();
        Session {
            name: SESSION.to_owned(),
            principal: authenticated
                .map(|authenticated| name::of::<name::Principals>(&authenticated.identity)),
            permissions: authenticated
                .map(|authenticated| {
                    self.0
                        .authorizer
                        .permissions(&authenticated.identity)
                        .iter()
                        .map(gang_wire::Permission::from)
                        .collect()
                })
                .unwrap_or_default(),
            expire_time: authenticated.and_then(|authenticated| authenticated.valid_until),
            login: self
                .0
                .login_provider
                .clone()
                .map(|provider| SessionLogin { provider }),
        }
    }

    /// Opens the live state stream for the caller; the response is the stream itself.
    fn watch_state(
        &self,
        dashboard: &ApiDashboard,
        caller: &Authenticated,
    ) -> Response<ApiResponseBody> {
        open_stream(StreamParams {
            hub: dashboard.hub.clone(),
            clock: self.0.core.host.clock.clone(),
            heartbeat: dashboard.heartbeat,
            identity: Arc::clone(&caller.identity),
            valid_until: caller.valid_until,
            revoked_principals: self.0.core.host.revoked_principals.clone(),
        })
    }

    /// Reads the request body, which may not exceed the size limit.
    async fn read_bytes(&self, mut body: ApiRequestBody) -> Result<Bytes, Status> {
        let limit = self.0.limits.max_request_size;
        let mut length = 0;
        let mut first: Option<Bytes> = None;
        let mut combined: Option<BytesMut> = None;
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|_| malformed_body())?;
            if let Ok(chunk) = frame.into_data() {
                if chunk.len() > limit.saturating_sub(length) {
                    return Err(ErrorReason::RequestTooLarge.into());
                }
                length += chunk.len();
                if let Some(buffer) = &mut combined {
                    buffer.extend_from_slice(&chunk);
                } else if let Some(previous) = first.take() {
                    let mut buffer = BytesMut::with_capacity(length);
                    buffer.extend_from_slice(&previous);
                    buffer.extend_from_slice(&chunk);
                    combined = Some(buffer);
                } else {
                    first = Some(chunk);
                }
            }
        }
        Ok(match combined {
            Some(buffer) => buffer.freeze(),
            None => first.unwrap_or_default(),
        })
    }

    /// Reads the request body as the JSON request `T`, which rejects unknown fields.
    async fn read_json<T: DeserializeOwned>(&self, body: ApiRequestBody) -> Result<T, Status> {
        let bytes = self.read_bytes(body).await?;
        serde_json::from_slice(&bytes).map_err(|_| {
            let fields = struct_fields::<T>();
            unknown_fields(&bytes, |field| !fields.contains(&field))
                .ok()
                .flatten()
                .unwrap_or_else(malformed_body)
        })
    }

    /// Reads the body of a request that takes no fields: absent or `{}`.
    async fn read_empty(&self, body: ApiRequestBody) -> Result<(), Status> {
        let bytes = self.read_bytes(body).await?;
        if bytes.iter().all(u8::is_ascii_whitespace) {
            return Ok(());
        }
        match unknown_fields(&bytes, |_| true)? {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl<K: Stack> ApiService for Api<K> {
    fn call(
        &self,
        request: Request<ApiRequestBody>,
    ) -> impl Future<Output = Response<ApiResponseBody>> + Send {
        let this = self.clone();
        async move {
            let (parts, body) = request.into_parts();
            let caller = Caller::of(&parts.extensions);
            let path = parts.uri.path();
            let client = parts
                .extensions
                .get::<ClientAddress>()
                .map(|address| address.0);
            let decision = this.decide(&parts.method, path, caller, client);
            tracing::debug!(
                authenticated = caller.as_authenticated().is_some(),
                method = %parts.method,
                path,
                refusal = ?decision.as_ref().err().and_then(Status::reason),
                "api request"
            );
            let operation = match decision {
                Ok(served) => served,
                Err(status) => return failure(status),
            };
            let answer = this.serve(operation, parts.uri.query(), body, caller);
            let deadline = this
                .0
                .core
                .host
                .clock
                .sleep(this.0.limits.request_timeout.get());
            tokio::select! {
                biased;
                response = answer => response,
                () = deadline => failure(ErrorReason::RequestTimedOut.into()),
            }
        }
    }
}

/// What a decided request asks the API to do, with what the decision proved: one variant per
/// route of the table. The variants that need the dashboard or the caller carry them, so serving
/// them needs no further lookup.
enum Operation<'a> {
    /// `GET /v1/server`: the server resource.
    GetServer,
    /// `POST /v1/session:authenticate`: exchanges a proof for a bearer credential.
    Authenticate,
    /// `POST /v1/certificates`: issues a worker certificate.
    CreateCertificate,
    /// `POST /v1/certificates/{certificate}:revoke`: revokes one certificate.
    RevokeCertificate(&'a str),
    /// `GET /v1/principals`: one page of principals.
    ListPrincipals,
    /// `POST /v1/principals/{principal}:revoke`: revokes a principal and its certificates.
    RevokePrincipal(&'a str),
    /// `POST /v1/principals/{principal}:restore`: restores a revoked principal.
    RestorePrincipal(&'a str),
    /// `GET /v1/roles`: one page of roles.
    ListRoles,
    /// `GET /v1/events`: one page of scaling events.
    ListEvents,
    /// `GET /v1/state`: the latest state, from the mounted dashboard.
    GetState(&'a ApiDashboard),
    /// `GET /v1/state:watch`: the live state stream, for an authenticated caller.
    WatchState {
        /// The mounted dashboard the stream reads from.
        dashboard: &'a ApiDashboard,
        /// The authenticated caller the stream is opened for.
        caller: &'a Authenticated,
    },
    /// `GET /v1/session`: the caller's session.
    GetSession,
}

/// The parameters every list request takes, as the caller sent them. An empty value is an unset
/// one.
#[derive(Debug, Default)]
struct ListQuery {
    page_size: Option<i64>,
    page_token: Option<PageToken>,
    filter: Option<String>,
}

impl ListQuery {
    /// Reads `pageSize`, `pageToken` and `filter` from the query string. Values decode as
    /// `application/x-www-form-urlencoded`: `%XX` escapes decode, `+` is a space, and bytes that
    /// are not UTF-8 are replaced. The first pair of each key wins, even when it is empty, and
    /// other keys are ignored.
    fn parse(query: Option<&str>) -> Result<Self, Status> {
        let page_size = query_param(query, "pageSize");
        let page_token = query_param(query, "pageToken");
        let filter = query_param(query, "filter");
        let page_size = page_size
            .filter(|value| !value.is_empty())
            .map(|value| {
                page_size_of(&value)
                    .ok_or_else(|| malformed_request([("pageSize", "must be an integer")]))
            })
            .transpose()?;
        Ok(Self {
            page_size,
            page_token: page_token
                .filter(|value| !value.is_empty())
                .map(|value| PageToken::from(value.into_owned())),
            filter: filter
                .filter(|value| !value.is_empty())
                .map(std::borrow::Cow::into_owned),
        })
    }

    /// The page the request asks for, for a list whose requests carry `filter` (the empty text
    /// if the list has no filter): its size and the cursor its page token resumes after.
    fn paging(&self, filter: &str) -> Result<Paging, Status> {
        let limit = page_size(self.page_size)
            .map_err(|_| malformed_request([("pageSize", "must not be negative")]))?;
        let cursor = self
            .page_token
            .as_ref()
            .map(|token| token.cursor(filter))
            .transpose()
            .map_err(|_| ErrorReason::InvalidPageToken)?;
        Ok(Paging {
            limit: NonZeroUsize::new(limit).unwrap_or(NonZeroUsize::MIN),
            cursor,
        })
    }
}

/// One page of a list: how many items it holds at most and where it resumes.
#[derive(Debug, PartialEq, Eq)]
struct Paging {
    limit: NonZeroUsize,
    cursor: Option<String>,
}

impl Paging {
    /// Parses the cursor as the identifier used by this list.
    fn cursor_as<T: std::str::FromStr>(&self) -> Result<Option<T>, Status> {
        self.cursor
            .as_deref()
            .map(str::parse)
            .transpose()
            .map_err(|_| ErrorReason::InvalidPageToken.into())
    }
}

/// The page size `value` spells: an integer, which may be larger than any page and is clamped
/// later. `None` when it is not an integer.
fn page_size_of(value: &str) -> Option<i64> {
    value.parse::<i64>().ok().or_else(|| {
        (!value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())).then_some(i64::MAX)
    })
}

/// The unencoded ID of a prebuilt role name. Role names contain only unreserved slug characters.
fn role_id(role: &Role) -> &str {
    role.name.trim_start_matches("roles/")
}

/// The roles of the page that `paging` asks for, and whether roles follow it. A cursor names the
/// last role of the previous page.
fn role_page<'r>(roles: &'r [Role], paging: &Paging) -> Result<(&'r [Role], bool), Status> {
    let start = match paging.cursor.as_deref() {
        None => 0,
        Some(cursor) => roles
            .binary_search_by(|role| role_id(role).cmp(cursor))
            .map(|position| position + 1)
            .map_err(|_| ErrorReason::InvalidPageToken)?,
    };
    let rest = &roles[start..];
    let page = &rest[..rest.len().min(paging.limit.get())];
    Ok((page, rest.len() > page.len()))
}

/// The typed id that the still-encoded path segment `raw` names; `field` names the segment in
/// the refusal.
fn id_of<C: name::Collection>(raw: &str, field: &'static str) -> Result<C::Id, Status> {
    name::decode(raw)
        .ok()
        .and_then(|id| id.parse().ok())
        .ok_or_else(|| malformed_request([(field, "invalid resource id")]))
}

/// Captures the field names supplied by a derived struct deserializer.
fn struct_fields<'de, T: Deserialize<'de>>() -> &'static [&'static str] {
    let mut fields = None;
    let _ = T::deserialize(StructFields(&mut fields));
    fields.unwrap_or(&[])
}

/// A deserializer that captures a struct's field names without reading a value.
struct StructFields<'a>(&'a mut Option<&'static [&'static str]>);

impl<'de> serde::Deserializer<'de> for StructFields<'_> {
    type Error = serde::de::value::Error;

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(serde::de::Error::custom("expected a struct"))
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        *self.0 = Some(fields);
        Err(serde::de::Error::custom("field names captured"))
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct
        map enum identifier ignored_any
    }
}

/// A JSON object key borrowed from the request unless it contains escapes.
#[derive(Deserialize, Eq, Ord, PartialEq, PartialOrd)]
struct BodyField<'a>(#[serde(borrow)] Cow<'a, str>);

/// Collects object keys while ignoring their values.
struct BodyFields;

impl<'de> serde::de::Visitor<'de> for BodyFields {
    type Value = BTreeSet<BodyField<'de>>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: serde::de::MapAccess<'de>,
    {
        let mut fields = BTreeSet::new();
        while let Some((field, IgnoredAny)) = map.next_entry()? {
            fields.insert(field);
        }
        Ok(fields)
    }
}

/// Reads object keys without materializing their values.
fn body_fields(bytes: &[u8]) -> Result<BTreeSet<BodyField<'_>>, Status> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let fields = serde::Deserializer::deserialize_map(&mut deserializer, BodyFields)
        .map_err(|_| malformed_body())?;
    deserializer.end().map_err(|_| malformed_body())?;
    Ok(fields)
}

/// Reports the unknown object keys identified by the request's serde schema.
fn unknown_fields(
    bytes: &[u8],
    is_unknown: impl Fn(&str) -> bool,
) -> Result<Option<Status>, Status> {
    let fields = body_fields(bytes)?;
    let mut unknown = fields
        .iter()
        .filter(|field| is_unknown(field.0.as_ref()))
        .map(|field| (field.0.as_ref(), "unknown field"))
        .peekable();
    if unknown.peek().is_none() {
        Ok(None)
    } else {
        Ok(Some(malformed_request(unknown)))
    }
}

/// The refusal of a request whose body is not the JSON the route takes.
fn malformed_body() -> Status {
    malformed_request([("body", "malformed request body")])
}

/// Renders the dashboard state as one JSON document, or the internal error if it cannot be
/// serialized.
fn state(dashboard: &ApiDashboard) -> Response<ApiResponseBody> {
    match dashboard.hub.state_json() {
        Ok(bytes) => json_response(StatusCode::OK, bytes),
        Err(error) => {
            tracing::error!(%error, "cannot serialize dashboard state");
            failure(ErrorReason::InternalError.into())
        }
    }
}

/// Renders `result` as JSON: the success body with 200, or the refusal with its status.
fn outcome<T: Serialize>(result: Result<T, Status>) -> Response<ApiResponseBody> {
    match result {
        Ok(body) => json(StatusCode::OK, &body),
        Err(error) => failure(error),
    }
}

/// Renders `error` as the error envelope with its HTTP status. A `401` names the scheme the
/// caller may authenticate with.
pub(crate) fn failure(error: Status) -> Response<ApiResponseBody> {
    let status = StatusCode::from_u16(error.code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let retry_delay = error.retry_delay();
    let mut response = json(status, &ErrorResponse::from(error));
    if let Some(delay) = retry_delay {
        response.headers_mut().insert(
            RETRY_AFTER,
            HeaderValue::from(crate::api_error::retry_seconds(delay)),
        );
    }
    if status == StatusCode::UNAUTHORIZED {
        response
            .headers_mut()
            .insert(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    }
    response
}

/// Renders `body` as JSON with `status`. A body that cannot be serialized becomes a constant
/// internal error.
fn json<T: Serialize>(status: StatusCode, body: &T) -> Response<ApiResponseBody> {
    match serde_json::to_vec(body) {
        Ok(bytes) => json_response(status, bytes),
        Err(error) => {
            tracing::error!(%error, "cannot serialize api response");
            json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                Bytes::from_static(INTERNAL_ERROR_BODY.as_bytes()),
            )
        }
    }
}

/// Renders `body` as a JSON response with `status` that the caller must not store.
fn json_response(status: StatusCode, body: impl Into<Bytes>) -> Response<ApiResponseBody> {
    let mut response = Response::new(full_body(body));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    crate::responses::no_store(response.headers_mut());
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paging(limit: usize, cursor: Option<&str>) -> Paging {
        Paging {
            limit: NonZeroUsize::new(limit).expect("limit is nonzero"),
            cursor: cursor.map(str::to_owned),
        }
    }

    fn role(name: &str) -> Role {
        Role {
            name: format!("roles/{name}"),
            permissions: Vec::new(),
            principals: Vec::new(),
        }
    }

    fn reason(status: &Status) -> Option<ErrorReason> {
        status.reason()
    }

    #[test]
    fn list_parameters_are_optional_and_empty_means_unset() {
        let queries = [
            None,
            Some(""),
            Some("other=1"),
            Some("pageSize=&pageToken=&filter="),
        ];
        for query in queries {
            let parsed = ListQuery::parse(query).expect("query parses");
            assert_eq!(parsed.page_size, None, "{query:?}");
            assert_eq!(parsed.page_token, None, "{query:?}");
            assert_eq!(parsed.filter, None, "{query:?}");
        }
    }

    #[test]
    fn list_parameters_are_percent_decoded_and_the_first_pair_wins() {
        let parsed = ListQuery::parse(Some("pageSize=7&pageSize=9&filter=revokeTime%3A*"))
            .expect("query parses");
        assert_eq!(parsed.page_size, Some(7));
        assert_eq!(parsed.filter.as_deref(), Some(REVOKED_FILTER));
    }

    #[test]
    fn invalid_utf8_in_list_parameters_decodes_lossily() {
        let status = ListQuery::parse(Some("pageSize=%FF")).expect_err("page size is refused");
        assert_eq!(reason(&status), Some(ErrorReason::MalformedRequest));
        let parsed = ListQuery::parse(Some("filter=%FF")).expect("query parses");
        assert_eq!(parsed.filter.as_deref(), Some("\u{FFFD}"));
    }

    #[test]
    fn plus_decodes_to_a_space_and_percent_escapes_decode() {
        let parsed = ListQuery::parse(Some(
            "filter=revokeTime%3A*+x&pageToken=a+b&pageSize=%31%30",
        ))
        .expect("query parses");
        assert_eq!(parsed.filter.as_deref(), Some("revokeTime:* x"));
        assert_eq!(parsed.page_token, Some(PageToken::from("a b".to_owned())));
        assert_eq!(parsed.page_size, Some(10));
    }

    #[test]
    fn the_first_pair_of_each_key_wins_even_when_empty() {
        let parsed = ListQuery::parse(Some("filter=&filter=revokeTime:*")).expect("query parses");
        assert_eq!(parsed.filter, None);
    }

    #[test]
    fn malformed_page_sizes_are_refused_with_a_field_violation() {
        for query in ["pageSize=x", "pageSize=1.5", "pageSize=--1"] {
            let status = ListQuery::parse(Some(query)).expect_err("page size is refused");
            assert_eq!(
                reason(&status),
                Some(ErrorReason::MalformedRequest),
                "{query}"
            );
        }
    }

    #[test]
    fn huge_page_sizes_are_clamped_and_negative_ones_refused() {
        let huge = ListQuery::parse(Some("pageSize=99999999999999999999999999"))
            .expect("query parses")
            .paging("")
            .expect("paging is valid");
        assert_eq!(huge.limit.get(), gang_wire::page::PAGE_SIZE_MAX);

        let negative = ListQuery::parse(Some("pageSize=-1"))
            .expect("query parses")
            .paging("")
            .expect_err("negative size is refused");
        assert_eq!(reason(&negative), Some(ErrorReason::MalformedRequest));
    }

    #[test]
    fn page_tokens_resume_only_the_list_they_were_issued_for() {
        let token = PageToken::new(REVOKED_FILTER, "example-idp:ada");
        let query = ListQuery {
            page_token: Some(token),
            ..ListQuery::default()
        };

        let resumed = query.paging(REVOKED_FILTER).expect("token is valid");
        assert_eq!(resumed.cursor.as_deref(), Some("example-idp:ada"));
        let refused = query.paging("").expect_err("token is for another filter");
        assert_eq!(reason(&refused), Some(ErrorReason::InvalidPageToken));
    }

    #[test]
    fn paging_parses_typed_cursors_and_rejects_invalid_values() {
        assert_eq!(paging(1, None).cursor_as::<u64>().expect("no cursor"), None);
        assert_eq!(
            paging(1, Some("42"))
                .cursor_as::<u64>()
                .expect("valid cursor"),
            Some(42)
        );
        let error = paging(1, Some("not-a-number"))
            .cursor_as::<u64>()
            .expect_err("invalid cursor");
        assert_eq!(reason(&error), Some(ErrorReason::InvalidPageToken));
    }

    #[test]
    fn roles_page_in_name_order() {
        let roles = [role("admin"), role("auditor"), role("reader")];

        let (first, more) = role_page(&roles, &paging(2, None)).expect("first page");
        assert_eq!(first, &roles[..2]);
        assert!(more);

        let (second, more) = role_page(&roles, &paging(2, Some("auditor"))).expect("second page");
        assert_eq!(second, &roles[2..]);
        assert!(!more);

        let (all, more) = role_page(&roles, &paging(3, None)).expect("one page");
        assert_eq!(all, &roles[..]);
        assert!(!more);
    }

    #[test]
    fn a_role_cursor_that_names_no_role_is_an_invalid_token() {
        let status = role_page(&[role("admin")], &paging(1, Some("gone"))).expect_err("refused");
        assert_eq!(reason(&status), Some(ErrorReason::InvalidPageToken));
    }

    #[test]
    fn path_ids_are_percent_decoded_and_parsed() {
        let identity = id_of::<name::Principals>("example-idp%3Aada%40example.org", "principal")
            .expect("id parses");
        assert_eq!(identity.to_string(), "example-idp:ada@example.org");

        for raw in ["", "no-separator", "example-idp%3A%ZZ", "%FF"] {
            let status = id_of::<name::Principals>(raw, "principal").expect_err("id is refused");
            assert_eq!(
                reason(&status),
                Some(ErrorReason::MalformedRequest),
                "{raw:?}"
            );
        }
    }

    #[test]
    fn unauthorized_failures_name_the_bearer_scheme() {
        let unauthorized = failure(ErrorReason::CredentialsMissing.into());
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            unauthorized.headers().get(WWW_AUTHENTICATE),
            Some(&HeaderValue::from_static("Bearer"))
        );

        let forbidden = failure(missing_permission(gang_types::Permission::StateRead));
        assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);
        assert_eq!(forbidden.headers().get(WWW_AUTHENTICATE), None);
    }
}
