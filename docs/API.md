# Gang HTTP API

The Gang API is a JSON API under `/v1` on the server's public listener, next to the REAPI services. It is resource-oriented in the style of Google's API Improvement Proposals: named resources, standard `Get` and `List` methods, single-verb custom methods (`:authenticate`, `:revoke`, `:restore`, `:watch`), and one error envelope. The browser surface under `/ui` is not API; it is described under [Browser flows](#browser-flows-ui).

This file is the one reference for routes and bodies. The route table, the wire types and the error catalog live in the `gang-wire` crate, and a test of `gang-server` fails if an example here stops parsing into its wire type, if a route is missing here or undocumented routes are served, or if the error table differs from the catalog.

Conventions that hold for every route:

- **Versioning.** The major version is the URL prefix. `/v1` evolves additively only: fields, enum values, oneof members, error reasons, metadata keys and routes may be added, and nothing is renamed, repurposed or removed. A break would be served as `/v2` alongside.
- **Authentication.** A caller presents `Authorization: Bearer <token>`, a token obtained from `POST /v1/session:authenticate`. The `GET` routes marked "cookie" below also accept the browser session cookie; the session cookie is never read for `POST`, so no state-changing route authenticates by it. A deployment whose authentication is `none` needs no credential and identifies every caller as the principal `none:anonymous`, whose permissions come from its roles like any other.
- **Authorization.** Each route has a permission, or is `open`. A caller holds a permission through a `[[server.roles]]` role of `gang.toml`. The caller is authenticated and authorized before the existence of any named resource is checked, so a caller without the permission learns nothing about what exists.
- **Names.** A resource is named `collection/{id}`; singletons are `server`, `session` and `state`. An ID is exactly one path segment: every byte outside `A-Z a-z 0-9 - . _ ~` is percent-encoded with uppercase hex, in URLs and in every `name` or reference field, so an encoded ID never contains `:` or `/`. The verb of a custom method is the text after the last `:` of the final segment. The principal `google:ops@example.org` is therefore `principals/google%3Aops%40example.org`, and `POST /v1/principals/google%3Aops%40example.org:revoke` revokes it. The decoded ID of a principal is its identity string `provider:principal`, the same string as the token `sub`. A reference to another resource is always its full name (`fleets/x86-64-spot`), never a bare ID.
- **JSON.** Keys are lowerCamelCase (map keys, such as fleet `properties` and the provider names under `authentication.token.login`, are data and are not renamed). Timestamps are RFC 3339 in UTC with milliseconds, `2026-01-14T09:20:02.000Z`, and field names end in `Time`; a parser accepts any RFC 3339 offset. Durations are seconds with an optional fraction and an `s` suffix, `600s` or `0.250s`. Enums are `UPPER_SNAKE` strings. A union is a oneof: exactly one member key is present and there is no tag field. Absent optional values are omitted, never `null`; scalars (counts, booleans, `0`) are always present. 64-bit integers are JSON numbers.
- **Leniency.** A request rejects unknown fields. A response may gain fields, so clients ignore unknown response fields, unknown enum values, unknown oneof members and unknown error-detail types.
- **Success.** Every success is `200`, including create. Every JSON response, errors included, carries `Content-Type: application/json` and `Cache-Control: no-store`.
- **Lists.** The three list routes take `pageSize` and `pageToken` and answer the plural collection key followed by `nextPageToken`, which is omitted on the last page. `pageSize` defaults to 100 and is clamped to 1000; a negative value is `400 MALFORMED_REQUEST`. A page token is opaque and URL-safe; one that is malformed, or was issued for a different `filter`, is `400 INVALID_PAGE_TOKEN`. Order is fixed and has no `orderBy`. Walk a collection by passing each `nextPageToken` back unchanged until it is absent.
- **Wrong method, unknown path.** A route is the pair of method and path. A wrong method is `404 ROUTE_NOT_FOUND`, there is no `405`, and a route that is not mounted is `404 ROUTE_NOT_FOUND` too. For an anonymous caller an unknown path is `401 CREDENTIALS_MISSING`, so routes cannot be probed anonymously.
- **Query parameters.** An empty `pageSize=`, `pageToken=` or `filter=` counts as unset. Query values are decoded as `application/x-www-form-urlencoded`: `%XX` escapes decode, `+` is a space, and bytes that are not UTF-8 are replaced. A `pageSize` that is not an integer, or a path ID with bad percent-encoding, is `400 MALFORMED_REQUEST` whose `BadRequest` names the field (`pageSize`, or the path parameter); an unsupported `filter` names `filter`. A request body that is not valid JSON names the field `body`; unknown fields are each named.

## server

### Name

The singleton `server`: how to reach and authenticate to this deployment. It carries no identifier beyond its name.

### Methods

| Method | Path | Permission |
|---|---|---|
| `GET` | `/v1/server` | `open` |

### Examples

A deployment with token authentication. Under `authentication.token.login` each key is an identity-provider name and each value is that provider's own login settings, owned by the provider plugin and not by the Gang wire format, so its keys keep the provider's spelling. Only providers that offer interactive login are listed.

```sh
curl https://gang.example.com/v1/server
```

```json response Server
{
  "name": "server",
  "remoteExecutor": "grpcs://gang.example.com:443",
  "remoteCache": "grpcs://gang.example.com:443",
  "caCertificate": "-----BEGIN CERTIFICATE-----\nMIIBszCCAVmgAwIBAgIUdGhpcyBpcyBhbiBleGFtcGxl\n-----END CERTIFICATE-----\n",
  "defaultIsa": "x86-64",
  "authentication": {
    "token": {
      "login": {
        "google": {
          "issuer_url": "https://accounts.example.com",
          "audience": "1234-example.apps.example.com",
          "principal_claim": "email",
          "login": {
            "scopes": ["openid", "email"]
          }
        }
      },
      "deviceLogin": {
        "provider": "google"
      }
    }
  }
}
```

A deployment on a trusted network with no authentication:

```json response Server
{
  "name": "server",
  "remoteExecutor": "grpc://gang.example.com:8980",
  "remoteCache": "grpc://gang.example.com:8980",
  "defaultIsa": "aarch64",
  "authentication": {
    "none": {}
  }
}
```

### Errors

| Reason | When |
|---|---|
| `ROUTE_NOT_FOUND` | the method is not `GET` |
| `RATE_LIMITED` | the caller sent more requests than `[server.rate_limit]` allows |

### Notes

- `remoteExecutor` and `remoteCache` are the endpoints for Bazel's `--remote_executor` and `--remote_cache`; the scheme is `grpcs` when the listener serves TLS and `grpc` otherwise.
- `caCertificate` is the certificate authority the client should trust for those endpoints; it is omitted when the server has none, and when `[server] endpoint_trust = "system"`, where a publicly trusted terminator presents the endpoint and clients use their system roots.
- `defaultIsa` is the instruction set a build gets when it does not name one. `gang bazelrc` writes it as `--remote_default_exec_properties=ISA=<defaultIsa>`.
- `authentication` is a oneof. `token` means the client obtains a bearer token with `POST /v1/session:authenticate`; `none` means no credential is needed. A client that meets a member it does not know must stop and say so.
- `authentication.token.deviceLogin` names the dashboard sign-in provider when device login is mounted; it is omitted otherwise.
- The route is open: it is the first call a client makes, before it has any credential.

## session

### Name

The singleton `session`: the caller's own session. It always exists; an anonymous caller has a session with no `principal`.

### Methods

| Method | Path | Permission |
|---|---|---|
| `GET` | `/v1/session` | `open` |
| `POST` | `/v1/session:authenticate` | `open` |

### Examples

Exchange a proof from an identity provider for a bearer token. The proof is the credential, so the route is open. `lifetime` is relative to the response, so client clock skew does not matter.

```sh
curl -X POST https://gang.example.com/v1/session:authenticate \
  -d '{"provider":"google","proof":"eyJhbGciOiJSUzI1NiJ9.e30.c2ln"}'
```

```json request AuthenticateRequest
{
  "provider": "google",
  "proof": "eyJhbGciOiJSUzI1NiJ9.e30.c2ln"
}
```

```json response AuthenticateResponse
{
  "token": "eyJhbGciOiJFUzI1NiJ9.e30.c2ln",
  "lifetime": "600s"
}
```

Renew a device login through Gang's confidential web client:

```json request AuthenticateRequest
{
  "provider": "google",
  "refreshToken": "provider-refresh-token"
}
```

```json response AuthenticateResponse
{
  "token": "eyJhbGciOiJFUzI1NiJ9.e30.c2ln",
  "lifetime": "600s",
  "refreshToken": "rotated-provider-refresh-token"
}
```

Read the session of the token's bearer:

```sh
curl -H "Authorization: Bearer $TOKEN" https://gang.example.com/v1/session
```

```json response Session
{
  "name": "session",
  "principal": "principals/google%3Aops%40example.org",
  "permissions": ["state.read", "events.read"],
  "expireTime": "2026-01-14T09:30:00.000Z",
  "login": {
    "provider": "google"
  }
}
```

The same route without a credential answers `200` with an anonymous session:

```json response Session
{
  "name": "session",
  "permissions": []
}
```

### Errors

| Reason | When |
|---|---|
| `MALFORMED_REQUEST` | the body is not JSON, has an unknown field or wrong type, or has both or neither of `proof` and `refreshToken` |
| `REQUEST_TOO_LARGE` | the body exceeds `max_request_size` |
| `UNKNOWN_PROVIDER` | `provider` names an identity provider the server does not configure; the metadata key is `provider` |
| `PROOF_REJECTED` | the provider rejected the proof or refresh token, or refresh was requested for a provider other than the mounted device-login provider (`401`) |
| `POLICY_DENIED` | the issuance policy refuses the principal |
| `PRINCIPAL_REVOKED` | the proof verified, but its principal is revoked |
| `RATE_LIMITED` | the caller sent more requests than `[server.rate_limit]` allows (`429`) |
| `DEPENDENCY_UNAVAILABLE` | the identity provider or the signer cannot be reached |

```json error UNKNOWN_PROVIDER
{
  "error": {
    "code": 400,
    "message": "unknown identity provider",
    "status": "INVALID_ARGUMENT",
    "details": [
      {
        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
        "reason": "UNKNOWN_PROVIDER",
        "domain": "gang",
        "metadata": {
          "provider": "example-idp"
        }
      }
    ]
  }
}
```

### Notes

- `GET /v1/session` is always `200`, also for a caller with no credential, an expired cookie, or no valid proof: the caller's session always exists, and an anonymous session simply has no `principal`. This is deliberate and is not an error case. A caller with a revoked principal or a malformed `Authorization` header is still refused with the envelope (`PRINCIPAL_REVOKED`, `CREDENTIALS_INVALID`). A bearer token works on this route, so a CLI gets `whoami` from it, and the browser classifies a failed `state:watch` connect by calling it.
- `GET /v1/session` accepts the session cookie. `permissions` is the set the caller holds, with `*` expanded to the permission names, in canonical order. `expireTime` is when the caller's credential stops being valid and is omitted when it does not expire. `login` is present exactly when the browser login routes are mounted; its `provider` is the provider the dashboard signs in with.
- `POST /v1/session:authenticate` is mounted only if the client authentication plugin issues tokens (`authentication` is `token` in `GET /v1/server`); otherwise it is `404 ROUTE_NOT_FOUND`. It verifies the proof with the named provider, applies the provider's required claims, and signs an access token for `provider:principal`. A token is accepted until its `lifetime` has elapsed; there is no per-token revocation, but revoking the principal makes the gate refuse its tokens at the next request.
- The request contains exactly one of `proof` and `refreshToken`. A refresh token is accepted only for the mounted device-login provider; the server renews it with the dashboard web client, then verifies and issues as for a proof. `refreshToken` in the response is optional and carries a rotated provider refresh token when returned; store it before the bearer token.
- The token is opaque to clients. Present it as `Authorization: Bearer <token>`.

## deviceAuthorizations

### Name

`deviceAuthorizations/{userCode}`: a pending headless login. The user code is displayed as `XXXX-XXXX`.

### Methods

| Method | Path | Permission |
|---|---|---|
| `POST` | `/v1/deviceAuthorizations` | `open` |
| `GET` | `/v1/deviceAuthorizations/{deviceAuthorization}` | `open` |
| `POST` | `/v1/deviceAuthorizations/{deviceAuthorization}:redeem` | `open` |

### Examples

Create an authorization with an empty body or `{}`:

```sh
curl -X POST https://gang.example.com/v1/deviceAuthorizations -d '{}'
```

```json response DeviceAuthorization
{
  "name": "deviceAuthorizations/BCDF-GHJK",
  "provider": "google",
  "verificationUri": "https://gang.example.com/ui/device",
  "clientAddress": "203.0.113.9",
  "state": "PENDING",
  "createTime": "2026-01-14T09:20:00.000Z",
  "expireTime": "2026-01-14T09:30:00.000Z",
  "pollInterval": "5s",
  "secret": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
}
```

Get the authorization, without its secret:

```sh
curl https://gang.example.com/v1/deviceAuthorizations/BCDF-GHJK
```

```json response DeviceAuthorization
{
  "name": "deviceAuthorizations/BCDF-GHJK",
  "provider": "google",
  "verificationUri": "https://gang.example.com/ui/device",
  "clientAddress": "203.0.113.9",
  "state": "APPROVED",
  "createTime": "2026-01-14T09:20:00.000Z",
  "expireTime": "2026-01-14T09:30:00.000Z",
  "pollInterval": "5s"
}
```

Redeem after the user approves:

```sh
curl -X POST https://gang.example.com/v1/deviceAuthorizations/BCDF-GHJK:redeem \
  -d '{"secret":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}'
```

```json request RedeemDeviceAuthorizationRequest
{
  "secret": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
}
```

```json response AuthenticateResponse
{
  "token": "eyJhbGciOiJFUzI1NiJ9.e30.c2ln",
  "lifetime": "600s",
  "refreshToken": "provider-refresh-token"
}
```

### Errors

| Reason | When |
|---|---|
| `AUTHORIZATION_PENDING` | redemption is not yet approved (`400`) |
| `DEVICE_AUTHORIZATION_NOT_FOUND` | the code is unknown, expired or already redeemed, or the secret is wrong; all give the identical `404` with `deviceAuthorization` metadata |
| `RATE_LIMITED` | the caller exceeds the rate limit or polls before `pollInterval`; retry after `Retry-After` (`429`) |
| `PRINCIPAL_REVOKED` | the approved principal is revoked at redemption (`403`); the authorization is consumed |

### Notes

- These routes are open and rate limited, and mounted only when device login is enabled and dashboard sign-in is mounted. `authentication.token.deviceLogin` announces the provider.
- The user types the code at `verificationUri` and signs in to approve. `clientAddress`, when present, is the server-observed address of the creator. Codes ignore case, spaces and hyphens on input.
- `secret` is returned only on create and is required for redemption. The authorization is in memory, bounded and expiring; the first approval wins, and successful redemption consumes it.
- Approval requests offline access. Redemption applies the normal issuance policy and revocation check and returns the provider refresh token; never expose the secret or refresh token to another user.

## state

### Name

The singleton `state`: the fleet as the control loop last saw it, with the workers, instances, queue, running and queued actions, and the most recent scaling events. It is derived from the control loop and never authoritative: nothing reads it back to decide.

### Methods

| Method | Path | Permission |
|---|---|---|
| `GET` | `/v1/state` | `state.read` |
| `GET` | `/v1/state:watch` | `state.read` |

### Examples

The current state. Embedded resources (fleets, workers, instances, actions, events) have names so they can become collections later without changing their representation; they have no routes of their own.

```sh
curl -H "Authorization: Bearer $TOKEN" https://gang.example.com/v1/state
```

```json response State
{
  "name": "state",
  "seq": 4812,
  "updateTime": "2026-01-14T09:20:02.000Z",
  "provisioner": "aws",
  "pass": {
    "state": "OK",
    "interval": "2s",
    "applied": 2,
    "failed": 0
  },
  "queue": {
    "total": 1,
    "unplaceable": 0,
    "oldestQueueTime": "2026-01-14T09:19:51.250Z"
  },
  "fleets": [
    {
      "name": "fleets/x86-64-spot",
      "isa": "x86-64",
      "sandbox": "bwrap",
      "properties": {
        "capacity": "spot"
      },
      "instanceSize": {
        "vcpus": 8,
        "memory": 17179869184
      },
      "policy": {
        "min": 2,
        "max": 20,
        "targetActions": 4,
        "idleTimeout": "600s",
        "maxDrainWait": "1800s",
        "bootGrace": "300s"
      },
      "desired": 2,
      "instances": {
        "pending": 1,
        "inService": 1,
        "terminating": 0,
        "warm": 0
      },
      "workers": {
        "busy": 1,
        "idle": 0,
        "draining": 0,
        "unconnected": 0
      },
      "demand": {
        "running": 1,
        "queued": 1,
        "candidates": 1,
        "needed": 2,
        "vcpus": 2,
        "memory": 4294967296
      }
    }
  ],
  "workers": [
    {
      "name": "workers/i-0e1000000000000a-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b",
      "fleet": "fleets/x86-64-spot",
      "instance": "instances/i-0e1000000000000a",
      "phase": "BUSY",
      "phaseTime": "2026-01-14T09:19:58.000Z",
      "running": 1,
      "slots": 4,
      "paused": false,
      "launchTime": "2026-01-14T09:18:30.000Z"
    }
  ],
  "instances": [
    {
      "name": "instances/i-0e1000000000000b",
      "fleet": "fleets/x86-64-spot",
      "lifecycle": "PENDING",
      "launchTime": "2026-01-14T09:19:41.000Z",
      "pastBootGrace": false
    }
  ],
  "actions": {
    "sampleTime": "2026-01-14T09:20:00.000Z",
    "staleAfter": "10s",
    "running": [
      {
        "name": "actions/ac9b1c52-3d0e-4f61-9a27-51c6d7e8f901",
        "fleet": "fleets/x86-64-spot",
        "worker": "workers/i-0e1000000000000a-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b",
        "digest": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/142",
        "target": "//app:server_test",
        "mnemonic": "TestRunner",
        "startTime": "2026-01-14T09:19:58.000Z",
        "priority": 0
      }
    ],
    "queued": [
      {
        "name": "actions/0b8e6f10-52c4-4a7d-8e13-9f4a2c6d7b30",
        "digest": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08/311",
        "target": "//app:lib",
        "mnemonic": "CppCompile",
        "queueTime": "2026-01-14T09:19:51.250Z",
        "priority": 0,
        "candidates": ["fleets/x86-64-spot"]
      }
    ],
    "truncated": {
      "running": 0,
      "queued": 0
    }
  },
  "events": [
    {
      "name": "events/995",
      "createTime": "2026-01-14T09:19:41.000Z",
      "fleet": "fleets/x86-64-spot",
      "cause": "DEMAND",
      "scaledOut": {
        "from": 1,
        "to": 2
      }
    },
    {
      "name": "events/996",
      "createTime": "2026-01-14T09:19:58.000Z",
      "fleet": "fleets/x86-64-spot",
      "workerRegistered": {
        "worker": "workers/i-0e1000000000000a-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b"
      }
    }
  ],
  "eventsLimit": 200,
  "completed": 980,
  "principalsEpoch": 3
}
```

`GET /v1/state:watch` answers `200` with `Content-Type: text/event-stream`. This transcript shows a connect, one change, a heartbeat, and the server ending the stream because the viewer cap was reached. Every frame is `event:` and `id:` lines and one `data:` line holding one JSON object, ended by a blank line; a `closed` frame with a retry delay starts with the SSE `retry:` line, in milliseconds. The snapshot here is cut to an empty fleet for brevity.

```sh
curl -N -H "Authorization: Bearer $TOKEN" https://gang.example.com/v1/state:watch
```

```sse
event: snapshot
id: 4812
data: {"name":"state","seq":4812,"updateTime":"2026-01-14T09:20:02.000Z","provisioner":"aws","pass":{"state":"OK","interval":"2s","applied":0,"failed":0},"queue":{"total":0,"unplaceable":0},"fleets":[],"workers":[],"instances":[],"events":[{"name":"events/996","createTime":"2026-01-14T09:19:58.000Z","fleet":"fleets/x86-64-spot","workerRegistered":{"worker":"workers/i-0e1000000000000a-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b"}}],"eventsLimit":200,"completed":980,"principalsEpoch":3}

event: delta
id: 4813
data: {"seq":4813,"base":4812,"updateTime":"2026-01-14T09:20:04.000Z","queue":{"total":0,"unplaceable":0},"workers":{"upsert":[{"name":"workers/i-0e1000000000000a-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b","fleet":"fleets/x86-64-spot","instance":"instances/i-0e1000000000000a","phase":"IDLE","phaseTime":"2026-01-14T09:20:04.000Z","running":0,"slots":4,"paused":false,"launchTime":"2026-01-14T09:18:30.000Z"}]},"actions":{"sampleTime":"2026-01-14T09:20:04.000Z","running":{"remove":["actions/ac9b1c52-3d0e-4f61-9a27-51c6d7e8f901"]},"queued":[]},"events":[{"name":"events/997","createTime":"2026-01-14T09:20:04.000Z","fleet":"fleets/x86-64-spot","cause":"IDLE_TIMEOUT","drainStarted":{"worker":"workers/i-0e1000000000000a-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b"}}],"completed":981}

event: tick
id: 4813
data: {"seq":4813,"time":"2026-01-14T09:20:12.000Z"}

retry: 5000
event: closed
data: {"reason":"VIEWER_CAP","retryDelay":"5s"}
```

### Errors

| Reason | When |
|---|---|
| `CREDENTIALS_MISSING` | no credential was presented (`401`) |
| `CREDENTIALS_INVALID` | the credential was presented but refused (`401`) |
| `PRINCIPAL_REVOKED` | the caller's principal is revoked |
| `MISSING_PERMISSION` | the caller lacks `state.read`; the metadata key is `permission` |
| `ROUTE_NOT_FOUND` | the dashboard is disabled (`[dashboard] enabled = false`), so the state is not served |

### Notes

- **Frames.** `Frame` is the type of each stream message, defined once in `gang-wire`:

  | Event | Data | Sent |
  |---|---|---|
  | `snapshot` | `State` | first on every connect, and again when a viewer fell behind the broadcast |
  | `delta` | `{seq, base, updateTime, pass?, queue?, fleets?, workers?, instances?, actions?, events?, completed?, principalsEpoch?}` | when a pass changed something and a viewer is connected |
  | `tick` | `{seq, time}` | every heartbeat interval, with the current `seq` |
  | `closed` | `{reason, retryDelay?}` | last, when the server ends the stream deliberately |

  The SSE event names `snapshot`, `delta`, `tick` and `closed` are protocol identifiers and stay lowercase. `id:` is the frame's `seq`. The server may also write comment lines starting with `:` between frames; a reader ignores them.
- **Applying a delta.** A delta moves the state from `base` to `seq`. A part that is present replaces the whole part (`pass`, `queue`, `completed`, `principalsEpoch`); `fleets`, `workers` and `instances` are `{upsert[], remove[]}` keyed by `name`, and a name is in at most one of the two lists (empty lists are omitted); `events` are appended, newest last. The `actions` patch is `{sampleTime?, staleAfter?, running?, queued?, truncated?}`: `running` is keyed by `name`, `queued` replaces the whole list. When `actions` first appears, the patch carries every field and all rows in `running.upsert`.
- **Resynchronizing.** `seq` advances only when a delta is sent, and only while a viewer is connected. A client reconnects when a delta's `base` differs from its own `seq`, or when a tick's `seq` is behind its own state. A tick whose `seq` equals or runs ahead of the client's is normal (a delta may still be queued for that viewer). There is no replay and no `Last-Event-ID` handling: every connect begins with a snapshot, and a second `snapshot` on an open stream replaces the client's state.
- **Events in the state.** `State.events` holds the newest `eventsLimit` scaling events, newest last (ascending). The same events are on `GET /v1/events`, which pages the whole history newest first.
- **Closing.** `closed.reason` is one of the values below. `retryDelay` is a `Duration` and the SSE `retry:` line repeats it in milliseconds. A client treats a reason it does not know as terminal and reconnects only after the delay, if there is one.

  | `reason` | When | `retryDelay` |
  |---|---|---|
  | `REVOKED` | the caller's principal was revoked | none |
  | `EXPIRED` | the caller's credential reached its expiry | none |
  | `VIEWER_CAP` | `[dashboard] max_viewers` streams are open; the response is a `200` stream that sends `closed` at once | `[dashboard] retry_after` |
  | `SHUTDOWN` | the server is stopping | `[dashboard] retry_after` |

- **Failures before the stream.** An authentication or authorization failure at connect is the normal error envelope with its HTTP status, not a `closed` frame. `EventSource` hides the status, so a browser classifies a failed connect by calling `GET /v1/session`.
- **Sampling.** `actions` is omitted until the first sample after the controller starts. It is stale when the frame's `updateTime` minus `sampleTime` exceeds `staleAfter`. A worker's `phaseTime` is carried over while its phase is unchanged, so rows do not change on every pass. `completed` counts actions that reached a result since the controller started; a decrease means a restart.
- Both routes accept the session cookie.

## principals

### Name

`principals/{principal}`. The ID is the identity string `provider:principal`, percent-encoded: `principals/google%3Aops%40example.org`. The registry records every principal the server has seen authenticate, plus any principal that was revoked before it was seen.

### Methods

| Method | Path | Permission |
|---|---|---|
| `GET` | `/v1/principals` | `principals.read` |
| `POST` | `/v1/principals/{principal}:revoke` | `principals.revoke` |
| `POST` | `/v1/principals/{principal}:restore` | `principals.revoke` |

### Examples

List the principals, two at a time. `filter=revokeTime:*` lists only the revoked ones.

```sh
curl -H "Authorization: Bearer $TOKEN" 'https://gang.example.com/v1/principals?pageSize=2'
curl -H "Authorization: Bearer $TOKEN" 'https://gang.example.com/v1/principals?filter=revokeTime:*'
```

```json response ListPrincipalsResponse
{
  "principals": [
    {
      "name": "principals/aws-workers%3Ai-0e10",
      "provider": "aws-workers",
      "principal": "i-0e10",
      "createTime": "2026-01-14T09:00:00.000Z",
      "lastSeenTime": "2026-01-14T09:00:00.000Z"
    },
    {
      "name": "principals/google%3Aops%40example.org",
      "provider": "google",
      "principal": "ops@example.org",
      "createTime": "2026-01-02T08:00:00.000Z",
      "lastSeenTime": "2026-01-14T09:20:01.250Z",
      "revokeTime": "2026-01-14T09:30:00.000Z"
    }
  ],
  "nextPageToken": "AQBnb29nbGU6b3BzQGV4YW1wbGUub3Jn"
}
```

Revoke a principal. The body is absent or `{}`. The principal is revoked, and every active certificate it holds is revoked with it; the response lists the certificates this call revoked.

```sh
curl -X POST -H "Authorization: Bearer $TOKEN" \
  'https://gang.example.com/v1/principals/aws-workers%3Ai-0e10:revoke'
```

```json response RevokePrincipalResponse
{
  "principal": {
    "name": "principals/aws-workers%3Ai-0e10",
    "provider": "aws-workers",
    "principal": "i-0e10",
    "createTime": "2026-01-14T09:00:00.000Z",
    "lastSeenTime": "2026-01-14T09:00:00.000Z",
    "revokeTime": "2026-01-14T09:30:00.000Z"
  },
  "certificates": [
    {
      "name": "certificates/3fa9",
      "principal": "principals/aws-workers%3Ai-0e10",
      "createTime": "2026-01-14T09:00:00.000Z",
      "expireTime": "2026-01-21T09:00:00.000Z",
      "revokeTime": "2026-01-14T09:30:00.000Z"
    }
  ]
}
```

Restore a revoked principal:

```sh
curl -X POST -H "Authorization: Bearer $TOKEN" \
  'https://gang.example.com/v1/principals/aws-workers%3Ai-0e10:restore'
```

```json response Principal
{
  "name": "principals/aws-workers%3Ai-0e10",
  "provider": "aws-workers",
  "principal": "i-0e10",
  "createTime": "2026-01-14T09:00:00.000Z",
  "lastSeenTime": "2026-01-14T09:00:00.000Z"
}
```

### Errors

| Reason | When |
|---|---|
| `MISSING_PERMISSION` | the caller lacks `principals.read` (list) or `principals.revoke` (revoke, restore) |
| `MALFORMED_REQUEST` | `pageSize` is negative or not an integer, `filter` is not `revokeTime:*`, the ID in the path is not valid percent-encoding of an identity string, or a revoke or restore body is not empty or `{}`; the `BadRequest` detail names the field |
| `INVALID_PAGE_TOKEN` | the token is malformed, or was issued for a different `filter` |
| `PRINCIPAL_NOT_FOUND` | restore of a principal the registry has never seen; the metadata key is `principal` |
| `DEPENDENCY_UNAVAILABLE` | the ledger cannot be reached |

```json error INVALID_PAGE_TOKEN
{
  "error": {
    "code": 400,
    "message": "invalid page token",
    "status": "INVALID_ARGUMENT",
    "details": [
      {
        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
        "reason": "INVALID_PAGE_TOKEN",
        "domain": "gang"
      }
    ]
  }
}
```

```json error PRINCIPAL_NOT_FOUND
{
  "error": {
    "code": 404,
    "message": "principal not found",
    "status": "NOT_FOUND",
    "details": [
      {
        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
        "reason": "PRINCIPAL_NOT_FOUND",
        "domain": "gang",
        "metadata": {
          "principal": "principals/github-actions%3Aexample-org%2Fexample-repo"
        }
      }
    ]
  }
}
```

### Notes

- **Order.** Principals are listed by provider, then principal, ascending. `nextPageToken` resumes after the last principal of the page.
- **Filter.** `filter` accepts one AIP-160 expression, `revokeTime:*`, which matches principals that have a `revokeTime`. Any other expression is `400 MALFORMED_REQUEST`. A page token belongs to the `filter` it was issued for.
- **Revoking is idempotent.** Repeating a revoke answers `200` and keeps the original `revokeTime`; `certificates` is then empty because this call revoked none. The revocation is recorded before the live sets are updated, so every surface refuses the principal at its next request, an open stream for it ends with `closed REVOKED`, and its bearer tokens stop working. Revoking a principal the registry has not seen creates its row, so a principal can be revoked before it first authenticates.
- **Restoring** clears `revokeTime` and puts the principal back into service. Certificates revoked with it stay revoked.
- `principal` is the principal as its provider names it; `name` is the resource name derived from `provider` and `principal`. `createTime` is when the registry first saw it, and `lastSeenTime` when it last authenticated.
- `GET /v1/principals` accepts the session cookie.

## certificates

### Name

`certificates/{certificate}`. The ID is the certificate serial as lowercase hexadecimal, such as `certificates/3fa9`.

### Methods

| Method | Path | Permission |
|---|---|---|
| `POST` | `/v1/certificates` | `open` |
| `POST` | `/v1/certificates/{certificate}:revoke` | `principals.revoke` |

### Examples

Enroll a build worker: present a proof from the worker identity provider and a certificate signing request, and receive a client certificate. The private key never leaves the worker. The proof is the credential, so the route is open.

```sh
curl -X POST https://gang.example.com/v1/certificates -d @request.json
```

```json request CreateCertificateRequest
{
  "provider": "aws-workers",
  "proof": "eyJhbGciOiJSUzI1NiJ9.e30.c2ln",
  "pemCsr": "-----BEGIN CERTIFICATE REQUEST-----\nMIIBHjCBxQIBADAAMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE\n-----END CERTIFICATE REQUEST-----\n"
}
```

```json response Certificate
{
  "name": "certificates/3fa9",
  "principal": "principals/aws-workers%3Ai-0e10",
  "createTime": "2026-01-14T09:00:00.000Z",
  "expireTime": "2026-01-21T09:00:00.000Z",
  "pemCertificate": "-----BEGIN CERTIFICATE-----\nMIIBszCCAVmgAwIBAgICM/kwCgYIKoZIzj0EAwIwEjEQMA4GA1UE\n-----END CERTIFICATE-----\n",
  "caCertificate": "-----BEGIN CERTIFICATE-----\nMIIBszCCAVmgAwIBAgIUdGhpcyBpcyBhbiBleGFtcGxl\n-----END CERTIFICATE-----\n"
}
```

Revoke one certificate. The body is absent or `{}`; the response is the certificate with its `revokeTime`.

```sh
curl -X POST -H "Authorization: Bearer $TOKEN" \
  https://gang.example.com/v1/certificates/3fa9:revoke
```

```json response Certificate
{
  "name": "certificates/3fa9",
  "principal": "principals/aws-workers%3Ai-0e10",
  "createTime": "2026-01-14T09:00:00.000Z",
  "expireTime": "2026-01-21T09:00:00.000Z",
  "revokeTime": "2026-01-14T09:30:00.000Z"
}
```

### Errors

| Reason | When |
|---|---|
| `MALFORMED_REQUEST` | the body is not JSON, has an unknown field, or `pemCsr` is not a certificate request; the serial in the path is not a valid ID |
| `REQUEST_TOO_LARGE` | the body exceeds `max_request_size` |
| `UNKNOWN_PROVIDER` | `provider` names an identity provider the server does not configure; the metadata key is `provider` |
| `PROOF_REJECTED` | the identity provider rejected the proof (`401`) |
| `POLICY_DENIED` | the issuance policy refuses the principal |
| `PRINCIPAL_REVOKED` | the proof verified, but its principal is revoked |
| `MISSING_PERMISSION` | revoke without `principals.revoke` |
| `CERTIFICATE_NOT_FOUND` | revoke of a serial that no surface issued; the metadata key is `certificate` |
| `DEPENDENCY_UNAVAILABLE` | the ledger, the identity provider or the signer cannot be reached |

### Notes

- `POST /v1/certificates` is mounted only if the worker authentication plugin offers certificates; otherwise it is `404 ROUTE_NOT_FOUND`. A certificate is `200` on create, like every success.
- The issuance is recorded in the ledger before the response is sent. A client that retries after a lost response gets a second certificate: it is harmless, it expires, and revoking the principal covers it.
- `pemCertificate` is returned only by create. Revoke returns the certificate without it, and `revokeTime` is present only once the certificate is revoked.
- `caCertificate` is the certificate of the authority that issued it, which a worker trusts for the worker listener. Like `pemCertificate`, only create returns it.
- `principal` is the principal the certificate was issued to. `createTime` and `expireTime` bound the certificate's validity.
- **Revoking is idempotent.** Repeating a revoke answers `200` and keeps the original `revokeTime`. Revoking a certificate does not revoke its principal; revoke the principal to revoke all its certificates.
- Certificates are revoked through `principals.revoke`: one revoke permission covers principals and certificates.

## roles

### Name

`roles/{role}`. The ID is the role name from `[[server.roles]]` in `gang.toml`. Roles are read-only.

### Methods

| Method | Path | Permission |
|---|---|---|
| `GET` | `/v1/roles` | `principals.read` |

### Examples

```sh
curl -H "Authorization: Bearer $TOKEN" https://gang.example.com/v1/roles
```

```json response ListRolesResponse
{
  "roles": [
    {
      "name": "roles/administrator",
      "permissions": ["state.read", "principals.read", "events.read", "principals.revoke"],
      "principals": ["principals/google%3Aops%40example.org"]
    },
    {
      "name": "roles/viewer",
      "permissions": ["state.read"],
      "principals": ["principals/google%3Adev%40example.org", "principals/github-actions%3Aexample-org%2Fexample-repo"]
    }
  ]
}
```

### Errors

| Reason | When |
|---|---|
| `MISSING_PERMISSION` | the caller lacks `principals.read` |
| `MALFORMED_REQUEST` | `pageSize` is negative |
| `INVALID_PAGE_TOKEN` | the token is malformed |

### Notes

- **Order.** Roles are listed in configuration order, paged in memory over the configured list.
- A role's `permissions` are the permission names with `*` expanded, in canonical order (`state.read`, `principals.read`, `events.read`, `principals.revoke`). `principals` are references to the principals the role names.
- Roles are read through `principals.read`: they are principal data, so no separate permission exists.
- The route accepts the session cookie.

## events

### Name

`events/{event}`. The ID is the scaling ledger's sequence number, which strictly increases in recording order. An event records one thing the autoscaler or a worker did to a fleet. The ledger holds scaling events only; any other kind of event would be a different resource.

### Methods

| Method | Path | Permission |
|---|---|---|
| `GET` | `/v1/events` | `events.read` |

### Examples

List the history, newest first, two at a time:

```sh
curl -H "Authorization: Bearer $TOKEN" 'https://gang.example.com/v1/events?pageSize=2'
```

```json response ListEventsResponse
{
  "events": [
    {
      "name": "events/997",
      "createTime": "2026-01-14T09:20:04.000Z",
      "fleet": "fleets/x86-64-spot",
      "cause": "IDLE_TIMEOUT",
      "drainStarted": {
        "worker": "workers/i-0e1000000000000a-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b"
      }
    },
    {
      "name": "events/996",
      "createTime": "2026-01-14T09:19:58.000Z",
      "fleet": "fleets/x86-64-spot",
      "workerRegistered": {
        "worker": "workers/i-0e1000000000000a-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b"
      }
    }
  ],
  "nextPageToken": "AQA5OTY"
}
```

### Errors

| Reason | When |
|---|---|
| `MISSING_PERMISSION` | the caller lacks `events.read` |
| `MALFORMED_REQUEST` | `pageSize` is negative |
| `INVALID_PAGE_TOKEN` | the token is malformed |
| `DEPENDENCY_UNAVAILABLE` | the ledger cannot be reached |

### Notes

- **Order.** Events are listed by sequence number, descending, so a chain of pages walks backward in time. `nextPageToken` resumes after the last event of the page.
- **Live and history.** The history is this list. Live events come only from `GET /v1/state:watch`: its snapshot `events` and every delta's `events` are full `Event` resources with the same shape. There is no long poll, no `since` and no `before`. A client that holds the newest events of a stream and wants older ones lists from the newest, and stops at the first event it already holds, deduplicating by `name`.
- **Event.** `createTime` is when the event was recorded and `fleet` is the fleet it concerns. `cause` is present for an autoscaler decision and omitted for worker joins and leaves, early exits, and events recorded before causes existed. Its values are `DEMAND`, `IDLE_TIMEOUT`, `ORPHAN`, `DRAIN_FINISHED` and `MAX_DRAIN_WAIT`; a client tolerates a value it does not know. Beside the common fields exactly one member describes the change:

  | Member | Fields |
  |---|---|
  | `scaledOut` | `from`, `to`: the fleet's desired capacity before and after |
  | `drainStarted` | `worker`: the worker that began draining |
  | `drainCancelled` | `worker`: the worker whose drain was cancelled |
  | `terminated` | `instance`: the instance that was terminated |
  | `exitedEarly` | `instance`, `failure`: a worker failed soon after its launch, before it could serve; `failure` is its own failure record |
  | `workerRegistered` | `worker`: the worker that connected |
  | `workerDeregistered` | `worker`: the worker that disconnected |

  Local Mode reports `exitedEarly` when a worker exits within `relaunch_backoff` of its launch and its log has a `fail` record.

  A member a client does not know is ignored.
- The route accepts the session cookie.

## Errors

Every non-2xx answer on `/v1/**` is the same envelope, `google.rpc.Status` as in Google's AIP-193:

- `code` is the HTTP status and `status` is the `google.rpc.Code` name for it.
- `message` is generic and human readable. It never carries a proof, a signing request or an internal cause. A value that appears in the message also appears in `metadata`.
- `details` holds one `ErrorInfo` exactly: `reason` is the stable machine-readable cause, `domain` is the constant `gang`, and `metadata` carries the dynamic values behind the reason, keyed lowerCamelCase. A `BadRequest` detail is added when a request field is at fault: its `fieldViolations` name each field and say what is wrong.
- A client dispatches on `reason`, never on `message`. Reasons, metadata keys and detail types are only ever added. A client that meets a reason or detail type it does not know falls back to `status`.
- Every `401` carries `WWW-Authenticate: Bearer`.
- A caller that is revoked, or whose credential is refused, gets the envelope on every `/v1/**` route, including the open ones.
- The server never answers `405`; a wrong method is `404 ROUTE_NOT_FOUND`.
- `Retry-After` and a `RetryInfo` detail are sent only with `RATE_LIMITED`.

| reason | HTTP | status | metadata keys |
|---|---|---|---|
| `MALFORMED_REQUEST` | 400 | `INVALID_ARGUMENT` | — |
| `REQUEST_TOO_LARGE` | 400 | `INVALID_ARGUMENT` | — |
| `INVALID_PAGE_TOKEN` | 400 | `INVALID_ARGUMENT` | — |
| `UNKNOWN_PROVIDER` | 400 | `INVALID_ARGUMENT` | `provider` |
| `AUTHORIZATION_PENDING` | 400 | `FAILED_PRECONDITION` | — |
| `PROOF_REJECTED` | 401 | `UNAUTHENTICATED` | — |
| `CREDENTIALS_MISSING` | 401 | `UNAUTHENTICATED` | — |
| `CREDENTIALS_INVALID` | 401 | `UNAUTHENTICATED` | — |
| `POLICY_DENIED` | 403 | `PERMISSION_DENIED` | — |
| `PRINCIPAL_REVOKED` | 403 | `PERMISSION_DENIED` | — |
| `MISSING_PERMISSION` | 403 | `PERMISSION_DENIED` | `permission` |
| `ROUTE_NOT_FOUND` | 404 | `NOT_FOUND` | `method`, `path` |
| `CERTIFICATE_NOT_FOUND` | 404 | `NOT_FOUND` | `certificate` |
| `PRINCIPAL_NOT_FOUND` | 404 | `NOT_FOUND` | `principal` |
| `DEVICE_AUTHORIZATION_NOT_FOUND` | 404 | `NOT_FOUND` | `deviceAuthorization` |
| `RATE_LIMITED` | 429 | `RESOURCE_EXHAUSTED` | — |
| `DEPENDENCY_UNAVAILABLE` | 503 | `UNAVAILABLE` | — |
| `REQUEST_TIMED_OUT` | 504 | `DEADLINE_EXCEEDED` | — |
| `INTERNAL_ERROR` | 500 | `INTERNAL` | — |

What each reason means:

- `MALFORMED_REQUEST`: the body is not JSON, has an unknown field or a field of the wrong type, or a path ID, `pageSize` or `filter` is invalid.
- `REQUEST_TOO_LARGE`: the body exceeds the server's `max_request_size`.
- `INVALID_PAGE_TOKEN`: the page token is not one this server issued, or was issued for a different `filter`.
- `UNKNOWN_PROVIDER`: the body names an identity provider the server does not configure. It is a bad reference in the request, not a missing resource, so it is `400`.
- `AUTHORIZATION_PENDING`: the device authorization has not been approved yet.
- `PROOF_REJECTED`: the identity provider rejected the proof of identity.
- `CREDENTIALS_MISSING`: the route needs an identity and the request presents none; this includes an unknown path for an anonymous caller.
- `CREDENTIALS_INVALID`: a credential was presented and refused: expired, malformed, for another audience, or an unusable `Authorization` header.
- `POLICY_DENIED`: the issuance policy refuses the principal, for example a required claim does not match.
- `PRINCIPAL_REVOKED`: the caller's principal is revoked.
- `MISSING_PERMISSION`: the caller is authenticated and lacks the permission the route requires; the metadata names it.
- `ROUTE_NOT_FOUND`: no route matches the method and path, or the route is not mounted on this deployment.
- `CERTIFICATE_NOT_FOUND`, `PRINCIPAL_NOT_FOUND`: the named resource does not exist.
- `DEVICE_AUTHORIZATION_NOT_FOUND`: the device authorization is unknown, expired or already redeemed, or its secret is wrong; these cases are indistinguishable.
- `RATE_LIMITED`: a client address, or the server as a whole, sent more requests to an open route than `[server.rate_limit]` allows. The answer carries `Retry-After` and a `RetryInfo` detail; retry after that wait.
- `DEPENDENCY_UNAVAILABLE`: the ledger, an identity provider or the signer cannot be reached, including when the gate cannot check the caller (`503` on every `/v1/**` route); retrying later may succeed.
- `REQUEST_TIMED_OUT`: the request did not finish within `request_timeout`.
- `INTERNAL_ERROR`: the server failed; the cause is in its logs.

A request with a bad field:

```json error MALFORMED_REQUEST
{
  "error": {
    "code": 400,
    "message": "invalid request",
    "status": "INVALID_ARGUMENT",
    "details": [
      {
        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
        "reason": "MALFORMED_REQUEST",
        "domain": "gang"
      },
      {
        "@type": "type.googleapis.com/google.rpc.BadRequest",
        "fieldViolations": [
          {
            "field": "pageSize",
            "description": "must not be negative"
          }
        ]
      }
    ]
  }
}
```

A request with no credential; the answer carries `WWW-Authenticate: Bearer`:

```json error CREDENTIALS_MISSING
{
  "error": {
    "code": 401,
    "message": "authentication required",
    "status": "UNAUTHENTICATED",
    "details": [
      {
        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
        "reason": "CREDENTIALS_MISSING",
        "domain": "gang"
      }
    ]
  }
}
```

A revoked caller. It is the same answer on every `/v1/**` route:

```json error PRINCIPAL_REVOKED
{
  "error": {
    "code": 403,
    "message": "principal revoked",
    "status": "PERMISSION_DENIED",
    "details": [
      {
        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
        "reason": "PRINCIPAL_REVOKED",
        "domain": "gang"
      }
    ]
  }
}
```

An authenticated caller without the permission the route requires:

```json error MISSING_PERMISSION
{
  "error": {
    "code": 403,
    "message": "missing permission events.read",
    "status": "PERMISSION_DENIED",
    "details": [
      {
        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
        "reason": "MISSING_PERMISSION",
        "domain": "gang",
        "metadata": {
          "permission": "events.read"
        }
      }
    ]
  }
}
```

A caller that sends too many requests to an open route gets this answer. It carries `Retry-After: 3`:

```json error RATE_LIMITED
{
  "error": {
    "code": 429,
    "message": "too many requests",
    "status": "RESOURCE_EXHAUSTED",
    "details": [
      {
        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
        "reason": "RATE_LIMITED",
        "domain": "gang"
      },
      {
        "@type": "type.googleapis.com/google.rpc.RetryInfo",
        "retryDelay": "3s"
      }
    ]
  }
}
```

A path or method no route matches, for an authenticated caller:

```json error ROUTE_NOT_FOUND
{
  "error": {
    "code": 404,
    "message": "no such route",
    "status": "NOT_FOUND",
    "details": [
      {
        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
        "reason": "ROUTE_NOT_FOUND",
        "domain": "gang",
        "metadata": {
          "method": "GET",
          "path": "/v1/scaling-events"
        }
      }
    ]
  }
}
```

## Browser flows (/ui)

`/ui/**` is the browser surface: HTML, static assets and redirect flows, shipped in the server binary and unversioned. It is not API and has no schema contract beyond what the bundle itself needs. Answers under `/ui` are not the error envelope.

| Method | Path | Behaviour |
|---|---|---|
| `GET` | `/ui/login?next=` | Starts the sign-in: `302` to the identity provider's authorization URL and sets the sign-in cookie. `next` must be `/ui` or below; anything else is replaced by `/ui`. Mounted only when `[dashboard.login]` is configured. |
| `GET` | `/ui/callback` | The registered OAuth redirect URI. A dashboard sign-in sets the session cookie and redirects to `next`; a device approval sets no session or renewal cookie and redirects to `/ui/device/approved`. Both clear the sign-in cookie; failures redirect to `/ui/error?reason=denied\|failed\|unavailable`. Mounted like `/ui/login`. |
| `POST` | `/ui/renew` | Renews from the sealed renewal cookie; requires `Sec-Fetch-Site: same-origin` or `Origin` equal to the endpoint's origin and is rate limited. Empty answers: `204` sets a new session cookie and reseals the renewal cookie only if the token rotated, keeping its deadline; `401` keeps the cookie past `session_max_age`, but clears it for a missing, invalid or refused token or another principal; `403` clears it for a revoked or denied principal but not for a cross-site request; `429` is rate limited; `503` keeps the cookie when the provider is unavailable. Mounted like `/ui/login`. |
| `POST` | `/ui/logout` | Clears the session and renewal cookies: `204` when `Sec-Fetch-Site` is `same-origin` or `Origin` equals the endpoint's origin, otherwise `403`. |
| `POST` | `/ui/device` | Approves a device login via form field `userCode`. Requires the same-origin rule and is rate limited; cross-site requests get `403`. Unknown, expired or already approved codes get `303` to `/ui/device?error=unknown`. Otherwise sets the sign-in cookie and redirects with `303` to the provider for an offline sign-in. Mounted only with device login. |
| `GET` | `/ui/device` | The code-entry and approval page; it shows the request's server-observed address and age before approval. |
| `GET` | `/ui/device/approved` | The page shown after a successful device approval. |
| `GET` | `/ui/error?reason=` | The page that explains a failed sign-in. `reason` is `denied`, `failed` or `unavailable`; these are lowercase route parameters, not API enum values. |
| `GET` | `/ui/**` | The embedded dashboard bundle: hashed assets with long-lived caching, and `index.html` for every path without a file extension. |

- `GET /ui/login`, `GET /ui/callback`, `POST /ui/renew` and `POST /ui/device` are rate limited like the open API routes. When a client address, or the server as a whole, exceeds `[server.rate_limit]`, the answer is an empty `429` with `Retry-After` in whole seconds. It is not the error envelope.
- The session cookie is `__Host-gang_session` under an `https` endpoint and `gang_session` otherwise. It is `HttpOnly; SameSite=Strict; Path=/` and is `Secure` under `https`. The sign-in cookie is `__Host-gang_login` or `gang_login`, `SameSite=Lax` because the callback is a cross-site navigation. The renewal cookie is `__Host-gang_renewal` or `gang_renewal`, `HttpOnly; SameSite=Strict; Path=/` and `Secure` under `https`; its `Max-Age` is 400 days (the RFC 6265bis browser cap), while the sealed `expireTime` bounds renewal. Keeping it beyond that deadline lets the next sign-in re-check the token without consent. Its sealed value is not set over 4000 bytes. Only `GET /ui/login` and `POST /ui/renew` read the renewal cookie; the callback uses the renewal carried by its sign-in attempt. The session cookie is read only for `GET` and `HEAD` of the cookie routes of the API; when a bearer token and a cookie are both present, the token wins.
- The dashboard reads the API with the session cookie: `GET /v1/session`, `GET /v1/state`, `GET /v1/state:watch`, `GET /v1/principals`, `GET /v1/roles` and `GET /v1/events`. Its sign-in state comes from `GET /v1/session`.
- The shell and the static assets carry no data, so a refusal recorded for the caller does not stop them: a revoked or signed-out visitor still loads the page, and the API calls it makes answer with the envelope.
- With the dashboard disabled, `/ui/**` is handled like any unknown path.
