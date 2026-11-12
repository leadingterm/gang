# ADR-10: Roles grant named permissions on API routes

Status: Accepted. Replaces the single `administrators` list.

## Context
- An administrators list authorizes everything. The dashboard adds read routes of different sensitivity, and an operator needs a viewer who cannot revoke.
- Authorization must not weaken the gate (ADR-05) or spread across handlers.

## Decision
- **Permissions** are a closed enum with a bitset (`gang_types::Permission`): `state.read`, `principals.read`, `events.read`, `principals.revoke`. Which route needs which is in docs/API.md.
- **Roles are configuration.** `[[server.roles]]` lists `name`, `permissions` (or `"*"` for every present and future permission) and `principals` as `(provider, principal)`. No role ships by default, so an empty table authorizes nobody. Names are unique, permission names known, lists non-empty, providers known or the reserved `none`.
- **One evaluation function.** `Authorizer::allows(&Identity, Permission)` is built once at start into a map from identity to permission set. API handlers and dashboard handlers call it, and a 403 names the missing permission. The gate stays authentication only.
- **Reading roles** needs `principals.read` (roles are principal data); `principals.revoke` covers its inverse, restore.

## Consequences
- Roles are static configuration; new permissions arrive with their routes.

## Rejected alternatives
- **Keeping `administrators` beside a roles table:** two tables once a second role exists.
- **A permission hierarchy:** nothing needs one.
