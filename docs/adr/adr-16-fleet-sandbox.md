# ADR-16: A fleet fixes the sandbox of its workers

Status: Accepted

## Context
- Isolation differs per environment: `bwrap` on Linux, `noop` on macOS and where a pod already isolates (ADR-17).
- NativeLink matches platform properties but cannot match set membership.

## Decision
- A fleet names exactly one sandbox, a key of a `[sandbox.<plugin>]` section. It is the sandbox for every action the fleet's workers run, and it is advertised as the `sandbox` property.
- The controller writes the choice into the sandbox file as `SandboxFile.selected`; `gang exec` changes nothing else. `permit_network_property` stays the only action-influenced security knob.
- An action chooses among operator-defined fleets and nothing else: the sandbox is placement only. A need for two sandboxes is two fleets.

## Consequences
- A fleet naming an unconfigured sandbox fails at parse (`FleetSandboxNotConfigured`).
- Serving several sandboxes from one fleet (`property_modifier` `Replace`) remains possible later without a contract break.

## Rejected alternatives
- **`allowed_sandboxes` with per-action selection:** NativeLink cannot match set membership, so placement would be wrong or need a rewrite layer.
