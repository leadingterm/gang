# ADR-57: Requestable action capabilities are an explicit fleet policy

## Status
Accepted; supersedes the action-influenced network policy in ADR-16.

## Context
Actions request platform properties, but security capabilities must be granted by the operator rather than inferred from a worker's sandbox. Observed clients use ISA and resource properties; rejecting unrecognized properties must preserve those existing requests.

## Decision
- Each fleet's `[fleets.actions]` policy has grants defaulting to `[]`; network is the first typed requestable capability.
- One `FleetCatalog` owns capability grants, existing exact selectors and minimum-resource request ceilings. Unknown platform properties are rejected except the harmless `supports-workers` and `gang_action_environment` properties; persistent-worker actions execute one-shot, and the latter identifies the action environment for caching. Admission precedes historical learning.
- Physical worker totals remain distinct from per-action ceilings. Native matching and retries honor the same ceilings and admitted capability constraints, without pinning an action to a single fleet.

## Consequences
- Refusals identify the failing property and relevant fleet names. A future capability requires actual enforcement, not just a declaration.
- Workers advertise physical capacity independently of per-action limits, allowing multiple eligible actions to use the available total.

## Rejected alternatives
- Silently ignoring unknown keys can turn an unsupported security request into an unprotected action.
- A parallel capability registry duplicates fleet policy and can disagree with placement.
- Advertising reduced worker totals confuses individual action ceilings with aggregate capacity.
