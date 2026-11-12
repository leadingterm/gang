# ADR-15: Provisioner fleet options are typed and validated at parse time

Status: Accepted

## Context
- Fleets need provisioner-specific selection such as `capacity` and instance class, but those concepts do not exist on every platform (no spot on Slurm or local).

## Decision
- **Typed struct per provisioner.** Each provisioner declares its options with `#[derive(FleetOptions)]` (in `gang-macros`). `ProvisionerPort` has `type FleetOptions`, and methods take the borrowed `Fleet<'a, O>`.
- **Validation is serde plus the type,** at `GangConfig::from_str`: unknown keys, bad values and missing required keys fail naming the fleet (`ConfigError::FleetOptions`). `Plugin::build` never sees fleets.
  `FleetOptions::validate_fleets` checks rules that span the provisioner's fleets.
- **Options are selectable scalars** registered as exact worker properties, except launch-only options (`#[option(launch)]`), which shape how instances are launched, register no property and may be any `Deserialize` type (ADR-23 uses one).
- **One vocabulary source.** `FleetConfig.properties` is built once from the typed struct; `gang options [<provisioner>]` and SPEC §3.1 derive from it. Reserved keys (`fleet`, `ISA`, `sandbox`, `cpu_count`, `memory_kb`, `disk_kb`, `network`) cannot be option names (compile error).

## Consequences
- AWS declares `capacity` and `machine_class`; Local declares none, so every option is rejected.
- Typos fail at parse, not at runtime.

## Rejected alternatives
- **Open string-keyed fleet labels owned by Gang:** typos silently stop selecting.
- **Gang-wide typed `capacity` and `machine_class`:** dead concepts on Slurm and local.
- **Validation in `Plugin::build`:** fleets are not visible there, and the error would arrive after parse.
