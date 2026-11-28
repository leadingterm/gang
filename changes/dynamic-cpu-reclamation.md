### Added

- Add Borg/Autopilot-style dynamic CPU reclamation driven by live worker CPU utilization, CPU PSI hysteresis, causal heartbeat watermarks (`assign_seq` / `sample_seq` / `observed_assign_seq`), guaranteed-capacity protection, and a local worker `CpuPressure` decline safety valve.
- Add configurable `[actions.cpu]` tuning in `gang.toml` and optional `gang_settings.cpu` in the OpenTofu `deployment` module, with live worker load ranking and raw-measurement in-flight learning for unseen heavy targets while preserving per-target isolation.

### Changed

- Measure container working-set memory from cgroup `memory.stat` (`anon + active_file`) instead of `memory.current` so reclaimable page cache does not inflate reservations.

### Removed

- Remove static `cpu_oversubscription` from `gang.toml`, the scheduler, and the OpenTofu `deployment` module (`gang_settings.cpu_oversubscription` is rejected).

### Upgrade notes

- Remove `cpu_oversubscription` from `gang.toml` and `gang_settings` in OpenTofu roots before upgrading; physical worker cores now represent 1x guaranteed capacity and spare CPU is reclaimed dynamically under `[actions.cpu]`.
