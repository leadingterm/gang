# Changelog

## 0.3.2

### Fixed

- Restore remote execution and cache to one client endpoint on port 443; regenerate `gang bazelrc` after upgrading.
- Grant controller lifecycle Describe calls at the scope required by Auto Scaling while retaining group-scoped lifecycle completion.
- Preserve resource-kill outcomes when output upload fails so escalation remains effective.
- Use sustained worker disk pressure instead of learned reservations as the default disk-kill trigger, requeue pressure kills with larger reservations, and floor nonzero disk reservations at 1 GiB by default.

## 0.3.1

### Added

- Show cache transfer bytes by build and across the farm, bounded-store usage and evictions, and the latest available S3 bucket measurement.

### Fixed

- Include follower-served cache lookups and hits in the dashboard's farm-wide history.
