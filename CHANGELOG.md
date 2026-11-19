# Changelog

## Unreleased

### Changed

- Partition the Linux x86-64 and end-to-end CI suites, start ARM end-to-end checks after the bundle build, and share bounded native GitHub cache snapshots with baseline-aware PR deltas.
- Publish primary-region AMIs before independently completing the regional catalog and retirement.
- Require successful main push CI for the exact release commit; never reuse PR artifacts or caches.

## 0.3.1

### Added

- Show cache transfer bytes by build and across the farm, bounded-store usage and evictions, and the latest available S3 bucket measurement.

### Fixed

- Include follower-served cache lookups and hits in the dashboard's farm-wide history.
