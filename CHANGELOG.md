# Changelog

## Unreleased

### Changed

- Make pull-request CI exclusively x86-64: partition the full test and end-to-end suites, build the bundle in parallel, and keep portable lint free of Darwin compilation; validate ARM, cross and macOS on default-branch pushes and version tags.
- Publish primary-region AMIs before independently completing the regional catalog and retirement.
- Require successful release validation from same-repository main push CI for the exact release commit; never reuse PR artifacts or caches. Prioritize one bounded x86 Bazel baseline and Nix store with small PR deltas; disable optional architecture caches.

## 0.3.1

### Added

- Show cache transfer bytes by build and across the farm, bounded-store usage and evictions, and the latest available S3 bucket measurement.

### Fixed

- Include follower-served cache lookups and hits in the dashboard's farm-wide history.
