### Added

- Report merge conflicts on open pull requests from a trusted main-branch workflow, without executing pull request code.
- Generate release changelogs from retained per-change fragments, preserving the historical archive and including the generated changelog with release assets.

### Changed

- Pull requests run a fast preflight with portable lint, a build, generated lint tests and affected Rust unit tests. The merge queue fully validates the combined tree before merging; the required check names represent the appropriate event's validation.
- Releases use qualified merge-queue CI evidence, and main-branch cache promotion reuses validated queue working sets without rerunning the full suite.
