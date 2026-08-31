# Changelog

All notable changes to mycel should be documented in this file.

This project follows the spirit of [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and Go module semantic versioning. Before `v1.0.0`, daemon behavior, CLI flags, and internal implementation details may still evolve, but user/operator-impacting and compatibility-affecting changes should be called out clearly.

## [Unreleased]

### Added

- Open-source project documentation: security policy, code of conduct, changelog, pull request template, and issue templates.

## Release notes policy

For each release, add a dated section such as:

```md
## [v0.8.0] - YYYY-MM-DD

### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security
```

Include notes for daemon behavior, CLI flags, configuration, storage formats, raft/clustering behavior, backup/restore behavior, authentication/authorization, public protobuf/API changes, migration requirements, and matching `mycel-api`/SDK versions.
