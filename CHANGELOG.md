# Changelog

All notable changes to mycel should be documented in this file.

This project follows the spirit of [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and Go module semantic versioning. Before `v1.0.0`, daemon behavior, CLI flags, and internal implementation details may still evolve, but user/operator-impacting and compatibility-affecting changes should be called out clearly.

## [Unreleased]

## [v0.9.0] - 2026-08-31

### Added

- First public-release baseline for the MycelDB daemon and CLI.
- Open-source project documentation: README links, contributing guide, security policy, code of conduct, changelog, pull request template, issue templates, and gitleaks false-positive baseline.
- Operator-facing daemon/CLI coverage for spaces, domains, graph/query workflows, semantic/inference setup, automation, activity, backups, and raft/cluster reliability.

### Changed

- Documented daemon-only repository boundaries and SDK/API consumption guidance.
- Documented release validation expectations for daemon-only checks, public Go surface checks, docs checks, tests, builds, and cluster-sensitive gates.

## Release notes policy

For each release, add a dated section such as:

```md
## [v0.9.0] - YYYY-MM-DD

### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security
```

Include notes for daemon behavior, CLI flags, configuration, storage formats, raft/clustering behavior, backup/restore behavior, authentication/authorization, public protobuf/API changes, migration requirements, and matching `mycel-api`/SDK versions.
