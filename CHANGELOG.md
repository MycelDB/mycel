# Changelog

All notable changes to mycel should be documented in this file.

This project follows the spirit of [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and Go module semantic versioning. Before `v1.0.0`, daemon behavior, CLI flags, and internal implementation details may still evolve, but user/operator-impacting and compatibility-affecting changes should be called out clearly.

## [Unreleased]

## [v0.11.1] - 2026-09-09

### Added

- Added third-party notices and license inventory for release compliance.

### Removed

- Removed deprecated root CLI commands: `mycel init`, `mycel acl`, and `mycel accounting`.

### Fixed

- Fixed raft backend transport churn by reusing pooled per-peer gRPC connections and bounding raft send timeouts.
- Made compose cluster release gates self-contained under `tests/compose/cluster`.
- Fixed K3s PVC replacement recovery so a same-node-ID raft member with empty persistent storage catches up instead of remaining stale after rejoin.

## [v0.11.0] - 2026-09-07

### Added

- Added lexical search v1: per-space/domain BM25 indexes, `SearchService` gRPC endpoints, freshness diagnostics, leader-owned indexing/forwarding seams, CLI search/status/rebuild commands, and operator documentation.

### Fixed

- Reduced raft-heavy release gate flakes by retrying transient lossless graph notification delivery failures and serializing raft-sensitive package phases by default.

## [v0.10.0] - 2026-09-04

### Added

- Added S3-backed blob payload storage for new blob uploads, with local storage remaining the default and existing local blobs remaining local (#6).
- Added REPL line editing and command history, with sensitive command filtering (#12).
- Added REPL paste-friendly command handling for semicolon-terminated commands, continuation lines, and pasted multi-command blocks (#5).

### Changed

- Moved product-facing query roadmap pages out of repository docs and replaced them with public-manual pointers (#14).
- Improved standalone daemon health semantics so a ready standalone node is not marked unhealthy solely because cluster admission does not apply (#17).

### Fixed

- Fixed `space` subcommands when run inside the REPL (#7).
- Fixed a backup scheduler retry-after state race after transient backup failures (#9).

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
