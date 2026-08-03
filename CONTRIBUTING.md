# Contributing to MycelDB

Thank you for contributing to MycelDB. This guide describes the expectations for
issues, pull requests, tests, documentation, licensing, and AI-assisted work.

MycelDB is preparing for an open-source release under the Apache License 2.0.
The guidance below follows common practices used by Apache-licensed projects:
clear issue discussion, small reviewable pull requests, contributor ownership of
submitted work, explicit licensing expectations, and reproducible validation.

## Code of conduct

Be respectful, constructive, and patient. Assume good intent, explain tradeoffs,
and keep technical disagreement focused on the code, docs, and user impact.
Maintainers may ask for revisions, split large changes, or close work that does
not fit the project direction.

## Licensing and contribution rights

MycelDB is intended to be released under the Apache License, Version 2.0. By
submitting a contribution, you agree that your contribution may be distributed
under the same license.

Only submit work that you have the right to contribute. Do not copy code,
configuration, tests, documentation, model output, or generated artifacts from
sources with incompatible licenses. If your change adapts prior work, identify
the source and license in the pull request.

If the project later adopts a CLA, DCO, or additional release-time provenance
process, maintainers will document it before requiring it for new contributions.

## Before starting work

For small fixes, opening a pull request directly is fine. For larger changes,
please open or comment on an issue first so maintainers can discuss scope,
approach, compatibility, and validation expectations.

Open an issue first for changes that affect:

- public APIs, protobuf contracts, SDK behavior, or CLI behavior;
- raft ownership, clustering, recovery, consistency, or backup/restore safety;
- storage formats, import/export formats, or migration behavior;
- security, authentication, authorization, or secret handling;
- broad package moves or documentation information architecture.

## Development setup

Use the repository Makefile for common workflows. Protobuf definitions live in
`github.com/myceldb/mycel-api`; daemon stubs are generated locally under
`internal/gen/`.

Common commands:

```sh
make build
make test
make docs-check
git diff --check
```

For docs-only changes, run:

```sh
make docs-check
git diff --check
```

For raft/clustering-sensitive changes, also run or discuss the relevant targeted
gates:

```sh
make test-phase-d
make test-phase-e
make test-phase-f
make test-phase-g
```

Destructive Compose/K3s tests reset local resources and must only be run when
you intentionally want that validation:

```sh
make test-compose-cluster
make test-k3s-cluster
make test-compose-user-backup-restore
```

## Repository guidance for agents and contributors

Read [AGENTS.md](AGENTS.md) before making changes. It captures project-specific
architecture and safety rules for both humans and AI coding agents.

Key expectations include:

- use the term **subsystem** for internal domain areas;
- keep daemon API adapters under `internal/daemon/api`;
- keep service implementations under subsystem packages;
- preserve fail-closed raft behavior and strong/read-index read consistency;
- keep backup/restore and divergence workflows explicit and operator-selected;
- avoid hidden destructive behavior.

## Generated code and artifacts

Do not commit generated artifacts unless the change explicitly requires it and
maintainers agree.

In particular:

- do not commit generated ANTLR parser output;
- do not commit generated public SDK/API code unless explicitly approved;
- do not hand-edit generated protobuf files;
- update source protobufs, grammar files, or generation scripts instead.

After running generators, inspect the diff carefully and remove unintended
artifacts before committing.

## Pull request expectations

Keep pull requests small, focused, and reviewable. A good PR should include:

- a clear summary of the user/operator/developer problem being solved;
- a concise explanation of the approach;
- tests or a reason tests are not applicable;
- documentation updates when behavior, CLI usage, operations, or APIs change;
- notes about compatibility, migrations, or follow-up work;
- exact commands run and their results.

Avoid mixing unrelated refactors with behavior changes. If a refactor is needed,
prefer a separate preparatory PR.

## Testing expectations

Behavior changes should include tests close to the affected subsystem. Prefer
unit and targeted integration tests before broad destructive gates.

Use the narrowest meaningful validation first, then broaden as risk increases.
For example:

- docs-only: `make docs-check` and `git diff --check`;
- ordinary Go changes: affected package tests, then `make test`;
- raft/cluster changes: targeted phase tests plus release-gate discussion;
- backup/restore changes: archive/unit tests plus explicit Compose validation
  when destructive testing is appropriate.

If you cannot run a relevant test, say so in the PR and explain why.

## Documentation expectations

Documentation is organized by intent:

- `docs/README.md` — documentation entrypoint;
- `docs/design/` — current architecture and subsystem design;
- `docs/operations/` — operator procedures, CLI usage, recovery, validation;
- `docs/implementation/` — archival/release-grouped implementation plans.

Operator-facing recovery docs belong under `docs/operations/procedures/`.
Implementation plans are useful context, but they are not current operator
runbooks.

When changing docs, run:

```sh
make docs-check
```

## Security and sensitive information

Do not include secrets, credentials, tokens, private keys, production data,
private customer/user data, or confidential infrastructure details in issues,
PRs, tests, fixtures, logs, screenshots, or AI prompts.

If you believe you found a vulnerability, do not open a public issue with
exploit details. Use the private security reporting channel published with the
open-source release, or contact the maintainers privately until that channel is
available.

## AI-assisted contributions

AI tools are allowed, but contributors remain responsible for everything they
submit. Before opening a PR, review any AI-assisted changes for correctness,
security, licensing, maintainability, and test coverage.

Do not paste secrets, credentials, private user data, proprietary third-party
code, or confidential operational details into AI tools. AI-assisted changes
must follow the same project standards as any other contribution, including
[AGENTS.md](AGENTS.md), applicable tests, documentation checks, and licensing
requirements.

Recommended practice when using AI:

- provide the agent with project guidance from `AGENTS.md`;
- ask for small, reviewable diffs;
- inspect every changed line yourself;
- run the same checks you would run for hand-written changes;
- disclose any uncertainty, generated provenance concerns, or unverified output
  in the PR description.

## Review and merge

Maintainers may request changes for correctness, maintainability, tests,
documentation, compatibility, safety, or project scope. Approval of a PR does
not guarantee immediate merge; maintainers may batch or sequence changes around
release, migration, or operational risk.

Thank you for helping make MycelDB reliable, understandable, and safe to
operate.
