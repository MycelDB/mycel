# SGR12 Semantic Generation Rules End-to-End Validation Report

## Status

Completed local validation for the semantic generation rules tranche set.

This report records the non-destructive validation run after SGR0-SGR11 API,
runtime, CLI, Console, SDK compatibility, and documentation changes. Destructive
Compose/K3s validation remains explicit operator tooling and was not run.

## Coverage evidence

The normal Go test suite covers the rule-native semantic subsystem and adjacent
integration points, including:

- rule create/update/delete/list API and CLI paths;
- node-type selector validation;
- bounded GQL selector validation;
- dirty event analysis into rule/binding work items;
- dirty work coalescing by rule, embedding binding, and target;
- source hash idempotency and empty-source tombstones;
- multi-binding generation paths;
- Intelligence Access denial before provider calls;
- inference usage attribution by semantic rule and embedding binding;
- vector tombstones and latest-live physical search indexes;
- physical search-index rebuild and fail-closed search behavior;
- raft/WAL semantic service replication-sensitive package coverage.

Console and Rust SDK checks validate downstream generated-proto compatibility and
rule-native Console authoring/maintenance/search surfaces.

## Commands run

From `mycel`:

```sh
make test
make docs-check
git diff --check
make test-phase-d
make test-phase-e
make test-phase-f
make test-phase-g
```

From `mycel-console`:

```sh
npm test -- --runInBand
MYCEL_API_ROOT="$(cd ../mycel-api && pwd)" npm run build
cd src-tauri
MYCEL_API_ROOT="$(cd ../../mycel-api && pwd)" cargo check -q
```

From `mycel-rust-sdk`:

```sh
MYCEL_API_ROOT="$(cd ../mycel-api && pwd)" cargo check -q
```

## Results

All commands above passed.

Notes:

- `npm run build` reported the existing Vite large-chunk warning only.
- Phase targets are non-destructive Go validation gates. Destructive/system
  validation targets such as Compose/K3s cluster tests were not run.
- No generated public SDK/API artifacts were committed as part of this report.

## Remaining follow-ups

- Commit the coordinated `mycel`, `mycel-console`, and `mycel-rust-sdk` changes
  when ready.
- Run destructive Compose/K3s validation only when explicitly requested by an
  operator.
