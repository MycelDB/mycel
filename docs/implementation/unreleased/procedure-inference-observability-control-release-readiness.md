# Procedure inference observability and control release readiness

Status: merged to `develop`; unreleased.

This note records compatibility, rollout, and release-order requirements for the
procedure inference observability and control work. It does not authorize a
release, image publication, destructive test, live-provider call, or issue
closure.

## Merged implementation baseline

- Source API contract: `MycelDB/mycel-api` PR #43, merge
  `f1078abb9b4614cf36801df98505bf53183f669f`.
- Go bindings: `MycelDB/mycel-go-sdk` PR #32, merge
  `52d4e93c882bbc13c80480e0cfd31e38ea4c18e9`.
- Rust bindings: `MycelDB/mycel-rust-sdk` PR #31, merge
  `205b83ceda592d9ed586d2f0b97c75a712425644`.
- Daemon and CLI: `MycelDB/mycel` PR #252, merge
  `af03ce57bd7c9472629de49ba55726085a1d60fb`.
- Independent exact-working-tree daemon and generated-binding reviews: accepted.

The protobuf additions are wire-compatible. Existing automation RPCs remain
available. New clients must not assume the new RPCs exist until the target daemon
version advertises them successfully.

## Storage and cluster rollout

Attempt ledgers, procedure-control fences/barriers, committed authorization
revisions, run references, change sequences, and retention watermarks are
Raft/snapshot-owned state. Provider calls remain outside graph transactions,
Raft apply, and publication-critical sections.

Roll a cluster completely before using procedure enable/disable or relying on
the new attempt feed. New mutation record kinds fail closed on replicas that do
not understand them; mixed-version operation is not an authorization bypass.
Do not force, rewrite, or delete ledger/control files during upgrade or rollback.

Before rollout:

1. take the normal supported backup and verify its manifest;
2. record current daemon/API/SDK versions and cluster membership;
3. verify every node can be upgraded to the same daemon build;
4. keep procedure inference disabled where an incomplete mixed-version rollout
   would otherwise create new work.

After rollout, verify procedure-control acknowledgements, attempt list/change
watermarks, authorization revisions, and snapshot/restore health before enabling
provider-backed procedures.

A downgrade must use the supported backup/restore procedure and a daemon version
that understands every retained mutation and snapshot record. Do not assume an
older daemon can safely consume state written by this feature.

## Capacity and polling operations

The initial per-space hard limits are 10,000 materialized attempts and 50,000
changes, 64 MiB of materialized attempt state, and 64 MiB of serialized change
history. Retention targets 8,000 attempts, 20,000 changes, 30 days, 48 MiB of
state, and 48 MiB of change history. Each attempt is bounded to 64 KiB encoded
and persisted evidence strings are bounded to 1,024 bytes. Active, ambiguous,
and run-referenced attempts remain
protected and can therefore drive a space to fail-closed capacity exhaustion.

Monitor lifecycle, capacity-rejection, pruning, history-byte, and gap-advance
diagnostics through the bounded daemon structured-log records documented in the
runbook. Process-local counters are diagnostic; durable high-water and
earliest-retained sequences are authoritative. Pollers persist opaque cursors,
deduplicate immutable event IDs, and resnapshot on an explicit history/state
gap. See [Automation CLI operations](../../operations/cli/automation.md).

## Release order

1. Tag and publish `mycel-api` from the accepted merged contract.
2. Regenerate and release the Go and Rust SDKs from that exact API tag.
3. Update `.release/mycel-api-ref` to the released compatible API tag and verify
   daemon generation is clean.
4. Re-run daemon tests, build, docs checks, public-surface checks, and the
   separately approved reliability gates against the exact candidate.
5. Publish daemon artifacts only after explicit promotion approval.

Until a new API tag is explicitly approved and published,
`.release/mycel-api-ref` pins the exact merged API commit above so daemon
release-generation cannot silently fall back to the older `v0.20.1` contract.
Replace the commit pin with the released API tag during the approved release
sequence.

## Remaining acceptance work

- complete non-destructive SGT1b gap/preflight inspection;
- obtain fresh approval before emulator-backed destructive SGT1b execution;
- independently review sanitized SGT1b evidence;
- update mycel#239 and mycel-api#37 from accepted evidence;
- obtain separate approval for any SGT9 live-provider execution;
- request explicit approval before closing issues or promoting a release.
