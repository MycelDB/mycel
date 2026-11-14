# Authoritative graph-change outbox

## Problem

A graph transaction can commit while the node-local graph-change notification
sink fails. The current notification history is bounded, local JSONL state, so a
restart can leave semantic and lexical consumers without a replayable handoff.
Derived projections must not be allowed to report successful delivery when that
history is missing.

## Contract

- Graph truth and the graph-change handoff are committed in the source graph
  partition's Raft state machine.
- Derived consumers are asynchronous and never block graph commits.
- Consumer delivery is at-least-once and idempotent by graph scope and revision.
- Projection artifacts remain local and rebuildable.
- Missing retained history produces explicit `rebuild_required` state.
- Journal entries contain bounded identifiers and revision metadata, not graph
  content or sensitive diagnostics.

## Phase 1: authoritative journal

Each graph Raft partition maintains a compact append-only journal under
`graph-change-outbox/partition-XXXXXXXX.jsonl`. Every applied graph commit
appends an entry after graph storage succeeds and before the applied-command
marker is recorded. Reapplication is idempotent by space, domain, and graph
revision.

The journal is reconstructed on Raft replay and included in graph Raft
snapshots. This makes the journal a replicated state-machine artifact rather
than an authority based on a node-local notification file. The current
notification module remains available for compatibility but is not the
authoritative source.

The bounded notification module remains available for live wake-ups and
compatibility, but it is no longer used as the recovery authority in clustered
mode.

## Phase 2: durable consumers

Semantic maintenance now replays compact journal entries into its replicated
maintenance manager. Its per-domain checkpoint is the durable cursor, dirty
append and checkpoint advancement are idempotent, and journal gaps persist
`rebuild_required` state.

Lexical indexing replays the journal while reading revision-pinned graph truth
for node materialization. Its local cursor advances only after artifact writes;
common projection publication occurs once the replay reaches the final
frontier. A journal gap fences the lexical projection as `rebuild_required`
and never acknowledges the missing history.

The Compose graph-change handoff-loss scenario is an acceptance test and must
pass with a digest-pinned image containing Phase 2.

## Phase 3: durable cursors and retention

Semantic and lexical consumers acknowledge the highest successfully materialized
revision through `graph.change_journal_cursor.v1` in the source-partition Raft
log. Cursor application is monotonic and idempotent, and cursor records are
included in graph Raft snapshots. Acknowledgement is issued only after the
consumer's durable checkpoint or final lexical publication succeeds.

The per-partition journal targets a bounded 4096-entry retained window per
scope. Compaction is conservative: it runs only when both registered derived
consumers have durable cursors covering the compaction point. The fence revision
itself is retained, so a restarted consumer can distinguish a resumable window
from a retention gap. Missing coverage prevents compaction rather than deleting
history optimistically. A retention gap continues to fail closed as
`rebuild_required`.

## Recovery and retention

Journal retention and compaction are fenced by consumer cursors and Raft
snapshot metadata. A snapshot carries the retained journal entries and durable
consumer cursors. A snapshot must either retain the required journal window or
carry an explicit rebuild fence for consumers that cannot resume.

## Qualification

Unit coverage must include sink failure, journal restart, duplicate Raft apply,
and snapshot restore. The Compose graph-change handoff-loss scenario must pass
only after Phase 2 wires derived consumers to this journal.
