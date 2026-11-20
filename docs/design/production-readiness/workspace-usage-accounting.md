# Workspace usage accounting for billing and quota

Status: draft  
Last updated: 2026-11-19

## Production requirement

CommonFolio needs authoritative current workspace usage values to bill users and
throttle database use. These values must be more stable, auditable, and precise
than ordinary operational metrics.

This work is mandatory for paid or quota-limited CommonFolio production.
Approximate disk metrics are not sufficient for billing or hard throttling.

## Ownership boundary

Mycel owns measurement. CommonFolio owns policy.

Mycel should expose current authoritative usage values. CommonFolio should own:

- plan limits;
- pricing;
- subscription state;
- invoices;
- warning thresholds;
- grace periods;
- hard or soft throttling decisions;
- customer-facing billing behavior.

Mycel should not encode product-plan policy such as "Pro plan has 10 GB".
Instead, Mycel should report values such as "workspace X uses N logical bytes as
of committed revision R".

## Mandatory first-cut capabilities

Before paid/quota-limited production, Mycel should provide:

- a current usage API by workspace or Mycel space;
- stable logical units for:
  - graph logical bytes;
  - node count;
  - edge count;
  - attachment count;
  - attachment logical bytes;
  - pending-delete attachment logical bytes;
- freshness and authority metadata:
  - measured time;
  - committed graph revision or equivalent state marker;
  - exact versus approximate;
  - billing/quota-safe versus diagnostic-only;
- replay/restore-safe counters for quota-relevant usage;
- tests for concurrent writes, deletes, restore/replay, pending-delete handling,
  and counter reconciliation.

## Authority classes

Usage fields should be explicitly classified so CommonFolio knows which fields
can drive billing or throttling.

Suggested classes:

- `RAFT_COMMITTED_EXACT`: deterministic logical counters derived from committed
  state and safe for quota decisions.
- `STATE_MACHINE_EXACT`: exact local materialized values that are persisted in
  snapshots/checkpoints and recoverable from the committed log.
- `OBSERVED_APPROXIMATE`: operational observations such as physical disk usage,
  object-store physical bytes, or index file bytes.
- `REBUILDABLE_OPERATIONAL`: derived state such as lexical/semantic index size
  and freshness, useful for diagnostics but not inherently billable.

Hard throttling should use only fields explicitly documented as quota-safe.

## Launch gate

CommonFolio production with billing or throttling should require:

1. authoritative logical usage API available for each billable workspace;
2. documented units and field semantics;
3. clear CommonFolio/Mycel ownership boundary;
4. restore/replay-safe counters or a proven reconciliation path;
5. freshness/authority metadata in API responses;
6. integration test proving CommonFolio can consume usage without relying on
   operational disk metrics.

## Follow-up design work

- Decide whether CommonFolio `workspace_id` maps one-to-one to Mycel `space_id`
  or requires a separate mapping layer.
- Decide how active versus pending-delete attachments affect quota.
- Decide whether semantic vectors count as graph-derived logical usage,
  operational index usage, or a separate quota dimension.
- Define API response shapes and SDK helpers.
- Define how throttling decisions are communicated back to Mycel requests, if at
  all, without moving policy ownership into Mycel.
