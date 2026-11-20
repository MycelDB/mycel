# Upgrade and rollback lifecycle

Status: draft  
Last updated: 2026-11-19

## Production requirement

CommonFolio will need to upgrade Mycel safely. Even with backups, operators need
a predictable upgrade path, preflight checks, post-upgrade validation, and clear
rollback rules.

This is mandatory for production because a bad upgrade without a tested rollback
path can become an outage or data-loss incident.

## Mandatory first-cut capabilities

### Pre-upgrade checks

Before an upgrade begins, operators should verify:

- cluster health is good;
- Raft quorum is healthy;
- no partition is under-replicated or unavailable;
- a recent backup exists;
- a recent restore verification or restore drill exists;
- daemon/API/SDK versions are compatible;
- deployment manifests reference the intended image/tag/digest;
- release-gate or equivalent validation has passed for the candidate.

### Rolling upgrade procedure

The first production procedure can be manual or semi-automated, but it must
state:

- pod upgrade order;
- readiness gates before continuing;
- what to do if a pod fails to become ready;
- what work is allowed during upgrade;
- what operator decision points exist.

### Post-upgrade validation

After upgrade, operators should validate:

- admin login/auth;
- client auth;
- graph read/write smoke tests;
- lexical search smoke tests;
- semantic search/projection readiness smoke tests if semantic search is used;
- workspace usage accounting smoke tests;
- backup still works;
- no unexpected downgrade boundary has been crossed.

### Rollback decision tree

The runbook must distinguish:

- binary rollback, when older daemons can safely read the current state;
- restore-from-backup, when storage formats, Raft records, projections, or other
  state have crossed a downgrade boundary;
- roll-forward fix, when rollback would be riskier than deploying a corrected
  patch.

## Launch gate

CommonFolio production should require:

1. a documented pre-upgrade checklist;
2. a documented rolling upgrade procedure;
3. a documented post-upgrade validation checklist;
4. a rollback decision tree;
5. at least one rehearsed upgrade and rollback/restore drill in staging or Mycel
   Lab for the release line.

## Follow-up design work

- Define machine-readable compatibility gates for deployment tooling.
- Define downgrade-boundary metadata in release notes or compatibility manifests.
- Integrate backup freshness and restore verification into pre-upgrade gates.
- Add Mycel Lab suites that exercise upgrade, smoke traffic, backup, restore,
  and rollback decisions around release candidates.
