# Advanced storage phase 1: domain-scoped graph stores

Status: in progress on the `advanced_storage` branch.

## Branching

Advanced storage implementation PRs target the long-lived `advanced_storage` branch, which is branched from `develop`. The branch is expected to use fresh data; no migration from the older space-level graph store layout is provided in this phase.

## Layout

Before this phase, graph storage was physically space-scoped:

```text
<data_dir>/graphs/<space_id>/
  manifest.mycel
  segments/
    nodes-000001.kseg
    edges-000001.kseg
    txns-000001.kseg
```

Phase 1 changes the physical graph store unit to a domain within a space:

```text
<data_dir>/graphs/<space_id>/domains/<domain_id>/
  manifest.mycel
  segments/
    nodes-000001.kseg
    edges-000001.kseg
    txns-000001.kseg
```

The `.kseg` segment format is unchanged. A `LocalStore` now represents one domain graph store path rather than one whole-space graph store path.

## Layering intent

The graph service owns domain graph management:

- space/domain scoping
- transaction routing
- graph validation
- raft integration
- graph change events
- logical cache lifecycle

The graph storage package owns durable storage mechanics:

- manifests
- `.kseg` segments
- record append/scan
- checksums/encryption
- storage transactions

Persistent indexes, compaction, checkpoints, and bounded cache eviction are out of scope for this phase.

## Runtime assumptions

- Writes are domain-local: node and edge domain IDs must match the transaction domain ID.
- Domain revisions are independent; a transaction in one domain does not advance another domain's graph revision.
- Space-scoped revision helpers are retained only as compatibility aggregations over known domain stores.
- Opening one domain store should not rebuild graph data for another domain in the same space.
