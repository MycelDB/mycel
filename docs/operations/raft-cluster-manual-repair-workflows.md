# Raft Cluster Manual Repair Workflows

Phase G repair workflows are manual and evidence-driven. MycelDB does not automatically merge, delete, overwrite, rebalance, or repair divergent PVCs during daemon startup.

Use these workflows only after writes are stopped and every PVC has been snapshotted.

## Safety rules

- Keep traffic pinned to the current known-good pod until cutover.
- Snapshot every PVC before changing images, selectors, replicas, or data.
- Do not copy graph segment files or graph directories between PVCs.
- Do not roll a fixed image across known-divergent PVCs expecting automatic repair.
- Use forensic exports/diffs to classify evidence before choosing a source.
- Import only into a fresh or explicitly controlled target through normal raft-owned APIs.
- Preserve old PVC snapshots and forensic reports after cutover.

## Planning helper

`scripts/planGraphRepairWorkflow.sh` prints read-only repair plans and classifications. It never mutates daemon state or Kubernetes resources. It refuses to run without an explicit snapshot acknowledgement:

```sh
cd mycel
scripts/planGraphRepairWorkflow.sh \
  --workflow fresh-cluster-import \
  --i-have-snapshots \
  --source-node myceld-0/pinned-good \
  --export-path /evidence/pinned-good-export.mycel-stream \
  --target-cluster fresh-empty-cluster
```

Classify a G6 forensic diff:

```sh
scripts/planGraphRepairWorkflow.sh \
  --workflow classify-diff \
  --i-have-snapshots \
  --source-node myceld-0/pinned-good \
  --diff /evidence/pinned-good-vs-archived-pvc-b.diff.json \
  --authoritative-side left
```

Possible classifications:

| Classification | Meaning | Operator action |
| --- | --- | --- |
| `identical_latest_state` | Supplied exports match for included entities. | Continue app-level validation before choosing a source. |
| `authoritative_source_strict_superset` | Candidate source contains the other side with no conflicts. | Fresh-cluster import is a valid manual recovery candidate after app validation. |
| `candidate_source_missing_entities` | Candidate source lacks entities present elsewhere. | Stop; investigate missing IDs before migration. |
| `conflict_recovery_required` | Same entity IDs differ. | Stop; produce human-readable conflict notes, do not auto-merge. |
| `incomplete_evidence` | One or both exports are truncated, or the diff only covers included entities. | Collect all pages before deciding. |

## Workflow A: fresh-cluster import from authoritative source

Recommended for the current pinned-pod split-brain scenario.

1. Keep service traffic pinned to the candidate authoritative pod/PVC.
2. Stop writes and enter a maintenance window.
3. Snapshot all PVCs and verify snapshots are restorable.
4. Capture evidence from every pod/PVC:
   - `cluster status` and `cluster health`;
   - `cluster consistency-report` for affected space/domain pairs;
   - `cluster forensic-export` with unique `--source-label` values;
   - application-level exports where available.
5. Choose the source only after graph counts/checksums and app-level validation agree.
6. Export application data from the chosen source through the supported client import/export API.
7. Create a fresh cluster with empty PVCs and the fixed image.
8. Import through normal raft-owned APIs.
9. Validate before cutover:
   - cluster identity and health on every pod;
   - raft group diagnostics;
   - `cluster consistency-report` for imported spaces/domains;
   - data-plane write/read/query validation;
   - application login/journal/domain validation.
10. Switch traffic after validation passes.
11. Retain old PVC snapshots and evidence until the migration has been audited.

## Workflow B: strict-superset recovery

Use this only when G6 forensic diffs prove one source is a strict superset and no entity IDs conflict.

1. Collect complete forensic exports for the candidate source and every archived PVC.
2. Run `cluster forensic-diff` for each pair.
3. Use the planning helper with `--workflow classify-diff`.
4. If any diff is truncated, collect remaining pages and repeat.
5. If any diff reports differing nodes or edges, switch to Workflow C.
6. If the candidate is a strict superset across all sources, treat it as the authoritative source for Workflow A.
7. Import into a fresh cluster through raft-owned APIs; do not copy local graph files.

## Workflow C: conflict recovery

Use this when the same node or edge ID differs between sources, or when different PVCs contain unique data and no strict superset exists.

1. Stop. Do not import over a target cluster.
2. Preserve all snapshots and forensic exports.
3. Produce a human-readable conflict report from `cluster forensic-diff` JSON:
   - missing node IDs;
   - missing edge IDs;
   - differing entity IDs;
   - changed canonical fields;
   - left/right source labels and checksums.
4. Have domain/application owners decide the source of truth for each conflict.
5. Build an explicit application-level migration plan for a fresh cluster.
6. Only then import through normal raft-owned APIs.

## What remains intentionally unsupported

- Automatic merge of divergent PVCs.
- In-place rebalancing of old split-brain local graph stores.
- Copying graph segment files into a running raft cluster.
- Deleting or overwriting data during daemon startup.
- Treating a latest-state checksum match as proof of historical equivalence.
