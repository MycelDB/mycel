#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/planGraphRepairWorkflow.sh --workflow fresh-cluster-import \
    --i-have-snapshots --source-node <pod-or-pvc> [--export-path <path>] [--target-cluster <name>]

  scripts/planGraphRepairWorkflow.sh --workflow classify-diff \
    --i-have-snapshots --source-node <pod-or-pvc> --diff <forensic-diff.json> \
    --authoritative-side left|right

This script is intentionally read-only. It prints an operator repair plan and
never imports, deletes, overwrites, copies graph segment files, scales pods, or
changes daemon state.
EOF
}

workflow="fresh-cluster-import"
have_snapshots="false"
source_node=""
diff_file=""
authoritative_side=""
export_path=""
target_cluster="fresh-empty-raft-cluster"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workflow)
      workflow="${2:-}"; shift 2 ;;
    --i-have-snapshots)
      have_snapshots="true"; shift ;;
    --source-node)
      source_node="${2:-}"; shift 2 ;;
    --diff)
      diff_file="${2:-}"; shift 2 ;;
    --authoritative-side)
      authoritative_side="${2:-}"; shift 2 ;;
    --export-path)
      export_path="${2:-}"; shift 2 ;;
    --target-cluster)
      target_cluster="${2:-}"; shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2 ;;
  esac
done

if [[ "$have_snapshots" != "true" ]]; then
  echo "refusing to produce a repair plan without --i-have-snapshots" >&2
  echo "snapshot every PVC before choosing a repair workflow" >&2
  exit 2
fi
if [[ -z "$source_node" ]]; then
  echo "--source-node is required to identify the candidate authoritative pod/PVC" >&2
  exit 2
fi

print_header() {
  cat <<EOF
# MycelDB Phase G7 Manual Repair Plan

Mode: read-only plan generation
Candidate source: $source_node
Target cluster: $target_cluster

Safety invariants:
- snapshots already exist for every PVC;
- do not copy graph segment files between PVCs;
- do not run new images across known-divergent PVCs expecting automatic repair;
- perform import only through normal raft-owned APIs into a fresh/controlled target;
- stop and preserve evidence if conflicts are present.
EOF
}

fresh_cluster_plan() {
  print_header
  cat <<EOF

Recommended workflow: fresh-cluster import from authoritative source

1. Keep application traffic pinned to the candidate source until cutover.
2. Stop writes and enter a maintenance window.
3. Verify snapshots for all PVCs are complete and restorable.
4. Capture evidence from every source PVC/pod:
   - cluster status and health;
   - cluster consistency-report for affected space/domain pairs;
   - forensic-export JSON with explicit --source-label;
   - application-level export when available.
5. Export application data from $source_node through the supported client export API.
EOF
  if [[ -n "$export_path" ]]; then
    echo "   Planned export path: $export_path"
  fi
  cat <<EOF
6. Create $target_cluster with empty PVCs and the fixed image.
7. Import only through normal raft-owned APIs; do not copy graph directories.
8. Run validation before cutover:
   - cluster status/health on every pod;
   - raft-groups diagnostics;
   - cluster consistency-report for each imported space/domain;
   - Compose/K3s data-plane validation where applicable;
   - application login/journal/domain validation.
9. Switch traffic only after validation passes.
10. Retain old PVC snapshots and forensic exports until the migration is audited.

No mutation was performed by this script.
EOF
}

classify_diff_plan() {
  if [[ -z "$diff_file" ]]; then
    echo "--diff is required for --workflow classify-diff" >&2
    exit 2
  fi
  if [[ "$authoritative_side" != "left" && "$authoritative_side" != "right" ]]; then
    echo "--authoritative-side must be left or right" >&2
    exit 2
  fi
  if [[ ! -f "$diff_file" ]]; then
    echo "diff file not found: $diff_file" >&2
    exit 2
  fi
  print_header
  python3 - "$diff_file" "$authoritative_side" <<'PY'
import json, sys
path, auth = sys.argv[1:3]
with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)
node = data.get("node_summary") or {}
edge = data.get("edge_summary") or {}
left_only = int(node.get("only_in_left") or 0) + int(edge.get("only_in_left") or 0)
right_only = int(node.get("only_in_right") or 0) + int(edge.get("only_in_right") or 0)
differing = int(node.get("differing") or 0) + int(edge.get("differing") or 0)
warnings = [str(w).lower() for w in (data.get("warnings") or [])]
source_truncated = any("truncated" in w or "diff only covers included entities" in w for w in warnings)
truncated = bool(data.get("truncated")) or source_truncated
print("\nForensic diff summary:")
print(f"- nodes only in left: {node.get('only_in_left', 0)}")
print(f"- nodes only in right: {node.get('only_in_right', 0)}")
print(f"- differing nodes: {node.get('differing', 0)}")
print(f"- edges only in left: {edge.get('only_in_left', 0)}")
print(f"- edges only in right: {edge.get('only_in_right', 0)}")
print(f"- differing edges: {edge.get('differing', 0)}")
if truncated:
    print("\nClassification: incomplete_evidence")
    print("Action: collect all export pages before selecting a repair path.")
    sys.exit(0)
if differing:
    print("\nClassification: conflict_recovery_required")
    print("Action: stop. Preserve evidence and do not import over any target until humans resolve conflicting entities.")
    sys.exit(0)
if auth == "left":
    if right_only:
        print("\nClassification: candidate_source_missing_entities")
        print("Action: stop. The left/candidate source is not a strict superset; investigate right-only IDs before migration.")
    elif left_only:
        print("\nClassification: authoritative_source_strict_superset")
        print("Action: fresh-cluster import from the left/candidate source is a valid manual recovery candidate after app validation.")
    else:
        print("\nClassification: identical_latest_state")
        print("Action: fresh-cluster import may proceed if application-level validation chooses this source.")
else:
    if left_only:
        print("\nClassification: candidate_source_missing_entities")
        print("Action: stop. The right/candidate source is not a strict superset; investigate left-only IDs before migration.")
    elif right_only:
        print("\nClassification: authoritative_source_strict_superset")
        print("Action: fresh-cluster import from the right/candidate source is a valid manual recovery candidate after app validation.")
    else:
        print("\nClassification: identical_latest_state")
        print("Action: fresh-cluster import may proceed if application-level validation chooses this source.")
print("\nNo mutation was performed by this script.")
PY
}

case "$workflow" in
  fresh-cluster-import)
    fresh_cluster_plan ;;
  classify-diff)
    classify_diff_plan ;;
  *)
    echo "unsupported workflow: $workflow" >&2
    usage >&2
    exit 2 ;;
esac
