# Semantic projection readiness after leadership changes

A healthy Raft partition and a logically fresh semantic rule do not, by themselves,
prove that the elected leader has a searchable local vector projection. Search
also requires a local projection lease for the same rule/binding, leader node,
leader term, semantic generation, rebuild epoch, and freshness frontier.

The v0.21.3 recovery fix (#300/#301) can safely re-certify current local evidence,
rebuild a physical index from retained vector records, or perform bounded
re-materialization from graph truth. This does not remove the readiness fence.
An uncertified node remains unavailable instead of serving stale vectors.

## Readiness error contract

This diagnostic contract is added after v0.21.3 for issue #299; older binaries
may still return the generic `Internal` readiness message.

Direct `SemanticSearch` and hybrid `Search` preserve categorized readiness errors,
including when a follower forwards execution to the semantic partition leader:

- `Unavailable`: local leadership or a projection certificate is not yet usable;
  retry with bounded backoff while leadership/reconciliation stabilizes.
- `FailedPrecondition`: the relevant rebuild state is `required` or `failed`;
  inspect reliability/rebuild diagnostics and obtain approval for any recovery
  mutation. Do not loop indefinitely or treat the failure as an empty result.
- A `running` rebuild remains transient (`Unavailable`) when readiness cannot be
  certified. Existing safe rebuild-blocked empty-result behavior is unchanged.
- Unrelated storage, decoding, and recovery I/O errors are not rewritten as
  transient readiness failures.

The status carries standard `google.rpc.ErrorInfo`, without changing public
protobuf definitions:

```text
reason: SEMANTIC_PROJECTION_NOT_READY
domain: myceldb.io
metadata.schema_version: 1
```

Metadata contains `readiness_reasons` (a deterministic comma-separated list),
`semantic_rule_id` and `embedding_binding_key` when known, `local_node`, expected
`leader_node`, `leader_term`, `semantic_generation`, `rebuild_epoch`,
`fresh_through_graph_revision`, and `rebuild_state`. `lease_present` distinguishes
an absent lease from observed zero-valued fields. Observed values, when present,
use `lease_` prefixes for the corresponding tuple fields. A freshness frontier
at or beyond the required revision is valid; exact equality is not required.

| Reason | Meaning |
| --- | --- |
| `missing_lease` | No local lease exists for the selected rule/binding. |
| `leader_mismatch` | The lease belongs to another node. |
| `term_mismatch` | The lease certifies a different leadership term. |
| `generation_mismatch` | The lease certifies a different semantic generation. |
| `rebuild_epoch_mismatch` | The lease certifies a different rebuild epoch. |
| `fresh_through_lag` | The lease frontier is behind authoritative semantic freshness. |
| `missing_local_baseline` | Automatic recovery could not establish a current local baseline. |
| `leadership_changed` | Recovery lacks the captured current local leadership context. |
| `local_leadership_unavailable` | This process does not own the current semantic partition leadership. |
| `rebuild_required`, `rebuild_running`, `rebuild_failed` | Current rebuild state prevents lease recovery. |

Multiple mismatches are reported rather than hiding all but the first. If
historical duplicate entries exist, diagnostics select the closest applicable
rule/binding lease. Neither messages nor details contain graph content, queries,
raw vectors, provider credentials, provider responses, or filesystem paths.
Hybrid readiness failures are not silently converted to lexical-only success,
even when the caller did not set `require_both`.

## Stateless ingress routing

Public lexical/hybrid `Search` and `GetLexicalIndexStatus` requests are scoped by
space/domain and the authenticated principal, not by a graph session or
transaction. A follower forwards these exact RPC methods to the graph partition
leader. The backend permits those two methods without graph route IDs while
retaining cluster-token authentication, admission/cluster matching, target-node
validation, forwarding-depth limits, and authorization by the target API handler.
Other graph/session/transaction operations retain their existing route-ID checks.

`InvalidArgument: session_id or transaction_id is required` on this search/status
path identifies the older forwarding guard, not a projection-readiness mismatch.
Do not work around it by inventing a graph session ID or disabling backend
authentication. Use a candidate containing the stateless search forwarding fix;
retain the exact source/image identity when reporting validation.

## Read-only triage

Do not delete PVCs, reset data directories, edit lease JSON, change domain privacy
policy, or force a backfill/rebuild merely to collect diagnostics.

1. Record the daemon image digest/version and the authorized request's rule ID,
   binding, and gRPC status/details. Do not record confidential query text.
2. Use [Raft cluster operations](raft-cluster-operations.md) to identify the
   semantic partition's current leader and term, not just the system leader.
3. Read `AdminSemanticMaintenanceService.GetSemanticReliabilityStatus` for the
   affected rule/binding. Compare generation, rebuild epoch/state/owner node and
   term, latest-observed/analyzed-through/fresh-through revisions, unresolved
   gaps, and degraded state. Queue depth alone is not readiness evidence.
4. With authorized host/pod access, inspect the elected leader's retained
   `<data-dir>/graphs/<space-id>/semantic_projection_leases.json` read-only.
   Compare its tuple with `ErrorInfo`. Do not modify or copy vector payloads.
5. For a stable leader with repeated `Unavailable`, retain the bounded error
   details and reliability snapshots. Escalate missing local baseline, recovery
   limits, and actual storage errors rather than bypassing the fence.
6. For `FailedPrecondition`, request an explicit scoped recovery plan. Backfill,
   rebuild, reconciliation, restart/election, or any retained-state deployment
   mutation requires separate operator approval.

A read-only admin/host inspection is different from executing a search: the
existing automatic reconciliation path can reconstruct derived state when a
search reaches the current leader.

## Validation and original deployment sign-off

Hermetic tests cover every mismatch dimension, missing-baseline refusal, retained
records/reliability/lease replay across subsystem restart, repeated term changes,
promotion with current local evidence, status/detail preservation across the
cluster-backend wire, and both public direct/hybrid search paths. They do not
claim to exercise the original Commonfolio/K3s deployment.

For that final deployment check, first obtain approval for a candidate image and
any restart/leadership perturbation. Keep all existing PVCs and rule/binding and
privacy configuration intact. Record baseline direct and hybrid results, observe
an approved stable leadership change, and verify automatic recovery without
manual backfill or PVC reset. Confirm unchanged record/generation provenance,
correct graph freshness, and fail-closed behavior while readiness is absent.
Retain sanitized image, leader/term, reliability, readiness, and result evidence.
Do not mark that deployment check complete based only on unit tests or the prior
v0.21.3 Compose/direct-search aggregate.
