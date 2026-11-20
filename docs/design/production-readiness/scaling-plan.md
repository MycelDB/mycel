# Scaling plan

Status: draft  
Last updated: 2026-11-19

## Production requirement

CommonFolio will start with a small Mycel cluster, likely three pods on AWS, but
needs a credible path to add capacity if usage grows. Dynamic pod scaling is
important, but its launch requirement depends on the risk accepted for first
production.

For first production, Mycel must provide either tested dynamic scaling or a
conservative static capacity plan with an emergency expansion path. Broader paid
rollout should require tested dynamic add/drain/remove workflows.

## Option A: Dynamic scaling is production-ready

If dynamic scaling is part of the first production launch, Mycel should support:

- add pod;
- authenticate and admit the new pod into cluster membership;
- rebalance or place partition groups safely;
- drain pod;
- remove or replace pod;
- preserve quorum and availability during membership changes;
- show rebalance progress;
- recover safely from interrupted scaling operations.

This option should be validated by Mycel Lab or staging scenarios for add-pod,
drain-pod, remove-pod, interrupted rebalance, and rollback/restore interaction.

## Option B: Static capacity plan for first production

If full dynamic scaling is not ready for first production, CommonFolio may accept
a temporary static-capacity launch only if:

- the initial three-pod cluster has measured headroom;
- there is a documented maximum supported production load;
- capacity inventory and alerts show when that limit is approaching;
- there is a documented emergency expansion path, even if manual;
- onboarding can be slowed or stopped before capacity becomes unsafe;
- dynamic scaling is treated as a near-term follow-up before broader rollout.

This is acceptable only as a conservative first-production bridge, not as the
long-term scaling strategy.

## Relationship to capacity inventory

Scaling decisions require capacity visibility. The scaling plan should consume
infrastructure capacity inventory values such as:

- healthy pod count;
- CPU and memory usage;
- PVC capacity and free space;
- partition/leader distribution;
- zone and resource class where available.

Future heterogeneous clusters should be able to use resource classes and
telemetry to inform placement, but first production may start with homogeneous
pods and manual scale decisions.

## Launch gate

CommonFolio production should require one of the following:

1. tested dynamic add/drain/remove workflows; or
2. a documented static capacity envelope, capacity alerts, emergency expansion
   runbook, and explicit acceptance that dynamic scaling is a near-term follow-up
   before broad growth.

## Follow-up design work

- Define cluster membership APIs and operator flows for add, drain, remove, and
  replace.
- Define rebalance planning and dry-run output.
- Define placement metadata needed for resource classes and zones.
- Add Mycel Lab scenarios for scaling workflows and interrupted rebalances.
- Decide when autoscaling recommendations or automation become safe.
