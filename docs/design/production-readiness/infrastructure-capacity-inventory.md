# Infrastructure capacity inventory

Status: draft  
Last updated: 2026-11-19

## Production requirement

Operators need to know the underlying capacity and current usage of the pods and
nodes running Mycel. This is mandatory for CommonFolio production because the
team must know when the initial cluster is approaching unsafe CPU, memory, or
storage pressure.

This work is separate from workspace usage accounting. Workspace usage answers
"what is the customer consuming?" Capacity inventory answers "what resources
does the cluster have and how heavily are they used?"

## Mandatory first-cut capabilities

The initial production version does not need full heterogeneous placement
optimization, but it should expose enough inventory to operate safely.

### Per-pod or per-node values

- pod ID and daemon/node identity;
- Mycel version;
- uptime;
- healthy/unhealthy state;
- Kubernetes node name when running on Kubernetes;
- zone or region when available;
- persistent volume capacity;
- persistent volume used/free bytes;
- CPU current usage;
- memory current usage;
- hosted partition or Raft-role summary when available.

### Cluster summary values

- total pod count;
- healthy pod count;
- total persistent storage capacity;
- used/free persistent storage;
- current storage pressure;
- leader/quorum status;
- high-level partition placement health.

## Future heterogeneous-equipment support

Future clusters may include different instance types, architectures, storage
classes, or resource profiles. Capacity inventory should therefore avoid
assuming homogeneous equipment.

Fields that become important for heterogeneous placement include:

- CPU architecture, such as `amd64` or `arm64`;
- CPU model when available;
- instance type when available;
- allocatable CPU and memory;
- storage class;
- observed disk throughput or pressure;
- resource class labels suitable for placement decisions.

## Authority and freshness

Capacity data is operational telemetry, not billing data. API responses should
include:

- observed time;
- source, such as Kubernetes API, cgroup, filesystem stat, daemon self-report,
  or cloud metadata;
- freshness;
- whether a value is exact, sampled, approximate, or unavailable.

High-frequency telemetry should not be committed through Raft. Durable topology,
resource-class labels, membership state, and placement decisions may be
committed where needed.

## Launch gate

CommonFolio production should require:

1. per-pod/PVC/CPU/memory visibility;
2. cluster storage and health summary;
3. alerts for storage pressure and unhealthy pod count;
4. an operator view or CLI command that answers whether the initial three-pod
   cluster has safe headroom.

## Follow-up design work

- Decide which collector owns Kubernetes/EKS telemetry: daemon, sidecar,
  operator, or external observability stack.
- Define API response shapes for cluster summary and pod inventory.
- Define resource-class labels for future heterogeneous placement.
- Connect capacity inventory to add-pod, drain-pod, and rebalance planning.
