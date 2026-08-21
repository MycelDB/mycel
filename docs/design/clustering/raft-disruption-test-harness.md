# Raft Disruption Test Harness

## Status

Design proposal for an explicit destructive integration test harness. The first
implementation should target a small K3s three-pod raft deployment because that
is closest to the expected production shape, but the harness should keep the
cluster driver pluggable so Docker Compose or a local multi-process deployment
can be added later.

This is not a normal unit-test target. It intentionally kills/restarts cluster
members and writes test data. It must only run when an operator or developer
explicitly asks for destructive cluster validation.

## Problem

A healthy mycel raft deployment must tolerate one pod restart without wedging the
whole system. The user-visible concern is that after one pod goes down and comes
back, writes and reads may stop working or a restarted pod may not catch up.

Ad-hoc manual testing is not enough because failures are timing-dependent and
hard to reproduce. We need a reusable test harness that can progressively add
more disruption scenarios and higher write pressure while preserving clear,
actionable evidence when something fails.

## Goals

- Prove a three-node raft deployment continues to accept writes with one pod
  unavailable, assuming a quorum remains.
- Prove a restarted pod becomes ready and catches up.
- Verify committed writes are not lost after disruption.
- Verify cluster health and graph consistency after recovery.
- Capture enough diagnostics to debug failures without rerunning immediately.
- Start small, then grow into a suite of disruption and pressure scenarios.
- Reuse code across K3s, Compose, and local cluster drivers where practical.

## Non-goals

- Do not run this from `make test` by default.
- Do not add automatic repair, rebalance, merge, or divergent PVC overwrite
  behavior.
- Do not assert that every attempted write during disruption must succeed. The
  harness should distinguish transient retryable errors from lost committed
  writes or permanent cluster wedging.
- Do not depend on a specific app workload such as Knot PKM. The harness should
  use direct mycel daemon APIs.
- Do not require an external LLM, semantic provider, or user data.

## Initial target

The first target should be K3s:

```sh
make test-k3s-raft-disruption
```

The target should always create a disposable test cluster/deployment at the
start of the run and tear it down at the end. This is required for consistency:
each run starts from empty PVCs, a known raft bootstrap configuration, and a
known Kubernetes service topology.

`kubectl` must be present and usable by the harness. Creating a brand-new K3s
cluster also requires a local provisioner. The preferred first provisioner is
`k3d` because it creates disposable K3s clusters with a standard kubeconfig. The
harness should keep this behind a provisioner interface so `kind`, an existing
single-node K3s server, or CI-specific provisioning can be added later.

Default behavior:

1. preflight `kubectl` and the selected provisioner;
2. create a unique k3d-safe cluster name such as `mycel-rdt-<timestamp>`;
3. build or load the requested mycel image into the cluster;
4. apply the three-pod raft StatefulSet, headless service, and client service;
5. run the disruption scenario;
6. collect artifacts;
7. delete the cluster, even on failure, unless `--keep-cluster-on-failure` is
   set for debugging.

## Harness architecture

Implement the test runner as Go code rather than shell. Shell may remain useful
for setup wrappers, but the disruption logic should be a Go binary/test package
so it can share retry, concurrency, client, and assertion code.

Suggested package layout:

```text
internal/clustering/disrupttest/
  harness.go          # scenario orchestration and lifecycle
  workload.go         # write/read pressure workers and accounting
  assertions.go       # health, catch-up, consistency assertions
  artifacts.go        # logs/status collection
  config.go           # flags/env parsing
  provisioner.go      # disposable cluster lifecycle interface
  k3d_provisioner.go  # initial K3s cluster provisioner
  driver.go           # ClusterDriver interface
  k3s_driver.go       # kubectl/K3s implementation
  compose_driver.go   # future Docker Compose implementation
  local_driver.go     # future local multi-process implementation
```

Suggested command/test entrypoint:

```text
cmd/mycel-raft-disrupttest/main.go
```

or a build-tagged integration test package:

```text
internal/clustering/disrupttest/disrupt_test.go
```

A standalone command is preferred for the first version because it can expose
flags and can be run by CI or operators without relying on Go test timeouts.

## Provisioner and cluster driver interfaces

The provisioner owns disposable environment lifecycle. The driver abstracts
in-cluster operations from raft assertions.

Conceptual provisioner interface:

```go
type ClusterProvisioner interface {
    Name() string
    Preflight(ctx context.Context) error
    Create(ctx context.Context, cfg ClusterConfig) (KubeContext, error)
    LoadImage(ctx context.Context, image string) error
    Delete(ctx context.Context) error
}
```

For the first implementation, `k3d` should implement this interface by creating
a disposable K3s cluster and exposing a kubeconfig context for `kubectl`.


Conceptual interface:

```go
type ClusterDriver interface {
    Name() string
    Nodes(ctx context.Context) ([]NodeRef, error)
    WaitReady(ctx context.Context, node NodeRef) error
    WaitAllReady(ctx context.Context) error
    RestartNode(ctx context.Context, node NodeRef) error
    StopNode(ctx context.Context, node NodeRef) error
    StartNode(ctx context.Context, node NodeRef) error
    PortForward(ctx context.Context, node NodeRef, port int) (Endpoint, func(), error)
    ServiceEndpoint(ctx context.Context) (Endpoint, func(), error)
    CollectArtifacts(ctx context.Context, dir string) error
}
```

For K3s, `NodeRef` maps to a pod in the disposable cluster namespace. The
initial restart operation can use `kubectl delete pod <pod>` and then wait for
the StatefulSet to recreate it. Later tests can add hard network partitions or
process pauses if needed.

## Deployment lifecycle

The harness should render/apply Kubernetes manifests from checked-in templates or
embedded Go templates. The initial deployment should include:

- one namespace unique to the run;
- one Secret for bootstrap/admin credentials generated for the run;
- one ConfigMap for raft environment and node addresses;
- one headless service for StatefulSet peer DNS;
- one client service for normal gRPC traffic;
- one three-replica StatefulSet with persistent volume claims;
- readiness/liveness probes matching the current deployment recommendations.

Because the cluster is disposable, the harness can use small resource requests
and local storage defaults appropriate for K3s/k3d. It should still exercise the
same raft bootstrap settings expected in production: three nodes, replica factor
three, and the configured partition count.

## Mycel client layer

The harness should use mycel Go clients or generated gRPC clients directly. It
should not shell out to the CLI for the main workload loop.

The client layer should provide:

- admin login/bootstrap login;
- create or reuse a test space and default domain;
- open short-lived sessions/transactions for write batches;
- execute GQL writes and committed reads;
- query cluster/admin diagnostics;
- connect either through the service endpoint or to a specific pod endpoint.

The harness should record every attempted write with:

- sequence number;
- start/end time;
- target endpoint;
- final result: succeeded, retryable failure, permanent failure;
- committed revision or mutation metadata when available;
- error code/message when failed.

Only writes that the daemon reports as successful are included in the final
must-exist set.

## Workload model

Use deterministic synthetic graph data. The first workload should be intentionally
small:

```gql
INSERT (:ChaosWrite {run: '<run-id>', seq: <n>, worker: '<worker-id>'})
```

Verification query:

```gql
MATCH (n:ChaosWrite {run: '<run-id>'})
RETURN count(n)
FETCH FIRST 1 ROW ONLY
```

Later workloads can add:

- edges between nodes;
- multi-statement transaction batches;
- mixed read/write workers;
- multiple spaces mapping to different partitions;
- larger payloads/blobs;
- schema validation;
- backup/restore interaction tests;
- automation-triggering writes once raft behavior is stable.

## Pressure profiles

Scenarios should accept named pressure profiles so tests can grow without
rewriting orchestration code.

Initial profiles:

| Profile | Duration | Writers | Rate | Purpose |
| --- | ---: | ---: | ---: | --- |
| `smoke` | 30s | 1 | 1-5 writes/s | Fast local confidence |
| `small` | 2m | 2 | 5-20 writes/s | Initial disruption gate |
| `medium` | 10m | 4-8 | 20-100 writes/s | Soak candidate |
| `soak` | 30-120m | configurable | configurable | Manual stress |

The first committed target should implement `smoke` and `small` only.

## Initial scenario: restart one follower/unknown pod under writes

The first scenario should not require leader discovery. It can restart a chosen
pod by ordinal or iterate over all pods one at a time.

Flow:

1. Discover three pods/nodes.
2. Wait for all pods ready.
3. Create a unique `run_id` and test space/domain.
4. Verify baseline cluster/admin health.
5. Start write workers through the service endpoint.
6. After a warm-up interval, restart one pod with the driver.
7. Keep write workers running during outage and recovery.
8. Wait for the restarted pod to become ready.
9. Continue writes for a cool-down interval.
10. Stop workers.
11. Wait for raft health/catch-up conditions.
12. Query the final count through the service endpoint.
13. Query the final count through each pod endpoint if possible.
14. Fail if successful writes are missing or per-pod counts diverge.
15. Collect artifacts regardless of pass/fail.

A later scenario can explicitly identify and restart the current leader. The
first scenario should be simpler and still useful because any pod can be killed
by Kubernetes in production.

## Health and convergence assertions

The harness should use existing admin/diagnostic APIs where available and keep
fallback graph assertions when diagnostics are incomplete.

Required first assertions:

- all expected pods become Kubernetes ready;
- mycel admin/health calls succeed on each pod;
- writes through the service endpoint succeed after recovery;
- `ChaosWrite` count is at least the number of writes reported successful by the
  harness;
- per-pod committed-read counts converge to the same value;
- no pod reports a different cluster identity;
- no cluster consistency/forensic report indicates graph divergence, if that API
  is available.

Future assertions:

- every raft group has a leader;
- restarted pod applied indexes catch up to leader indexes;
- read-index committed reads succeed on each group;
- no retry queues or pending raft apply loops remain stuck;
- partition checksums agree for the touched space partition.

## Retry and failure policy

During disruption, the harness should retry operations with bounded exponential
backoff and classify errors.

Acceptable transient errors during the disruption window:

- gRPC `Unavailable`;
- deadline exceeded;
- no leader/leader transfer in progress;
- route target temporarily unavailable;
- read-index temporarily unavailable.

Unacceptable failures:

- successful write missing from final graph;
- permanent failure after the recovery deadline;
- pod ready but mycel health/cluster diagnostics fail;
- different final committed counts across pods;
- divergent cluster IDs;
- graph checksum mismatch;
- panic/crash loops after the restart window.

The test should report both attempted and successful write counts. It should not
assume attempts sent to a down pod were committed.

## Artifacts

Each run should write an artifact directory such as:

```text
artifacts/raft-disruption/<timestamp>-<run-id>/
  config.json
  scenario.json
  write-events.jsonl
  final-counts.json
  cluster-health-before.json
  cluster-health-during.json
  cluster-health-after.json
  kubectl-pods-before.txt
  kubectl-pods-after.txt
  pod-describe-*.txt
  pod-logs-*.log
```

Artifacts must not include plaintext passwords, refresh tokens, or provider
secrets.

## Make targets

Add explicit targets only:

```make
.PHONY: test-k3s-raft-disruption
test-k3s-raft-disruption:
	docker build -f Dockerfile -t $(MYCEL_RAFT_DISRUPT_IMAGE) ..
	go run ./cmd/mycel-raft-disrupttest --driver k3s --provisioner k3d --profile small --restart-node all --image $(MYCEL_RAFT_DISRUPT_IMAGE) --confirm-destructive

.PHONY: test-k3s-raft-disruption-smoke
test-k3s-raft-disruption-smoke:
	docker build -f Dockerfile -t $(MYCEL_RAFT_DISRUPT_IMAGE) ..
	go run ./cmd/mycel-raft-disrupttest --driver k3s --provisioner k3d --profile smoke --image $(MYCEL_RAFT_DISRUPT_IMAGE) --confirm-destructive
```

These targets should be documented as destructive and should not be dependencies
of `make test`.

## Configuration

Suggested flags/env:

| Flag | Env | Meaning |
| --- | --- | --- |
| `--driver` | `MYCEL_DISRUPT_DRIVER` | `k3s`, future `compose`, `local` |
| `--provisioner` | `MYCEL_DISRUPT_PROVISIONER` | `k3d` initially, future `kind` or `external` |
| `--cluster-name` | `MYCEL_DISRUPT_CLUSTER_NAME` | Optional disposable cluster name prefix/override |
| `--namespace` | `MYCEL_K3S_NAMESPACE` | Kubernetes namespace created inside the disposable cluster |
| `--selector` | `MYCEL_K3S_SELECTOR` | Pod label selector |
| `--service` | `MYCEL_K3S_SERVICE` | Service name for normal client traffic |
| `--image` | `MYCEL_DISRUPT_IMAGE` | myceld image to deploy; default local test image |
| `--admin-username` | `MYCEL_ADMIN_USERNAME` | Admin username |
| `--admin-password-file` | `MYCEL_ADMIN_PASSWORD_FILE` | Password file, preferred |
| `--profile` | `MYCEL_DISRUPT_PROFILE` | `smoke`, `small`, `medium`, `soak` |
| `--restart-node` | | Pod ordinal/name, or `all`; default first pod |
| `--no-disruption` | `MYCEL_DISRUPT_NO_DISRUPTION` | Run write/read pressure without pod restarts |
| `--scenario` | `MYCEL_DISRUPT_SCENARIO_FILE` | Optional JSON scenario defaults |
| `--workload` | `MYCEL_DISRUPT_WORKLOAD` | Workload: `nodes`, `edges`, or `multi-space` |
| `--artifacts-dir` | | Artifact output root |
| `--keep-cluster-on-failure` | | Preserve the disposable cluster for debugging |
| `--confirm-destructive` | | Required acknowledgement for cluster create/delete/restart |

Avoid accepting plaintext password flags in CI logs where possible.

## Phased implementation plan

### Phase 1 — Smoke harness

- K3s/k3d provisioner with disposable cluster create/delete lifecycle.
- K3s driver with namespace deployment, pod discovery, restart, application-level
  readiness (`mycel cluster readiness check`), wait-ready, and artifact
  collection.
- Mycel client wrapper for login, test space/domain creation, GQL write/read.
- One writer, low rate, one pod restart.
- Final service count assertion.

### Phase 2 — Per-pod convergence

- Port-forward or direct pod endpoints.
- Per-pod committed count checks.
- Cluster identity and health diagnostics.
- Better retry classification.

### Phase 3 — Pressure profiles and matrix scenarios

- Configurable writer counts/rates/durations.
- Restart each pod one at a time.
- Leader restart scenario if leader discovery is available.
- Separate warm-up/outage/cool-down timing.

### Phase 4 — Data-shape and partition coverage

- `edges` workload writes parent/child nodes plus `CHAOS_EDGE` relationships and
  verifies both node and edge counts.
- `multi-space` workload distributes writes across multiple spaces/domains to
  touch more partition routes.
- Writers perform periodic committed reads during pressure; read failures are
  recorded and fail the scenario.
- Checksum/forensic consistency integration can be added as diagnostics stabilize.

### Phase 5 — Soak and CI gating

- Long-running manual soak profile.
- Optional nightly CI against disposable K3s.
- Historical artifact summaries and flake tracking.

## Open questions

1. Which existing admin API is the best source of raft group leader/apply-index
   diagnostics for automated assertions?
2. Should write workers route through one service endpoint only, or also test
   direct-to-pod endpoints during pressure?
3. Should the first test restart a fixed pod, each pod, or attempt to identify
   and restart the current leader?
4. What should be the initial recovery deadline for a pod restart: 60s, 120s, or
   configurable only?
5. Should CI keep failed disposable clusters for post-mortem via a retention
   policy, or always delete clusters after artifact capture?

## Safety notes

- The harness must create a unique run ID and test space/domain, never operate on
  a production space by default.
- Destructive operations are limited to the disposable test cluster and namespace
  created by the harness.
- The harness must print the disposable cluster name, namespace, selector,
  service, and pods before any restart and require an explicit
  `--confirm-destructive` flag unless running in a known CI environment.
- Cleanup should run from `defer`/finalization paths so clusters are deleted on
  success and failure. `--keep-cluster-on-failure` is allowed only for explicit
  debugging and must be obvious in the final output.
- Divergence tooling remains forensic/read-only. If divergence is detected, the
  harness fails and preserves artifacts; it must not repair data automatically.
