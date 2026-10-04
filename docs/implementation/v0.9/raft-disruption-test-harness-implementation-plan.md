# Raft Disruption Test Harness Implementation Plan

## Status

Proposed. This plan implements the design in
[Raft disruption test harness](../../design/clustering/raft-disruption-test-harness.md).

The harness is intentionally destructive and must not run as part of `make test`.
It creates a disposable K3s cluster, deploys a three-pod raft mycel cluster,
applies write pressure, restarts a pod, verifies recovery/convergence, collects
artifacts, and tears the cluster down.

## Goals

- Provide a repeatable Go-based disruption harness for raft reliability testing.
- Always start from a fresh disposable cluster and fresh PVCs.
- Initially target K3s via `k3d` plus `kubectl`.
- Exercise writes while one raft pod is restarted.
- Verify the cluster recovers and successful writes remain visible.
- Capture artifacts that make failures actionable.
- Keep code reusable for future pressure/scenario growth.

## Non-goals

- Do not add this to `make test` or other normal validation targets.
- Do not run against production namespaces or existing clusters by default.
- Do not add automatic divergent PVC repair, automatic merge, rebalance, delete,
  overwrite, or repair behavior.
- Do not require external inference/semantic providers.
- Do not make all attempted writes mandatory successes during disruption; only
  daemon-confirmed successful writes become must-exist assertions.

## Safety requirements

- Require `--confirm-destructive` for cluster create/delete/restart unless an
  explicit CI environment variable is set.
- Generate a unique cluster name, namespace, run ID, space, and domain for every
  run.
- Delete the disposable cluster in finalization paths on success and failure.
- Support `--keep-cluster-on-failure` only for explicit debugging and print the
  retained cluster name prominently.
- Never log plaintext passwords, refresh tokens, provider secrets, or kubeconfig
  credential material in artifacts.
- Fail read-only/forensic if divergence is detected; do not repair.

## Proposed package and command layout

```text
cmd/mycel-raft-disrupttest/main.go
internal/clustering/disrupttest/
  config.go
  harness.go
  provisioner.go
  k3d_provisioner.go
  driver.go
  k3s_driver.go
  manifests.go
  client.go
  workload.go
  assertions.go
  artifacts.go
  retry.go
```

Future optional drivers can add:

```text
internal/clustering/disrupttest/compose_driver.go
internal/clustering/disrupttest/local_driver.go
```

## Phases

## RDT0: Design, command skeleton, and guardrails

### Scope

Add the command/package skeleton and safety guardrails without creating clusters
or disrupting pods yet.

### Tasks

1. Add `cmd/mycel-raft-disrupttest/main.go` with flags/env parsing:
   - `--driver` default `k3s`;
   - `--provisioner` default `k3d`;
   - `--profile` default `smoke`;
   - `--cluster-name` optional;
   - `--namespace` default generated;
   - `--image`;
   - `--artifacts-dir`;
   - `--keep-cluster-on-failure`;
   - `--confirm-destructive`.
2. Add `internal/clustering/disrupttest/config.go` with validation.
3. Require `kubectl` and selected provisioner preflight before destructive work.
4. Add a dry-run/preflight mode that prints resolved config and exits.
5. Add `Makefile` targets, clearly marked explicit/destructive:

   ```sh
   make test-k3s-raft-disruption-smoke
   make test-k3s-raft-disruption
   ```

6. Update operations/design docs with command usage and safety notes.

### Tests

- Unit: config defaults and env overrides.
- Unit: missing `--confirm-destructive` fails before provisioner calls.
- Unit: dry-run does not call destructive provisioner methods.

### Acceptance

```sh
go test ./internal/clustering/disrupttest -count=1
go run ./cmd/mycel-raft-disrupttest --dry-run
make docs-check
git diff --check
```

## RDT1: Disposable K3s/k3d provisioner

### Scope

Create and delete a disposable K3s cluster using `k3d`, and expose a kube context
for the K3s driver.

### Tasks

1. Define `ClusterProvisioner`:

   ```go
   type ClusterProvisioner interface {
       Name() string
       Preflight(ctx context.Context) error
       Create(ctx context.Context, cfg ClusterConfig) (KubeContext, error)
       LoadImage(ctx context.Context, image string) error
       Delete(ctx context.Context) error
   }
   ```

2. Implement `k3d` provisioner:
   - `k3d cluster create <name>`;
   - kubeconfig/context discovery;
   - image import/load into cluster;
   - `k3d cluster delete <name>`.
3. Ensure cleanup runs from deferred/finalization paths.
4. Add artifact capture for provisioner logs and resolved kube context.

### Tests

- Unit: command construction uses generated cluster name.
- Unit: cleanup is invoked on scenario failure.
- Optional integration/manual: create/delete empty k3d cluster.

### Acceptance

```sh
go test ./internal/clustering/disrupttest -count=1
go run ./cmd/mycel-raft-disrupttest --dry-run --provisioner k3d
# Optional/manual when k3d is installed:
go run ./cmd/mycel-raft-disrupttest --driver k3s --provisioner k3d --preflight-create-delete --confirm-destructive
git diff --check
```

## RDT2: K3s raft deployment templates

### Scope

Deploy a three-pod mycel raft StatefulSet into the disposable cluster.

### Tasks

1. Add embedded manifest templates for:
   - namespace;
   - bootstrap/admin Secret;
   - raft ConfigMap;
   - headless service;
   - client service;
   - StatefulSet with three replicas and PVC templates.
2. Configure raft env consistently:
   - cluster engine `raft`;
   - node count `3`;
   - replica factor `3`;
   - partition count from flag/default;
   - node addresses using StatefulSet DNS.
3. Ensure each pod derives local node ID from ordinal or explicit env.
4. Apply manifests with `kubectl` via Go exec wrappers.
5. Wait for all pods ready.
6. Capture `kubectl get/describe` artifacts before and after deployment.

### Tests

- Unit: rendered manifests include expected raft node addresses and replica
  counts.
- Unit: namespace/name escaping is safe.
- Manual integration: disposable cluster reaches three ready pods.

### Acceptance

```sh
go test ./internal/clustering/disrupttest -count=1
docker build -f Dockerfile -t myceldb/mycel:raft-disrupt-local ..
go run ./cmd/mycel-raft-disrupttest --driver k3s --provisioner k3d --profile smoke --image myceldb/mycel:raft-disrupt-local --setup-only --confirm-destructive
kubectl --context <generated-context> -n <namespace> get pods
git diff --check
```

## RDT3: Mycel client wrapper and smoke write/read workload

### Scope

Add direct gRPC client logic for login, test space/domain creation, GQL writes,
and committed read verification.

### Tasks

1. Add client wrapper that can connect through the K3s service endpoint.
2. Support port-forwarding or service-forwarding from host to client service.
3. Login using generated admin credentials.
4. Create a unique test space and default domain.
5. Implement one-shot GQL execution helper.
6. Implement deterministic write workload:

   ```gql
   INSERT (:ChaosWrite {run: '<run-id>', seq: <n>, worker: '<worker-id>'})
   ```

7. Implement final count query:

   ```gql
   MATCH (n:ChaosWrite {run: '<run-id>'})
   RETURN count(n)
   FETCH FIRST 1 ROW ONLY
   ```

8. Record write events as JSONL artifacts.

### Tests

- Unit: write event accounting only marks daemon-confirmed writes as successful.
- Unit: retry classifier distinguishes transient vs permanent errors.
- Integration/manual: smoke write/read succeeds on fresh cluster without pod
  disruption.

### Acceptance

```sh
go test ./internal/clustering/disrupttest -count=1
go run ./cmd/mycel-raft-disrupttest --driver k3s --provisioner k3d --profile smoke --no-disruption --confirm-destructive
git diff --check
```

## RDT4: Pod restart scenario under small pressure

### Scope

Restart one pod while write workers continue running, then verify the cluster
recovers.

### Tasks

1. Define `ClusterDriver` for K3s pod operations:

   ```go
   type ClusterDriver interface {
       Nodes(ctx context.Context) ([]NodeRef, error)
       WaitReady(ctx context.Context, node NodeRef) error
       WaitAllReady(ctx context.Context) error
       RestartNode(ctx context.Context, node NodeRef) error
       ServiceEndpoint(ctx context.Context) (Endpoint, func(), error)
       PortForward(ctx context.Context, node NodeRef, port int) (Endpoint, func(), error)
       CollectArtifacts(ctx context.Context, dir string) error
   }
   ```

2. Implement restart with `kubectl delete pod <pod>` and StatefulSet ready wait.
3. Implement workload phases:
   - warm-up;
   - restart/outage;
   - recovery wait;
   - cool-down.
4. Implement `smoke` and `small` profiles.
5. Retry writes through bounded exponential backoff.
6. Assert:
   - all pods become ready;
   - writes succeed after recovery;
   - final count is at least successful write count;
   - artifact collection completes.

### Tests

- Unit: scenario state machine calls restart between warm-up and cool-down.
- Unit: recovery timeout produces failure and artifacts.
- Manual/destructive: smoke disruption passes on disposable K3s.

### Acceptance

```sh
go test ./internal/clustering/disrupttest -count=1
make test-k3s-raft-disruption-smoke
git diff --check
```

## RDT5: Per-pod convergence and cluster diagnostics

### Scope

Verify restarted and non-restarted pods converge, not only the service endpoint.

### Tasks

1. Port-forward to each pod and create direct per-pod client endpoints.
2. Run committed read count through every pod after recovery.
3. Query existing admin/cluster diagnostic APIs for:
   - cluster ID;
   - node identity;
   - raft group health/leader state if available;
   - partition readiness/read-index status if available.
4. Fail if counts diverge or cluster IDs differ.
5. Save per-pod diagnostic JSON artifacts.

### Tests

- Unit: per-pod assertion reports all mismatched nodes.
- Unit: missing diagnostic API degrades to graph-count assertions with a warning,
  not a false pass.
- Manual/destructive: per-pod smoke disruption passes.

### Acceptance

```sh
go test ./internal/clustering/disrupttest -count=1
make test-k3s-raft-disruption-smoke
git diff --check
```

## RDT6: Iterate pods and pressure profiles

### Scope

Scale from restarting one pod to a small matrix of scenarios and pressure levels.

### Tasks

1. Add `--restart-node` support:
   - specific pod name;
   - ordinal;
   - `all` one at a time.
2. Add named profiles:
   - `smoke`;
   - `small`;
   - `medium`;
   - `soak`.
3. Add JSON scenario config support for future scenario definitions.
4. Add summary output:
   - attempted writes;
   - successful writes;
   - transient failures;
   - permanent failures;
   - final counts;
   - recovery duration.

### Tests

- Unit: profile values are deterministic and bounded.
- Unit: `all` restart sequence restarts each pod once and waits between pods.
- Manual/destructive: small profile passes.

### Acceptance

```sh
go test ./internal/clustering/disrupttest -count=1
make test-k3s-raft-disruption
git diff --check
```

## RDT7: Richer data shapes and partition coverage

### Scope

Add workloads that more closely resemble application writes without relying on
app code.

### Tasks

1. Add edge workload:
   - create parent node;
   - create child nodes;
   - create ordered edges.
2. Add transaction-batch workload when transaction API helpers are stable.
3. Add multi-space workload to touch multiple partition groups.
4. Add mixed committed reads during write pressure.
5. Integrate forensic/checksum consistency diagnostics where available.

### Tests

- Unit: successful edge write verification tracks both node and edge counts.
- Manual/destructive: edge workload survives one pod restart.
- Manual/destructive: multi-space workload survives one pod restart.

### Acceptance

```sh
go test ./internal/clustering/disrupttest -count=1
go run ./cmd/mycel-raft-disrupttest --driver k3s --provisioner k3d --profile small --workload edges --confirm-destructive
git diff --check
```

## RDT8: Documentation, CI hooks, and release gate integration

### Scope

Document how to run the harness, how to interpret results, and how to use it as
an explicit release/soak gate.

### Tasks

1. Add operator procedure under `docs/operations/procedures/`.
2. Cross-link from clustering design docs.
3. Document prerequisites:
   - `kubectl`;
   - `k3d`;
   - Docker/container runtime;
   - local image build/load behavior.
4. Document artifact layout and common failure signatures.
5. Add optional CI job template or notes; do not make it mandatory for normal PR
   validation.
6. Add release-gate recommendation for raft-sensitive changes.

### Tests

- Docs check.
- Make target help/listing if available.

### Acceptance

```sh
make docs-check
git diff --check
```

## Initial implementation recommendation

Implement RDT0-RDT4 first. That delivers a useful smoke disruption gate:

```sh
make test-k3s-raft-disruption-smoke
```

Then add RDT5 before trusting the harness as a convergence signal, because a
service-level count alone can hide per-pod lag or divergence.

## Risks and mitigations

- **Slow/flaky local provisioning:** keep `smoke` short and collect artifacts on
  every failure.
- **Host environment differences:** preflight versions and print all resolved
  tools/config.
- **Image mismatch:** require explicit image flag or build/load step and record
  image digest/tag in artifacts.
- **False positives during disruption:** classify transient errors separately and
  only assert successful writes.
- **False confidence from service-only reads:** add per-pod convergence in RDT5.
- **Leaked test clusters:** finalization deletes by generated cluster name;
  retained clusters require explicit flag.

## Validation summary for each tranche

Normal code validation:

```sh
go test ./internal/clustering/disrupttest -count=1
git diff --check
```

Docs-affecting tranches:

```sh
make docs-check
git diff --check
```

Destructive validation, explicitly requested only:

```sh
make test-k3s-raft-disruption-smoke
make test-k3s-raft-disruption
```
