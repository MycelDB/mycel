# K3s Cluster Validation

## Command

```sh
make test-k3s-cluster
```

Run from the `mycel/` directory.

## What it does

This destructive k3d/K3s test is now delegated to the Mycel Lab
`k3d-cluster-validation` suite for migrated coverage. The compatibility target
builds the current checkout as `myceldb/mycel:latest`, then runs the suite from
the sibling `mycel-lab` checkout (`MYCEL_LAB_ROOT`, default `../mycel-lab`).

The migrated suite creates or resets local disposable k3d/Kubernetes resources
and validates:

1. fresh bootstrap;
2. shared cluster identity;
3. health/readiness;
4. graph write/read/query behavior through driver-discovered endpoints;
5. rolling restart behavior;
6. data-plane revalidation after restart.

The legacy one-PVC replacement/rejoin step remains in `scripts/testK3sCluster.sh`
until Mycel Lab gains a dedicated volume-replacement capability.

## Parameters

The target executes `mycel-lab run suite k3d-cluster-validation
--confirm-destructive`. Common configuration is through Mycel Lab environment
selection and the Make variables below.

| Variable | Default | Meaning |
| --- | --- | --- |
| `MYCEL_LAB_ROOT` | `../mycel-lab` | Sibling Mycel Lab checkout used by the compatibility target. |

Required local tools:

```sh
kubectl version --client=true
k3d version
docker version
```

## How to interpret results

The test passes when the make target exits `0`.

Failure interpretation:

- k3d or kubectl preflight failure: fix local toolchain/Docker Desktop state;
- bootstrap/identity failure: investigate raft metadata initialization;
- readiness failure: inspect pod readiness logs and cluster readiness output;
- graph validation failure: inspect pod-specific query/write evidence;
- PVC replacement failure: use the legacy `scripts/testK3sCluster.sh` path until
  Mycel Lab volume-replacement support is implemented.

## Cleanup

The Mycel Lab suite is destructive and manages its own local k3d/Kubernetes
resources. If interrupted, list and delete retained clusters manually:

```sh
k3d cluster list
k3d cluster delete <cluster-name>
```
