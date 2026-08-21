# K3s Cluster Validation

## Command

```sh
make test-k3s-cluster
```

Run from the `mycel/` directory.

## What it does

This destructive k3d/K3s test validates the Kubernetes raft deployment path. It
creates or resets local k3d/Kubernetes resources and then validates:

1. fresh bootstrap;
2. shared cluster identity;
3. health/readiness;
4. real pod-to-pod graph write/read/query behavior;
5. rolling restart behavior;
6. one-PVC replacement/rejoin behavior;
7. data-plane revalidation after replacement.

## Parameters

The target executes `scripts/testK3sCluster.sh`. Common configuration is through
that script and Kubernetes/k3d environment variables.

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
- PVC replacement failure: investigate snapshot/rejoin/persistent identity
  behavior.

## Cleanup

The script is destructive and manages its own local k3d/Kubernetes resources. If
interrupted, list and delete retained clusters manually:

```sh
k3d cluster list
k3d cluster delete <cluster-name>
```
