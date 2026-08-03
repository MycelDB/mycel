# K3s Cluster Validation

K3s/k3d validation is destructive. It creates or resets local k3d/Kubernetes
resources for cluster release validation.

Run from the `mycel/` directory:

```sh
make test-k3s-cluster
```

This gate validates fresh bootstrap, shared cluster identity, data-plane
behavior, rolling restart, and one-PVC replacement/rejoin validation.

See [Raft cluster test matrix](raft-cluster-test-matrix.md) for the broader gate
sequence.
