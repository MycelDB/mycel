# K3s Cluster Validation

K3s/k3d validation is destructive. It creates or resets local k3d/Kubernetes
resources for cluster release validation.

Run the standard K3s cluster gate from the `mycel/` directory:

```sh
make test-k3s-cluster
```

This gate validates fresh bootstrap, shared cluster identity, data-plane
behavior, rolling restart, and one-PVC replacement/rejoin validation.

Run the full-system backup/restore K3s gate with:

```sh
make test-k3s-system-backup-restore
```

This release-gate validation creates graph/blob data, runs one coordinated
cluster system backup, requires raft freeze/checkpoint evidence in
`backup-set.json`, wipes the namespace including PVCs, restores each ordinal's
archive into fresh PVCs, restarts the StatefulSet, and verifies the restored
cluster health plus graph/blob data through every pod.

See [Raft cluster test matrix](raft-cluster-test-matrix.md) for the broader gate
sequence.
