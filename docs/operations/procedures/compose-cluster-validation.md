# Compose Cluster Validation

Docker Compose validation is destructive: it resets local Compose resources and
volumes used by the sibling Knot PKM development environment.

Run from the `mycel/` directory:

```sh
make test-compose-cluster
```

User-scoped backup/restore cluster validation:

```sh
make test-compose-user-backup-restore
```

These gates validate fresh bootstrap, cluster identity, health/readiness,
pod-to-pod graph writes and reads, restart behavior, and user-scoped
backup/restore into a wiped fresh cluster.
