# `mycel graph`

Manage graph nodes and edges through daemon transactions.

Authentication mode: **user**.

## Common tasks

- Create/get/list/update/delete nodes.
- Create/get/list/delete edges.
- Create blob-backed nodes.
- Run graph write benchmark/smoke workloads against a live daemon.

## Graph write benchmark

Use `mycel graph benchmark writes` to measure graph create/update write latency through the normal daemon APIs. The command can seed a domain to a target graph size, then run one-operation-per-transaction, multi-operation transactions, or `ApplyGraphOperations` batches.

Useful flags:

- `--space-id`: target space ID.
- `--domain-id` / `--domain`: target domain by ID or key.
- `--graph-size`: `small`, `medium`, `large`, or `custom`.
- `--seed-nodes`: explicit target seed count; overrides `--graph-size` when positive.
- `--operation`: `create`, `update`, `create-edge`, or `mixed`.
- `--template`: `minimal`, `properties`, `content`, `commonfolio-session`, or `audit`.
- `--operations`: measured operation count.
- `--batch-size`: operations per transaction.
- `--apply-operations`: use the `ApplyGraphOperations` RPC for create/update batches.
- `--cleanup`: delete nodes created by the measured create/create-edge workload after measurement.

The command reports latency summaries for transaction begin, graph operation RPCs, transaction commit, and total transaction duration. Use `--output json` to capture machine-readable benchmark artifacts.

## Examples

```sh
mycel graph node create --transaction-id <tx-id> --content "hello"
```

```sh
mycel graph blob-node create file.txt --transaction-id <tx-id>
```

```sh
mycel graph benchmark writes \
  --space-id <space-id> \
  --domain registration \
  --graph-size small \
  --operation create \
  --template commonfolio-session \
  --operations 50 \
  --batch-size 1
```

```sh
mycel graph benchmark writes \
  --space-id <space-id> \
  --domain registration \
  --seed-nodes 1000 \
  --operation update \
  --template commonfolio-session \
  --operations 200 \
  --batch-size 20 \
  --apply-operations \
  --output json
```

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
