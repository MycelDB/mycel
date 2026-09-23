# `mycel graph`

Manage graph nodes and edges through daemon transactions.

Authentication mode: **user**.

## Common tasks

- Create/get/list/update/delete nodes.
- Create/get/list/delete edges.
- Create blob-backed nodes.
- Run graph write benchmark/smoke workloads against a live daemon.

## Graph write benchmark

Use `mycel graph benchmark writes` to measure graph create/update write latency through the normal daemon APIs. The command can seed a domain to a target graph size, then run one-operation-per-transaction, multi-operation transactions, or `ApplyGraphOperations` batches. The `update-references` workload uses server-side `replace_references` operations so node updates and reference-edge reconciliation can be measured without client-side edge diffing.

Useful flags:

- `--space-id`: target space ID.
- `--domain-id` / `--domain`: target domain by ID or key.
- `--graph-size`: `small`, `medium`, `large`, or `custom`.
- `--seed-nodes`: explicit target seed count; overrides `--graph-size` when positive.
- `--operation`: `create`, `update`, `create-edge`, `create-references`, `update-references`, or `mixed`.
- `--template`: `minimal`, `properties`, `content`, `commonfolio-session`, `commonfolio-journal`, `commonfolio-journal-entry`, `commonfolio-project`, `commonfolio-task`, or `audit`.
- `--seed-template`: node shape used for the seed/reference pool; defaults to `--template`.
- `--references-per-node`: number of edges per measured node for relationship workloads.
- `--reference-labels`: comma-separated edge labels cycled by relationship workloads.
- `--operations`: measured operation count.
- `--batch-size`: operations per transaction.
- `--apply-operations`: use the `ApplyGraphOperations` RPC for create/update batches. `update-references` always uses `ApplyGraphOperations` with `replace_references` operations.
- `--cleanup`: delete nodes created by the measured create/create-edge workload after measurement.

The command reports latency summaries for transaction begin, graph operation RPCs, transaction commit, and total transaction duration. Relationship workloads also report prepared referenced-node counts and measured reference-edge counts. For `update-references`, measured reference edges are reconciled by daemon-side diffing grouped by reference label. Use `--output json` to capture machine-readable benchmark artifacts.

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

```sh
mycel graph benchmark writes \
  --space-id <space-id> \
  --domain registration \
  --seed-template commonfolio-journal \
  --template commonfolio-journal-entry \
  --seed-nodes 1000 \
  --operation create-references \
  --references-per-node 2 \
  --reference-labels belongs_to,references \
  --operations 100 \
  --batch-size 1 \
  --output json
```

```sh
mycel graph benchmark writes \
  --space-id <space-id> \
  --domain registration \
  --seed-template commonfolio-journal \
  --template commonfolio-journal-entry \
  --seed-nodes 1000 \
  --operation update-references \
  --references-per-node 2 \
  --reference-labels belongs_to,references \
  --operations 100 \
  --batch-size 1 \
  --output json
```

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
