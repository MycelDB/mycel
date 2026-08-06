# Mycel CLI Reference

The `mycel` CLI talks to a running `myceld` daemon over gRPC. Commands generally
use one of two authentication modes:

- **User auth** with `--username` / `--password` for normal client operations.
- **Operator auth** with `--username` / `--password` for admin/operator commands.

Common connection flags:

```sh
mycel --daemon-addr 127.0.0.1:9091 --username <name> --password <password> <command>
```

Use `--output json` for scripting.

## Top-level commands

| Command | Purpose |
| --- | --- |
| [admin](admin.md) | Operator/admin management surfaces. |
| [auth](auth.md) | Standard-user authentication and auth sessions. |
| [automation](automation.md) | Graph automation management. |
| [blob](blob.md) | Raw blob upload/download/metadata/delete. |
| [change-stream](change-stream.md) | Watch domain graph changes. |
| [cluster](cluster.md) | Cluster status, health, raft, consistency, and forensics. |
| [domain](domain.md) | Client domain management inside a space. |
| [export](export.md) | Domain export through readable transactions. |
| [graph](graph.md) | Graph node, edge, children, parent, and blob-node operations. |
| [import](import.md) | Domain import through read-write transactions. |
| [inference](inference.md) | Admin inference/model/vector/credential configuration. |
| [metadata](metadata.md) | Metadata catalog queries. |
| [node](node.md) | Alias for graph node operations. |
| [query](query.md) | Query nodes or execute GQL. |
| [schema](schema.md) | Domain schema get/put/delete/validate. |
| [semantic](semantic.md) | Semantic index/search/maintenance operations. |
| [session](session.md) | Graph session and transaction helper commands. |
| [space](space.md) | Space create/list/show/delete operations. |
| [transaction](transaction.md) | Transaction helper commands. |
| [user](user.md) | Operator user management. |

Future work: generate this reference from Cobra help output or gate it with a
CLI-doc presence check.
