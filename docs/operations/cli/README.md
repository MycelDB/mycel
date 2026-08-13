# Mycel CLI Reference

The `mycel` CLI talks to a running `myceld` daemon over gRPC. Commands authenticate as principals with `--username` / `--password`.

Data-plane commands require a principal with access to the target space/domain. Admin commands require a principal with the relevant system-management roles or capability grants.

Common connection flags:

```sh
mycel --daemon-addr 127.0.0.1:9091 --username <name> --password <password> <command>
```

Use `--output json` for scripting.

## Top-level commands

| Command | Purpose |
| --- | --- |
| [admin](admin.md) | Operator/admin management surfaces. |
| [auth](auth.md) | Principal authentication and auth sessions. |
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
| [principal](principal.md) | Principal identity, role, capability, and session management. |
| [query](query.md) | Query nodes or execute GQL. |
| [schema](schema.md) | Domain schema get/put/delete/validate. |
| [semantic](semantic.md) | Semantic index/search/maintenance operations. |
| [session](session.md) | Graph session and transaction helper commands. |
| [space](space.md) | Space create/list/show/delete operations. |
| [transaction](transaction.md) | Transaction helper commands. |
| [user](user.md) | Compatibility aliases for principal management. |

Future work: generate this reference from Cobra help output or gate it with a
CLI-doc presence check.
