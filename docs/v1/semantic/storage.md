# Semantic Storage

Low-level semantic and embedding storage is documented in:

```text
docs/storage/semantic.md
```

Inference accounting ledger paths are documented in:

```text
docs/storage/layout.md
docs/storage/meta.md
```

That storage document defines:

- current embedding metadata files
- current `.kvec` append-only block structure
- proposed inference/model-endpoint/model/capability/vector-store JSON structures
- proposed credential, grant, and policy JSON structures
- proposed semantic index, index state, dirty queue, graph dirty event, semantic config event, and policy decision structures
- proposed per-index `.kvec` vector record and tombstone/delete structure
- external vector deletion/verification status
- recovery and compaction behavior
- accounting ledger location and derived index/rollup locations

This `docs/semantic/` directory should focus on architecture and behavior. It should not duplicate filesystem/block-format details from `docs/storage/semantic.md`.
