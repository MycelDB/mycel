# Semantic Storage

Low-level semantic and embedding storage is documented in:

```text
docs/storage/semantic.md
```

That storage document defines:

- current embedding metadata files
- current `.kvec` append-only block structure
- proposed inference/model-endpoint/model/capability/vector-store JSON structures
- proposed credential, grant, and policy JSON structures
- proposed semantic index, index state, dirty queue, and policy decision JSON structures
- proposed per-index `.kvec` vector record structure
- recovery and compaction behavior

This `docs/semantic/` directory should focus on architecture and behavior. It should not duplicate filesystem/block-format details from `docs/storage/semantic.md`.
