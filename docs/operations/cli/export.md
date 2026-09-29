# `mycel export`

Export Mycel data through daemon gRPC.

Authentication mode: **user**.

## Common tasks

- Export domain content from a readable transaction.
- Create a first-class space data export ZIP for the authenticated user.
- Include blobs when requested.

## Domain export

```sh
mycel export domain --transaction-id <tx-id> --file domain.json --include-blobs
```

`export domain` writes a Mycel stream JSON document for one readable graph
transaction. It is intended for graph/domain portability and import workflows.

## Space data export

```sh
mycel export space --space-id <space-id> --file mycel-space-export.zip --include-blobs
```

`export space` logs in as the requesting user, creates a daemon-managed export
job for the requested space, waits for completion, downloads the completed ZIP
artifact, and writes it to `--file`. It does not require admin impersonation and
exports only domains the caller can read. Repeat `--domain-id` to export a subset
of domains; omit it to export all visible non-system domains in the space.

For applications or operators that want explicit job control, use the subcommands:

```sh
mycel export space create --space-id <space-id> --domain-id <domain-a> --domain-id <domain-b>
mycel export space status <export-id>
mycel export space list --space-id <space-id>
mycel export space download <export-id> --file mycel-space-export.zip
mycel export space delete <export-id>
```

Jobs expose queued/running/succeeded/failed/deleted/expired status, progress,
artifact size, counts, and an expiry timestamp. Completed artifacts are streamed
through the client API and are retained by the daemon for a bounded window until
explicit deletion or expiry.

ZIP layout:

```text
manifest.json
README.md
export.json
spaces/<space-id>/space.json
spaces/<space-id>/domains/<domain-id>/domain.json
spaces/<space-id>/domains/<domain-id>/nodes.jsonl
spaces/<space-id>/domains/<domain-id>/edges.jsonl
spaces/<space-id>/domains/<domain-id>/schema.gwl        # when configured
spaces/<space-id>/domains/<domain-id>/blobs/manifest.jsonl
spaces/<space-id>/blobs/files/<blob-id>                 # when --include-blobs
```

`manifest.json` records the format version (`mycel-space-export-v1`), requester
principal, requested space/domain options, included space/domains, counts, and SHA-256 checksums
for payload files. `nodes.jsonl`, `edges.jsonl`, and blob manifests are newline-delimited JSON so tools can process large exports incrementally.

Intentionally omitted from space exports: daemon secrets, credentials,
plaintext passwords, WAL/Raft logs, local indexes, checkpoints, runtime caches,
and derived semantic/vector index payloads. Blob bytes are included by default;
pass `--include-blobs=false` to export graph metadata without raw blob files.

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
