# Lexical search operations

Lexical search provides BM25-ranked full-text lookup for user-authored string values in graph node payloads and properties. Indexes are scoped to one `space_id` and `domain_id`.

## API surface

Client API:

- `mycel.client.v1.SearchService/Search`
- `mycel.client.v1.SearchService/GetLexicalIndexStatus`

Admin API:

- `mycel.admin.v1.AdminLexicalMaintenanceService/RebuildLexicalIndex`

Search responses return node IDs and scoring/freshness metadata. They do not return full node payloads by default; fetch selected nodes through graph APIs.

## Operational model

- Index files are derived local daemon state under `search/lexical/<space-id>/<domain-id>/` in the daemon data directory.
- Graph data remains authoritative. Missing, corrupt, or incompatible index files should be handled by rebuild rather than graph data recovery.
- Full source text is not stored in the lexical index, but index files contain token/posting derivatives of user-authored text and must be treated as sensitive local data.
- Backups do not need to include lexical index files. Restored data can rebuild indexes from graph state.
- Automatic compaction is deferred in v1. Tombstones are retained until a rebuild or future compaction workflow rewrites the physical index.

## Check status

```sh
mycel search lexical status --space-id <space-id> --domain default
```

Key fields:

| Field | Meaning |
| --- | --- |
| `state` | Ready/building/stale/unavailable/error status. |
| `indexed_graph_revision` | Highest graph revision reflected in the index. |
| `latest_graph_revision` | Latest graph revision known to the daemon for the scope. |
| `revision_lag` | Difference between latest known and indexed revisions. |
| `segments` | Number of immutable physical index segments. |
| `live_documents` / `deleted_documents` | Current document and tombstone counts. |

## Search

```sh
mycel search lexical --space-id <space-id> --domain default 'raft AND snapshot'
```

By default, stale indexes fail closed. Operators or applications that can tolerate staleness may opt in:

```sh
mycel search lexical \
  --space-id <space-id> \
  --domain default \
  --allow-stale \
  --max-revision-lag 100 \
  'raft snapshot'
```

## Rebuild

Use rebuild when status reports missing/corrupt/incompatible index files, when tombstone accumulation is excessive, or after an operator intentionally discards local derived search state.

```sh
mycel search lexical rebuild --space-id <space-id> --domain default
```

Dry-run checks authorization and scope without rewriting files:

```sh
mycel search lexical rebuild --space-id <space-id> --domain default --dry-run
```

Use `--force` when rebuilding an already-fresh index is intentional.

## Query syntax and limitations

Supported v1 syntax:

- terms and quoted phrases;
- `AND`, `OR`, `NOT`;
- unary `-` exclusions;
- parentheses;
- implicit `AND` between adjacent terms/phrases/groups.

Not supported in v1:

- stemming/language-specific analyzers;
- wildcard, prefix, fuzzy, range, and fielded Lucene clauses;
- blob text extraction;
- result snippets/highlighting;
- hybrid lexical/semantic/metadata reranking.

## Clustered mode

The authoritative owner for the space/domain builds the lexical index and advances progress. Followers forward Search API requests to that owner. If a safe route cannot be determined, requests return unavailable/routing errors rather than serving from a stale follower-owned index.

## Troubleshooting

| Symptom | Action |
| --- | --- |
| Search returns unavailable/building | Check status; wait for catch-up or rebuild if stuck. |
| Search returns stale-index error | Retry later, or use `--allow-stale` only if stale reads are acceptable. |
| Invalid query syntax | Remove unsupported wildcard/fuzzy/range/fielded syntax. |
| Unexpected empty results | Verify the domain is searchable, node text is in payload/properties, and status revision has caught up. |
| Large deleted-document count | Schedule a rebuild to rewrite physical segments without tombstones. |

## Related docs

- [Search CLI reference](../cli/search.md)
- [Lexical search design](../../design/search/lexical-search.md)
- [Backup and restore](backup-restore.md)
