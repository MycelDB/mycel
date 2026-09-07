# `mycel search`

Run general search APIs against graph content.

Authentication mode: **client/user** for search and status; **operator/admin** for rebuild.

## Lexical search

Lexical search is BM25-ranked full-text search over user-authored string values in graph node payloads and properties for one space/domain.

```sh
mycel search lexical \
  --space-id <space-id> \
  --domain default \
  '"vector database" AND raft'
```

Equivalent query flag form:

```sh
mycel search lexical --space-id <space-id> --domain default --query "raft log"
```

Useful flags:

| Flag | Purpose |
| --- | --- |
| `--page-size <n>` | Limit returned results. Defaults to `20`. |
| `--page-token <token>` | Continue from a previous response. |
| `--allow-stale` | Permit results from a stale index. By default stale results fail closed. |
| `--max-revision-lag <n>` | Maximum accepted index lag when stale reads are allowed. |
| `--diagnostics` | Include query/index diagnostics in JSON output. |
| `--output json` | Emit the raw Search API response for scripting. |

Text output prints score, node ID, indexed graph revision, warnings, next-page token, and freshness summary. Fetch current node content with `mycel graph node get` after selecting result IDs.

## Index status

```sh
mycel search lexical status --space-id <space-id> --domain default
mycel --output json search lexical status --space-id <space-id> --domain default
```

Status includes:

- lexical index state;
- indexed and latest-known graph revisions;
- revision lag;
- segment and live/deleted document counts.

## Rebuild

```sh
mycel search lexical rebuild --space-id <space-id> --domain default
mycel search lexical rebuild --space-id <space-id> --domain default --dry-run
mycel search lexical rebuild --space-id <space-id> --domain default --force
```

Rebuild is an admin maintenance operation and requires a principal with space maintenance authority.

## Query syntax

Supported v1 syntax:

- terms: `raft database`;
- phrases: `"vector database"`;
- boolean operators: `AND`, `OR`, `NOT`;
- unary exclusion: `-draft`;
- grouping: `(raft OR wal) AND snapshot`.

Implicit operators default to `AND`, so `raft snapshot` is treated as `raft AND snapshot`.

Unsupported v1 syntax returns an invalid-argument error instead of silently changing semantics:

- wildcard/prefix: `raft*`;
- fuzzy: `raft~`;
- ranges: `[a TO z]`;
- fielded clauses: `title:raft`.

## Freshness and consistency

Lexical indexes are derived state from committed graph changes and are eventually consistent. Responses include freshness metadata so callers can decide whether an index is acceptable for their workflow.

In clustered mode, the authoritative owner for a space/domain builds and serves the lexical index. Followers forward search/status requests to the current owner when routing is known. If ownership is unknown or unsafe, the daemon fails closed instead of serving potentially misleading results.

## Related docs

- [CLI index](README.md)
- [Lexical search operations](../procedures/lexical-search.md)
- [Lexical search design](../../design/search/lexical-search.md)
