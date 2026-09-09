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

## Hybrid search

Hybrid search combines lexical BM25 candidates and semantic/vector candidates into one fused, non-streaming result set. Metadata filters are hard eligibility filters: a node either satisfies the filter or is excluded; metadata does not change the score.

```sh
mycel search hybrid \
  --space-id <space-id> \
  --domain default \
  --lexical-weight 0.6 \
  --semantic-weight 0.4 \
  'raft recovery after pod restart'
```

Useful hybrid flags:

| Flag | Purpose |
| --- | --- |
| `--lexical-weight <n>` | Lexical contribution to fused rank. Defaults to `0.5`. |
| `--semantic-weight <n>` | Semantic contribution to fused rank. Defaults to `0.5`. |
| `--require-both` | Return only nodes found by both lexical and semantic retrieval. |
| `--lexical-candidates <n>` | Lexical candidates to fetch before filtering/fusion. Server defaults when unset. |
| `--semantic-candidates <n>` | Semantic candidates to fetch before filtering/fusion. Server defaults when unset. |
| `--semantic-rule-id <id>` | Restrict semantic retrieval to one searchable semantic rule. |
| `--embedding-binding-key <key>` | Restrict semantic retrieval to one binding; requires `--semantic-rule-id`. |
| `--semantic-min-score <n>` | Pass a minimum semantic score threshold to semantic retrieval. |
| `--label <label>` | Require a node label. Repeatable. |
| `--node-id <id>` | Restrict results to an allow-list of node IDs. Repeatable. |
| `--property-filter <spec>` | Require a property filter. Repeatable. Format: `path:operator:value[,value]`. |

Supported property filter operators are `equals`, `not-equals`, `in`, `contains`, and `exists`.

Example with hard metadata filters:

```sh
mycel search hybrid \
  --space-id <space-id> \
  --domain default \
  --label Note \
  --property-filter tags:contains:k3s \
  --property-filter status:equals:published \
  'raft recovery'
```

Hybrid scoring uses weighted reciprocal-rank fusion. The daemon normalizes non-negative weights, so `--lexical-weight 2 --semantic-weight 1` behaves like roughly `0.67 / 0.33`. Raw BM25 and vector scores are reported as source diagnostics when available, but the top-level score is the fused score.

Hybrid mode does not support `--page-token` in v1. Request the desired top-K with `--page-size`.

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
- [Hybrid search design](../../design/search/hybrid-search.md)
