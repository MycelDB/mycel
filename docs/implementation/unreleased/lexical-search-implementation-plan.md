# Lexical Search Implementation Plan

## Status

LS0-LS2 complete on the `lexical_search` coordination branches. Do not start LS3+ daemon implementation until the design document and implementation plan are reviewed.

Design source: [Lexical search design](../../design/search/lexical-search.md)

## Summary

Implement lexical search as a first-class MycelDB search mechanism, peer to semantic search and metadata/tag/property search.

The first release tranche adds:

- a dedicated Search API;
- per-space/domain lexical indexes;
- user-authored string indexing from node payloads/properties;
- Lucene-style query syntax subset;
- BM25-style ranking;
- phrase search, boolean operators, and grouping;
- leader/master-owned indexing in clustered mode;
- eventually-consistent freshness diagnostics;
- Mycel-native immutable segment files;
- CLI and documentation surfaces;
- SDK regeneration and basic client helpers where appropriate.

Hybrid reranking across lexical, semantic, and metadata signals is planned as a later layer and must not block lexical search v1.

## Approved product/design decisions

- Feature name: **lexical search**.
- General MycelDB feature, not Knot-PKM-specific.
- Peer mechanism to semantic/vector search and metadata/tag/property search.
- Dedicated Search API first, not only GQL/structured-query integration.
- Per `space_id + domain_id` index scope.
- Index all user-authored strings in node payloads/properties.
- Do not index system metadata/internal bookkeeping in v1.
- BM25-style ranking.
- Lucene-style query subset in v1.
- Phrase search, `AND`/`OR`/`NOT`, unary `-`, and grouping in v1.
- No stemming in v1.
- Unicode-aware/simple language-neutral analyzer in v1.
- No fuzzy/wildcard/prefix search in v1.
- No result snippets/highlights in v1.
- No blob text extraction in v1.
- Slightly stale results are acceptable when freshness is reported.
- Indexing behavior should mirror semantic indexing at the consistency level: asynchronous/eventual derived state from committed graph changes.
- In clustered mode, indexing is done by the authoritative leader/master for the domain/space scope.
- Followers forward lexical search requests to the current owner or fail closed.
- Physical index files are local derived state and are not Raft-replicated.

## Repository scope

Primary implementation repo:

- `mycel`

Coordinated downstream repos:

- `mycel-api` — protobuf Search API definitions.
- `mycel-go-sdk` — regenerated bindings and optional convenience helpers.
- `mycel-rust-sdk` — regenerated bindings and optional convenience helpers.
- `mycel-console` — search UI/diagnostics after the backend API exists.

Long-lived coordination branches already exist:

```text
lexical_search
```

in each release repo.

## Proposed package layout

Daemon packages in `mycel`:

```text
internal/search/lexical/analyzer
internal/search/lexical/parser
internal/search/lexical/query
internal/search/lexical/index
internal/search/lexical/storage
internal/search/lexical/service
internal/search/service
internal/daemon/api/client/search_service.go
```

Possible responsibilities:

| Package | Responsibility |
| --- | --- |
| `analyzer` | Unicode-aware tokenization, normalization, positions, analyzer version. |
| `parser` | Lucene-style syntax parser and parse diagnostics. |
| `query` | Logical query AST, validation, rewrite/normalization. |
| `storage` | Segment files, manifest/cursor IO, checksums, atomic publish, compaction hooks. |
| `index` | Writer/searcher interfaces, posting-list iteration, BM25 scoring. |
| `lexical/service` | Index maintenance workers, rebuild, search orchestration for one scope. |
| `search/service` | Higher-level Search API manager and future hybrid-search seam. |
| daemon API adapter | Auth, protobuf mapping, routing/forwarding, gRPC errors. |

Package names can change during implementation if a simpler layout emerges, but keep analyzer/parser/index/storage independently testable.

## Data and file layout

Use the approved Mycel-native immutable segment structure:

```text
<MYCELD_DATA_DIR>/search/lexical/<space-id>/<domain-id>/
├── manifest.json
├── cursor.json
├── segments/
│   ├── seg_000001/
│   │   ├── segment.json
│   │   ├── terms.idx
│   │   ├── postings.bin
│   │   ├── docs.bin
│   │   └── checksum.sha256
│   └── seg_000002/
└── locks/
```

Implementation requirements:

- `manifest.json`, `cursor.json`, and `segment.json` are JSON for diagnostics and safe inspection.
- `terms.idx`, `postings.bin`, and `docs.bin` are binary and versioned.
- Posting lists are varint/delta encoded.
- Positions are stored so phrase search works in v1.
- Segments are immutable after publication.
- Segment publication is atomic through temporary directory/write/fsync/checksum/manifest rename where practical.
- Missing/corrupt physical files trigger rebuild/unavailable behavior, not graph data loss.
- Full source text is not stored in the lexical index.

## API shape plan

Add a dedicated Search API in `mycel-api`, likely under:

```text
api/proto/mycel/client/v1/search.proto
```

Initial services:

```proto
service SearchService {
  rpc Search(SearchRequest) returns (SearchResponse);
  rpc GetLexicalIndexStatus(GetLexicalIndexStatusRequest) returns (GetLexicalIndexStatusResponse);
}

service AdminLexicalMaintenanceService {
  rpc RebuildLexicalIndex(RebuildLexicalIndexRequest) returns (RebuildLexicalIndexResponse);
}
```

V1 Search request fields should include:

- `space_id`;
- `domain_id`;
- `mode`, initially `LEXICAL`;
- query text;
- page size/page token;
- stale-read policy, e.g. `allow_stale` and optional maximum revision lag;
- diagnostics flag;
- reserved filter message for future metadata constraints/hybrid search.

V1 Search response fields should include:

- ordered results;
- node IDs, space/domain IDs;
- lexical score;
- matched terms/fields diagnostics where practical;
- page token;
- index freshness;
- warnings;
- query diagnostics.

Avoid returning full node payloads by default. Search returns candidates; callers can read current node state through graph APIs.

## Raft/distributed execution plan

### Ownership

In clustered mode, the authoritative space/domain leader/master owns lexical indexing progress.

The owner:

- consumes committed graph changes;
- writes local segment files;
- advances durable lexical cursor/progress;
- serves Search API requests for owned scopes;
- resumes/catches up/rebuilds after restart or leadership change.

Followers:

- do not advance authoritative lexical indexing progress in v1;
- forward requests to the owner using authenticated backend routing;
- fail closed with clear unavailable/routing diagnostics when owner is unknown.

### Progress/cursor

Implementation must decide the exact cursor persistence mechanism, but it must be cluster-safe:

- standalone: local durable cursor is sufficient;
- clustered: cursor/progress must respect ownership/fencing and not allow stale followers to publish progress.

Candidate approaches:

1. WAL/Raft-owned lexical cursor records per space/domain.
2. Owner-local cursor plus leadership fencing, with full rebuild/catch-up after owner transition.

Prefer the safer Raft-owned cursor if implementation complexity is acceptable. If owner-local cursor is used for v1, document failover/rebuild behavior explicitly and add tests proving correctness.

### Request forwarding

Add backend forwarding support for lexical search requests, similar to existing routed client operations. Requirements:

- authenticated backend call;
- cluster ID validation;
- requester identity metadata;
- route by space/domain owner;
- fail closed when route is unknown or unsafe;
- preserve Search API diagnostics.

## Phased implementation

### LS0 — Finalize API/design details

Status: complete for the initial implementation tranche.

Decisions:

1. Client search API: `mycel.client.v1.SearchService` in `mycel/client/v1/search.proto`.
2. Admin maintenance API: `mycel.admin.v1.AdminLexicalMaintenanceService` in `mycel/admin/v1/lexical_maintenance.proto`.
3. Default implicit query operator: `AND`.
4. Stale results are opt-in with `allow_stale`; callers may also set `max_revision_lag`.
5. Client-facing status is available through `GetLexicalIndexStatus`; manual rebuild is admin-only through `RebuildLexicalIndex`.
6. Clustered cursor/progress is Raft/WAL-owned per space/domain. Local `cursor.json` remains diagnostic/cache state only.
7. BM25 defaults are `k1=1.2` and `b=0.75`; phrase matches receive a deterministic `1.25` multiplier boost.
8. Logical/export-style backups do not need to include derived lexical index files; restored data dirs validate and rebuild indexes when needed.
9. Automatic compaction is deferred; tombstones plus manual rebuild are the v1 cleanup path.

Acceptance:

- API/design decisions are documented before coding begins.

### LS1 — Search API protobufs

Status: complete for the initial implementation tranche.

Repo: `mycel-api`.

Tasks:

1. Add `search.proto` with Search service/messages/enums.
2. Include lexical freshness, diagnostics, and result score messages.
3. Add status/rebuild APIs in the selected client/admin surface.
4. Update buf generation config if needed.
5. Validate protobuf lint/build.

Acceptance:

- `mycel-api` CI passes.
- Search API protobufs are reviewable and versioned.

### LS2 — Regenerate daemon/SDK bindings

Status: complete for the initial implementation tranche.

Repos: `mycel`, `mycel-go-sdk`, `mycel-rust-sdk`.

Tasks:

1. Regenerate protobuf bindings from `mycel-api/lexical_search`.
2. Commit generated code where each repo policy requires it.
3. Add minimal SDK helper stubs only if useful and low-risk.
4. Keep public helper semantics thin until backend behavior is stable.

Acceptance:

- API/daemon/SDK generation is reproducible.
- SDK tests/builds pass.

### LS3 — Analyzer and document extraction

Status: complete for the initial implementation tranche.

Repo: `mycel`.

Tasks:

1. Implement Unicode-aware tokenizer.
2. Implement lowercase/case-fold normalization.
3. Preserve token positions.
4. Extract strings from node payloads and properties with deterministic traversal.
5. Exclude node metadata/system fields.
6. Record analyzer version and field paths.

Tests:

- Unicode/case/punctuation determinism.
- Nested payload/property traversal.
- Metadata exclusion.
- Position tracking.

Acceptance:

- Analyzer output is deterministic across platforms.

### LS4 — Lucene-style query parser

Status: complete for the initial implementation tranche.

Repo: `mycel`.

Tasks:

1. Implement v1 grammar for terms, phrases, `AND`, `OR`, `NOT`, unary `-`, and grouping.
2. Apply default implicit `AND` operator.
3. Return structured parse errors for unsupported wildcard/fuzzy/range/fielded syntax.
4. Normalize parsed terms through the analyzer.
5. Add query AST validation and rewrite helpers.

Tests:

- Parser success cases.
- Operator precedence/grouping.
- Phrase parsing.
- Unsupported syntax diagnostics.
- Invalid syntax errors.

Acceptance:

- Parser supports the approved v1 syntax and rejects unsupported Lucene forms clearly.

### LS5 — Segment storage engine

Status: complete for the initial implementation tranche.

Repo: `mycel`.

Tasks:

1. Implement manifest/cursor/segment JSON structs and atomic IO.
2. Implement binary `terms.idx`, `postings.bin`, and `docs.bin` readers/writers.
3. Implement checksums and version validation.
4. Implement immutable segment creation and publication.
5. Implement deleted-document/tombstone representation.
6. Add basic segment merge/compaction interfaces, even if automatic compaction is delayed.

Tests:

- Round-trip segment write/read.
- Atomic publish crash-simulation where practical.
- Checksum/version mismatch handling.
- Missing/corrupt index recovery paths.
- Tombstone semantics.

Acceptance:

- Segment storage can index and read a deterministic batch of documents.

### LS6 — Index writer and BM25 searcher

Status: complete for the initial implementation tranche.

Repo: `mycel`.

Tasks:

1. Build inverted-index writer from analyzed documents.
2. Implement posting-list iterators.
3. Implement BM25 scoring.
4. Implement phrase matching using positions.
5. Implement boolean query evaluation.
6. Implement stable tie-break ordering.
7. Implement pagination cursor format.

Tests:

- BM25 ranking known cases.
- Phrase match/non-match cases.
- Boolean query behavior.
- Deleted document filtering.
- Stable pagination/tie ordering.

Acceptance:

- Searcher returns deterministic ranked node IDs for segment-backed indexes.

### LS7 — Index maintenance service

Status: complete for the initial implementation tranche.

Repo: `mycel`.

Tasks:

1. Add lexical search module/service lifecycle.
2. Register graph-change consumer/replay source.
3. Implement per-space/domain indexing queue and cursor.
4. Implement initial backfill/rebuild from graph state.
5. Implement update/delete handling from graph changes.
6. Implement freshness status.
7. Implement startup recovery for missing/corrupt/incompatible indexes.
8. Integrate quiesce/backup behavior.

Tests:

- Create/update/delete graph changes update index.
- Rebuild produces equivalent query results.
- Cursor/freshness advances correctly.
- Missing/corrupt index triggers rebuild/unavailable behavior.
- Quiesce prevents unsafe mutation during backup as required.

Acceptance:

- Standalone daemon can build/update/search per-domain lexical indexes eventually.

### LS8 — Cluster ownership and forwarding

Status: complete for the initial ownership/forwarding seam tranche. Daemon backend/API adapter wiring remains in LS9.

Repo: `mycel`.

Tasks:

1. Wire lexical indexing to space/domain leader/master ownership.
2. Fence indexing work on leadership loss.
3. Resume/catch up/rebuild on leadership gain.
4. Add backend request forwarding for Search API calls.
5. Ensure followers do not advance authoritative progress.
6. Add fail-closed behavior when owner route is unavailable.

Tests:

- Leader indexes and serves search.
- Follower forwards to leader.
- Unknown owner fails closed.
- Leadership change stops old owner indexing and starts new owner catch-up.
- Stale/corrupt new-owner index rebuilds before serving authoritative results.
- Raft restart/snapshot scenarios do not corrupt index progress.

Acceptance:

- Clustered lexical search follows MycelDB’s fail-closed raft ownership model.

### LS9 — Daemon Search API adapter

Status: complete for the initial implementation tranche.

Repo: `mycel`.

Tasks:

1. Implement gRPC Search API service in daemon client API layer.
2. Enforce principal authentication and domain read authorization.
3. Map parser/index/freshness errors to appropriate gRPC status codes.
4. Map internal results to protobuf response messages.
5. Expose index status/rebuild API in the selected client/admin service.

Tests:

- Auth required.
- Unauthorized domain returns permission error without leakage.
- Valid lexical query returns ranked results.
- Invalid syntax returns `InvalidArgument`.
- Rebuilding/unavailable maps correctly.
- Pagination and diagnostics mapping.

Acceptance:

- Search API is usable end-to-end in standalone mode and routed correctly in clustered mode.

### LS10 — CLI support

Status: complete for the initial implementation tranche.

Repo: `mycel`.

Tasks:

1. Add `mycel search lexical` or equivalent command.
2. Support `--space-id`, `--domain-id`, query text, page size/token, stale policy, and diagnostics output.
3. Add index status/rebuild commands if exposed.
4. Provide text and JSON output.

Example target UX:

```sh
mycel search lexical --space-id <space> --domain-id <domain> '"vector database" AND raft'
mycel search lexical status --space-id <space> --domain-id <domain>
```

Tests:

- CLI request construction.
- Text/JSON output.
- Error output for invalid syntax/unavailable index.

Acceptance:

- Operators/developers can exercise lexical search without writing code.

### LS11 — Console support

Status: complete for the initial implementation tranche.

Repo: `mycel-console`.

Tasks:

1. Add SDK binding usage after Rust SDK exposes Search API.
2. Add lexical search UI in the appropriate space/domain context.
3. Show ranked results, scores, freshness, and warnings.
4. Support query text, pagination, and diagnostics toggle.
5. Clearly distinguish lexical search from semantic search.

Tests:

- Tauri command mapping.
- Search page/component rendering.
- Freshness/warning display.
- Error classification.

Acceptance:

- Console can run lexical searches for a selected space/domain and display freshness diagnostics.

### LS12 — Documentation and operations

Status: complete for the initial implementation tranche.

Repos: primarily `mycel`; console docs as needed.

Tasks:

1. Update design docs if implementation differs.
2. Add Search API docs.
3. Add CLI examples.
4. Add operations docs for index status, rebuild, backup/restore, and troubleshooting.
5. Document query syntax and v1 unsupported syntax.
6. Document consistency/freshness semantics.
7. Document disk usage and compaction/rebuild expectations.

Acceptance:

- Users understand lexical search syntax, staleness, and operational behavior.

### LS13 — Release validation

Status: complete for the initial implementation tranche. Validation evidence is captured in [LS13-LS14 validation and handoff report](lexical-search-ls13-ls14-validation-handoff-report.md).

Tasks:

1. Run default unit/integration tests.
2. Run clustered raft-sensitive tests that cover indexing ownership/failover.
3. Run API/SDK generation checks.
4. Run CLI smoke tests.
5. Run Console tests/build if UI is included in the release tranche.
6. Run docs checks.
7. Add release notes.

Suggested commands:

```sh
# mycel-api
buf lint
buf generate

# mycel
go test ./...
python3 scripts/checkDocs.py
git diff --check

# mycel-go-sdk
go test ./...

# mycel-rust-sdk
cargo test --workspace

# mycel-console
npm test -- --runInBand
npm run build
cargo test --manifest-path src-tauri/Cargo.toml
```

Acceptance:

- All changed repos pass required CI.
- Release notes document feature scope and limitations.

### LS14 — Cross-repo release handoff

Status: complete for the initial implementation tranche. Handoff details are captured in [LS13-LS14 validation and handoff report](lexical-search-ls13-ls14-validation-handoff-report.md).

Tasks:

1. Record coordinated release-note summaries across `mycel`, `mycel-api`, SDKs, and Console.
2. Record dependency ordering for PRs and final merge/release gates.
3. Document compatibility, migration, limitations, and residual risks.
4. Verify the Rust SDK API submodule pointer matches the lexical API contract used for generation.

Acceptance:

- Maintainers have a concise handoff artifact for PR review, release notes, and final gate reruns.

## Security and privacy checklist

- Search authorization must match graph read authorization.
- Per-space/domain indexes must not leak cross-space/domain terms, counts, or diagnostics.
- Errors must not expose unauthorized node IDs or term statistics.
- Index files contain user-authored text derivatives and must be treated as sensitive data.
- Backups/restores must handle index files consistently with sensitive local daemon data.
- Rebuild/status APIs must require appropriate capabilities.

## Compatibility and migration

- Existing data dirs start without lexical indexes.
- First startup after enabling lexical search should build indexes asynchronously or on demand.
- Search requests against unbuilt indexes return rebuilding/unavailable diagnostics unless stale results exist and are explicitly allowed.
- Analyzer/index format versions must trigger rebuilds when incompatible.
- No public Blob/Graph/Query behavior should change for callers not using the Search API.

## Risks

| Risk | Mitigation |
| --- | --- |
| Large indexes or slow indexing | Immutable segment batching, diagnostics, compaction hooks, backpressure. |
| Raft ownership bugs | Leader-only indexing, follower forwarding, fail-closed routing, explicit failover tests. |
| Stale search surprises | Always return freshness metadata and warnings. |
| Query parser complexity | Deliberately small v1 Lucene subset with clear unsupported syntax errors. |
| Authorization leakage | Per-space/domain indexes plus API authorization and diagnostic redaction tests. |
| Rebuild time after restore | Status APIs, async rebuild, optional future physical-index backup inclusion. |

## Definition of done for v1

- Dedicated Search API supports lexical search for a space/domain.
- Lexical indexes all user-authored string payload/property values for graph nodes.
- Query syntax supports terms, phrases, boolean operators, unary minus, and grouping.
- Results use BM25-style ranking with deterministic tie ordering.
- Search responses include freshness diagnostics.
- Standalone indexing/search works end-to-end.
- Clustered mode uses leader/master-owned indexing and follower forwarding/fail-closed behavior.
- Index files use the approved manifest/cursor/immutable-segment structure.
- CLI can run lexical searches and inspect status.
- Docs cover query syntax, consistency, operations, and limitations.
- Tests cover parser/analyzer/index/ranking/API/auth/raft/failover paths.
