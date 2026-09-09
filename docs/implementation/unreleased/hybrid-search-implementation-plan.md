# Hybrid Search Implementation Plan

## Status

Proposed plan for review. Implementation should not begin until the companion design document is approved.

Design source: [Hybrid search design](../../design/search/hybrid-search.md)

Tracking issue: [MycelDB/mycel#27](https://github.com/MycelDB/mycel/issues/27)

## Summary

Implement hybrid search as a non-streaming `SearchService.Search` mode that combines lexical full-text retrieval and semantic/vector retrieval into one fused result set. Metadata filters are hard eligibility constraints and do not contribute to ranking.

The first implementation should:

- extend the public Search API with `SEARCH_MODE_HYBRID`;
- add lexical, semantic, hybrid, and filter option messages;
- reuse the existing lexical search implementation for lexical candidates;
- reuse the existing semantic search implementation for semantic candidates;
- enforce metadata filters against graph node state;
- deduplicate candidates by `node_id`;
- rank results with weighted reciprocal-rank fusion;
- expose component scores and source diagnostics when requested;
- add CLI support for hybrid mode, weights, candidate counts, and filters;
- regenerate/update Go SDK, Rust SDK, and Console API bindings after `mycel-api` changes land.

## Approved design assumptions

- Hybrid search v1 is unary/non-streaming.
- Hybrid search belongs under `SearchService.Search`, not a separate service.
- `SEARCH_MODE_UNSPECIFIED` remains compatible with current lexical behavior.
- Hybrid ranking uses lexical and semantic signals only.
- Metadata, tag, label, and property criteria are hard filters only.
- Default weights are `0.5 lexical / 0.5 semantic`.
- User-provided non-zero weights are normalized by the server.
- V1 fusion strategy is weighted reciprocal-rank fusion.
- Raw BM25 and vector scores are not directly combined.
- Results are discovery candidates; graph reads remain authoritative for current state.

## Repository scope

Primary API repo:

- `mycel-api` — protobuf API additions.

Primary daemon/CLI repo:

- `mycel` — generated API stubs, search orchestration, filter evaluation, CLI, docs, tests.

Coordinated downstream repos:

- `mycel-go-sdk` — regenerated bindings and optional helper surface.
- `mycel-rust-sdk` — regenerated bindings and optional helper surface.
- `mycel-console` — generated bindings and UI integration if in scope for the first release tranche.

Recommended branch naming:

```text
feature/hybrid-lexical-semantic-search
```

for each repo that needs changes.

## Proposed tranche breakdown

### HS0 — Design and planning

Repo: `mycel`

Deliverables:

- Add `docs/design/search/hybrid-search.md`.
- Add this implementation plan.
- Link both documents from the relevant README indexes.
- Capture open product/API questions before code changes.

Validation:

- Documentation review.
- No generated-code or implementation changes.

Exit criteria:

- Design and implementation plan approved.

### HS1 — Public API shape

Repo: `mycel-api`

Deliverables:

- Extend `api/proto/mycel/client/v1/search.proto`:
  - add `SEARCH_MODE_HYBRID`;
  - add `SEARCH_SCORE_KIND_HYBRID_FUSED`;
  - add `HybridSearchOptions`;
  - add `HybridFusionStrategy`;
  - add `LexicalSearchOptions`;
  - add `SemanticSearchOptions`;
  - add structured `SearchFilters`;
  - add `PropertyFilter` and `FilterOperator`;
  - add `SearchResultSource` and `SearchResultSourceKind`.
- Preserve wire compatibility with existing fields.
- Avoid renumbering existing fields.
- Document default behavior and degradation expectations in proto comments.
- Update changelog/unreleased notes if the repo uses them for API changes.

Validation:

- `buf format` / repository proto formatting command.
- `buf lint` or repo CI equivalent.
- `buf breaking` if configured.

Exit criteria:

- API PR merged to `develop`.
- No generated SDK updates committed in `mycel-api` unless that repo already owns generated outputs.

### HS2 — Regenerate daemon API stubs

Repo: `mycel`

Deliverables:

- Update the `mycel-api` dependency/reference consumed by `mycel`.
- Regenerate daemon protobuf stubs under `internal/gen/` using the project script.
- Do not hand-edit generated files.
- Keep ignored/uncommitted generated daemon stubs consistent with project policy if applicable.
- Update any compile errors from the new enum/message definitions.

Validation:

- `go test ./internal/daemon/api/client ./internal/cli/cmd -count=1` or narrower compile checks as needed.
- `go test ./...` before PR completion.

Exit criteria:

- Daemon builds against the new Search API definitions.

### HS3 — Internal hybrid search domain model

Repo: `mycel`

Deliverables:

- Add a small internal orchestration package or extend existing search service code with types for:
  - hybrid request options;
  - normalized weights;
  - candidate records;
  - per-source rank/score diagnostics;
  - fusion result.
- Keep lexical and semantic service APIs decoupled from protobuf messages where practical.
- Add validation helpers:
  - page size and candidate count caps;
  - weight validation/normalization;
  - fusion strategy validation;
  - `require_both` validation.

Likely locations:

```text
internal/search/service
internal/search/hybrid
internal/daemon/api/client/search_service.go
```

The exact package can change during implementation if a simpler existing seam is available.

Validation:

- Unit tests for weight defaults and normalization.
- Unit tests for invalid requests.

Exit criteria:

- Internal request/fusion types are independently testable.

### HS4 — Weighted reciprocal-rank fusion

Repo: `mycel`

Deliverables:

- Implement weighted reciprocal-rank fusion:

```text
source_score = 1 / (rank_constant + source_rank)
fused_score = lexical_weight * lexical_source_score + semantic_weight * semantic_source_score
```

- Use a stable server-defined `rank_constant`, e.g. `60`.
- Treat missing source as `0` contribution.
- Deduplicate by `node_id`.
- Implement deterministic tie-breaks:
  1. fused score descending;
  2. candidates present in both sources before one-source candidates;
  3. best individual rank;
  4. `node_id` lexical order.
- Populate source diagnostics for lexical and semantic matches.

Validation:

- Unit tests for:
  - lexical-only candidates;
  - semantic-only candidates;
  - candidates present in both sources;
  - unequal weights;
  - zero lexical or semantic weight;
  - `require_both=true`;
  - deterministic ties.

Exit criteria:

- Fusion is deterministic and independent of backend result order except for explicit rank inputs.

### HS5 — Metadata hard filter evaluator

Repo: `mycel`

Deliverables:

- Implement metadata filter evaluation against graph node state.
- Start with conservative supported filters:
  - node labels;
  - explicit node IDs;
  - property `EXISTS`;
  - property `EQUALS`;
  - property `NOT_EQUALS`;
  - property `IN`;
  - property `CONTAINS` for arrays/lists and string containment only if semantics are clear.
- Avoid broad implicit type coercion in v1.
- Return structured invalid-argument errors for unsupported operators or ambiguous value shapes.
- Ensure filters are authorization-safe and do not leak hidden node existence through diagnostics.

Implementation options:

1. Reuse graph/query predicate evaluation code if a stable internal evaluator exists.
2. Add a small search-local evaluator for the first supported metadata subset.

Validation:

- Unit tests over representative node payload/property structures.
- Tests for missing properties, arrays, strings, multiple values, and invalid filters.
- Authorization-sensitive tests where practical.

Exit criteria:

- Metadata filters are applied before returned results are exposed.
- No metadata score or metadata boost exists in v1.

### HS6 — Search service orchestration

Repo: `mycel`

Deliverables:

- Extend `SearchService.Search` handling:
  - keep current lexical behavior for `UNSPECIFIED` and `LEXICAL`;
  - route `HYBRID` to the new orchestration path.
- Run lexical candidate retrieval with `LexicalSearchOptions.candidate_count` or server default.
- Run semantic candidate retrieval with `SemanticSearchOptions` and candidate count.
- Merge candidates, read graph nodes as needed, apply filters, fuse, sort, and return top-K.
- Include warnings from lexical and semantic paths.
- Map diagnostics into protobuf response fields.
- Keep response unary.

Important compatibility point:

- Existing clients that omit `mode` must see unchanged lexical behavior.

Validation:

- Service-level tests with fake lexical and semantic providers.
- Tests proving existing lexical API behavior is unchanged.
- Tests for semantic candidate failures and degraded warnings.

Exit criteria:

- `SEARCH_MODE_HYBRID` works through the gRPC Search API in standalone test fixtures.

### HS7 — Failure and degradation semantics

Repo: `mycel`

Deliverables:

- Define and implement concrete error handling for:
  - missing lexical index;
  - stale lexical index with `allow_stale=false`;
  - unavailable semantic rules/bindings;
  - semantic vector-store runtime failures;
  - both backends unavailable;
  - `require_both=true` with one backend unavailable.
- Prefer explicit warnings for safe partial degradation and errors for unsafe/ambiguous cases.
- Ensure gRPC status codes are stable and documented.

Suggested initial policy:

- lexical freshness violations continue to fail according to current lexical policy;
- no semantic searchable binding returns lexical-only results with a warning unless `require_both=true`;
- semantic runtime failure returns an error unless it is already classified as safely non-fatal;
- both backends unavailable returns an error.

Validation:

- Unit/integration tests for each error/degradation case.
- Confirm warnings do not include secrets or hidden metadata.

Exit criteria:

- Failure behavior is documented and covered by tests.

### HS8 — CLI support

Repo: `mycel`

Deliverables:

- Extend existing search CLI command with:
  - `--mode lexical|hybrid`;
  - `--lexical-weight`;
  - `--semantic-weight`;
  - `--lexical-candidates`;
  - `--semantic-candidates`;
  - `--semantic-rule-id`;
  - `--embedding-binding-key`;
  - `--semantic-min-score`;
  - label/property/node-id filter flags.
- Keep default CLI mode as lexical.
- Render fused score plus component diagnostics when `--diagnostics` is set.
- Keep output stable for scripts as much as possible.

Validation:

- CLI unit tests for flag parsing.
- CLI integration tests against daemon test fixtures where available.
- Golden output tests if the command uses existing golden patterns.

Exit criteria:

- Users can run a hybrid search from `mycel search` without using raw gRPC tooling.

### HS9 — Documentation and examples

Repos: `mycel`, possibly `mycel-api`

Deliverables:

- Update search operation docs:
  - API examples;
  - CLI examples;
  - scoring explanation;
  - metadata filter semantics;
  - freshness/degradation warnings;
  - limitations.
- Update release notes/changelog.
- Update design docs if implementation decisions differ from the proposal.

Likely files:

```text
docs/operations/cli/search.md
docs/operations/procedures/lexical-search.md
docs/design/search/hybrid-search.md
CHANGELOG.md
```

Validation:

- docs link check, if available.
- `python3 scripts/checkDocs.py` or project equivalent.

Exit criteria:

- Operator and user-facing docs describe how to use and interpret hybrid search.

### HS10 — SDK regeneration

Repos: `mycel-go-sdk`, `mycel-rust-sdk`

Deliverables:

- Update the API/proto dependency to include the hybrid search definitions.
- Regenerate committed bindings.
- Add helper methods only if they fit existing SDK style.
- Add tests/compile checks for constructing a hybrid search request.

Validation:

Go SDK:

- `go test ./...`

Rust SDK:

- `cargo test --workspace`
- `cargo clippy --workspace --all-targets` if required by repo CI.

Exit criteria:

- SDKs expose the new protobuf types and can build hybrid requests.

### HS11 — Console integration, if included in first release tranche

Repo: `mycel-console`

Deliverables:

- Update generated bindings/client types.
- Add a hybrid mode selector to the search UI.
- Add lexical/semantic weight controls with sane defaults.
- Add metadata filter UI only if it can be kept simple and safe; otherwise defer advanced filter UI.
- Display fused score and optional source/component diagnostics.
- Show warnings/degraded-state messages clearly.

Validation:

- `npm test` / `pnpm test` equivalent.
- `npm run build` / Tauri build checks as appropriate.

Exit criteria:

- Console can issue hybrid search requests or explicitly defers UI support while generated types remain current.

### HS12 — End-to-end validation and release handoff

Repos: all touched repos

Deliverables:

- Add or run an end-to-end scenario:
  1. create test nodes with text and metadata;
  2. create/enable semantic generation/search rule;
  3. wait for lexical and semantic readiness;
  4. search with balanced hybrid weights;
  5. search with lexical-heavy weights;
  6. search with semantic-heavy weights;
  7. apply metadata filters;
  8. verify ranking/source diagnostics.
- Collect validation evidence in a handoff report if the work spans multiple repos.
- Prepare changelogs for coordinated release.

Validation:

- API CI.
- Daemon `go test ./...`.
- SDK test suites.
- Console test/build if touched.
- Any cluster-sensitive gate only if hybrid changes touch clustered routing or indexing behavior.

Exit criteria:

- All touched repos have PRs merged to `develop`.
- Issue #27 marked `status:fixed-in-develop` after merge.
- Release notes identify the coordinated version that will release the feature.

## Detailed daemon implementation notes

### Candidate model

Use an internal representation similar to:

```go
type HybridCandidate struct {
    NodeID string

    Lexical *CandidateSource
    Semantic *CandidateSource

    FusedScore float64
}

type CandidateSource struct {
    RawScore float64
    Rank int
    NormalizedScore float64
}
```

Only populate semantic fields when semantic retrieval returns that node. Only populate lexical fields when lexical retrieval returns that node.

### Candidate count defaults

Suggested defaults:

```text
candidate_count = max(page_size * 5, 50)
```

Suggested caps:

```text
page_size <= existing Search API cap
candidate_count <= server-defined cap, e.g. 1000
```

The exact caps should follow current daemon search limits if they already exist.

### Filtering order

Correctness order:

1. retrieve candidates;
2. load/authorize graph nodes needed for filtering;
3. apply hard filters;
4. apply `require_both`;
5. fuse and sort;
6. return top-K.

Efficiency can improve this later by pushing filters into lexical or semantic retrieval. V1 must prioritize correctness and safe authorization.

### Diagnostics mapping

When `include_diagnostics=false`:

- return only stable top-level scores and source kinds as needed;
- avoid verbose component descriptions.

When `include_diagnostics=true`:

- include lexical and semantic ranks;
- include normalized source scores;
- include fused score component descriptions;
- include query plan text such as `lexical_candidates=100 semantic_candidates=100 fusion=wrrf`.

### Existing semantic API reuse

The current semantic API returns results with `node_id`, raw score, rule ID, binding key, matched chunk IDs, and snippet. Hybrid v1 should consume node IDs and semantic ranks/scores. It should avoid exposing semantic snippets in generic `SearchResult` unless the API design explicitly adds a source-neutral snippet field.

### Existing lexical API reuse

The current lexical search response already returns `SearchResult` with score, score kind, matched terms/fields, score components, and freshness diagnostics. Hybrid should reuse the lexical candidate path but should not expose raw lexical BM25 as the final `score` in hybrid mode. The final `score` must be the fused score.

## API compatibility notes

- Adding enum values and new fields is wire-compatible for protobuf clients.
- Existing `SEARCH_MODE_UNSPECIFIED` behavior must remain lexical-compatible.
- Existing clients should ignore unknown fields from newer servers.
- SDKs must be regenerated before clients can conveniently construct new options.
- If `optional` fields are added, confirm generated Go/Rust/Tauri bindings handle presence consistently.

## Test matrix

### Unit tests

- weight defaulting and normalization;
- invalid negative weights;
- all-zero weights;
- unsupported fusion strategy;
- reciprocal-rank scoring;
- deterministic tie-breaking;
- `require_both`;
- metadata filter evaluation;
- protobuf mapping.

### Service tests

- lexical mode unchanged;
- hybrid mode with lexical-only match;
- hybrid mode with semantic-only match;
- hybrid mode with shared match;
- metadata filter excludes otherwise high-ranking candidate;
- labels and property filters;
- stale lexical index policy;
- missing semantic rule/binding policy;
- diagnostics enabled/disabled.

### CLI tests

- flag parsing;
- default mode lexical;
- hybrid mode request construction;
- weight validation;
- filter flag parsing;
- diagnostics rendering.

### Integration tests

- standalone daemon hybrid search flow;
- semantic rule/binding scoped hybrid search;
- authorization behavior for hidden/unreadable nodes;
- empty results and warnings.

### Cross-repo checks

- `mycel-api` proto CI;
- `mycel` Go tests;
- Go SDK tests;
- Rust SDK tests;
- Console build/tests if touched.

## Operational and security considerations

- Hybrid search should not leak semantic rule availability, hidden node IDs, matched terms, snippets, or metadata for unauthorized nodes.
- Diagnostics must be redacted or omitted when they would reveal hidden graph content.
- Candidate over-fetching should be capped to prevent expensive requests.
- Semantic provider/vector-store failures should be classified carefully so partial results do not hide systemic failures.
- Request options should be logged without query text if query text is considered user content in operational logs.

## Open questions for review

1. Should no semantic searchable rule degrade to lexical-only with a warning by default, or fail hybrid requests?
2. Should `require_both` ship in v1 or be reserved?
3. Which property filter operators are safe and necessary for the first implementation?
4. Should `CONTAINS` mean array membership only, string substring only, or both?
5. Should hybrid mode support `page_token` in v1, or return only top-K with no continuation?
6. Should `SearchResponse.freshness` remain lexical-oriented, or should we add a combined freshness/degradation message?
7. Should semantic snippets be exposed in generic search results, or omitted from hybrid v1?
8. Should zero lexical or semantic weight skip that backend entirely for efficiency, or still retrieve for diagnostics?

## Suggested first PR sequence

1. `mycel` docs-only PR for design and implementation plan.
2. `mycel-api` API PR.
3. `mycel` daemon stub regeneration and internal fusion/filter unit tests.
4. `mycel` service orchestration PR.
5. `mycel` CLI/docs PR.
6. SDK regeneration PRs.
7. Console PR if included.
8. Coordinated develop-to-main release PRs.
