# Hybrid Search Design

## Status

Proposed design. Implementation should not begin until this design and the related API changes have been reviewed.

## Summary

Hybrid search is a first-class MycelDB search mode that combines lexical full-text retrieval and semantic/vector retrieval into one ranked, non-streaming result set. Metadata/tag/property criteria are hard eligibility filters, not ranking signals.

The first version should extend the existing `SearchService.Search` API with a `SEARCH_MODE_HYBRID` mode. A hybrid request runs lexical and semantic candidate retrieval for the same query, applies structured metadata filters, deduplicates candidates by graph node ID, fuses lexical and semantic ranks using configurable weights, and returns a bounded top-K response with optional diagnostics.

## Goals

1. Provide one API entry point for combined lexical + semantic discovery.
2. Keep the response unary/non-streaming for v1, consistent with current lexical and semantic search APIs.
3. Support configurable lexical and semantic weights, defaulting to balanced search.
4. Treat metadata, tags, labels, and properties as hard filters that determine candidate eligibility.
5. Deduplicate results by `node_id` and expose the source signals that contributed to each result.
6. Return explainable score diagnostics when requested, including lexical rank/score, semantic rank/score, normalized component values, and fused score.
7. Preserve existing pure lexical search behavior.
8. Reuse current lexical and semantic subsystems rather than introducing a third physical index.
9. Degrade predictably when one retrieval subsystem is stale or unavailable, with explicit warnings.
10. Leave room for future fusion strategies without changing the basic request/response shape.

## Non-goals for v1

- Streaming/progressive search responses.
- Metadata reranking or metadata score boosts.
- Cross-space or cross-domain search.
- Learned rerankers or LLM reranking.
- New vector-store provider behavior.
- Blob text extraction changes.
- Query expansion or automatic synonym generation.
- Full pagination over an unstable fused ranking. V1 may cap results to top-K and either omit pagination for hybrid mode or use a conservative continuation-token contract.

## Search families

MycelDB search should remain conceptually split into three complementary families:

| Family | Role in hybrid v1 | Examples |
| --- | --- | --- |
| Lexical search | Retrieval and ranking signal | exact terms, quoted phrases, Boolean text queries |
| Semantic search | Retrieval and ranking signal | concepts, paraphrases, natural language intent |
| Metadata/tag/property search | Hard filter only | labels, tags, status, type, owner, timestamps |

Metadata is not a third score source in v1. A result either satisfies metadata filters or it is excluded.

## API proposal

Hybrid search should live in the existing client `SearchService`:

```proto
service SearchService {
  rpc Search(SearchRequest) returns (SearchResponse);
}
```

Extend `SearchMode`:

```proto
enum SearchMode {
  SEARCH_MODE_UNSPECIFIED = 0;
  SEARCH_MODE_LEXICAL = 1;
  SEARCH_MODE_HYBRID = 2;
}
```

Extend `SearchRequest`:

```proto
message SearchRequest {
  string space_id = 1;
  string domain_id = 2;
  SearchMode mode = 3;
  string query = 4;

  // Hard eligibility filters. These do not contribute to score.
  SearchFilters filters = 5;

  int32 page_size = 6;
  string page_token = 7;
  bool include_diagnostics = 8;
  bool allow_stale = 9;
  int64 max_revision_lag = 10;

  HybridSearchOptions hybrid = 11;
  SemanticSearchOptions semantic = 12;
  LexicalSearchOptions lexical = 13;
}
```

New options:

```proto
message HybridSearchOptions {
  // Defaults to 0.5 and 0.5. The server normalizes non-zero weights.
  double lexical_weight = 1;
  double semantic_weight = 2;

  // Default: weighted reciprocal rank fusion.
  HybridFusionStrategy fusion_strategy = 3;

  // Default false. When false, a node can match either backend. When true, a
  // node must be returned by both lexical and semantic retrieval.
  bool require_both = 4;
}

message LexicalSearchOptions {
  // Number of lexical candidates to retrieve before filtering/fusion. If unset,
  // the server chooses a bounded value such as max(page_size * 5, 50).
  int32 candidate_count = 1;
}

message SemanticSearchOptions {
  // Optional. If omitted, search all enabled searchable semantic rule bindings
  // for the domain that the caller can read.
  optional string semantic_rule_id = 1;

  // Optional. Requires semantic_rule_id when set.
  optional string embedding_binding_key = 2;

  // Optional model-dependent threshold passed through to semantic retrieval.
  optional double min_score = 3;

  // Number of semantic candidates to retrieve before filtering/fusion. If unset,
  // the server chooses a bounded value such as max(page_size * 5, 50).
  int32 candidate_count = 4;
}

enum HybridFusionStrategy {
  HYBRID_FUSION_STRATEGY_UNSPECIFIED = 0;
  HYBRID_FUSION_STRATEGY_WEIGHTED_RECIPROCAL_RANK = 1;
}
```

Extend filters from the current reserved seam into structured hard filters:

```proto
message SearchFilters {
  repeated string node_labels = 1;
  repeated PropertyFilter properties = 2;
  repeated string node_ids = 3;
}

message PropertyFilter {
  string path = 1;
  FilterOperator operator = 2;
  repeated string values = 3;
}

enum FilterOperator {
  FILTER_OPERATOR_UNSPECIFIED = 0;
  FILTER_OPERATOR_EQUALS = 1;
  FILTER_OPERATOR_NOT_EQUALS = 2;
  FILTER_OPERATOR_IN = 3;
  FILTER_OPERATOR_CONTAINS = 4;
  FILTER_OPERATOR_EXISTS = 5;
}
```

Filter details can be expanded later for numeric ranges, timestamps, arrays, and typed values. V1 should avoid ambiguous typed comparison semantics unless they are already defined elsewhere in graph/query APIs.

## Response proposal

Reuse `SearchResponse` and `SearchResult`, extending the score model:

```proto
message SearchResult {
  string space_id = 1;
  string domain_id = 2;
  string node_id = 3;

  // In hybrid mode this is the fused score used for final ranking.
  double score = 4;
  SearchScoreKind score_kind = 5;

  int64 indexed_graph_revision = 6;
  repeated string matched_terms = 7;
  repeated string matched_field_paths = 8;
  repeated SearchScoreComponent score_components = 9;

  repeated SearchResultSource sources = 10;
}

message SearchResultSource {
  SearchResultSourceKind kind = 1;
  double raw_score = 2;
  int32 rank = 3;
  double normalized_score = 4;
}

enum SearchResultSourceKind {
  SEARCH_RESULT_SOURCE_KIND_UNSPECIFIED = 0;
  SEARCH_RESULT_SOURCE_KIND_LEXICAL = 1;
  SEARCH_RESULT_SOURCE_KIND_SEMANTIC = 2;
}

enum SearchScoreKind {
  SEARCH_SCORE_KIND_UNSPECIFIED = 0;
  SEARCH_SCORE_KIND_BM25 = 1;
  SEARCH_SCORE_KIND_HYBRID_FUSED = 2;
}
```

`score_components` can include human-readable diagnostics such as:

- `lexical.weight`
- `lexical.rank_score`
- `semantic.weight`
- `semantic.rank_score`
- `hybrid.fused_score`

The `sources` field is the stable machine-readable explanation of which backends returned a result. `score_components` remains a flexible diagnostics surface.

## Weight semantics

Weights control how much lexical and semantic retrieval influence the final fused rank.

Rules:

1. Missing weights default to `0.5 / 0.5`.
2. Weights must be non-negative.
3. At least one weight must be greater than zero.
4. The server normalizes weights before scoring, so `2 / 1` behaves like `0.6667 / 0.3333`.
5. A zero weight disables that score contribution but does not necessarily disable candidate retrieval unless the server explicitly optimizes it.

Recommended default:

```text
lexical_weight = 0.5
semantic_weight = 0.5
```

## Fusion strategy

V1 should use weighted reciprocal rank fusion rather than combining raw BM25 and vector scores directly. Raw lexical and semantic scores have different scales and may not be comparable across query types, analyzers, embedding models, or vector stores.

For each candidate node:

```text
lexical_rank_score  = 1 / (rank_constant + lexical_rank)
semantic_rank_score = 1 / (rank_constant + semantic_rank)

fused_score =
  normalized_lexical_weight  * lexical_rank_score +
  normalized_semantic_weight * semantic_rank_score
```

Where:

- lower rank is better (`rank = 1` is the best result from that backend);
- missing backend result contributes `0` for that backend;
- `rank_constant` is server-defined and should be stable, e.g. `60`, to avoid over-amplifying tiny rank differences.

The server should sort by:

1. `fused_score` descending;
2. presence in both sources before one source, as a deterministic tie-breaker;
3. best individual rank;
4. `node_id` lexical order for final stability.

## Metadata filters

Metadata filters are hard constraints. They should be interpreted as eligibility checks over graph node state, not as scoring inputs.

Example:

```json
{
  "mode": "SEARCH_MODE_HYBRID",
  "query": "raft recovery",
  "filters": {
    "node_labels": ["Note"],
    "properties": [
      {"path": "tags", "operator": "FILTER_OPERATOR_CONTAINS", "values": ["k3s"]},
      {"path": "status", "operator": "FILTER_OPERATOR_EQUALS", "values": ["published"]}
    ]
  }
}
```

This means: retrieve and rank candidates for `raft recovery`, but only return nodes that are `Note`s, contain tag `k3s`, and have status `published`.

Implementation should apply filters as early as feasible for efficiency. Correctness requires filters to be enforced before results are returned, even if early filtering is not available for one backend.

## Execution model

For `SEARCH_MODE_HYBRID`:

1. Validate request, authorization, weights, limits, and filters.
2. Choose lexical and semantic candidate counts.
3. Run lexical candidate retrieval for the query.
4. Run semantic candidate retrieval for the query and selected semantic binding scope.
5. Merge candidates by `node_id`.
6. Read enough graph/node metadata to enforce authorization and hard filters.
7. If `require_both=true`, drop candidates not present in both lexical and semantic result sets.
8. Compute weighted reciprocal-rank fused scores.
9. Sort deterministically and return top `page_size`.
10. Include freshness and warnings from both retrieval paths.

Lexical and semantic retrieval may run concurrently when implementation structure permits it.

## Freshness and consistency

Hybrid search combines eventually consistent subsystems. The response should report freshness and warnings clearly.

Recommended behavior:

- `SearchResponse.freshness` should continue to represent lexical index freshness for compatibility.
- Hybrid-specific diagnostics should report semantic index/rule warnings in `warnings` and `diagnostics.query_plan` when requested.
- If lexical freshness violates `allow_stale=false` or `max_revision_lag`, fail closed as existing lexical search intends.
- If semantic state is stale/degraded, include warnings. A stricter semantic freshness gate can be added later if semantic APIs define revision-based freshness.

Search results remain discovery candidates. Clients requiring current truth should re-read returned node IDs through graph APIs.

## Failure/degradation policy

Default hybrid behavior should be useful but explicit about degradation:

- If lexical retrieval fails because the lexical index is unavailable and lexical weight is non-zero, return an error unless the failure is classified as a warning-compatible stale/degraded state.
- If semantic retrieval fails because no searchable semantic binding exists, return lexical-only results with a warning by default.
- If semantic retrieval fails due to provider/vector-store runtime errors and semantic weight is non-zero, return lexical-only results with a warning only when the semantic failure is explicitly non-fatal; otherwise return an error.
- If both backends fail or return no eligible candidates, return an empty result set with warnings or an error depending on failure severity.
- If `require_both=true`, backend unavailability should generally be an error because the server cannot prove intersection semantics.

This policy can be tightened during implementation after current semantic error classes are reviewed.

## Authorization

Hybrid search must not widen access beyond existing graph, lexical, or semantic permissions.

A caller must be authorized for:

- the target space/domain;
- graph/node read visibility for returned nodes;
- lexical search access where lexical retrieval is used;
- semantic search access and selected semantic rule/binding access where semantic retrieval is used.

Diagnostics must not leak unauthorized terms, field paths, snippets, rule metadata, provider names, or hidden node existence.

## Pagination and limits

Hybrid ranking is produced by fusing bounded candidate sets. V1 should prefer a simple top-K contract:

- `page_size` controls the returned top-K count;
- the daemon may cap `page_size` and candidate counts;
- hybrid mode may return an empty `next_page_token` until stable pagination semantics are designed;
- if pagination is supported, the page token must encode the request shape and candidate/fusion state sufficiently to avoid inconsistent continuation behavior.

Candidate count defaults should over-fetch relative to `page_size`, for example:

```text
candidate_count = max(page_size * 5, 50)
```

with a server-defined upper cap.

## CLI proposal

Extend the existing search CLI with hybrid mode:

```sh
mycel search \
  --space SPACE \
  --domain DOMAIN \
  --mode hybrid \
  --query "raft recovery" \
  --lexical-weight 0.6 \
  --semantic-weight 0.4 \
  --label Note \
  --property tags contains k3s \
  --limit 20 \
  --diagnostics
```

CLI defaults should match API defaults:

- mode remains lexical unless explicitly set to hybrid;
- weights default to `0.5 / 0.5`;
- metadata filters are hard filters;
- diagnostics are opt-in.

## Example API request

```json
{
  "space_id": "space-1",
  "domain_id": "notes",
  "mode": "SEARCH_MODE_HYBRID",
  "query": "authentication timeout after pod restart",
  "page_size": 20,
  "include_diagnostics": true,
  "filters": {
    "node_labels": ["IncidentNote"],
    "properties": [
      {"path": "tags", "operator": "FILTER_OPERATOR_CONTAINS", "values": ["k3s"]}
    ]
  },
  "hybrid": {
    "lexical_weight": 0.6,
    "semantic_weight": 0.4,
    "fusion_strategy": "HYBRID_FUSION_STRATEGY_WEIGHTED_RECIPROCAL_RANK"
  },
  "lexical": {
    "candidate_count": 100
  },
  "semantic": {
    "candidate_count": 100
  }
}
```

## Open questions

1. Should `SEARCH_MODE_UNSPECIFIED` remain lexical forever, or should the default eventually become hybrid when semantic indexes are configured?
2. Should hybrid mode return an error or lexical-only degraded results when no semantic searchable rule exists?
3. Which metadata filter operators should be supported in the first implementation without introducing ambiguous type coercion?
4. Should semantic snippets be included in `SearchResult`, or should hybrid v1 avoid snippets to keep the generic search response source-neutral?
5. Should `SearchResponse.freshness` remain lexical-only in hybrid mode, or should a new combined freshness message be added?
6. Should `require_both` be included in v1, or reserved until users ask for intersection-only hybrid retrieval?

## Implementation phases

1. API design review in `mycel-api`, including proto field numbering and generated SDK impacts.
2. Daemon search orchestration layer that can call existing lexical and semantic search services and merge by `node_id`.
3. Metadata filter evaluator reused from graph/query code where possible.
4. Weighted reciprocal-rank fusion implementation and tests.
5. CLI support for hybrid mode, weights, candidate counts, and metadata filters.
6. Documentation and examples.
7. SDK updates after proto regeneration.

## Review checklist

- Confirm non-streaming response contract.
- Confirm metadata filters are hard filters only.
- Confirm weighted reciprocal-rank fusion as v1 default.
- Confirm default weights `0.5 / 0.5`.
- Confirm whether semantic unavailability should degrade or fail by default.
- Confirm whether hybrid pagination is omitted or constrained in v1.
