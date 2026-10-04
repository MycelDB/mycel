# Intelligence Access model kind implementation plan

## Status

Planned. Product is unreleased, so this plan intentionally allows breaking API,
storage, Console, SDK, and package-format changes. No backwards-compatible read
or write path is required beyond whatever short-lived migration tooling is useful
for local developer fixtures.

## Problem

Intelligence Access currently uses `operation` in two places with overlapping
meaning:

- inference profile / request / credential grant / policy / endpoint capability
  `operation` — the requested workload operation, such as `chat`, `summarize`,
  `classify`, or `embeddings`;
- inference model `operation` — currently treated as the model's operation and
  required by the resolver to equal the requested workload operation.

This makes generative models too restrictive. A model such as
`openai/gpt-5.6-luna` can be a generative/chat-family model while an enabled
endpoint capability declares that the endpoint+model pair supports `summarize`
and `classify`. The resolver currently rejects that valid configuration because
`model.operation == chat` does not equal a `summarize` request.

## Goals

- Make endpoint capabilities the authoritative declaration of supported workload
  operations for a specific endpoint+model pair.
- Replace model-level workload `operation` with model-level kind/category.
- Allow `chat`, `summarize`, and `classify` workloads to use generative/chat
  models when an enabled capability declares the workload operation.
- Keep `embeddings` fail-closed to embedding-kind models.
- Update API, runtime, CLI, Console, Rust SDK, docs, examples, and tests in one
  coordinated breaking change.

## Non-goals

- No compatibility guarantee for old `InferenceModel.operation` fields in public
  APIs, package files, or Console code.
- No automatic repair of operator credentials, grants, policies, or provider
  account state.
- No change to grants/policies/profiles as workload-operation concepts.
  `summarize` and `classify` remain first-class operations for authorization,
  telemetry, usage, and UI filtering.

## Target model

### Workload operation

A workload operation is what the caller wants to do and what policy/grants meter
and authorize:

- `chat`
- `summarize`
- `classify`
- `embeddings`
- `rerank` if/when implemented

These remain on:

- requests / resolve inputs;
- Intelligence profiles;
- model endpoint capabilities;
- credential grants;
- access policies;
- policy decisions;
- usage events;
- endpoint advertised operation lists.

Add image analysis as a workload operation when the product needs separately
metered/authorized vision use, for example `image_analysis`. If the caller is
only summarizing text that happens to mention an image, use `summarize`; if the
model must inspect image bytes/pixels, use the image-analysis workload so grants,
policies, telemetry, and privacy controls can distinguish it.

### Model kind

A model kind describes the model family/category independent of a specific
workload operation. Initial values:

- `generative` — text or multimodal generation model usable by capabilities for
  `chat`, `summarize`, `classify`, and image-analysis style workloads when the
  model/capability also support the required modalities;
- `embedding` — vector embedding model usable by `embeddings` capabilities;
- `reranker` — rerank model usable by `rerank` capabilities when supported.

Model kind is intentionally coarse. It should not encode every input modality or
workload. A model such as a vision-capable GPT model is still `generative`; its
image support belongs in modality/features metadata and endpoint capabilities.

Optional aliases may be accepted in code constants during the tranche for
operator ergonomics, but public docs and examples should use canonical values.
For example, `chat` can be treated as an internal alias for `generative` only if
that keeps tests/simple imports readable; otherwise avoid aliases.

### Modalities and image analysis

Image analysis should not be modeled by creating a separate model kind for every
media type. Add explicit modality support alongside kind:

- model input modalities, for example `text`, `image`;
- model output modalities, for example `text`, `json`;
- capability input/output modality constraints where an endpoint+model pair only
  supports a subset of the model's theoretical abilities;
- optional capability required features such as `vision`, `json_mode`, or
  `structured_output`.

The existing scalar `modality` field is too small for this. Replace it with
`input_modalities` and `output_modalities`, or keep `modality` only as a display
alias if the implementation wants a short-term UI label. Because compatibility
is not required, prefer the explicit list fields.

## Compatibility matrix

| Requested workload | Required capability operation | Compatible model kind |
| --- | --- | --- |
| `chat` | `chat` | `generative` |
| `summarize` | `summarize` | `generative` |
| `classify` | `classify` | `generative` |
| `embeddings` | `embeddings` | `embedding` |
| `rerank` | `rerank` | `reranker` |
| `image_analysis` | `image_analysis` | `generative` with image input modality |

Endpoint support is still required: the endpoint operation list must be empty or
include the requested workload operation. Modality support is also required for
multimodal requests: image-analysis requests require a model/capability that
accepts image input and produces an allowed output modality, normally text or
JSON.

## Tranche 1 — API and package surface

1. Update `mycel-api` protobuf source:
   - rename `InferenceModel.operation` to `kind` or `model_kind`;
   - replace scalar model `modality` with `input_modalities` and
     `output_modalities`;
   - add optional capability modality/feature fields if they are not already
     available in metadata;
   - update comments to distinguish model kind from workload operation;
   - keep `ModelEndpointCapability.operation` unchanged;
   - keep profile/grant/policy/request/usage operations unchanged.
2. Update inference package definition fields:
   - model definitions use `kind` instead of `operation`;
   - model definitions declare input/output modalities;
   - capability definitions continue to use `operation` and may narrow
     modalities/features.
3. Update examples:
   - OpenAI generative text model: `kind: generative`, input `text`, output
     `text`/`json`;
   - OpenAI vision-capable model: `kind: generative`, input `text,image`, output
     `text`/`json`, capabilities for `chat` and `image_analysis` as appropriate;
   - OpenAI embedding model: `kind: embedding`, input `text`, output
     `embedding`;
   - summarize/classify support represented as separate endpoint capabilities.
4. Regenerate protobuf outputs in downstream repos through the existing scripts.

Validation:

```sh
cd ../mycel-api && make test # or the available proto lint/generation target
```

## Tranche 2 — Go domain model and storage

1. Rename domain model field:
   - `internal/inference/model/types.go`: `Model.Operation` -> `Model.Kind`;
   - add `ModelKind` constants;
   - replace model scalar `Modality` with input/output modality lists.
2. Update file/WAL canonicalization and storage tests:
   - model uniqueness remains key-based;
   - capabilities remain unique by endpoint+model+operation.
3. Since compatibility is not required, stored model JSON may change shape from
   `operation` to `kind`. Developer data can be recreated or manually edited.
4. Update package import mapping:
   - `InferenceModel.kind` maps into `domaininference.Model.Kind`;
   - capability operation mapping is unchanged.

Validation:

```sh
go test ./internal/inference/... ./internal/daemon/api/admin
```

## Tranche 3 — Resolver and connector semantics

1. Replace `candidateMatchesOperation` with two checks:
   - capability/endpoint workload operation match;
   - model-kind compatibility with requested workload operation.
2. Required behavior:
   - `summarize` with model kind `generative` and capability operation
     `summarize` resolves successfully;
   - `classify` with model kind `generative` and capability operation
     `classify` resolves successfully;
   - `image_analysis` with model kind `generative`, capability operation
     `image_analysis`, and image input modality resolves successfully;
   - `image_analysis` without image input support is denied;
   - `embeddings` with model kind `generative` is denied even if bad catalog
     data declares an embeddings capability;
   - `chat` with model kind `embedding` is denied.
3. Update connector preflight checks:
   - embedding connector requires model kind `embedding` and capability operation
     `embeddings`;
   - chat-compatible connector requires model kind `generative` and capability
     operation in `chat|summarize|classify`;
   - image-analysis connector path requires model kind `generative`, capability
     operation `image_analysis`, and image input modality support.
4. Ensure policy decisions preserve requested workload operation, not model kind.

Tests to add/update:

- resolver allows summarize/classify via generative model;
- resolver rejects incompatible model-kind/capability combinations;
- OpenAI-compatible connector tests use model kind instead of operation;
- automation summarize path exercises generative model + summarize capability.

Validation:

```sh
go test ./internal/inference/... ./internal/automation/... ./internal/daemon/api/admin
```

## Tranche 4 — CLI and docs

1. Update CLI output labels:
   - model tables show `Kind`, not `Operation`;
   - capability tables continue to show workload `Operation`.
2. Update CLI import/create flags if present:
   - `--kind` for models;
   - `--operation` remains for capabilities, profiles, grants, and policies.
3. Update docs:
   - `docs/design/admin/inference.md`;
   - `docs/design/semantic/inference-for-graph-automations.md`;
   - `docs/operations/cli/inference.md`;
   - package/example docs.
4. Add troubleshooting text for `no enabled inference capability matches request`:
   - check model kind compatibility;
   - check endpoint capability operation;
   - check endpoint operation list.

Validation:

```sh
make docs-check
go test ./internal/cli/cmd
```

## Tranche 5 — Console

1. Update Console inference types:
   - `InferenceModelInfo.operation` -> `kind`.
2. Update catalog tables:
   - model table displays `Kind`;
   - capability table keeps `Operation`.
3. Update profile creation model multiselect:
   - selected profile operation filters model options by enabled capabilities;
   - direct `model.operation === profile.operation` filtering is removed;
   - model hints show kind/modality/dimensions.
4. Update grant creation UI:
   - model picker displays kind;
   - endpoint/capability-derived operation checkboxes remain unchanged.
5. Update tests for summarize profile creation against generative models.

Validation:

```sh
cd ../mycel-console && npm test -- --runInBand
cd ../mycel-console && MYCEL_API_ROOT="$(cd ../mycel-api && pwd)" npm run build
```

## Tranche 6 — Rust SDK

1. Regenerate/update Rust protobuf bindings after the API change.
2. Update SDK admin model structs/helpers to expose `kind`.
3. Update query/admin tests affected by model fields.

Validation:

```sh
cd ../mycel-rust-sdk && MYCEL_API_ROOT="$(cd ../mycel-api && pwd)" cargo test
```

## Tranche 7 — End-to-end validation

Use a fresh dev daemon or reset existing dev inference catalog data.

1. Import/provision OpenAI endpoint, models, and capabilities:
   - model `openai/gpt-5.6-luna`, kind `generative`;
   - capabilities for `chat`, `summarize`, `classify`;
   - model `openai/text-embedding-3-small`, kind `embedding`;
   - capability for `embeddings`.
2. Create active credential, grants, and policies for a PKM space/domain.
3. Create or update the page-summary automation profile for operation
   `summarize`, model ref `openai/gpt-5.6-luna`.
4. Create/update a PKM page and page entries.
5. Verify automation run succeeds and writes `properties.summary`.
6. Verify semantic embedding profile still resolves and semantic search still
   works.
7. Verify bad combinations fail closed:
   - summarize profile restricted to embedding model;
   - embeddings profile restricted to generative model.

Suggested commands:

```sh
make test
make docs-check
cd ../mycel-console && npm test -- --runInBand
cd ../mycel-console && MYCEL_API_ROOT="$(cd ../mycel-api && pwd)" npm run build
cd ../mycel-rust-sdk && MYCEL_API_ROOT="$(cd ../mycel-api && pwd)" cargo test
```

## Rollout notes

Because compatibility is not required, the implementation can be committed as a
single coordinated breaking branch across:

- `mycel-api`;
- `mycel`;
- `mycel-console`;
- `mycel-rust-sdk`.

Developer/operator action after merge:

- reimport inference packages or recreate dev catalog state;
- recreate affected profiles/grants only if they reference removed model IDs;
- rerun page-summary automation or update a page to trigger it again;
- rerun semantic rule backfill where the dev graph was reset.

## Acceptance criteria

- Public API no longer exposes model-level workload `operation`; models expose
  kind/category instead.
- Resolver authorization remains operation-based for requests, profiles, grants,
  policies, decisions, and usage.
- `summarize` and `classify` requests can resolve against a `generative` model
  when the endpoint capability declares the requested operation.
- `embeddings` requests cannot resolve against generative models.
- Console profile creation offers models through capability compatibility rather
  than exact model operation equality.
- Image-analysis capable generative models can be represented without creating a
  separate model kind, using workload operation + modality compatibility.
- The Knot PKM page-summary automation no longer fails with
  `no enabled inference capability matches request` for the observed
  generative-model/summarize-capability setup.
