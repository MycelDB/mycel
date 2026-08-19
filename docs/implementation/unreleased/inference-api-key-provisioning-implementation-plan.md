# Inference API Key Provisioning Implementation Plan

## Goal

Make direct API key provisioning the only supported credential provisioning path for inference credentials. Operators should be able to provision and rotate API keys through mycel Console, the mycel CLI, or direct gRPC calls without using external reference mechanisms.

The daemon/API remains authoritative for authorization and secret handling. Console affordances are UX hints only.

## Decisions

- Remove external secret reference provisioning and plumbing from inference credential create/rotate paths.
- Support API key secret material through:
  - mycel Console password input.
  - mycel CLI, preferably via stdin.
  - gRPC `secret_value` field.
- Do not return stored secret values from APIs.
- It is acceptable to return safe secret metadata, including the last 4 characters of the key.
- Inference packages must continue to contain catalog metadata only; no credentials, API keys, or secrets.
- No compatibility or migration is required for existing external-ref credentials.

## Public API Shape

Start public protobuf changes in:

```text
mycel-api/api/proto/mycel/admin/v1/inference.proto
```

Credential create/rotate requests should retain direct secret material and remove external refs:

```proto
message AdminInferenceCredentialServiceCreateCredentialRequest {
  string key = 1;
  string display_name = 2;
  string model_endpoint = 3;
  string model_endpoint_id = 4;
  string owner_type = 5;
  string owner_id = 6;
  string auth_type = 7;
  string secret_value = 11;
  bool is_default = 10;
}

message AdminInferenceCredentialServiceRotateCredentialRequest {
  string credential = 1;
  string credential_id = 2;
  string secret_value = 3;
  string reason = 4;
}
```

If field numbering or oneof removal needs care for generated code, prefer a clean source-of-truth proto update over preserving deprecated external-reference fields, since no compatibility is required.

Credential response metadata should include a safe suffix if the daemon can compute/store it without exposing the secret:

```proto
message InferenceCredential {
  ...
  string secret_suffix = <next_field>;
}
```

Use prose labels such as **last 4 characters**, not **last 4 digits**, because API keys are not numeric.

## Secret Handling Requirements

- Accept secret values only on create/rotate requests.
- Never include full secret values in:
  - list/get responses
  - error messages
  - logs
  - Console detail drawers
  - import history
  - inference packages
  - tests snapshots/fixtures unless fake values are clearly non-secret
- Store only derived display metadata alongside the secret, for example:
  - `secret_suffix`: last 4 characters
  - optionally `secret_fingerprint`: short non-reversible hash prefix for operator verification
- Treat empty/whitespace-only API keys as invalid.
- Clear secret input fields in Console after successful create/rotate.

## Implementation Phases

### Phase 1 — API source-of-truth update

Repository: `mycel-api`

- Remove external-reference from inference credential create/rotate request messages.
- Remove any oneof branch dedicated to external refs.
- Add credential metadata for `secret_suffix` if not already present.
- Regenerate downstream protobuf outputs from source; do not hand-edit generated protobuf files.
- Update public API docs/examples to show direct API key provisioning only.

Validation:

```sh
cd mycel-api
make generate
```

### Phase 2 — Daemon credential/secret model

Repository: `mycel`

- Update generated proto bindings from `mycel-api`.
- Remove daemon handling of external secret references for inference credentials.
- Replace helper logic such as `credential_secret_material(external-reference, secret_value)` with direct `secret_value` validation.
- Update create credential flow to persist API key secret material.
- Update rotate credential flow to replace stored API key secret material.
- Compute/store/report `secret_suffix` safely.
- Remove external-reference mentions from inference setup docs, examples, tests, and package workflows.
- Ensure inference package import still rejects or ignores credential material.

Tests:

- Create credential with `secret_value` succeeds.
- Create credential with empty secret fails.
- Rotate credential with `secret_value` succeeds and updates suffix/version.
- List/get credential never returns full secret.
- External refs are no longer accepted by API/server code.

### Phase 3 — CLI UX

Repository: `mycel`

Update inference credential commands:

- Remove external-reference flags.
- Prefer safe stdin entry:

```sh
mycel inference credential create openai-default \
  --model-endpoint openai \
  --owner-type system \
  --auth-type api_key \
  --secret-stdin
```

- Optionally retain a less-safe explicit flag for automation only:

```sh
--secret-value <value>
```

If `--secret-value` is kept, CLI help should warn that it may be captured in shell history/process listings.

Add rotate support with the same secret input options:

```sh
mycel inference credential rotate openai-default --secret-stdin
```

Tests:

- CLI create via stdin sends `secret_value`.
- CLI rotate via stdin sends `secret_value`.
- external-reference flags is absent from help and command parsing.
- Table/detail output shows suffix but not full key.

### Phase 4 — SDK updates

Repositories: `mycel-go-sdk`, `mycel-rust-sdk`

- Regenerate/update protobuf bindings.
- Remove external-ref fields from helper builders and examples.
- Keep `secret_value` available for direct gRPC/helper invocation.
- Add or update docs to state that returned credential objects contain only metadata, not the key.

Tests:

- Helper builders compile and construct requests with `secret_value`.
- No SDK docs/examples reference external-reference provisioning.

### Phase 5 — mycel Console credentials page

Repository: `mycel-console`

Update Credentials tab create flow:

- Replace `External ref` field with `API key` password input.
- Submit using `secretValue` only.
- Clear the API key field after successful create.
- Add copy such as: “The key is sent to the daemon once and will not be displayed again.”
- Show safe credential metadata in rows/details:
  - endpoint/model endpoint key
  - owner type
  - auth type
  - status
  - default marker
  - secret version
  - secret suffix, formatted as `••••abcd`
  - rotated/last used timestamps

Add credential rotation UX if feasible in the same pass:

- Row action: `Rotate`
- Modal with password input for replacement API key.
- Submit using `rotateInferenceCredential({ credentialId, secretValue })`.
- Clear input after success.

Remove Console references to:

- external-reference API-key setup
- external references
- “external ref” labels/help text

### Phase 6 — OpenAI setup wizard

Repository: `mycel-console`

- Replace wizard `external-reference` with password-style OpenAI API key input.
- Create credential with `secretValue`.
- Keep package/catalog setup credential-free.
- After wizard completion, clear the API key value from component state.
- Ensure wizard summaries do not echo the key.

### Phase 7 — Tauri command/types/service cleanup

Repository: `mycel-console`

- Remove `external-reference` from TypeScript types:
  - `CreateCredentialInput`
  - `RotateCredentialInput`
  - wizard form state
  - credential form state
- Remove external-reference from Rust Tauri input structs and command mapping.
- Update secret material helpers to require `secret_value` only.
- Ensure errors mention “API key” or “secret value”, not external refs.

### Phase 8 — Documentation/examples cleanup

Repositories: all mycel repos touched above

Search and remove/update references to:

```text
environment-reference-scheme
external-reference
external-reference
external-reference flag
OPENAI_API_KEY provisioning via env ref
```

Keep environment variables only where they are used as a shell convenience to feed stdin, for example:

```sh
printf '%s' "$OPENAI_API_KEY" | mycel inference credential create openai-default --secret-stdin ...
```

This is different from storing an external reference in mycel.

## Acceptance Criteria

- A user can provision an OpenAI API key in mycel Console without creating an environment variable.
- A user can provision and rotate an API key through CLI stdin.
- A direct gRPC caller can provision and rotate an API key using `secret_value`.
- External reference provisioning are removed from code, tests, docs, and UI.
- Inference packages still contain no credential material.
- Full API keys are never returned or displayed after submission.
- Credential list/detail surfaces show only safe metadata, including optional last 4 characters.
- Authorization remains enforced by the daemon/API.

## Validation Commands

Run relevant validation after implementation:

```sh
# mycel-api
cd mycel-api
make generate

# mycel
cd ../mycel
go test ./...
git diff --check

# mycel-go-sdk
cd ../mycel-go-sdk
go test ./...

# mycel-rust-sdk
cd ../mycel-rust-sdk
cargo test

# mycel-console
cd ../mycel-console
npx tsc --noEmit
npm test -- --runInBand
npm run build
cd src-tauri && MYCEL_API_ROOT=../../mycel-api cargo check
```

## Risks / Follow-ups

- At-rest secret protection depends on the daemon secret store implementation. If needed, add a follow-up to encrypt local secret storage or integrate OS/key-management backends.
- CLI `--secret-value` is convenient but less safe than stdin. Consider omitting it entirely if stdin covers automation needs.
- If public proto field removal causes generated-code churn, keep the implementation clean and update all downstream callers in the same change set rather than preserving dead external-ref branches.
