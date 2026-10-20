# Deterministic inference fixtures

## Scope

The `fake` inference connector keeps its historical default response (`[1,0,0]`, or the explicitly configured in-process test vector). An inference catalog opts into deterministic semantic fixtures only by setting model, capability, or endpoint metadata under `mycel_fixture`.

The first strategy is `token-basis-v1`. It is intended for credential-free correctness and reliability tests. It performs no network I/O, does not resolve credential values for the `fake` endpoint, uses no random source or wall clock, and never selects fixture behavior from text unless the catalog metadata explicitly enables the strategy.

## `token-basis-v1` algorithm

The model must declare `embedding_dims`/`dimensions` from 1 through 4096.

Input normalization is byte-defined:

1. ASCII `A-Z` become `a-z`.
2. ASCII space, tab, CR, LF, form-feed, and vertical-tab runs collapse to one space.
3. Leading and trailing collapsed space is removed.
4. All other UTF-8 bytes are retained unchanged.

Tokens are separated by any character other than ASCII `a-z`, `0-9`, or `-`. A token exactly matching `axis-N`, where `N` is a base-10 index without leading zeroes and is smaller than the declared dimensions, contributes `1` to basis component `N`. Duplicate tokens contribute repeatedly. If one or more basis tokens are recognized, other tokens are ignored. The result is normalized to unit L2 length.

When no basis token is recognized, the connector produces a stable hash vector. For block counter `b = 0,1,...`, it hashes:

```text
SHA-256(normalized UTF-8 bytes || 0x00 || uint32-big-endian(b))
```

Each consecutive big-endian signed 32-bit word is divided by `2147483648.0` and assigned to the next vector component. Blocks continue until all dimensions are filled, then the vector is normalized to unit L2 length. A zero vector is defensively replaced by basis axis 0.

Provider request IDs contain only the strategy marker and truncated resolved-profile/input hashes. The profile hash binds the fixture config, endpoint key, model key, provider model, dimensions, vector space, capability key, and provider-model override, so independent profiles do not collide or consume one another's scripts. Usage uses the connector's deterministic whitespace token estimate. Vectors and request IDs are covered by exact daemon and independent Mycel Lab goldens.

## Finite outcome scripts

A bounded script may be added under the same metadata object:

```json
{
  "mycel_fixture": {
    "strategy": "token-basis-v1",
    "script": {
      "version": "v1",
      "reset": "process",
      "require_match": true,
      "entries": [
        {
          "input_sha256": "8a945d36c4beb8edeedf23e3990d95e71fc82f96091ebb406d778201b9cebc8a",
          "outcomes": [
            {"kind": "retryable-error", "code": "fixture_busy"},
            {"kind": "success", "delay_ms": 25}
          ]
        }
      ]
    }
  }
}
```

`input_sha256` is the lowercase SHA-256 of normalized input. Supported outcomes are:

- `success`;
- `retryable-error`;
- `error`;
- `blocked`;
- `refused`;
- `wrong-dimensions`.

Fixture metadata is capped at 1 MiB before decoding, and strategy/version/reset/outcome text is independently bounded. Scripts allow at most 256 input entries, 32 outcomes per input, and 30 seconds delay per outcome. Unknown fields, strategies, versions, reset modes, outcome kinds, duplicate digests, unsafe error codes, excessive dimensions, excessive text, and excessive delays fail with fixed bounded categories before execution. Outcome sequences never repeat: after the last entry the connector returns `fake_fixture_script_exhausted`.

Attempt selection is linearized under the connector lock. Concurrent calls for one script/input digest consume each finite outcome exactly once. Delay waits are context-cancelable. Accounting retains only script/profile/input hashes and bounded counts for attempts, completions, failures, and cancellations; it never retains raw input.

`reset: process` means daemon restart or connector recreation starts each sequence at attempt one. The connector's test-only `ResetFixtureState` method advances a state generation, rejects delayed requests selected before the reset, and prevents their completions from contaminating new accounting. A changed canonical script has a new script hash and independent sequence. Process-local script state is capped at 1024 script/input pairs.

## Response validation

The inference service rejects empty, non-finite, or wrong-dimension embedding responses before they can be published. Scripted `wrong-dimensions` outcomes therefore exercise the same validation boundary as malformed external-provider responses.

## Security and compatibility

- Existing fake connector callers without `mycel_fixture` are unchanged.
- Fixture configuration belongs in catalog metadata, not graph text.
- Script errors use bounded safe codes and fixed sanitized messages.
- Fixture stats expose hashes and counts only.
- No secret or credential value is needed or included in request IDs, usage, errors, or fixture state.
