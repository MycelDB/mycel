# `mycel semantic migrate legacy-embeddings`

Migrates the current user's MVP embedding keys/profiles into advanced semantic resources for a target space/domain.

## Usage

```sh
mycel semantic migrate legacy-embeddings \
  --space-id <space_id> \
  --domain <domain-key-or-id> \
  [--profile <legacy-profile-id-or-name>] \
  [--allow-background-use=true] \
  [--add-allow-policy=true]
```

## Behavior

For each migratable legacy profile, the command creates or reuses:

- `ModelEndpoint`
- `InferenceModel`
- `ModelEndpointCapability`
- `InferenceCredential`
- `CredentialGrant`
- `InferencePolicy` allow rule, unless disabled
- `SemanticIndex`

The command currently migrates OpenAI-compatible legacy embedding providers. Unsupported provider protocols are skipped with warnings unless `--strict` is set. Re-running the command reuses matching semantic indexes, grants, and policies instead of duplicating them.

Inline migrated secrets require the CLI user-store encryption key so advanced semantic connectors can decrypt them later:

```sh
mycel --user-store-encryption-key-b64 <base64-32-byte-key> \
  semantic migrate legacy-embeddings --space-id ... --domain ...
```

No model endpoint calls are made by this migration command.
