# `mycel automation`

Manage graph automations.

Authentication mode: **user**.

The canonical authoring model is split into two resources:

- **procedure**: reusable graph work, input rendering, inference operation, prompt,
  and output actions;
- **binding**: trigger, scope, runtime actor/on-behalf-of principal context,
  debounce, and idempotency settings.

Canonical commands:

```sh
mycel automation procedure ...
mycel automation binding ...
```

Legacy combined automation definitions remain available for compatibility, but new
authoring should use procedures plus bindings.

## Domain scope

Prefer space plus domain refs on all procedure, binding, legacy, run, and
invocation commands:

```sh
--space-id <space-id> --domain default
```

`--domain` accepts a domain key or UUID when `--space-id` is provided. A bare
UUID still works for compatibility. `--domain-id` is also accepted for scripts
that already store the resolved domain UUID, but it is deprecated.

## Canonical procedure and binding commands

Procedures and bindings live under the `automation` namespace:

```sh
mycel automation procedure validate examples/procedures/page-summary.json
mycel automation procedure validate examples/procedures/page-summary.json \
  --server \
  --space-id <space-id> \
  --domain default
mycel automation procedure put examples/procedures/page-summary.json \
  --space-id <space-id> \
  --domain default
mycel automation procedure list --space-id <space-id> --domain default
mycel automation procedure get knot-pkm.page-summary --space-id <space-id> --domain default

mycel automation binding validate examples/automation-bindings/page-summary-user.json
mycel automation binding validate examples/automation-bindings/page-summary-user.json \
  --server \
  --space-id <space-id> \
  --domain default
mycel automation binding put examples/automation-bindings/page-summary-user.json \
  --space-id <space-id> \
  --domain default
mycel automation binding list --space-id <space-id> --domain default
mycel automation binding enable knot-pkm.user.example.page-summary.entry-trigger \
  --space-id <space-id> \
  --domain default
```

See the focused references:

- [`mycel automation procedure`](procedure.md)
- [`mycel automation binding`](automation-binding.md)

Compatibility aliases remain available for existing scripts:

```sh
mycel procedure ...
mycel automation-binding ...
```

Broad top-level aliases such as `mycel binding` and `mycel bindings` are
compatibility aliases only and should not be used in new docs or scripts.

## Recommended provisioning workflow

1. Create or select an inference profile and model capability for the operation.
   See [Inference CLI](inference.md).
2. Create an inference credential and grant it to the automation actor with the
   intended on-behalf-of principal and `automation` usage mode.
3. Write the graph procedure JSON and run local validation:

   ```sh
   mycel automation procedure validate procedure.json
   ```

4. Run daemon-backed validation for normalized JSON and daemon-state checks:

   ```sh
   mycel --output json automation procedure validate procedure.json \
     --server \
     --space-id <space-id> \
     --domain default
   ```

5. Upsert the procedure:

   ```sh
   mycel automation procedure put procedure.json \
     --space-id <space-id> \
     --domain default
   ```

6. Write the binding JSON, including `scope`, `trigger`, and `runtime`, then
   validate it against daemon state. Server validation catches missing referenced
   procedures:

   ```sh
   mycel --output json automation binding validate binding.json \
     --server \
     --space-id <space-id> \
     --domain default
   ```

7. Upsert and enable the binding:

   ```sh
   mycel automation binding put binding.json --space-id <space-id> --domain default
   mycel automation binding enable <binding-id> --space-id <space-id> --domain default
   ```

Bindings can be created by an operator/user while their runtime context executes
later as the built-in `automation` actor on behalf of a configured principal,
subject to inference credential grants and access policies.

## Example: summarize Console image attachments

Console image uploads create graph blob nodes labeled `attachment` and `blob`.
Bind image-analysis automations to those labels, not to the parent business node
label, when you want the uploaded image node itself to trigger the workflow.

The runnable example files are:

- `examples/procedures/image-attachment-summary.json`
- `examples/automation-bindings/image-attachment-summary.json`

Provision the OpenAI catalog and an encrypted credential first. Inline inference
secrets require encryption at rest; daemons without encryption reject them. Use
`--secret-stdin` so API keys are not stored in shell history:

```sh
mycel inference package apply examples/inference/standard-openai-chat.json

printf '%s' "$OPENAI_API_KEY" | mycel inference credential create openai-key \
  --model-endpoint openai \
  --owner-type system \
  --owner-id system \
  --secret-stdin
```

Create an image-analysis profile and allow the automation actor to use the
credential on behalf of the user/principal that owns the binding:

```sh
mycel inference profile create image-attachment-summary \
  --space-id <space-id> \
  --domain default \
  --operation image_analysis \
  --purpose automation \
  --model openai/gpt-5.6 \
  --privacy-class third_party \
  --max-output-tokens 512

mycel inference grant openai-key \
  --space-id <space-id> \
  --domain default \
  --operation image_analysis \
  --model-endpoint openai \
  --model openai/gpt-5.6 \
  --grantee-principal-id automation \
  --allow-on-behalf-of-principal-id <owner-principal-id>

mycel inference policy allow \
  --space-id <space-id> \
  --domain default \
  --operation image_analysis \
  --privacy-class third_party \
  --reason "image attachment summaries are allowed"
```

Edit the binding example so `scope.space_id`, `scope.domain_id`,
`runtime.owner_principal_id`, and `runtime.on_behalf_of_principal_id` match your
deployment. Then validate and apply the procedure and binding with the canonical
commands:

```sh
mycel automation procedure validate examples/procedures/image-attachment-summary.json
mycel automation procedure put examples/procedures/image-attachment-summary.json \
  --space-id <space-id> \
  --domain default

mycel automation binding validate examples/automation-bindings/image-attachment-summary.json \
  --server \
  --space-id <space-id> \
  --domain default
mycel automation binding put examples/automation-bindings/image-attachment-summary.json \
  --space-id <space-id> \
  --domain default
```

After uploading an image in Console's **Attachments** tab, inspect generated
summary nodes:

```sql
MATCH (i:attachment)-[:contains]->(s:image_summary)
RETURN i.properties.name, s.payload.text
FETCH FIRST 20 ROWS ONLY
```

The bundled OpenAI package marks GPT-5.6 capabilities with
`output_token_parameter=max_completion_tokens`, so profiles can use
`--max-output-tokens` with models that reject legacy `max_tokens`.

## Procedure control and inference audit evidence

Disable or re-enable a logical procedure with a committed execution-partition
barrier:

```sh
mycel automation procedure disable <procedure-id> --space-id <space-id> --domain default
mycel automation procedure enable <procedure-id> --space-id <space-id> --domain default
```

A successful response includes the control epoch plus affected and acknowledged
partitions. Disablement prevents later provider starts and suppresses unpublished
output from calls that were already accepted by a provider. It cannot retract an
already accepted provider request, so operators must still treat that request as
conservatively accounted. Enablement does not complete until its partition fences
are committed.

Inspect the daemon-owned, sanitized attempt ledger:

```sh
mycel automation inference-attempt list --space-id <space-id> \
  --procedure-id <procedure-id>
mycel automation inference-attempt get <attempt-id> --space-id <space-id>
mycel automation inference-attempt changes --space-id <space-id> \
  --after-cursor <snapshot-or-next-cursor>
```

Start polling with the `snapshot_cursor` returned by `list`. Cursors are opaque
and bound to the principal, space, and filter; do not parse or reuse them under a
different identity or filter. Delivery is at least once, so consumers must
deduplicate by immutable event ID. A reported history gap is not an empty result:
discard the old cursor, obtain a fresh current-state snapshot, and resume from
its snapshot cursor.

These read surfaces require both `automation.read` and `inference.audit.read`.
They intentionally exclude prompts, rendered graph content, outputs, vectors,
credential material, provider request/body identifiers, raw errors, and
arbitrary metadata. Use bounded policy-decision, credential-grant, configuration,
and authorization revisions for correlation instead.

Capacity is bounded per space. Capacity errors fail before provider access.
Raft-owned retention prunes oldest immutable changes by age, count, and bytes,
then prunes only old terminal materialized attempts; active and ambiguous
attempts are retained. A terminal attempt that is still referenced by a
retained run is not pruned; it becomes eligible only after run retention removes
the reference. Retention advances explicit history/state watermarks past every
snapshot that can no longer be
reconstructed, so collectors must handle gaps as described above. Do not delete
ledger files or active attempt state manually.

Provider start and output publication use two committed authorization fences:
global endpoint/model/capability/credential state is derived from authoritative
semantic catalog state and asserted in system-Raft order, while the semantic
partition state machine passes the exact profile/grant/policy revision into the
automation-owned execution-partition fence. Startup/rebuild backfills that fence
for existing spaces. The combined revision remains available for correlation,
but state-machine decisions use the separately committed scope revisions and do
not depend on derived inference projections or a node-local decision counter. Raft snapshots
include current attempts, retained immutable changes, and procedure control
fences, so restored partitions retain polling watermarks and start/publication
fencing.

## Legacy combined automation definitions

Legacy combined definition commands are compatibility surfaces. They keep older
scripts working, but new automation authoring should use procedures and bindings.

The compatibility commands remain at the root:

```sh
mycel automation validate legacy-automation.json
mycel automation create legacy-automation.json --space-id <space-id> --domain default
mycel automation update <automation-id> legacy-automation.json --space-id <space-id> --domain default
mycel automation put legacy-automation.json --space-id <space-id> --domain default
mycel automation list --space-id <space-id> --domain default
mycel automation get <automation-id> --space-id <space-id> --domain default
mycel automation enable <automation-id> --space-id <space-id> --domain default
mycel automation disable <automation-id> --space-id <space-id> --domain default
mycel automation delete <automation-id> --space-id <space-id> --domain default
```

They are also available under the explicit legacy namespace:

```sh
mycel automation legacy validate legacy-automation.json
mycel automation legacy create legacy-automation.json --space-id <space-id> --domain default
mycel automation legacy put legacy-automation.json --space-id <space-id> --domain default
mycel automation legacy list --space-id <space-id> --domain default
```

To migrate old combined definitions into explicit procedure+binding records while
keeping the legacy file readable:

```sh
mycel automation migrate-combined --space-id <space-id> --domain default --dry-run
mycel automation migrate-combined --space-id <space-id> --domain default
```

Migration prints the source automation ID, generated procedure ID, binding ID,
runtime owner/on-behalf principal, and a warning when the legacy owner looks like
an operator/admin principal. The generated binding uses the same ID as the legacy
automation, so runtime selection prefers the explicit binding and avoids duplicate
execution while the legacy definition remains available for compatibility.

## Runs and invocations

```sh
mycel automation runs \
  --space-id <space-id> \
  --domain default \
  --automation <automation-or-binding-id> \
  --status failed \
  --limit 20

mycel automation run get <run-id> --space-id <space-id> --domain default

mycel automation invocation retry <invocation-id> --space-id <space-id> --domain default
mycel automation invocation cancel <invocation-id> --space-id <space-id> --domain default
```

Run records include neutral inference provenance such as profile, capability,
credential grant, policy decision, provider request ID, token usage, actor,
on-behalf-of, and automation owner references. Procedure/binding-backed run
records also include `binding_id`, `procedure_id`, `owner_principal_id`, and
`event_origin_principal_id` when available.

## Graph-context automation notes

Conditions can return node aliases in addition to `changed`. Those aliases can be
used as input targets and `update_node.target` values. Context queries are
read-only and must be bounded in GQL with `FETCH FIRST`; the optional `limit`
field further caps accepted rows. `gql_template` supports scalar interpolation
plus `{{#each name}}...{{/each}}` loops over named context result sets.

Target-scoped idempotency and debounce/coalescing are available under
`safety.idempotency` and `debounce`/`safety.debounce`, depending on whether the
configuration is split or legacy combined.

## Related docs

- [`mycel automation procedure`](procedure.md)
- [`mycel automation binding`](automation-binding.md)
- [Inference CLI](inference.md)
- [CLI index](README.md)
