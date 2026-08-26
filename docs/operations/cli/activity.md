# Activity CLI

Activity events are durable operator history records for daemon lifecycle,
identity, space/domain, backup, cluster, semantic, automation, and trusted
external-service activity.

Activity events are evidence only. They do not drive repair, rebalance, merge,
or other operational mutations.

## List events

```sh
mycel admin activity list --page-size 50
mycel admin activity list --category lifecycle
mycel admin activity list --severity warning --severity error
```

Requires `audit.read`.

## Get one event

```sh
mycel admin activity get evt_...
```

Requires `audit.read`.

## Append an external event

Trusted service principals can append sanitized external events:

```sh
mycel admin activity append --file event.json
```

Example event file:

```json
{
  "severity": "info",
  "category": "external",
  "type": "external.service.started",
  "message": "Worker service started",
  "source": { "service": "worker" },
  "idempotencyKey": "worker-start-2026-08-18T00:00:00Z"
}
```

Requires `audit.write`. Never include passwords, tokens, credential material,
private keys, or raw sensitive provider responses in metadata.
