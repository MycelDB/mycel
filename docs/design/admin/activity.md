# Admin Activity API

The Admin Activity API exposes durable operator activity events for Console,
CLI, auditors, and trusted external services.

RPCs:

- `AppendActivityEvent` — appends a sanitized event and deduplicates retries by
  source plus idempotency key. Requires `audit.write`.
- `ListActivityEvents` — lists newest-first events with bounded pagination and
  filters for severity, category, type, source, actor, resource, and
  correlation id. Requires `audit.read`.
- `GetActivityEvent` — fetches one event by id. Requires `audit.read`.

Activity events are evidence, not authority. They must not be used as a command
bus or as input to automatic repair, merge, rebalance, or split-brain recovery.
Raft/system metadata remains authoritative.

Metadata must be sanitized by emitters. The daemon rejects obvious
secret-bearing metadata keys such as password, token, secret, api key,
credential, and private key.

See also: [Activity Events Design](../activity/README.md).
