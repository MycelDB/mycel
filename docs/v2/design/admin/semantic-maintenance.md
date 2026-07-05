# Admin Semantic Maintenance API

## Status

Implemented daemon-backed MVP.

Protobuf source:

```text
api/proto/mycel/admin/v1/semantic_maintenance.proto
```

## Service

```text
mycel.admin.v1.AdminSemanticMaintenanceService
```

Implemented RPCs:

- `AnalyzeSemanticDirtyWork`
- `ProcessSemanticDirtyWork`
- `BackfillSemanticIndex`

## Authorization

Requires an operator bearer token with the semantic/inference admin capability currently represented by:

```text
CAPABILITY_SEMANTIC_SEARCH
```

## CLI

Daemon-backed commands are used when `--daemon-addr` is supplied:

```sh
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic maintenance analyze --space-id '<space-id>'

./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic maintenance process --space-id '<space-id>' --limit 10

./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic index backfill '<semantic-index-id-or-key>' \
  --space-id '<space-id>' \
  --domain '<domain-id>' \
  --force \
  --continue-on-error
```

When the backfill index argument is a key, daemon mode uses AdminDomainService so `--domain` may be a domain key such as `default` or a domain UUID.

## Notes and limitations

- Backfill currently uses the `mycel-file` vector backend.
- The worker processes pending dirty work in a bounded single pass; durable job scheduling is future work.
- Legacy embedding migration remains an embedded CLI path until its engine/store dependencies are extracted into a daemon service.
