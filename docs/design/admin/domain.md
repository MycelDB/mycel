# Admin Domain API

## Status

Implemented daemon-backed operator lookup MVP.

Protobuf source:

```text
github.com/myceldb/mycel-api/api/proto/mycel/admin/v1/domain.proto
```

## Service

```text
mycel.admin.v1.AdminDomainService
```

Implemented RPCs:

- `ListDomains`
- `GetDomain`

## Purpose

`AdminDomainService` provides operator-facing domain lookup helpers for admin semantic and inference workflows. It removes the previous need to pass raw domain UUIDs to daemon admin commands.

`GetDomain.domain_ref` accepts:

- domain UUID
- stable domain key such as `default`
- empty string, which resolves the default domain

## Authorization

Requires an operator bearer token with either:

- `CAPABILITY_DOMAIN_READ`, or
- `CAPABILITY_SEMANTIC_SEARCH`

The semantic capability allowance lets semantic/inference admins resolve domains needed for provisioning without requiring broader domain administration.

## CLI

```sh
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin domain list --space-id '<space-id>'

./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin domain get default --space-id '<space-id>'
```

Daemon admin semantic/inference commands now accept domain keys:

```sh
semantic index add notes-search --space-id '<space-id>' --domain default ...
inference grant openai-key --space-id '<space-id>' --domain default ...
inference policy allow --space-id '<space-id>' --domain default
semantic index backfill notes-search --space-id '<space-id>' --domain default
```
