# Minimum observability and alerting

Status: draft  
Last updated: 2026-11-19

## Production requirement

CommonFolio production needs enough visibility to detect data-loss risk,
availability risk, quota-accounting risk, and degraded search behavior before
users are affected or backups become untrustworthy.

Perfect observability is not required for first production, but a minimum health
and alerting baseline is mandatory.

## Mandatory first-cut signals

### Cluster health

- pod/daemon health;
- Raft leader presence;
- quorum status;
- under-replicated or unavailable partitions;
- leader distribution when available.

### Storage and capacity pressure

- persistent volume capacity;
- used and free disk/PVC bytes;
- object-store access failures when object storage is part of the deployment;
- storage pressure alerts.

### Backup health

- last successful backup time;
- last backup failure reason;
- last successful restore verification or restore drill time;
- stale-backup alert.

### Search and projection health

- lexical index availability;
- semantic projection readiness;
- projection lag/freshness;
- semantic consistency policy outcomes where applicable;
- search channel degraded/unavailable status.

### Usage accounting health

- usage measurement freshness;
- counter reconciliation failures;
- counter authority/freshness errors;
- inability to produce quota-safe usage.

### API health

- client/admin API request latency;
- error rate by major gRPC status class;
- authentication/session error trends, including stale delegated-session reasons.

## Mandatory first-cut capabilities

Before CommonFolio production, operators should have:

- a cluster health dashboard;
- alert rules for quorum risk, storage pressure, backup failure/staleness,
  projection/index outage, usage accounting staleness, and elevated API errors;
- structured logs that preserve correlation IDs while redacting credentials and
  private data;
- runbooks linked from alerts or alert descriptions.

## Launch gate

CommonFolio production should require:

1. dashboards for cluster health, storage pressure, backup health, search health,
   usage freshness, and API error/latency basics;
2. alert rules for the highest-risk conditions;
3. operator runbooks for those alerts;
4. evidence that backup and usage-accounting alerts fire in staging or Mycel Lab
   when the corresponding failure is injected.

## Follow-up design work

- Define the metrics contract and naming scheme.
- Decide which metrics are daemon-native versus deployment/operator-provided.
- Define log redaction rules and tests.
- Add traces for high-value request paths such as writes, search, backup, and
  restore.
- Build richer Console/operator UI surfaces after the metrics contract is stable.
