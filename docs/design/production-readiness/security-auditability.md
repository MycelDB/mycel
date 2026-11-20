# Basic security and auditability

Status: draft  
Last updated: 2026-11-19

## Production requirement

Production operations require strong controls around backups, restore, admin
actions, delegated sessions, credentials, and diagnostic artifacts. CommonFolio
production must have basic security and auditability before real customer data is
stored in Mycel.

The full security program can mature over time, but the first production launch
must protect high-risk operations and avoid leaking secrets or customer data.

## Mandatory first-cut capabilities

### Protected administration

- Admin authentication enforced for backup, restore, cluster membership,
  maintenance, and usage-accounting APIs.
- Least-privilege access for backup storage such as S3.
- Explicit operator permissions for destructive operations.

### Backup security

- Backup data stored in encrypted storage or encrypted before upload.
- Backup manifests avoid embedding secrets.
- Restore operations require explicit authorization.
- Backup credentials are not logged or included in Mycel Lab artifacts.

### Audit trail

The highest-risk actions should emit audit records, including:

- backup create;
- backup verify;
- restore;
- add pod, remove pod, drain pod, or replace pod when supported;
- admin auth/session changes;
- destructive maintenance commands;
- security-sensitive delegated-session actions.

### Redaction

Logs and reliability artifacts must not expose:

- bearer tokens;
- refresh tokens;
- TLS private keys;
- object-store credentials;
- customer payload data;
- private attachment contents;
- other sensitive tenant data.

## Launch gate

CommonFolio production should require:

1. admin authentication on production-critical APIs;
2. protected and encrypted backup storage;
3. no known token/secret leakage in logs or artifacts;
4. audit records for backup, restore, and other destructive/admin actions;
5. documented IAM/credential model for the AWS deployment.

## Follow-up design work

- Define the admin audit event schema and retention policy.
- Define backup/restore IAM guidance for AWS.
- Add sensitive-field redaction tests.
- Define break-glass operator behavior and required audit evidence.
- Decide how audit records are exposed to CommonFolio or external audit sinks.
