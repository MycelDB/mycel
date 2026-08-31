# Security Policy

## Reporting a vulnerability

Please report suspected security vulnerabilities privately through GitHub Security Advisories / private vulnerability reporting for this repository.

Use the repository's **Security** tab and choose **Report a vulnerability**. Do not open a public issue with vulnerability details.

If private vulnerability reporting is not visible, open a public issue that says only that you need a private security reporting channel. Do not include exploit details, affected secrets, proof-of-concept code, private data, or infrastructure details in the public issue.

## What to include

When possible, include:

- The affected mycel version, commit, release tag, or Docker image tag.
- The affected binary, package, subsystem, API, CLI command, configuration path, or deployment mode.
- A description of the vulnerability and expected impact.
- Reproduction steps or proof-of-concept details.
- Whether the issue affects standalone, clustered/raft, backup/restore, TLS/mTLS, authentication, authorization, import/export, semantic/inference, automation, or storage behavior.
- Any known mitigations or workarounds.

## Scope

Security reports may apply to mycel when behavior could expose data, weaken authentication or authorization, mishandle credentials, bypass TLS expectations, retry unsafe operations, corrupt or leak storage/backup data, break raft consistency guarantees, leak sensitive telemetry, or enable unintended destructive operations.

Vulnerabilities in the API contract, SDKs, deployment charts, or downstream applications may be redirected to the corresponding MycelDB repository after initial triage.

## Sensitive information

Do not include secrets, passwords, bearer tokens, refresh tokens, TLS private keys, production data, private user data, or confidential infrastructure details in public issues, pull requests, logs, screenshots, test fixtures, or AI prompts.
