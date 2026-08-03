# Security Policy

## Supported versions

`replicant-client` has not yet published a `1.0.0` release. Security
guidance below applies to the `main` branch during development.

## Report a vulnerability

Do not disclose vulnerabilities in public issues, discussions, pull
requests, or commits. Use the repository's private security-advisory
feature, or the maintainers' private contact method if that is unavailable.

Include the affected commit or version, the Replicant Space version
(`2.3.5`), impact, reproduction steps, and any known mitigation.

## Consumer security responsibilities

- Load API tokens from a secret manager or protected environment. Never
  commit, print, trace, or serialize them.
- Keep TLS verification enabled; validate custom base URLs and proxies in
  your deployment threat model.
- Protect SQLite databases and backups with operating-system access
  controls. Backups can contain account state even when tokens are not
  intentionally persisted.
- Do not retry an ambiguous unsafe mutation blindly; treat it as a durable
  operation to reconcile instead.

The client is designed to redact authentication material from `Debug`
output, diagnostics, and the operation journal. This is not a substitute for
reviewing your own tracing configuration and panic reports.

Administrative `/v1/admin/**` operations are intentionally excluded from
this client; see `policy/operations.json`.
