# Security

## Reporting a vulnerability

This is a personal project and not a commercially supported product. Report
security issues privately through the repository host's private vulnerability
reporting, or directly to the maintainer — not in a public issue.

Please include what the issue allows, how to reproduce it, and the affected
version or commit.

## Scope

The client talks to a third-party game API. Issues in the Replicant Space
service itself belong with Replicant Space, not here. Relevant to this
repository:

- Token or credential leakage through logs, traces, panics, error messages,
  serialized state, or the daemon's HTTP/WebSocket surface.
- Unauthenticated or under-authenticated access to `replicantd`.
- Anything that lets the browser obtain `RS_API_TOKEN` or `REPLICANTD_TOKEN`.
- SQL injection or database corruption in the managed, history, workflow, or
  telemetry stores.

## Secret handling

Rules enforced across the codebase:

- API tokens are never persisted to any database.
- `tracing` output never records secret values, authorization headers, or
  request bodies. Duration fields are milliseconds.
- `RS_API_TOKEN` and `REPLICANTD_TOKEN` live in `.env`, which is gitignored.
  `.env.example` carries placeholders only.
- `REPLICANTD_TOKEN` is injected server-side by the web container and is never
  sent to the browser.
- Never commit tokens, authorization headers, private message bodies, or
  databases containing user data.

`replicantd` requires a token whenever it binds beyond loopback, which the
Compose stack does. Generate one with `make token`; rotate with
`make token-rotate`, then restart the stack and rebuild the web image.

## Local data

Managed, history, workflow, and telemetry databases contain account state and
message history. Treat them as one dated backup set, and close the client and
daemon before copying or removing them.
