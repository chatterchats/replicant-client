# replicantd

`replicantd` is the long-running local Replicant application process. It owns one managed client, one workflow database, and one workflow supervisor.

Start it with:

```sh
RS_API_TOKEN=... cargo run -p replicant-server --bin replicantd
```

Configuration is environment-based:

| Variable | Default | Purpose |
| --- | --- | --- |
| `RS_API_TOKEN` | required | Replicant Space authentication token |
| `RS_API_TOKEN_FILE` | unset | File containing the token when `RS_API_TOKEN` is absent |
| `REPLICANT_PROFILE` | `default` | Local profile name |
| `REPLICANT_DB` | `replicant-client.sqlite` | Managed SDK database |
| `REPLICANT_RUNTIME_DB` | `replicant-runtime.sqlite` | Workflow/runtime database |
| `REPLICANTD_BIND` | `127.0.0.1:8080` | HTTP listen address |
| `RUST_LOG` | `info` | Tracing filter |

A non-empty `RS_API_TOKEN` takes precedence over `RS_API_TOKEN_FILE`. Token
files are trimmed when read and neither source is printed in logs or status.

The default binding is loopback-only. Binding to a non-loopback address is an explicit advanced deployment choice and should only be done behind an authenticated same-origin proxy or on an isolated container network.

The daemon exposes these local routes:

- `GET /api/health`
- `GET /api/snapshot`
- `GET /api/entities`
- `GET /ws` (WebSocket upgrade)
- `GET /api/descriptors`
- `GET, POST /api/workflows`
- `GET /api/workflows/{id}`
- `GET /api/workflows/{id}/activity`
- `POST /api/workflows/{id}/pause`
- `POST /api/workflows/{id}/resume`
- `POST /api/workflows/{id}/cancel`

Responses use the versioned types from `replicant-protocol`. Upstream game events remain managed-client SSE traffic; this server does not expose webhook endpoints.

Each WebSocket connection starts with a `snapshot` message naming the current application revision; the frontend then fetches `/api/snapshot` and applies subsequent typed messages in revision order. Reconnecting always starts from a fresh snapshot. The server sends WebSocket ping frames every 15 seconds and closes connections that do not pong within 45 seconds. Live updates use a bounded buffer; a lagging client receives a fresh `snapshot` message and is disconnected so it can reconnect without applying an uncertain delta sequence.

## Recovery and troubleshooting

On startup the supervisor reports counts for resumable, paused, and terminal workflows and releases only orphaned or terminal resource claims. Running and waiting workflows enter reconciliation before execution. Managed operation records and SSE-backed durable state remain the evidence for mutations whose workflow checkpoint was interrupted.

Workflow config/checkpoint JSON is versioned independently from the runtime database. A registered factory must explicitly transform every supported old version. Unsupported or malformed state is failed with its original payload and history retained; it is never passed to a current executor. A newer or gapped database migration history, failed SQLite integrity check, or unreadable workflow row prevents that reconciliation tick from starting automation.

For a consistent backup, gracefully stop `replicantd`, then use SQLite's online-backup command for both databases:

```sh
mkdir -p backup
sqlite3 replicant-client.sqlite ".backup 'backup/replicant-client.sqlite'"
sqlite3 replicant-runtime.sqlite ".backup 'backup/replicant-runtime.sqlite'"
sqlite3 backup/replicant-client.sqlite "PRAGMA integrity_check;"
sqlite3 backup/replicant-runtime.sqlite "PRAGMA integrity_check;"
```

Use the paths configured by `REPLICANT_DB` and `REPLICANT_RUNTIME_DB`. Keep the two files as one dated backup set. Do not copy a live `.sqlite` file by itself because WAL contents may be omitted. To restore, stop the daemon, preserve the failed files for diagnosis, replace both databases from the same backup set, verify `PRAGMA integrity_check` returns `ok`, then restart and inspect the startup reconciliation summary.

If startup or reconciliation fails:

- Do not delete the runtime database or manually release claims; that can permit duplicate automation.
- Preserve the database and daemon log, and inspect the actionable schema/integrity error.
- For an unsupported workflow checkpoint, upgrade to code containing its explicit migration or restore the matching application/database backup. Do not edit the JSON version by hand.
- A disconnected GUI is not evidence of daemon failure. Reconnect it and let the initial snapshot replace uncertain local deltas.

API tokens are accepted only through the managed client's secret handling. Workflow debug output omits serialized payloads, HTTP errors are generic, result/parameter keys that look sensitive are redacted, and frontend failure notifications never include stored error text.
