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
| `REPLICANT_PROFILE` | `default` | Local profile name |
| `REPLICANT_DB` | `replicant-client.sqlite` | Managed SDK database |
| `REPLICANT_RUNTIME_DB` | `replicant-runtime.sqlite` | Workflow/runtime database |
| `REPLICANTD_BIND` | `127.0.0.1:8080` | HTTP listen address |
| `RUST_LOG` | `info` | Tracing filter |

The default binding is loopback-only. Binding to a non-loopback address is an explicit advanced deployment choice and should only be done behind an authenticated same-origin proxy or on an isolated container network.

HTTP routes are rooted at `/api`:

- `GET /api/health`
- `GET /api/snapshot`
- `GET /api/descriptors`
- `GET, POST /api/workflows`
- `GET /api/workflows/{id}`
- `GET /api/workflows/{id}/activity`
- `POST /api/workflows/{id}/pause`
- `POST /api/workflows/{id}/resume`
- `POST /api/workflows/{id}/cancel`

Responses use the versioned types from `replicant-protocol`. Upstream game events remain managed-client SSE traffic; this server does not expose webhook endpoints.
