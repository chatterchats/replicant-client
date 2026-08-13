# Target Architecture Snapshot

```text
Replicant Space
      |
      | upstream SSE + managed HTTP
      v
+-----------------------------+
| replicant-client            |
| managed state / operations  |
| rate limit / SSE / SQLite   |
+--------------+--------------+
               |
               v
+-----------------------------+
| replicant-runtime           |
| reports / actions / queries |
+--------------+--------------+
               |
               v
+-----------------------------+
| replicant-workflow          |
| supervisor / claims / waits |
| checkpoints / triggers      |
+--------------+--------------+
               |
               v
+-----------------------------+
| replicantd                  |
| HTTP commands/queries       |
| local WebSocket deltas      |
+------+----------------------+
       |                  |
       |                  |
       v                  v
replicant-cli          React GUI
                          |
                          v
                     Tauri shell
```

## Event distinction

- Replicant Space -> application: **SSE**
- daemon -> GUI: **WebSocket**
- No Webhook trigger architecture.

## Authority distinction

- managed client DB = game/API truth and operation reconciliation;
- runtime DB = application/workflow truth;
- frontend store = disposable projection/cache.


## Deployment Targets

The same runtime architecture supports three independent deployment styles:

### Native development / headless

```text
replicant-cli ---> replicantd ---> Replicant Space
                       |
                       +-- persistent local databases
```

### Docker / server deployment

```text
Browser
  |
  v
Web static server + reverse proxy   [published]
  |
  | private Docker network: /api + /ws
  v
replicantd                           [not published by default]
  |
  +-- persistent Docker volume(s)
  |
  +-- outbound SSE/HTTP -> Replicant Space
```

The proxy provides same-origin access for the browser and WebSocket upgrades. `replicantd`
may listen on all interfaces **inside the private container network**, while native mode
continues to default to loopback.

### Tauri desktop

```text
Tauri/React UI ---> local replicantd ---> Replicant Space
```

Tauri does not require Docker, and Docker does not require Tauri.

## Container Persistence / Secrets

- SDK managed-state database and runtime/workflow database must survive container replacement.
- Never bake API keys, databases, `.env`, or player-specific config into an image.
- Prefer environment variables, mounted secret files/Docker secrets, and explicit persistent volumes.
- Container logs default to stdout/stderr.
- Default Compose publishes only the web/proxy port, not the daemon.
