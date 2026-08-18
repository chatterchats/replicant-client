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

## Intent-driven automation

Web and Tauri automation is goal-oriented rather than CLI-shaped. Frontends submit a small,
typed intent to a registered durable workflow; the workflow owns device selection, resource
claims, progress, and restart state.

```text
UI / Tauri intent
      |
      v
replicant-workflow instance
      |
      +-- authoritative checkpoint_json
      +-- resource claims / parent-child relationships
      |
      v
replicant-runtime automation blocks and existing managed executors
      |
      v
replicant-client managed operations / state
```

The initial intent-native workflow kinds are `scan.system`, `scan.belt`, `scan.tour`,
`salvage.site`, `mining.deploy`, `logistics.delivery`, `exploration.frontier`,
`event.delivery`, `event.tour`, and `observatory.search`.

Legacy `survey.route`, `relay.expansion`, `mining.expansion`, `event.fulfillment`, and
`requirement.fulfillment` remain registered for persisted-workflow and CLI compatibility, but
are categorized as `compatibility` and are not offered by the normal web/Tauri operation
picker.

### Checkpoint authority

The workflow database is authoritative for intent-native execution state. Where a mature legacy
executor still requires a mission/plan file, the intent workflow materializes that file into a
workflow-owned temporary directory from its checkpoint immediately before calling the executor,
and writes the resulting state back into `checkpoint_json`. A missing durable checkpoint deletes
any stale temporary adapter file rather than trusting it after restart.

### Parent and child work

`WorkflowContext::create_child` persists `parent_id` automatically, and
`WorkflowContext::child_workflows` allows a restarted coordinator to rediscover work it created
before a crash. For example, `event.tour` reuses an existing matching `event.delivery` workflow
(or creates one as a child), waits until staging succeeds, and only then claims the Replicant and
resolves the event. This keeps manufacturing/logistics independent from Replicant dispatch while
making both phases visible to the workflow UI.
