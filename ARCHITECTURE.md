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

## Automation Director

Intent-native workflows are the execution layer, not the strategic control plane. The
Automation Director continuously reconciles standing empire goals against managed game state and
creates or reuses durable **batch/campaign workflows** when work is required.

```text
standing empire goals
        |
        v
Automation Director
  +-- discovered / established regions
  +-- permanent regional Replicant assignments
  +-- objective / blocker / next action
  +-- regional workforce pressure
        |
        v
regional goal instances / campaign planners
        |
        v
durable intent-native workflows
        |
        v
managed operations
```

The Director never issues game commands directly. Mechanical work remains inside registered
workflows and managed client operations. Director state (settings, goal controls, regional
assignments, goal runtime, and workforce pressure) is persisted in the workflow database through
the generic `runtime_documents` store.

The Director has three operating modes:

- `off`: preserve configuration and report state without planning new work;
- `advisory`: reconcile goals and report blockers / next actions without launching work;
- `automatic`: reconcile goals and create the required campaign workflows.

The initial standing goals are intentionally batch-oriented rather than one-goal-per-object:

- **Establish Regions** discovers regions without an owned foothold, grows a two-Replicant
  bootstrap pool when required, and runs one regional bootstrap campaign at a time. This serial
  establishment policy prevents newly discovered regions from causing a burst of simultaneous
  cloning/ark construction. Newly established regions automatically become eligible for regional
  goals.
- **Expand Star Catalogue** uses owned galactic observatories to prospect for undiscovered stars.
- **Enhance Star Catalogue** runs regional survey tours over known systems that still need survey
  coverage. Large regional backlogs are partitioned into disjoint exact-system shards across up to
  four idle region-assigned Replicants/racing vessels. The shard backlog contributes real regional
  worker pressure, so the grow-only workforce policy can add catalogue capacity when useful survey
  work is persistently waiting rather than cloning merely because utilization is high.
- **Expand Mining Ops** batches uncovered known belt systems into regional mining campaigns.
- **Event Completion** batches active regional events into campaign planning, staging, routing,
  and completion.
- **Expand FTL Network** and **Establish Beacons** are persisted goal kinds but remain disabled by
  default until their autonomous placement/scoring policies are implemented. Explicit frontier,
  relay, event, and bootstrap workflows remain available in the meantime.

### Regions and worker ownership

A Replicant may be permanently assigned to an operating region. The Director automatically makes
an initial assignment from live location when a Replicant has no saved assignment, but it never
automatically moves or clears an existing assignment. Regional campaign planners only consume
workers assigned to that region, preventing normal automation from sending an Alpha worker across
the galaxy to service Beta work merely because it is momentarily idle.

Cross-region movement remains an explicit workflow/operator concern. Region aliases are
canonicalized at the Director boundary, while previously unknown future region names remain valid
without code changes. A region may contain multiple system hubs; the Director deterministically
chooses the hub system with the strongest manufacturing footprint as the regional capital, then
prefers an owned Autofactory in that system as the campaign home. This avoids letting an arbitrary
relay/expansion hub become the operating centre merely because it appeared first in device state.

### Grow-only Replicant workforce

Automated workforce management is deliberately **grow-only**. There is no Director operation,
workflow, or policy that deletes, retires, decommissions, or otherwise scales down Replicants.
Idle Replicants are retained permanently.

Scale-up is based on regional useful-work pressure rather than utilization alone. Ordinary
established regions must have campaign work blocked on a missing worker, remain below the idle
reserve threshold for a sustained hold period, and respect a scale-up cooldown. Establishing a new
region is the exception: it may explicitly request the two-worker bootstrap pool even before that
region has a local hub. The resulting `replicant.provision` workflow prints an empty Replicant
matrix and cradle vessel at an established manufacturing home, performs replication, and records
the new Replicant as permanently assigned to the target region.
