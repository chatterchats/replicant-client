# Architecture

This describes the architecture as **currently implemented**, not an
aspirational target, with two explicit exceptions called out inline: the
`Expand FTL Network` and `Establish Beacons` Director goals are persisted goal
kinds that remain disabled by default.

Read this file before changing the runtime / workflow / daemon layering, adding
a workflow kind, or touching Director behaviour. `AGENTS.md` carries the
repository map and the short version of the authority rules.

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

### Managed-store reserved tables

The managed inbox is persisted incrementally in `messages`; its cursor, unread count, and refresh
time live in `message_metadata`. The older generic provenance scaffold remains intentionally
reserved rather than partially implemented. In particular, BobNet history is volatile,
relay/request-scoped data and is not cached, while the remaining unused tables have no hot-path
consumer. `source_documents` cannot be dropped safely without rebuilding populated SQLite tables
whose legacy foreign-key columns still reference it, so that destructive cleanup is deferred until
those columns have a tested forward migration.

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
`salvage.site`, `mining.deploy`, `logistics.delivery`, `logistics.regional_dispatch`,
`exploration.frontier`, `event.delivery`, `event.tour`, and `observatory.search`.

Legacy `survey.route`, `relay.expansion`, `mining.expansion`, `event.fulfillment`, and
`requirement.fulfillment` remain registered for persisted-workflow and CLI compatibility, but
are categorized as `compatibility` and are not offered by the normal web/Tauri operation
picker.

Mining Ops additionally uses the internal `mining.transport_capacity` workflow for durable AMI
Cargo Freighter count changes. The Director owns only capacity policy and structural health; the
operating AMI Transport Controller continues to own tactical route execution and automatically
picks up newly adopted transports without being recreated or restarted.

Mining Ops also uses the internal `mining.maintenance_rotation` workflow for one exact worn
remote maintenance drone and `mining.maintenance_pool` for the regional repair pool. Remote mining
sites intentionally keep exactly one maintenance drone; a patrolling drone below 30% operational
capacity is rotated rather than treated as missing hardware. Replacements are delivered with the
shared logistics planner and the worn drone is not recovered until replacement patrol is verified.
Returned drones go to the region's exact manufacturing/home belt, join the mutually repairing hub
patrol pool, and remain non-ready until authoritative operational capacity reaches 95%. The hub
pool has a hard minimum of two healthy patrol drones and a target of three; extra drones remain
regional stock and are never automatically decommissioned.

`logistics.regional_dispatch` is the operator-facing regional provisioning workflow. Its source
may be the regional hub system, the owned System Hub device location, or the exact manufacturing
home. Execution resolves all three forms to an owned Autofactory location in that same hub system
before stock selection, printing, replication, and delivery staging. It reserves reusable unclaimed
vessels, empty Replicant matrices, and requested devices before printing only the shortfall; a vessel that already carries
an empty matrix is preferred, and loose empty matrices may be paired with vessels that still need
to be printed. Requested Racing, HEAVEN, and Cargo vessels receive an empty matrix, are replicated
into from any claimable local Replicant vessel when one is available, and otherwise remain empty.
The workflow also ensures resource/device transport exists, then hands the complete manifest to
the shared logistics executor, whose travel legs use managed smart hub routing.

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

Regional goal instances have independent enable controls. Empire-wide goals (`Establish Regions`,
`Expand Star Catalogue`, and `Blueprint Acquisition`) retain one global control. A regional
override is keyed by the stable goal instance identity (`kind:region`); regions without an override
inherit the pre-existing goal-kind setting so upgrades preserve operator policy.

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
  A server response that no new stars are visible from the sampled sparse directions is treated as
  an exhausted deployment rather than a failed workflow; the Director does not retry that same
  observatory deployment until the owned observatory/location signature changes.
- **Enhance Star Catalogue** runs regional survey tours over known systems that still need survey
  coverage and are within 30 LY (3D galactic distance) of the selected regional hub. Systems beyond
  that operating radius are deliberately left out of the standing survey footprint. Large regional
  backlogs are partitioned into disjoint exact-system batches of at most 20 systems per worker across
  up to four idle region-assigned Replicants/racing vessels. Exact
  Season Three re-survey batches use bounded targeted star-knowledge reads rather than traversing
  the complete Replicant-star pagination before every tour. The shard backlog contributes real regional
  worker pressure, so the grow-only workforce policy can add catalogue capacity when useful survey
  work is persistently waiting rather than cloning merely because utilization is high.
- **Discover Belts** searches known systems within 30 LY (3D galactic distance) of each selected
  regional hub, prioritizing targets outward by distance rather than catalogue name. Systems beyond
  that operating radius are deliberately left out of the standing belt-search footprint.
  Already-explored systems use the durable managed location projection when available; the workflow
  only revisits an explored system when location data is absent, because remote location reads are
  presence-gated.
- **Mining Ops** retains the persisted `ExpandMiningOps` / `expand_mining_ops` identity while
  reconciling a protected regional mining footprint. It repairs existing productive stacks,
  rotates worn remote maintenance drones, restores AMI transport service, services critical
  backlog, and preserves the regional maintenance reserve before protection, normal scaling, or
  new expansion. Expansion remains within 30 LY of the selected regional hub and follows the
  dense/moderate/sparse policy; already-managed in-range sites remain managed if their density is
  later disabled for new expansion.
  Healthy routes are capacity-managed from authoritative exact-belt inventory and the durable
  per-route backlog trend. Backlog above 30 Cargo Freighter loads suppresses expansion; eligible
  backlog above 10 loads scales before expansion, while 2–10 loads alone does not block it.
  Flat/rising backlog triggers a narrow authoritative route refresh before bounded scale-up, and a
  stuck route already at six usable freighters surfaces an actionable blocker. Sustained low
  backlog releases at most one extra freighter to regional stock.
  Established inter-system ferries that lose valid FTL reach raise the existing regional
  Connectivity requirement instead of rewriting their Transport Controller. Up to four non-hub
  mining systems receive System Wards by density and distance; a donor ward is never stripped
  before destination connectivity is authoritative.
  These thresholds (30 LY, 500 units per planning load, 10/30-load backlog bands,
  one-to-six route freighters, 30% remote rotation, 95% replacement readiness,
  hub minimum two/target three, and four priority wards) are **client policy**,
  not Replicant Space API contract guarantees.
  `DirectorSnapshot.mining_ops` reports independent regional observations:
  exact-belt site health, route health including FTL reach, usable adopted Cargo
  Freighters, known backlog, remote maintenance, unclaimed replacement-ready hub
  patrol stock, priority ward coverage, and expansion candidates. It defaults to
  an empty collection for legacy snapshots; unknown inventory or an unresolved
  hub is not presented as a healthy zero. `mining_policies` remains operator
  expansion-density policy only. The UI never derives health by parsing prose
  and does not combine these dimensions into a health percentage.
  These site and route totals use the existing Director footprint: known managed
  sites beyond 30 LY are excluded, while in-range managed sites remain included
  regardless of disabled expansion-density classes. They are not an all-owned-assets census.
- **Maintain System Hubs** keeps each operational hub stocked at its exact location. The
  `quantity_per_20pct` value is the authoritative material cost of one 20% repair tranche: when
  every upkeep resource has at least two complete tranches on hand, no shipment is launched; when
  any resource falls below two, the Director replenishes every upkeep resource to five tranches.
  Explicit missing/deficit fields are used only as a legacy fallback when no tranche rate is
  available, and generic total requirement fields are never treated as deficits.
- **Event Completion** batches active regional events into campaign planning, staging, routing,
  and completion.
- **Expand FTL Network** prioritizes strategic event, mining, and explicit connectivity targets
  within 30 LY (3D galactic distance) of each selected regional hub. Systems beyond that operating
  radius are deliberately left out of the standing FTL footprint.
- **Establish Beacons** remains disabled by default until its autonomous placement/scoring policy
  is implemented. Explicit frontier, relay, event, and bootstrap workflows remain available in the
  meantime.
- **Asteroid Diversion** remains disabled by default while its event-driven diversion mechanics are
  operator-proven. A diversion reserves the regional Autofactory and occurrence resources actually
  required by the campaign rather than parking a regional Replicant for the life of the asteroid.
  Propulsor print tags use a compact deterministic `ast-div:` identity that fits the server tag limit.
  Propulsor sizing is an explicitly versioned Director planning heuristic, and completion still
  requires terminal occurrence evidence rather than merely observing deployed hardware.
- **Stranded Device Recovery** remains disabled by default and retains the conservative failed-placement
  provenance/census checks used to prove that an exact device is safe to recover. Owned-device authority
  requires the Essential Account+Devices baseline plus event continuity, live SSE, and healthy durable
  storage; it does not require the unrelated Full startup baseline. Its execution path is
  Replicant-first: a local-control `logistics.manifest` claims a region-assigned Replicant and hosted
  vessel, travels that authority to the disconnected origin before cleanup or pickup, re-reads the exact
  device, then either commands a travel-capable device home directly or uses normal carrier logistics.
  Once local authority is established, ordinary transport staging is allowed: non-travel-capable device
  recovery can bring in an attachment-capable carrier rather than requiring the Replicant's cradle vessel
  to carry the payload. Delivery completion is checkpointed before the Replicant returns so restart
  recovery cannot replay the shipment.
- **Unserviced Resources** remains disabled by default and means one-shot recovery of positive regional
  resource stock outside the regional hub system and outside systems already in the managed mining
  footprint. When enabled, the Director establishes a bounded authoritative account-inventory baseline
  on demand and reuses it for five minutes while event continuity remains healthy. Positive stock whose
  system has no regional authority stays outside every regional goal until that system is classified; it
  does not block recovery of known regional stock. Candidates are prioritized by recoverable quantity
  and dispatched through a Replicant-led
  local-control `logistics.manifest`. After the Replicant establishes authority at the source, the normal
  transport planner stages cargo-capable transport (for example a cargo freighter) to collect the stock,
  deliver it to the exact regional hub, and return borrowed transport while the Replicant returns home.
  Persistent AMI transport-route maintenance belongs to **Mining Ops**, not this goal.

### Regions and worker ownership

A Replicant may be permanently assigned to an operating region. The Director automatically makes
an initial assignment from live location when a Replicant has no saved assignment, but it never
automatically moves or clears an existing assignment. Regional campaign planners only consume
workers assigned to that region, preventing normal automation from sending an Alpha worker across
the galaxy to service Beta work merely because it is momentarily idle. `racing_vessel` and
`heaven_vessel` cradles both qualify as operational regional worker vessels. General/event and
local-control work prefers an idle Heaven vessel when available so the faster Racing vessels remain
free for catalogue, survey, FTL, and other speed-sensitive work that explicitly requires them.

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

Scale-up is based on regional useful-work pressure rather than utilization alone. Once a region
has its bootstrap population, ordinary growth must have campaign work blocked on genuine missing
capacity, remain below the idle reserve threshold for a sustained hold period, and respect a
scale-up cooldown. Travelling, workflow-busy, and reserved assigned workers are transient capacity,
not immediate evidence for cloning.

Establishing a new region may explicitly request a two-worker bootstrap pool before it has a local
hub. Permanently assigned workers and non-terminal `replicant.provision` workflows for that region
both count toward the target, regardless of current operational availability. Bootstrap growth
stops at that target; a region remaining `establishing` does not disable ordinary safeguards. The
provision workflow prints an empty Replicant matrix and cradle vessel, performs replication, and
records the new Replicant as permanently assigned to the target region. Ordinary growth prefers an
owned Autofactory at the designated regional home, falling back to an established manufacturing
region only when no usable local source Replicant is available.
