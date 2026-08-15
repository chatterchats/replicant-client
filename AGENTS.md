# AGENTS.md — Replicant Application Migration

This file is shared context for every prompt in this pack. Read it before making changes.

## Mission

Evolve the existing `replicant-client` Rust workspace from a collection of capable but disjoint CLI commands/examples into a cohesive Replicant Space application platform:

- one durable managed Replicant client/runtime;
- reusable Reports, Actions, Planners, and Workflows;
- a persisted workflow supervisor that survives frontend disconnects and process restarts;
- a long-running local daemon (`replicantd`);
- a thin CLI frontend;
- a React/TypeScript GUI;
- an interactive galaxy map using the supplied `galaxy-renderer`;
- an interactive system map based on the supplied React source;
- higher-level event/state/schedule-driven automation;
- a Tauri desktop shell after the web/daemon architecture is stable.

The GUI is the primary rich frontend. The CLI remains supported as a second frontend and for diagnostics.

## Current Repository Shape

At the start of this pack, the workspace contains:

- root crate `replicant-client`
  - `src/raw/`: typed raw HTTP/API transport
  - `src/managed/`: durable state, managed `Client`, SQLite projections, operations, synchronization, travel, trading, etc.
  - managed event handling includes durable account-event application and a local `Client::events().watch()` style watcher.
- planner/library crates:
  - `replicant-route-planner`
  - `replicant-event-planner`
  - `replicant-mining-planner`
  - `replicant-bootstrap-planner`
  - `replicant-printing`
  - `replicant-transport`
- `replicant-cli`
  - printing and transport mostly call reusable libraries already;
  - significant execution/application logic still lives in CLI modules for survey, relay, mining, events, bootstrap, ownership, observatory, trade, Rikers/belt-search, etc.
- root examples including:
  - `clear_tags.rs`
  - `contribute_twaffy_injectors.rs`
  - `nearby_belt_report.rs`
  - other SDK examples.

Always inspect the actual repository before editing. The repository may have evolved since this prompt pack was created.

## Supplied Reference UI / Renderer

The user has explicit permission from the author to use the supplied `replicant.react` source and `galaxy-renderer`.
`replicant.react` is available at `/run/media/chats/22d0a494-68e2-4df8-9e89-ab37d31eb5b8/replicant.react/`
`galaxy-renderer` is available at `$REPO/crates/galaxy-renderer`

Reference React source contains useful patterns/components such as:

- `AppShell`
- command palette
- navigation
- `AutomationsPage`
- universe snapshot/delta state handling
- WebSocket connection logic
- device/location context menus
- `GalaxyPage`
- `GalaxyMapWasm`
- `SystemPage`
- `SystemMapGl`

The supplied `galaxy-renderer` is a Rust `cdylib`/WASM WebGL renderer with support for:

- stars;
- signals;
- relay/highlight/travel links;
- pulses;
- life/device/influence spheres;
- camera rotation/pan/zoom;
- picking stars/signals.

Reuse and adapt these assets rather than rewriting them from scratch unless there is a concrete incompatibility.

Preserve attribution/provenance for imported code. Do not remove notices that exist in supplied source. If the repository has no suitable third-party notice location, add a concise source/permission note when the renderer/UI code is first imported.

## Critical Event Architecture: SSE, Not Webhooks

Replicant Space deprecated webhooks in favor of SSE.

Therefore:

- **Upstream Replicant Space events enter the application through SSE.**
- Reuse the managed client's SSE synchronization and durable event journal.
- Workflows should consume the managed/local event stream, not open their own independent upstream SSE connections.
- Use durable history/cursors for gap recovery.
- A slow in-process watcher that lags must recover from durable event history/state rather than silently losing events.
- Do not add webhook endpoints, webhook trigger types, webhook configuration, or webhook terminology as an upstream event mechanism.
- **WebSockets are still appropriate locally** between `replicantd` and the GUI for snapshot/delta synchronization and live workflow/activity updates. Do not confuse this with the upstream Replicant Space event transport.

Preferred flow:

```text
Replicant Space
     |
     | SSE
     v
replicant-client managed event/sync pipeline
     |
     +--> durable SQLite projections/event journal
     |
     +--> local managed event watcher
                  |
                  v
            replicant-runtime
                  |
          workflow supervisor
                  |
     +------------+-------------+
     |                          |
 HTTP command/query API    local WebSocket deltas
     |                          |
   CLI                         GUI
```

## Core Architectural Rules

### 1. The managed client remains the Replicant Space authority

Do not reimplement SDK responsibilities in the runtime, daemon, CLI, or GUI.

Use the managed API by default for:

- rate limiting;
- SSE;
- durable event application;
- state projections;
- operation journaling;
- ambiguity/reconciliation behavior;
- managed mutations;
- typed domain models and queries.

`raw` is an explicit low-level escape hatch, not the normal solution for missing application wiring. If an application feature appears to need raw data, first determine whether the managed client should expose that information.

### 2. One long-lived managed Client per daemon/profile

Normal application operation should converge on:

```text
replicantd
  owns one managed Client
  owns one workflow supervisor
  owns runtime persistence
```

The GUI and normal CLI commands should not each create competing managed clients.

A diagnostic/direct CLI mode may remain available, but it is exceptional and should be explicit.

### 3. Keep layers separate

Use these conceptual categories:

**Query / Report**
- read-only;
- computes/returns information;
- examples: nearby belt report, status summaries.

**Action**
- finite mutation or bounded operation;
- examples: clear tags, contribution, one transport operation, ownership reassignment.

**Planner**
- computes a plan without owning background lifecycle;
- examples: route/event/mining/bootstrap planners.

**Workflow**
- durable, potentially long-running, multi-step orchestration;
- examples: survey route, relay expansion, mining expansion, event fulfillment, bootstrap campaign.

Do not turn every helper into a workflow.

### 4. Workflows are persisted state machines

The database row/checkpoint is the source of truth. A Tokio task is merely the current executor.

A workflow must be able to:

- persist configuration;
- persist current step/checkpoint;
- expose status;
- wait without busy polling where SSE/state-change signals are available;
- pause cooperatively;
- cancel cooperatively;
- resume;
- reconcile after restart;
- record useful activity/error information;
- identify claimed resources.

Use explicit states such as:

- queued;
- running;
- waiting;
- paused;
- reconciling;
- succeeded;
- failed;
- cancelled.

Do not attempt to suspend arbitrary Rust futures mid-mutation.

### 5. Resource claims prevent automation conflicts

Long-running workflows must be able to claim resources such as:

- replicants;
- vessels/carriers;
- devices;
- autofactories;
- other exclusive application resources as needed.

A device that merely looks idle in game state may still be reserved by another workflow.

Claims must be persisted and reconciled after restart. Avoid a second ad-hoc reservation mechanism inside each workflow.

### 6. Do not duplicate the managed operation journal

Replicant API mutations made by workflows/actions should still use the managed client's durable operation mechanisms.

Workflow checkpoints answer "where is this orchestration?"

Managed operation records answer "what happened to this API mutation?"

They are different layers and should remain different.

### 7. Preserve CLI compatibility during migration

Unless a prompt explicitly changes a command contract:

- keep existing command names/aliases/options working;
- move behavior behind reusable libraries/runtime services without unnecessarily changing output;
- preserve the current interactive CLI during the migration;
- avoid a flag-day rewrite.

When daemon-backed commands are introduced, direct/local operation may remain temporarily for compatibility and diagnostics.

### 8. Typed, readable APIs

Prefer strongly typed, readable Rust APIs and the project's fluent query style.

Avoid using loose `serde_json::Value` as an internal domain API when a type is practical. JSON is fine at network/storage boundaries and for extensible metadata where justified.

Do not persist user-editable saved queries as JSON. Query behavior belongs in typed Rust code.

### 9. Runtime and SDK persistence should remain logically separate

Preferred split:

- SDK/client database: Replicant/game projections, managed events, managed operations, client reconciliation.
- runtime database: workflow instances, checkpoints, claims, triggers, schedules, workflow activity, application-level settings.

They may reference stable IDs/codes across the boundary but do not create brittle cross-database foreign-key coupling.

### 10. Frontend is not authoritative

The React frontend:

- renders snapshots/deltas;
- keeps ephemeral UI state such as camera position, selected entity, filters;
- sends typed commands to the daemon;
- never owns the Replicant API key;
- never directly calls Replicant Space;
- never decides that an API mutation succeeded merely because a button was clicked.

### 11. Local daemon transport

Preferred first implementation:

- HTTP/JSON for request/response commands and queries;
- WebSocket for daemon -> GUI live deltas/activity.

Keep the protocol typed in a shared Rust protocol crate and mirrored/generated carefully on the TypeScript side.

Do not expose `replicantd` broadly by default.

- Normal native/standalone mode should bind to loopback by default.
- Container deployment may bind `replicantd` to `0.0.0.0` **inside an isolated Docker network** so the web/reverse-proxy container can reach it.
- The default Docker Compose deployment must not publish the daemon port directly to the host. Publish the web/proxy service and proxy application HTTP/WebSocket traffic internally to `replicantd`.
- A deliberately headless/advanced deployment may expose the daemon only when the operator explicitly configures that behavior.

### 12. Secrets

Never serialize API keys or sensitive auth data into GUI snapshots, WebSocket frames, logs, workflow metadata, error dumps, or generated fixtures.

Use the existing secret-handling conventions.

### 13. No premature distributed infrastructure

Do not add MongoDB, NATS, Redis, Kafka, etc. The existing architecture is intentionally local-first and SQLite-backed.

Add infrastructure only if a concrete demonstrated need appears later.

### 14. Do not hardcode one player's current operation into general runtime code

Existing operational defaults may remain where compatibility requires them, but new runtime/workflow APIs should accept typed parameters/config rather than baking in specific systems, tags, device codes, or replicant names.

## Suggested Target Workspace

Treat this as a direction, not a mandate to create all crates immediately:

```text
crates/
  replicant-runtime/
  replicant-workflow/
  replicant-workflows/
  replicant-protocol/
  replicant-server/
  replicant-cli/
  existing planner/library crates...
apps/
  web/
  desktop/
```

It is acceptable to keep domain-specific workflow implementation in existing crates if that produces cleaner ownership. The important boundary is that reusable gameplay logic does not remain owned by `replicant-cli`.

## Docker / Container Deployment

Docker is a first-class supported deployment target in addition to native CLI/web development and Tauri desktop packaging.

The preferred production container topology is:

```text
                         Docker host
+----------------------------------------------------------+
|                                                          |
|  published port                                          |
|       |                                                  |
|       v                                                  |
|  +-------------------+       private Docker network      |
|  | web / reverse     |-------------------------------+   |
|  | proxy container   |                               |   |
|  |                   |  /api + /ws                   |   |
|  | React static UI   |---------------------+         |   |
|  +-------------------+                     |         |   |
|                                            v         |   |
|                                  +----------------+  |   |
|                                  | replicantd     |  |   |
|                                  | Rust runtime   |  |   |
|                                  +-------+--------+  |   |
|                                          |           |   |
|                                          v           |   |
|                                   persistent volume  |   |
+----------------------------------------------------------+
                                           |
                                           | HTTPS/SSE/HTTP outbound
                                           v
                                    Replicant Space
```

Requirements:

- Provide a production image for `replicantd`.
- Provide a production image for the React UI that also acts as the same-origin reverse proxy for daemon HTTP and WebSocket endpoints.
- Provide Docker Compose for the normal full-stack deployment.
- Support a daemon-only/headless image for server/NAS/homelab operation.
- Use multi-stage builds so compilers/package managers are not shipped in final images unnecessarily.
- Final containers should run as non-root unless a concrete dependency makes that impossible.
- Add container health checks.
- Ensure SIGTERM/SIGINT cause graceful daemon shutdown/checkpointing.
- Persist SDK/client DB, runtime DB, and other required durable application data on mounted volumes/bind mounts. Container recreation must not reset automation/game state.
- Logs should go to stdout/stderr by default for container observability; optional file logging must write into an explicitly persistent/configured location.
- Never bake API keys, `.env` files, credentials, runtime databases, or player-specific configuration into images.
- Accept secrets through environment variables, mounted secret files, or Docker secrets/configuration as appropriate.
- Commit an `.env.example` only with safe placeholder values.
- Keep build contexts small with `.dockerignore`.
- Pin major tool/runtime choices sufficiently for reproducible builds; do not rely on `latest` tags.
- Make WebSocket proxy upgrade/timeout behavior correct for long-lived connections.
- Make SSE outbound connectivity from `replicantd` work normally; no inbound webhook ports are required.
- The frontend should use same-origin relative `/api` and `/ws` style endpoints in the containerized deployment where practical.
- Do not require Docker for normal Rust/React development or tests.
- Do not make Tauri depend on Docker. Docker and Tauri are independent deployment targets over the same daemon/web architecture.
- Prefer standard Compose/network/volume mechanisms over privileged containers, host networking, or Docker socket mounts.
- Do not mount the Docker socket into the application.
- Add a documented backup/restore path for persistent container data before considering the deployment production-ready.

When container-specific binding is needed, make it explicit through configuration (for example a bind/listen address) rather than weakening the native default.

## Runtime Event Model

Prefer application events/deltas that represent meaningful changes, e.g.:

- state revision changed;
- entity upsert/remove;
- workflow created/updated;
- workflow activity appended;
- managed operation updated;
- notification raised;
- daemon health/sync state changed.

Do not forward the entire raw upstream SSE feed directly to the browser as the application's protocol.

## GUI Direction

Primary navigation target:

```text
OPERATIONS
  Overview
  Galaxy
  System

ASSETS
  Devices
  Inventory
  Autofactory
  Cargo

MISSIONS
  Survey
  Mining
  Relay
  Events
  Bootstrap
  Trade

AUTOMATION
  Automations
  Requirements
  History

INTELLIGENCE
  Reports
  Messages
  Network
  Standing
  Leaderboards

Settings
```

This can evolve as implementation reveals better grouping.

Cross-cutting GUI concepts:

- persistent top status area;
- command palette;
- global selected-entity inspector;
- collapsible activity/debug drawer;
- Active / Templates / Schedules / History automation views;
- smart typed selectors for SYSTEM, LOCATION, REPLICANT, DEVICE, DEVICE TYPE, etc.;
- context actions from galaxy/system/device/location views.

## Backend-Defined Workflow/Action Descriptors

Prefer a descriptor registry so the frontend does not need a bespoke form for every operation.

Descriptor data may include:

- stable kind ID;
- display name;
- description;
- category;
- operation class (report/action/workflow);
- typed parameter schema;
- defaults;
- validation hints;
- risk/mutation classification;
- supported trigger types.

Example parameter kinds:

- string;
- integer/number;
- boolean;
- enum;
- system;
- location;
- replicant;
- device;
- device type;
- tag;
- path/file only where appropriate for a local UI.

The frontend renders selectors based on the semantic type.

## Automation Trigger Model

When triggers are eventually added, supported concepts should include:

- Manual;
- Schedule;
- SSE/GameEvent condition;
- StateCondition;
- ParentWorkflow.

Do **not** add Webhook.

Prefer conditions evaluated against managed state/events over high-frequency polling.

## Observability

Use `tracing` consistently.

Logs should make it possible to answer:

- which workflow/action initiated a mutation;
- current workflow instance and step;
- what it is waiting for;
- what changed after reconciliation;
- why it retried or failed;
- which resource claim blocked execution.

Never log secrets.

## Testing Expectations

Add tests at the correct layer:

- unit tests for pure planners/state transitions;
- SQLite/runtime tests for persistence and restart behavior;
- integration tests for workflow lifecycle and claims;
- protocol serialization compatibility tests;
- server route/WebSocket tests;
- frontend tests/build checks where practical;
- Docker image/Compose configuration validation and container smoke tests where practical;
- regression tests for bugs discovered while applying prompts.

Prefer deterministic tests using fixtures/fakes over sleeping for real game timings.

## Dirty Working Tree Safety

Codex may be operating in a repository with user changes.

- Inspect `git status` before editing.
- Do not reset, checkout, clean, stash, discard, or overwrite unrelated user changes.
- Do not amend an unrelated existing commit.
- Stage only files intentionally changed for the current prompt.
- If pre-existing changes overlap a file you need to edit, preserve them and work around them carefully.
- Never use destructive Git commands merely to make tests pass.

## Scope Discipline

Each prompt in this pack is intended to become one reviewable commit.

- Complete only the requested prompt.
- Do not opportunistically implement later phases.
- Small supporting refactors are fine when required for the prompt.
- Keep backward compatibility unless the prompt explicitly authorizes removal.
- Prefer finishing the current vertical slice over creating many empty abstractions.

## Mandatory Prompt Finalization Protocol

Every implementation prompt must be finalized as follows.

1. Review the diff and status:
   - `git status --short`
   - inspect the diff for accidental/unrelated edits.
2. Run formatting:
   - `make fmt`
3. Run the full local CI-equivalent suite:
   - `make ci`
4. If **any** formatter, compiler, Clippy, test, documentation, policy, frontend check, or other CI step fails:
   - diagnose it;
   - fix the root cause;
   - add/update regression coverage when appropriate;
   - run `make fmt` again;
   - run `make ci` again;
   - repeat until the entire suite passes.
5. Do not declare the prompt complete while `make ci` is failing.
6. Once all checks pass:
   - inspect `git status --short` and the final diff again;
   - stage only the files changed for this prompt;
   - create **one Conventional Commit** that accurately covers the work done by this prompt.

Examples:

```text
feat(runtime): add shared application context
refactor(relay): extract relay execution from cli
feat(workflow): persist workflow instances and checkpoints
feat(server): expose runtime snapshot api
feat(web): add daemon-backed application shell
feat(galaxy): integrate wasm galaxy renderer
build(docker): add containerized deployment
fix(workflow): reconcile claimed resources after restart
test(workflow): cover sse-driven wait recovery
```

Use an appropriate Conventional Commit type such as `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, or `build`.

Do not combine unrelated changes into the commit. Do not commit pre-existing unrelated working-tree changes.

If the prompt genuinely requires no file changes, report that clearly instead of creating an empty commit. Otherwise, a successful prompt ends with a green `make ci` and one scoped commit.

## Completion Report for Each Prompt

At the end of a Codex run, report:

- what changed;
- key design choices;
- tests added/updated;
- `make fmt` result;
- `make ci` result;
- commit hash and Conventional Commit subject;
- any intentionally deferred work that belongs to a later prompt.
