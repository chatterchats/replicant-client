# Phase 9 AGENTS.md — Application Pages & Live Projections

This file is supplemental common context for the Phase 9 prompt pack.

**Before editing, also read and obey the repository-root `AGENTS.md`.**  
If this file conflicts with the repository-root `AGENTS.md`, the repository-root file wins unless the current prompt explicitly narrows the Phase 9 implementation.

## Phase 9 Mission

Replace the remaining placeholder React navigation pages with useful, daemon-backed application views.

At the beginning of this phase, the web shell is real, Galaxy/System are real, and the Automation pages are real, but most other navigation entries still fall through to one generic placeholder page.

The target is not merely to make every route look different. Each page should expose a typed application projection built from the managed Replicant client and the existing runtime/daemon architecture.

## Current Repository State

The Phase 9 pack was prepared against the Stage 8-era repository uploaded on 2026-08-15.

Relevant current structure includes:

```text
apps/web/
  src/App.tsx
  src/api.ts
  src/daemon.tsx
  src/protocol.ts
  src/GalaxyPage.tsx
  src/SystemPage.tsx
  src/AutomationsPage.tsx
  src/RequirementsPage.tsx
  src/HistoryPage.tsx

crates/
  replicant-protocol/
  replicant-runtime/
  replicant-server/
  replicant-workflow/
  replicant-cli/
  ...
```

The current navigation is approximately:

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

At the start of Phase 9, these have dedicated page implementations:

- Galaxy
- System
- Automations
- Requirements
- History

The others currently fall through to the generic placeholder in `App.tsx`.

Always inspect the actual repository before editing because it may have changed since this pack was generated.

## Important Existing Behaviors to Preserve

### Managed Client Is Still Authoritative

Do not implement page data by directly calling the upstream Replicant API from React or from ad-hoc server HTTP clients.

Use the existing managed client and runtime:

```text
Replicant Space
     |
     | SSE + managed HTTP
     v
replicant-client
     |
     v
replicant-runtime
     |
     v
replicantd
     |
     +--> typed HTTP projections
     |
     +--> local WebSocket invalidations/deltas
               |
               v
             React
```

- upstream game events: SSE;
- daemon-to-GUI updates: local WebSocket;
- no webhook architecture.

### Do Not Turn RuntimeSnapshot Into "Everything"

`GET /api/snapshot` is intentionally a compact runtime/lifecycle snapshot.

Do not stuff every device, inventory row, message, event, trade, leaderboard entry, and settings object into `RuntimeSnapshot`.

Instead use typed domain snapshots/endpoints similar in spirit to:

```text
GET /api/galaxy-scene
GET /api/system-scene/:system
```

Examples of appropriate future page projections:

```text
GET /api/overview
GET /api/devices
GET /api/inventory
GET /api/autofactories
GET /api/cargo
GET /api/missions/...
GET /api/events
GET /api/trade
GET /api/messages
GET /api/network
GET /api/standing
GET /api/leaderboards
GET /api/settings
```

Exact endpoint grouping may be adjusted if a cleaner typed design emerges.

### Complete Live Projection Plumbing

Current protocol already has concepts such as:

- `DomainSlice`;
- `LiveDelta::EntityUpsert`;
- `LiveDelta::EntityRemove`;
- `LiveDelta::DomainInvalidated`.

At the start of this phase:

- `RuntimeSnapshot` does not carry entity data;
- the frontend clears `entities` to `{}` whenever a runtime snapshot is applied;
- the frontend knows how to process entity upsert/remove deltas;
- the server does not normally publish entity upserts;
- managed state changes currently invalidate `DomainSlice::Universe`, but most other domain slices are not driven by the state revision watcher.

Phase 9 should finish the intended live-projection architecture instead of creating unrelated polling loops per page.

A good model is:

```text
managed state revision/event changes
        |
        v
replicantd
        |
        +--> invalidate affected domain slice(s)
        |
        +--> optional small normalized entity updates/index
        |
        v
local WebSocket
        |
        v
React marks slice stale
        |
        v
visible/interested page refetches typed projection
```

A coarse invalidation followed by a typed refetch is acceptable when the managed client cannot cheaply identify the exact changed entity.

Correctness and maintainability matter more than micro-optimizing invalidation during this phase.

### Entity Index vs Page Projection

Normalized entities are useful for:

- global inspector;
- shell/current-replicant context;
- command-palette defaults;
- cross-page selection;
- links from history/activity.

They should **not** become a giant `unknown` blob store that replaces typed page DTOs.

Prefer a small typed entity index/summary projection for cross-cutting shell functionality, while Devices/Inventory/etc. use their own typed snapshots.

### Frontend Page Data Pattern

Prefer one reusable pattern for domain pages:

```text
typed protocol DTO
       |
server projection builder
       |
HTTP endpoint
       |
daemonApi parser/client
       |
page hook/cache
       |
DomainSlice invalidation
       |
refetch
```

Do not copy a slightly different `useEffect(fetch(...))` implementation into every page.

The shared page-data helper should support:

- initial loading;
- abort on unmount/request replacement;
- explicit refresh;
- invalidation-triggered refresh;
- loading state without blanking useful stale data;
- error state;
- empty state;
- revision/last-updated information when useful.

### Use Existing Registered Operations

For mutations and workflows, page buttons should invoke the existing:

- descriptor catalogue;
- finite reports/actions;
- durable workflows;
- command palette/runtime command path.

Do not reimplement Survey/Mining/Relay/Bootstrap/Event/Trade algorithms in React.

Mission pages are **domain dashboards around existing operations**, not alternate workflow engines.

### Global Inspector

The right-hand inspector should become more useful during this phase.

Prefer typed summaries/cards for known entity kinds where practical rather than defaulting to raw JSON.

However, do not make every page dependent on finishing a perfect inspector. Improve it incrementally.

### No Strategic Huwanu/Regional Logistics Yet

A separate future design exists for Regional Event Logistics / Strategic Event Readiness.

Phase 9 should implement a useful normal Events page and existing event fulfillment capabilities, but must **not** expand scope into:

- operational bases;
- strategic reserve optimization;
- true galaxy-wide event ETA;
- regional convoy planning;
- Huwanu-scale strategic fulfillment.

The Events page should be designed so those features can be added later.

## Page UX Principles

### Tables Are Fine — But Use Them Well

Dense operational data belongs in tables when a table is the clearest representation.

Useful table behavior may include:

- search;
- sort;
- filters;
- sticky headings where helpful;
- row selection;
- multi-select only when bulk actions exist;
- compact status chips;
- links to system/location/entity inspector;
- meaningful empty states.

Do not replace useful dense data with a wall of oversized cards merely for aesthetics.

### Keep Visual Language Consistent

Reuse existing shell/map/automation styling:

- existing typography;
- panel/card treatment;
- spacing;
- status dots/chips;
- buttons;
- inspector;
- command palette.

Do not import a second design system for Phase 9.

### Loading / Error / Empty

Every data-backed page must distinguish:

```text
loading
error
empty
loaded
```

Do not show "0 devices" when the daemon request actually failed.

### Cross-Page Navigation

Entity links should support flows such as:

```text
Devices -> select device -> inspector
Inventory row -> open location/system
Mission -> workflow detail
Event -> Galaxy
Galaxy/System -> domain page where appropriate
History -> affected entity
```

Use the shell's existing selection/navigation mechanisms rather than one-off browser URLs where practical.

### Read-Only First

For complicated pages, first land a reliable read-only projection and then connect existing operations.

Do not block the entire page on implementing every possible mutation.

## Domain Expectations

These are intended UX goals, not requirements to invent unavailable backend data.

### Overview

Useful sections can include:

- daemon/sync/automation health;
- replicants and current locations;
- active travel;
- active workflows;
- workflows needing attention;
- active/high-value events;
- resource totals;
- autofactory utilization;
- recent activity;
- current notifications.

Avoid duplicating the entire contents of every other page.

### Devices

Expected high-value columns/filters where data exists:

- code;
- type;
- status;
- system/location;
- owner/replicant;
- tag;
- attached/stowed/controller relationship;
- health/maintenance/capacity;
- workflow claim if available.

Support search and useful filters.

### Inventory

Support both:

- location-centric view;
- resource-centric aggregation.

Useful concepts:

- resource totals;
- system/location;
- region if known;
- selected-resource distribution;
- selected-location contents.

### Autofactory

Useful concepts:

- factory code/location/owner;
- active job;
- queue;
- availability/status;
- remaining time;
- aggregate utilization/throughput.

Reuse existing printing functionality for commands.

### Cargo

Useful concepts:

- carrier/vessel;
- current location;
- cargo contents;
- cargo capacity/remaining capacity;
- attached-device capacity where applicable;
- active transport workflow/claim;
- transport actions.

### Mission Dashboards

Survey / Mining / Relay / Bootstrap should summarize domain state and expose existing planners/workflows.

Do not duplicate Automation workflow-detail UI.

### Events

Initial Phase 9 Events page should support what the current client/runtime actually knows:

- active/discovered events;
- location/system;
- type/category/tier;
- criteria;
- progress;
- rewards;
- existing event workflow state;
- inspect/show on Galaxy;
- existing plan/run actions where registered.

Device rewards should be displayed if the current event model supports them by implementation time. If not, do not silently invent them; surface the limitation in the implementation report and keep DTO design extensible.

### Trade

Use the existing managed trading/runtime APIs. Show useful controllers/orders/trades and existing actions. Do not create a second trade engine.

### Reports

This page should be a discoverable UI for registered read-only Report descriptors and their recent results.

### Messages

Use managed BobNet/message capabilities. Prefer channel/relay/history semantics supported by the current SDK.

### Network

Build from actual managed/account/relay/network data. Do not invent a social graph if the SDK does not expose one.

### Standing

Show actual achievement/civilisation/reputation/standing information available from managed state/API.

### Leaderboards

May be refresh-on-demand rather than persistent managed state if the current SDK/API models it that way. Keep it typed and daemon-mediated.

### Settings

Settings should expose **application/runtime configuration**, not secrets.

Potential categories:

- daemon/profile;
- default replicant;
- preferred home/system/location values already represented in runtime config;
- logging;
- automation safety defaults;
- UI settings;
- Docker/headless/desktop environment information where useful;
- data/database paths in a safe form.

Do not return API keys to React.

Write support should use explicit typed update APIs and safe validation.

## Testing Expectations

For each prompt:

- Rust projection builder tests;
- protocol serde/round-trip/parser tests;
- server route tests;
- frontend reducer/hook/component tests;
- regression tests for discovered bugs.

Prefer fixture/state-based tests.

Do not require a live Replicant account for `make ci`.

When adding page-data hooks, test invalidation/refetch behavior without timing-dependent sleeps where practical.

## Scope Discipline

Each numbered prompt in this pack should produce **one reviewable Conventional Commit**.

Do not opportunistically implement later prompts.

Small support refactors are allowed when required, but avoid sweeping unrelated architecture changes.

## Mandatory Finalization

Every implementation prompt must end with:

1. inspect:
   - `git status --short`
   - final diff for accidental edits;
2. run:
   - `make fmt`
   - `make ci`
3. fix **all** failures, including Rust, Clippy, tests, docs, policy checks, web checks, and desktop checks;
4. rerun until everything is green;
5. stage only the files intentionally changed for the prompt;
6. create exactly one scoped Conventional Commit.

Examples:

```text
feat(projections): add typed domain snapshot plumbing
feat(web): add operations overview
feat(devices): add device fleet dashboard
feat(inventory): add resource inventory views
feat(manufacturing): add autofactory dashboard
feat(missions): add survey and mining dashboards
feat(events): add event and trade pages
feat(intelligence): add reports and communications pages
feat(settings): add runtime settings page
```

Do not commit unrelated pre-existing user edits.

## Completion Report

At the end of each Codex prompt, report:

- what changed;
- important design choices;
- endpoints/DTOs added;
- tests added/updated;
- `make fmt` result;
- `make ci` result;
- commit hash and subject;
- anything intentionally deferred.
