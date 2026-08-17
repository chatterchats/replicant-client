# Current UI / Projection State at Start of Phase 9

This document records the specific repository observations that motivated the phase. It is a snapshot, not an eternal invariant; Codex must inspect the current tree before relying on it.

## Placeholder Rendering

`apps/web/src/App.tsx` currently has dedicated branches for:

- `AutomationsPage`
- `RequirementsPage`
- `HistoryPage`
- `GalaxyPage`
- `SystemPage`

All other navigation values fall through to a generic article displaying roughly:

```text
<page title>

Live application state is synchronized through the local daemon.

Daemon connection
Daemon state is current.
Revision N
```

The fallback also contains a generic "Live entities" section, but that section normally has no data.

## Runtime Snapshot

`replicant-protocol::RuntimeSnapshot` currently contains:

- snapshot metadata;
- managed sync state;
- global automation state;
- workflow summaries;
- requirement summaries;
- notifications.

It does not contain the full entity set or domain page projections.

That compactness should be preserved.

## Existing Live Protocol

`DomainSlice` currently includes at least:

- Universe
- Devices
- Inventory
- Autofactories
- Workflows
- Operations

`LiveDelta` includes:

- Snapshot
- EntityUpsert
- EntityRemove
- DomainInvalidated
- WorkflowCreated
- WorkflowUpdated
- WorkflowActivity
- OperationUpdated
- Notification
- AutomationChanged
- daemon/sync status changes

The frontend already has reducer logic for entity upsert/remove and domain invalidation.

## Current Entity Gap

When the frontend applies a fresh runtime snapshot, `entities` is currently reset to `{}`.

The server does not currently use `EntityUpsert` as the normal managed-state bootstrap path.

This means cross-cutting shell functionality that expects a normalized entity list cannot rely on it yet.

Phase 9.1 should address this deliberately.

## Existing Typed Projection Pattern

The server already exposes:

```text
GET /api/galaxy-scene
GET /api/system-scene/:system
```

with typed protocol DTOs and frontend parsers/API methods.

Phase 9 should extend this model to the rest of the application rather than using raw JSON from the browser.

## Current Managed APIs Worth Reusing

The managed client already exposes gateways including:

- `client.devices()`
- `client.replicants()`
- `client.directory()`
- `client.galaxy()`
- `client.events()`
- `client.state()`
- `client.operations()`
- `client.messages()`
- `client.locations()`
- `client.location_events()`
- `client.inventory()`
- `client.bobnet()`
- `client.trading()`
- `client.simulations()`

`StateGateway` already exposes managed revision watches and inventory projection access.

The implementation should add managed/domain APIs only when a clean typed read is genuinely missing. Do not jump to `raw` merely because the server projection has not been written yet.

## Existing Operation Catalogue

`replicant-runtime` already has one catalogue for registered:

- Reports
- Actions
- Workflows

The UI should call those existing application capabilities for page actions rather than embedding gameplay logic in page components.

## Existing Deployment / CI

The repository already has:

- React web checks;
- Tauri desktop checks;
- Docker support;
- `make fmt`;
- `make ci`.

Phase 9 pages must remain compatible with browser, Docker, and Tauri deployments.
