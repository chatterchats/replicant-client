# Replicant Client 1.0 Rewrite and Conversion Guide

**Status:** Authoritative implementation guide  
**Target crate:** `replicant-client`  
**Target version:** `1.0.0`  
**Replicant Space contract:** `2.3.1`  
**Rust crate import:** `replicant_client`  
**Primary public type:** `replicant_client::Client`

This guide is intended to be read by every implementation prompt working on the new crate. It records the product decisions, current game semantics, conversion boundaries, architecture, implementation sequence, and release acceptance criteria.

---

## 1. Repository and source paths

Use these paths exactly unless the user explicitly changes them:

```text
NEW_REPO=/run/media/chats/0c7bd812-03b4-405c-9602-31282b68fd64/replicant-client/

OLD_REPO=/run/media/chats/22d0a494-68e2-4df8-9e89-ab37d31eb5b8/replicant-space-rust-sdk/
```

The new repository is the only write target.

The old repository is a read-only reference source for:

- raw transport code;
- request and response models;
- endpoint implementations;
- event parsing and reduction;
- domain conversion and authority logic;
- SQLite repository patterns;
- state indexes and typed query builders;
- command journaling;
- reconciliation algorithms;
- tests, fixtures, scripts, and policy checks;
- the managed-client prototype completed through the old 1.1.0 prompt 3.

Do not continue developing the old crate as a release product. Do not preserve its public API, package layout, feature tiers, module names, database schema, or compatibility obligations unless this guide explicitly adopts a particular behavior.

### 1.1 Upstream documentation location

The corrected Replicant Space 2.3.1 documentation must be copied into the new repository at:

```text
$NEW_REPO/reference/replicant-space/
```

The corrected documentation archive used to prepare this guide had:

```text
Archive SHA-256:
a6b02569b17fcfccf29b4f439777df9864c04b91f89d43f2da464b3c28f65e8d

OpenAPI SHA-256:
ca018a938541f23c4838e8fe58f78889d9ca4b9ab81b488112f90589dd83c2f4
```

The copied contract corpus must include:

```text
reference/replicant-space/openapi.json
reference/replicant-space/api/
reference/replicant-space/concepts/
reference/replicant-space/ami/
reference/replicant-space/simulations/
reference/replicant-space/rate-limits/
```

Add a small checked-in metadata file recording:

- Replicant Space version `2.3.1`;
- crawl timestamp;
- documentation archive checksum;
- OpenAPI checksum;
- the fact that rendered documentation asides override missing OpenAPI deprecation flags.

Do not assume the documentation inside the old repository is corrected. Verify its deprecation asides and checksums before using it as the new contract corpus.

---

## 2. Immediate decision on the unfinished 1.1.0 work

**Stop the old 1.1.0 prompt sequence after prompt 3. Do not run prompts 4–8.**

The completed work is still valuable:

- Prompt 1 is a prototype for integrated `Client` ownership and builder delegation.
- Prompt 2 is a prototype for one-request managed reads that commit before returning.
- Prompt 3 is a prototype for a managed synchronization surface.

The remaining prompts should not be completed because they target the wrong product:

- Prompt 4 assumes event-stream behaviors that the corrected documentation does not guarantee. In particular, an old SSE cursor does not produce a distinct rejection; the server silently begins at the earliest retained event.
- Prompt 5 is constrained by the old exhaustive `TypedCommand` and 1.x compatibility requirements. The new crate should design a new durable operation model.
- Prompt 6 rewrites documentation for the predecessor crate, which is no longer the target product.
- Prompt 7 enforces old feature tiers and compatibility.
- Prompt 8 prepares a release that will not happen.

### 2.1 Stabilize the old repository as a reference checkpoint

Perform only checkpoint work in the old repository:

```sh
cd "$OLD_REPO"

cargo fmt --all
cargo test --all-features
cargo check --all-features --examples
```

Fix only regressions introduced by prompts 1–3. Do not add the remaining lifecycle, command, documentation, or release work.

Then commit or tag the state clearly, for example:

```sh
git add -A
git commit -m "checkpoint managed client prototype through sync API"
git tag prototype/managed-client-prompt-3
```

Do not bump the old crate to 1.1.0. Do not publish another old-crate release solely for this checkpoint.

Future prompts should inspect the actual prompt 1–3 implementation in `$OLD_REPO`; the older archive may not contain those changes.

---

## 3. Product identity

`replicant-client` is a new product, not version 2 of the old public API.

Its product statement is:

> A durable, stateful Rust client for building Replicant Space applications.

It is client-centered rather than assembly-centered. The ordinary user should not need to construct a transport client, runtime, state actor, persistence repositories, event reducers, command executor, or cancellation graph manually.

The crate remains broad enough to qualify as a client SDK because it includes:

- typed raw API access;
- domain models;
- local durable state;
- fluent queries;
- live subscriptions;
- event history;
- synchronization and reconciliation;
- durable asynchronous operations;
- game-specific high-level interfaces;
- current OpenAPI and documentation fixtures.

The primary workflow is:

```rust
use replicant_client::{Client, SecretString};

let client = Client::builder()
    .authentication_token(SecretString::from(token))
    .sqlite("replicant-client.sqlite")
    .start()
    .await?;

client.ready().await?;

let account = client.account().get().await?;

let miners = client
    .devices()
    .miners()
    .idle()
    .at("SOL")
    .collect()
    .await?;

let miner = client.devices().get(&miners[0].code).await?;

let operation = miner.activate().await?;
let outcome = operation.wait().await?;

client.close().await?;
```

All examples must use:

```rust
let client = Client::builder()
```

Do not use `let sdk = ...`.

---

## 4. Locked release decisions

The following decisions are not open to reinterpretation by implementation prompts:

1. The package and repository name are `replicant-client`.
2. The initial version is `1.0.0`.
3. The crate targets Replicant Space `2.3.1`.
4. The repository is a single root Rust package, not a multi-crate workspace.
5. Source code lives under root `src/`, not `crates/`.
6. `replicant_client::Client` is the normal entry point.
7. There is no public `Runtime`.
8. There is no compatibility layer for the predecessor crate.
9. There are no old module aliases.
10. Deprecated game endpoints and fields are not part of the public managed API.
11. Deprecated game endpoints are not exposed through `raw` either.
12. Admin-only endpoints are excluded.
13. The managed client uses SQLite as a required durability component.
14. Raw transport access remains available deliberately through `replicant_client::raw`.
15. The corrected rendered documentation and its asides override incomplete OpenAPI deprecation metadata.
16. Queries are defined in Rust through fluent typed APIs. They are not persisted as editable JSON.
17. Managed local queries perform no hidden network requests.
18. Managed network reads commit and publish before returning success.
19. Unsafe managed mutations are durably registered before transmission.
20. SSE is a low-latency observation channel, not the sole source of truth.
21. REST reconciliation is the correctness mechanism.
22. Simulation state and live-world state are isolated by realm.
23. Public directory data must not erase richer owned-account data.
24. Public enums and event models must tolerate additive upstream values.

---

## 5. Contract authority and exclusions

The corrected documentation corpus contains 84 OpenAPI operations across 70 paths.

The clean client excludes seven operations:

### Deprecated operations

```text
GET    /v1/accounts/webhook
POST   /v1/accounts/webhook
DELETE /v1/accounts/webhook

GET    /v1/replicants/{replicant_code}/events

GET    /v1/locations/{designation}/inventory
```

The current inventory endpoint is:

```text
GET /v1/inventory
```

### Administrative operations

```text
POST /v1/admin/message
POST /v1/admin/story/advance
```

That leaves **77 current, non-admin operations** to classify and support.

### Deprecated fields

Do not expose these as normal account settings:

```text
message_notify
```

Normalize deprecated mining response aliases internally:

```text
belt        -> location
designation -> site
```

A raw DTO may deserialize those aliases when the server still emits them, but the public normalized model should expose only `location` and `site`.

### BobNet documentation inconsistency

The account-settings documentation still contains an older sentence stating that BobNet messages can only be delivered by webhook. The corrected deprecation aside and event catalogue define `bobnet.new` and direct new integrations to the account event system.

The client must:

- consume `bobnet.new` from account events;
- support relay history from `/v1/devices/{code}/messages`;
- support channel discovery;
- omit webhook delivery support;
- avoid promising webhook-only behavior.

---

## 6. Recommended Cargo feature model

Keep features small and product-oriented:

```toml
[features]
default = ["managed", "rustls-tls"]

raw = [
  # HTTP transport, authentication, models, pagination, rate-limit metadata
]

events = [
  "raw",
  # SSE parsing and raw event streaming
]

managed = [
  "events",
  # SQLite, state, synchronization, operations, managed Client
]

rustls-tls = [
  # reqwest rustls feature
]

native-tls = [
  # reqwest native-tls feature
]
```

Requirements:

- `managed` is enabled by default.
- `Client` exists only with `managed`.
- `raw::Client` exists with `raw`.
- `events` allows raw SSE use without the managed store.
- Do not recreate the old `api`, `state`, `sqlite`, `runtime`, and `full` feature maze.
- SQLite is mandatory when `managed` is enabled.
- Support in-memory SQLite through the builder for tests and temporary applications.
- Do not create a `compat` feature.

---

## 7. Public module shape

Keep the public surface conceptually flat:

```text
replicant_client
├── Client
├── ClientBuilder
├── ClientStatus
├── Error
├── Result
├── SecretString
├── Realm
├── Device
├── DeviceHandle
├── Replicant
├── ReplicantHandle
├── Account
├── Location
├── Operation
├── OperationStatus
├── OperationOutcome
├── SyncReport
├── Event
├── EventStream
│
├── account
├── devices
├── replicants
├── directory
├── galaxy
├── locations
├── inventory
├── messages
├── bobnet
├── events
├── location_events
├── blueprints
├── achievements
├── reputation
├── trading
├── simulations
├── leaderboards
├── operation
├── state
├── sync
└── raw
```

Root-reexport common types used in normal application code:

```rust
use replicant_client::{
    Client,
    DeviceStatus,
    DeviceType,
    Realm,
    SecretString,
};
```

Do not force users through deeply nested implementation paths.

### 7.1 Public versus internal boundaries

Public:

- managed `Client`;
- domain gateways;
- snapshots and handles;
- typed query builders;
- event and state subscriptions;
- operation handles;
- synchronization reports;
- raw current-contract client and raw DTOs.

Internal:

- runtime/task orchestration;
- SQL repositories;
- migrations and transaction coordinator;
- state actor implementation;
- event reducers;
- hydration adapters;
- reconciliation queue;
- operation journal state machine;
- request scheduler;
- lifecycle cancellation;
- source-document storage.

There must be no public `Runtime`, `CommandExecutor`, `RuntimeHydrationJob`, repository trait, or state actor handle.

---

## 8. Managed client ownership

The implementation should converge on an internal shape similar to:

```rust
#[derive(Clone)]
pub struct Client {
    inner: std::sync::Arc<ClientInner>,
}

struct ClientInner {
    raw: raw::Client,
    scheduler: RequestScheduler,
    store: Store,
    state: StateEngine,
    events: EventEngine,
    sync: SyncEngine,
    operations: OperationEngine,
    lifecycle: Lifecycle,
}
```

The exact internal names may differ, but the ownership rules are fixed:

- one shared raw transport configuration;
- one account-wide rate-limit scheduler;
- one durable store;
- one state publication engine;
- one event journal and reducer pipeline;
- one operation journal;
- one reconciliation queue;
- one lifecycle shared by every clone.

### 8.1 Builder

The builder should support at least:

```rust
Client::builder()
    .authentication_token(...)
    .base_url(...)
    .sqlite(...)
    .in_memory()
    .startup_policy(...)
    .request_timeout(...)
    .connect_timeout(...)
    .read_rate_limit_policy(...)
    .action_rate_limit_policy(...)
    .event_stream_options(...)
    .reconciliation_policy(...)
    .tracing(...)
    .start()
    .await
```

The builder must:

- redact authentication in `Debug`;
- delegate shared raw-client validation;
- validate mutually exclusive TLS features;
- open and migrate SQLite;
- restore durable state before publishing readiness;
- prevent accidental use of one database by a different account;
- clean up partial startup after failure.

### 8.2 Account binding

Persist a non-secret account identity after authentication.

If a later token identifies a different account while using the same database, fail with a structured account/store mismatch error. Do not merge two accounts into one state database.

Do not persist bearer tokens.

### 8.3 Lifecycle

Required methods:

```rust
client.status()
client.watch_status()
client.ready().await?
client.close().await?
```

`close()` must be:

- idempotent;
- safe across clones;
- responsible for cancelling and joining producers;
- responsible for flushing durable state;
- observable through `ClientStatus`;
- successful when called again after a clean close.

Dropping the final client clone may trigger cancellation as a safety net, but only `close()` guarantees a fully awaited shutdown.

---

## 9. Startup and readiness

Use explicit status states such as:

```rust
#[non_exhaustive]
pub enum ClientStatus {
    Starting,
    Restoring,
    CatchingUp,
    Synchronizing,
    Connecting,
    Ready,
    Degraded(ClientDegradation),
    Offline,
    Closing,
    Closed,
}
```

Recommended default startup policy:

```rust
StartupPolicy::Essential
```

`start()` should return after:

- configuration validation;
- database migration;
- local restoration;
- lifecycle task ownership is established.

`ready()` should wait for the configured startup policy.

Recommended policies:

```rust
pub enum StartupPolicy {
    RestoreOnly,
    Essential,
    Full,
}
```

- `RestoreOnly`: local state is usable; no required initial remote sweep.
- `Essential`: account identity, owned account device baseline, owned replicant baseline, event catch-up, and live event connection.
- `Full`: all bounded account domains except intentionally cached/global/volatile surfaces.

The star catalogue should not be downloaded automatically during every startup.

---

## 10. Managed read contract

A successful managed read means:

```text
request
→ decode and validate
→ normalize into domain observations
→ apply endpoint-specific authority rules
→ commit SQLite transaction
→ publish state revision
→ return domain value or handle
```

For example:

```rust
let device = client.devices().get(code).await?;
```

When it returns successfully, the device is already durable and visible to local queries and subscribers.

Do not return the remote DTO as success if persistence or publication failed.

Do not issue a second HTTP request merely to reuse a refresh method.

Do not install a generic “persist every valid response” interceptor. Endpoint semantics determine:

- entity identity;
- ownership;
- visibility;
- authority;
- completeness;
- reconciliation scope;
- whether absence is meaningful.

### 10.1 Local query contract

These methods are local-only:

```rust
client.devices().find()
client.devices().miners()
client.devices().cached(code)
client.state()
```

They must never perform an implicit network request.

Network behavior is explicit:

```rust
client.devices().get(code).await?
client.devices().refresh(code).await?
client.devices().sync().await?
```

Use consistent vocabulary:

- `cached`: local lookup;
- `find`: local query;
- `get` or `refresh`: targeted remote observation and commit;
- `sync`: complete bounded reconciliation;
- `watch`: subscription;
- `raw`: bypass managed behavior.

---

## 11. Domain-first return types

Managed APIs return domain snapshots or handles, not raw API DTOs.

Examples:

```rust
let account: Account = client.account().get().await?;
let device: DeviceHandle = client.devices().get(code).await?;
let replicant: ReplicantHandle = client.replicants().get_owned(code).await?;
```

Raw metadata and DTO shapes remain under:

```rust
client.raw()
replicant_client::raw::Client
```

Where commit details are useful, expose a focused receipt type rather than making every ordinary return value transport-shaped:

```rust
pub struct CommitReceipt<T> {
    pub value: T,
    pub revision: StateRevision,
    pub changed: bool,
    pub observed_at: OffsetDateTime,
}
```

Avoid duplicating every method as `get`, `get_response`, `get_detailed`, and `get_with_metadata`. Add detailed variants only where applications have a real use case.

---

## 12. State model

### 12.1 Realm isolation

Every world entity must be keyed by realm:

```rust
#[non_exhaustive]
pub enum Realm {
    Live,
    Simulation(SimulationId),
}
```

Simulation devices, locations, inventories, and operations must never overwrite live-world records.

When a simulation completes, expires, or is abandoned:

- archive its run summary;
- resolve or cancel simulation operations;
- remove or tombstone ephemeral simulation projections;
- restore the replicant’s live-world relationship;
- leave live devices untouched.

### 12.2 Observation metadata

Persist observation metadata sufficient to answer:

- where the observation came from;
- whether it was a complete entity snapshot or partial delta;
- when it was observed;
- whether it is currently reachable;
- whether it is owned, shared, granted, or public;
- whether it is stale or tombstoned.

Recommended internal concepts:

```rust
enum ObservationSource {
    RestDetail,
    RestCollection,
    EventLog,
    Sse,
    CommandResponse,
    Reconciliation,
}

enum ObservationAuthority {
    EntitySnapshot,
    CollectionMember,
    CompleteCollection,
    EventDelta,
    OperationResult,
}

enum AccessScope {
    Owned,
    SiblingShared,
    Granted,
    Public,
}

enum Reachability {
    Reachable,
    OutOfRange,
    AccessRevoked,
    Historical,
}
```

These need not all be public, but the behavior they represent must be testable.

### 12.3 Visibility is not existence

A missing entity may be:

- out of FTL range;
- hidden by replicant cooperation settings;
- no longer granted through permissions;
- outside the active simulation realm;
- not yet discovered;
- absent from a filtered page.

Do not tombstone based on a single failed lookup or visibility-scoped list.

Use tombstones only when supported by authoritative evidence, such as:

- an explicit decommission event;
- a definitive mutation response;
- a successfully completed full unfiltered collection reconciliation whose contract establishes membership;
- an explicit simulation cleanup;
- another documented authoritative removal signal.

### 12.4 Owned and public replicants

Public directory observations and owned replicant observations must have different authority.

A public profile must never clear private owned fields.

Expose separate concepts:

```rust
client.replicants().get_owned(code).await?
client.directory().replicant(code).await?
client.directory().search("Syl").await?
```

The internal merge policy may share identity records, but private fields require stronger provenance and cannot be erased by public snapshots.

---

## 13. Authority and reconciliation matrix

Implementation prompts must create a checked-in machine-readable policy covering all 77 supported operations. At minimum, enforce the following rules.

### Accounts

- `GET /v1/accounts/me`: authoritative current account snapshot.
- `PATCH /v1/accounts/me`: authoritative returned account settings; exclude `message_notify` from managed requests.
- `DELETE /v1/accounts/me`: destructive account wipe; explicit confirmation required.
- registration, verification, and recovery: bootstrap/raw workflows, not normal managed state operations.
- account achievements: complete account achievement set; achievements are additive.
- account reputation: authoritative reputation snapshot.
- account simulations: authoritative simulation history snapshot.
- account location events: authoritative set only within documented account discovery/completion scope.

### Devices

- `GET /v1/devices/{code}`: authoritative full entity snapshot.
- `GET /v1/devices`: every returned item is a full device snapshot.
- A filtered page is not collection-complete.
- Only a successful full unfiltered traversal may reconcile the account’s active non-decommissioned device membership.
- `GET /v1/devices/tags/{tag}`: full snapshots for returned devices; tag-filtered collection only.
- device logs and audit: diagnostic append/history, not entity authority.
- device channels/messages/network: volatile relay/network views.
- device permissions list: complete for that device when the full response succeeds.
- granted/revoked permission mutations reconcile device access metadata.
- device decommissioning is an explicit removal signal.
- device configuration and commands use durable operations.

### Replicants

- `GET /v1/replicants`: partial public directory pages.
- `GET /v1/replicants/{code}`: owned detail or public detail depending authorization and identity.
- `PATCH /v1/replicants/{code}`: authoritative returned profile/configuration.
- `GET /v1/replicants/{code}/devices`: range-scoped visibility. Never tombstone devices absent from this response.
- `GET /v1/replicants/{code}/inventory`: replicant/vessel inventory scope only.
- `GET /v1/replicants/{code}/stars`: discovered/known star observations; do not delete knowledge solely from absence unless the contract explicitly says the list is replace-authoritative.
- scan-device responses are visibility observations, not ownership collection completeness.
- travel, teleport, transfer, mining, printing, scanning, messaging, and replication are durable operations or state-neutral previews as appropriate.

### Locations and galaxy

- `GET /v1/stars`: complete global catalogue snapshot; replace atomically only after full successful commit.
- `GET /v1/locations`: current overview of locations where the account has devices, replicants, accessible sites, or stockpiles. Absence can mean lost visibility; do not interpret it as physical deletion.
- `GET /v1/locations/{code}`: authoritative for the fields currently discoverable at that location.
- Missing unsurveyed moons, sites, salvage, or foreign devices are not proof of absence.
- a fresh system scan establishes major bodies but not all deep-scan discoveries.
- `GET /v1/inventory`: current account-visible inventory view. It does not justify deleting inaccessible historical stockpiles.
- location event lists are authoritative within discovered event scope.
- event completion and megastructure contribution are durable operations.
- asteroid diversion state is reconciled from current location/device/event evidence.

### Events

- `/v1/events` is append-only account event history.
- `/v1/events/stream` is filtered low-latency SSE.
- event IDs deduplicate log and SSE delivery.
- unknown events are stored, surfaced, and reconciled; never discarded.
- AMI digests are operational summaries, not complete device snapshots.
- device logs remain separate from account events.
- location events remain separate gameplay entities.

### Messages and BobNet

- account messages are paginated partial history.
- marking messages read is a durable managed mutation.
- BobNet account events and relay history are separate sources.
- relay history is ordered diagnostic/chat history and may be bounded.
- channel/network discovery is volatile and should use time-aware caches.

### Blueprints, achievements, reputation, and species

- unlocked blueprints are account knowledge and generally additive.
- public achievement catalogue is reference data.
- public achievement detail is public player data.
- species catalogue is global reference data.
- account and replicant reputation are current snapshots.

### Trading

- trades are scoped to a trade controller.
- a full successful controller trade traversal may reconcile that controller’s trade set.
- create, execute, and delete are durable operations.
- trade completion can affect escrow, inventory, ownership, devices, and newly created or transferred device codes.
- completion reconciliation must include every affected domain.

### Simulations

- scenarios are interface-visible reference data.
- starting a simulation creates a new realm.
- active simulation lists include both own and other players’ runs; `is_mine` controls authority.
- abandonment is a durable operation.
- completion, expiry, and abandonment clean up the realm.
- simulation history is account history, not live realm state.

### Leaderboards

Leaderboards are volatile public caches. They are not authoritative account state and do not require durable normalized projections unless needed for offline display.

### Health and feedback

Health and feedback are state-neutral raw operations. Feedback must honor its dedicated hourly rate limit.

---

## 14. Event architecture

The corrected game semantics require three complementary lanes.

```text
SSE stream
    filtered, low latency, approximately 10,000 retained events

Unfiltered account event log
    durable catch-up and muted-event recovery

Authoritative REST synchronization
    correctness and gap recovery
```

### 14.1 SSE facts

`GET /v1/events/stream`:

- uses standard SSE `id`, `event`, and `data`;
- accepts `cursor` or `Last-Event-ID`;
- applies account mute patterns automatically;
- never emits muted events;
- never mutes AMI digest events;
- retains approximately the most recent 10,000 events;
- silently begins at the earliest retained event when the requested cursor is too old.

Do not implement a “cursor rejected” branch that assumes an explicit server error.

### 14.2 Event log facts

`GET /v1/events`:

- defaults `filtered=false`;
- can filter by device, category, event, and time;
- returns up to 100 events;
- pages forward by event ID;
- returns the most recent page when no cursor is supplied;
- shares the event envelope used by SSE.

The managed client must use `filtered=false` for correctness, regardless of account mute settings.

### 14.3 Initial synchronization boundary

Recommended first-start sequence:

1. Open and restore the store.
2. Fetch the latest unfiltered event ID as a baseline watermark when available.
3. Run the essential authoritative REST baseline.
4. Fetch and apply unfiltered events after the watermark until caught up.
5. Connect SSE from the last durably applied event ID.
6. Start periodic unfiltered log catch-up.
7. Schedule domain reconciliation when continuity is uncertain.

Recommended restart sequence:

1. Restore the last durably applied event ID.
2. Catch up through the unfiltered log.
3. If continuity cannot be proven, run essential REST reconciliation.
4. Connect SSE from the last durably applied event ID.
5. Continue periodic log catch-up and staleness-based sync.

### 14.4 Durable ordering

For every event:

```text
receive
→ validate envelope
→ store raw/sanitized event
→ reduce known fields
→ commit projections and applied cursor atomically
→ publish state revision
```

Resume from the last **applied** cursor, never merely the last received cursor.

### 14.5 Unknown events

Unknown dotted event names or payload versions must:

- deserialize into a forward-compatible event envelope;
- preserve the raw payload;
- be appended to the event journal;
- be exposed through event subscriptions;
- trigger the narrowest safe reconciliation inferred from envelope category, device, replicant, star, or location;
- never crash the stream or disappear silently.

### 14.6 Managed versus raw streams

```rust
client.events().watch()
```

may combine deduplicated events learned from SSE and unfiltered log catch-up.

```rust
client.raw().events().stream()
```

exposes the server’s filtered SSE stream directly and does not mutate managed state.

---

## 15. Shared request scheduling and rate limits

All requests under one authenticated client share token-scoped limits:

```text
Reads:   120 per minute
Actions: 60 per minute
```

Special limits include:

```text
Account registration: 10/hour
Account verification: 30/hour
Feedback:             10/hour
Star catalogue:       1/minute
```

The shared scheduler must:

- apply one read bucket across every gateway and background task;
- apply one action bucket across every unsafe operation;
- support endpoint-specific windows;
- respect `Retry-After`;
- record `X-RateLimit-Limit`, `Remaining`, and `Reset`;
- prioritize foreground requests over background refresh;
- coalesce duplicate safe reads;
- cancel stale queued background work;
- prevent synchronization tasks from starving interactive calls.

Safe reads may retry according to policy.

Unsafe actions must not be automatically retried after a request may have reached the server. Such outcomes become ambiguous durable operations.

The star catalogue requires a dedicated cache and request coalescer.

---

## 16. Star catalogue strategy

`GET /v1/stars` is:

- a complete global catalogue;
- identical across accounts;
- regenerated server-side approximately every five minutes;
- limited to one request per minute;
- allowed to return temporary `503`.

The client should:

1. Restore the last durable catalogue.
2. Expose it immediately for local use.
3. inspect its generation timestamp;
4. coalesce concurrent refreshes;
5. refresh only when stale or explicitly requested;
6. respect `Retry-After`;
7. atomically replace the complete catalogue after successful commit;
8. retain the prior catalogue if refresh fails.

Expose through:

```rust
client.galaxy().catalogue().await?
client.galaxy().cached_catalogue()
client.galaxy().refresh_catalogue().await?
```

Do not include a catalogue download in every essential startup.

---

## 17. Fluent local query API

Preserve the user’s preferred fluent, strongly typed query style.

Core form:

```rust
client
    .devices()
    .find()
    .of_type(DeviceType::MiningDrone)
    .with_status(DeviceStatus::Idle)
    .collect()
    .await?;
```

Convenience forms:

```rust
client
    .devices()
    .miners()
    .idle()
    .at("SOL")
    .collect()
    .await?;
```

Controller query:

```rust
client
    .devices()
    .controllers(DeviceType::MiningController)
    .idle()
    .without_adopted_devices()
    .collect()
    .await?;
```

Requirements:

- compile-time typed filters where practical;
- clear names over clever generic machinery;
- no persisted saved-query JSON;
- local-only execution;
- deterministic snapshot semantics;
- optional subscription from the same filter;
- realm filters;
- ownership/access filters;
- location and system matching;
- relationship filters;
- status/capability filters.

Avoid query builders whose generic states produce unreadable compiler errors. Strong typing is valuable only when the API remains approachable.

---

## 18. Entity handles

Use handles for mutable or watchable entities.

Example:

```rust
let device = client.devices().get(code).await?;

let snapshot = device.snapshot().await?;
let mut updates = device.watch().await?;

let operation = device.activate().await?;
operation.wait().await?;
```

Recommended handles:

- `DeviceHandle`
- `ReplicantHandle`
- typed AMI controller handles
- possibly `TradeHandle` and `SimulationHandle`

A handle contains identity and a weak/shared link to the client. It is not the authoritative state object.

Handles should provide:

- `snapshot`;
- `refresh`;
- `watch`;
- identity access;
- capability inspection;
- typed mutations.

A handle must report a structured closed-client error after shutdown.

---

## 19. Capability-driven devices and commands

The current device response includes:

```text
features
available_commands
```

Both depend on device type and current state.

Before a typed device operation is submitted, the managed client should compare the intended command with the latest snapshot’s `available_commands`. A stale capability check does not replace server validation, but it improves diagnostics and avoids obvious invalid calls.

Known commands should have typed methods:

```rust
device.activate().await?
device.deactivate().await?
device.deploy().await?
device.stow(target).await?
device.attach(target).await?
device.compact().await?
device.unfurl().await?
```

Do not create one public exhaustive `TypedCommand` enum that makes every additive server command a breaking client change.

Use:

- typed methods and request structs for known commands;
- string-backed forward-compatible command names;
- a deliberately explicit dynamic command escape hatch for newly added upstream commands.

Example:

```rust
device
    .command(
        DynamicCommand::new("future_command")
            .argument("field", value),
    )
    .await?;
```

Dynamic commands still use durable operation handling.

---

## 20. Durable operation model

Every unsafe managed gameplay mutation must be registered durably before transmission.

Typical flow:

```text
validate request locally
→ create durable operation record
→ persist sanitized intent
→ submit exactly once
→ classify transport outcome
→ persist response or ambiguity
→ hydrate authoritative response fields
→ await event evidence and/or REST reconciliation
→ resolve operation
```

Recommended public API:

```rust
let operation = device.activate().await?;

operation.id();
operation.status().await?;
operation.watch().await?;
operation.wait().await?;
operation.reconcile().await?;
```

Recommended statuses:

```rust
#[non_exhaustive]
pub enum OperationStatus {
    Prepared,
    Submitted,
    Accepted,
    InProgress,
    AwaitingEvidence,
    ReconciliationRequired,
    Completed,
    Cancelled,
    Rejected,
    Ambiguous,
    Failed,
}
```

`wait()` must not claim failure merely because a local timeout elapsed. It may return a timeout/unresolved result while leaving the durable operation recoverable.

### 20.1 Operation coverage

Durable managed handling should cover:

- account settings;
- account wipe with explicit destructive confirmation;
- replicant profile configuration;
- device commands;
- device tags/configuration;
- device permissions;
- device decommissioning;
- travel and cancellation;
- teleportation;
- transfers;
- mining start/stop;
- scanning;
- printing;
- replication;
- messaging;
- marking account messages read;
- location event completion;
- megastructure contribution;
- trade create/execute/delete;
- simulation start/abandon.

Bootstrap registration, verification, recovery, health, and feedback may use raw/state-neutral workflows because no durable authenticated client may exist yet.

### 20.2 Sanitization

Operation journals must not store:

- bearer tokens;
- secret headers;
- recovery tokens;
- verification tokens;
- raw authentication material.

Message content and gameplay payloads may be persisted only when required for operation recovery and must be documented as local database content.

---

## 21. First-class travel API

Replicant travel is a unified route-planning domain with:

- in-system and interstellar destinations;
- dry-run preview;
- route legs;
- cruise and surge travel;
- current-leg and final arrival times;
- cancellation;
- server route selection.

Expose a builder:

```rust
let plan = replicant
    .travel()
    .to("MIRFAKA")
    .preview()
    .await?;

let operation = plan.depart().await?;
```

Support server-contract route controls only when they are actually present in the current request schema. Do not invent `via` or `direct` methods unless the corrected OpenAPI/request model supports them.

Device travel remains a capability-driven device command.

Travel events should update low-latency state, while replicant/device detail reconciliation confirms final authoritative state.

---

## 22. AMI controllers

AMI should be represented through typed device handles rather than a generic endpoint mirror.

Common controller behavior includes:

- adopt;
- release;
- launch;
- withdraw;
- assemble;
- activate/deactivate;
- set directive;
- clear/pause/resume directives.

Typed examples:

```rust
let controller = device.as_mining_controller()?;

controller.adopt(miners).await?;

controller
    .set_directive(MiningDirective::GatherResources { resources })
    .await?;

controller.launch().await?;
```

Support mining, survey, transport, and fleet controller semantics from the corrected AMI documentation.

AMI digest events are periodic reports. Persist them as reports/event history, but do not treat a digest as a complete authoritative fleet snapshot.

Controller queries should support:

```rust
client
    .devices()
    .controllers(DeviceType::MiningController)
    .idle()
    .without_adopted_devices()
    .collect()
    .await?;
```

---

## 23. BobNet

BobNet spans relay devices, replicants, account subscriptions, relay history, and account events.

Expose one coherent gateway:

```rust
client.bobnet().channels(relay).await?;
client.bobnet().history(relay).latest(100).await?;
client.bobnet().send(replicant, "#trade", text).await?;
client.bobnet().watch().await?;
```

Relay handles may expose:

```rust
relay.channels().await?
relay.messages().latest(50).await?
relay.network().await?
```

Key semantics:

- BobNet requires an active FTL relay path.
- It may be available during travel when both origin and destination systems have active relays.
- relay history is bounded and newest-first;
- sending to an unsubscribed channel auto-subscribes;
- `bobnet.new` is the modern live event;
- no webhook models or delivery APIs are included.

---

## 24. Trading

Trading can affect:

- trade records;
- escrow;
- inventory;
- device ownership;
- buyer/seller projections;
- new or transferred device codes.

Expose:

```rust
client.trading().visible_to(replicant).await?;
client.trading().for_controller(controller).sync().await?;
client.trading().create(controller, request).await?;
client.trading().execute(controller, trade_code).await?;
client.trading().delete(controller, trade_code).await?;
```

Trade operations require multi-domain reconciliation. `trade.completed` includes `new_device_codes` in Replicant Space 2.3.1 and should drive targeted device refreshes.

A successful server transaction may be atomic while the client needs multiple reads to reconstruct all affected local projections.

---

## 25. Simulations

Simulations are isolated virtual worlds.

Expose:

```rust
client.simulations().scenarios(interface).await?;
client.simulations().active(interface).await?;
client.simulations().start(interface, replicant, scenario).await?;
client.simulations().abandon(interface, simulation_id).await?;
client.simulations().history().await?;
```

Starting a simulation must:

- create `Realm::Simulation(id)`;
- record the virtual star and starting location;
- insert the full starting loadout;
- associate the plugged-in replicant with the simulation realm;
- leave the live vessel and live devices intact but out of range for that replicant.

On exit:

- cancel in-progress simulation operations;
- remove virtual devices and locations;
- archive the simulation result;
- restore live-world replicant state;
- never award live XP/achievement/distance state from simulation actions.

Active simulation lists include other players. Only entries with `is_mine=true` can establish owned simulation state.

---

## 26. Location events, messages, and diagnostics

Keep these concepts distinct:

```text
Account events:
    modern account-wide event log and SSE

Location events:
    discoverable/completable civilisation gameplay events

Device logs:
    device-specific operational history

BobNet relay history:
    bounded channel messages

Account messages:
    in-game inbox and email subscription categories
```

Do not expose a generic ambiguous `activity()` API that merges all five.

Suggested surfaces:

```rust
client.events()
client.location_events()
client.messages()
device.logs()
client.bobnet()
```

---

## 27. Persistence

Create a fresh schema version 1 for `replicant-client`.

There is no requirement to migrate a predecessor-crate database. Do not copy the old migrations unchanged.

The new schema should include durable support for:

- application/schema metadata;
- bound account identity;
- realm-qualified accounts/replicants/devices/locations;
- device relationships and capabilities;
- inventories;
- public directory profiles;
- star catalogue and discovered-star details;
- resource sites, salvage, megastructures, and location events;
- messages and BobNet history where retained;
- blueprints, achievements, reputation, species;
- trades and simulations;
- source documents/provenance;
- event journal and stream state;
- operation journal;
- reconciliation queue and runs;
- synchronization freshness;
- tombstones and reachability state.

Requirements:

- use foreign keys;
- use composite realm-aware keys where needed;
- use WAL for file databases;
- use SQL transactions for projection and cursor/operation updates;
- persist before publication;
- keep raw source documents where they materially aid forward compatibility and debugging;
- sanitize diagnostics;
- version migrations from the first release;
- test restart restoration and interrupted transactions.

SQLite is part of the managed client contract, not an optional hidden cache.

---

## 28. Error design

Expose one crate-level non-exhaustive error with structured categories:

```rust
#[non_exhaustive]
pub enum Error {
    Configuration(...),
    Authentication(...),
    RateLimited(...),
    Transport(...),
    Decode(...),
    Contract(...),
    Normalize(...),
    Persistence(...),
    State(...),
    Event(...),
    Synchronization(...),
    Operation(...),
    Closed,
}
```

Requirements:

- preserve server status/code/request IDs where safe;
- expose `Retry-After` information;
- identify whether an unsafe request is definitely unsent, definitely rejected, or ambiguous;
- redact secrets;
- retain source errors;
- avoid leaking raw SQL or credentials in user-facing display;
- make forward-compatible public enums non-exhaustive.

---

## 29. Forward compatibility

Replicant Space can add fields, commands, event types, and values without changing the `/v1` path.

The client must:

- ignore unknown JSON fields by default;
- preserve unknown event payloads;
- use string-backed known-or-unknown types for open server vocabularies;
- avoid exhaustive public event and command enums;
- use `#[non_exhaustive]` on public structs/enums where appropriate;
- retain raw source JSON for observations that cannot yet be fully normalized;
- fail only when unknown data prevents safe identity or authority decisions.

Contract checks should flag additions for review without automatically treating every additive field as a build failure.

---

## 30. Raw client

Provide two access patterns:

```rust
let raw = client.raw();
```

and:

```rust
use replicant_client::raw::Client;

let raw = Client::builder()
    .authentication_token(token)
    .build()?;
```

The raw client:

- exposes only current non-deprecated, non-admin operations;
- returns transport DTOs and response metadata;
- does not hydrate, persist, publish, journal operations, or reconcile;
- exposes raw SSE with the server’s mute behavior;
- shares transport code with the managed client;
- clearly documents unsafe mutation semantics.

Do not name it `api::Client`.

---

## 31. Reuse map from the old repository

| Old area | New treatment |
|---|---|
| `src/api/client.rs` | Port request construction, authentication, tracing, error decoding, and response metadata into `src/raw/`; redesign names and public surface. |
| `src/api/*/service.rs` | Port current endpoint implementations. Delete deprecated webhook, replicant-event, and legacy inventory operations. Exclude admin operations. |
| `src/api/*/models.rs` | Reuse DTO shapes selectively. Normalize deprecated aliases and redesign open enums for forward compatibility. |
| `src/api/rate_limit.rs` | Reuse header parsing and limiter concepts; replace with one client-wide prioritized scheduler. |
| `src/events/*` | Reuse SSE framing, event envelope, typed known payloads, and unknown-event preservation. Correct cursor/gap assumptions. |
| `src/domain/*` | Reuse entities, merge logic, provenance, tombstones, and reconciliation concepts. Add realm, access, reachability, and public/owned authority. |
| `src/state/*` | Reuse snapshot/index/query ideas and benchmarks. Hide the actor implementation; expose domain gateways and typed local queries. |
| `src/persistence/*` | Reuse transaction/repository patterns and journal logic. Create a new schema rather than copying migrations. |
| old managed `Client` from prompts 1–3 | Use as a prototype for builder ownership, one-request hydration, and `sync()` ergonomics. Rewrite into the new root client without compatibility constraints. |
| `src/runtime/hydration.rs` and `refresh.rs` | Port safe traversal, dependency order, and reconciliation algorithms into internal `sync`. Remove public runtime vocabulary. |
| `src/runtime/sse.rs` | Reuse durable-before-publish mechanics. Redesign around unfiltered log catch-up plus filtered SSE. |
| `src/runtime/command_executor.rs` | Reuse journal state-machine and ambiguity logic. Replace `TypedCommand` with the new operation model. |
| `policy/*.json` and coverage scripts | Port and simplify. Create current-operation, authority, event, operation, and deprecation exclusion policies. |
| contract drift and OpenAPI scripts | Port and point at the corrected contract corpus. |
| old README/docs/examples | Do not copy as product documentation. Reuse only technical facts that remain correct. |
| old Cargo/package/release metadata | Rewrite from scratch for `replicant-client` 1.0.0. |
| old public API baseline | Do not carry forward. Create a new baseline only when the 1.0 API is final. |

### 31.1 Porting discipline

Future prompts must not copy entire directories blindly.

For each reused component:

1. Read the implementation and tests.
2. Identify its invariant.
3. Port the smallest coherent unit.
4. rename it for the new architecture;
5. remove compatibility branches;
6. update it for corrected 2.3.1 semantics;
7. add a new test proving the invariant in the new crate.

The old repository is evidence and implementation material, not the specification.

---

## 32. Implementation phases

Each phase should be executed as one or more focused prompts. Do not combine all phases into one giant code-generation request.

### Phase 0 — Checkpoint the old prototype

Goal:

- stabilize old prompt 1–3 work;
- commit/tag it;
- make no additional product changes.

Definition of done:

- old tests for completed work pass;
- checkpoint is identifiable;
- no 1.1.0 release work is performed.

### Phase 1 — Bootstrap the new repository and contract policy

Create:

- root Cargo package `replicant-client` version `1.0.0`;
- edition 2024;
- license, README skeleton, contribution/security files;
- `src/lib.rs`;
- feature model;
- formatting/linting/toolchain configuration;
- CI and Makefile/justfile;
- corrected contract corpus;
- contract metadata/checksums;
- machine-readable list of 77 supported and 7 excluded operations.

Port only the contract tooling and minimal repository quality gates.

Definition of done:

- `cargo check`;
- package identity is correct;
- no old crate name appears except in historical notes;
- deprecation policy test passes;
- operation inventory matches the corrected contract.

### Phase 2 — Implement the current raw transport

Port and redesign:

- authentication;
- base URL and timeouts;
- request/response metadata;
- pagination;
- server error decoding;
- rate-limit header parsing;
- safe read retry policy;
- raw services for all 77 supported operations;
- raw SSE under the `events` feature.

Explicitly omit excluded endpoints and fields.

Definition of done:

- raw coverage policy reports all 77 operations;
- deprecated/admin endpoints are impossible to call;
- raw examples compile;
- unsafe raw calls are documented as unmanaged;
- contract fixtures and schema tests pass.

### Phase 3 — Define normalized domain and authority rules

Create:

- ID newtypes;
- `Realm`;
- account/device/replicant/location/inventory/event/trade/simulation models;
- known-or-unknown vocabularies;
- observation provenance;
- authority and reachability;
- merge rules;
- tombstone rules;
- owned/public replicant separation;
- simulation realm isolation.

Port domain logic selectively from the old repository.

Definition of done:

- table-driven authority tests cover major endpoints;
- public snapshots contain no raw DTO leakage;
- unknown values round-trip safely;
- public data cannot erase private owned data;
- simulation and live entities cannot collide.

### Phase 4 — Implement SQLite store and state engine

Create fresh migrations and internal repositories.

Port:

- durable transaction patterns;
- event/operation journals;
- snapshot publication;
- query indexes;
- restart recovery.

Definition of done:

- migrate/open/close tests;
- persist-before-publish tests;
- interrupted transaction tests;
- account binding tests;
- realm isolation tests;
- state query benchmarks;
- no public repository/runtime types.

### Phase 5 — Implement managed `Client` foundation

Use old prompt 1 as a prototype, then redesign cleanly.

Implement:

- `Client`/`ClientBuilder`;
- shared `ClientInner`;
- status/watch/readiness;
- idempotent close;
- raw accessor;
- startup policies;
- account/store binding;
- lifecycle task registry.

Definition of done:

- clone/shutdown tests;
- redacted debug;
- partial-start cleanup;
- local restoration before status advances;
- feature-tier tests for managed and raw.

### Phase 6 — Implement managed reads and domain gateways

Use old prompt 2 as a prototype.

Implement:

- endpoint-specific adapters;
- commit-before-return;
- domain gateways;
- handles;
- local `cached` and `find`;
- targeted `get`/`refresh`;
- full `sync` boundaries.

Start with account, devices, owned replicants, directory, locations, inventory, and galaxy, then cover remaining domains.

Definition of done:

- exactly one request for targeted managed reads;
- state visible before return;
- restart restoration;
- no false tombstones;
- every supported read has an authority classification;
- raw calls do not mutate managed state.

### Phase 7 — Implement synchronization and reconciliation

Use old prompt 3 and refresh algorithms as references.

Implement:

- `client.sync()`;
- essential and full plans;
- domain sync;
- targeted entity sync;
- pagination bounds;
- generation-based collection reconciliation;
- durable reconciliation queue;
- staleness policy;
- partial-failure reports.

Definition of done:

- dependency-order tests;
- complete/filtered traversal tests;
- cancellation/restart tests;
- full unfiltered devices can reconcile;
- visibility-scoped collections cannot tombstone.

### Phase 8 — Implement event log, SSE, and readiness integration

Do not reuse old prompt 4 unchanged.

Implement:

- baseline watermark;
- unfiltered log catch-up;
- filtered SSE;
- durable deduplication;
- applied cursor semantics;
- periodic log polling;
- unknown event storage;
- reconciliation triggers;
- reconnect/backoff;
- status degradation;
- clean shutdown.

Definition of done:

- muted events are recovered through unfiltered logs;
- SSE and log duplicates apply once;
- events persist before publication;
- old/uncertain cursors trigger reconciliation without assuming explicit rejection;
- restart resumes from applied state;
- unknown events survive and trigger safe sync.

### Phase 9 — Implement durable operations

Use the old command executor only as an algorithm reference.

Implement:

- operation journal;
- exactly-once submission attempt;
- ambiguity classification;
- evidence matching;
- operation handles;
- recovery after restart;
- managed unsafe method coverage;
- destructive confirmation;
- sanitization.

Definition of done:

- every unsafe supported operation is classified;
- intent is durable before send;
- ambiguous actions are not retried automatically;
- event and REST evidence resolve operations;
- restart recovers unresolved operations;
- raw mutations remain explicit bypasses.

### Phase 10 — Implement high-level game interfaces

Build client-shaped APIs for:

- travel;
- AMI;
- BobNet;
- trading;
- simulations;
- location events;
- device permissions;
- relay/network/audit views;
- mining, scanning, printing, teleportation, and transfer.

Definition of done:

- APIs follow game concepts rather than endpoint groupings;
- capabilities are checked;
- operation reconciliation covers all affected domains;
- simulation realms clean up correctly;
- BobNet uses modern events and relay history;
- no deprecated surfaces reappear.

### Phase 11 — Finish fluent queries and subscriptions

Complete:

- device query builder;
- convenience queries;
- controller relationships;
- realm/access/location/status filters;
- replicant/location/trade queries where useful;
- query subscriptions;
- stable snapshot semantics.

Definition of done:

- preferred query examples compile exactly;
- no hidden network access;
- query errors are readable;
- indexes support expected large fleets;
- benchmarks meet documented targets.

### Phase 12 — Documentation, policy gates, and release

Write from scratch:

- README;
- getting started;
- managed versus raw;
- persistence/security;
- events and reconciliation;
- operations;
- queries;
- simulations;
- API coverage;
- examples;
- release notes.

Create policy checks:

- current-operation coverage;
- deprecated-operation exclusion;
- authority coverage;
- event coverage;
- operation coverage;
- feature tiers;
- public API baseline;
- documentation examples;
- contract drift.

Definition of done:

- `cargo fmt --check`;
- Clippy with warnings denied;
- all tests and examples;
- feature-combination checks;
- docs with warnings denied;
- package contents audit;
- `cargo package`;
- clean public API baseline;
- version `1.0.0`;
- Replicant Space metadata `2.3.1`;
- no compatibility module or deprecated endpoint.

---

## 33. Standard instructions for every implementation prompt

Every future prompt should begin with instructions equivalent to:

> Work in `/run/media/chats/0c7bd812-03b4-405c-9602-31282b68fd64/replicant-client/`. Read `docs/implementation/rewrite-guide.md` before making changes. Treat `/run/media/chats/22d0a494-68e2-4df8-9e89-ab37d31eb5b8/replicant-space-rust-sdk/` as read-only reference code. The corrected Replicant Space documentation under `reference/replicant-space/` is the contract; rendered deprecation asides override missing OpenAPI flags. Implement only the assigned phase, preserve all locked decisions, run the relevant tests, and stop when the phase definition of done is met.

Each prompt must also state:

- which phase it implements;
- which old modules may be inspected;
- which new files may be created or changed;
- what is explicitly out of scope;
- expected public API;
- authority and persistence guarantees;
- tests and completion commands.

### 33.1 Prompt behavior rules

Implementation prompts must:

- inspect before copying;
- prefer incremental commits;
- avoid compatibility code;
- avoid speculative endpoints or request fields;
- not invent game mechanics absent from corrected docs/OpenAPI;
- update machine-readable coverage whenever an operation changes;
- preserve forward compatibility;
- include deterministic tests;
- keep user-facing examples centered on `Client`;
- stop after the assigned phase.

Implementation prompts must not:

- continue work in the old repository;
- copy old migrations unchanged;
- expose deprecated endpoints in `raw`;
- create a public runtime;
- make local queries perform network calls;
- treat SSE as authoritative history;
- treat partial pages as complete collections;
- use a monolithic exhaustive public command enum;
- add editable JSON saved queries;
- publish or version bump early.

---

## 34. Suggested repository layout

```text
replicant-client/
├── .cargo/
├── .github/workflows/
├── benches/
├── docs/
│   ├── implementation/
│   │   └── rewrite-guide.md
│   ├── architecture.md
│   ├── getting-started.md
│   ├── managed-client.md
│   ├── raw-client.md
│   ├── events.md
│   ├── operations.md
│   ├── persistence.md
│   ├── queries.md
│   └── release/
├── examples/
├── migrations/
├── policy/
├── reference/
│   └── replicant-space/
├── scripts/
├── src/
│   ├── lib.rs
│   ├── client/
│   ├── raw/
│   ├── account/
│   ├── devices/
│   ├── replicants/
│   ├── directory/
│   ├── galaxy/
│   ├── locations/
│   ├── inventory/
│   ├── messages/
│   ├── bobnet/
│   ├── events/
│   ├── location_events/
│   ├── blueprints/
│   ├── achievements/
│   ├── reputation/
│   ├── trading/
│   ├── simulations/
│   ├── leaderboards/
│   ├── operation/
│   ├── state/
│   ├── sync/
│   └── store/
├── Cargo.toml
├── README.md
└── LICENSE
```

Internal module depth is allowed. Public imports should remain flat and coherent.

---

## 35. Initial 1.0 documentation examples

### Managed startup

```rust
use replicant_client::{Client, SecretString};

let client = Client::builder()
    .authentication_token(SecretString::from(token))
    .sqlite("replicant-client.sqlite")
    .start()
    .await?;

client.ready().await?;
```

### Local query

```rust
let miners = client
    .devices()
    .miners()
    .idle()
    .at("SOL")
    .collect()
    .await?;
```

### Targeted remote read

```rust
let miner = client.devices().get(device_code).await?;
let snapshot = miner.snapshot().await?;
```

### Durable mutation

```rust
let operation = miner.activate().await?;
let outcome = operation.wait().await?;
```

### Initial/full synchronization

```rust
let report = client.sync().full().await?;
```

### Raw escape hatch

```rust
let response = client
    .raw()
    .devices()
    .get(device_code)
    .await?;
```

### Clean shutdown

```rust
client.close().await?;
```

---

## 36. 1.0 release acceptance checklist

The release is not ready until all of the following are true.

### Product

- [ ] Crate is named `replicant-client`.
- [ ] Version is `1.0.0`.
- [ ] Root import is `replicant_client`.
- [ ] Root managed type is `Client`.
- [ ] No compatibility layer exists.
- [ ] No public runtime exists.
- [ ] Documentation identifies the old crate only as historical lineage.

### Contract

- [ ] Corrected Replicant Space 2.3.1 corpus is checked in.
- [ ] OpenAPI checksum is recorded.
- [ ] All 77 supported operations are classified.
- [ ] All seven excluded operations are enforced as absent.
- [ ] `message_notify` is absent from managed account settings.
- [ ] deprecated mining aliases are normalized.
- [ ] admin operations are absent.

### Managed behavior

- [ ] Reads commit and publish before returning.
- [ ] Local queries perform no network requests.
- [ ] Full and partial collection authority is correct.
- [ ] visibility loss is not mistaken for deletion.
- [ ] public data cannot erase private data.
- [ ] live and simulation realms are isolated.
- [ ] SQLite restoration works.
- [ ] account/database mismatch is rejected.

### Events

- [ ] Unfiltered event-log catch-up exists.
- [ ] SSE uses the applied cursor.
- [ ] muted events can reach state through log catch-up.
- [ ] duplicate log/SSE events apply once.
- [ ] unknown events are retained.
- [ ] uncertain continuity triggers REST reconciliation.
- [ ] no explicit cursor-rejection assumption exists.

### Operations

- [ ] Unsafe intent is durable before send.
- [ ] automatic ambiguous retries are prohibited.
- [ ] every unsafe current operation is classified.
- [ ] operation recovery works after restart.
- [ ] operation diagnostics are sanitized.
- [ ] typed device capabilities and dynamic fallback exist.

### Game domains

- [ ] Travel preview and durable departure exist.
- [ ] AMI typed handles exist.
- [ ] BobNet uses events and relay history, not webhooks.
- [ ] Inventory uses `/v1/inventory`.
- [ ] Simulations use realm isolation.
- [ ] Trading reconciles cross-domain effects.
- [ ] Star catalogue uses dedicated caching and rate limiting.
- [ ] location events are distinct from account events.

### Quality

- [ ] Formatting passes.
- [ ] Clippy passes with warnings denied.
- [ ] All tests pass.
- [ ] All examples compile.
- [ ] feature combinations pass.
- [ ] docs build with warnings denied.
- [ ] contract drift check passes.
- [ ] authority/event/operation coverage checks pass.
- [ ] package contents are audited.
- [ ] `cargo package` succeeds.
- [ ] public API baseline is generated and reviewed only after stabilization.

---

## 37. Final implementation principle

The old crate’s core mistake was not its internal separation. It was requiring consumers to assemble and understand that separation.

The new crate should preserve strong internal boundaries while presenting one coherent application abstraction:

```text
replicant_client::Client
    fetches
    validates
    normalizes
    persists
    publishes
    watches
    reconciles
    and safely performs operations
```

Raw transport remains available, but it no longer competes with the normal workflow.

The implementation is complete when application code can focus on Replicant Space concepts—devices, replicants, travel, AMI, BobNet, locations, trades, simulations, and events—without learning the client’s internal hydration, persistence, reducer, or task-orchestration machinery.
