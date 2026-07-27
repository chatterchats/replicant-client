# Replicant Client Post–Phase 11 Technical Review

**Repository reviewed:** `replicant-client`  
**Repository snapshot:** uploaded as `replicant-client.zip`  
**Target package:** `replicant-client 1.0.0`  
**Target Replicant Space contract:** `2.3.1`  
**Review stage:** after implementation prompt 11, before release documentation/policy finalization  
**Review date:** 2026-07-25

---

## Executive conclusion

The repository has a **good architectural foundation** and is substantially aligned with the intended clean-client rewrite:

- the crate identity is clean;
- the managed `Client` is the primary interface;
- current non-deprecated raw operations are broadly represented;
- deprecated and administrative operations are excluded by policy;
- the event architecture recognizes unfiltered log catch-up, filtered SSE, and REST reconciliation;
- device collection authority is handled more carefully than in the original SDK;
- open event names and unknown payloads are retained;
- the fluent device query API is readable and local-only;
- simulation realms, public/owned replicant authority, durable operations, and typed AMI interfaces are present conceptually.

However, I would **not begin the final release-documentation pass yet**.

Several correctness defects affect the guarantees that distinguish this crate from a normal HTTP binding:

1. a rate-limit reset header is interpreted as a duration instead of an epoch timestamp;
2. a mutating request may be sent even if the durable `Submitted` transition failed;
3. unrelated events can falsely complete durable operations;
4. concurrent SSE and event-log lanes can both apply and publish the same event;
5. first-start event watermarking occurs after the REST baseline instead of before it;
6. simulation state can be deleted before an abandon operation is confirmed;
7. simulation seeding silently discards failures;
8. every account event is currently assigned to the live realm;
9. `full()` synchronization is currently identical to account-and-device essential sync;
10. observation timestamps are strings compared lexicographically despite mixed formats;
11. restart restoration and in-memory publication are incomplete for multiple persisted domains;
12. managed mutation dispatch duplicates raw routing and bypasses endpoint-specific typed adapters.

These should be treated as **release blockers**, not documentation polish.

---

# 1. Scope and methodology

The review compared:

- `reference/replicant-space/openapi.json`;
- the rendered Markdown under `reference/replicant-space/`;
- `docs/implementation/rewrite-guide.md`;
- the checked-in operation, authority, schema, and compatibility policies;
- raw transport services and DTOs;
- managed client ownership and lifecycle;
- managed gateways;
- synchronization and reconciliation;
- event history and SSE;
- durable operations;
- state restoration and SQLite persistence;
- simulations, trading, BobNet, travel, and AMI;
- package configuration and CI;
- general asynchronous Rust and library design practices.

The following repository policy scripts were executed successfully:

```text
contract policy check passed:
84 operations
77 supported
5 deprecated
2 admin
message_notify exclusion recorded
mining aliases recorded
no stray replicant-sdk references

forward compatibility policy check passed

raw transport policy check passed:
77 callable methods
7 excluded operations absent

schema policy check passed

authority matrix check passed:
77 supported operations covered
```

## Important validation limitation

This environment does not have `cargo` or `rustc` installed. I could not independently execute:

- `cargo check`;
- `cargo test`;
- Clippy;
- Rustdoc;
- feature-combination builds;
- `cargo package`.

Compile issues are therefore labeled as **static compile risks** rather than confirmed compiler failures. Behavioral findings based on direct source paths and control flow are still actionable.

---

# 2. Release recommendation

## Current recommendation: not release-ready

Before release documentation, add a focused remediation phase with this order:

1. **Durability and operation correctness**
2. **Event serialization and continuity**
3. **Realm and simulation correctness**
4. **Timestamp and persistence model correction**
5. **Complete managed state restoration and synchronization**
6. **Asynchronous lifecycle and backpressure**
7. **Contract-test hardening**
8. **Release/package hygiene**

After those changes, rerun the complete Cargo, Clippy, test, docs, feature, and packaging matrix.

---

# 3. What is already strong

## 3.1 Clean product identity

`Cargo.toml` correctly defines:

```toml
name = "replicant-client"
version = "1.0.0"
edition = "2024"
rust-version = "1.94"
```

The root crate name is `replicant_client`, and the default feature enables the managed product.

There is no public compatibility namespace for the old crate.

## 3.2 Current operation exclusion

The operation policy correctly distinguishes:

- 77 current supported operations;
- five deprecated operations;
- two administrator-only operations.

The raw transport checker verifies that the seven excluded routes are not exposed as callable methods.

This is a good release gate and matches the clean-release decision.

## 3.3 Correct high-level event model

The code and guide understand the three required lanes:

```text
unfiltered account event log
filtered SSE
authoritative REST reconciliation
```

The implementation does not appear to assume that the server emits an explicit “cursor rejected” response. That is aligned with the rendered documentation.

## 3.4 Better collection authority

The device synchronizer distinguishes entity authority from collection completeness:

- returned device items are treated as full snapshots;
- filtered or partial pages do not prove absence;
- only a completed unfiltered traversal may reconcile membership.

That is an important improvement over a naïve REST cache.

## 3.5 Forward-compatible events and vocabularies

The event layer retains unknown dotted event names and raw payloads. The forward-compatibility policy also checks open vocabularies.

This is appropriate because Replicant Space may add event names, fields, commands, and values without changing `/v1`.

## 3.6 Clear local query ergonomics

The fluent query API is readable and matches the desired style:

```rust
client
    .devices()
    .miners()
    .idle()
    .at("SOL")
    .collect()
    .await?;
```

The query is local rather than secretly invoking REST.

## 3.7 Useful security defaults

Positive details include:

- HTTPS validation for non-local base URLs;
- secret-redacted client debugging;
- no bearer-token persistence;
- bounded error excerpts;
- explicit destructive-operation concepts;
- SQLite foreign keys and WAL setup;
- operation and event journals.

---

# 4. Release blockers

## B-01 — `X-RateLimit-Reset` is parsed as a delay instead of a Unix timestamp

**Severity:** Critical  
**Confidence:** Confirmed from source and rendered documentation

The rate-limit documentation gives:

```http
Retry-After: 47
X-RateLimit-Reset: 1779087998
```

The reset value is an absolute Unix timestamp.

Relevant documentation:

```text
reference/replicant-space/rate-limits/index.md:34-45
```

Current parser:

```text
src/raw/client.rs:733-740
```

```rust
let reset_after = header_string(headers, "x-ratelimit-reset")
    .and_then(|value| value.parse::<u64>().ok().map(Duration::from_secs));
```

Current scheduler behavior:

```text
src/raw/rate_limit.rs:157-165
```

```rust
bucket.next = Instant::now() + delay;
```

A value such as `1779087998` becomes a wait of roughly 56 years.

### Required correction

Represent reset as one of:

```rust
SystemTime
OffsetDateTime
i64 Unix seconds
```

Then derive a nonnegative wait relative to the current wall clock.

Keep `Retry-After` as a duration. Do not merge the two fields into the same untyped property.

### Required tests

- Parse the exact documented header value.
- Assert the resulting delay is close to `reset_epoch - now`, not billions of seconds.
- Test an already-passed reset timestamp.
- Test malformed reset values.
- Test that `Retry-After` takes the appropriate precedence.

---

## B-02 — The client can transmit an unsafe operation after its durable `Submitted` write failed

**Severity:** Critical  
**Confidence:** Confirmed

At:

```text
src/managed/operation.rs:1014-1019
```

the result of the durable state transition is ignored:

```rust
let _ = client
    .managed_state()
    .set_operation_state(id.as_str(), OperationStatus::Submitted.as_str());

notify(client, id, OperationStatus::Submitted);

match dispatch(client, &kind, &path, &body).await {
```

If SQLite fails to record `Submitted`, the network mutation is still sent. The operation remains durably `Prepared`.

The restart recovery comments state that `Prepared` is safe to resubmit. That can duplicate an unsafe command.

The same method ignores persistence failures for:

- `Ambiguous`;
- `Rejected`;
- `AwaitingEvidence`;
- `Completed`.

### Required correction

Make `Prepared → Submitted` an atomic compare-and-set or transactional transition.

The request must not be transmitted unless that transaction succeeds.

A safer invariant is:

```text
persist prepared intent
→ atomically claim submission attempt
→ send at most one automatic request attempt
```

Avoid describing this as exactly-once delivery. The client can guarantee **one automatic submission attempt**, not exactly-once execution by the server.

Terminal transition persistence failures must be surfaced and retried durably; they must not be hidden behind `let _ =`.

### Required tests

Inject a store failure during `Prepared → Submitted` and assert:

```text
server request count == 0
```

Then simulate:

- process crash after durable `Submitted`, before request;
- request reaches server, response is lost;
- terminal-state write fails after response;
- restart recovery for each case.

---

## B-03 — Any event on the same target can falsely complete an unrelated operation

**Severity:** Critical  
**Confidence:** Confirmed

The implementation explicitly documents name-agnostic evidence:

```text
src/managed/operation.rs:1065-1069
```

```rust
/// Matching is intentionally name-agnostic
/// (any subsequent event on the same target counts as evidence)
```

Then:

```text
src/managed/operation.rs:1070-1099
```

marks all operations awaiting evidence for the same device, replicant, or location as `Completed`.

Examples of false completion:

- a BobNet event completes a travel operation;
- an AMI digest completes a controller directive;
- a device status/log event completes printing;
- a mining event completes decommissioning;
- one operation completes several concurrent operations against the same entity.

`Operation::reconcile` is also too broad if it marks an operation complete merely because the target can be fetched successfully, without validating the expected state transition.

### Required correction

Persist an operation-specific proof plan:

```rust
struct EvidencePlan {
    accepted_event_names: Vec<EventNamePattern>,
    target: OperationTarget,
    submitted_after: Timestamp,
    expected_payload: Option<...>,
    expected_state: Option<StatePredicate>,
    reconciliation_reads: Vec<ReconciliationTarget>,
}
```

Completion must require one of:

- a matching event name and payload;
- an authoritative response proving the result;
- an authoritative REST state satisfying the operation predicate.

Unknown or dynamic commands should remain `ReconciliationRequired` or `Ambiguous` until meaningful proof exists.

### Required tests

- Unrelated event on the same target does not resolve.
- Event predating submission does not resolve.
- Two concurrent operations on one device resolve independently.
- A successful target `GET` with unchanged state does not resolve.
- A matching event with wrong payload does not resolve.
- An authoritative matching snapshot does resolve.

---

## B-04 — Event deduplication is not atomic across SSE and log catch-up

**Severity:** Critical  
**Confidence:** Confirmed

Current flow:

```text
src/managed/events.rs:132-140
```

1. query `has_event(id)`;
2. later insert and reduce the event.

The two event lanes run concurrently. Both can observe “not present” before either inserts.

The store uses `INSERT OR REPLACE` rather than an insert-if-absent claim:

```text
src/managed/store.rs:278-305
```

Possible result:

- event projection applied twice;
- two state revisions published;
- two subscriber notifications;
- operation evidence resolved twice;
- event cursor moves inconsistently.

### Required correction

Serialize application through one event-applier task or use an atomic transaction:

```sql
INSERT INTO events (...) VALUES (...)
ON CONFLICT(event_id) DO NOTHING
```

Inspect affected rows. Only when the insert wins should the transaction:

- reduce projections;
- advance the applied cursor;
- commit;
- publish;
- notify operation evidence.

A single ordered applier is preferable because it also addresses cursor monotonicity.

### Required tests

Deliver the same event concurrently through simulated SSE and log lanes and assert:

```text
one event row
one projection change
one state revision
one subscription notification
one operation-evidence evaluation
```

---

## B-05 — First-start watermarking occurs after the REST baseline

**Severity:** Critical  
**Confidence:** Confirmed

The implementation comment describes the correct order:

```text
watermark
→ REST baseline
→ catch-up after watermark
```

But current control flow performs REST synchronization first and obtains the watermark later:

```text
src/managed/events.rs:430-484
```

An event occurring during the initial REST baseline can be missed from:

- the event journal;
- application event subscribers;
- durable-operation evidence;
- event-derived diagnostics.

REST may still leave current state correct, but event continuity and operation proof are not preserved.

### Required correction

Recommended first start:

1. fetch newest unfiltered event ID as baseline watermark;
2. run essential authoritative REST sync;
3. fetch/apply all unfiltered events after watermark;
4. connect SSE from the last durably applied cursor;
5. begin periodic unfiltered catch-up.

When a watermark cannot be obtained, mark continuity uncertain and retain a reconciliation diagnostic.

### Required test

Block the REST baseline with a test server, insert an event during the block, complete the baseline, and assert the event is subsequently journaled and published exactly once.

---

## B-06 — Simulation abandonment deletes the local realm before server confirmation

**Severity:** Critical  
**Confidence:** Confirmed

At:

```text
src/managed/simulations.rs:217-222
```

the code:

1. creates/submits a durable abandon operation;
2. immediately calls `cleanup_realm`;
3. returns the operation handle.

The operation may be:

- rejected;
- ambiguous;
- not yet transmitted;
- accepted but not completed.

Local simulation data is therefore removed before authoritative proof.

### Required correction

Keep the realm while the operation is unresolved.

Possible local state:

```text
Active
AbandonPending
AbandonAmbiguous
Ended
```

Run `cleanup_realm` only after:

- an authoritative successful response that proves ending;
- a matching event;
- a REST refresh that confirms the run is no longer active.

### Required tests

- Rejected abandon retains realm.
- Ambiguous abandon retains realm and schedules reconciliation.
- Confirmed abandon removes only the simulation realm.
- Live devices are untouched.
- Restart during `AbandonPending` preserves and recovers the realm.

---

## B-07 — Simulation realm seeding silently ignores failures

**Severity:** Critical  
**Confidence:** Confirmed

At:

```text
src/managed/simulations.rs:174-181
```

the result of `seed_realm` is discarded.

Inside seeding:

```text
src/managed/simulations.rs:193-207
```

individual device fetch and persistence failures are silently skipped.

The operation can appear successful even though:

- no simulation record was committed;
- only part of the starting loadout was loaded;
- device realm data is absent;
- later queries present an incomplete simulation as ready.

### Required correction

Treat the simulation-enter response as an authoritative simulation seed.

Persist the simulation and any loadout supplied directly in the response in one transaction where practical.

For details requiring follow-up reads:

- create durable targeted reconciliation work;
- expose simulation state as synchronizing/degraded;
- keep the operation `ReconciliationRequired` until the required local projection is committed;
- report failed device codes.

Never silently discard normalization, network, or persistence errors.

---

## B-08 — Every account event is assigned to `Realm::Live`

**Severity:** Critical  
**Confidence:** Confirmed

At:

```text
src/managed/events.rs:141
```

all events are normalized with:

```rust
Some(Realm::Live)
```

This can:

- decommission the live device when a simulation device emits the event;
- resolve a live operation from simulation evidence;
- update live projections using simulation changes;
- enqueue reconciliation in the wrong realm.

The event catalogue includes simulation lifecycle payloads with `simulation_id`. Other device events may require resolving the device’s realm from local state.

### Required correction

Realm resolution should follow:

1. explicit `simulation_id` in payload;
2. known entity-to-realm mapping;
3. active simulation association;
4. otherwise unresolved/ambiguous realm.

Do not default an ambiguous event to live.

An unresolved event should still be journaled and should schedule the narrowest safe reconciliation.

### Required tests

- Simulation lifecycle event maps to simulation realm.
- Simulation device event cannot modify same-code live entity.
- Unknown-realm event is retained but does not apply destructive live changes.
- Operation evidence is realm-qualified.

---

## B-09 — `SyncPlan::full()` is identical to essential account-and-device sync

**Severity:** Critical for advertised API completeness  
**Confidence:** Confirmed

At:

```text
src/managed/sync.rs:181-194
```

`essential()` includes only:

- account;
- devices.

`full()` returns `essential()` unchanged.

At:

```text
src/managed/sync.rs:325-335
```

replicant and location synchronization return “not implemented.”

This conflicts with:

- the method name `full`;
- the rewrite guide’s Phase 7 definition;
- a 1.0 promise of managed state spanning game domains.

### Required correction

Either:

1. implement a genuine bounded full plan, or
2. remove/rename `full()` before 1.0.

A genuine full plan should classify and synchronize:

- account;
- owned replicants;
- account-visible devices;
- current accessible locations;
- inventory;
- account messages as appropriate;
- blueprints/achievements/reputation;
- simulations/trades as their authority allows.

Global catalogue and volatile public surfaces may remain separate and documented.

`sync_domain(Replicants)` and `sync_domain(Locations)` must not ship as public advertised methods that always return configuration errors.

---

## B-10 — Observation timestamps are strings compared lexicographically

**Severity:** Critical data-integrity risk  
**Confidence:** Confirmed

`ObservationMetadata.observed_at` is a `String`:

```text
src/domain/observation.rs:55-70
```

Merge ordering compares the strings:

```text
src/domain/merge.rs:28-46
```

Runtime-generated observations use decimal Unix seconds in several modules, while tests and fixtures also use RFC 3339 strings.

Examples:

```text
src/managed/gateways.rs
src/managed/events.rs:37-41
src/managed/sync.rs:402-406
```

Lexicographic comparison across mixed representations is invalid.

Example:

```text
"999999999" > "1700000000" lexicographically
```

RFC 3339 and epoch strings are also incomparable.

### Required correction

Change the schema before 1.0:

```rust
pub struct ObservedAt(i64); // Unix milliseconds
```

or use a typed UTC timestamp.

Store it as an SQLite integer and compare numerically.

Normalize all server timestamps and local observation times at boundaries.

### Required tests

- old/new epoch values;
- subsecond ordering;
- server RFC 3339 normalization;
- equal timestamp tie-breaking by authority/source;
- restart round-trip.

---

## B-11 — Persisted state is not fully restored or represented in the in-memory snapshot

**Severity:** Critical for durable-client promises  
**Confidence:** Confirmed

At startup:

```text
src/managed/state.rs:50-66
```

only devices and simulations are restored.

The snapshot initializes:

- account as `None`;
- replicants as empty.

Locations and inventory are not represented in `StateSnapshot`.

`persist_inventory` writes to SQLite but publishes no state revision:

```text
src/managed/state.rs:192-205
```

`persist_location` publishes a new revision without including location data in the snapshot:

```text
src/managed/state.rs:207-225
```

This breaks several public promises:

- managed reads publish before returning;
- durable state survives restart;
- query/subscription state corresponds to committed data;
- local state is an offline-capable view.

### Required correction

Before 1.0, decide the actual durable managed domains and make them consistent.

For every managed persisted domain:

```text
SQLite projection
↔ startup restoration
↔ in-memory snapshot or query index
↔ state revision
↔ local query/subscription
```

Do not persist a domain that is permanently invisible to managed state unless it is explicitly a journal/reference cache rather than a state projection.

### Required restart tests

After reopening the same database, verify restoration of:

- account;
- devices;
- owned replicants;
- public profiles if retained;
- locations;
- inventory;
- simulations;
- event cursor;
- unresolved operations;
- reconciliation work.

---

## B-12 — Managed mutation routing is a second hand-written transport implementation

**Severity:** High/Critical maintainability and contract risk  
**Confidence:** Confirmed

`src/managed/operation.rs` manually maps operation kinds and JSON path/body values to HTTP requests.

This duplicates:

- raw service paths;
- methods;
- body shapes;
- response handling;
- endpoint semantics.

Consequences:

- raw and managed paths can drift;
- method/path policy checks may validate only raw services;
- managed responses bypass endpoint-specific DTO adapters;
- successful operation responses may not hydrate affected domains;
- adding a server route requires editing two routing systems.

### Required correction

Use one endpoint adapter per operation.

Possible designs:

```rust
trait ManagedMutation {
    type Request;
    type Response;

    fn durable_intent(&self) -> SanitizedIntent;
    async fn submit(&self, raw: &raw::Client) -> Result<RawResponse<Self::Response>>;
    fn evidence_plan(&self, response: &Self::Response) -> EvidencePlan;
    fn observations(&self, response: &Self::Response) -> Vec<Observation>;
}
```

The raw method and managed operation should share the same typed route/request implementation.

Add a machine-readable managed-operation route policy generated or checked against OpenAPI.

---

# 5. High-severity findings

## H-01 — Concurrent `close()` calls have a lost-notification race

At:

```text
src/managed/client.rs:695-714
```

a second caller:

1. sees `closing == true`;
2. checks `closed == false`;
3. awaits `Notify::notified()`.

The first caller can call `notify_waiters()` between steps 2 and 3. `notify_waiters()` does not preserve a permit for a future waiter, so the second caller can wait forever.

### Correction

Use a `watch` channel, shared future, `OnceCell`, or a loop that registers the notification future before checking the condition.

Add a stress test that calls `close()` concurrently from many clones repeatedly.

---

## H-02 — Startup can report `Ready` after essential REST sync failed

The startup path can set `Degraded`, then overwrite it with `CatchingUp`, and the SSE loop can later set `Ready` merely because the stream connected.

Readiness should not be represented by one last-writer-wins status field.

### Correction

Track dimensions:

```rust
struct ReadinessState {
    restored: bool,
    essential_rest: ReadinessComponent,
    event_catchup: ReadinessComponent,
    sse: ConnectivityComponent,
}
```

Derive the public status from all dimensions.

Connecting SSE must not erase an essential-baseline failure.

---

## H-03 — Event and catch-up errors are frequently swallowed

Several paths use `let _ =` or ignore errors from:

- event application;
- catch-up;
- baseline cursor persistence;
- synchronization fallback;
- lifecycle task registration.

A persistence failure can therefore leave the client appearing operational even though event state stopped advancing.

### Correction

- Log structured errors.
- Set an appropriate degraded status.
- persist retry/reconciliation work;
- stop cursor advancement on event transaction failure;
- expose diagnostics through readiness/sync status.

---

## H-04 — Event cursor can regress under concurrent application

SSE and log lanes may deliver different event IDs concurrently.

The cursor update is unconditional. A later transaction containing an older event can overwrite a newer applied cursor.

### Correction

Use one ordered event-applier queue.

If event IDs are Redis-stream style, implement and test a proper numeric tuple comparison rather than string comparison.

Cursor movement must be monotonic.

---

## H-05 — Account/store binding relies on mutable email

At:

```text
src/managed/client.rs:393-404
```

email is used as the account identity because the contract exposes no immutable account ID.

But account settings allow changing email.

After a successful email change, reopening the same database may report an account mismatch.

### Correction options

- transactionally update the bound identity after verified email mutation;
- store a composite fingerprint using stable account traits if the contract provides any;
- introduce an explicit store-rebind flow after authenticated account-email change.

Document the server limitation clearly. Do not silently create a second database identity for the same account.

---

## H-06 — Synchronous SQLite and standard mutexes are used directly in async tasks

The managed layer uses `rusqlite::Connection` behind `std::sync::Mutex`, and performs database work directly from async request and event tasks.

This can block Tokio worker threads during:

- large device syncs;
- event catch-up;
- restoration;
- operation recovery;
- checkpointing.

Numerous lock poison paths use `expect`, which can panic a library process after one poisoned lock.

### Correction

Prefer a dedicated store actor/thread with request messages, or consistently use `spawn_blocking`.

Return structured errors for poisoned/unavailable state rather than panicking.

---

## H-07 — Subscription channels are unbounded synchronous channels

Event, state, and operation subscriptions use `std::sync::mpsc`.

Problems:

- unbounded memory for slow consumers;
- no async `Stream`;
- no lag/error semantics;
- synchronous polling in an async-first crate.

### Correction

Use bounded Tokio channels:

- `watch` for latest snapshots/status;
- `broadcast` when dropping/lagging semantics are acceptable;
- bounded `mpsc` for per-consumer queues.

Expose an async `Stream` and document what happens when a subscriber falls behind.

---

## H-08 — `Operation::wait()` busy-polls SQLite

The operation wait loop queries persistence every 100 ms.

This creates unnecessary database load and latency.

### Correction

Use an operation-specific `watch` receiver or shared status notifier, with a final durable read when resolving or reconnecting after restart.

---

## H-09 — Managed API/state coverage remains incomplete after Phase 11

Several high-level managed methods are raw pass-throughs or do not commit state.

Examples identified during inspection:

- public directory reads normalize but do not commit public observations;
- location-event list reads do not persist discovered events;
- simulation scenario/active/history reads return raw DTOs;
- active owned simulations are not fully hydrated into realms;
- trading “sync” parses response data but lacks durable trade projections;
- several reference/account domains do not have a managed gateway.

This does not necessarily mean every result must be persisted. It means the API must explicitly classify each method as:

```text
managed stateful read
managed state-neutral read
volatile cache
raw-only operation
```

A method should not appear durable merely because it hangs from the managed client.

---

## H-10 — `SyncReport` drops error detail and computes readiness incorrectly

At:

```text
src/managed/sync.rs:302-317
```

all errors are reduced to `SyncProgress::Failed` without diagnostic cause.

The readiness branch checks essential completion first, so even a complete plan may return `RestBaseline` instead of `Complete`.

### Correction

Include:

- domain;
- structured error;
- retryability;
- pages/items completed;
- whether authority was complete;
- cancellation versus failure.

Check complete-plan success before essential-baseline success.

Do not return `Error::Closed` for ordinary sync cancellation.

---

## H-11 — Deprecated `message_notify` remains in the public raw API

The raw account response and update request expose `message_notify`:

```text
src/raw/accounts.rs:178-180
src/raw/accounts.rs:214-218
src/raw/accounts.rs:240-242
```

The rewrite guide only explicitly requires it to be absent from managed settings, so the current policy checker passes.

However, the clean-release product decision was stronger: no deprecated game fields or endpoints should be promoted.

### Recommendation

Remove `message_notify` from the public update request.

Serde can ignore the deprecated response field without defining it. If retaining it for response diagnostics, keep it private or clearly hidden/deprecated rather than providing a setter.

---

## H-12 — The response body cap is checked only after buffering the entire body

At:

```text
src/raw/client.rs:722-730
```

`response.bytes().await` buffers the entire body before checking its length.

For a chunked response without a trustworthy `Content-Length`, the configured limit does not protect memory.

### Correction

Read the response stream in chunks and abort once the accumulated length exceeds the cap.

---

## H-13 — Raw SSE does not share all request metadata and rate-limit handling

The raw event-stream request path is separate from the normal request executor.

It does not consistently:

- attach the local request ID;
- observe successful rate-limit headers;
- share all tracing/metadata behavior.

### Correction

Factor shared request preparation and response-header observation into internal primitives used by both normal HTTP and SSE.

---

## H-14 — Some OpenAPI-typed success responses still return `serde_json::Value`

Example:

```text
src/raw/accounts.rs:363-374
```

`DELETE /v1/accounts/me` returns `RawResponse<Value>` even though OpenAPI defines a success schema.

Opaque JSON is reasonable only for operations where the contract genuinely lacks a response schema and that exception is recorded in `policy/contract-metadata.json`.

### Correction

Add a policy gate:

> Every OpenAPI success response with a named schema must map to a typed DTO.

Maintain explicit exceptions only for schema-less routes.

---

# 6. Medium-severity and Rust-quality findings

## M-01 — Contract checks prove method presence, not route/schema correctness

`raw_transport_policy_check.py` primarily discovers public async method names.

It does not prove that each method uses the correct:

- HTTP method;
- path;
- authentication requirement;
- query parameters;
- body schema;
- success type;
- rate-limit bucket.

### Recommendation

Generate or maintain a route descriptor table:

```rust
OperationDescriptor {
    operation_id,
    method,
    path,
    auth,
    safety,
    request_type,
    response_type,
}
```

Check it against OpenAPI.

Add fixture tests for all 77 supported operations.

---

## M-02 — Request bounds are mostly left to the server

Where documentation gives clear maximums, builders can catch obvious errors earlier:

- event page size;
- pagination limits;
- message limits;
- channel lengths;
- bounded lists.

Keep server validation authoritative, but return clear client errors for objectively invalid local values.

---

## M-03 — Public documentation lint suppression remains

Some modules use `allow(missing_docs)` and phase-era comments such as:

```text
Phase 5 owns...
Phase 11 owns...
```

These should not survive into a 1.0 public release.

Remove phase scaffolding and document public types/method invariants.

---

## M-04 — Error source chains are weak

The crate has a structured public `Error`, but many underlying errors are collapsed into strings, and the manual `std::error::Error` implementation does not expose useful `source()` chains.

### Recommendation

Use private/source-carrying error fields while keeping `Display` sanitized.

Distinguish:

- definitely unsent;
- server rejected;
- ambiguous transport;
- persistence failure after server success;
- event continuity failure;
- reconciliation failure.

A `RateLimited` variant should report HTTP 429 consistently rather than deriving it indirectly from an arbitrary server code field.

---

## M-05 — One fluent query filter is O(n²)

`without_adopted_devices()` repeatedly scans device relationships.

Build a set of adopted/controlled device IDs once and filter against it.

The existing benchmark measures raw SQL rather than the real in-memory fluent query path. Add representative fleet-query benchmarks.

---

## M-06 — Local tool files may be published

The repository contains:

```text
.claude/settings.local.json
.tokensave/config.json
.tokensave/branch-meta.json
.tokensave/tokensave.db
```

They are not ignored by `.gitignore`.

`Cargo.toml` has no package `include` allowlist or relevant exclusions.

### Correction

- remove local files from version control;
- add ignores;
- use an explicit package `include` list;
- inspect `cargo package --list`;
- ensure the large reference corpus and private tooling metadata are not unintentionally shipped.

---

## M-07 — CI does not test the declared MSRV or pinned primary toolchain

The repository declares:

```text
rust-version = 1.94
rust-toolchain = 1.96
```

GitHub Actions installs `stable`.

### Correction

Test:

- pinned primary 1.96;
- MSRV 1.94;
- raw-only;
- events-only;
- default managed;
- each TLS backend separately.

Avoid relying only on `--all-features`, which enables both TLS backends simultaneously.

---

## M-08 — Migration handling accepts unknown schema versions

The store appears to initialize schema version 1 when absent but does not robustly reject or migrate other versions.

Before 1.0, implement:

```text
version 0 → 1 migration
version 1 → current
version > current → structured unsupported-schema error
```

Also configure and test SQLite busy timeout and synchronous durability policy.

---

## M-09 — Shutdown aborts tasks rather than first allowing graceful completion

Task abortion is useful as a timeout fallback.

A clean close should preferably:

1. signal cancellation;
2. stop accepting new work;
3. let event and reconciliation tasks exit at safe transaction boundaries;
4. wait with a timeout;
5. abort only stragglers;
6. flush the store.

---

## M-10 — Raw device command modeling should remain forward-compatible

The managed layer has a dynamic command escape hatch, but the raw command enum should also avoid becoming a closed exhaustive vocabulary.

Use `#[non_exhaustive]`, a string-backed command name, or a request builder that preserves unknown future commands.

---

## M-11 — `ready()` considers any degraded state ready

This is documented in source, so it is not necessarily a bug.

It may still surprise callers:

```rust
client.ready().await?;
```

can succeed even if essential synchronization failed.

Consider:

```rust
client.wait_until_usable()
client.wait_until_ready()
client.readiness()
```

or make `ready()` return a readiness report describing degraded components.

---

## M-12 — The rate limiter is conservative but not yet the promised scheduler

The current limiter smooths requests rather than allowing token-bucket bursts. That is safe but may add latency.

More importantly, the rewrite guide expected:

- foreground priority;
- background priority;
- duplicate safe-read coalescing;
- stale background cancellation.

The current scheduler hooks appear incomplete.

Either implement these guarantees or narrow the release documentation.

---

## M-13 — Operation success can be declared before state hydration/reconciliation

For operations not marked as expecting evidence, a successful response can immediately become `Completed`, even if:

- the response was not normalized;
- affected projections were not committed;
- scheduled reconciliation later fails.

A managed operation’s completion should mean the local authoritative result is committed, or its status should remain `ReconciliationRequired`.

---

# 7. Contract-specific review notes

## 7.1 OpenAPI coverage

The operation inventory has the correct high-level count and exclusions.

Before release, strengthen it from name coverage to descriptor and schema coverage.

Recommended per-operation fields:

```json
{
  "operation_id": "...",
  "method": "GET",
  "path": "/v1/...",
  "auth": true,
  "safety": "safe_read",
  "request_schema": "...",
  "response_schema": "...",
  "managed_classification": "...",
  "authority": "...",
  "operation_evidence": "..."
}
```

## 7.2 Rendered deprecation asides

The repository correctly excludes deprecated routes.

The remaining inconsistency is `message_notify`, which is still public in raw account DTOs.

## 7.3 Events

The high-level lane design is correct, but implementation must fix:

- atomic dedup;
- ordered cursor advancement;
- first-start watermark ordering;
- swallowed failures;
- realm inference;
- operation-specific evidence.

## 7.4 Devices

The list authority model is one of the strongest parts of the repository.

Continue to preserve:

```text
full snapshots for returned devices
no deletion inference from filtered/partial pages
membership reconciliation only after full unfiltered traversal
```

## 7.5 Replicants

Owned and public merge concepts are present, but public-directory reads need a clear persistence/state classification.

A public profile must never erase private owned fields.

## 7.6 Simulations

Realm types exist, but lifecycle correctness is incomplete.

Fix:

- realm seeding;
- active-owned simulation restoration;
- event realm mapping;
- confirmed cleanup;
- pending/ambiguous transitions;
- restart recovery.

## 7.7 Trading

Trading requires durable cross-domain reconciliation.

A parsed `Value` is not enough for a managed “sync” promise. Persist:

- trade state;
- escrow/inventory effects;
- transferred/new device codes;
- ownership changes;
- controller-specific membership.

## 7.8 BobNet

The modern design is directionally aligned:

- account events for live updates;
- relay history for bounded history;
- no webhook compatibility.

Ensure relay availability and current/destination travel semantics are represented where documented.

## 7.9 Travel and AMI

The typed surfaces appear conceptually aligned with the rendered docs.

Continue to verify request shapes directly against OpenAPI and avoid adding route options not present in the contract.

---

# 8. Recommended remediation sequence

## Pass A — Durability barrier

Fix first:

1. operation `Prepared → Submitted` atomicity;
2. terminal-state persistence;
3. operation-specific evidence;
4. state-predicate reconciliation;
5. simulation operation confirmation;
6. eliminate silent operation/seeding failures.

Do not proceed until unsafe mutation tests prove no duplicate automatic submission.

## Pass B — Event barrier

Fix:

1. first-start watermark ordering;
2. one serialized event applier;
3. atomic insert-if-new;
4. monotonic cursor;
5. error propagation/degraded status;
6. realm inference;
7. unknown-event reconciliation.

## Pass C — State model barrier

Fix before schema freeze:

1. typed numeric timestamps;
2. complete restoration;
3. location/inventory snapshot/query representation;
4. account identity mutation strategy;
5. schema-version handling.

## Pass D — Sync completeness

Implement or remove:

- `full()`;
- replicant sync;
- location sync;
- managed state classifications for remaining domains;
- detailed sync diagnostics.

## Pass E — Async/runtime quality

Address:

- store actor or blocking boundary;
- bounded async subscriptions;
- event-driven operation wait;
- close race;
- graceful task shutdown;
- foreground/background scheduler guarantees.

## Pass F — Contract hardening

Add:

- per-operation method/path/auth/safety checks;
- typed-success-schema coverage;
- body/query fixture coverage;
- `message_notify` removal;
- streamed body cap;
- shared raw/SSE request metadata.

## Pass G — Release hygiene

Then perform:

- public documentation;
- remove phase comments and lint suppressions;
- package allowlist;
- pinned toolchain/MSRV CI;
- real query benchmarks;
- package audit and public API baseline.

---

# 9. Minimum regression test plan

The following tests should exist before release.

## Rate limiting

- documented reset epoch produces short relative delay;
- stale reset produces zero delay;
- retry-after precedence;
- malformed headers do not panic;
- foreground request is not starved by background sync.

## Durable operations

- store failure before `Submitted` causes zero HTTP calls;
- process restart from `Prepared` submits once;
- restart from `Submitted` does not blindly resubmit;
- ambiguous response remains unresolved;
- unrelated target event does not complete;
- matching event completes;
- authoritative unchanged snapshot does not complete;
- two operations on one target resolve independently;
- terminal persistence failure remains recoverable.

## Events

- SSE/log duplicate applies once;
- cursor cannot regress;
- event during initial REST baseline is caught after watermark;
- muted SSE event arrives through unfiltered log;
- event persistence failure does not advance cursor;
- unknown event is retained and schedules reconciliation;
- simulation event cannot mutate live realm;
- concurrent event delivery preserves subscription order.

## Simulations

- successful enter seeds all known starting state;
- partial seed schedules durable reconciliation;
- rejected abandon retains realm;
- ambiguous abandon retains realm;
- confirmed end cleans only simulation realm;
- restart restores active realm and pending operations.

## State and persistence

- mixed timestamp formats are impossible;
- account/replicant/location/inventory/device/simulation restore;
- unsupported future schema version is rejected;
- account email update does not orphan the store;
- persistence precedes publication;
- slow subscriber cannot grow memory without bound.

## Synchronization

- `full()` covers all advertised domains;
- full device traversal reconciles absence;
- filtered device traversal never tombstones;
- canceled sync reports cancellation;
- failure diagnostics preserve cause;
- complete plan reports `Complete`.

## Contract

For every supported operation:

- HTTP method;
- path;
- authentication;
- safety classification;
- query/body serialization;
- success DTO;
- representative documented fixture decoding.

## Lifecycle

- 100 concurrent `close()` callers all return;
- repeated close is idempotent;
- closing waits for safe store boundary;
- final status is `Closed`;
- no background task survives close.

---

# 10. Commands to run in a Rust-capable environment

```sh
cargo fmt --all -- --check

cargo clippy \
  --all-targets \
  --all-features \
  -- \
  -D warnings

cargo test --all-features

cargo check --no-default-features --features raw,rustls-tls
cargo check --no-default-features --features raw,native-tls
cargo check --no-default-features --features events,rustls-tls
cargo check --features managed,rustls-tls
cargo check --no-default-features --features managed,native-tls

RUSTDOCFLAGS="-D warnings" \
  cargo doc --all-features --no-deps

python3 scripts/contract_policy_check.py
python3 scripts/forward_compatibility_policy_check.py
python3 scripts/raw_transport_policy_check.py
python3 scripts/schema_policy_check.py
python3 scripts/authority_matrix_check.py

cargo package --list
cargo package

cargo +1.94 check --all-targets --features managed,rustls-tls
cargo +1.94 test --all-features
```

Recommended additional checks:

```sh
cargo audit
cargo deny check
cargo machete
```

Run TLS backends separately rather than assuming `--all-features` proves each supported configuration.

---

# 11. Suggested release gate

Do not start the final documentation/release prompt until:

- all twelve blocker findings are resolved or intentionally removed from the public API;
- operation submission durability has fault-injection tests;
- event application is serialized/atomic;
- simulation realm behavior is correct;
- timestamps are typed and schema-safe;
- `full()` is real or absent;
- persisted managed domains restore and publish consistently;
- Cargo, Clippy, tests, docs, features, MSRV, and package checks pass.

At that point, the final documentation pass can accurately describe guarantees the implementation actually enforces.

---

# 12. Overall assessment

This is **not a failed rewrite**. The repository is much closer to the intended product than the old SDK architecture:

- it has the right primary abstraction;
- it has a disciplined contract inventory;
- it correctly excludes obsolete surfaces;
- it has a strong authority model foundation;
- it has coherent domain-oriented APIs;
- it has already solved a significant amount of hard plumbing.

The remaining issues are concentrated around the hardest parts of a durable client:

```text
exact mutation-attempt semantics
event ordering and deduplication
proof of operation completion
realm isolation
restart correctness
async lifecycle behavior
```

Those are exactly the areas worth correcting before the 1.0 API and database schema become permanent.

The best next step is a dedicated **Phase 11.5 correctness and contract-hardening pass**, followed by the release-documentation phase only after its acceptance tests pass.
