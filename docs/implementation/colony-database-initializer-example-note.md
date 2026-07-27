# Colony Database Initializer Example

The bundled `reference/examples/initialize_colony_database.rs` is a required
Phase 11.6 target example. Install it as:

```text
examples/initialize_colony_database.rs
```

## Purpose

The Riker candidate query is deliberately local-only. It can rank only
environmental facts that have already been normalized and committed to the
SQLite database.

This initializer performs the explicit remote hydration pass:

1. Complete bounded managed synchronization:
   - account profile;
   - every owned replicant detail;
   - every page of owned/account-visible devices;
   - every other advertised durable managed domain.
2. Fetch and atomically persist the complete `GET /v1/stars` catalogue.
3. For each owned replicant, traverse every page of
   `GET /v1/replicants/{code}/stars`.
4. Persist each replicant's star-knowledge observation and deduplicate stars
   whose `explored` value is known true.
5. For every explored star:
   - fetch `GET /v1/locations/{star}`;
   - persist the system/root observation;
   - extract verified child designation fields;
   - fetch every known planet, moon, belt, Lagrange point, Kuiper/Oort object,
     hub, and other designated system object;
   - recursively discover known moons or child objects from detail responses;
   - commit each successful observation before continuing.
6. Run the hard colony-candidate query locally as a validation count.

## Safety

The initializer is read-only with respect to game state.

It must never:

- call the replicant system-scan mutation;
- command survey drones;
- start travel;
- send BobNet messages;
- submit candidates;
- infer undiscovered objects by constructing designations;
- turn missing survey data into known absence.

It can only download knowledge already available to the account. Worlds that
have not been survey-scanned remain unknown and do not pass hard candidate
predicates.

## Required managed APIs

The implementation prompt must provide coherent public managed APIs equivalent
to:

```rust
client.galaxy().refresh_catalogue().await?;

let star_report = client
    .galaxy()
    .sync_replicant_stars(replicant_code)
    .await?;

let system_report = client
    .locations()
    .hydrate_system(star_designation)
    .all_known_objects()
    .max_locations(4096)
    .concurrency(4)
    .run()
    .await?;
```

Names may change only to improve accuracy/consistency. The final example must
compile without placeholders.

### `refresh_catalogue`

- performs exactly one coalesced `GET /v1/stars` refresh attempt;
- respects the special one-request-per-minute limit and `Retry-After`;
- persists generation metadata and all catalogue stars atomically;
- keeps the previous catalogue when refresh fails;
- publishes state only after commit.

### `sync_replicant_stars`

- traverses every page using `page`/`per_page`;
- validates page progression and a configurable page bound;
- persists replicant-specific star knowledge, including explored/has-life and
  distance where known;
- returns a report with pages, stars seen, and explored designations;
- does not treat missing stars as deletion unless the endpoint contract proves
  complete membership for that perspective;
- supports overlapping knowledge from multiple replicants without erasing
  richer observations.

### `hydrate_system`

- starts from the star root location;
- obtains designations only from verified response fields/fixtures;
- never guesses location codes from counts;
- recursively follows known child locations with cycle detection;
- accepts explicit maximum-location and concurrency bounds;
- uses background request priority and the shared read scheduler;
- commits partial progress and returns structured per-location errors;
- merges incomplete root/child responses without erasing richer survey data;
- remains realm-aware;
- is idempotent and restart-safe.

## Required tests

- Complete managed sync provides all owned replicants and all device pages.
- Star catalogue persists atomically and restores after restart.
- Catalogue refresh respects the one-minute special bucket.
- Every replicant-star page is fetched exactly once.
- Duplicate explored stars across replicants hydrate once.
- Non-explored stars are persisted as knowledge but are not system-hydrated.
- System root fixture yields known planets/belts/outer objects.
- Planet detail fixture yields known moon designations.
- Recursive hydration deduplicates cycles and repeated designations.
- No designation is generated from `estimated_planets`, `moon_count`, or naming
  assumptions.
- Partial system failure commits prior successes and reports failures.
- Rerunning after partial failure fetches/merges safely.
- Location request concurrency never exceeds the configured bound.
- The final candidate query performs zero HTTP requests.
- The initializer issues no mutating request.
- The example compiles with the final public API.
