# Phase 11.6.01b — Location domain and predicates

## Contract evidence

| Source | SHA-256 |
| --- | --- |
| `reference/replicant-space/openapi.json` | `ca018a938541f23c4838e8fe58f78889d9ca4b9ab81b488112f90589dd83c2f4` |
| `api/locations/index.md` | `ea842b6d0f21ed0bbec10c3139aee5ad2234db0adebf66fc12ae05d74d5fad6a` |
| `api/replicants/scan/index.md` | `bb71529d77a01f04548f7c1c5c5ef89cd1812fc946180fa4ea5cc551dc484f3b` |
| `concepts/civilisations/index.md` | `9058f6493b4992b0101853ba59bdd9f5b4b3df416a8206e558aea2c4543d0709` |
| sanitized `ILPHARD-3` fixture | `d42909cf73d5866fbf3d700a839455ba80d442245ecc8b461167ee7283d0b429` |

The fixture is derived from the supplied authenticated response with devices,
event text, account-specific progress, and resource details removed. It verifies
the nested `planet` shape: `atmosphere`, `in_habitable_zone`, `life_stage`,
`magnetic_field`, `surface_gravity` (Earth gravities), and `surface_temp_c`
(Celsius). `moon` accepts the same verified shape. Unknown top-level and nested
fields are retained as JSON maps.

## Domain, authority, and persistence

`raw::locations::PlanetaryBody` feeds `domain::location_detail` and produces a
realm-qualified `Location` with `LocationEnvironment`. `Knowledge<T>` models
`Unknown`, `Absent`, and `Present(T)` distinctly. In particular, a surveyed
planet/moon with a body but no life stage is known no-life; an unobserved body
is unknown. Unknown future `LifeStage` strings round-trip but have no ordering.

Location commits merge into the current observation before SQLite upsert and
snapshot publication. A `Knowledge::Unknown` field never replaces a known
value, so a less-complete detail response cannot erase prior survey data.
The existing realm-qualified `locations.observation_json` projection requires
no schema migration and restores the complete environment on restart.

## Public API and predicate truth table

`Client::locations()` now coherently exposes `cached`, explicit remote
`get`/`refresh`, local `find`, and the existing contribution mutation.
`LocationQuery` has `planetary_bodies`, `surveyed`, `has_atmosphere`,
`atmosphere_is`, `has_magnetic_field`, `in_habitable_zone`,
`life_stage_below`, strict `gravity_g_*`/`surface_temp_c_*` bounds, inclusive
`*_between(RangeInclusive)`, `in_realm`, `in_system`, and `at`.

| Knowledge/value | Predicate decision |
| --- | --- |
| unknown required field | `Unknown`; does not enter `collect()` |
| known absent atmosphere/gravity/temperature | rejected |
| surveyed false | `surveyed()` rejected |
| canonical life rank strictly below threshold | matched |
| known no-life | matched by `life_stage_below` |
| intelligent or higher at `below(Intelligent)` | rejected |
| future life-stage string | `Unknown`; does not enter ordered predicates |
| `above`/`below` equality | rejected (strict) |
| `between(a..=b)` endpoint | matched (inclusive) |

`collect_with_diagnostics()` uses the identical evaluator and returns stable
predicate IDs, matched/rejected/unknown outcomes, sanitized observed values,
and reasons. Both query paths read only the immutable state snapshot. The
durable state currently has no star-coordinate projection, so
`distance_from_sol_ly()` remains `Knowledge::Unknown` until a verified,
durably persisted coordinate join exists; it cannot silently match a hard
predicate or gain a heuristic bonus.

## Riker candidate CLI

`crates/replicant-rikers-cli/src/main.rs` is the installed target command. Its
only remote action is `client.sync().full()`. It then runs Riker's hard query
locally, assigns an explainable non-authoritative score, sorts by score then
distance from SOL, caps output at ten, and only prints `Riker, how about
<designation>?`; it never sends BobNet.

The final direct-value query API returns `Location` snapshots rather than
handles, so the CLI uses `world.id().as_str()` instead of
`handle.snapshot().await?.key.id`. This preserves the same stable
realm-qualified designation without a redundant local async hop.

`Location` exposes typed `Knowledge` accessors for the scoring facts. Verified
nested fields populate atmosphere, gravity, temperature, magnetic field,
habitable-zone membership, life stage, and axial tilt. Rotation, host spectral
type, belt richness, and SOL distance deliberately remain `Unknown` until an
authoritative response fixture and durable source projection verify them. The
heuristic treats unknown bonuses as neutral; M dwarfs and near-tidal-locking,
when known, are cautions rather than exclusions, while complex life remains
eligible but receives an ethical-risk caution.

## Flow and verification

The electronics Mermaid pack now documents:

`raw::locations::PlanetaryBody -> location_detail -> persist_location -> SQLite
and snapshot -> LocationQuery -> collect/diagnostics`.

Regression coverage includes nested fixture decoding, unknown nested fields,
unit boundaries and unknown behavior, canonical/future life stages, local-only
collection with diagnostics equivalence, and SQLite restart/partial-observation
merge preservation. `full_sync_restores_every_durable_managed_domain_after_restart`
also executes the exact Riker hard query three times after `full()` with all
mock HTTP expectations exhausted, proving all three `collect()` calls are
local-only. The existing snapshot benchmark remains the performance baseline;
no new index or query engine was introduced.

Commands passed before this report was written:

```text
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

Files changed by this prompt: `src/raw/locations.rs`, `src/domain/model.rs`,
`src/domain/adapters.rs`, `src/domain/vocab.rs`, `src/managed/state.rs`,
`src/managed/gateways.rs`, `src/managed/operation.rs`, `src/managed/sync.rs`, `src/lib.rs`,
`crates/replicant-rikers-cli/src/main.rs`, the
sanitized fixture, Mermaid pack, remediation ledger/checker, and this report.
