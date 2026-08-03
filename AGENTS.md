# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`replicant-client` is a durable, stateful Rust client for Replicant Space documentation through `2.3.5`, using the checked-in verified `2.3.3` OpenAPI file as the machine-readable baseline plus explicit rendered-document deltas. The primary entry point is `replicant_client::Client` — it fetches, validates, normalizes, persists (SQLite), publishes, watches, reconciles, and performs game operations without the application assembling a transport client, runtime, state actor, or persistence layer itself.

**Status:** Phase 1 bootstrap stage (see `Cargo.toml` package version `1.0.0`). The package, feature graph, contract corpus, domain model, and managed-client skeleton exist; large parts of the managed client (sync engine, event engine, operations engine) are still stubs. Before starting any new work, read `docs/implementation/rewrite-guide.md` in full — it is the authoritative implementation guide, records every locked product decision, and defines the phase sequence and each phase's definition of done. Do not treat this CLAUDE.md as a substitute for it.

This repo is a from-scratch rewrite of an earlier project (`replicant-sdk`), not a v2 of its public API. Do not port old naming, feature tiers, or compatibility concerns — see `docs/implementation/rewrite-guide.md` §31 for what is/isn't reusable from the old repo and §33 for porting discipline.

## Commands

```sh
cargo fmt --all -- --check          # formatting (make fmt-check)
cargo clippy --all-targets --all-features -- -D warnings   # lint, warnings denied (make lint)
cargo test --all-features           # full test suite (make test)
cargo check                         # default features
cargo check --no-default-features --features raw
cargo check --no-default-features --features events
cargo check --all-features
python3 scripts/contract_policy_check.py   # contract/deprecation policy gate (make contract-policy-check)
make ci                              # fmt-check + lint + test + all feature checks + doc + contract-policy-check
```

Single test: `cargo test --all-features <test_name>`. Cargo aliases (`.cargo/config.toml`): `cargo t` = `test --all-features`, `cargo cl` = `clippy --all-targets --all-features -- -D warnings`.

When a change affects which operations/fields/aliases the client exposes, regenerate and re-check policy:

```sh
python3 scripts/generate_operation_inventory.py
python3 scripts/contract_policy_check.py
```

Never weaken `scripts/contract_policy_check.py` (or the other `scripts/*_policy_check.py` gates) to make a change pass — fix the implementation or update the relevant `policy/*.json` file with an accurate reason and evidence citation.

## Architecture

### Feature-gated module tiers (module boundaries, not crates — single root package, edition 2024, no workspace)

```
raw      -> HTTP transport, auth, request/response DTOs, pagination, rate-limit metadata
events   -> raw + SSE framing / raw event streaming
managed  -> events + SQLite store, state engine, sync, durable operations, managed Client (default)
```

Feature implication is cumulative (`managed` implies `events` implies `raw`). Never add a dependency to a lower tier merely because a higher tier needs it — mark it `optional = true` and attach it to the feature that actually needs it (see `Cargo.toml`).

- `src/raw/` — one submodule per API resource group (accounts, devices, replicants, galaxy, trading, simulations, ...). `raw::Client` exposes the 77 current, non-deprecated, non-admin operations recorded as `"supported"` in `policy/operations.json` plus operations explicitly recorded in `policy/documented-operation-deltas.json`; it returns transport DTOs + response metadata only and never hydrates, persists, publishes, journals, or reconciles. Mutating (unsafe) calls are never auto-retried — a `Transport` error on them is definitionally ambiguous (`Error::is_ambiguous_transport_failure`); safe reads (`GET`/`HEAD`) may retry with bounded backoff.
- `src/domain/` — normalized snapshot types (`model.rs`), ID newtypes (`ids.rs`), pure merge/authority rules (`merge.rs`), observation provenance (`observation.rs`), local query types (`query.rs`), and open/forward-compatible vocab enums (`vocab.rs`). Endpoint DTOs are converted into domain types only through `adapters.rs`. This module deliberately contains **no** persistence, networking, or raw DTOs in its snapshot types.
- `src/managed/` — the managed client: `client.rs` (lifecycle/builder/`ClientInner` ownership), `gateways.rs` (per-resource gateways/handles that normalize the one response they fetch, commit it to SQLite, publish the resulting state revision, and only then return a domain value), `state.rs` (state engine/publication), `store.rs` (SQLite repository layer), `sync.rs` (synchronization/reconciliation surface). Only normalized domain types and the gateway/handle/sync API are public; runtime orchestration, SQL repositories, event reducers, and the operation journal are internal.
- `migrations/` — SQLite schema (schema v1, fresh — not a migration of the old crate's DB).
- `reference/replicant-space/` — the checked-in, corrected Replicant Space 2.3.1 OpenAPI baseline and historical rendered corpus. Newer rendered-document changes are recorded under `docs/contract/` and `policy/documented-operation-deltas.json`; do not attribute them to the older OpenAPI file. Rendered documentation deprecation asides override missing OpenAPI `deprecated` flags (see `policy/contract-metadata.json`).
- `policy/` — machine-readable contract inventory: `operations.json` (the 2.3.1 OpenAPI operations: 77 supported / 5 deprecated / 2 admin), `documented-operation-deltas.json` (newer rendered-doc-only operations), `authority-matrix.json` (per-endpoint authority/reconciliation classification), `contract-metadata.json` (checksums + doc/OpenAPI mismatch log), `excluded-fields.json`, `normalization-aliases.json` (e.g. mining response `belt`->`location`, `designation`->`site`), `persistence-schema.json`.
- `scripts/*_policy_check.py` — gates that verify the checked-in policy files against `reference/replicant-space/openapi.json` and the implementation (contract drift, authority-matrix coverage, forward-compatibility, raw-transport coverage, schema policy). Run via `make ci` / individual `make` targets and in `.github/workflows/ci.yml`.

### Core invariants to preserve

- **Managed reads commit before returning.** A successful `client.devices().get(code)`-style call means: decode -> validate -> normalize into a domain observation -> apply endpoint-specific authority rules -> commit SQLite transaction -> publish state revision -> return the domain value. Never return success if persistence/publication failed, and never issue a second HTTP request just to "refresh" internally.
- **Local queries never do network I/O.** `find()`, `miners()`, `cached()`, `state()` are local-only. Network behavior is always an explicit method: `get`/`refresh` (targeted), `sync` (bounded reconciliation), `watch` (subscription), `raw` (bypass).
- **Realm isolation.** Every world entity is keyed by `Realm` (`Live` vs `Simulation(SimulationId)`). Simulation entities must never overwrite live-world records.
- **Visibility is not existence.** A missing/absent entity in a filtered or scoped response is not proof of deletion — only specific authoritative signals (documented in `docs/implementation/rewrite-guide.md` §12.3 and `policy/authority-matrix.json`) justify a tombstone. Only a successful *full, unfiltered* collection traversal may reconcile membership for that collection.
- **Owned vs public data.** Public/directory observations must never erase private owned fields (see `client.replicants().get_owned()` vs `client.directory().replicant()`).
- **Durable operations.** Every unsafe managed mutation is registered durably (SQLite) before transmission, submitted at most once, and its transport outcome is classified (accepted/rejected/ambiguous) rather than blindly retried.
- **Forward compatibility.** Unknown JSON fields are ignored by default; unknown event/command values are preserved, not discarded; public enums that model open server vocabularies are non-exhaustive / string-backed rather than exhaustive `TypedCommand`-style enums.
- **No deprecated or admin surface**, even through `raw`: excluded operations have no corresponding client method at all (enforced by `scripts/contract_policy_check.py` against `policy/operations.json`).

### Error handling

One crate-level `Error` enum (`src/error.rs`) with structured variants (`Configuration`, `Authentication`, `RateLimited`, `Transport`, `Decode`, `Contract`, `Normalize`, `Persistence`, `State`, `Event`, `Synchronization`, `Operation`, `Closed`, ...). Avoid unjustified `panic!`/`unwrap`/`expect` and `todo!`/`unimplemented!` in production code paths (per `CONTRIBUTING.md`).

## Contribution rules (from CONTRIBUTING.md / SECURITY.md)

- Never commit tokens, authorization headers, private message bodies, or databases containing user data.
- Keep PRs scoped to one phase of `docs/implementation/rewrite-guide.md`.
- Do not weaken lint, test, or contract-policy gates to make a change pass.
- See `SECURITY.md` for vulnerability reporting.

# context-mode — MANDATORY routing rules

context-mode MCP tools available. Rules protect context window from flooding. One unrouted command dumps 56 KB into context.

## Think in Code — MANDATORY

Analyze/count/filter/compare/search/parse/transform data: **write code** via `ctx_execute(language, code)`, `console.log()` only the answer. Do NOT read raw data into context. PROGRAM the analysis, not COMPUTE it. Pure JavaScript — Node.js built-ins only (`fs`, `path`, `child_process`). `try/catch`, handle `null`/`undefined`. One script replaces ten tool calls.

## BLOCKED — do NOT attempt

### curl / wget — BLOCKED
Intercepted and replaced with error. Do NOT retry.
Use: `ctx_fetch_and_index(url, source)` or `ctx_execute(language: "javascript", code: "const r = await fetch(...)")`

### Inline HTTP — BLOCKED
`fetch('http`, `requests.get(`, `requests.post(`, `http.get(`, `http.request(` — intercepted. Do NOT retry.
Use: `ctx_execute(language, code)` — only stdout enters context

### WebFetch — BLOCKED
Use: `ctx_fetch_and_index(url, source)` then `ctx_search(queries)`

## REDIRECTED — use sandbox

### Bash (>20 lines output)
Bash ONLY for: `git`, `mkdir`, `rm`, `mv`, `cd`, `ls`, `npm install`, `pip install`.
Otherwise: `ctx_batch_execute(commands, queries)` or `ctx_execute(language: "javascript", code: "...")`. Use `language: "shell"` only when code matches the host shell.

### Read (for analysis)
Reading to **Edit** → Read correct. Reading to **analyze/explore/summarize** → `ctx_execute_file(path, language, code)`.

### Grep — may flood context
Use `ctx_execute(language: "javascript", code: "...")` in sandbox for portable filtering/counting.

## Tool selection

0. **MEMORY**: `ctx_search(sort: "timeline")` — after resume, check prior context before asking user.
1. **GATHER**: `ctx_batch_execute(commands, queries)` — runs all commands, auto-indexes, returns search. ONE call replaces 30+. Each command: `{label: "header", command: "..."}`.
2. **FOLLOW-UP**: `ctx_search(queries: ["q1", "q2", ...])` — all questions as array, ONE call (default relevance mode).
3. **PROCESSING**: `ctx_execute(language, code)` | `ctx_execute_file(path, language, code)` — sandbox, only stdout enters context.
4. **WEB**: `ctx_fetch_and_index(url, source)` then `ctx_search(queries)` — raw HTML never enters context.
5. **INDEX**: `ctx_index(content, source)` — store in FTS5 for later search.

## Parallel I/O batches

For multi-URL fetches or multi-API calls, **always** include `concurrency: N` (1-8):

- `ctx_batch_execute(commands: [3+ network commands], concurrency: 5)` — gh, curl, dig, docker inspect, multi-region cloud queries
- `ctx_fetch_and_index(requests: [{url, source}, ...], concurrency: 5)` — multi-URL batch fetch

**Use concurrency 4-8** for I/O-bound work (network calls, API queries). **Keep concurrency 1** for CPU-bound (npm test, build, lint) or commands sharing state (ports, lock files, same-repo writes).

GitHub API rate-limit: cap at 4 for `gh` calls.

## Subagent routing

Routing block auto-injected into subagent prompts. Bash-type subagents upgraded to general-purpose. No manual instruction needed.

## Output

Write artifacts to FILES — never inline. Return: file path + 1-line description.
Descriptive source labels for `ctx_search(source: "label")`.

## Session Continuity

Skills, roles, and decisions persist for the entire session. Do not abandon them as the conversation grows.

## Memory

Session history is persistent and searchable. On resume, search BEFORE asking the user:

| Need | Command |
|------|---------|
| What were we working on? | `ctx_search(queries: ["summary"], source: "compaction", sort: "timeline")` |
| What was the first request? | `ctx_search(queries: ["prompt"], source: "user-prompt", sort: "timeline")` |
| What did we decide? | `ctx_search(queries: ["decision"], source: "decision", sort: "timeline")` |
| What NOT to repeat? | `ctx_search(queries: ["rejected"], source: "rejected-approach")` |
| What constraints exist? | `ctx_search(queries: ["constraint"], source: "constraint")` |

DO NOT ask "what were we working on?" — SEARCH FIRST.
If search returns 0 results, proceed as a fresh session.

## ctx commands

| Command | Action |
|---------|--------|
| `ctx stats` | Call `ctx_stats` MCP tool, display full output verbatim |
| `ctx doctor` | Call `ctx_doctor` MCP tool, run returned shell command, display as checklist |
| `ctx upgrade` | Call `ctx_upgrade` MCP tool, run returned shell command, display as checklist |
| `ctx purge` | Call `ctx_purge` MCP tool with confirm: true. Warns before wiping knowledge base. |

After /clear or /compact: knowledge base and session stats preserved. Use `ctx purge` to start fresh.
