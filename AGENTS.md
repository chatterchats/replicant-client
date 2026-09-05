# AGENTS.md

Rust workspace for Replicant Space automation: a durable API client, an
application runtime above it, a local daemon, and three frontends.

This file is injected into every session. It is a map and a rulebook, not a
tutorial. Read the linked files when you actually need them.

---

## Orientation

Read this section before searching the tree. It exists so you don't have to
glob your way to the answer.

| Path | What lives there | Size |
| --- | --- | --- |
| `src/raw/` | Typed HTTP transport, one module per API tag. DTOs, pagination, rate-limit metadata. No state. | 9.4k |
| `src/managed/` | SQLite store, sync engine, durable operations, gateways. The managed `Client`. | — |
| `src/domain/` | Normalized domain types and pure authority rules. | — |
| `src/events.rs` | SSE event envelope + typed AMI digest payloads. | — |
| `src/raw/vocab.rs` | **Event name registry.** Every `foo.bar` event string the client knows. | — |
| `crates/replicant-runtime/` | Reports, actions, queries, Automation Director, campaign planning. | 64k |
| `crates/replicant-workflow/` | Durable workflows: supervisor, claims, waits, checkpoints. | 5.5k |
| `crates/replicant-server/` | `replicantd` — HTTP commands/queries + local WebSocket deltas. | 8.7k |
| `crates/replicant-protocol/` | Typed daemon↔frontend protocol. `RuntimeSnapshot`, `DomainSlice`, `LiveDelta`. | 3.4k |
| `crates/replicant-cli/` | Unified CLI. | 5.8k |
| `crates/replicant-*-planner/`, `-printing`, `-transport` | Pure planning primitives. No I/O. | — |
| `crates/galaxy-renderer/` | WASM renderer. **Not a workspace member** — built via `make galaxy-wasm`. | 2.2k |
| `apps/web/` | React + TS + Vite + MUI. | — |
| `apps/desktop/` | Tauri shell. Wraps `apps/web` and bundles `replicantd` as a sidecar. | — |
| `reference/replicant-space-*/` | Pinned API doc snapshots + `openapi.json`. Read-only corpus. | — |
| `policy/` | JSON policy files the contract gates check against. | — |
| `scripts/` | Python policy checkers and generators. | — |
| `tests/contract_*.rs` | Per-version contract fixtures. Wiremock-based, no live account. | — |

`replicant-runtime` is 64k lines. Never read it whole — use `grep`, `lsp`, or
`glob` to reach the specific module. Its submodules are named for their domain
(`mining.rs`, `survey.rs`, `relay.rs`, `orchestration.rs`, `automation.rs`).

---

## Layering and authority

```
Replicant Space --SSE--> replicant-client --> replicant-runtime
                                                    |
                                             replicant-workflow
                                                    |
                                              replicantd --WebSocket--> GUI
                                                    |
                                            replicant-cli / React / Tauri
```

Three databases, three different truths. Do not blur them:

- **managed client DB** — game/API truth and operation reconciliation.
- **runtime/workflow DB** — application and workflow truth.
- **frontend store** — disposable projection. Never authoritative.

Upstream is SSE. Daemon→GUI is WebSocket. There is no webhook architecture.

Two invariants that are easy to violate by accident:

- The Automation Director never issues game commands directly. Mechanical work
  belongs in registered workflows and managed operations.
- Workforce automation is **grow-only**. Nothing deletes, retires, or scales
  down Replicants.

Read `ARCHITECTURE.md` before changing anything in the runtime/workflow/daemon
layering or adding a workflow kind.

---

## Feature tiers

Module boundaries in the root package are enforced by Cargo features, not by
crates:

```
raw  ->  events  ->  managed (default)
```

- `raw` — transport, auth, DTOs, pagination, rate-limit metadata.
- `events` — SSE framing and streaming, on top of `raw`.
- `managed` — SQLite store, sync, durable operations, the managed `Client`.

Do not add a dependency to a lower tier because a higher tier already uses it.
Mark feature-specific deps `optional = true` and attach them to the owning
feature.

`Client` (managed) is the normal entry point. `raw` is an explicit escape
hatch: it never hydrates, persists, publishes, journals, or reconciles.

---

## Commands

Toolchain is pinned: Rust 1.96.0, edition 2024, MSRV 1.94. The pinned toolchain
also installs `clippy`, `rustfmt`, and `wasm32-unknown-unknown`.

```sh
make doctor       # verify host tools
make bootstrap    # install repo-local npm/crawler dependencies
make fmt-check    # verify all formatting
make check        # compile supported Rust configurations
make lint         # Rust/Galaxy/web/desktop lint gates
make test         # Rust/web/desktop tests
make doc          # rustdoc gates, warnings denied
make policy-checks
make ci           # authoritative full repository gate
```

Domain CI targets are composed by the conditional self-hosted Actions job:

```sh
make ci-core
make ci-policy
make ci-galaxy
make ci-web
make ci-desktop
make ci-docs
```

Feature combinations are first-class Make targets: `check-default`,
`check-raw`, `check-events`, `check-native-tls`, `check-all-features`, and
`feature-checks`. `make msrv-check` separately proves the root client against
its declared MSRV. CI intentionally avoids standalone build/check passes when
Clippy/tests already compile the same all-feature configuration.

Cargo aliases remain available for narrow work, but Make owns cross-component
ordering. See `docs/development.md` for the build graph, dependency stamps, and
GitHub path-classification rules.

**Cost discipline.** Do not run full `make ci` to validate a one-line change.
Iterate with the narrowest leaf/domain target that proves the change. The
GitHub mirror applies the same rule automatically: it classifies changed paths,
runs the affected domains together in one Make invocation, and lets a newer run
replace an older in-flight run without losing unvalidated changes.

---

## Contract authority

The highest semantic-version snapshot under `reference/replicant-space-*` is
the current contract. Tooling resolves it automatically. Current pin lives in
`Cargo.toml` under `[package.metadata.replicant-space]`, including sha256
digests of both the doc manifest and the OpenAPI document.

- The reference corpus is **byte-for-byte pinned**. Do not reformat it. The
  root `.prettierignore` excludes it; do not defeat that.
- Refresh it only through `make docs-reference-sync`.
- Rendered-doc deprecation asides override missing OpenAPI `deprecated` flags.
  See `policy/contract-metadata.json`.
- Where the rendered docs and `openapi.json` disagree, that disagreement is a
  finding to record, not a coin flip to resolve silently.

When a change affects which operations, fields, or aliases the client exposes,
update the relevant file under `policy/` and regenerate:

```sh
make policy-generate
make policy-checks
```

---

## Hard rules

These are gates, not preferences. Do not negotiate with them.

1. **Never weaken a check to make a change pass.** Not clippy, not a test, not
   a policy script. Fix the implementation, or amend the policy file with an
   accurate reason and an evidence citation.
2. **No production `todo!`, `unimplemented!`, unjustified `panic!`, or casual
   `unwrap`/`expect`.**
3. **Never commit secrets**: API tokens, authorization headers, private message
   bodies, or databases containing user data. Secrets live in `.env`
   (gitignored); `.env.example` holds placeholders only.
4. **Never log secrets.** The client emits `tracing` events but never records
   secret values, authorization headers, or request bodies. Keep it that way.
5. **Do not expose deprecated or admin-only Replicant Space operations**, even
   through `raw`.
6. **Do not return API keys to the frontend.** `REPLICANTD_TOKEN` is injected
   server-side by the web container and never reaches the browser.
7. **`make ci` must never require a live Replicant account.** Tests are
   fixture- and wiremock-based.

---

## Conventions

- Rust: `max_width = 100`, field-init shorthand, try shorthand. `missing_docs`
  is a warning — public items need doc comments.
- Clippy: `correctness` and `suspicious` are **deny**; `all` is warn. Cognitive
  complexity threshold is 20.
- Tests: prefer fixture/state-based over timing-dependent. New contract
  behaviour gets a case in the matching `tests/contract_<version>.rs`.
  Regressions get a test.
- Commits: one reviewable Conventional Commit per logical change
  (`feat(devices): ...`, `fix(sync): ...`). Stage only files you intended to
  touch; do not sweep up pre-existing working-tree edits.
- Prefer a typed managed/domain read over dropping to `raw`. Reach for `raw`
  only when a clean typed read genuinely does not exist.

---

## Known stale references

Do not trust these; the code is authoritative:

- `CURRENT_STATE.md` is a historical Phase 9 UI snapshot, not current state.

If you fix one of these, delete its bullet here.
