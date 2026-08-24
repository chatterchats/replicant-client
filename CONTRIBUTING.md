# Contributing

`replicant-client` is a Cargo workspace (edition 2024). The root package is the
durable client; the members under `crates/` add the application runtime,
durable workflows, the local daemon, planners, and the CLI.
`apps/desktop/src-tauri` is also a member. `crates/galaxy-renderer` is
deliberately **not** a member — it is built for WASM through
`make galaxy-wasm`.

Within the root package, module boundaries are enforced by Cargo features
rather than by separate crates:

| Feature             | Implies  | Owns                                                                                       |
| ------------------- | -------- | ------------------------------------------------------------------------------------------ |
| `raw`               | —        | HTTP transport, authentication, request/response models, pagination, rate-limit metadata.  |
| `events`            | `raw`    | SSE framing and raw event streaming.                                                       |
| `managed` (default) | `events` | SQLite store, state engine, synchronization, durable operations, and the managed `Client`. |

Do not add a dependency to a lower tier merely because a higher tier already
uses it. Mark feature-specific dependencies `optional = true` and attach
them to the feature that needs them.

## Toolchain

`rust-toolchain.toml` pins the build to Rust **1.96.0**. `Cargo.toml` declares
an MSRV (`rust-version`) of **1.94**, and `clippy.toml` sets `msrv = "1.94"`.
Do not use APIs newer than the declared MSRV without raising it deliberately in
both places.

## Contract authority

The highest semantic-version Replicant Space snapshot under
`reference/replicant-space-*` is the current contract. The contract/policy
tooling resolves it automatically; older snapshots remain available for
regression work. The active pin, including sha256 digests of both the rendered
documentation manifest and the OpenAPI document, is recorded in `Cargo.toml`
under `[package.metadata.replicant-space]`.

The corpus is byte-for-byte pinned by its manifest and must not be reformatted;
the repository-level `.prettierignore` excludes it so a manual
`prettier . --write` does not alter the verified reference material. Refresh
that directory only through `make docs-reference-sync`.

Rendered documentation deprecation asides override missing OpenAPI
`deprecated` flags — see `policy/contract-metadata.json`. Where the rendered
documentation and `openapi.json` disagree on anything else, treat the
disagreement as a finding to record rather than a choice to make silently.

Whenever a change affects which operations, fields, or aliases the client
exposes, update the relevant policy file under `policy/` and re-run:

```sh
python3 scripts/generate_operation_inventory.py
python3 scripts/generate_authority_matrix.py
python3 scripts/contract_policy_check.py
```

Do not weaken `scripts/contract_policy_check.py` — or any other gate — to make
a change pass. Fix the implementation, or update the policy file with an
accurate reason and evidence citation.

## Tests and checks

The full repository gate is one command:

```sh
make ci
```

It runs `desktop-prepare`, `lint`, `test`, `check-all`, `doc`,
`policy-checks`, `web-check`, and `desktop-check`. It is expensive — it builds
the whole workspace plus the web bundle plus the Tauri desktop check. Iterate
with the narrowest check that proves your change, then run
`make fmt && make ci` once before committing.

Individual gates:

```sh
make fmt          # format Rust, web, and desktop sources
make lint         # cargo clippy --all-targets --all-features -- -D warnings
make test         # cargo test --all-features
make check-all    # cargo check --all-features --all-targets
make doc          # rustdoc with warnings denied
make policy-checks
make web-check    # frontend format, lint, test, build
make desktop-check
```

Feature-combination checks have no make target; run them directly:

```sh
cargo check --no-default-features --features raw
cargo check --no-default-features --features events
cargo check --all-features
```

Cargo aliases from `.cargo/config.toml`: `cargo t` (test all features) and
`cargo cl` (the clippy gate).

`make ci` must never require a live Replicant Space account. Tests are
fixture- and `wiremock`-based. New contract behaviour belongs in the matching
`tests/contract_<version>.rs`; discovered bugs get a regression test.

## Pull requests and scope

- One reviewable Conventional Commit per logical change
  (`feat(devices): ...`, `fix(sync): ...`).
- Do not weaken lint, test, or contract-policy gates.
- Do not add production `todo!`, `unimplemented!`, unjustified `panic!`, or
  casual `unwrap`/`expect`.
- Never commit tokens, authorization headers, private message bodies, or
  databases containing user data. Secrets live in `.env` (gitignored);
  `.env.example` carries placeholders only.
- Do not expose deprecated or admin-only Replicant Space operations, even
  through `raw`.
- Do not return API keys or the daemon token to the browser.
- Stage only the files you intended to change; do not sweep up unrelated
  pre-existing working-tree edits.

See [AGENTS.md](AGENTS.md) for the repository map and layering rules, and
[ARCHITECTURE.md](ARCHITECTURE.md) before changing the
runtime / workflow / daemon layering or adding a workflow kind.

See [SECURITY.md](SECURITY.md) for vulnerability reporting.
