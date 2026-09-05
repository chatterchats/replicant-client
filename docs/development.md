# Development and CI

The repository is a multi-tool build, but the root `Makefile` is the canonical
cross-component orchestration interface. Cargo, npm, Python, wasm-pack, and
Docker remain responsible for their own work; Make owns dependency order and
composes those leaf operations into local and CI gates.

## Toolchain

The normal Rust compiler is pinned by `rust-toolchain.toml`. The file also
requests `clippy`, `rustfmt`, and the `wasm32-unknown-unknown` target used by the
Galaxy renderer. `Cargo.toml` separately declares the library MSRV; `make
msrv-check` installs that second toolchain on demand if it is missing and checks
the root client with it.

The full local build expects:

- Rust/rustup plus the repository-pinned components;
- `mold`, because `.cargo/config.toml` selects it as the native linker;
- Node.js and npm;
- Python 3 with `venv` support;
- `wasm-pack`;
- Docker with the Compose plugin only for deployment/container targets.

Run `make doctor` for normal development tools or `make doctor-docker` when
working on deployment.

## Bootstrap

Repository-local dependencies are lockfile/requirements-backed Make targets:

```sh
make bootstrap
```

This runs `npm ci` for the web and desktop applications and creates/updates the
documentation crawler virtualenv. Each dependency set has a stamp beneath its
ignored generated directory, so subsequent invocations are no-ops until the
corresponding lockfile or requirements file changes.

Narrow bootstrap targets are also available:

```sh
make web-deps
make desktop-deps
make crawler-deps
```

CI checkouts are clean, so Actions still get reproducible clean dependency
installs while local repeated checks avoid reinstalling unchanged dependencies.

## Domain CI targets

`make ci` is the authoritative full repository gate. It is composed from
independently runnable domains:

| Target | Covers |
| --- | --- |
| `make ci-core` | Core Rust format, feature matrix, all-feature Clippy/tests, rustdoc, and MSRV |
| `make ci-policy` | Contract/persistence/authority policy gates and repository utility tests |
| `make ci-galaxy` | Galaxy renderer format, Clippy, rustdoc, and WASM build |
| `make ci-web` | Web format, lint, tests, typecheck, and production bundle |
| `make ci-desktop` | Desktop formatting, Tauri Clippy/tests/docs, and sidecar-script tests |
| `make ci-docs` | Documentation crawler tests |

The Tauri package remains a Cargo workspace member, but core CI excludes it and
`ci-desktop` validates it explicitly. This preserves full `make ci` coverage
without making backend-only changes pay the Tauri cost.

The root client feature matrix is explicit:

```sh
make check-default
make check-raw
make check-events
make check-native-tls
make check-all-features
make feature-checks
```

`feature-checks` covers the configurations that differ from the primary
all-feature CI configuration. `check-all-features`, `rust-check-all`, and
`rust-build` remain available as explicit developer targets, but `ci-core`
does not repeat those standalone passes because all-feature Clippy and tests
already compile that configuration. Desktop CI follows the same rule and does
not run a separate `desktop-rust-check` before Clippy/tests.

Use the narrowest target that proves a change while iterating, then run the
applicable domain CI target. Run `make ci` before a release or whenever a change
crosses multiple domains.

## GitHub Actions change selection

The self-hosted GitHub workflow resolves the most recent successful run of the
same workflow on the current branch, then runs `scripts/ci_changed.py` against
that validated commit and the current `HEAD`. It classifies changed paths by
dependency impact. Examples:

- `apps/web/**` runs web CI but not core Rust or desktop CI;
- `crates/galaxy-renderer/**` runs both Galaxy and web CI because the generated
  WASM is a web dependency;
- root client/core crate changes run core and policy CI;
- `apps/desktop/**` runs desktop CI;
- crawler changes run crawler CI;
- Compose/Docker deployment files run `make compose-check`;
- build-orchestration files such as `Makefile`, `rust-toolchain.toml`, the
  workflow itself, or the classifier force every domain to run.

All affected domains execute in one `Selective CI` job and one Make invocation.
That matches the single self-hosted runner: there is no useful job-level
parallelism to gain, while one checkout lets Make deduplicate shared
prerequisites such as dependency bootstrap and Galaxy WASM generation.

A documentation-only push that does not affect a build domain completes after
change detection with no build target. Manual `workflow_dispatch` always runs
every domain. If the successful-run lookup or Git history cannot be resolved,
the classifier deliberately falls back to all domains rather than risk
skipping a necessary gate.

The workflow cancels an in-flight older run when a newer push arrives. This is
safe because the replacement run compares the newest `HEAD` to the last
successful validation baseline, so it sees the union of every still-unvalidated
change rather than only the immediately preceding commit.

`Selective CI` is the stable job status to use for branch protection if GitHub
is used as a required CI surface.

## Generated Galaxy WASM

`make galaxy-wasm` owns the cross-language renderer build. Its stamp lives
inside the ignored generated WASM directory and depends on the renderer's
Cargo manifests/lockfile and Rust sources. Unchanged renderer inputs therefore
do not cause repeat wasm-pack builds in one checkout; deleting the generated
directory naturally invalidates the stamp.

The web npm scripts remain usable directly, but root Make targets do not call an
npm script that recursively calls Make. This avoids duplicated orchestration.

## Formatting, checks, and tests

Common full-repository aggregates:

```sh
make fmt
make fmt-check
make check
make lint
make test
make doc
```

For web-only iteration:

```sh
make web-fmt-check
make web-lint
make web-typecheck
make web-test
make web-build
```

For desktop-only iteration:

```sh
make desktop-fmt-check
make desktop-check
```

## Policy maintenance

Validation is separate from state-changing generation:

```sh
make policy-checks
make policy-generate
```

`policy-generate` regenerates the operation inventory and authority matrix. The
outputs are checked in; inspect their diff and then run `make policy-checks`.
Reference documentation is refreshed separately with:

```sh
make docs-reference-sync
```

## Docker

`make compose-check` validates the base, secret-overlay, and headless Compose
configurations with harmless placeholder credentials and does not compile or
package application artifacts.

`make docker-build` is intentionally heavier: it builds the release daemon and
web artifacts on the host, stages the web bundle, validates Compose, and then
packages the production images. `make docker-check` is retained as a
compatibility alias for that full image-build validation.

State-changing deployment targets encode their required sequence in their
recipes rather than relying on prerequisite ordering, so `make -j` cannot turn
a restart/redeploy into concurrent stop/start operations.

See `docs/docker.md` for runtime, persistence, backup, and observability details.

## Cleaning

```sh
make clean
```

removes Cargo/generated build outputs while retaining downloaded npm/crawler
dependencies. Use:

```sh
make distclean
```

to remove those repo-local dependency directories too.
