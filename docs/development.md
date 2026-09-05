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
| `make ci-core` | Core Rust workspace (excluding the Tauri package), feature matrix, rustdoc, and MSRV |
| `make ci-policy` | Contract/persistence/authority policy gates and repository utility tests |
| `make ci-galaxy` | Galaxy renderer format, Clippy, rustdoc, and WASM build |
| `make ci-web` | Web format, lint, tests, typecheck, and production bundle |
| `make ci-desktop` | Desktop formatting, Tauri Rust checks/lint/tests/docs, and sidecar-script tests |
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

Use the narrowest target that proves a change while iterating, then run the
applicable domain CI target. Run `make ci` before a release or whenever a change
crosses multiple domains.

## GitHub Actions change selection

The self-hosted GitHub workflow first runs `scripts/ci_changed.py` against the
push's base and head commits. It classifies changed paths by dependency impact,
then starts only the affected domain jobs. Examples:

- `apps/web/**` runs web CI but not core Rust or desktop CI;
- `crates/galaxy-renderer/**` runs both Galaxy and web CI because the generated
  WASM is a web dependency;
- root client/core crate changes run core and policy CI;
- `apps/desktop/**` runs desktop CI;
- crawler changes run crawler CI;
- Compose/Docker deployment files run `make compose-check`;
- build-orchestration files such as `Makefile`, `rust-toolchain.toml`, the
  workflow itself, or the classifier force every domain to run.

A documentation-only push that does not affect a build domain completes with
only change detection and the final summary job. Manual `workflow_dispatch`
always runs every domain. If the push base cannot be resolved (for example, an
initial or rewritten history boundary), the classifier deliberately falls back
to all domains rather than risk skipping a necessary gate.

The workflow does not cancel an in-flight run when a newer push arrives. Change
selection compares each push to its immediate predecessor; cancelling the older
run could otherwise let a later docs-only push skip code changes that never
finished validation.

`CI Summary` is always emitted and treats selected-job failures as failures while
accepting intentionally skipped domains. Use that stable status for branch
protection if GitHub is used as a required CI surface.

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
