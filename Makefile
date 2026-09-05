SHELL := /bin/sh
.DELETE_ON_ERROR:

CARGO ?= cargo
PYTHON ?= python3
NPM ?= npm
NODE ?= node
RUSTUP ?= rustup
WASM_PACK ?= wasm-pack
DOCKER_COMPOSE ?= docker compose

WEB_DIR := apps/web
DESKTOP_DIR := apps/desktop
GALAXY_RENDERER_DIR := crates/galaxy-renderer
GALAXY_WASM_OUT := ../../apps/web/src/wasm/galaxy_renderer
GALAXY_WASM_DIR := $(WEB_DIR)/src/wasm/galaxy_renderer
DOCS_CRAWLER_DIR := reference/replicant-docs-crawler
DOCS_CRAWLER_PYTHON ?= $(DOCS_CRAWLER_DIR)/venv/bin/python
MSRV ?= $(shell sed -n 's/^rust-version = "\([^"]*\)"/\1/p' Cargo.toml | head -n 1)

CORE_WORKSPACE_ARGS := --workspace --exclude replicant-desktop
WEB_DEPS_STAMP := $(WEB_DIR)/node_modules/.make-ready
DESKTOP_DEPS_STAMP := $(DESKTOP_DIR)/node_modules/.make-ready
CRAWLER_DEPS_STAMP := $(DOCS_CRAWLER_DIR)/venv/.make-ready
GALAXY_WASM_STAMP := $(GALAXY_WASM_DIR)/.make-ready
GALAXY_WASM_INPUTS := \
  Makefile \
  rust-toolchain.toml \
  .cargo/config.toml \
  $(GALAXY_RENDERER_DIR)/Cargo.toml \
  $(GALAXY_RENDERER_DIR)/Cargo.lock \
  $(shell find $(GALAXY_RENDERER_DIR)/src -type f -name '*.rs' 2>/dev/null)

# Public aggregate targets.
.PHONY: help doctor doctor-docker bootstrap web-deps desktop-deps crawler-deps
.PHONY: ci ci-core ci-policy ci-galaxy ci-web ci-desktop ci-docs
.PHONY: build check lint test doc fmt fmt-check clean distclean msrv-check msrv-bootstrap

# Core Rust targets.
.PHONY: rust-build rust-check-all rust-lint rust-test rust-doc rust-fmt rust-fmt-check
.PHONY: check-default check-raw check-events check-native-tls check-all-features feature-checks

# Galaxy renderer targets.
.PHONY: galaxy-check galaxy-doc galaxy-fmt galaxy-fmt-check galaxy-lint galaxy-wasm

# Web targets.
.PHONY: web-check web-build web-typecheck web-lint web-test web-fmt web-fmt-check

# Desktop targets.
.PHONY: desktop-build desktop-check desktop-dev desktop-fmt desktop-fmt-check
.PHONY: desktop-prepare desktop-sidecar desktop-script-test
.PHONY: desktop-rust-fmt desktop-rust-fmt-check desktop-rust-check desktop-rust-lint
.PHONY: desktop-rust-test desktop-rust-doc

# Documentation and policy targets.
.PHONY: docs-crawler-check docs-reference-sync policy-generate
.PHONY: contract-policy-check coverage-audit-check mutation-adapter-policy-check
.PHONY: package-contents-check contract-coverage-check forward-compatibility-policy-check
.PHONY: raw-transport-policy-check schema-policy-check authority-matrix-check
.PHONY: policy-checks policy-tests utility-tests

# Deployment and utility targets.
.PHONY: daemon-release web-release docker-artifacts compose-check docker-build docker-check
.PHONY: docker-down docker-persistence-smoke docker-rebuild-deploy docker-restart docker-smoke docker-up
.PHONY: observability-down observability-up token token-rotate zip zip-all

help:
	@printf '%s\n' \
	  'replicant-client' \
	  '' \
	  'Usage: make <target>' \
	  '' \
	  'Setup and diagnostics' \
	  '  doctor                   Verify tools required by the full local build' \
	  '  doctor-docker            Verify Docker and Compose in addition to normal tools' \
	  '  bootstrap                Install repo-local web, desktop, and crawler dependencies' \
	  '  web-deps                 Install web dependencies when package-lock.json changes' \
	  '  desktop-deps             Install desktop dependencies when package-lock.json changes' \
	  '  crawler-deps             Create/update the crawler virtualenv from requirements.txt' \
	  '' \
	  'CI gates' \
	  '  ci                       Full repository gate; composes all domain CI targets' \
	  '  ci-core                  Core Rust workspace, feature matrix, docs, and MSRV' \
	  '  ci-policy                Contract/persistence policy gates and utility tests' \
	  '  ci-galaxy                Galaxy WASM formatter, lint, docs, and build' \
	  '  ci-web                   Web format, lint, test, typecheck, and production build' \
	  '  ci-desktop               Desktop format, Rust gates, and sidecar-script tests' \
	  '  ci-docs                  Documentation crawler tests' \
	  '  check                    Compile supported Rust configurations' \
	  '  lint                     Run Rust, Galaxy, web, and desktop lint gates' \
	  '  test                     Run Rust, web, and desktop tests' \
	  '  doc                      Build Rust and Galaxy docs with warnings denied' \
	  '  msrv-check               Check replicant-client with the declared Rust MSRV' \
	  '  policy-checks            Run every checked-in policy gate' \
	  '' \
	  'Build and format' \
	  '  build                    Build the core Rust workspace' \
	  '  fmt                      Format Rust, Galaxy, web, and desktop sources' \
	  '  fmt-check                Verify all repository formatting' \
	  '  galaxy-wasm              Build generated Galaxy WASM when its inputs change' \
	  '  web-build                Typecheck and build the production web bundle' \
	  '  desktop-build            Build native desktop release packages' \
	  '  clean                    Remove Cargo and generated build outputs' \
	  '  distclean                clean plus repo-local npm and crawler dependencies' \
	  '' \
	  'Policy and reference maintenance' \
	  '  policy-generate          Regenerate checked-in operation and authority policy files' \
	  '  docs-reference-sync      Refresh the newest Replicant Space reference snapshot' \
	  '' \
	  'Docker and observability' \
	  '  compose-check            Validate all Compose configurations without building images' \
	  '  docker-artifacts         Build release daemon + staged web artifacts locally' \
	  '  docker-build             Build artifacts and package production images' \
	  '  docker-check             Compatibility alias for full docker-build validation' \
	  '  docker-up/down/restart   Manage the production Compose stack' \
	  '  docker-rebuild-deploy    Rebuild images, then redeploy sequentially' \
	  '  docker-smoke             Build, start, and probe a configured full stack' \
	  '  docker-persistence-smoke Prove the data directory survives recreation' \
	  '  observability-up/down    Manage the optional Grafana companion service' \
	  '' \
	  'Utilities' \
	  '  zip / zip-all            Create clean handoff archives' \
	  '  token / token-rotate     Create or rotate REPLICANTD_TOKEN in .env' \
	  ''

# -----------------------------------------------------------------------------
# Setup and dependency bootstrap
# -----------------------------------------------------------------------------

doctor:
	@set -eu; \
	for tool in cargo rustc rustup node npm python3 wasm-pack mold; do \
	  command -v "$$tool" >/dev/null 2>&1 || { printf 'missing required tool: %s\n' "$$tool"; exit 1; }; \
	done
	@printf 'Rust: %s\n' "$$($(CARGO) --version)"
	@printf 'rustc: %s\n' "$$(rustc --version)"
	@printf 'Node: %s\n' "$$($(NODE) --version)"
	@printf 'npm: %s\n' "$$($(NPM) --version)"
	@printf 'Python: %s\n' "$$($(PYTHON) --version 2>&1)"
	@printf 'wasm-pack: %s\n' "$$($(WASM_PACK) --version)"
	@printf 'MSRV: %s\n' "$(MSRV)"

doctor-docker: doctor
	@command -v docker >/dev/null 2>&1 || { printf '%s\n' 'missing required tool: docker'; exit 1; }
	@docker version >/dev/null
	@$(DOCKER_COMPOSE) version

$(WEB_DEPS_STAMP): $(WEB_DIR)/package.json $(WEB_DIR)/package-lock.json
	$(NPM) --prefix $(WEB_DIR) ci
	@touch $@

$(DESKTOP_DEPS_STAMP): $(DESKTOP_DIR)/package.json $(DESKTOP_DIR)/package-lock.json
	$(NPM) --prefix $(DESKTOP_DIR) ci
	@touch $@

$(CRAWLER_DEPS_STAMP): $(DOCS_CRAWLER_DIR)/requirements.txt
	@test -x "$(DOCS_CRAWLER_PYTHON)" || $(PYTHON) -m venv $(DOCS_CRAWLER_DIR)/venv
	$(DOCS_CRAWLER_PYTHON) -m pip install -r $(DOCS_CRAWLER_DIR)/requirements.txt
	@touch $@

web-deps: $(WEB_DEPS_STAMP)
desktop-deps: $(DESKTOP_DEPS_STAMP)
crawler-deps: $(CRAWLER_DEPS_STAMP)
bootstrap: web-deps desktop-deps crawler-deps

# Keep the second toolchain explicit instead of adding it to rust-toolchain.toml:
# the pinned toolchain is the normal compiler; this one exists only to prove MSRV.
msrv-bootstrap:
	@$(RUSTUP) toolchain list | grep -q '^$(MSRV)' || $(RUSTUP) toolchain install $(MSRV) --profile minimal

msrv-check: msrv-bootstrap
	RUSTFLAGS="" $(CARGO) +$(MSRV) check --locked -p replicant-client --all-features

# -----------------------------------------------------------------------------
# CI aggregates. These are intentionally domain-shaped so Actions can run only
# the domains affected by a push while `make ci` remains the authoritative full
# local gate.
# -----------------------------------------------------------------------------

ci: ci-core ci-policy ci-galaxy ci-web ci-desktop ci-docs

ci-core: rust-fmt-check rust-build rust-lint rust-test rust-check-all feature-checks rust-doc msrv-check
ci-policy: policy-checks utility-tests
ci-galaxy: galaxy-check
ci-web: web-check
ci-desktop: desktop-check
ci-docs: docs-crawler-check

# -----------------------------------------------------------------------------
# Core Rust workspace. The Tauri package is intentionally excluded here and is
# validated by the desktop domain so backend-only changes do not rebuild it.
# -----------------------------------------------------------------------------

build: rust-build
check: rust-check-all feature-checks desktop-rust-check
lint: rust-lint galaxy-lint web-lint desktop-rust-lint
test: rust-test web-test desktop-rust-test desktop-script-test
doc: rust-doc galaxy-doc desktop-rust-doc

rust-build:
	$(CARGO) build --locked $(CORE_WORKSPACE_ARGS) --all-features

rust-check-all:
	$(CARGO) check --locked $(CORE_WORKSPACE_ARGS) --all-targets --all-features

rust-lint:
	$(CARGO) clippy --locked $(CORE_WORKSPACE_ARGS) --all-targets --all-features -- -D warnings

rust-test:
	$(CARGO) test --locked $(CORE_WORKSPACE_ARGS) --all-features

rust-doc:
	RUSTDOCFLAGS="-D warnings" $(CARGO) doc --locked $(CORE_WORKSPACE_ARGS) --all-features --no-deps

rust-fmt:
	$(CARGO) fmt --all

rust-fmt-check:
	$(CARGO) fmt --all -- --check

check-default:
	$(CARGO) check --locked -p replicant-client --all-targets

check-raw:
	$(CARGO) check --locked -p replicant-client --no-default-features --features raw

check-events:
	$(CARGO) check --locked -p replicant-client --no-default-features --features events

check-native-tls:
	$(CARGO) check --locked -p replicant-client --no-default-features --features managed,native-tls

check-all-features:
	$(CARGO) check --locked -p replicant-client --all-targets --all-features

feature-checks: check-default check-raw check-events check-native-tls check-all-features

# -----------------------------------------------------------------------------
# Galaxy renderer
# -----------------------------------------------------------------------------

$(GALAXY_WASM_STAMP): $(GALAXY_WASM_INPUTS)
	RUSTFLAGS="" $(WASM_PACK) build $(GALAXY_RENDERER_DIR) --target web --out-dir $(GALAXY_WASM_OUT) --release --locked
	@touch $@

galaxy-wasm: $(GALAXY_WASM_STAMP)

galaxy-fmt:
	$(CARGO) fmt --manifest-path $(GALAXY_RENDERER_DIR)/Cargo.toml

galaxy-fmt-check:
	$(CARGO) fmt --manifest-path $(GALAXY_RENDERER_DIR)/Cargo.toml -- --check

galaxy-lint:
	RUSTFLAGS="" $(CARGO) clippy --locked --manifest-path $(GALAXY_RENDERER_DIR)/Cargo.toml --target wasm32-unknown-unknown --all-targets -- -D warnings

galaxy-doc:
	RUSTFLAGS="" RUSTDOCFLAGS="-D warnings" $(CARGO) doc --locked --manifest-path $(GALAXY_RENDERER_DIR)/Cargo.toml --target wasm32-unknown-unknown --no-deps

galaxy-check: galaxy-fmt-check galaxy-lint galaxy-doc galaxy-wasm

# -----------------------------------------------------------------------------
# Web frontend. npm scripts stay leaf-oriented; Make owns cross-language order.
# -----------------------------------------------------------------------------

web-fmt: web-deps
	$(NPM) --prefix $(WEB_DIR) run format

web-fmt-check: web-deps
	$(NPM) --prefix $(WEB_DIR) run format:check

web-lint: web-deps
	$(NPM) --prefix $(WEB_DIR) run lint

web-typecheck: web-deps
	$(NPM) --prefix $(WEB_DIR) run typecheck

web-test: web-deps galaxy-wasm
	$(NPM) --prefix $(WEB_DIR) run test

web-build: web-deps galaxy-wasm web-typecheck
	$(NPM) --prefix $(WEB_DIR) run build:web

web-check: web-fmt-check web-lint web-test web-build

# -----------------------------------------------------------------------------
# Desktop application
# -----------------------------------------------------------------------------

desktop-fmt: web-deps
	$(NPM) --prefix $(WEB_DIR) exec -- prettier --write \
	  "apps/desktop/package.json" "apps/desktop/README.md" "apps/desktop/scripts/*.mjs" \
	  "apps/desktop/src-tauri/tauri.conf.json" "apps/desktop/src-tauri/capabilities/*.json"

desktop-fmt-check: web-deps
	$(NPM) --prefix $(WEB_DIR) exec -- prettier --check \
	  "apps/desktop/package.json" "apps/desktop/README.md" "apps/desktop/scripts/*.mjs" \
	  "apps/desktop/src-tauri/tauri.conf.json" "apps/desktop/src-tauri/capabilities/*.json"

desktop-prepare:
	$(NODE) $(DESKTOP_DIR)/scripts/prepare-sidecar.mjs

desktop-rust-fmt:
	$(CARGO) fmt -p replicant-desktop

desktop-rust-fmt-check:
	$(CARGO) fmt -p replicant-desktop -- --check

desktop-rust-check: desktop-prepare
	$(CARGO) check --locked -p replicant-desktop --all-targets

desktop-rust-lint: desktop-prepare
	$(CARGO) clippy --locked -p replicant-desktop --all-targets -- -D warnings

desktop-rust-test: desktop-prepare
	$(CARGO) test --locked -p replicant-desktop

desktop-rust-doc: desktop-prepare
	RUSTDOCFLAGS="-D warnings" $(CARGO) doc --locked -p replicant-desktop --no-deps

desktop-script-test: desktop-deps
	$(NPM) --prefix $(DESKTOP_DIR) run check

desktop-check: desktop-fmt-check desktop-rust-fmt-check desktop-rust-check desktop-rust-lint desktop-rust-test desktop-rust-doc desktop-script-test

desktop-sidecar:
	$(NODE) $(DESKTOP_DIR)/scripts/prepare-sidecar.mjs --release

desktop-dev: web-deps desktop-deps
	$(NPM) --prefix $(DESKTOP_DIR) run dev

desktop-build: web-deps desktop-deps
	$(NPM) --prefix $(DESKTOP_DIR) run build

# -----------------------------------------------------------------------------
# Formatting aggregates
# -----------------------------------------------------------------------------

fmt: rust-fmt galaxy-fmt web-fmt desktop-fmt
fmt-check: rust-fmt-check galaxy-fmt-check web-fmt-check desktop-fmt-check

# -----------------------------------------------------------------------------
# Documentation and policy
# -----------------------------------------------------------------------------

docs-reference-sync: crawler-deps
	$(DOCS_CRAWLER_PYTHON) $(DOCS_CRAWLER_DIR)/crawl_replicant_docs.py --refresh

docs-crawler-check: crawler-deps
	cd $(DOCS_CRAWLER_DIR) && $(abspath $(DOCS_CRAWLER_PYTHON)) -m unittest discover -p 'test_*.py'

policy-generate:
	$(PYTHON) scripts/generate_operation_inventory.py
	$(PYTHON) scripts/generate_authority_matrix.py

contract-policy-check:
	$(PYTHON) scripts/contract_policy_check.py

coverage-audit-check:
	$(PYTHON) scripts/coverage_audit.py check

mutation-adapter-policy-check:
	$(PYTHON) scripts/mutation_adapter_policy_check.py

package-contents-check:
	$(PYTHON) scripts/package_contents_check.py

contract-coverage-check:
	$(PYTHON) scripts/contract_coverage_check.py

forward-compatibility-policy-check:
	$(PYTHON) scripts/forward_compatibility_policy_check.py

raw-transport-policy-check:
	$(PYTHON) scripts/raw_transport_policy_check.py

schema-policy-check:
	$(PYTHON) scripts/schema_policy_check.py

authority-matrix-check:
	$(PYTHON) scripts/authority_matrix_check.py

policy-tests:
	$(PYTHON) scripts/test_contract_coverage.py

utility-tests:
	$(PYTHON) scripts/test_repo_zip.py
	$(PYTHON) scripts/test_manage_token.py
	$(PYTHON) scripts/test_ci_changed.py

policy-checks: contract-policy-check coverage-audit-check mutation-adapter-policy-check \
  package-contents-check contract-coverage-check forward-compatibility-policy-check \
  raw-transport-policy-check schema-policy-check authority-matrix-check policy-tests

# -----------------------------------------------------------------------------
# Deployment and observability. Dockerfiles intentionally package host-built
# artifacts; they do not compile the Rust workspace or web application.
# -----------------------------------------------------------------------------

daemon-release:
	$(CARGO) build --locked --release -p replicant-server --bin replicantd

web-release: web-build
	rm -rf target/docker/web
	mkdir -p target/docker/web
	cp -a $(WEB_DIR)/dist/. target/docker/web/

docker-artifacts: daemon-release web-release

# Use a harmless placeholder so configuration validation never depends on a
# developer's real daemon or Replicant Space credentials.
compose-check:
	REPLICANTD_TOKEN=compose-check $(DOCKER_COMPOSE) config --quiet
	REPLICANTD_TOKEN=compose-check RS_API_TOKEN_FILE_HOST=.env.example \
	  $(DOCKER_COMPOSE) -f compose.yaml -f compose.secret.yaml config --quiet
	REPLICANTD_TOKEN=compose-check \
	  $(DOCKER_COMPOSE) -f compose.yaml -f compose.headless.yaml config --quiet

docker-build: docker-artifacts compose-check
	$(DOCKER_COMPOSE) build

# Backward-compatible name retained for existing operator habits/documentation.
docker-check: docker-build

docker-up:
	$(DOCKER_COMPOSE) up -d

docker-down:
	$(DOCKER_COMPOSE) stop

docker-restart:
	$(DOCKER_COMPOSE) stop
	$(DOCKER_COMPOSE) up -d

docker-rebuild-deploy:
	$(MAKE) docker-build
	$(DOCKER_COMPOSE) up -d

observability-up: daemon-release compose-check
	mkdir -p "$${REPLICANT_DATA_DIR:-$${HOME}/.local/share/replicant}/telemetry" "$${REPLICANT_DATA_DIR:-$${HOME}/.local/share/replicant}/grafana"
	$(DOCKER_COMPOSE) --profile observability up -d --build replicantd grafana

observability-down:
	$(DOCKER_COMPOSE) --profile observability stop grafana

# Probes go through the web container, which injects the daemon credential.
docker-smoke: docker-build
	$(DOCKER_COMPOSE) up -d --wait
	curl --fail --silent "http://127.0.0.1:$${REPLICANT_WEB_PORT:-8080}/healthz" >/dev/null
	curl --fail --silent "http://127.0.0.1:$${REPLICANT_WEB_PORT:-8080}/api/health" >/dev/null
	curl --http1.1 --silent --max-time 2 --include \
	  -H 'Connection: Upgrade' -H 'Upgrade: websocket' \
	  -H 'Sec-WebSocket-Version: 13' -H 'Sec-WebSocket-Key: MDEyMzQ1Njc4OWFiY2RlZg==' \
	  "http://127.0.0.1:$${REPLICANT_WEB_PORT:-8080}/ws?token=$${REPLICANTD_TOKEN}" | grep -q '101 Switching Protocols'

docker-persistence-smoke: docker-build
	$(DOCKER_COMPOSE) run --rm --no-deps --entrypoint sh replicantd \
	  -c 'printf persisted > /var/lib/replicant/.persistence-smoke'
	$(DOCKER_COMPOSE) run --rm --no-deps --entrypoint sh replicantd \
	  -c 'test "$$(cat /var/lib/replicant/.persistence-smoke)" = persisted'

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------

zip:
	$(PYTHON) scripts/repo_zip.py $(if $(ZIP_NAME),--output "$(ZIP_NAME)")

zip-all:
	$(PYTHON) scripts/repo_zip.py --include-local-data $(if $(ZIP_NAME),--output "$(ZIP_NAME)")

token:
	$(PYTHON) scripts/manage_token.py

token-rotate:
	$(PYTHON) scripts/manage_token.py --rotate

clean:
	$(CARGO) clean
	$(CARGO) clean --manifest-path $(GALAXY_RENDERER_DIR)/Cargo.toml
	rm -rf target/docker $(WEB_DIR)/dist $(GALAXY_WASM_DIR)

distclean: clean
	rm -rf $(WEB_DIR)/node_modules $(DESKTOP_DIR)/node_modules $(DOCS_CRAWLER_DIR)/venv
