SHELL := /bin/sh
CARGO ?= cargo
PYTHON ?= python3
NPM ?= npm
WASM_PACK ?= wasm-pack
DOCKER_COMPOSE ?= docker compose
WEB_DIR := apps/web
DESKTOP_DIR := apps/desktop
GALAXY_RENDERER_DIR := crates/galaxy-renderer
GALAXY_WASM_OUT := ../../apps/web/src/wasm/galaxy_renderer
DOCS_CRAWLER_DIR := reference/replicant-docs-crawler
DOCS_CRAWLER_PYTHON ?= $(DOCS_CRAWLER_DIR)/venv/bin/python

# Aggregate and workspace targets
.PHONY: help ci clean build build-workspace fmt fmt-check lint test doc
.PHONY: check-all check-all-features check-events check-raw feature-checks

# Galaxy renderer targets
.PHONY: galaxy-check galaxy-doc galaxy-fmt galaxy-fmt-check galaxy-lint galaxy-wasm

# Frontend and desktop targets
.PHONY: web-check web-fmt web-fmt-check
.PHONY: desktop-build desktop-check desktop-dev desktop-fmt desktop-fmt-check
.PHONY: desktop-prepare desktop-sidecar

# Documentation and policy targets
.PHONY: docs-crawler-check docs-reference-sync contract-policy-check coverage-audit-check
.PHONY: mutation-adapter-policy-check package-contents-check policy-checks policy-tests

# Deployment and utility targets
.PHONY: docker-artifacts docker-build docker-check docker-down docker-persistence-smoke
.PHONY: docker-rebuild-deploy docker-restart docker-smoke docker-up
.PHONY: observability-down observability-up token token-rotate utility-tests zip zip-all

help:
	@printf '%s\n' \
	  'replicant-client' \
	  '' \
	  'Usage: make <target>' \
	  '' \
	  'Gates' \
	  '  ci                       Full local CI-equivalent suite (expensive)' \
	  '  lint                     Clippy all workspace targets and feature modes' \
	  '  test                     Test the workspace in default and all-feature modes' \
	  '  check-all                Check all workspace targets and features' \
	  '  feature-checks           Check raw, events, and all-feature configurations' \
	  '  galaxy-check             Format, lint, document, and build the WASM crate' \
	  '  docs-crawler-check       Test the documentation crawler' \
	  '  doc                      Build workspace docs with warnings denied' \
	  '  policy-checks            Run all checked-in policy gates and policy tests' \
	  '  contract-policy-check    Verify operation inventory and exclusions only' \
	  '  coverage-audit-check     Verify current units and schema fields' \
	  '  utility-tests            Test repository utility scripts' \
	  '' \
	  'Build and format' \
	  '  build                    Build the workspace in default and all-feature modes' \
	  '  build-workspace          Alias for build' \
	  '  clean                    cargo clean' \
	  '  fmt                      Format Rust and frontend sources' \
	  '  fmt-check                Verify Rust and frontend formatting' \
	  '  galaxy-wasm              Build the WASM galaxy renderer into apps/web' \
	  '' \
	  'Frontend and desktop' \
	  '  web-check                Frontend format, lint, test, and build checks' \
	  '  desktop-check            Compile and smoke-test desktop packaging' \
	  '  desktop-sidecar          Build the release replicantd sidecar' \
	  '  desktop-dev              Run the desktop development shell' \
	  '  desktop-build            Build native desktop release packages' \
	  '' \
	  'Docker and observability' \
	  '  docker-artifacts         Build release daemon + web artifacts locally' \
	  '  docker-build             Build locally, then package production images' \
	  '  docker-check             Validate Compose and build the production images' \
	  '  docker-up                Start the production Compose stack' \
	  '  docker-down              Stop the stack without deleting durable data' \
	  '  docker-restart           Restart the running stack' \
	  '  docker-rebuild-deploy    Rebuild and redeploy the stack' \
	  '  docker-smoke             Start and probe a configured full stack' \
	  '  docker-persistence-smoke Prove the data directory survives recreation' \
	  '  observability-up         Start Grafana with the provisioned dashboards' \
	  '  observability-down       Stop the optional Grafana companion service' \
	  '' \
	  'Utilities' \
	  '  docs-reference-sync      Refresh the newest Replicant Space reference snapshot' \
	  '  zip                      Create a clean working-tree ZIP for handoff' \
	  '  zip-all                  Create repository, local log, and database ZIPs' \
	  '  token                    Generate a REPLICANTD_TOKEN in .env if not present' \
	  '  token-rotate             Rotate the REPLICANTD_TOKEN in .env' \
	  ''

# Aggregate gate
ci: fmt-check build lint test check-all feature-checks doc policy-checks utility-tests \
  docs-crawler-check galaxy-check web-check desktop-check

# Workspace lifecycle and quality
clean:
	$(CARGO) clean
	$(CARGO) clean --manifest-path $(GALAXY_RENDERER_DIR)/Cargo.toml

build: desktop-prepare
	$(CARGO) build --workspace
	$(CARGO) build --workspace --all-features

build-workspace: build

fmt: galaxy-fmt
	$(CARGO) fmt --all
	$(MAKE) web-fmt
	$(MAKE) desktop-fmt

fmt-check: galaxy-fmt-check
	$(CARGO) fmt --all -- --check
	$(MAKE) web-fmt-check
	$(MAKE) desktop-fmt-check

lint: galaxy-lint
	$(CARGO) clippy --workspace --all-targets -- -D warnings
	$(CARGO) clippy --workspace --all-targets --all-features -- -D warnings

check-all: check-all-features
	$(CARGO) check --workspace --all-targets

check-all-features:
	$(CARGO) check --workspace --all-targets --all-features

check-raw:
	$(CARGO) check -p replicant-client --no-default-features --features raw

check-events:
	$(CARGO) check -p replicant-client --no-default-features --features events

feature-checks: check-raw check-events check-all-features

test:
	$(CARGO) test --workspace
	$(CARGO) test --workspace --all-features

doc: galaxy-doc
	RUSTDOCFLAGS="-D warnings" $(CARGO) doc --workspace --all-features --no-deps

# Galaxy renderer
galaxy-wasm:
	RUSTFLAGS="" $(WASM_PACK) build $(GALAXY_RENDERER_DIR) --target web --out-dir $(GALAXY_WASM_OUT) --release --locked

galaxy-fmt:
	$(CARGO) fmt --manifest-path $(GALAXY_RENDERER_DIR)/Cargo.toml

galaxy-fmt-check:
	$(CARGO) fmt --manifest-path $(GALAXY_RENDERER_DIR)/Cargo.toml -- --check

galaxy-lint:
	RUSTFLAGS="" $(CARGO) clippy --manifest-path $(GALAXY_RENDERER_DIR)/Cargo.toml --target wasm32-unknown-unknown --all-targets -- -D warnings

galaxy-doc:
	RUSTFLAGS="" RUSTDOCFLAGS="-D warnings" $(CARGO) doc --manifest-path $(GALAXY_RENDERER_DIR)/Cargo.toml --target wasm32-unknown-unknown --no-deps

galaxy-check: galaxy-fmt-check galaxy-lint galaxy-doc galaxy-wasm

# Web frontend
web-fmt:
	$(NPM) --prefix $(WEB_DIR) run format

web-fmt-check:
	$(NPM) --prefix $(WEB_DIR) run format:check

web-check:
	RUSTFLAGS="" $(NPM) --prefix $(WEB_DIR) run check

# Desktop application
desktop-fmt:
	$(NPM) --prefix $(WEB_DIR) exec -- prettier --write \
	  "apps/desktop/package.json" "apps/desktop/README.md" "apps/desktop/scripts/*.mjs" \
	  "apps/desktop/src-tauri/tauri.conf.json" "apps/desktop/src-tauri/capabilities/*.json"

desktop-fmt-check:
	$(NPM) --prefix $(WEB_DIR) exec -- prettier --check \
	  "apps/desktop/package.json" "apps/desktop/README.md" "apps/desktop/scripts/*.mjs" \
	  "apps/desktop/src-tauri/tauri.conf.json" "apps/desktop/src-tauri/capabilities/*.json"

desktop-prepare:
	node $(DESKTOP_DIR)/scripts/prepare-sidecar.mjs

desktop-check: desktop-prepare
	$(CARGO) check -p replicant-desktop --all-targets
	$(NPM) --prefix $(DESKTOP_DIR) run check

desktop-sidecar:
	node $(DESKTOP_DIR)/scripts/prepare-sidecar.mjs --release

desktop-dev:
	$(NPM) --prefix $(DESKTOP_DIR) run dev

desktop-build:
	$(NPM) --prefix $(DESKTOP_DIR) run build

# Documentation and policy
docs-reference-sync:
	@test -x "$(DOCS_CRAWLER_PYTHON)" || { \
	  printf '%s\n' "Missing crawler virtualenv: $(DOCS_CRAWLER_PYTHON)" \
	    "Create it with: python3 -m venv $(DOCS_CRAWLER_DIR)/venv" \
	    "Then install: $(DOCS_CRAWLER_PYTHON) -m pip install -r $(DOCS_CRAWLER_DIR)/requirements.txt"; \
	  exit 1; \
	}
	$(DOCS_CRAWLER_PYTHON) $(DOCS_CRAWLER_DIR)/crawl_replicant_docs.py --refresh

docs-crawler-check:
	@test -x "$(DOCS_CRAWLER_PYTHON)" || { \
	  printf '%s\n' "Missing crawler virtualenv: $(DOCS_CRAWLER_PYTHON)" \
	    "Create it with: python3 -m venv $(DOCS_CRAWLER_DIR)/venv" \
	    "Then install: $(DOCS_CRAWLER_PYTHON) -m pip install -r $(DOCS_CRAWLER_DIR)/requirements.txt"; \
	  exit 1; \
	}
	cd $(DOCS_CRAWLER_DIR) && $(abspath $(DOCS_CRAWLER_PYTHON)) -m unittest discover -p 'test_*.py'

contract-policy-check:
	$(PYTHON) scripts/contract_policy_check.py

coverage-audit-check:
	$(PYTHON) scripts/coverage_audit.py check

mutation-adapter-policy-check:
	$(PYTHON) scripts/mutation_adapter_policy_check.py

package-contents-check:
	$(PYTHON) scripts/package_contents_check.py

policy-tests:
	$(PYTHON) scripts/test_contract_coverage.py

utility-tests:
	$(PYTHON) scripts/test_repo_zip.py

policy-checks: contract-policy-check coverage-audit-check mutation-adapter-policy-check \
  package-contents-check policy-tests
	$(PYTHON) scripts/contract_coverage_check.py
	$(PYTHON) scripts/forward_compatibility_policy_check.py
	$(PYTHON) scripts/raw_transport_policy_check.py
	$(PYTHON) scripts/schema_policy_check.py
	$(PYTHON) scripts/authority_matrix_check.py

# Deployment and observability
docker-artifacts:
	$(CARGO) build --locked --release -p replicant-server --bin replicantd
	$(NPM) --prefix $(WEB_DIR) ci
	$(MAKE) galaxy-wasm
	$(NPM) --prefix $(WEB_DIR) run build:web
	rm -rf target/docker/web
	mkdir -p target/docker/web
	cp -a $(WEB_DIR)/dist/. target/docker/web/

docker-build: docker-artifacts
	$(DOCKER_COMPOSE) build

docker-check: docker-artifacts
	$(DOCKER_COMPOSE) config --quiet
	RS_API_TOKEN_FILE_HOST=.env.example $(DOCKER_COMPOSE) -f compose.yaml -f compose.secret.yaml config --quiet
	$(DOCKER_COMPOSE) -f compose.yaml -f compose.headless.yaml config --quiet
	$(DOCKER_COMPOSE) build

docker-up:
	$(DOCKER_COMPOSE) up -d

docker-down:
	$(DOCKER_COMPOSE) stop

observability-up:
	$(CARGO) build --locked --release -p replicant-server --bin replicantd
	mkdir -p "$${REPLICANT_DATA_DIR:-$${HOME}/.local/share/replicant}/telemetry" "$${REPLICANT_DATA_DIR:-$${HOME}/.local/share/replicant}/grafana"
	$(DOCKER_COMPOSE) --profile observability up -d --build replicantd grafana

observability-down:
	$(DOCKER_COMPOSE) --profile observability stop grafana

docker-rebuild-deploy: docker-build docker-up

docker-restart: docker-down docker-up

# Probes go through the web container, which injects the daemon credential,
# so no token is needed here even though the daemon requires one.
docker-smoke: docker-check
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

# Utilities
zip:
	$(PYTHON) scripts/repo_zip.py $(if $(ZIP_NAME),--output "$(ZIP_NAME)")

zip-all:
	$(PYTHON) scripts/repo_zip.py --include-local-data $(if $(ZIP_NAME),--output "$(ZIP_NAME)")


# Kept as a `define` block rather than an inline recipe: Make joins
# backslash-continued recipe lines before the shell sees them, so a shell
# heredoc or multi-line script cannot survive there. Exporting it puts the
# program in the environment with its newlines intact.
define REPLICANTD_TOKEN_PY
import os
import pathlib
import secrets

env = pathlib.Path(".env")
example = pathlib.Path(".env.example")
rotate = os.environ.get("ROTATE") == "1"

if not env.exists():
    if not example.exists():
        raise SystemExit("no .env and no .env.example to copy from")
    env.write_text(example.read_text())
    print("created .env from .env.example")

lines = env.read_text().splitlines()
current = next(
    (line.split("=", 1)[1] for line in lines if line.startswith("REPLICANTD_TOKEN=")),
    "",
)

if current and not rotate:
    print('REPLICANTD_TOKEN is already set in .env; use "make token-rotate" to replace it')
    raise SystemExit(0)

token = secrets.token_urlsafe(32)
if any(line.startswith("REPLICANTD_TOKEN=") for line in lines):
    lines = [
        f"REPLICANTD_TOKEN={token}" if line.startswith("REPLICANTD_TOKEN=") else line
        for line in lines
    ]
else:
    lines.append(f"REPLICANTD_TOKEN={token}")
env.write_text("\n".join(lines) + "\n")

if rotate:
    print("rotated REPLICANTD_TOKEN in .env")
    print("restart the stack, and rebuild the web image so the frontend picks it up")
else:
    print("wrote a new REPLICANTD_TOKEN to .env")
endef
export REPLICANTD_TOKEN_PY

token:
	@$(PYTHON) -c "$$REPLICANTD_TOKEN_PY"

token-rotate:
	@ROTATE=1 $(PYTHON) -c "$$REPLICANTD_TOKEN_PY"