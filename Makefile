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

.PHONY: help build build-workspace check-all ci clean contract-policy-check desktop-build desktop-check desktop-dev desktop-fmt desktop-fmt-check desktop-prepare desktop-sidecar doc docker-artifacts docker-build docker-check docker-down docker-persistence-smoke docker-rebuild-deploy docker-restart docker-smoke docker-up docs-reference-sync else fmt fmt-check galaxy-wasm lint observability-down observability-up policy-checks test token token-rotate web-check web-fmt web-fmt-check zip

help:
	@printf '%s\n' \
	  'replicant-client' \
	  '' \
	  'Usage: make <target>' \
	  '' \
	  'Gates' \
	  '  ci                       Full local CI-equivalent suite (expensive)' \
	  '  lint                     Clippy with warnings denied' \
	  '  test                     cargo test --all-features' \
	  '  check-all                cargo check --all-features --all-targets' \
	  '  doc                      Build docs with warnings denied' \
	  '  policy-checks            Run all checked-in policy gates' \
	  '  contract-policy-check    Verify operation inventory and exclusions only' \
	  '' \
	  'Build and format' \
	  '  build                    cargo build --all-features (root package)' \
	  '  build-workspace          cargo build --workspace --all-features' \
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
	  '  token                    Generate a REPLICANTD_TOKEN in .env if not present' \
	  '  token-rotate             Rotate the REPLICANTD_TOKEN in .env' \
	  '' \
	  'Feature-combination checks have no target; run cargo directly:' \
	  '  cargo check --no-default-features --features raw' \
	  '  cargo check --no-default-features --features events'

clean:
	$(CARGO) clean

build:
	$(CARGO) build --all-features

build-workspace:
	$(CARGO) build --workspace --all-features

fmt:
	$(CARGO) fmt --all
	$(MAKE) web-fmt
	$(MAKE) desktop-fmt

fmt-check:
	$(CARGO) fmt --all -- --check
	$(MAKE) web-fmt-check
	$(MAKE) desktop-fmt-check

galaxy-wasm:
	$(WASM_PACK) build $(GALAXY_RENDERER_DIR) --target web --out-dir $(GALAXY_WASM_OUT) --release --locked

web-fmt:
	$(NPM) --prefix $(WEB_DIR) run format

web-fmt-check:
	$(NPM) --prefix $(WEB_DIR) run format:check

web-check:
	$(NPM) --prefix $(WEB_DIR) run check

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

lint:
	$(CARGO) clippy --all-targets --all-features -- -D warnings

check-all:
	$(CARGO) check --all-features --all-targets

test:
	$(CARGO) test --all-features

doc:
	RUSTDOCFLAGS="-D warnings" $(CARGO) doc --all-features --no-deps

docs-reference-sync:
	@test -x "$(DOCS_CRAWLER_PYTHON)" || { \
	  printf '%s\n' "Missing crawler virtualenv: $(DOCS_CRAWLER_PYTHON)" \
	    "Create it with: python3 -m venv $(DOCS_CRAWLER_DIR)/venv" \
	    "Then install: $(DOCS_CRAWLER_PYTHON) -m pip install -r $(DOCS_CRAWLER_DIR)/requirements.txt"; \
	  exit 1; \
	}
	$(DOCS_CRAWLER_PYTHON) $(DOCS_CRAWLER_DIR)/crawl_replicant_docs.py --refresh

contract-policy-check:
	$(PYTHON) scripts/contract_policy_check.py

policy-checks: contract-policy-check
	$(PYTHON) scripts/contract_coverage_check.py
	$(PYTHON) scripts/forward_compatibility_policy_check.py
	$(PYTHON) scripts/raw_transport_policy_check.py
	$(PYTHON) scripts/schema_policy_check.py
	$(PYTHON) scripts/authority_matrix_check.py

ci: desktop-prepare lint test check-all doc policy-checks web-check desktop-check

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

zip:
	$(PYTHON) scripts/repo_zip.py $(if $(ZIP_NAME),--output "$(ZIP_NAME)")


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