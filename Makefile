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

.PHONY: help fmt fmt-check galaxy-wasm web-fmt web-fmt-check web-check desktop-fmt desktop-fmt-check desktop-prepare desktop-check desktop-sidecar desktop-dev desktop-build lint test doc check check-raw check-events check-all-features feature-checks contract-policy-check observability-policy-check policy-checks remediation-policy-check ci docker-artifacts docker-build docker-check docker-up docker-down docker-smoke docker-persistence-smoke zip token token-rotate

help:
	@printf '%s\n' \
	  'replicant-client' \
	  '' \
	  'Usage: make <target>' \
	  '' \
	  'fmt                    		Format Rust and frontend sources' \
	  'fmt-check              		Verify Rust and frontend formatting' \
	  'web-check              		Run frontend format, lint, test, and build checks' \
	  'desktop-check          		Compile and smoke-test desktop packaging' \
	  'desktop-sidecar        		Build the release replicantd sidecar' \
	  'desktop-dev            		Run the desktop development shell' \
	  'desktop-build          		Build native desktop release packages' \
	  'lint                   		Run Clippy with warnings denied' \
	  'test                   		Run tests with all features enabled' \
	  'doc                    		Build docs with warnings denied' \
	  'feature-checks         		cargo check across the supported feature combinations' \
	  'contract-policy-check  		Verify the Replicant Space 2.5.0 operation inventory and exclusions' \
	  'observability-policy-check 	Verify tracing targets, timing events, and secret guards' \
	  'policy-checks          		Run all checked-in policy gates' \
	  'ci                    		Run the full local CI-equivalent suite' \
	  'docker-artifacts      		Build release daemon + web artifacts locally' \
	  'docker-build          		Build locally, then package production images' \
	  'docker-check          		Validate Compose and build the production images' \
	  'docker-up             		Start the production Compose stack' \
	  'docker-down           		Stop the stack without deleting durable data' \
	  'docker-smoke          		Start and probe a configured full stack' \
	  'docker-persistence-smoke		Prove the data directory survives container recreation' \
	  'zip                    		Create a clean working-tree ZIP for handoff' \
	  'token                  		Generate a new REPLICANTD_TOKEN in .env if not present' \
	  'token-rotate           		Rotate the REPLICANTD_TOKEN in .env

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

contract-policy-check:
	$(PYTHON) scripts/contract_policy_check.py

policy-checks: contract-policy-check
	$(PYTHON) scripts/forward_compatibility_policy_check.py
	$(PYTHON) scripts/raw_transport_policy_check.py
	$(PYTHON) scripts/schema_policy_check.py
	$(PYTHON) scripts/authority_matrix_check.py

ci: desktop-prepare fmt-check lint test check-all doc policy-checks web-check desktop-check

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
	$(DOCKER_COMPOSE) down

docker-rebuild-deploy: docker-down docker-build docker-up

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