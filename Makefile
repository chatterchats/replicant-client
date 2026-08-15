SHELL := /bin/sh
CARGO ?= cargo
PYTHON ?= python3
NPM ?= npm
WASM_PACK ?= wasm-pack
DOCKER_COMPOSE ?= docker compose
WEB_DIR := apps/web
GALAXY_RENDERER_DIR := crates/galaxy-renderer
GALAXY_WASM_OUT := ../../apps/web/src/wasm/galaxy_renderer

.PHONY: help fmt fmt-check galaxy-wasm web-fmt web-fmt-check web-check lint test doc check check-raw check-events check-all-features feature-checks contract-policy-check observability-policy-check policy-checks remediation-policy-check ci docker-build docker-check docker-up docker-down docker-smoke docker-persistence-smoke zip

help:
	@printf '%s\n' \
	  'replicant-client' \
	  '' \
	  'Usage: make <target>' \
	  '' \
	  'fmt                    		Format Rust and frontend sources' \
	  'fmt-check              		Verify Rust and frontend formatting' \
	  'web-check              		Run frontend format, lint, test, and build checks' \
	  'lint                   		Run Clippy with warnings denied' \
	  'test                   		Run tests with all features enabled' \
	  'doc                    		Build docs with warnings denied' \
	  'feature-checks         		cargo check across the supported feature combinations' \
	  'contract-policy-check  		Verify the Replicant Space 2.5.0 operation inventory and exclusions' \
	  'observability-policy-check 	Verify tracing targets, timing events, and secret guards' \
	  'policy-checks          		Run all checked-in policy gates' \
	  'ci                    		Run the full local CI-equivalent suite' \
	  'docker-build          		Build the production container images' \
	  'docker-check          		Validate Compose and build the production images' \
	  'docker-up             		Start the production Compose stack' \
	  'docker-down           		Stop the stack without deleting durable data' \
	  'docker-smoke          		Start and probe a configured full stack' \
	  'docker-persistence-smoke	Prove the data volume survives container recreation' \
	  'zip                    		Create a clean working-tree ZIP for handoff'

clean:
	$(CARGO) clean

build:
	$(CARGO) build --all-features

build-workspace:
	$(CARGO) build --workspace --all-features

fmt:
	$(CARGO) fmt --all
	$(MAKE) web-fmt

fmt-check:
	$(CARGO) fmt --all -- --check
	$(MAKE) web-fmt-check

galaxy-wasm:
	$(WASM_PACK) build $(GALAXY_RENDERER_DIR) --target web --out-dir $(GALAXY_WASM_OUT) --release --locked

web-fmt:
	$(NPM) --prefix $(WEB_DIR) run format

web-fmt-check:
	$(NPM) --prefix $(WEB_DIR) run format:check

web-check:
	$(NPM) --prefix $(WEB_DIR) run check

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

ci: fmt-check lint test check-all doc policy-checks web-check

docker-build:
	$(DOCKER_COMPOSE) build

docker-check:
	$(DOCKER_COMPOSE) config --quiet
	RS_API_TOKEN_FILE_HOST=.env.example $(DOCKER_COMPOSE) -f compose.yaml -f compose.secret.yaml config --quiet
	$(DOCKER_COMPOSE) -f compose.yaml -f compose.headless.yaml config --quiet
	$(DOCKER_COMPOSE) build

docker-up:
	$(DOCKER_COMPOSE) up -d

docker-down:
	$(DOCKER_COMPOSE) down

docker-smoke: docker-check
	$(DOCKER_COMPOSE) up -d --wait
	curl --fail --silent "http://127.0.0.1:$${REPLICANT_WEB_PORT:-8080}/healthz" >/dev/null
	curl --fail --silent "http://127.0.0.1:$${REPLICANT_WEB_PORT:-8080}/api/health" >/dev/null
	curl --http1.1 --silent --max-time 2 --include \
	  -H 'Connection: Upgrade' -H 'Upgrade: websocket' \
	  -H 'Sec-WebSocket-Version: 13' -H 'Sec-WebSocket-Key: MDEyMzQ1Njc4OWFiY2RlZg==' \
	  "http://127.0.0.1:$${REPLICANT_WEB_PORT:-8080}/ws" | grep -q '101 Switching Protocols'

docker-persistence-smoke: docker-build
	$(DOCKER_COMPOSE) run --rm --no-deps --entrypoint sh replicantd \
	  -c 'printf persisted > /var/lib/replicant/.persistence-smoke'
	$(DOCKER_COMPOSE) run --rm --no-deps --entrypoint sh replicantd \
	  -c 'test "$$(cat /var/lib/replicant/.persistence-smoke)" = persisted'

zip:
	$(PYTHON) scripts/repo_zip.py $(if $(ZIP_NAME),--output "$(ZIP_NAME)")
