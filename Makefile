SHELL := /bin/sh
CARGO ?= cargo
PYTHON ?= python3
NPM ?= npm
WASM_PACK ?= wasm-pack
WEB_DIR := apps/web
GALAXY_RENDERER_DIR := crates/galaxy-renderer
GALAXY_WASM_OUT := ../../apps/web/src/wasm/galaxy_renderer

.PHONY: help fmt fmt-check galaxy-wasm web-fmt web-fmt-check web-check lint test doc check check-raw check-events check-all-features feature-checks contract-policy-check observability-policy-check policy-checks remediation-policy-check ci zip

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

zip:
	$(PYTHON) scripts/repo_zip.py $(if $(ZIP_NAME),--output "$(ZIP_NAME)")
