SHELL := /bin/sh
CARGO ?= cargo
PYTHON ?= python3

.PHONY: help fmt fmt-check lint test doc check check-raw check-events check-all-features feature-checks contract-policy-check observability-policy-check policy-checks remediation-policy-check ci zip

help:
	@printf '%s\n' \
	  'replicant-client' \
	  '' \
	  'Usage: make <target>' \
	  '' \
	  'fmt                    		Format all Rust sources' \
	  'fmt-check              		Verify formatting without modifying files' \
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

fmt-check:
	$(CARGO) fmt --all -- --check

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

ci: fmt-check lint test check-all doc policy-checks

zip:
	$(PYTHON) scripts/repo_zip.py $(if $(ZIP_NAME),--output "$(ZIP_NAME)")
