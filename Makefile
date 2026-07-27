SHELL := /bin/sh
CARGO ?= cargo
PYTHON ?= python3

.PHONY: help fmt fmt-check lint test doc check check-raw check-events check-all-features feature-checks contract-policy-check observability-policy-check policy-checks remediation-policy-check ci

help:
	@printf '%s\n' \
	  'replicant-client' \
	  '' \
	  'Usage: make <target>' \
	  '' \
	  'fmt                    Format all Rust sources' \
	  'fmt-check              Verify formatting without modifying files' \
	  'lint                   Run Clippy with warnings denied' \
	  'test                   Run tests with all features enabled' \
	  'doc                    Build docs with warnings denied' \
	  'feature-checks         cargo check across the supported feature combinations' \
	  'contract-policy-check  Verify the Replicant Space 2.3.1 operation inventory and exclusions' \
	  'observability-policy-check Verify tracing targets, timing events, and secret guards' \
	  'policy-checks          Run all checked-in policy gates' \
	  'remediation-policy-check Verify the Phase 11.5 remediation ledger' \
	  'ci                     Run the full local CI-equivalent suite'

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

lint:
	$(CARGO) clippy --all-targets --all-features -- -D warnings

test:
	$(CARGO) test --all-features

doc:
	RUSTDOCFLAGS="-D warnings" $(CARGO) doc --all-features --no-deps

check:
	$(CARGO) check

check-raw:
	$(CARGO) check --no-default-features --features raw

check-events:
	$(CARGO) check --no-default-features --features events

check-all-features:
	$(CARGO) check --all-features

feature-checks: check check-raw check-events check-all-features

contract-policy-check:
	$(PYTHON) scripts/contract_policy_check.py

observability-policy-check:
	$(PYTHON) scripts/observability_policy_check.py

remediation-policy-check:
	$(PYTHON) scripts/phase_11_5_remediation_check.py
	$(PYTHON) scripts/phase_11_5_remediation_check.py --self-test

policy-checks: contract-policy-check
	$(PYTHON) scripts/forward_compatibility_policy_check.py
	$(PYTHON) scripts/raw_transport_policy_check.py
	$(PYTHON) scripts/schema_policy_check.py
	$(PYTHON) scripts/authority_matrix_check.py
	$(MAKE) observability-policy-check
	$(MAKE) remediation-policy-check

ci: fmt-check lint test feature-checks doc policy-checks
