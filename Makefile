SHELL := /bin/sh
CARGO ?= cargo
PYTHON ?= python3

.PHONY: help fmt fmt-check lint test doc check check-raw check-events check-all-features feature-checks contract-policy-check ci

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

ci: fmt-check lint test feature-checks doc contract-policy-check
