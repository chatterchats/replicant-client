# Contributing

`replicant-client` is a single root package (edition 2024, no workspace).
Module boundaries are enforced by Cargo features, not by separate crates:

| Feature | Implies | Owns |
| --- | --- | --- |
| `raw` | — | HTTP transport, authentication, request/response models, pagination, rate-limit metadata. |
| `events` | `raw` | SSE framing and raw event streaming. |
| `managed` (default) | `events` | SQLite store, state engine, synchronization, durable operations, and the managed `Client`. |

Do not add a dependency to a lower tier merely because a higher tier already
uses it. Mark feature-specific dependencies `optional = true` and attach
them to the feature that needs them.

## Contract authority

The verified Replicant Space 2.3.3 OpenAPI corpus under
[`reference/replicant-space/`](reference/replicant-space/) is the contract.
Rendered documentation deprecation asides override missing OpenAPI
`deprecated` flags — see `policy/contract-metadata.json`.

Whenever a change affects which operations, fields, or aliases the client
exposes, update the relevant policy file under `policy/` and re-run:

```sh
python3 scripts/generate_operation_inventory.py
python3 scripts/generate_authority_matrix.py
python3 scripts/contract_policy_check.py
```

Do not weaken `scripts/contract_policy_check.py` to make a change pass. Fix
the implementation, or update the policy file with an accurate reason and
evidence citation.

## Tests and checks

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo check --no-default-features --features raw
cargo check --no-default-features --features events
cargo check --all-features
make contract-policy-check
```

## Pull requests and scope

- Keep changes focused on one phase of `docs/implementation/rewrite-guide.md`.
- Do not weaken lint, test, or contract-policy gates.
- Do not add production `todo!`, `unimplemented!`, unjustified `panic!`, or
  casual `unwrap`/`expect`.
- Never commit tokens, authorization headers, private message bodies, or
  databases containing user data.
- Do not expose deprecated or admin-only Replicant Space operations, even
  through `raw`.

See [SECURITY.md](SECURITY.md) for vulnerability reporting.
