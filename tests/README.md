# Integration tests

```sh
cargo test --all-features          # or: cargo t
```

**No test may require a live Replicant Space account.** HTTP is faked with
`wiremock`; state is built from fixtures. `make ci` runs these and must stay
green offline.

## Layout

| File | Covers |
| --- | --- |
| `contract_2_3_3.rs`, `contract_2_4_0.rs`, `contract_2_5_0.rs`, `contract_2_5_1.rs` | Per-release contract fixtures. Each pins the behaviour its version introduced. |
| `raw_transport.rs` | Raw HTTP surface: request shape, query params, pagination, rate-limit metadata, retry policy. |
| `events.rs` | SSE envelope parsing and typed payload decoding. |
| `managed_reads.rs` | Managed gateway reads normalize, commit, and publish before returning. |
| `domain_authority.rs` | Authority rules — which layer's data wins on conflict. |
| `device_relationships.rs` | Owner/operator relationship semantics across migration 0002. |
| `package_identity.rs` | Package metadata and contract-pin consistency. |
| `fixtures/` | Shared JSON fixtures. |

## Contract test convention

Each `contract_<version>.rs` is **additive and permanent**. Do not edit an
older file when a new version changes behaviour — add a case to the new
version's file so the older assertion keeps proving the client still handles
the older wire shape.

New contract behaviour goes in the file matching the version that introduced
it. Discovered bugs get a regression test in the topical file, not in a
contract file.

## Writing one

`contract_2_5_1.rs` is the current template. It provides two local helpers
worth reusing: a `client(&MockServer)` builder that disables retries so a
failure surfaces immediately, and an `event(name, payload)` constructor that
wraps a payload in a complete SSE envelope.

Prefer fixture- and state-based assertions over timing. Avoid sleeps; where a
test must observe async progress, drive it deterministically with
`tokio`'s `test-util` time control.
