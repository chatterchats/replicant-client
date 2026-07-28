# Replicant Space 2.3.3 release audit

Audit date: 2026-07-28

## Scope and baseline

The requested root `00-README.md` is not present. The closest checked-in
phase-00 validator is
`docs/implementation/phase-11.6/00-baseline-and-validator.md`; its command
set was used, augmented with the requested feature matrix, package audit, and
2.3.3 behavioral regressions.

The pre-update phase-00 baseline passed every policy gate against the
2.3.1 OpenAPI corpus: 84 operations (77 supported, five deprecated, two
admin), checksum
`ca018a938541f23c4838e8fe58f78889d9ca4b9ab81b488112f90589dd83c2f4`.

## Commands and results

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | passed |
| `cargo clippy --all-targets --all-features -- --deny warnings` | passed |
| `RUSTDOCFLAGS='-D warnings' cargo doc --all-features --no-deps` | passed |
| `cargo test` | passed; 154 unit tests plus integration tests and doctests |
| `cargo test --no-default-features --features raw` | passed |
| `cargo test --no-default-features --features events` | passed |
| `cargo test --all-features` | passed; 154 unit tests plus integration tests and doctests |
| `cargo check` | passed |
| `cargo check --no-default-features --features raw` | passed |
| `cargo check --no-default-features --features events` | passed |
| `cargo check --all-features` | passed |
| `python3 scripts/contract_policy_check.py` | passed |
| `python3 scripts/forward_compatibility_policy_check.py` | passed |
| `python3 scripts/raw_transport_policy_check.py` | passed; 79 callable methods |
| `python3 scripts/schema_policy_check.py` | passed |
| `python3 scripts/authority_matrix_check.py` | passed; 79 supported operations covered |
| `python3 scripts/phase_11_5_remediation_check.py` and `--self-test` | passed; no remaining release blockers reported |
| `make remediation-policy-check` | passed |
| `git diff --check` | passed |
| `cargo package --list --allow-dirty` | passed; 82 paths; both contract-evidence documents present; no database, log, token/secret/credential, `.env`, or `explore-survey-route.json` path |

The first audit pass found and corrected two release regressions:

- `explore_survey_route` logged removed survey-summary fields and therefore
  failed all-target Clippy compilation. Its log now uses the current aggregate
  planet/moon fields.
- The strengthened old-crate-name policy failed on five documentation
  references outside the allowed historical section. Those references now say
  “predecessor crate”; the policy gate passes.

Raw-only and events-only tests also initially compiled three managed examples.
`game_concepts`, `initialize_colony_database`, and
`rikers_colony_candidates` now use the existing
`required-features = ["managed"]` manifest pattern.

## Release-specific behavior

| Requirement | Evidence | Result |
| --- | --- | --- |
| Integer ETA and print-time JSON remains source-compatible | `refreshed_openapi_dtos_decode_changed_fields_and_ignore_unknowns` and `integer_blueprint_print_time_remains_source_compatible` passed; public source types remain `Option<f64>` | passed |
| Colony leaderboards are authenticated safe reads | `colony_leaderboards_use_the_openapi_paths` now requires `GET`, the exact route, and `Authorization: Bearer test-token` for both `colony_moon` and `colony_planet` | passed |
| Filtered device lists cannot tombstone omitted devices | `device_lists_are_full_entities_but_only_full_unfiltered_traversals_reconcile` and `visibility_scoped_collections_never_tombstone` passed | passed |
| Direct and digest scan delivery persist equivalent knowledge | `inactive_direct_scans_and_active_digest_replay_to_the_same_state` and `scan_reports_project_direct_and_digest_delivery_modes` passed | passed |
| Unsafe mutations register before transmit and never retry ambiguity | `failed_submission_claim_never_transmits_the_request` and `unsafe_timeout_is_ambiguous_and_never_retried` passed | passed |

## Contract provenance and checksums

| Artifact | Current value |
| --- | --- |
| Checked-in OpenAPI | 2.3.3, 86 operations, SHA-256 `d6f89cbadc523160d25e26cec8ac9673fda7296512ea408c5dd7c55a13c08c3f` |
| Supplied rendered-document archive | 2.3.3, SHA-256 `e271b1a32602dd9ec80da6b9d64f392efc58a574f8da7584de0ae9205acabc73` |
| Pre-update OpenAPI baseline | 2.3.1, 84 operations, SHA-256 `ca018a938541f23c4838e8fe58f78889d9ca4b9ab81b488112f90589dd83c2f4` |
| Current `policy/documented-operation-deltas.json` | declares 2.3.3 as both baseline and documentation version, with no operations |

This is the material provenance problem: the project guide requires the
checked-in 2.3.1 OpenAPI file to remain the machine-readable baseline and
requires 2.3.2/2.3.3 rendered-document differences to be explicit deltas.
The current metadata and `docs/contract/openapi-2.3.3-refresh.md` instead
state that 2.3.3 replaces the baseline. The policy banner is inconsistent as
well: it reports “84 OpenAPI operations plus 2 documented leaderboard deltas”
while the checked-in OpenAPI metadata reports 86 operations and the delta file
is empty. The gates passing does not establish the required provenance model.

## Public API and semver review

No existing public enum declaration or struct-style enum variant changed in
the source diff. The new AMI digest/event payload structs are
`#[non_exhaustive]`, so their additive fields do not require external struct
literals and are semver-safe additions.

The following existing public, exhaustively constructible structs or methods
are source-breaking if version 1.0.0 has already been released:

- `DeviceRelationships::hosted_by` was replaced with
  `assigned_replicant`, and `hosting_replicant` was added.
- `DeviceQuery::hosted_by` was replaced with `assigned_to`; callers must also
  use `hosting_replicant` when they mean a physically hosted matrix.
- `raw::devices::DeviceListQuery` gained `tag` and `untagged` fields.
- Normalized `Star` and `StarKnowledge` gained `has_hub` and `region` fields.

`LeaderboardEntry::designation` is additive on an already non-exhaustive raw
DTO. Integer wire values for existing `f64` ETA and print-time fields are
wire-compatible and do not change callers' source types.

## Migration notes

- Replace relationship uses of `hosted_by` with `assigned_replicant` or
  `hosting_replicant` according to the intended meaning.
- Replace `DeviceQuery::hosted_by(...)` with `assigned_to(...)`.
- Prefer `..Default::default()` for `DeviceListQuery`; direct struct literals
  must add `tag` and `untagged`.
- Direct `Star` and `StarKnowledge` literals must add `has_hub` and `region`.
- No migration is needed for integer ETA or print-time payloads: Serde accepts
  them into the preserved `f64` fields.

## Remaining limitations

Non-blocking follow-up work:

- `cargo package` warns that the manifest lacks `documentation`, `homepage`,
  and `repository` metadata.
- Raw-only and events-only test invocations emit a warning for the cfg-gated
  `tests/domain_authority.rs` crate's missing crate documentation. The tests
  pass and all-feature Clippy is clean.

## Recommendation: NO-GO

Release blockers:

1. Restore and record the required provenance model: retain the verified
   2.3.1 OpenAPI file as the baseline, represent 2.3.2/2.3.3 changes in the
   rendered-document delta policy, and make metadata, inventory, and the
   policy banner agree.
2. If `replicant-client` 1.0.0 is already published, either restore source
   compatibility for the public struct/query changes or release them in a
   major version with the migration notes above. If 1.0.0 is not published,
   record that fact in the release decision before tagging it.

All executable quality, feature, package, authorization, persistence, and
wire-compatibility checks pass after the fixes above. The manifest metadata
warning and cfg-gated test warning are non-blocking follow-up work.
