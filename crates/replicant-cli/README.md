# replicant-cli

`replicant-cli` is the single user-facing command crate for the Replicant Space
workspace. It consolidates the former printing, transport, survey, relay,
mining, event, bootstrap, and Riker binaries while keeping their planner and
reusable workflow crates separate.

## Commands

```text
replicant-cli print       Distributed printing, status, and queue clearing
replicant-cli transport   Point-to-point resource/device delivery
replicant-cli survey      Survey-route planning and execution
replicant-cli relay       FTL relay-network expansion
replicant-cli mining      Mining-network expansion
replicant-cli event       Civilisation-event automation
replicant-cli bootstrap   Regional bootstrap automation
replicant-cli rikers      Local Riker colony-candidate reporting
```

Stateful commands accept either the old operation word or a flag-style
operation. For example, these are equivalent:

```sh
cargo run -p replicant-cli -- survey plan --center SCEPTURUM --radius 30
cargo run -p replicant-cli -- survey --plan --center SCEPTURUM --radius 30
```

The flag form is preferred for new examples.

## Examples

Printing status against a desired tagged quantity:

```sh
cargo run -p replicant-cli -- print \
  --status \
  --system SCEPTURUM \
  --print 17 exotic_matter_injector \
  --tag twaffy-ring-001
```

Plan a survey route:

```sh
cargo run -p replicant-cli -- survey \
  --plan \
  --replicant B7AF4A8C \
  --vessel 6592B774 \
  --center THYFFAWFF \
  --radius 30
```

Continue a regional bootstrap mission:

```sh
cargo run -p replicant-cli -- bootstrap \
  --run \
  --mission-file regional-bootstrap-beta.json \
  --log-file logs/regional-bootstrap-beta.log
```

Transport every matching tagged device:

```sh
cargo run -p replicant-cli -- transport \
  --origin SCEPTURUM \
  --device-tag twaffy-obj-1 \
  --carrier 1 mobile_fleet \
  --destination TWAFFY-OBJ-1
```

Use `cargo run -p replicant-cli -- help COMMAND` or
`cargo run -p replicant-cli -- COMMAND --help` for command-specific options.

## Migration from the old CLI crates

| Old package / binary | Unified command |
| --- | --- |
| `replicant-printing-cli` / `replicant-printing` | `replicant-cli print` |
| `replicant-transport-cli` / `replicant-transport` | `replicant-cli transport` |
| `replicant-survey-cli` / `replicant-survey` | `replicant-cli survey` |
| `replicant-relay-cli` / `replicant-relay` | `replicant-cli relay` |
| `replicant-mining-cli` / `replicant-mining` | `replicant-cli mining` |
| `replicant-event-cli` / `replicant-events` | `replicant-cli event` |
| `replicant-bootstrap-cli` / `replicant-bootstrap` | `replicant-cli bootstrap` |
| `replicant-rikers-cli` / `replicant-rikers` | `replicant-cli rikers` |

The reusable non-CLI crates such as `replicant-printing`,
`replicant-transport`, and the planner crates remain independent libraries.
