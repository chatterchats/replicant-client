# replicant-cli

`replicant-cli` is the workspace's single user-facing command for Replicant
Space automation. It combines the local client, planners, printing, and
transport packages into restart-safe workflows.

The binary is not published. Run it from the repository root.

## Start

```sh
export RS_API_TOKEN='your-token'
cargo run -p replicant-cli -- --help
cargo run -p replicant-cli -- help survey
```

For repeated use, build once and invoke the local binary:

```sh
cargo build -p replicant-cli
./target/debug/replicant-cli --help
```

## Commands

| Command | Operations | Purpose |
| --- | --- | --- |
| `print` | `queue`, `status`, `clear` | Distribute Autofactory work, inspect manufacturing, or clear factory queues. |
| `transport` | `--plan` or execute | Deliver resources and devices between locations. |
| `survey` | `plan`, `run`, `status` | Plan and execute durable survey routes. |
| `relay` | `plan`, `run`, `status` | Expand an account-owned FTL relay network. |
| `mining` | `plan`, `run`, `status` | Build repeatable mining sites and routes. |
| `event` | `list`, `plan`, `run`, `status` | Plan one or all civilisation events and execute logistics. |
| `bootstrap` | `plan`, `stage`, `run`, `status` | Stage and deploy a regional bootstrap mission. |
| `rikers` | — | Produce a read-only local colony-candidate report. |

Stateful commands accept an operation word or its flag form. For example,
`survey plan` and `survey --plan` are equivalent.

## Examples

```sh
# Manufacturing status and demand
cargo run -p replicant-cli -- print status \
  --system SCEPTURUM \
  --print 17 exotic_matter_injector \
  --tag twaffy-ring-001

# Point-to-point delivery
cargo run -p replicant-cli -- transport --plan \
  --origin SCEPTURUM-BELT-1 \
  --destination THYFFAWFF-BELT-1 \
  --resource 500 iron

# Survey planning
cargo run -p replicant-cli -- survey plan \
  --replicant B7AF4A8C \
  --vessel 6592B774 \
  --center THYFFAWFF \
  --radius 30

# Regional bootstrap continuation
cargo run -p replicant-cli -- bootstrap run \
  --mission-file regional-bootstrap-beta.json \
  --log-file logs/regional-bootstrap-beta.log
```

Use `cargo run -p replicant-cli -- COMMAND --help` before executing a workflow;
each command documents its current flags and defaults.

## State and recovery

Managed state defaults to `replicant-client.sqlite`. Long-running workflows
also write mission JSON. SQLite records normalized observations and durable
operation outcomes; mission files record workflow phases. Keep both when
backing up or moving active work.

Planning commands do not perform gameplay mutations. `run` reconciles saved
state and continues the first incomplete phase. There is no global `--execute`
or separate resume command.

Common configuration:

- `RS_API_TOKEN` — required bearer token.
- `REPLICANT_DB` — managed SQLite path.
- command-specific variables use `RS_PRINTING_*`, `RS_TRANSPORT_*`,
  `RS_EXPLORE_*`, `RS_RELAY_*`, `RS_MINING_*`, and `RS_EVENT_*` prefixes.
- `--verbose` and `--log-file PATH` enable diagnostics where supported.
- `--json` selects machine-readable output where supported.

## Safety

Mutations use the managed client's durable operation journal. An ambiguous
transport outcome is not blindly resubmitted. Re-run the workflow after fixing
a transient failure so it can reconcile server evidence and continue.

## Verify

```sh
cargo test -p replicant-cli
cargo clippy -p replicant-cli --all-targets -- -D warnings
```
