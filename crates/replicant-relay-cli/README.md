# replicant-relay-cli

`replicant-relay` plans and executes a restart-safe Replicant Space FTL relay
expansion mission. It uses `replicant-route-planner` to minimize new relays,
then reuses or activates owned relays, manufactures shortages, deploys and
verifies the network, and returns the selected replicant to its hub.

The binary is an auxiliary repository tool and is not published.

## Run

```sh
export RS_API_TOKEN='your-token'

cargo run -p replicant-relay-cli -- plan \
  --replicant Chats-1 \
  --hub SCEPTURUM-BELT-1 \
  WIHAX ILPHARD KRAKHUX

cargo run -p replicant-relay-cli -- run
cargo run -p replicant-relay-cli -- status
```

Targets are system designations, not planet or belt locations. `plan` is
read-only. `run` reconciles and continues the persisted mission, so there is no
separate resume command or `--execute` confirmation.

## Options

| Option | Meaning |
| --- | --- |
| `--replace-plan` | Replace the saved mission. |
| `--replicant NAME_OR_CODE` | Transport replicant; defaults to `Chats-1`. |
| `--hub LOCATION` | Manufacturing hub; defaults to `SCEPTURUM-BELT-1`. |
| `--plan PATH` | Saved mission JSON. |
| `--database PATH` | Managed SQLite database. |
| `--max-hop LY` | Uniform relay range; defaults to 7.499. |
| `--wait-timeout-secs N` | Per-phase timeout. |
| `--verbose` / `--log-file PATH` | Enable terminal or file tracing. |

Environment equivalents include `REPLICANT_DB`, `RS_RELAY_REPLICANT`,
`RS_RELAY_HUB`, `RS_RELAY_PLAN`, `RS_RELAY_REPLACE_PLAN`,
`RS_RELAY_WAIT_TIMEOUT_SECS`, `RS_RELAY_VERBOSE`, and `RS_RELAY_LOG_FILE`.

## Recovery

The plan file tracks mission phases; SQLite tracks committed observations and
durable mutation outcomes. `run` checks both before acting. Keep the files
together and rerun the command after resolving transient errors. The executor
does not blindly resend an ambiguous deployment or activation.

## Verify

```sh
cargo test -p replicant-relay-cli
cargo clippy -p replicant-relay-cli --all-targets -- -D warnings
```
