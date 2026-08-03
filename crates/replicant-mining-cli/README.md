# replicant-mining-cli

`replicant-mining` plans and executes restart-safe mining expansion across a
set of Replicant Space systems. For each system it selects the densest known
belt, repairs or creates one nine-device mining setup, and establishes a Cargo
Freighter ferry route back to the manufacturing hub.

The binary is an auxiliary repository tool and is not published.

## Run

```sh
export RS_API_TOKEN='your-token'

cargo run -p replicant-mining-cli -- plan \
  --hub SCEPTURUM-BELT-1 \
  --systems-file examples/mining-expansion-systems.txt

cargo run -p replicant-mining-cli -- run
cargo run -p replicant-mining-cli -- status
```

`plan` is read-only and writes `mining-expansion.json`. `run` always reconciles
that mission with managed state and durable operation evidence, then continues
the first incomplete stage. There is no separate resume command or `--execute`
flag.

## Behavior

The executor reuses idle hub stock before printing shortages, balances print
work across Autofactories, deploys as many complete sites concurrently as
available Surge Carriers permit, retroactively tags reusable automation, and
persists stage progress. The plan records enough state to continue after a
normal interruption without repeating completed work.

Systems can be passed positionally, repeated with `--system`, or read from a
whitespace/newline-separated `--systems-file`.

## Options

| Option | Meaning |
| --- | --- |
| `--system SYSTEM` | Add a target system; repeatable. |
| `--systems-file PATH` | Read target systems from a file. |
| `--replicant NAME_OR_CODE` | Acting replicant; defaults to `Chats-1`. |
| `--hub LOCATION` | Manufacturing and delivery hub; defaults to `SCEPTURUM-BELT-1`. |
| `--database PATH` | Managed SQLite database. |
| `--mission-file PATH` | Mission JSON; defaults to `mining-expansion.json`. |
| `--replace-plan` | Replace an existing incomplete mission. |
| `--wait-timeout-secs N` | Per-stage timeout; defaults to 21600. |
| `--max-concurrency N` | Simultaneous carrier deployments; defaults to 8. |
| `--verbose` / `--log-file PATH` | Enable terminal or file tracing. |
| `--json` | Emit machine-readable output. |

Configuration is also available through `RS_MINING_REPLICANT`,
`RS_MINING_HUB`, `REPLICANT_DB`, `RS_MINING_PLAN`,
`RS_MINING_WAIT_TIMEOUT_SECS`, `RS_MINING_MAX_CONCURRENCY`,
`RS_MINING_VERBOSE`, and `RS_MINING_LOG_FILE`.

## Recovery

Keep the mission JSON and SQLite database together. The mission represents
workflow progress; SQLite holds managed snapshots and durable mutation
outcomes. After a failure, fix the cause and invoke `run` again. Do not delete
the operation database merely to force a retry of an ambiguous mutation.

## Verify

```sh
cargo test -p replicant-mining-cli
cargo clippy -p replicant-mining-cli --all-targets -- -D warnings
```
