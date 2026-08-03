# replicant-event-cli

`replicant-events` plans and executes restart-safe Replicant Space civilisation
event logistics. It combines `replicant-event-planner` with the managed client,
distributed printing, durable operations, and a persisted mission file.

The binary is an auxiliary repository tool and is not published.

## Run

```sh
export RS_API_TOKEN='your-token'

cargo run --quiet -p replicant-event-cli -- list
cargo run --quiet -p replicant-event-cli -- plan WIXUKHHU-4-EVT-002
cargo run --quiet -p replicant-event-cli -- run
cargo run --quiet -p replicant-event-cli -- status
```

Running without a subcommand starts the interactive flow. `list` and `plan`
perform no gameplay mutations. `run` loads the persisted mission, reconciles
durable operations and live state, and continues the first incomplete phase.

## Workflow

1. `list` shows available civilisation events.
2. `plan [EVENT]` selects a criterion, inventories progress and destination
   stock, plans manufacturing and transport, and writes `event-mission.json`.
3. `run` reconciles durable operations and continues the first incomplete
   phase. Run the same command again after an interruption or transient error.
4. `status` reads the mission file without connecting to the API.

The mission owns claimed devices and execution progress. A pre-existing active
plan is preserved unless `--replace-plan` is supplied.

## Options

| Option | Meaning |
| --- | --- |
| `--event DESIGNATION` | Event to plan. |
| `--criterion NAME` | Completion option to select. |
| `--replicant NAME_OR_CODE` | Acting replicant; defaults to `Chats-1`. |
| `--home LOCATION` | Manufacturing hub; defaults to `SCEPTURUM-BELT-1`. |
| `--database PATH` | Managed SQLite database. |
| `--plan-file PATH` | Mission JSON; defaults to `event-mission.json`. |
| `--replace-plan` | Replace an existing active plan. |
| `--wait-timeout-secs N` | Per-phase timeout; defaults to 21600. |
| `--verbose` / `--log-file PATH` | Enable terminal or file tracing. |
| `--json` | Emit machine-readable output. |

Environment equivalents include `RS_EVENT_REPLICANT`, `RS_EVENT_HOME`,
`REPLICANT_DB`, `RS_EVENT_PLAN`, `RS_EVENT_WAIT_TIMEOUT_SECS`,
`RS_EVENT_VERBOSE`, and `RS_EVENT_LOG_FILE`.
Command-line values take precedence.

## Safety and recovery

Every mutation goes through the managed client's durable operation journal.
The mission file records higher-level phase progress; the SQLite database
records submission outcomes. Keep both files together when moving or backing
up a mission, and do not edit them while the command is running.

An ambiguous operation is reconciled from later evidence instead of being
blindly resubmitted. Re-run `run` after fixing transient failures. Only one
executor may use a mission file at a time; `run` owns a sibling `.lock` file
for the process lifetime and recovers a stale lock after an interrupted process.

## Verify

```sh
cargo test -p replicant-event-cli
cargo clippy -p replicant-event-cli --all-targets -- -D warnings
```
