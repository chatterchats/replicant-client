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

To plan and work through every active discovered event:

```sh
cargo run --quiet -p replicant-event-cli -- plan --all
cargo run --quiet -p replicant-event-cli -- run
```

Discovery can be constrained to a catalogue region:

```sh
cargo run --quiet -p replicant-event-cli -- list --region alpha
cargo run --quiet -p replicant-event-cli -- plan --all --region alpha
```

For an operating area that does not align exactly with a catalogue region,
use a centre and light-year radius. The centre accepts a star, system, planet,
belt, or Lagrange-point designation and is normalized to its star system:

```sh
cargo run --quiet -p replicant-event-cli -- list \
  --center SCEPTURUM \
  --radius 35

cargo run --quiet -p replicant-event-cli -- plan --all \
  --replicant Chats-1 \
  --home SCEPTURUM-BELT-1 \
  --radius 35
```

When `--radius` is supplied without `--center`, the centre defaults to the
`--home` system. The selected scope is saved in both single-event missions and
all-events campaigns, so later `run` commands and blocked-event replanning use
the same boundary.

An all-events campaign automatically selects the feasible completion option
with the most planner recommendation badges. Ties prefer fewer prints, a
shorter print schedule, fewer trips, and finally the stable criterion name.

## Workflow

1. `list` shows available civilisation events.
2. `plan [EVENT]` selects a criterion, inventories progress and destination
   stock, plans manufacturing and transport, and writes `event-mission.json`.
3. `run` reconciles durable operations and continues the first incomplete
   phase. Run the same command again after an interruption or transient error.
4. `status` reads the mission file without connecting to the API.

For an all-events campaign, planning reserves home resources and consumable
device stock across the entire campaign. Execution fills available
Autofactory slots round-robin, then completes events requiring no printed
devices while manufacturing continues. Device-dependent events follow in
projected print-ready order. While a material-only event is in flight, a
background feeder continues filling newly opened Autofactory slots. Travel and
event resolution remain serialized so missions do not fight over the selected
replicant or reusable transports. Within each mission, the replicant departs as
soon as the outbound phase begins, in parallel with Cargo Freighter and Surge
Carrier delivery work; resolution waits for arrival rather than starting a
second outbound leg after staging.

The mission owns claimed devices and execution progress. A pre-existing active
plan is preserved unless `--replace-plan` is supplied.

## Options

| Option | Meaning |
| --- | --- |
| `--event DESIGNATION` | Event to plan. |
| `--criterion NAME` | Completion option to select. |
| `--all` | Plan every active discovered event and choose criteria automatically. |
| `--region REGION` | Include only events whose star belongs to the catalogue region. |
| `--center LOCATION` | Centre for radius filtering; requires `--radius`. |
| `--radius LY` | Include events within this distance of `--center`, or `--home` when no centre is supplied. |
| `--replicant NAME_OR_CODE` | Acting replicant; defaults to `Chats-1`. |
| `--home LOCATION` | Manufacturing hub; defaults to `SCEPTURUM-BELT-1`. |
| `--database PATH` | Managed SQLite database. |
| `--plan-file PATH` | Mission JSON; defaults to `event-mission.json`. |
| `--replace-plan` | Replace an existing active plan. |
| `--wait-timeout-secs N` | Per-phase timeout; defaults to 21600. |
| `--verbose` / `--log-file PATH` | Enable terminal or file tracing. |
| `--json` | Emit machine-readable output. |

Environment equivalents include `RS_EVENT_REPLICANT`, `RS_EVENT_HOME`,
`RS_EVENT_REGION`, `RS_EVENT_CENTER`, `RS_EVENT_RADIUS_LY`, `REPLICANT_DB`,
`RS_EVENT_PLAN`, `RS_EVENT_WAIT_TIMEOUT_SECS`, `RS_EVENT_VERBOSE`, and
`RS_EVENT_LOG_FILE`.
Command-line values take precedence.

## Safety and recovery

Every mutation goes through the managed client's durable operation journal.
The mission file records higher-level phase progress; the SQLite database
records submission outcomes. Keep both files together when moving or backing
up a mission, and do not edit them while the command is running.

An all-events campaign also creates a sibling campaign directory containing
one durable mission JSON per event. Keep that directory with the main campaign
file. Events that cannot yet be funded remain listed as blocked; after the
currently feasible events finish, `run` replans those events against newly
returned rewards and live inventory.

An ambiguous operation is reconciled from later evidence instead of being
blindly resubmitted. Re-run `run` after fixing transient failures. Only one
executor may use a mission file at a time; `run` owns a sibling `.lock` file
for the process lifetime and recovers a stale lock after an interrupted process.

## Verify

```sh
cargo test -p replicant-event-cli
cargo clippy -p replicant-event-cli --all-targets -- -D warnings
```
