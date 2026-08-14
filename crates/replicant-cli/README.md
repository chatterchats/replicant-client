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
| `interactive` | — | Guided builder for every CLI workflow, with smart SYSTEM/LOCATION lookup. |
| `daemon` | — | Show local `replicantd` health. |
| `operation` | `catalogue`, `report`, `action` | Discover and run registered capabilities through `replicantd`. |
| `workflow` | `list`, `inspect`, `start`, `pause`, `resume`, `cancel` | Control durable workflows owned by `replicantd`. |
| `print` | `queue`, `status`, `clear` | Distribute Autofactory work, inspect manufacturing, or clear factory queues. |
| `transport` | `--plan` or execute | Deliver resources and devices between locations. |
| `survey` | `plan`, `run`, `status` | Plan and execute durable survey routes. |
| `relay` | `plan`, `run`, `status` | Expand an account-owned FTL relay network. |
| `mining` | `plan`, `run`, `status` | Build repeatable mining sites and routes. |
| `ownership` | `reassign` | Preview or bulk-reassign non-vessel devices by catalogue region. |
| `observatory` | `status`, `prospect`, `triangulate` | Automate Galactic Observatory fringe prospecting and spectral triangulation. |
| `event` | `list`, `plan`, `run`, `status` | Plan one or all civilisation events and execute logistics. |
| `bootstrap` | `plan`, `stage`, `deliver`, `run`, `status` | Stage, deliver, or fully deploy a regional bootstrap mission. |
| `rikers` | — | Produce a read-only local colony-candidate report. |

Stateful commands accept an operation word or its flag form. For example,
`survey plan` and `survey --plan` are equivalent.

Survey `plan`/`run` and Relay `run` submit a durable workflow to
`replicantd` and return immediately; exiting the CLI does not stop it. Use
`--direct` only for deliberate standalone diagnostics or compatibility.
`REPLICANTD_URL` defaults to `http://127.0.0.1:8080`.

```sh
cargo run -p replicant-server --bin replicantd
cargo run -p replicant-cli -- daemon
cargo run -p replicant-cli -- workflow list
cargo run -p replicant-cli -- workflow inspect WORKFLOW_ID
cargo run -p replicant-cli -- workflow pause WORKFLOW_ID
cargo run -p replicant-cli -- workflow resume WORKFLOW_ID
cargo run -p replicant-cli -- workflow cancel WORKFLOW_ID
```

Generic workflow submission accepts typed `NAME=VALUE` parameters. JSON
scalars, arrays, and objects retain their types; other values are strings:

```sh
cargo run -p replicant-cli -- workflow start relay.expansion \
  replicant=Chats-1 hub=SCEPTURUM-BELT-1 targets_csv=THYFFAWFF \
  mission_file=ftl-relay-expansion.json
```

Registered reports and finite actions use the same typed catalogue and accept
familiar legacy example names as aliases:

```sh
cargo run -p replicant-cli -- operation catalogue
cargo run -p replicant-cli -- operation report nearby_belt_report \
  origin=SCEPTURUM radius_ly=25
cargo run -p replicant-cli -- operation action clear_tags \
  tag_prefix=evt- dry_run=true
```

Use `dry_run=true` before reviewing a mutating action.

### Interactive command builder

Run the menu with:

```sh
cargo run -p replicant-cli -- interactive
```

You can also jump directly to a command or operation:

```sh
cargo run -p replicant-cli -- interactive relay plan
cargo run -p replicant-cli -- interactive observatory triangulate
```

The builder selects the ordinary CLI command/operation, lets you add its options,
previews the resulting command line, and then dispatches through the same handler
as non-interactive use. SYSTEM and LOCATION values use the live star catalogue and
location map for exact, prefix, substring, and typo-tolerant suggestions. For
example, entering `SCEPT` for a SYSTEM offers `SCEPTURUM`; LOCATION prompts also
include concrete matching locations. If catalogue lookup is unavailable, manual
entry remains available.

## Examples

```sh
# Manufacturing status and demand
cargo run -p replicant-cli -- print status \
  --system SCEPTURUM \
  --print 17 exotic_matter_injector \
  --tag twaffy-ring-001

# Clear system-wide Autofactory queues and stop active prints, except for the
# active job currently running on FF259175.
cargo run -p replicant-cli -- print clear \
  --system SCEPTURUM \
  --exclude-active FF259175

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

# Preview every non-replicant-vessel device in the established regions that
# is not already assigned to Chats-1. `solregion` aliases catalogue `solzone`.
cargo run -p replicant-cli -- ownership reassign \
  --region solregion,alpha,beta,gamma

# Once a new region is known, select every named region except that one.
# Add --execute only after reviewing the preview.
cargo run -p replicant-cli -- ownership reassign \
  --all-regions \
  --ignore-region delta \
  --owner Chats-1 \
  --execute

# Automatic fringe prospecting. The CLI scores sparse hemispheres locally and
# reports the server's fringe diagnostics if a direction is blocked.
cargo run -p replicant-cli -- observatory prospect

# Explicit prospect directions are also available.
cargo run -p replicant-cli -- observatory prospect \
  --direction toward-star --star SCEPTURUM

# Current ring-event triangulation. With --all, targets are spread over a
# deterministic deep-space sphere seeded by the selected observatories,
# instead of clustering on one coordinate.
cargo run -p replicant-cli -- observatory triangulate --all

# Plan a bootstrap directly from a landing star; the region is inferred.
cargo run -p replicant-cli -- bootstrap plan \
  --landing-star LUMBUNGA \
  --mission-file bootstrap-lumbunga.json

# Manufacture/load the ark and send only its devices to the landing entry.
# If the mission file does not exist yet, `deliver` creates it first.
cargo run -p replicant-cli -- bootstrap deliver \
  --landing-star LUMBUNGA \
  --mission-file bootstrap-lumbunga.json \
  --log-file logs/bootstrap-lumbunga.log

# Later, the same durable mission can continue into the full regional workflow.
cargo run -p replicant-cli -- bootstrap run \
  --mission-file bootstrap-lumbunga.json \
  --log-file logs/bootstrap-lumbunga.log
```

Use `cargo run -p replicant-cli -- COMMAND --help` before executing a workflow;
each command documents its current flags and defaults.

## State and recovery

`replicantd` owns the normal long-running managed client and durable workflow
supervisor. Managed state defaults to `replicant-client.sqlite`; the runtime
database and mission files must also be retained when backing up active work.

Planning commands do not perform gameplay mutations. `deliver` reconciles a saved
bootstrap mission through arrival at its landing entry and stops before regional
deployment; `run` continues the first incomplete regional phase. Bootstrap derives
its Surge Carrier requirement from the payload: each mining setup stays together,
expansion relays and beacons use dedicated carrier groups, and the remaining ark
payload is packed into additional Surge Carriers. Idle source-hub Surge Carriers
are borrowed first; equivalent replacement prints are queued after loading.
There is no global `--execute` or separate resume command.

Common configuration:

- `RS_API_TOKEN` — required bearer token.
- `REPLICANT_DB` — managed SQLite path.
- command-specific variables use `RS_PRINTING_*`, `RS_TRANSPORT_*`,
  `RS_EXPLORE_*`, `RS_RELAY_*`, `RS_MINING_*`, and `RS_EVENT_*` prefixes.
- `RS_OWNERSHIP_TARGET`, `RS_OWNERSHIP_REGIONS`, `RS_OWNERSHIP_ALL_REGIONS`, and
  `RS_OWNERSHIP_IGNORE_REGIONS` configure the regional ownership utility.
- `RS_OBSERVATORY_SIGNATURE` overrides the default spectral signature used by
  `observatory triangulate` (currently `934d3ac4dcc918ad`).
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
