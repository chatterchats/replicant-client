# replicant-survey-cli

`replicant-survey` plans and executes a restart-safe survey route through
nearby Replicant Space systems. It discovers catalogue targets, selects a
bounded route around a centre system, transports a survey fleet, and persists
progress after each travel and survey phase.

The binary is an auxiliary repository tool and is not published.

## Run

```sh
export RS_API_TOKEN='your-token'

cargo run -p replicant-survey-cli -- plan \
  --replicant B6BA399E \
  --vessel FD5EA802 \
  --center SCEPTURUM \
  --radius 30

cargo run -p replicant-survey-cli -- run
cargo run -p replicant-survey-cli -- status
```

`plan` performs remote reads and writes only the mission file. `run` reconciles
the saved mission against managed state and continues it. `status` reads the
mission without executing gameplay mutations.

## Options

| Option | Meaning |
| --- | --- |
| `--replicant CODE` | Surveying replicant. |
| `--vessel CODE` | Vessel carrying the fleet. |
| `--center SYSTEM` | Route centre; defaults to `SCEPTURUM`. |
| `--radius LY` | Catalogue search radius; defaults to 30. |
| `--system-limit N` | Maximum route systems; defaults to 80. |
| `--star-detail-concurrency N` | Concurrent detail reads, from 1 through 16. |
| `--controller CODE` | Override the survey controller. |
| `--drones A,B,C` | Override the three survey drones. |
| `--include-explored` | Include already explored systems. |
| `--mission-file PATH` | Persisted mission JSON. |
| `--replace-plan` | Replace an existing mission during planning. |
| `--database PATH` | Managed SQLite database. |
| `--travel-timeout-secs N` | Per-travel timeout. |
| `--survey-timeout-secs N` | Per-survey timeout. |
| `--maintenance-home LOC` | System or location containing the repair fleet; defaults to the route centre. |
| `--maintenance-interval-systems N` | Proactive return interval; defaults to 40 completed route stops. |
| `--maintenance-threshold-pct N` | Return when any survey asset reaches this capacity; defaults to 25%. |
| `--maintenance-resume-pct N` | Minimum repaired capacity before resuming; defaults to 95%. |
| `--maintenance-check-secs N` | Capacity-check cadence during a long survey; defaults to 900 seconds. |
| `--verbose` / `--log-file PATH` | Enable terminal or file tracing. |

Every option has an `RS_EXPLORE_*` environment counterpart; the shared
database uses `REPLICANT_DB`. In particular, automation can set
`RS_EXPLORE_REPLICANT`, `RS_EXPLORE_VESSEL`, `RS_EXPLORE_CENTER`,
`RS_EXPLORE_RADIUS_LY`, `RS_EXPLORE_SYSTEM_LIMIT`, and `RS_EXPLORE_PLAN`.
Maintenance policy uses `RS_EXPLORE_MAINTENANCE_HOME`,
`RS_EXPLORE_MAINTENANCE_INTERVAL_SYSTEMS`,
`RS_EXPLORE_MAINTENANCE_THRESHOLD_PCT`,
`RS_EXPLORE_MAINTENANCE_RESUME_PCT`, and
`RS_EXPLORE_MAINTENANCE_CHECK_SECS`.

## Recovery and safety

The mission file records route, phase, and maintenance progress. The managed
SQLite store records state and durable operation outcomes. Run reconciliation
avoids repeating completed travel or survey work and does not blindly retry
ambiguous mutations. Keep both artifacts together for backup and recovery.

The route checks all three survey drones and their controller before departure,
after each completed stop, every 15 minutes during a long survey, and at the
survey timeout. The default 25% return threshold leaves margin above the roughly
21% point where a degraded device can stop responding. Maintenance is also
scheduled after 40 completed route stops even when capacity still looks healthy.

When maintenance is required, responsive devices are recalled first. The vessel
then visits any disabled device that cannot recall itself and stows it directly.
The complete fleet returns to the configured maintenance system, deploys beside
a functional account-owned maintenance drone, repairs to at least 95%, restores
the survey directive, restows, and resumes the same route stop. Existing version
1 and version 2 mission files migrate in place.

## Verify

```sh
cargo test -p replicant-survey-cli
cargo clippy -p replicant-survey-cli --all-targets -- -D warnings
```
