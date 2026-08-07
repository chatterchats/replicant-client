# replicant-printing-cli

`replicant-printing` distributes requested devices across live Autofactory
queues at one hub. It also expands blueprint component requirements, reports
interrupted manufacturing state, and can clear every Autofactory in a star
system.

The binary is an auxiliary repository tool and is not published.

## Queue devices

```sh
export RS_API_TOKEN='your-token'

cargo run -p replicant-printing-cli -- queue \
  --print 1 exotic_matter_injector \
  --hub SCEPTURUM-BELT-1 \
  --tag event-stock
```

The optional word `queue` may be omitted.

Blueprint `components` are expanded recursively. Existing free component stock
at the exact hub is reserved first. Before replanning, active or queued
prerequisite types already at that hub are allowed to finish and are then
counted as completed stock. This prevents a restarted command from duplicating
an interrupted prerequisite wave. Missing components are printed in leaf-first
waves, and each prerequisite wave must physically finish before the next wave
or the requested parent is queued. The final requested devices return when
their submissions are accepted rather than when they finish printing.

For an `exotic_matter_injector`, a locally available `casimir_array` can satisfy
the locked/event component requirement. Missing printable
`exotic_particle_trap` and `negative_energy_conduit` units are completed before
the injector is submitted. If a required component is absent locally and its
blueprint is not unlocked, the command fails before queueing the parent.

## Inspect interrupted printing

```sh
cargo run --quiet -p replicant-printing-cli -- status \
  --system SCEPTURUM \
  --print 5 exotic_matter_injector \
  --tag event-stock \
  --log-file logs/replicant-printing.log
```

`status` is read-only. It refreshes every account-owned device in the selected
system and reads every Autofactory's active print and queue. With one or more
`--print` targets it reports completed outputs, active outputs, queued outputs,
the quantity still missing, and any surplus beyond the requirement. Recursive
prerequisites are calculated only for those missing outputs, so already
completed or in-flight parent devices are not double-counted.

`--tag` is optional. When present, only completed devices and print jobs carrying
every supplied tag are used by the gap calculation. Autofactory work outside the
filter is still displayed and marked `outside filter`. Without `--print`, the
command lists system device counts by type/status plus all Autofactory work.

The `Still needs queueing` lines can be passed back to `queue`; the normal queue
command will reconstruct and print any missing prerequisite components.

## Clear a system

```sh
cargo run -p replicant-printing-cli -- clear \
  --system SCEPTURUM \
  --log-file logs/printing-clear.log
```

`clear` discovers every account-owned Autofactory anywhere in the selected
system. For each factory, in stable device-code order, it clears any queued work,
waits for that state to settle, and submits `deactivate` only if an active print
still remains. Completion requires no queued or active print; an idle factory
is valid and remains ready for new work. A planet, belt, or Lagrange-point
designation can be supplied to `--system`; it is normalized to the containing
star. `--hub` is
also accepted and derives the clear system from that location.

## Options

| Option | Command | Meaning |
| --- | --- | --- |
| `--print N DEVICE_TYPE` | queue, status | Queue a quantity, or compare live state with a desired quantity. Repeatable; required only by queue. |
| `--hub LOCATION` | all | Queue hub, or location used to derive the clear/status system. |
| `--system SYSTEM` | clear, status | Star system to clear or inspect. |
| `--tag TAG` | queue, status | Apply a print tag, or require that tag during status gap calculations. Repeatable. |
| `--flatpack` | queue | Request compacted output for final modular devices. Prerequisites remain assembled for consumption. |
| `--database PATH` | all | Managed SQLite database. |
| `--wait-timeout-secs N` | queue, clear | Queue, prerequisite-completion, or clear timeout; defaults to 21600. |
| `--poll-seconds N` | queue, clear | State polling interval; defaults to 5. |
| `--verbose` / `--log-file PATH` | all | Enable terminal or file tracing. |
| `--json` | all | Emit the command report as JSON. |

Environment configuration includes `RS_PRINTING_HUB`, `RS_PRINTING_SYSTEM`,
`REPLICANT_DB`, `RS_PRINTING_FLATPACK`, `RS_PRINTING_WAIT_TIMEOUT_SECS`,
`RS_PRINTING_POLL_SECONDS`, `RS_PRINTING_VERBOSE`, and
`RS_PRINTING_LOG_FILE`.

## Failure behavior

Invalid quantities and unknown options fail before connecting. Missing parent
blueprints, unavailable component blueprints after local-stock subtraction,
component cycles, unavailable factories, capacity/completion timeouts,
rejected operations, and ambiguous outcomes return errors without inventing
success. Accepted operation IDs are included in the final reports for audit
and recovery.

## Verify

```sh
cargo test -p replicant-printing --all-features
cargo test -p replicant-printing-cli
cargo clippy -p replicant-printing -p replicant-printing-cli \
  --all-targets --all-features -- -D warnings
```
