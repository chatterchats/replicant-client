# replicant-printing-cli

`replicant-printing` distributes requested devices across live Autofactory
queues at one hub. It refreshes capacity, submits one device per available
queue slot through durable managed operations, and waits for more capacity
until every requested unit is queued.

The binary is an auxiliary repository tool and is not published.

## Run

```sh
export RS_API_TOKEN='your-token'

cargo run -p replicant-printing-cli -- \
  --print 6 "Mining Drone" \
  --print 1 "Mining Controller" \
  --hub SCEPTURUM-BELT-1 \
  --tag expansion-01
```

The optional word `queue` is accepted before the options. The command returns
when all submissions are accepted, not when the devices finish printing.

## Options

| Option | Meaning |
| --- | --- |
| `--print N DEVICE_TYPE` | Queue a quantity; repeatable and required. |
| `--hub LOCATION` | Autofactory location; defaults to `SCEPTURUM-BELT-1`. |
| `--tag TAG` | Apply a tag to every printed device; repeatable. |
| `--flatpack` | Request compacted modular output. |
| `--database PATH` | Managed SQLite database. |
| `--wait-timeout-secs N` | Capacity wait timeout; defaults to 21600. |
| `--poll-seconds N` | Capacity poll interval; defaults to 5. |
| `--verbose` / `--log-file PATH` | Enable terminal or file tracing. |
| `--json` | Print the final `QueueReport` as JSON. |

Environment configuration includes `RS_PRINTING_HUB`, `REPLICANT_DB`,
`RS_PRINTING_FLATPACK`, `RS_PRINTING_WAIT_TIMEOUT_SECS`,
`RS_PRINTING_POLL_SECONDS`, `RS_PRINTING_VERBOSE`, and
`RS_PRINTING_LOG_FILE`.

## Failure behavior

Invalid quantities and unknown options fail before connecting. Missing
blueprints, unavailable factories, capacity timeouts, rejected operations, and
ambiguous outcomes return errors without inventing success. Accepted operation
IDs are included in the final report for audit and recovery.

## Verify

```sh
cargo test -p replicant-printing-cli
cargo clippy -p replicant-printing-cli --all-targets -- -D warnings
```
