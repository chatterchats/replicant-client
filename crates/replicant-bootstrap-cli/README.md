# replicant-bootstrap-cli

`replicant-bootstrap` establishes a restart-safe, autonomous FTL island in
Beta or Gamma. It manufactures a regional ark at the source hub, loads six
seed-resource freighters, and dispatches all Surge Carriers, Cargo Freighters,
and both replicants together.

At the destination, the explorer performs a short survey to locate the first
dense belt. The workflow establishes a System Hub, a conventional root relay,
regional Autofactories, and the first mining site there. It then runs a full
30 ly survey, selects the closest configured number of dense-belt systems,
expands the island's relay network, and establishes mining and AMI freight
routes back to the regional capital.

The island does not need an FTL connection to SCEPTURUM. It can be joined to
the wider network later.

## Typical Beta bootstrap

```sh
export RS_API_TOKEN='your-token'

cargo run --quiet -p replicant-bootstrap-cli -- plan \
  --region beta \
  --landing-star RHWYRHYR \
  --source-hub SCEPTURUM-BELT-1 \
  --operator Chats-1 \
  --explorer Chats-2 \
  --mission-file regional-bootstrap-beta.json

cargo run --quiet -p replicant-bootstrap-cli -- run \
  --mission-file regional-bootstrap-beta.json \
  --log-file logs/regional-bootstrap-beta.log
```

For Gamma, use `--region gamma --landing-star OWLOAEI`. Those landing stars are
also the defaults for their respective regions.

`run` always loads, reconciles, and continues the persisted mission. There is
no separate `resume` command and no `--execute` flag.

## Default ark

| Asset | Quantity |
| --- | ---: |
| Complete mining setups | 8 |
| Regional Autofactories | 6 |
| Seed Cargo Freighters | 6 |
| Seed cargo per resource | 500 |
| AMI transport controllers | 6 |
| System Hubs | 1 |
| Conventional root relays | 1 |
| Dedicated replicants | 2 |

The seed freighters carry one resource each: carbon, conductive, rares,
silicates, structural, and volatiles. Modular structures are printed
flatpacked. Printing continuously fills every source-hub Autofactory queue as
slots become available.

## Persistence and recovery

The parent JSON stores the ark allocation, convoy, selected capital, selected
dense belts, and child mission paths. Child missions live under a sibling
`MISSION_STEM.d/MISSION_ID` directory and retain their own mining, survey, and
relay checkpoints.

If the process stops during travel or a child workflow, invoke `run` again. If
it stops while print submissions are actively being accepted, the workflow
intentionally refuses a blind resubmission; inspect jobs carrying the
`boot-m:*` tag before resolving the `print.submission_started` marker. This
prevents silently printing the entire ark twice.

Keep the parent mission, its child directory, and the managed SQLite database
together.

## Useful options

| Option | Meaning |
| --- | --- |
| `--mining-setups N` | Complete staged mining sites, 5–10. |
| `--autofactories N` | Regional Autofactories, 3–6. |
| `--freighters N` | Initial Cargo Freighters, 6–12. |
| `--seed-quantity N` | Units carried by each of the six seed freighters. |
| `--quick-scout-radius LY` | Radius used to find the capital's first dense belt. |
| `--survey-radius LY` | Full regional survey radius; default 30. |
| `--min-sites N` / `--max-sites N` | Bounds for selected dense-belt systems. |
| `--max-concurrency N` | Concurrent child deployments; default 8. |
| `--verbose` / `--log-file PATH` | Terminal or file tracing. |
| `--json` | Machine-readable plan/status output. |

## Verify

```sh
cargo fmt --all
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```
