# replicant-bootstrap-cli

`replicant-bootstrap` establishes a restart-safe, autonomous FTL island in
Beta or Gamma. It manufactures a regional ark at the source hub, loads six
seed-resource freighters, and dispatches all attachment carriers, Cargo
Freighters, and both replicants together. The ark always includes three newly
provisioned Surge Carriers for its relay and beacon reserve. Existing empty
Surge Carriers, Surge Platforms, and Mobile Fleets are reused only when the
rest of the payload still needs capacity; the workflow no longer claims every
idle carrier at the source hub.

At the destination, the explorer performs a lightweight quick-scout route:
it visits nearby systems and runs the replicant system scan endpoint to locate
a dense belt. It does not deploy or use the survey controller and drones until
the later full regional survey. The workflow then establishes a conventional
root relay, regional Autofactories, capital maintenance coverage, and the first
mining site there.
It then runs a full 30 ly survey, selects the closest configured number of
dense-belt systems, expands the island's relay network, and establishes mining
and AMI freight routes back to the regional capital.

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

## Prepare before the replicants exist

`plan` accepts future replicant names. For example, the following is valid even
when neither replicant has been created yet:

```sh
cargo run --quiet -p replicant-bootstrap-cli -- plan \
  --region beta \
  --operator Chats-3 \
  --explorer Chats-4 \
  --mission-file regional-bootstrap-beta.json

cargo run --quiet -p replicant-bootstrap-cli -- stage \
  --mission-file regional-bootstrap-beta.json \
  --log-file logs/regional-bootstrap-beta.log
```

`stage` prints and tags the ark, loads the six seed freighters, attaches every
non-self-travelling device to empty Surge Carriers, Surge Platforms, or Mobile
Fleets, and moves the assembled fleet to the source system's entry point. It
does not depart for the new region or require either planned replicant.

`stage` also performs a manifest check when the mission is already marked
`staged_at_source`. If the profile gained new devices after the original
staging pass, it reconciles completed and queued prints, submits only the
shortfall, builds new carrier loads, and moves those catch-up loads to the
source entry point. Existing staged loads are left in place.

System Hubs are deliberately excluded from the ark. The regional island starts
with a conventional root relay and can receive a System Hub later through a
separate workflow if desired. Existing mission files that still request a
`system_hub` are migrated automatically when `stage` or `run` resumes.

After Chats-3 and Chats-4 exist and have hosted vessels, use the normal command:

```sh
cargo run --quiet -p replicant-bootstrap-cli -- run \
  --mission-file regional-bootstrap-beta.json \
  --log-file logs/regional-bootstrap-beta.log
```

`run` resolves the saved names, reconciles ownership and attachments at the
source entry point, and continues the same mission. `stage` is itself
restart-safe; invoking it again continues an incomplete staging pass.

Replicant identities are revalidated from their requested names whenever
`run` starts. Newly replicated operators missing from the local owned-state
snapshot are discovered through the managed directory and refreshed as owned
details, including their actual replicant and hosted-vessel codes. Do not place
vessel or matrix device codes in the mission's `operator.code` or
`explorer.code` fields.

## Default ark

| Asset | Quantity |
| --- | ---: |
| Complete mining setups | 8 |
| Regional Autofactories | 6 |
| Seed Cargo Freighters | 6 |
| Seed cargo per resource | 500 |
| AMI transport controllers | 6 |
| Conventional root relay | 1 |
| Expansion FTL relays | 18 (two carrier loads) |
| Monitoring beacons | 9 (one carrier load) |
| Newly provisioned Surge Carriers | 3 |
| Capital maintenance drones | 2 |
| Dedicated replicants | 2 |

The seed freighters carry one resource each: carbon, conductive, rares,
silicates, structural, and volatiles. The root relay travels with the general
capital payload; the 18 expansion relays and nine beacons are packed into three
self-contained nine-device carrier loads. Modular structures are printed
flatpacked. Printing continuously fills every source-hub Autofactory queue as
slots become available.

## Persistence and recovery

The parent JSON stores the ark allocation, convoy, quick-scout progress,
selected capital, selected dense belts, and child mission paths. The lightweight
quick scout checkpoints each completed system scan directly in the parent
mission. The mining, full-survey, and relay child missions live under a sibling
`MISSION_STEM.d/MISSION_ID` directory and retain their own checkpoints. The
legacy `quick-survey.json` child path remains in the mission format for
compatibility, but the file is no longer consulted and may be left in place or
removed after the parent mission advances. If an older
quick-scout attempt already launched its selected controller and three drones,
`run` recalls and stows that exact fleet before beginning scan-only travel.

If the process stops during travel or a child workflow, invoke `run` again (or
`stage` again while preparing the source convoy). Every staging pass reconciles
the desired manifest against recorded assets, completed devices, and live
Autofactory jobs carrying the mission's `boot-m:*` tag; only an unaccounted
remainder is submitted. This both prevents duplicate printing and lets an
already-staged mission catch up after the default ark is expanded.

Keep the parent mission, its child directory, and the managed SQLite database
together.

## Useful options

| Option | Meaning |
| --- | --- |
| `--mining-setups N` | Complete staged mining sites, 5–10. |
| `--autofactories N` | Regional Autofactories, 3–6. |
| `--freighters N` | Initial Cargo Freighters, 6–12. |
| `--seed-quantity N` | Units carried by each of the six seed freighters. |
| `--quick-scout-radius LY` | Radius for visit-and-scan reconnaissance used to find the capital's first dense belt. |
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
