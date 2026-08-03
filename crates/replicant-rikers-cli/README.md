# replicant-rikers-cli

`replicant-rikers` produces an explainable shortlist of potential Replicant
Space colony worlds. It performs one full managed synchronization, then applies
hard eligibility predicates and a weighted heuristic entirely against the
committed local snapshot.

The binary is read-only after synchronization. It prints suggestions and never
sends a BobNet message or performs a gameplay mutation. It is not published.

## Run

```sh
export RS_API_TOKEN='your-token'

cargo run -p replicant-rikers-cli -- --limit 10
```

The default database is `replicant-client.sqlite`. Reusing it makes prior
observations available immediately, while the initial full sync refreshes the
authoritative domains needed by the report.

## Options

| Option | Meaning |
| --- | --- |
| `--database PATH` | Managed SQLite database. |
| `--limit N` | Maximum candidates; defaults to 10. |
| `--no-diagnostics` | Hide staged local-query counts. |

`REPLICANT_DB` and `RS_RIKERS_LIMIT` provide environment defaults.

## Interpreting results

Hard predicates reject worlds missing required survey facts or failing colony
constraints. The remaining worlds are ranked with documented environmental,
rotation, distance, and resource clues. The score is a planning heuristic, not
a prediction of a hidden server event score.

Diagnostics show how many local records survive each predicate. A zero count
can mean that the necessary survey value is unknown, not that every world is
known to fail. Refresh or survey the relevant locations before weakening a
filter.

## Verify

```sh
cargo test -p replicant-rikers-cli
cargo clippy -p replicant-rikers-cli --all-targets -- -D warnings
```
